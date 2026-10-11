namespace PipeL4.Tests

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Microsoft.Extensions.Logging.Abstractions
open PipeL4.Core
open Xunit

module TcpForwarderIntegrationTests =
    let private loopback = IPAddress.Loopback

    let private endpointPort (listener: TcpListener) =
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let private reservePort () =
        use listener = new TcpListener(loopback, 0)
        listener.Start()
        endpointPort listener

    let private createConfiguration listenPort backendPort =
        let arguments =
            [| "--listen-address"
               string loopback
               "--listen-port"
               string listenPort
               "--backend-host"
               string loopback
               "--backend-port"
               string backendPort |]

        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Test configuration was invalid: %s" error

    let rec private connectWithRetry port (cancellationToken: CancellationToken) =
        task {
            let client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(loopback, port, cancellationToken)
                return client
            with
            | :? SocketException when not cancellationToken.IsCancellationRequested ->
                client.Dispose()
                do! Task.Delay((10: int), cancellationToken)
                return! connectWithRetry port cancellationToken
            | error ->
                client.Dispose()
                return raise error
        }

    let private readExactly
        (stream: NetworkStream)
        length
        (cancellationToken: CancellationToken)
        =
        task {
            let result = Array.zeroCreate<byte> length
            let mutable offset = 0

            while offset < length do
                let! read =
                    stream.ReadAsync(
                        result.AsMemory(offset, length - offset),
                        cancellationToken
                    )

                if read = 0 then
                    failwithf "Stream ended after %d of %d expected bytes." offset length

                offset <- offset + read

            return result
        }

    let private expectEndOfStream
        (stream: NetworkStream)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> 1
            let! read = stream.ReadAsync(buffer.AsMemory(), cancellationToken)
            read |> should equal 0
        }

    let private expectDisconnect
        (stream: NetworkStream)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> 1

            try
                let! read = stream.ReadAsync(buffer.AsMemory(), cancellationToken)

                if read <> 0 then
                    failwith "Expected the connection to close."
            with
            | :? IOException
            | :? SocketException
            | :? ObjectDisposedException -> ()
        }

    let private runWithProxy
        backendPort
        (testCancellationToken: CancellationToken)
        (body: int -> CancellationTokenSource -> Task<unit>)
        =
        task {
            let listenPort = reservePort ()
            let configuration = createConfiguration listenPort backendPort

            use proxyShutdown =
                CancellationTokenSource.CreateLinkedTokenSource testCancellationToken

            let proxyTask =
                TcpForwarder.runAsync
                    configuration
                    NullLogger.Instance
                    proxyShutdown.Token

            let mutable failure: exn option = None

            try
                do! body listenPort proxyShutdown
            with error ->
                failure <- Some error

            proxyShutdown.Cancel()

            try
                do! proxyTask.WaitAsync testCancellationToken
            with error ->
                if failure.IsNone then
                    failure <- Some error

            match failure with
            | Some error ->
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error).Throw()
                return ()
            | None -> return ()
        }

    let private createPayload length seed =
        Array.init length (fun index -> byte ((index + seed) % 251))

    [<Fact>]
    let ``forwards bytes from client to backend`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let payload = createPayload 4096 3
                        let receive = readExactly (backend.GetStream()) payload.Length timeout.Token

                        do! client.GetStream().WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``forwards bytes from backend to client`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let payload = createPayload 4096 7
                        let receive = readExactly (client.GetStream()) payload.Length timeout.Token

                        do! backend.GetStream().WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``forwards both directions concurrently`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let fromClient = createPayload 32_000 11
                        let fromBackend = createPayload 32_000 17

                        let receiveAtBackend =
                            readExactly (backend.GetStream()) fromClient.Length timeout.Token

                        let receiveAtClient =
                            readExactly (client.GetStream()) fromBackend.Length timeout.Token

                        do!
                            Task.WhenAll(
                                client.GetStream().WriteAsync(
                                    fromClient,
                                    0,
                                    fromClient.Length,
                                    timeout.Token
                                ),
                                backend.GetStream().WriteAsync(
                                    fromBackend,
                                    0,
                                    fromBackend.Length,
                                    timeout.Token
                                )
                            )

                        let! receivedAtBackend = receiveAtBackend
                        let! receivedAtClient = receiveAtClient

                        receivedAtBackend |> should equal fromClient
                        receivedAtClient |> should equal fromBackend
                    })
        }

    [<Fact>]
    let ``forwards payload larger than the internal buffer`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let payload = createPayload (3 * 64 * 1024 + 137) 23
                        let receive = readExactly (backend.GetStream()) payload.Length timeout.Token

                        do! client.GetStream().WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``treats multiple writes as one ordered byte stream`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let chunks =
                            [| createPayload 7 29
                               createPayload 31 31
                               createPayload 8193 37
                               createPayload 3 41 |]

                        let expected = Array.concat chunks
                        let receive = readExactly (backend.GetStream()) expected.Length timeout.Token

                        for chunk in chunks do
                            do! client.GetStream().WriteAsync(chunk, 0, chunk.Length, timeout.Token)

                        let! received = receive
                        received |> should equal expected
                    })
        }

    [<Fact>]
    let ``backend EOF leaves client to backend direction usable`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let clientStream = client.GetStream()
                        let backendStream = backend.GetStream()

                        backend.Client.Shutdown(SocketShutdown.Send)
                        do! expectEndOfStream clientStream timeout.Token

                        let payload = createPayload 2048 43
                        let receive = readExactly backendStream payload.Length timeout.Token

                        do! clientStream.WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``client EOF leaves backend to client direction usable`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let clientStream = client.GetStream()
                        let backendStream = backend.GetStream()

                        client.Client.Shutdown(SocketShutdown.Send)
                        do! expectEndOfStream backendStream timeout.Token

                        let payload = createPayload 2048 47
                        let receive = readExactly clientStream payload.Length timeout.Token

                        do! backendStream.WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``backend connection failure does not stop the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            let backendPort = reservePort ()

            do!
                runWithProxy backendPort timeout.Token (fun proxyPort _ ->
                    task {
                        use! failedClient = connectWithRetry proxyPort timeout.Token
                        do! expectDisconnect (failedClient.GetStream()) timeout.Token

                        use backendListener = new TcpListener(loopback, backendPort)
                        backendListener.Start()

                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        let payload = createPayload 1024 53
                        let receive = readExactly (backend.GetStream()) payload.Length timeout.Token

                        do! client.GetStream().WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``established connection reset does not stop the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort _ ->
                    task {
                        let acceptFirstBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! firstClient = connectWithRetry proxyPort timeout.Token
                        use! firstBackend = acceptFirstBackend

                        firstBackend.Client.LingerState <- LingerOption(true, 0)
                        firstBackend.Dispose()

                        let trigger = createPayload 32 59

                        try
                            do!
                                firstClient.GetStream().WriteAsync(
                                    trigger,
                                    0,
                                    trigger.Length,
                                    timeout.Token
                                )
                        with
                        | :? IOException
                        | :? SocketException -> ()

                        do! expectDisconnect (firstClient.GetStream()) timeout.Token

                        let acceptSecondBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! secondClient = connectWithRetry proxyPort timeout.Token
                        use! secondBackend = acceptSecondBackend

                        let payload = createPayload 1024 61
                        let receive =
                            readExactly (secondBackend.GetStream()) payload.Length timeout.Token

                        do!
                            secondClient.GetStream().WriteAsync(
                                payload,
                                0,
                                payload.Length,
                                timeout.Token
                            )

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``application cancellation closes active connections`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxyPort proxyShutdown ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connectWithRetry proxyPort timeout.Token
                        use! backend = acceptBackend

                        proxyShutdown.Cancel()

                        do! expectDisconnect (client.GetStream()) timeout.Token
                        do! expectDisconnect (backend.GetStream()) timeout.Token
                    })
        }
