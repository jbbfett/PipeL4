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

    [<NoEquality; NoComparison>]
    type private ProxyContext =
        { ListenPort: int
          Shutdown: CancellationTokenSource
          Completion: Task }

    let private endpointPort (listener: TcpListener) =
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let private socketPort (socket: Socket) =
        match socket.LocalEndPoint with
        | :? IPEndPoint as endpoint -> endpoint.Port
        | _ -> failwith "Expected the socket to have an IP endpoint."

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

    let private connect port (cancellationToken: CancellationToken) =
        task {
            let client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(loopback, port, cancellationToken)
                return client
            with error ->
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

    let private readExactlyWithBoundaries
        (stream: NetworkStream)
        length
        (readSizes: int array)
        (cancellationToken: CancellationToken)
        =
        task {
            let result = Array.zeroCreate<byte> length
            let mutable offset = 0
            let mutable readIndex = 0

            while offset < length do
                let requested = min readSizes[readIndex % readSizes.Length] (length - offset)
                let! read = stream.ReadAsync(result.AsMemory(offset, requested), cancellationToken)

                if read = 0 then
                    failwithf "Stream ended after %d of %d expected bytes." offset length

                offset <- offset + read
                readIndex <- readIndex + 1

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

    let private expectConnectionFailure port (cancellationToken: CancellationToken) =
        task {
            use client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(loopback, port, cancellationToken)
                failwith "Expected the connection attempt to fail."
            with :? SocketException ->
                ()
        }

    let private runWithProxy
        backendPort
        (testCancellationToken: CancellationToken)
        (body: ProxyContext -> Task<unit>)
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

            let proxy =
                { ListenPort = listenPort
                  Shutdown = proxyShutdown
                  Completion = proxyTask :> Task }

            let mutable failure: exn option = None

            try
                do! body proxy
            with error ->
                failure <- Some error

            proxyShutdown.Cancel()

            try
                do! proxyTask.WaitAsync(testCancellationToken)
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
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
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
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
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
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
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
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
                        use! backend = acceptBackend

                        let payload = createPayload (3 * 64 * 1024 + 137) 23
                        let receive = readExactly (backend.GetStream()) payload.Length timeout.Token

                        do! client.GetStream().WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``treats multiple writes and reads as one ordered byte stream`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
                        use! backend = acceptBackend

                        let chunks =
                            [| createPayload 7 29
                               createPayload 31 31
                               createPayload 8193 37
                               createPayload 3 41
                               createPayload 65_537 43 |]

                        let expected = Array.concat chunks

                        let receive =
                            readExactlyWithBoundaries
                                (backend.GetStream())
                                expected.Length
                                [| 1; 17; 4093; 5; 257 |]
                                timeout.Token

                        for chunk in chunks do
                            do! client.GetStream().WriteAsync(chunk, 0, chunk.Length, timeout.Token)

                        let! received = receive
                        received |> should equal expected
                    })
        }

    [<Fact>]
    let ``forwards multiple concurrent connections independently`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let clients = ResizeArray<TcpClient>()
                        let backends = ResizeArray<TcpClient>()

                        try
                            for _ in 1..4 do
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                let! client = connect proxy.ListenPort timeout.Token
                                clients.Add(client)
                                let! backend = acceptBackend
                                backends.Add(backend)

                            let transfers =
                                Array.init clients.Count (fun index ->
                                    task {
                                        let fromClient = createPayload (12_000 + index * 137) (67 + index)
                                        let fromBackend = createPayload (9_000 + index * 211) (79 + index)
                                        let clientStream = clients[index].GetStream()
                                        let backendStream = backends[index].GetStream()

                                        let receiveAtBackend =
                                            readExactly backendStream fromClient.Length timeout.Token

                                        let receiveAtClient =
                                            readExactly clientStream fromBackend.Length timeout.Token

                                        do!
                                            Task.WhenAll(
                                                clientStream.WriteAsync(
                                                    fromClient,
                                                    0,
                                                    fromClient.Length,
                                                    timeout.Token
                                                ),
                                                backendStream.WriteAsync(
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
                                    }
                                    :> Task)

                            do! Task.WhenAll(transfers)
                        finally
                            for client in clients do
                                client.Dispose()

                            for backend in backends do
                                backend.Dispose()
                    })
        }

    [<Fact>]
    let ``backend EOF leaves client to backend direction usable`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
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

                        client.Client.Shutdown(SocketShutdown.Send)
                        do! expectEndOfStream backendStream timeout.Token
                    })
        }

    [<Fact>]
    let ``client EOF leaves backend to client direction usable`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
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

                        backend.Client.Shutdown(SocketShutdown.Send)
                        do! expectEndOfStream clientStream timeout.Token
                    })
        }

    [<Fact>]
    let ``client reset disconnects its backend`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
                        use! backend = acceptBackend

                        client.Client.LingerState <- LingerOption(true, 0)
                        client.Dispose()

                        do! expectDisconnect (backend.GetStream()) timeout.Token
                    })
        }

    [<Fact>]
    let ``backend connection failure does not stop the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener =
                new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)

            backendListener.Bind(IPEndPoint(loopback, 0))

            do!
                runWithProxy (socketPort backendListener) timeout.Token (fun proxy ->
                    task {
                        use! failedClient = connect proxy.ListenPort timeout.Token
                        do! expectDisconnect (failedClient.GetStream()) timeout.Token

                        backendListener.Listen(1)
                        let acceptBackend = backendListener.AcceptAsync(timeout.Token).AsTask()

                        use! client = connect proxy.ListenPort timeout.Token
                        use! backend = acceptBackend
                        use backendStream = new NetworkStream(backend, false)

                        let payload = createPayload 1024 53
                        let receive = readExactly backendStream payload.Length timeout.Token

                        do! client.GetStream().WriteAsync(payload, 0, payload.Length, timeout.Token)

                        let! received = receive
                        received |> should equal payload
                    })
        }

    [<Fact>]
    let ``backend reset does not affect another connection or the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptHealthyBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! healthyClient = connect proxy.ListenPort timeout.Token
                        use! healthyBackend = acceptHealthyBackend

                        let acceptFailedBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! failedClient = connect proxy.ListenPort timeout.Token
                        use! failedBackend = acceptFailedBackend

                        failedBackend.Client.LingerState <- LingerOption(true, 0)
                        failedBackend.Dispose()

                        do! expectDisconnect (failedClient.GetStream()) timeout.Token

                        let fromHealthyClient = createPayload 2048 59
                        let fromHealthyBackend = createPayload 3072 61

                        let receiveAtHealthyBackend =
                            readExactly
                                (healthyBackend.GetStream())
                                fromHealthyClient.Length
                                timeout.Token

                        let receiveAtHealthyClient =
                            readExactly
                                (healthyClient.GetStream())
                                fromHealthyBackend.Length
                                timeout.Token

                        do!
                            Task.WhenAll(
                                healthyClient.GetStream().WriteAsync(
                                    fromHealthyClient,
                                    0,
                                    fromHealthyClient.Length,
                                    timeout.Token
                                ),
                                healthyBackend.GetStream().WriteAsync(
                                    fromHealthyBackend,
                                    0,
                                    fromHealthyBackend.Length,
                                    timeout.Token
                                )
                            )

                        let! receivedAtHealthyBackend = receiveAtHealthyBackend
                        let! receivedAtHealthyClient = receiveAtHealthyClient
                        receivedAtHealthyBackend |> should equal fromHealthyClient
                        receivedAtHealthyClient |> should equal fromHealthyBackend

                        let acceptNewBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! newClient = connect proxy.ListenPort timeout.Token
                        use! newBackend = acceptNewBackend

                        let newPayload = createPayload 1024 71

                        let receiveAtNewBackend =
                            readExactly (newBackend.GetStream()) newPayload.Length timeout.Token

                        do!
                            newClient.GetStream().WriteAsync(
                                newPayload,
                                0,
                                newPayload.Length,
                                timeout.Token
                            )

                        let! receivedAtNewBackend = receiveAtNewBackend
                        receivedAtNewBackend |> should equal newPayload
                    })
        }

    [<Fact>]
    let ``application cancellation closes active connections and listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProxy (endpointPort backendListener) timeout.Token (fun proxy ->
                    task {
                        let acceptFirstBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! firstClient = connect proxy.ListenPort timeout.Token
                        use! firstBackend = acceptFirstBackend

                        let acceptSecondBackend =
                            backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                        use! secondClient = connect proxy.ListenPort timeout.Token
                        use! secondBackend = acceptSecondBackend

                        proxy.Shutdown.Cancel()
                        do! proxy.Completion.WaitAsync(timeout.Token)

                        do! expectDisconnect (firstClient.GetStream()) timeout.Token
                        do! expectDisconnect (firstBackend.GetStream()) timeout.Token
                        do! expectDisconnect (secondClient.GetStream()) timeout.Token
                        do! expectDisconnect (secondBackend.GetStream()) timeout.Token
                        do! expectConnectionFailure proxy.ListenPort timeout.Token
                    })
        }
