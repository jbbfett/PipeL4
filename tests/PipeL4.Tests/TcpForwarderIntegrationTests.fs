namespace PipeL4.Tests

open System
open System.Collections.Concurrent
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Microsoft.Extensions.Logging
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

    type private SignalingAsyncDisposable
        (
            disposed: TaskCompletionSource<unit>,
            failure: exn option
        ) =
        interface IAsyncDisposable with
            member _.DisposeAsync() =
                disposed.TrySetResult(()) |> ignore

                match failure with
                | Some error -> ValueTask(Task.FromException error)
                | None -> ValueTask()

    type private GatedAsyncDisposable
        (
            started: TaskCompletionSource<unit>,
            release: TaskCompletionSource<unit>,
            completed: TaskCompletionSource<unit>
        ) =
        interface IAsyncDisposable with
            member _.DisposeAsync() =
                ValueTask(
                    task {
                        started.TrySetResult(()) |> ignore
                        do! release.Task
                        completed.TrySetResult(()) |> ignore
                    }
                )

    type private NoopScope() =
        interface IDisposable with
            member _.Dispose() = ()

    type private RecordingLogger() =
        let firstFailure = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)

        member _.FirstFailure = firstFailure.Task

        interface ILogger with
            member _.BeginScope<'TState when 'TState: not null>(_state: 'TState) =
                new NoopScope() :> IDisposable

            member _.IsEnabled(_logLevel) = true

            member _.Log<'TState>
                (
                    _logLevel: LogLevel,
                    _eventId: EventId,
                    _state: 'TState,
                    error: exn | null,
                    _formatter: Func<'TState, exn | null, string>
                ) =
                match Option.ofObj error with
                | Some error -> firstFailure.TrySetResult(error) |> ignore
                | None -> ()

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

    let private runWithForwarder
        startForwarder
        backendPort
        (testCancellationToken: CancellationToken)
        (body: ProxyContext -> Task<unit>)
        =
        task {
            let listenPort = reservePort ()
            let configuration = createConfiguration listenPort backendPort

            use proxyShutdown =
                CancellationTokenSource.CreateLinkedTokenSource testCancellationToken

            let proxyTask = startForwarder configuration proxyShutdown.Token

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

    let private runWithProxy backendPort testCancellationToken body =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runAsync configuration NullLogger.Instance cancellationToken)
            backendPort
            testCancellationToken
            body

    let private completed retainedLength =
        ValueTask<ProcessorResult>({ RetainedLength = retainedLength })

    let private processor action =
        { new IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                action input output cancellationToken }

    let private emit (output: IProcessorOutput) (value: byte array) =
        if value.Length > 0 then
            value.AsSpan().CopyTo(output.GetSpan(value.Length))
            output.Advance(value.Length)

    let private passThroughProcessor () =
        processor (fun input output _ ->
            output.AppendInput(0L, input.Bytes.Length)
            completed 0L)

    let private passThroughPipelines (_: ConnectionState) =
        { ClientToBackend = [| passThroughProcessor () |]
          BackendToClient = [| passThroughProcessor () |] }

    let private runWithProcessorProxy processorPipelineFactory backendPort testCancellationToken body =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runWithProcessorPipelinesAsync
                    configuration
                    processorPipelineFactory
                    NullLogger.Instance
                    cancellationToken)
            backendPort
            testCancellationToken
            body

    let private runWithProcessorProxyAndLogger
        processorPipelineFactory
        (logger: ILogger)
        backendPort
        testCancellationToken
        body
        =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runWithProcessorPipelinesAsync
                    configuration
                    processorPipelineFactory
                    logger
                    cancellationToken)
            backendPort
            testCancellationToken
            body

    let private createPayload length seed =
        Array.init length (fun index -> byte ((index + seed) % 251))

    [<Fact>]
    let ``processor pipelines forward both directions without changing the stream`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProcessorProxy
                    passThroughPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            let fromClient = createPayload 32_000 83
                            let fromBackend = createPayload 29_000 89
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

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
                        })
        }

    [<Fact>]
    let ``processor work in one direction does not block the opposite direction`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (_: ConnectionState) =
                let blocked =
                    processor (fun input output cancellationToken ->
                        if input.IsCompleted then
                            completed 0L
                        else
                            ValueTask<ProcessorResult>(
                                task {
                                    started.TrySetResult(()) |> ignore
                                    do! release.Task.WaitAsync(cancellationToken)
                                    output.AppendInput(0L, input.Bytes.Length)
                                    return { RetainedLength = 0L }
                                }
                            ))

                { ClientToBackend = [| blocked |]
                  BackendToClient = [| passThroughProcessor () |] }

            try
                do!
                    runWithProcessorProxy
                        createPipelines
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend

                                let clientStream = client.GetStream()
                                let backendStream = backend.GetStream()
                                let blockedPayload = createPayload 2048 97
                                let reversePayload = createPayload 3072 101

                                let receiveBlocked =
                                    readExactly backendStream blockedPayload.Length timeout.Token

                                do!
                                    clientStream.WriteAsync(
                                        blockedPayload,
                                        0,
                                        blockedPayload.Length,
                                        timeout.Token
                                    )

                                do! started.Task.WaitAsync(timeout.Token)

                                let receiveReverse =
                                    readExactly clientStream reversePayload.Length timeout.Token

                                do!
                                    backendStream.WriteAsync(
                                        reversePayload,
                                        0,
                                        reversePayload.Length,
                                        timeout.Token
                                    )

                                let! receivedReverse = receiveReverse
                                receivedReverse |> should equal reversePayload

                                release.TrySetResult(()) |> ignore
                                let! receivedBlocked = receiveBlocked
                                receivedBlocked |> should equal blockedPayload
                            })
            finally
                release.TrySetResult(()) |> ignore
        }

    [<Fact>]
    let ``slow processor on one connection does not block another connection`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable createdConnections = 0

            let createPipelines (_: ConnectionState) =
                let connectionNumber = Interlocked.Increment(&createdConnections)

                let clientToBackend =
                    if connectionNumber = 1 then
                        processor (fun input output cancellationToken ->
                            if input.IsCompleted then
                                completed 0L
                            else
                                ValueTask<ProcessorResult>(
                                    task {
                                        started.TrySetResult(()) |> ignore
                                        do! release.Task.WaitAsync(cancellationToken)
                                        output.AppendInput(0L, input.Bytes.Length)
                                        return { RetainedLength = 0L }
                                    }
                                ))
                    else
                        passThroughProcessor ()

                { ClientToBackend = [| clientToBackend |]
                  BackendToClient = [| passThroughProcessor () |] }

            try
                do!
                    runWithProcessorProxy
                        createPipelines
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptFirstBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! firstClient = connect proxy.ListenPort timeout.Token
                                use! firstBackend = acceptFirstBackend
                                let firstPayload = createPayload 2048 127

                                let receiveFirst =
                                    readExactly
                                        (firstBackend.GetStream())
                                        firstPayload.Length
                                        timeout.Token

                                do!
                                    firstClient.GetStream().WriteAsync(
                                        firstPayload,
                                        0,
                                        firstPayload.Length,
                                        timeout.Token
                                    )

                                do! started.Task.WaitAsync(timeout.Token)

                                let acceptSecondBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! secondClient = connect proxy.ListenPort timeout.Token
                                use! secondBackend = acceptSecondBackend
                                let secondPayload = createPayload 3072 131

                                let receiveSecond =
                                    readExactly
                                        (secondBackend.GetStream())
                                        secondPayload.Length
                                        timeout.Token

                                do!
                                    secondClient.GetStream().WriteAsync(
                                        secondPayload,
                                        0,
                                        secondPayload.Length,
                                        timeout.Token
                                    )

                                let! receivedSecond = receiveSecond
                                receivedSecond |> should equal secondPayload

                                release.TrySetResult(()) |> ignore
                                let! receivedFirst = receiveFirst
                                receivedFirst |> should equal firstPayload
                            })
            finally
                release.TrySetResult(()) |> ignore
        }

    [<Fact>]
    let ``multi-stage processor pipeline flushes final output before forwarding EOF`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let stageAMarker = [| 251uy |]
            let stageBMarker = [| 252uy |]
            let mutable stageBSawCompletion = false
            let mutable stageBFinalLength = 0L

            let createPipelines (_: ConnectionState) =
                let retainUntilEnd =
                    processor (fun input output _ ->
                        if input.IsCompleted then
                            output.AppendInput(0L, input.Bytes.Length)
                            emit output stageAMarker
                            completed 0L
                        else
                            completed input.Bytes.Length)

                let finalizeDownstream =
                    processor (fun input output _ ->
                        if input.IsCompleted then
                            stageBSawCompletion <- true
                            stageBFinalLength <- input.Bytes.Length
                            output.AppendInput(0L, input.Bytes.Length)
                            emit output stageBMarker
                            completed 0L
                        else
                            completed input.Bytes.Length)

                { ClientToBackend = [| retainUntilEnd; finalizeDownstream |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithProcessorProxy
                    createPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            let payload = createPayload 4096 103
                            let expected =
                                Array.concat [| payload; stageAMarker; stageBMarker |]

                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let receive = readExactly backendStream expected.Length timeout.Token

                            do!
                                clientStream.WriteAsync(
                                    payload,
                                    0,
                                    payload.Length,
                                    timeout.Token
                                )

                            client.Client.Shutdown(SocketShutdown.Send)

                            let! received = receive
                            received |> should equal expected
                            stageBSawCompletion |> should equal true
                            stageBFinalLength
                            |> should equal (int64 (payload.Length + stageAMarker.Length))
                            do! expectEndOfStream backendStream timeout.Token

                            let response = createPayload 3072 137
                            let receiveResponse = readExactly clientStream response.Length timeout.Token

                            do!
                                backendStream.WriteAsync(
                                    response,
                                    0,
                                    response.Length,
                                    timeout.Token
                                )

                            backend.Client.Shutdown(SocketShutdown.Send)

                            let! receivedResponse = receiveResponse
                            receivedResponse |> should equal response
                            do! expectEndOfStream clientStream timeout.Token
                        })
        }

    [<Fact>]
    let ``processor failure does not affect another connection or the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let createPipelines (_: ConnectionState) =
                let failOnMarker =
                    processor (fun input output _ ->
                        if not input.Bytes.IsEmpty && input.Bytes.FirstSpan[0] = 255uy then
                            raise (InvalidOperationException("processor failed"))

                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

                { ClientToBackend = [| failOnMarker |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithProcessorProxy
                    createPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptHealthyBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! healthyClient = connect proxy.ListenPort timeout.Token
                            use! healthyBackend = acceptHealthyBackend

                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend

                            do! failedClient.GetStream().WriteAsync([| 255uy |], timeout.Token)
                            do! expectDisconnect (failedClient.GetStream()) timeout.Token
                            do! expectDisconnect (failedBackend.GetStream()) timeout.Token

                            let healthyPayload = createPayload 2048 107

                            let receiveHealthy =
                                readExactly
                                    (healthyBackend.GetStream())
                                    healthyPayload.Length
                                    timeout.Token

                            do!
                                healthyClient.GetStream().WriteAsync(
                                    healthyPayload,
                                    0,
                                    healthyPayload.Length,
                                    timeout.Token
                                )

                            let! receivedHealthy = receiveHealthy
                            receivedHealthy |> should equal healthyPayload

                            let acceptNewBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! newClient = connect proxy.ListenPort timeout.Token
                            use! newBackend = acceptNewBackend
                            let newPayload = createPayload 1024 109

                            let receiveNew =
                                readExactly (newBackend.GetStream()) newPayload.Length timeout.Token

                            do!
                                newClient.GetStream().WriteAsync(
                                    newPayload,
                                    0,
                                    newPayload.Length,
                                    timeout.Token
                                )

                            let! receivedNew = receiveNew
                            receivedNew |> should equal newPayload
                        })
        }

    [<Fact>]
    let ``socket failure in a processor pipeline does not stop the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProcessorProxy
                    passThroughPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend

                            failedBackend.Client.LingerState <- LingerOption(true, 0)
                            failedBackend.Dispose()
                            do! expectDisconnect (failedClient.GetStream()) timeout.Token

                            let acceptHealthyBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! healthyClient = connect proxy.ListenPort timeout.Token
                            use! healthyBackend = acceptHealthyBackend
                            let payload = createPayload 2048 113

                            let receive =
                                readExactly (healthyBackend.GetStream()) payload.Length timeout.Token

                            do!
                                healthyClient.GetStream().WriteAsync(
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
    let ``application cancellation closes processor pipeline connections and listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithProcessorProxy
                    passThroughPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)

                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            do! expectConnectionFailure proxy.ListenPort timeout.Token
                        })
        }

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

    [<Fact>]
    let ``connection state is shared across stages and directions but isolated between connections`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let contexts = ConcurrentQueue<int * ConnectionState * obj>()
            let observations = ConcurrentQueue<int * string * obj>()
            let mutable nextConnection = 0

            let createPipelines (state: ConnectionState) =
                let connectionNumber = Interlocked.Increment(&nextConnection)
                let expected = state.GetOrAdd("shared", fun () -> obj())
                contexts.Enqueue((connectionNumber, state, expected))

                let observe stageName =
                    processor (fun input output _ ->
                        if not input.Bytes.IsEmpty then
                            let observed =
                                state.TryGet<obj>("shared")
                                |> Option.defaultWith (fun () -> failwith "Shared state was missing.")

                            observations.Enqueue((connectionNumber, stageName, observed))

                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

                { ClientToBackend = [| observe "first"; observe "second" |]
                  BackendToClient = [| observe "reverse" |] }

            do!
                runWithProcessorProxy
                    createPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptFirst =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! firstClient = connect proxy.ListenPort timeout.Token
                            use! firstBackend = acceptFirst

                            let acceptSecond =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! secondClient = connect proxy.ListenPort timeout.Token
                            use! secondBackend = acceptSecond

                            let firstRequest = [| 1uy; 2uy |]
                            let secondRequest = [| 3uy; 4uy; 5uy |]

                            do!
                                Task.WhenAll(
                                    firstClient.GetStream().WriteAsync(firstRequest, timeout.Token).AsTask(),
                                    secondClient.GetStream().WriteAsync(secondRequest, timeout.Token).AsTask()
                                )

                            let! firstForwarded =
                                readExactly
                                    (firstBackend.GetStream())
                                    firstRequest.Length
                                    timeout.Token

                            let! secondForwarded =
                                readExactly
                                    (secondBackend.GetStream())
                                    secondRequest.Length
                                    timeout.Token

                            firstForwarded |> should equal firstRequest
                            secondForwarded |> should equal secondRequest

                            let firstResponse = [| 6uy |]
                            let secondResponse = [| 7uy; 8uy |]

                            do!
                                Task.WhenAll(
                                    firstBackend.GetStream().WriteAsync(firstResponse, timeout.Token).AsTask(),
                                    secondBackend.GetStream().WriteAsync(secondResponse, timeout.Token).AsTask()
                                )

                            let! firstReturned =
                                readExactly
                                    (firstClient.GetStream())
                                    firstResponse.Length
                                    timeout.Token

                            let! secondReturned =
                                readExactly
                                    (secondClient.GetStream())
                                    secondResponse.Length
                                    timeout.Token

                            firstReturned |> should equal firstResponse
                            secondReturned |> should equal secondResponse

                            let created = contexts.ToArray() |> Array.sortBy (fun (number, _, _) -> number)
                            created.Length |> should equal 2
                            let _, firstState, firstValue = created[0]
                            let _, secondState, secondValue = created[1]
                            Object.ReferenceEquals(firstState, secondState) |> should equal false
                            Object.ReferenceEquals(firstValue, secondValue) |> should equal false

                            let observed = observations.ToArray()

                            for number, _, expected in created do
                                let connectionObservations =
                                    observed
                                    |> Array.filter (fun (observedNumber, _, _) -> observedNumber = number)

                                connectionObservations
                                |> Array.map (fun (_, stageName, _) -> stageName)
                                |> Array.sort
                                |> should equal [| "first"; "reverse"; "second" |]

                                connectionObservations
                                |> Array.forall (fun (_, _, value) -> Object.ReferenceEquals(expected, value))
                                |> should equal true
                        })
        }

    [<Fact>]
    let ``connection state survives half-close and asynchronous cleanup is awaited`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let disposalCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let reverseObserved = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (state: ConnectionState) =
                state.GetOrAdd(
                    "lifetime",
                    fun () ->
                        new GatedAsyncDisposable(
                            disposalStarted,
                            releaseDisposal,
                            disposalCompleted
                        )
                )
                |> ignore

                state.GetOrAdd("marker", fun () -> obj()) |> ignore

                let reverse =
                    processor (fun input output _ ->
                        if not input.Bytes.IsEmpty then
                            state.TryGet<obj>("marker").IsSome |> should equal true
                            reverseObserved.TrySetResult(()) |> ignore

                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

                { ClientToBackend = [| passThroughProcessor () |]
                  BackendToClient = [| reverse |] }

            try
                do!
                    runWithProcessorProxy
                        createPipelines
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend
                                let clientStream = client.GetStream()
                                let backendStream = backend.GetStream()
                                let request = [| 11uy; 12uy |]
                                do! clientStream.WriteAsync(request, timeout.Token)
                                let! forwarded = readExactly backendStream request.Length timeout.Token
                                forwarded |> should equal request

                                client.Client.Shutdown(SocketShutdown.Send)
                                do! expectEndOfStream backendStream timeout.Token
                                disposalStarted.Task.IsCompleted |> should equal false

                                let response = [| 13uy; 14uy; 15uy |]
                                do! backendStream.WriteAsync(response, timeout.Token)
                                let! returned = readExactly clientStream response.Length timeout.Token
                                returned |> should equal response
                                do! reverseObserved.Task.WaitAsync(timeout.Token)

                                backend.Client.Shutdown(SocketShutdown.Send)
                                do! expectEndOfStream clientStream timeout.Token
                                do! disposalStarted.Task.WaitAsync(timeout.Token)

                                proxy.Shutdown.Cancel()
                                proxy.Completion.IsCompleted |> should equal false
                                releaseDisposal.TrySetResult(()) |> ignore
                                do! proxy.Completion.WaitAsync(timeout.Token)
                                disposalCompleted.Task.IsCompletedSuccessfully |> should equal true
                            })
            finally
                releaseDisposal.TrySetResult(()) |> ignore
        }

    [<Fact>]
    let ``state cleanup follows processor failure`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (state: ConnectionState) =
                let failing =
                    processor (fun input _ _ ->
                        if not input.Bytes.IsEmpty then
                            state.GetOrAdd(
                                "owned",
                                fun () -> new SignalingAsyncDisposable(disposed, None)
                            )
                            |> ignore

                            invalidOp "processor failed"

                        completed 0L)

                { ClientToBackend = [| failing |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithProcessorProxy
                    createPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            do! client.GetStream().WriteAsync([| 21uy |], timeout.Token)
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            do! disposed.Task.WaitAsync(timeout.Token)
                        })
        }

    [<Fact>]
    let ``state factory failure cleans previously published values`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (state: ConnectionState) =
                let failing =
                    processor (fun input _ _ ->
                        if not input.Bytes.IsEmpty then
                            state.GetOrAdd(
                                "owned",
                                fun () -> new SignalingAsyncDisposable(disposed, None)
                            )
                            |> ignore

                            state.GetOrAdd<obj>("failed", fun () -> invalidOp "state creation failed")
                            |> ignore

                        completed 0L)

                { ClientToBackend = [| failing |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithProcessorProxy
                    createPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            do! client.GetStream().WriteAsync([| 31uy |], timeout.Token)
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            do! disposed.Task.WaitAsync(timeout.Token)
                        })
        }

    [<Fact>]
    let ``pipeline initialization and state cleanup failures are both reported`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let cleanupFailure = InvalidOperationException("state disposal failed")
            let logger = RecordingLogger()

            let createPipelines (state: ConnectionState) : DirectionalProcessorPipelines =
                state.GetOrAdd(
                    "owned",
                    fun () -> new SignalingAsyncDisposable(disposed, Some cleanupFailure)
                )
                |> ignore

                invalidOp "pipeline initialization failed"

            do!
                runWithProcessorProxyAndLogger
                    createPipelines
                    logger
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            do! disposed.Task.WaitAsync(timeout.Token)

                            let! logged = logger.FirstFailure.WaitAsync(timeout.Token)
                            logged |> should be ofExactType<AggregateException>
                            let aggregate = logged :?> AggregateException
                            aggregate.InnerExceptions.Count |> should equal 2

                            aggregate.InnerExceptions
                            |> Seq.map _.Message
                            |> Seq.toArray
                            |> should equal [| "pipeline initialization failed"; "state disposal failed" |]
                        })
        }

    [<Fact>]
    let ``application cancellation cleans connection state`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let registered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (state: ConnectionState) =
                state.GetOrAdd(
                    "owned",
                    fun () ->
                        registered.TrySetResult(()) |> ignore
                        new SignalingAsyncDisposable(disposed, None)
                )
                |> ignore

                passThroughPipelines state

            do!
                runWithProcessorProxy
                    createPipelines
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            do! registered.Task.WaitAsync(timeout.Token)
                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)
                            do! disposed.Task.WaitAsync(timeout.Token)
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                        })
        }

    [<Fact>]
    let ``slow state initialization does not block another key or TCP direction`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use backendListener = new TcpListener(loopback, 0)
            use releaseSlow = new ManualResetEventSlim(false)
            backendListener.Start()
            let slowStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let fastCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (state: ConnectionState) =
                let slow =
                    processor (fun input output cancellationToken ->
                        if not input.Bytes.IsEmpty then
                            let initialization =
                                Task.Factory.StartNew(
                                    (fun () ->
                                        state.GetOrAdd(
                                            "slow",
                                            fun () ->
                                                slowStarted.TrySetResult(()) |> ignore
                                                releaseSlow.Wait(cancellationToken)
                                                obj()
                                        )),
                                    CancellationToken.None,
                                    TaskCreationOptions.LongRunning,
                                    TaskScheduler.Default
                                )

                            ValueTask<ProcessorResult>(
                                task {
                                    let! _ = initialization.WaitAsync(cancellationToken)
                                    output.AppendInput(0L, input.Bytes.Length)
                                    return { RetainedLength = 0L }
                                }
                            )
                        else
                            completed 0L)

                let fast =
                    processor (fun input output _ ->
                        if not input.Bytes.IsEmpty then
                            state.GetOrAdd("fast", fun () -> obj()) |> ignore
                            fastCompleted.TrySetResult(()) |> ignore

                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

                { ClientToBackend = [| slow |]
                  BackendToClient = [| fast |] }

            try
                do!
                    runWithProcessorProxy
                        createPipelines
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend
                                let fromClient = [| 41uy |]
                                let fromBackend = [| 42uy |]

                                do!
                                    Task.WhenAll(
                                        client.GetStream().WriteAsync(fromClient, timeout.Token).AsTask(),
                                        backend.GetStream().WriteAsync(fromBackend, timeout.Token).AsTask()
                                    )

                                do! slowStarted.Task.WaitAsync(timeout.Token)
                                do! fastCompleted.Task.WaitAsync(timeout.Token)

                                let! receivedAtClient =
                                    readExactly
                                        (client.GetStream())
                                        fromBackend.Length
                                        timeout.Token

                                receivedAtClient |> should equal fromBackend
                                releaseSlow.Set()

                                let! receivedAtBackend =
                                    readExactly
                                        (backend.GetStream())
                                        fromClient.Length
                                        timeout.Token

                                receivedAtBackend |> should equal fromClient
                            })
            finally
                releaseSlow.Set()
        }
