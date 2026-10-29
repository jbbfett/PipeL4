namespace PipeL4.Tests

open System
open System.Collections.Concurrent
open System.Collections.Generic
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
    type private StructuredLogRecord =
        { Level: LogLevel
          Message: string
          Properties: (string * obj) array }

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

    type private GatedDisposalProcessor
        (
            action: ProcessorInput -> IProcessorOutput -> CancellationToken -> ValueTask<ProcessorResult>,
            disposalStarted: TaskCompletionSource<unit>,
            releaseDisposal: TaskCompletionSource<unit>,
            disposalCompleted: TaskCompletionSource<unit>
        ) =
        interface IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                action input output cancellationToken

        interface IAsyncDisposable with
            member _.DisposeAsync() =
                ValueTask(
                    task {
                        disposalStarted.TrySetResult(()) |> ignore
                        do! releaseDisposal.Task
                        disposalCompleted.TrySetResult(()) |> ignore
                    }
                )

    type private NoopScope() =
        interface IDisposable with
            member _.Dispose() = ()

    type private RecordingLogger() =
        let firstFailure = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)
        let twoFailures = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let failures = ConcurrentQueue<exn>()
        let messages = ConcurrentQueue<string>()
        let structuredRecords = ConcurrentQueue<StructuredLogRecord>()
        let mutable failureCount = 0

        member _.FirstFailure = firstFailure.Task
        member _.TwoFailures = twoFailures.Task
        member _.Failures = failures.ToArray()
        member _.Messages = messages.ToArray()
        member _.StructuredRecords = structuredRecords.ToArray()

        interface ILogger with
            member _.BeginScope<'TState when 'TState: not null>(_state: 'TState) =
                new NoopScope() :> IDisposable

            member _.IsEnabled(_logLevel) = true

            member _.Log<'TState>
                (
                    _logLevel: LogLevel,
                    _eventId: EventId,
                    state: 'TState,
                    error: exn | null,
                    formatter: Func<'TState, exn | null, string>
                ) =
                messages.Enqueue(formatter.Invoke(state, error))

                let properties =
                    match box state with
                    | :? IEnumerable<KeyValuePair<string, obj>> as values ->
                        values |> Seq.map (fun value -> value.Key, value.Value) |> Seq.toArray
                    | _ -> Array.empty

                structuredRecords.Enqueue(
                    { Level = _logLevel
                      Message = formatter.Invoke(state, error)
                      Properties = properties }
                )

                match Option.ofObj error with
                | Some error ->
                    failures.Enqueue error
                    firstFailure.TrySetResult(error) |> ignore

                    if Interlocked.Increment(&failureCount) >= 2 then
                        twoFailures.TrySetResult(()) |> ignore
                | None -> ()

    type private ThrowingOptionalDiagnosticsLogger() =
        let failures = ConcurrentQueue<exn>()
        let mutable optionalFailureCount = 0

        member _.Failures = failures.ToArray()
        member _.OptionalFailureCount = Volatile.Read(&optionalFailureCount)

        interface ILogger with
            member _.BeginScope<'TState when 'TState: not null>(_state: 'TState) =
                new NoopScope() :> IDisposable

            member _.IsEnabled(_logLevel) = true

            member _.Log<'TState>
                (
                    _logLevel: LogLevel,
                    _eventId: EventId,
                    state: 'TState,
                    error: exn | null,
                    formatter: Func<'TState, exn | null, string>
                ) =
                let message = formatter.Invoke(state, error)

                if message.Contains("runtime diagnostics") then
                    Interlocked.Increment(&optionalFailureCount) |> ignore
                    raise (InvalidOperationException("optional diagnostics logger failed"))

                match Option.ofObj error with
                | Some value -> failures.Enqueue value
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

    let private createConfigurationWithArguments listenPort backendPort extraArguments =
        let arguments =
            Array.append
                [| "--listen-address"
                   string loopback
                   "--listen-port"
                   string listenPort
                   "--backend-host"
                   string loopback
                   "--backend-port"
                   string backendPort |]
                extraArguments

        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Test configuration was invalid: %s" error

    let private createConfiguration listenPort backendPort =
        createConfigurationWithArguments listenPort backendPort Array.empty

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

    let private runWithForwarderAndArguments
        startForwarder
        backendPort
        extraArguments
        (testCancellationToken: CancellationToken)
        (body: ProxyContext -> Task<unit>)
        =
        task {
            let listenPort = reservePort ()
            let configuration =
                createConfigurationWithArguments listenPort backendPort extraArguments

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

    let private runWithForwarder
        startForwarder
        backendPort
        (testCancellationToken: CancellationToken)
        (body: ProxyContext -> Task<unit>)
        =
        runWithForwarderAndArguments
            startForwarder
            backendPort
            Array.empty
            testCancellationToken
            body

    let private runWithProcessorProxyAndLoggerForDiagnostics
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

    let private runWithProxy backendPort testCancellationToken body =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runAsync configuration NullLogger.Instance cancellationToken)
            backendPort
            testCancellationToken
            body

    let private runWithObservedProxy observer backendPort testCancellationToken body =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runWithMemoryObserverAsync
                    configuration
                    observer
                    NullLogger.Instance
                    cancellationToken)
            backendPort
            testCancellationToken
            body

    let private runWithConfiguredObservedProxy
        capacityArguments
        observer
        backendPort
        testCancellationToken
        body
        =
        runWithForwarderAndArguments
            (fun configuration cancellationToken ->
                TcpForwarder.runWithMemoryObserverAsync
                    configuration
                    observer
                    NullLogger.Instance
                    cancellationToken)
            backendPort
            capacityArguments
            testCancellationToken
            body

    let private runWithConfiguredObservedProcessorProxy
        capacityArguments
        processorPipelineFactory
        observer
        backendPort
        testCancellationToken
        body
        =
        runWithForwarderAndArguments
            (fun configuration cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                    configuration
                    processorPipelineFactory
                    observer
                    NullLogger.Instance
                    cancellationToken)
            backendPort
            capacityArguments
            testCancellationToken
            body

    let private runWithConfiguredObservedProcessorProxyAndLogger
        capacityArguments
        processorPipelineFactory
        observer
        (logger: ILogger)
        backendPort
        testCancellationToken
        body
        =
        runWithForwarderAndArguments
            (fun configuration cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                    configuration
                    processorPipelineFactory
                    observer
                    logger
                    cancellationToken)
            backendPort
            capacityArguments
            testCancellationToken
            body

    let private runWithConfiguredObservedProcessorProxyAndTestHooks
        capacityArguments
        processorPipelineFactory
        observer
        (testHooks: TcpForwarderTestHooks)
        (logger: ILogger)
        backendPort
        testCancellationToken
        body
        =
        runWithForwarderAndArguments
            (fun configuration cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAndTestHooksAsync
                    configuration
                    processorPipelineFactory
                    observer
                    testHooks
                    logger
                    cancellationToken)
            backendPort
            capacityArguments
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

    let private runtimeDiagnosticRecords (logger: RecordingLogger) =
        logger.StructuredRecords
        |> Array.filter (fun record -> record.Message.Contains("runtime diagnostics"))

    let private waitForPhysicalShareWaiter
        (accounting: ConnectionRuntimeMetrics)
        (predicate: CapacityUsageSnapshot -> bool)
        (cancellationToken: CancellationToken)
        =
        task {
            let mutable snapshot = accounting.GetSnapshot().ClientToBackendPhysicalShare
            let mutable satisfied = predicate snapshot

            while not satisfied do
                do! Task.Delay(10, cancellationToken)
                snapshot <- accounting.GetSnapshot().ClientToBackendPhysicalShare
                satisfied <- predicate snapshot

            return snapshot
        }

    let private property name (record: StructuredLogRecord) =
        record.Properties
        |> Array.tryFind (fun (key, _) -> key = name)
        |> Option.map snd

    let private requiredProperty name (record: StructuredLogRecord) =
        property name record
        |> Option.defaultWith (fun () -> failwithf "Structured property '%s' was missing." name)

    let private int64Property name record =
        requiredProperty name record |> unbox<int64>

    let private doubleProperty name record =
        requiredProperty name record |> unbox<float>

    let private stringProperty name record =
        requiredProperty name record :?> string

    let private passThroughPipelines (_: ConnectionState) =
        { ClientToBackend = [| passThroughProcessor () |]
          BackendToClient = [| passThroughProcessor () |] }

    [<Fact>]
    let ``enabled terminal diagnostics emit structured connection and direction summaries`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()

            do!
                runWithForwarderAndArguments
                    (fun configuration cancellationToken ->
                        TcpForwarder.runAsync configuration (logger :> ILogger) cancellationToken)
                    (endpointPort backendListener)
                    Array.empty
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)

                            while runtimeDiagnosticRecords logger |> Array.length < 3 do
                                do! Task.Delay(10, timeout.Token)
                        })

            let records = runtimeDiagnosticRecords logger
            records.Length |> should equal 3

            let connection =
                records
                |> Array.find (fun record -> record.Message.StartsWith("Connection "))

            property "ConnectionPhysicalLimitBytes" connection |> Option.isSome |> should equal true
            property "ConnectionPhysicalTotalWaitMilliseconds" connection
            |> Option.isSome
            |> should equal true

            let directions =
                records
                |> Array.filter (fun record ->
                    record.Message.StartsWith("Direction ")
                    && not (record.Message.Contains(" stage ")))

            directions.Length |> should equal 2

            for direction in directions do
                property "TerminalOutcome" direction |> Option.isSome |> should equal true
                property "PhysicalShareHandoffState" direction |> Option.isSome |> should equal true
                property "ConnectionId" direction |> should equal (Some(box 1L))
        }

    [<Fact>]
    let ``parsed production capacities reach processor forwarding and terminal diagnostics`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let capacityArguments =
                [| "--client-to-server-logical-capacity-bytes"
                   "32"
                   "--server-to-client-logical-capacity-bytes"
                   "48"
                   "--connection-physical-capacity-bytes"
                   "128" |]

            do!
                runWithConfiguredObservedProcessorProxyAndLogger
                    capacityArguments
                    passThroughPipelines
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let clientPayload = [| 1uy; 2uy; 3uy; 4uy; 5uy |]
                            let backendPayload = [| 9uy; 8uy; 7uy; 6uy |]
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let receiveAtBackend =
                                readExactly backendStream clientPayload.Length timeout.Token

                            let receiveAtClient =
                                readExactly clientStream backendPayload.Length timeout.Token

                            do!
                                Task.WhenAll(
                                    clientStream.WriteAsync(clientPayload, timeout.Token).AsTask(),
                                    backendStream.WriteAsync(backendPayload, timeout.Token).AsTask()
                                )

                            let! receivedAtBackend = receiveAtBackend
                            let! receivedAtClient = receiveAtClient
                            receivedAtBackend |> should equal clientPayload
                            receivedAtClient |> should equal backendPayload

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                            do! expectEndOfStream backendStream timeout.Token
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.ConnectionPhysicalCapacity.LimitBytes |> should equal 128L
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.PeakPhysicalBufferCapacityBytes |> should be (greaterThan 0L)
            snapshot.PeakPhysicalBufferCapacityBytes |> should be (lessThanOrEqualTo 128L)

            let assertDirection direction expectedLimit expectedInput =
                direction.LogicalCapacityLimitBytes |> should equal expectedLimit
                direction.CurrentLogicalBufferedBytes |> should equal 0L
                direction.PeakLogicalBufferedBytes |> should be (greaterThan 0L)
                direction.PeakLogicalBufferedBytes |> should be (lessThanOrEqualTo expectedLimit)
                direction.InputBytes |> should equal expectedInput
                direction.InputBackedOutputBytes |> should equal expectedInput
                direction.GeneratedOutputBytes |> should equal 0L
                direction.CurrentRetainedBytes |> should equal 0L
                direction.TerminalOutcome |> should equal "completed"
                direction.CleanupFailurePresent |> should equal false
                direction.ReceiveSuccessfullyExposedOwners |> should be (greaterThan 0L)
                direction.LogicalReceiveExhaustionCount |> should equal 0L
                direction.SynchronousLogicalOutputExhaustionCount |> should equal 0L
                direction.SynchronousPhysicalOutputExhaustionCount |> should equal 0L
                direction.PhysicalReceiveExhaustionCount |> should equal 0L
                direction.PoolOwnerFitFailureCount |> should equal 0L

            assertDirection snapshot.ClientToBackend 32L 5L
            assertDirection snapshot.BackendToClient 48L 4L

            let records = runtimeDiagnosticRecords logger
            records.Length |> should equal 5

            let connection =
                records
                |> Array.find (fun record -> record.Message.StartsWith("Connection "))

            property "ConnectionId" connection |> should equal (Some(box 1L))
            int64Property "CurrentPhysicalBufferCapacityBytes" connection |> should equal 0L
            int64Property "PeakPhysicalBufferCapacityBytes" connection
            |> should be (greaterThan 0L)
            int64Property "ConnectionPhysicalLimitBytes" connection |> should equal 128L
            property "ClientToBackendHandoffState" connection
            |> should equal (Some(box "completed"))
            property "BackendToClientHandoffState" connection
            |> should equal (Some(box "completed"))
            property "CleanupFailurePresent" connection |> should equal (Some(box false))

            let directions =
                records
                |> Array.filter (fun record ->
                    record.Message.StartsWith("Direction ")
                    && not (record.Message.Contains(" stage ")))

            directions.Length |> should equal 2

            let clientDirection =
                directions
                |> Array.find (fun record -> stringProperty "Direction" record = "client-to-backend")

            let backendDirection =
                directions
                |> Array.find (fun record -> stringProperty "Direction" record = "backend-to-client")

            int64Property "LogicalCapacityLimitBytes" clientDirection |> should equal 32L
            int64Property "LogicalCapacityLimitBytes" backendDirection |> should equal 48L

            for direction in [| clientDirection; backendDirection |] do
                property "ConnectionId" direction |> should equal (Some(box 1L))
                property "TerminalOutcome" direction |> should equal (Some(box "completed"))
                int64Property "CurrentLogicalBufferedBytes" direction |> should equal 0L
                property "PhysicalShareHandoffState" direction
                |> should equal (Some(box "completed"))
                property "CleanupFailurePresent" direction |> should equal (Some(box false))

            let stages =
                records
                |> Array.filter (fun record -> record.Message.Contains(" stage "))

            stages.Length |> should equal 2

            for stage in stages do
                property "ConnectionId" stage |> should equal (Some(box 1L))
                property "StageIndex" stage |> should equal (Some(box 0))
                int64Property "GeneratedOutputBytes" stage |> should equal 0L
                int64Property "CurrentRetainedBytes" stage |> should equal 0L
        }

    [<Fact>]
    let ``production forwarding physical-share waits resume and reach terminal diagnostics`` () =
        task {
            use timeout = new CancellationTokenSource(15_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )
            let secondReceiveReady =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let expansionCommitted =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let backendEndOfStream =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let backendReceiveStarted =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseExpansion =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable clientToBackendReceiveCount = 0

            use releaseGatesOnTimeout =
                timeout.Token.Register(fun () ->
                    expansionCommitted.TrySetResult(()) |> ignore
                    releaseExpansion.TrySetResult(()) |> ignore)

            let createPipelines (_: ConnectionState) =
                let retainUntilSecondReceive =
                    processor (fun input output _ ->
                        if input.IsCompleted then
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L
                        elif input.Bytes.Length = 1L then
                            completed input.Bytes.Length
                        else
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L)

                { ClientToBackend = [| retainUntilSecondReceive |]
                  BackendToClient = [| passThroughProcessor () |] }

            let hooks =
                { BeforeReceive =
                      (fun direction ->
                          if direction = BackendToClient then
                              backendReceiveStarted.TrySetResult(()) |> ignore
                          elif direction = ClientToBackend
                             && Interlocked.Increment(&clientToBackendReceiveCount) = 2 then
                              secondReceiveReady.TrySetResult(()) |> ignore
                              expansionCommitted.Task.GetAwaiter().GetResult())
                  AfterReceive =
                      (fun direction received ->
                          if direction = BackendToClient && received = 0 then
                              backendEndOfStream.TrySetResult(()) |> ignore)
                  PendingSend = ignore
                  CapacityAccounting =
                      Some
                          { BeforePhysicalShareExpansion =
                                (fun clientToBackend ->
                                    if not clientToBackend then
                                        expansionCommitted.TrySetResult(()) |> ignore
                                        releaseExpansion.Task.GetAwaiter().GetResult())
                            AfterPhysicalShareExpansion = ignore } }

            try
                do!
                    runWithConfiguredObservedProcessorProxyAndTestHooks
                        [| "--client-to-server-logical-capacity-bytes"
                           "1024"
                           "--server-to-client-logical-capacity-bytes"
                           "1024"
                           "--connection-physical-capacity-bytes"
                           "256" |]
                        createPipelines
                        (fun value -> observed.TrySetResult(value) |> ignore)
                        hooks
                        (logger :> ILogger)
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend
                                let! accounting = observed.Task.WaitAsync(timeout.Token)
                                let clientStream = client.GetStream()
                                let backendStream = backend.GetStream()
                                let received = readExactly backendStream 2 timeout.Token

                                do! backendReceiveStarted.Task.WaitAsync(timeout.Token)
                                do! clientStream.WriteAsync([| 1uy |], timeout.Token)
                                do! secondReceiveReady.Task.WaitAsync(timeout.Token)

                                backend.Client.Shutdown(SocketShutdown.Send)
                                do! backendEndOfStream.Task.WaitAsync(timeout.Token)
                                do! expansionCommitted.Task.WaitAsync(timeout.Token)

                                let! waitingShare =
                                    waitForPhysicalShareWaiter
                                        accounting
                                        (fun share -> share.CurrentWaiterCount > 0L)
                                        timeout.Token

                                waitingShare.CurrentWaiterCount |> should be (greaterThan 0L)
                                waitingShare.QueuedWaitCount |> should be (greaterThan 0L)
                                accounting.GetSnapshot().BackendToClientHandoffState
                                |> should equal PhysicalShareHandoffState.Committed

                                releaseExpansion.TrySetResult(()) |> ignore

                                let! resumedShare =
                                    waitForPhysicalShareWaiter
                                        accounting
                                        (fun share ->
                                            share.CurrentWaiterCount = 0L
                                            && share.QueuedWaitCount > 0L)
                                        timeout.Token

                                resumedShare.CurrentWaiterCount |> should equal 0L
                                resumedShare.QueuedWaitCount |> should be (greaterThan 0L)
                                resumedShare.TotalWaitDuration |> should be (greaterThan TimeSpan.Zero)
                                resumedShare.MaximumWaitDuration |> should be (greaterThan TimeSpan.Zero)

                                do! clientStream.WriteAsync([| 2uy |], timeout.Token)
                                client.Client.Shutdown(SocketShutdown.Send)
                                let! actual = received
                                actual |> should equal [| 1uy; 2uy |]
                                do! expectEndOfStream backendStream timeout.Token
                                do! expectEndOfStream clientStream timeout.Token
                            })
            finally
                expansionCommitted.TrySetResult(()) |> ignore
                releaseExpansion.TrySetResult(()) |> ignore

            let snapshot = observed.Task.Result.GetSnapshot()
            let direction = snapshot.ClientToBackend
            let physicalShare = direction.PhysicalShare

            physicalShare.CurrentWaiterCount |> should equal 0L
            physicalShare.QueuedWaitCount |> should be (greaterThan 0L)
            physicalShare.TotalWaitDuration |> should be (greaterThan TimeSpan.Zero)
            physicalShare.MaximumWaitDuration |> should be (greaterThan TimeSpan.Zero)
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            direction.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
            direction.TerminalOutcome |> should equal "completed"
            snapshot.BackendToClient.TerminalOutcome |> should equal "completed"
            snapshot.CleanupFailurePresent |> should equal false

            let records = runtimeDiagnosticRecords logger
            let connection =
                records
                |> Array.find (fun record -> record.Message.StartsWith("Connection "))

            int64Property "ConnectionPhysicalCurrentWaiterCount" connection |> should equal 0L
            int64Property "ConnectionPhysicalQueuedWaitCount" connection |> should equal 0L

            let directionRecord =
                records
                |> Array.find (fun record ->
                    record.Message.StartsWith("Direction ")
                    && not (record.Message.Contains(" stage "))
                    && stringProperty "Direction" record = "client-to-backend")

            int64Property "PhysicalShareCurrentWaiterCount" directionRecord
            |> should equal physicalShare.CurrentWaiterCount
            int64Property "PhysicalSharePeakWaiterCount" directionRecord
            |> should equal physicalShare.PeakWaiterCount
            int64Property "PhysicalShareQueuedWaitCount" directionRecord
            |> should equal physicalShare.QueuedWaitCount
            doubleProperty "PhysicalShareTotalWaitMilliseconds" directionRecord
            |> should equal physicalShare.TotalWaitDuration.TotalMilliseconds
            doubleProperty "PhysicalShareMaximumWaitMilliseconds" directionRecord
            |> should equal physicalShare.MaximumWaitDuration.TotalMilliseconds
        }

    [<Fact>]
    let ``concurrent terminal direction summaries carry their connection ids`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()

            do!
                runWithForwarder
                    (fun configuration cancellationToken ->
                        TcpForwarder.runAsync configuration (logger :> ILogger) cancellationToken)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptFirst = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            let acceptSecond = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! firstClient = connect proxy.ListenPort timeout.Token
                            use! secondClient = connect proxy.ListenPort timeout.Token
                            use! firstBackend = acceptFirst
                            use! secondBackend = acceptSecond

                            firstClient.Client.Shutdown(SocketShutdown.Send)
                            firstBackend.Client.Shutdown(SocketShutdown.Send)
                            secondClient.Client.Shutdown(SocketShutdown.Send)
                            secondBackend.Client.Shutdown(SocketShutdown.Send)

                            while runtimeDiagnosticRecords logger |> Array.length < 6 do
                                do! Task.Delay(10, timeout.Token)
                        })

            let directions =
                runtimeDiagnosticRecords logger
                |> Array.filter (fun record ->
                    record.Message.StartsWith("Direction ")
                    && not (record.Message.Contains(" stage ")))

            directions.Length |> should equal 4

            let connectionIds =
                directions
                |> Array.choose (property "ConnectionId" >> Option.map unbox<int64>)
                |> Array.distinct
                |> Array.sort

            connectionIds |> should equal [| 1L; 2L |]

            for connectionId in connectionIds do
                directions
                |> Array.filter (fun record ->
                    match property "ConnectionId" record with
                    | Some value -> unbox<int64> value = connectionId
                    | _ -> false)
                |> Array.length
                |> should equal 2
        }

    [<Fact>]
    let ``disabled runtime diagnostics omit terminal summaries`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()

            do!
                runWithForwarderAndArguments
                    (fun configuration cancellationToken ->
                        TcpForwarder.runAsync configuration (logger :> ILogger) cancellationToken)
                    (endpointPort backendListener)
                    [| "--disable-runtime-diagnostics" |]
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)

                            while
                                logger.Messages
                                |> Array.exists (fun message -> message.Contains("closed with reason"))
                                |> not do
                                do! Task.Delay(10, timeout.Token)
                        })

            runtimeDiagnosticRecords logger |> should be Empty
        }

    [<Fact>]
    let ``disabled production diagnostics preserve bounded processor forwarding and cleanup`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let capacityArguments =
                [| "--client-to-server-logical-capacity-bytes"
                   "8"
                   "--server-to-client-logical-capacity-bytes"
                   "8"
                   "--connection-physical-capacity-bytes"
                   "128"
                   "--disable-runtime-diagnostics" |]

            do!
                runWithForwarderAndArguments
                    (fun configuration cancellationToken ->
                        TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                            configuration
                            passThroughPipelines
                            (fun value -> observed.TrySetResult(value) |> ignore)
                            NullLogger.Instance
                            cancellationToken)
                    (endpointPort backendListener)
                    capacityArguments
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let payload = [| 11uy; 12uy; 13uy |]
                            let receive = readExactly backendStream payload.Length timeout.Token
                            do! clientStream.WriteAsync(payload, timeout.Token)
                            let! received = receive
                            received |> should equal payload

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                            do! expectEndOfStream backendStream timeout.Token
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.PeakPhysicalBufferCapacityBytes |> should be (greaterThan 0L)
            snapshot.PeakPhysicalBufferCapacityBytes |> should be (lessThanOrEqualTo 128L)
            snapshot.ConnectionPhysicalCapacity.LimitBytes |> should equal 128L
            snapshot.ClientToBackend.LogicalCapacityLimitBytes |> should equal 8L
            snapshot.BackendToClient.LogicalCapacityLimitBytes |> should equal 8L

            for direction in [| snapshot.ClientToBackend; snapshot.BackendToClient |] do
                direction.TerminalOutcome |> should equal "disabled"
                direction.CurrentLogicalBufferedBytes |> should equal 0L
                direction.LogicalCapacity.QueuedWaitCount |> should equal 0L
                direction.ReceiveRentalAttempts |> should equal 0L
                direction.Stages |> should not' Empty
                direction.Stages
                |> List.iter (fun stage ->
                    stage.InputBytes |> should equal 0L
                    stage.InvocationCount |> should equal 0L
                    stage.CurrentRetainedBytes |> should equal 0L)
        }

    [<Fact>]
    let ``disabled production diagnostics preserve essential failure logging and listener survival`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let primary = InvalidOperationException("primary processor failure with diagnostics disabled")
            let mutable connectionCount = 0

            let createPipelines (state: ConnectionState) =
                if Interlocked.Increment(&connectionCount) = 1 then
                    { ClientToBackend =
                          [| processor (fun input _ _ ->
                                 if not input.Bytes.IsEmpty then
                                     raise primary

                                 completed 0L) |]
                      BackendToClient = [| passThroughProcessor () |] }
                else
                    passThroughPipelines state

            let capacityArguments =
                [| "--client-to-server-logical-capacity-bytes"
                   "64"
                   "--server-to-client-logical-capacity-bytes"
                   "64"
                   "--connection-physical-capacity-bytes"
                   "128"
                   "--disable-runtime-diagnostics" |]

            do!
                runWithConfiguredObservedProcessorProxyAndLogger
                    capacityArguments
                    createPipelines
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let firstBackendAccept = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! firstClient = connect proxy.ListenPort timeout.Token
                            use! firstBackend = firstBackendAccept
                            let firstClientStream = firstClient.GetStream()
                            let firstBackendStream = firstBackend.GetStream()

                            do! firstClientStream.WriteAsync([| 255uy |], timeout.Token)
                            let! observedFailure = logger.FirstFailure.WaitAsync(timeout.Token)
                            Object.ReferenceEquals(observedFailure, primary) |> should equal true
                            do! expectDisconnect firstClientStream timeout.Token
                            do! expectDisconnect firstBackendStream timeout.Token

                            let secondBackendAccept = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! secondClient = connect proxy.ListenPort timeout.Token
                            use! secondBackend = secondBackendAccept
                            let secondClientStream = secondClient.GetStream()
                            let secondBackendStream = secondBackend.GetStream()
                            let payload = [| 21uy; 22uy; 23uy |]
                            let receive = readExactly secondBackendStream payload.Length timeout.Token

                            do! secondClientStream.WriteAsync(payload, timeout.Token)
                            let! received = receive
                            received |> should equal payload

                            secondClient.Client.Shutdown(SocketShutdown.Send)
                            secondBackend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream secondClientStream timeout.Token
                            do! expectEndOfStream secondBackendStream timeout.Token
                        })

            runtimeDiagnosticRecords logger |> should be Empty
            logger.Failures |> Array.length |> should equal 1
            Object.ReferenceEquals(logger.Failures[0], primary) |> should equal true

            let failureRecord =
                logger.StructuredRecords
                |> Array.find (fun record -> record.Message.Contains("forwarding failed"))

            property "ConnectionId" failureRecord |> should equal (Some(box 1L))
            property "Direction" failureRecord
            |> should equal (Some(box "client-to-backend"))

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.ClientToBackend.TerminalOutcome |> should equal "disabled"
            snapshot.BackendToClient.TerminalOutcome |> should equal "disabled"
        }

    [<Fact>]
    let ``optional terminal diagnostics cannot change a successful connection result`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = ThrowingOptionalDiagnosticsLogger()
            let payload = [| 1uy; 2uy; 3uy |]

            do!
                runWithProcessorProxyAndLoggerForDiagnostics
                    passThroughPipelines
                    (logger :> ILogger)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            do! clientStream.WriteAsync(payload, timeout.Token)
                            let! received = readExactly backendStream payload.Length timeout.Token
                            received |> should equal payload

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                            do! expectEndOfStream backendStream timeout.Token
                        })

            logger.OptionalFailureCount |> should be (greaterThan 0)
            logger.Failures |> should be Empty
        }

    [<Fact>]
    let ``optional terminal diagnostics cannot replace the primary processor failure`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = ThrowingOptionalDiagnosticsLogger()
            let expected = InvalidOperationException("processor failed")

            let failingPipelines (_: ConnectionState) =
                { ClientToBackend =
                      [| processor (fun _ _ _ -> raise expected) |]
                  BackendToClient = [||] }

            do!
                runWithProcessorProxyAndLoggerForDiagnostics
                    failingPipelines
                    (logger :> ILogger)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            do! client.GetStream().WriteAsync([| 7uy |], timeout.Token)
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                        })

            logger.OptionalFailureCount |> should be (greaterThan 0)
            logger.Failures.Length |> should equal 1
            Object.ReferenceEquals(logger.Failures[0], expected) |> should equal true
        }

    [<Fact>]
    let ``terminal diagnostics retain cleanup failure presence`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()

            let createPipelines (state: ConnectionState) =
                state.GetOrAdd(
                    "diagnostics-cleanup",
                    fun () ->
                        new SignalingAsyncDisposable(
                            disposed,
                            Some(InvalidOperationException("diagnostics cleanup failed"))
                        )
                )
                |> ignore

                passThroughPipelines state

            do!
                runWithConfiguredObservedProcessorProxyAndLogger
                    Array.empty
                    createPipelines
                    ignore
                    logger
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! disposed.Task.WaitAsync(timeout.Token)

                            while runtimeDiagnosticRecords logger |> Array.length < 5 do
                                do! Task.Delay(10, timeout.Token)
                        })

            let records = runtimeDiagnosticRecords logger
            records.Length |> should equal 5

            let connection =
                records
                |> Array.find (fun record -> record.Message.StartsWith("Connection "))

            property "CleanupFailurePresent" connection |> should equal (Some(box true))

            records
            |> Array.filter (fun record ->
                record.Message.StartsWith("Direction ")
                && not (record.Message.Contains(" stage ")))
            |> Array.iter (fun record ->
                property "CleanupFailurePresent" record |> should equal (Some(box true))
                property "ConnectionId" record |> should equal (Some(box 1L)))

            records
            |> Array.filter (fun record ->
                record.Message.StartsWith("Direction ")
                && record.Message.Contains(" stage "))
            |> Array.iter (fun record ->
                property "ConnectionId" record |> should equal (Some(box 1L))
                property "StageIndex" record |> Option.isSome |> should equal true)
        }

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

    let private runWithObservedOptionalProcessorProxy
        processorPipelineFactory
        observer
        backendPort
        testCancellationToken
        body
        =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAndMemoryObserverAsync
                    configuration
                    processorPipelineFactory
                    observer
                    NullLogger.Instance
                    cancellationToken)
            backendPort
            testCancellationToken
            body

    let private runWithBoundedObservedProcessorProxy
        processorPipelineFactory
        capacityLimits
        observer
        backendPort
        testCancellationToken
        body
        =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAndCapacityLimitsAndMemoryObserverAsync
                    configuration
                    processorPipelineFactory
                    capacityLimits
                    observer
                    NullLogger.Instance
                    cancellationToken)
            backendPort
            testCancellationToken
            body

    let private runWithBoundedObservedProcessorProxyAndLogger
        processorPipelineFactory
        capacityLimits
        observer
        (logger: ILogger)
        testHooks
        backendPort
        testCancellationToken
        body
        =
        runWithForwarder
            (fun configuration cancellationToken ->
                TcpForwarder.runWithOptionalProcessorPipelinesAndCapacityLimitsAndMemoryObserverAndTestHooksAsync
                    configuration
                    processorPipelineFactory
                    capacityLimits
                    observer
                    testHooks
                    logger
                    cancellationToken)
            backendPort
            testCancellationToken
            body

    let private waitForPhysicalCapacity
        (accounting: ConnectionRuntimeMetrics)
        (predicate: int64 -> bool)
        (cancellationToken: CancellationToken)
        =
        task {
            let mutable sample = accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes
            let mutable satisfied = predicate sample

            while not satisfied do
                do! Task.Delay(10, cancellationToken)
                sample <- accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes
                satisfied <- predicate sample

            return sample
        }

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
    let ``production raw forwarding exposes active capacity and cleans it on cancellation`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let mutable accounting = None

            do!
                runWithObservedProxy
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            let! value = observed.Task.WaitAsync(timeout.Token)
                            accounting <- Some value

                            let! active =
                                waitForPhysicalCapacity
                                    value
                                    (fun capacity -> capacity > 0L)
                                    timeout.Token

                            active |> should be (greaterThan 0L)

                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)
                        })

            match accounting with
            | Some value ->
                value.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            | None -> failwith "The production connection did not expose its accounting instance."
        }

    [<Fact>]
    let ``configured finite physical capacity is enforced by live raw forwarding`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            do!
                runWithConfiguredObservedProxy
                    [| "--connection-physical-capacity-bytes"; "65" |]
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let! accounting = observed.Task.WaitAsync(timeout.Token)
                            let payload = createPayload 2048 197
                            let receive =
                                readExactly (backend.GetStream()) payload.Length timeout.Token

                            do! client.GetStream().WriteAsync(payload, timeout.Token)
                            let! received = receive
                            received |> should equal payload

                            let! active =
                                waitForPhysicalCapacity
                                    accounting
                                    (fun capacity -> capacity > 0L)
                                    timeout.Token

                            active |> should be (lessThanOrEqualTo 65L)
                            accounting.GetSnapshot().PeakPhysicalBufferCapacityBytes
                            |> should be (lessThanOrEqualTo 65L)
                        })
        }

    [<Fact>]
    let ``production configured asymmetric logical capacities govern generated output`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let expandingProcessor () =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        let marker = input.Bytes.FirstSpan[0]

                        let outputLength =
                            match marker with
                            | 1uy
                            | 3uy -> 4
                            | 2uy -> 16
                            | _ -> 64

                        emit output (Array.create outputLength marker)
                        completed 0L)

            let createPipelines (_: ConnectionState) =
                { ClientToBackend = [| expandingProcessor () |]
                  BackendToClient = [| expandingProcessor () |] }

            let capacityArguments =
                [| "--client-to-server-logical-capacity-bytes"
                   "8"
                   "--server-to-client-logical-capacity-bytes"
                   "32"
                   "--connection-physical-capacity-bytes"
                   "131072" |]

            let assertAccounting (snapshot: ConnectionMetricsSnapshot) =
                snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
                snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
                snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
                snapshot.PeakPhysicalBufferCapacityBytes
                |> should be (lessThanOrEqualTo 131072L)

            let runClientToBackendFailure () =
                task {
                    let observed =
                        TaskCompletionSource<ConnectionRuntimeMetrics>(
                            TaskCreationOptions.RunContinuationsAsynchronously
                        )

                    let logger = RecordingLogger()

                    do!
                        runWithConfiguredObservedProcessorProxyAndLogger
                            capacityArguments
                            createPipelines
                            (fun value -> observed.TrySetResult(value) |> ignore)
                            (logger :> ILogger)
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
                                    do! clientStream.WriteAsync([| 2uy |], timeout.Token)
                                    do! expectDisconnect clientStream timeout.Token
                                    do! expectDisconnect backendStream timeout.Token
                                    let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                                    failure.Message
                                    |> should haveSubstring "logical generated output occurrence"
                                })

                    let snapshot = observed.Task.Result.GetSnapshot()
                    assertAccounting snapshot
                    snapshot.ClientToBackend.FailedInvocationCount
                    |> should be (greaterThanOrEqualTo 1L)
                    snapshot.ClientToBackend.PeakLogicalBufferedBytes
                    |> should be (lessThanOrEqualTo 8L)
                }

            let runBackendToClientFailure () =
                task {
                    let observed =
                        TaskCompletionSource<ConnectionRuntimeMetrics>(
                            TaskCreationOptions.RunContinuationsAsynchronously
                        )

                    let logger = RecordingLogger()

                    do!
                        runWithConfiguredObservedProcessorProxyAndLogger
                            capacityArguments
                            createPipelines
                            (fun value -> observed.TrySetResult(value) |> ignore)
                            (logger :> ILogger)
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
                                    do! backendStream.WriteAsync([| 4uy |], timeout.Token)
                                    do! expectDisconnect backendStream timeout.Token
                                    do! expectDisconnect clientStream timeout.Token
                                    let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                                    failure.Message
                                    |> should haveSubstring "logical generated output occurrence"
                                })

                    let snapshot = observed.Task.Result.GetSnapshot()
                    assertAccounting snapshot
                    snapshot.BackendToClient.FailedInvocationCount
                    |> should be (greaterThanOrEqualTo 1L)
                    snapshot.BackendToClient.PeakLogicalBufferedBytes
                    |> should be (lessThanOrEqualTo 32L)
                }

            do! runClientToBackendFailure ()
            do! runBackendToClientFailure ()

            let healthyObserved =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            do!
                runWithConfiguredObservedProcessorProxy
                    capacityArguments
                    createPipelines
                    (fun value -> healthyObserved.TrySetResult(value) |> ignore)
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
                            let clientPayload = [| 1uy |]
                            let backendPayload = [| 2uy |]
                            let expectedAtBackend = Array.create 4 1uy
                            let expectedAtClient = Array.create 16 2uy
                            let receiveAtBackend =
                                readExactly backendStream expectedAtBackend.Length timeout.Token

                            let receiveAtClient =
                                readExactly clientStream expectedAtClient.Length timeout.Token

                            do!
                                Task.WhenAll(
                                    clientStream.WriteAsync(clientPayload, timeout.Token).AsTask(),
                                    backendStream.WriteAsync(backendPayload, timeout.Token).AsTask()
                                )

                            let! receivedAtBackend = receiveAtBackend
                            let! receivedAtClient = receiveAtClient
                            receivedAtBackend |> should equal expectedAtBackend
                            receivedAtClient |> should equal expectedAtClient

                            client.Client.Shutdown(SocketShutdown.Send)
                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token
                            do! expectEndOfStream backendStream timeout.Token
                        })

            let healthy = healthyObserved.Task.Result.GetSnapshot()
            assertAccounting healthy
            healthy.ClientToBackend.SuccessfulInvocationCount
            |> should be (greaterThanOrEqualTo 1L)
            healthy.BackendToClient.SuccessfulInvocationCount
            |> should be (greaterThanOrEqualTo 1L)
            healthy.ClientToBackend.GeneratedOutputBytes |> should equal 4L
            healthy.BackendToClient.GeneratedOutputBytes |> should equal 16L
            healthy.ClientToBackend.PeakLogicalBufferedBytes
            |> should be (lessThanOrEqualTo 8L)
            healthy.BackendToClient.PeakLogicalBufferedBytes
            |> should be (lessThanOrEqualTo 32L)
        }

    [<Fact>]
    let ``configured capacity accounting is isolated per accepted connection`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed = ConcurrentQueue<ConnectionRuntimeMetrics>()

            do!
                runWithConfiguredObservedProxy
                    [| "--connection-physical-capacity-bytes"; "65" |]
                    (fun value -> observed.Enqueue value)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackends =
                                Task.WhenAll(
                                    [| backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                                       backendListener.AcceptTcpClientAsync(timeout.Token).AsTask() |]
                                )

                            use! firstClient = connect proxy.ListenPort timeout.Token
                            use! secondClient = connect proxy.ListenPort timeout.Token
                            let! backends = acceptBackends
                            use firstBackend = backends[0]
                            use secondBackend = backends[1]

                            let firstPayload = createPayload 2048 211
                            let secondPayload = createPayload 2048 223
                            let firstReceive =
                                readExactly
                                    (firstBackend.GetStream())
                                    firstPayload.Length
                                    timeout.Token

                            let secondReceive =
                                readExactly
                                    (secondBackend.GetStream())
                                    secondPayload.Length
                                    timeout.Token

                            do!
                                Task.WhenAll(
                                    firstClient.GetStream().WriteAsync(firstPayload, timeout.Token).AsTask(),
                                    secondClient.GetStream().WriteAsync(secondPayload, timeout.Token).AsTask()
                                )

                            let! received = Task.WhenAll(firstReceive, secondReceive)
                            received[0] |> should equal firstPayload
                            received[1] |> should equal secondPayload

                            while observed.Count < 2 do
                                do! Task.Delay(10, timeout.Token)

                            observed.Count |> should equal 2
                        })

            observed.ToArray()
            |> Array.iter (fun accounting ->
                accounting.GetSnapshot().PeakPhysicalBufferCapacityBytes
                |> should be (lessThanOrEqualTo 65L))
        }

    [<Fact>]
    let ``small valid configured physical capacity fails one connection without stopping the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            do!
                runWithForwarderAndArguments
                    (fun configuration cancellationToken ->
                        TcpForwarder.runAsync
                            configuration
                            NullLogger.Instance
                            cancellationToken)
                    (endpointPort backendListener)
                    [| "--connection-physical-capacity-bytes"; "2" |]
                    timeout.Token
                    (fun proxy ->
                        task {
                            let firstBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! firstClient = connect proxy.ListenPort timeout.Token
                            use! firstAcceptedBackend = firstBackend
                            do! expectDisconnect (firstClient.GetStream()) timeout.Token
                            do! expectDisconnect (firstAcceptedBackend.GetStream()) timeout.Token
                            proxy.Completion.IsCompleted |> should equal false

                            let secondBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! secondClient = connect proxy.ListenPort timeout.Token
                            use! secondAcceptedBackend = secondBackend
                            do! expectDisconnect (secondClient.GetStream()) timeout.Token
                            do! expectDisconnect (secondAcceptedBackend.GetStream()) timeout.Token
                            proxy.Completion.IsCompleted |> should equal false
                        })
        }

    [<Fact>]
    let ``throwing memory observer disposes the accepted client`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            let expected = InvalidOperationException("memory observer failed")
            let logger = RecordingLogger()

            do!
                runWithForwarder
                    (fun configuration cancellationToken ->
                        TcpForwarder.runWithMemoryObserverAsync
                            configuration
                            (fun _ -> raise expected)
                            (logger :> ILogger)
                            cancellationToken)
                    (reservePort ())
                    timeout.Token
                    (fun proxy ->
                        task {
                            use! client = connect proxy.ListenPort timeout.Token
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            let! observed = logger.FirstFailure.WaitAsync(timeout.Token)
                            Object.ReferenceEquals(expected, observed) |> should equal true
                        })
        }

    [<Fact>]
    let ``production processor forwarding exposes its input lease after the opposite direction ends`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let started =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (_: ConnectionState) =
                let gated =
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

                { ClientToBackend = [| gated |]
                  BackendToClient = [||] }

            try
                do!
                    runWithObservedOptionalProcessorProxy
                        createPipelines
                        (fun value -> observed.TrySetResult(value) |> ignore)
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend

                                let! accounting = observed.Task.WaitAsync(timeout.Token)
                                let marker = [| 211uy |]
                                let markerRead = readExactly (client.GetStream()) 1 timeout.Token

                                do!
                                    backend.GetStream().WriteAsync(
                                        marker,
                                        0,
                                        marker.Length,
                                        timeout.Token
                                    )

                                let! receivedMarker = markerRead
                                receivedMarker |> should equal marker

                                let payload = createPayload 4096 149
                                let receive =
                                    readExactly (backend.GetStream()) payload.Length timeout.Token

                                do!
                                    client.GetStream().WriteAsync(
                                        payload,
                                        0,
                                        payload.Length,
                                        timeout.Token
                                    )

                                do! started.Task.WaitAsync(timeout.Token)

                                let! bothDirections =
                                    waitForPhysicalCapacity
                                        accounting
                                        (fun capacity -> capacity > 0L)
                                        timeout.Token

                                bothDirections |> should be (greaterThan 0L)
                                backend.Client.Shutdown(SocketShutdown.Send)
                                do! expectEndOfStream (client.GetStream()) timeout.Token

                                let! processorOnly =
                                    waitForPhysicalCapacity
                                        accounting
                                        (fun capacity ->
                                            capacity > 0L && capacity < bothDirections)
                                        timeout.Token

                                processorOnly |> should be (greaterThan 0L)
                                bothDirections |> should be (greaterThan processorOnly)
                                release.TrySetResult(()) |> ignore
                                let! received = receive
                                received |> should equal payload
                            })
            finally
                release.TrySetResult(()) |> ignore

            let! accounting = observed.Task.WaitAsync(timeout.Token)
            let snapshot = accounting.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.Stages |> List.length |> should equal 1
            snapshot.ClientToBackend.InputBytes |> should equal 4096L
            snapshot.ClientToBackend.InputBackedOutputBytes |> should equal 4096L
            snapshot.ClientToBackend.GeneratedOutputBytes |> should equal 0L
            snapshot.ClientToBackend.SuccessfulInvocationCount
            |> should be (greaterThanOrEqualTo 1L)
            snapshot.BackendToClient.Stages |> should be Empty
            snapshot.BackendToClient.InputBytes |> should equal 0L
        }

    [<Fact>]
    let ``mixed production directions share accounting across a half-close`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let started =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (_: ConnectionState) =
                let gated =
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

                { ClientToBackend = [| gated |]
                  BackendToClient = [||] }

            try
                do!
                    runWithObservedOptionalProcessorProxy
                        createPipelines
                        (fun value -> observed.TrySetResult(value) |> ignore)
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend =
                                    backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend

                                let! accounting = observed.Task.WaitAsync(timeout.Token)
                                // Drive the raw opposite direction once before taking
                                // the first capacity snapshot so its runtime-owned
                                // lease is definitely active.
                                let probe = [| 231uy |]
                                let probeRead =
                                    readExactly (client.GetStream()) probe.Length timeout.Token

                                do!
                                    backend.GetStream().WriteAsync(
                                        probe,
                                        0,
                                        probe.Length,
                                        timeout.Token
                                    )

                                let! receivedProbe = probeRead
                                receivedProbe |> should equal probe

                                let payload = createPayload 2048 157
                                let receive =
                                    readExactly (backend.GetStream()) payload.Length timeout.Token

                                do!
                                    client.GetStream().WriteAsync(
                                        payload,
                                        0,
                                        payload.Length,
                                        timeout.Token
                                    )

                                do! started.Task.WaitAsync(timeout.Token)
                                let! bothDirections =
                                    waitForPhysicalCapacity
                                        accounting
                                        (fun capacity -> capacity > 0L)
                                        timeout.Token

                                bothDirections |> should be (greaterThan 0L)

                                let response = createPayload 1024 163
                                let responseRead =
                                    readExactly (client.GetStream()) response.Length timeout.Token

                                do!
                                    backend.GetStream().WriteAsync(
                                        response,
                                        0,
                                        response.Length,
                                        timeout.Token
                                    )

                                let! receivedResponse = responseRead
                                receivedResponse |> should equal response
                                client.Client.Shutdown(SocketShutdown.Send)
                                release.TrySetResult(()) |> ignore
                                let! received = receive
                                received |> should equal payload
                                do! expectEndOfStream (backend.GetStream()) timeout.Token

                                let! remaining =
                                    waitForPhysicalCapacity
                                        accounting
                                        (fun capacity ->
                                            capacity > 0L && capacity < bothDirections)
                                        timeout.Token

                                // The processor direction has reached EOF, so its
                                // contribution must be gone while the raw opposite
                                // direction remains active.
                                remaining |> should be (greaterThan 0L)
                                bothDirections |> should be (greaterThan remaining)
                            })
            finally
                release.TrySetResult(()) |> ignore

            let! accounting = observed.Task.WaitAsync(timeout.Token)
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

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
    let ``bounded processor forwarding isolates directional logical budgets and drains to zero`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            do!
                runWithBoundedObservedProcessorProxy
                    passThroughPipelines
                    (ConnectionCapacityLimits(65_536L, 32_768L, 131_072L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            let fromClient = createPayload 2048 149
                            let fromBackend = createPayload 1024 151
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            let receiveAtBackend =
                                readExactly backendStream fromClient.Length timeout.Token

                            let receiveAtClient =
                                readExactly clientStream fromBackend.Length timeout.Token

                            do!
                                Task.WhenAll(
                                    clientStream.WriteAsync(fromClient, timeout.Token).AsTask(),
                                    backendStream.WriteAsync(fromBackend, timeout.Token).AsTask()
                                )

                            let! receivedAtBackend = receiveAtBackend
                            let! receivedAtClient = receiveAtClient
                            receivedAtBackend |> should equal fromClient
                            receivedAtClient |> should equal fromBackend
                        })

            let! accounting = observed.Task.WaitAsync(timeout.Token)
            let snapshot = accounting.GetSnapshot()
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.ClientToBackend.PeakLogicalBufferedBytes |> should be (greaterThan 0L)
            snapshot.BackendToClient.PeakLogicalBufferedBytes |> should be (greaterThan 0L)
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``bounded physical shares keep an idle direction from blocking active traffic`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            do!
                runWithBoundedObservedProcessorProxy
                    passThroughPipelines
                    (ConnectionCapacityLimits(4096L, 4096L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            let payload = createPayload 2048 173
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let receive = readExactly backendStream payload.Length timeout.Token

                            do! clientStream.WriteAsync(payload, timeout.Token).AsTask()
                            let! received = receive
                            received |> should equal payload
                        })

            let snapshot = (observed.Task |> _.Result).GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.PeakPhysicalBufferCapacityBytes |> should be (lessThanOrEqualTo 64L)
        }

    [<Fact>]
    let ``raw admission failures remain connection scoped outcomes`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()

            let noProcessors (_: ConnectionState) =
                { ClientToBackend = [||]
                  BackendToClient = [||] }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    noProcessors
                    (ConnectionCapacityLimits(1L, 1L, 1L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    { BeforeReceive = ignore
                      AfterReceive = fun _ _ -> ()
                      PendingSend = ignore
                      CapacityAccounting = None }
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                            let! _ = logger.FirstFailure.WaitAsync(timeout.Token)
                            proxy.Completion.IsCompleted |> should equal false
                        })

            logger.Messages
            |> Array.exists (fun message -> message.Contains("task faulted outside its lifecycle handler"))
            |> should equal false

            logger.Messages
            |> Array.exists (fun message -> message.Contains("failed unexpectedly"))
            |> should equal false

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``bounded logical headroom limits receive chunks without changing bytes`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let mutable maximumInput = 0L

            let createPipelines (_: ConnectionState) =
                let recording =
                    processor (fun input output _ ->
                        let inputLength = input.Bytes.Length
                        let rec recordMaximum () =
                            let current = Volatile.Read(&maximumInput)

                            if inputLength <= current
                               || Interlocked.CompareExchange(&maximumInput, inputLength, current) = current then
                                ()
                            else
                                recordMaximum ()

                        recordMaximum ()
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

                { ClientToBackend = [| recording |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithBoundedObservedProcessorProxy
                    createPipelines
                    (ConnectionCapacityLimits(64L, 64L, 64L))
                    ignore
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let payload = createPayload 1024 179
                            let receive = readExactly (backend.GetStream()) payload.Length timeout.Token

                            do! client.GetStream().WriteAsync(payload, timeout.Token).AsTask()
                            let! received = receive
                            received |> should equal payload
                        })

            maximumInput |> should be (greaterThan 0L)
            maximumInput |> should be (lessThanOrEqualTo 32L)
        }

    [<Fact>]
    let ``retained input fails deterministically instead of waiting for a future half close`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let firstRetained = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()

            let createPipelines (_: ConnectionState) =
                let retaining =
                    processor (fun input output _ ->
                        if input.IsCompleted then
                            completed 0L
                        elif input.Bytes.Length < 64 then
                            firstRetained.TrySetResult(()) |> ignore
                            completed input.Bytes.Length
                        else
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L)

                { ClientToBackend = [| retaining |]
                  BackendToClient = [||] }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    createPipelines
                    (ConnectionCapacityLimits(128L, 128L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    { BeforeReceive = ignore
                      AfterReceive = fun _ _ -> ()
                      PendingSend = ignore
                      CapacityAccounting = None }
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
                            let first = createPayload 32 181

                            do! clientStream.WriteAsync(first, timeout.Token).AsTask()
                            do! firstRetained.Task.WaitAsync(timeout.Token)
                            client.Client.Shutdown(SocketShutdown.Send)
                            let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                            failure.Message |> should haveSubstring "quiescent receive boundary"
                            do! expectDisconnect clientStream timeout.Token
                            do! expectDisconnect backendStream timeout.Token
                        })

            let! accounting = observed.Task.WaitAsync(timeout.Token)
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``both directions retaining their physical shares fail together without stranded reservations`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let allRetained = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let boundaryReady = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let boundaryRelease = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()
            let mutable retainedInvocations = 0
            let mutable clientToBackendReceives = 0
            let mutable backendToClientReceives = 0

            let retainingProcessor () =
                processor (fun input _ _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        if Interlocked.Increment(&retainedInvocations) = 2 then
                            allRetained.TrySetResult(()) |> ignore

                        ValueTask<ProcessorResult>(
                            task {
                                do! allRetained.Task
                                return { RetainedLength = input.Bytes.Length }
                            }
                        ))

            let createPipelines (_: ConnectionState) =
                { ClientToBackend = [| retainingProcessor () |]
                  BackendToClient = [| retainingProcessor () |] }

            let hooks =
                { BeforeReceive =
                      (fun direction ->
                          let count =
                              match direction with
                              | ClientToBackend -> Interlocked.Increment(&clientToBackendReceives)
                              | BackendToClient -> Interlocked.Increment(&backendToClientReceives)

                          if clientToBackendReceives >= 2 && backendToClientReceives >= 2 then
                              boundaryReady.TrySetResult(()) |> ignore

                          if count >= 2 then
                              boundaryRelease.Task.GetAwaiter().GetResult())
                  AfterReceive = fun _ _ -> ()
                  PendingSend = ignore
                  CapacityAccounting = None }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    createPipelines
                    (ConnectionCapacityLimits(128L, 128L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    hooks
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let clientPayload = createPayload 32 193
                            let backendPayload = createPayload 32 197

                            do!
                                Task.WhenAll(
                                    client.GetStream().WriteAsync(clientPayload, timeout.Token).AsTask(),
                                    backend.GetStream().WriteAsync(backendPayload, timeout.Token).AsTask()
                                )

                            do! allRetained.Task.WaitAsync(timeout.Token)
                            do! boundaryReady.Task.WaitAsync(timeout.Token)

                            let occupied = observed.Task.Result.GetSnapshot()
                            occupied.CurrentPhysicalBufferCapacityBytes |> should equal 64L
                            occupied.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 32L
                            occupied.BackendToClient.CurrentLogicalBufferedBytes |> should equal 32L

                            boundaryRelease.TrySetResult(()) |> ignore
                            let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                            failure.Message |> should haveSubstring "quiescent receive boundary"
                            proxy.Completion.IsCompleted |> should equal false
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token
                        })

            let! accounting = observed.Task.WaitAsync(timeout.Token)
            let snapshot = accounting.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``retained progress failure cleans both directions`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )
            let retained = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let createPipelines (_: ConnectionState) =
                let processor =
                    processor (fun input _ _ ->
                        if input.IsCompleted then
                            completed 0L
                        else
                            retained.TrySetResult(()) |> ignore
                            completed input.Bytes.Length)

                { ClientToBackend = [| processor |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithBoundedObservedProcessorProxy
                    createPipelines
                    (ConnectionCapacityLimits(128L, 128L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! _backend = acceptBackend
                            do! client.GetStream().WriteAsync(createPayload 32 191, timeout.Token).AsTask()
                            do! retained.Task.WaitAsync(timeout.Token)
                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)
                        })

            (observed.Task |> _.Result).GetSnapshot().CurrentPhysicalBufferCapacityBytes
            |> should equal 0L
        }

    [<Fact>]
    let ``bounded logical capacity failure cancels its sibling without stopping the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let mutable connectionNumber = 0

            let createPipelines (_: ConnectionState) =
                if Interlocked.Increment(&connectionNumber) = 1 then
                    passThroughPipelines Unchecked.defaultof<ConnectionState>
                else
                    { ClientToBackend = [||]
                      BackendToClient = [||] }

            do!
                runWithBoundedObservedProcessorProxy
                    createPipelines
                    (ConnectionCapacityLimits(0L, 65_536L, 131_072L))
                    ignore
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend
                            do! failedClient.GetStream().WriteAsync([| 1uy |], timeout.Token)
                            do! expectDisconnect (failedClient.GetStream()) timeout.Token
                            do! expectDisconnect (failedBackend.GetStream()) timeout.Token

                            let acceptHealthyBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! healthyClient = connect proxy.ListenPort timeout.Token
                            use! healthyBackend = acceptHealthyBackend
                            let payload = createPayload 32 157
                            let receive =
                                readExactly (healthyBackend.GetStream()) payload.Length timeout.Token

                            do! healthyClient.GetStream().WriteAsync(payload, timeout.Token)
                            let! received = receive
                            received |> should equal payload
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
    let ``bounded slow processor suspends one receive while the opposite direction continues`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable clientToBackendReceives = 0

            let slow =
                processor (fun input output cancellationToken ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        started.TrySetResult(()) |> ignore

                        ValueTask<ProcessorResult>(
                            task {
                                do! release.Task.WaitAsync(cancellationToken)
                                output.AppendInput(0L, input.Bytes.Length)
                                return { RetainedLength = 0L }
                            }
                        ))

            let createPipelines (_: ConnectionState) =
                { ClientToBackend = [| slow |]
                  BackendToClient = [| passThroughProcessor () |] }

            let hooks =
                { BeforeReceive =
                      (fun direction ->
                          if direction = ClientToBackend then
                              Interlocked.Increment(&clientToBackendReceives) |> ignore)
                  AfterReceive = fun _ _ -> ()
                  PendingSend = ignore
                  CapacityAccounting = None }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    createPipelines
                    (ConnectionCapacityLimits(128L, 128L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    NullLogger.Instance
                    hooks
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let first = createPayload 32 211
                            let second = createPayload 32 223
                            let reverse = createPayload 32 227
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()

                            do! clientStream.WriteAsync(first, timeout.Token).AsTask()
                            do! started.Task.WaitAsync(timeout.Token)
                            do! clientStream.WriteAsync(second, timeout.Token).AsTask()

                            let metrics = observed.Task.Result
                            let suspended = metrics.GetSnapshot()
                            Volatile.Read(&clientToBackendReceives) |> should equal 1
                            suspended.ClientToBackend.CurrentLogicalBufferedBytes |> should be (greaterThan 0L)
                            suspended.CurrentPhysicalBufferCapacityBytes |> should be (greaterThan 0L)

                            let reverseReceive = readExactly clientStream reverse.Length timeout.Token
                            do! backendStream.WriteAsync(reverse, timeout.Token).AsTask()
                            let! reverseReceived = reverseReceive
                            reverseReceived |> should equal reverse

                            let expected = Array.append first second
                            let forwardReceive = readExactly backendStream expected.Length timeout.Token
                            release.TrySetResult(()) |> ignore
                            let! received = forwardReceive
                            received |> should equal expected
                            Volatile.Read(&clientToBackendReceives) |> should be (greaterThanOrEqualTo 2)
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``bounded slow processor cancellation releases its suspended receive`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)

            let slow =
                processor (fun _ _ cancellationToken ->
                    started.TrySetResult(()) |> ignore

                    ValueTask<ProcessorResult>(
                        task {
                            do! Task.Delay(Timeout.Infinite, cancellationToken)
                            return { RetainedLength = 0L }
                        }
                    ))

            let createPipelines (_: ConnectionState) =
                { ClientToBackend = [| slow |]
                  BackendToClient = [| passThroughProcessor () |] }

            let hooks =
                { BeforeReceive = ignore
                  AfterReceive = fun _ _ -> ()
                  PendingSend = ignore
                  CapacityAccounting = None }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    createPipelines
                    (ConnectionCapacityLimits(128L, 128L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    NullLogger.Instance
                    hooks
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! _backend = acceptBackend
                            do! client.GetStream().WriteAsync(createPayload 32 229, timeout.Token).AsTask()
                            do! started.Task.WaitAsync(timeout.Token)
                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``cancellation after observed EOF remains non flushing`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let eofObserved = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseEof = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable completionInvocations = 0

            let completionAware =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        Interlocked.Increment(&completionInvocations) |> ignore

                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            let createPipelines (_: ConnectionState) =
                { ClientToBackend = [| completionAware |]
                  BackendToClient = [| passThroughProcessor () |] }

            let hooks =
                { BeforeReceive = ignore
                  AfterReceive =
                      (fun direction received ->
                          if direction = ClientToBackend && received = 0 then
                              eofObserved.TrySetResult(()) |> ignore
                              releaseEof.Task.GetAwaiter().GetResult())
                  PendingSend = ignore
                  CapacityAccounting = None }

            try
                do!
                    runWithBoundedObservedProcessorProxyAndLogger
                        createPipelines
                        (ConnectionCapacityLimits(128L, 128L, 64L))
                        (fun value -> observed.TrySetResult(value) |> ignore)
                        NullLogger.Instance
                        hooks
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                                use! client = connect proxy.ListenPort timeout.Token
                                use! _backend = acceptBackend

                                client.Client.Shutdown(SocketShutdown.Send)
                                do! eofObserved.Task.WaitAsync(timeout.Token)
                                proxy.Shutdown.Cancel()
                                releaseEof.TrySetResult(()) |> ignore
                                do! proxy.Completion.WaitAsync(timeout.Token)
                                Volatile.Read(&completionInvocations) |> should equal 0
                            })
            finally
                releaseEof.TrySetResult(()) |> ignore

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``cancellation during an awaited socket write releases terminal ownership`` () =
        task {
            use timeout = new CancellationTokenSource(15_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let pendingSend = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeReceive = ignore
                  AfterReceive = fun _ _ -> ()
                  PendingSend =
                      (fun direction ->
                          if direction = ClientToBackend then
                              pendingSend.TrySetResult(()) |> ignore)
                  CapacityAccounting = None }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    passThroughPipelines
                    (ConnectionCapacityLimits(262_144L, 262_144L, 131_072L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    NullLogger.Instance
                    hooks
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! _backend = acceptBackend
                            let payload = createPayload (4 * 1024 * 1024) 237
                            let write = client.GetStream().WriteAsync(payload, timeout.Token).AsTask()

                            do! pendingSend.Task.WaitAsync(timeout.Token)
                            proxy.Shutdown.Cancel()
                            do! proxy.Completion.WaitAsync(timeout.Token)

                            try
                                do! write.WaitAsync(timeout.Token)
                            with
                            | :? IOException
                            | :? SocketException
                            | :? OperationCanceledException
                            | :? ObjectDisposedException -> ()
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``socket send failure after opposite EOF terminates the remaining direction`` () =
        task {
            use timeout = new CancellationTokenSource(15_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let pendingSend = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()

            let hooks =
                { BeforeReceive = ignore
                  AfterReceive = fun _ _ -> ()
                  PendingSend =
                      (fun direction ->
                          if direction = ClientToBackend then
                              pendingSend.TrySetResult(()) |> ignore)
                  CapacityAccounting = None }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    passThroughPipelines
                    (ConnectionCapacityLimits(262_144L, 262_144L, 131_072L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    hooks
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let clientStream = client.GetStream()

                            backend.Client.Shutdown(SocketShutdown.Send)
                            do! expectEndOfStream clientStream timeout.Token

                            let payload = createPayload (4 * 1024 * 1024) 243
                            let write = clientStream.WriteAsync(payload, timeout.Token).AsTask()
                            do! pendingSend.Task.WaitAsync(timeout.Token)

                            backend.Client.LingerState <- LingerOption(true, 0)
                            backend.Dispose()

                            let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                            failure |> should be ofExactType<SocketException>
                            do! expectDisconnect clientStream timeout.Token

                            try
                                do! write.WaitAsync(timeout.Token)
                            with
                            | :? IOException
                            | :? SocketException
                            | :? OperationCanceledException
                            | :? ObjectDisposedException -> ()
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``bounded slow TCP receiver suspends upstream reads until downstream progress`` () =
        task {
            use timeout = new CancellationTokenSource(15_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let pendingSend = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed =
                TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable clientToBackendReceives = 0

            let createPipelines (_: ConnectionState) = passThroughPipelines Unchecked.defaultof<ConnectionState>

            let hooks =
                { BeforeReceive =
                      (fun direction ->
                          if direction = ClientToBackend then
                              Interlocked.Increment(&clientToBackendReceives) |> ignore)
                  AfterReceive = fun _ _ -> ()
                  PendingSend =
                      (fun direction ->
                          if direction = ClientToBackend then
                              pendingSend.TrySetResult(()) |> ignore)
                  CapacityAccounting = None }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    createPipelines
                    (ConnectionCapacityLimits(262_144L, 262_144L, 131_072L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    NullLogger.Instance
                    hooks
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend
                            let payload = createPayload (4 * 1024 * 1024) 233
                            let clientStream = client.GetStream()
                            let backendStream = backend.GetStream()
                            let write = clientStream.WriteAsync(payload, timeout.Token).AsTask()

                            do! pendingSend.Task.WaitAsync(timeout.Token)
                            let suspended = observed.Task.Result.GetSnapshot()
                            let receivesAtSuspension = Volatile.Read(&clientToBackendReceives)
                            suspended.ClientToBackend.CurrentLogicalBufferedBytes |> should be (greaterThan 0L)
                            suspended.CurrentPhysicalBufferCapacityBytes |> should be (lessThanOrEqualTo 131_072L)

                            do! Task.Delay(100, timeout.Token)
                            Volatile.Read(&clientToBackendReceives) |> should equal receivesAtSuspension

                            let! received = readExactly backendStream payload.Length timeout.Token
                            received |> should equal payload
                            do! write
                        })

            observed.Task.Result.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
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
    let ``half close hands off physical share before processor disposal completes`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let disposalCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let beforeBackendReceive = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseSecondBackendReceive = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let retainedFirst = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable backendReceiveCount = 0

            let createPipelines (_: ConnectionState) =
                let completedDirection =
                    GatedDisposalProcessor(
                        (fun input output _ ->
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L),
                        disposalStarted,
                        releaseDisposal,
                        disposalCompleted
                    )

                let survivingDirection =
                    processor (fun input output _ ->
                        if input.IsCompleted then
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L
                        elif input.Bytes.Length < 64L then
                            retainedFirst.TrySetResult(()) |> ignore
                            completed input.Bytes.Length
                        else
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L)

                { ClientToBackend = [| completedDirection :> IByteStreamProcessor |]
                  BackendToClient = [| survivingDirection |] }

            try
                do!
                    runWithBoundedObservedProcessorProxyAndLogger
                        createPipelines
                        (ConnectionCapacityLimits(128L, 128L, 64L))
                        (fun value -> observed.TrySetResult(value) |> ignore)
                        NullLogger.Instance
                        { BeforeReceive =
                            fun direction ->
                                if direction = BackendToClient then
                                    if Interlocked.Increment(&backendReceiveCount) = 1 then
                                        beforeBackendReceive.TrySetResult(()) |> ignore
                                    else
                                        releaseSecondBackendReceive.Task.GetAwaiter().GetResult()
                          AfterReceive = fun _ _ -> ()
                          PendingSend = ignore
                          CapacityAccounting = None }
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend
                                let clientStream = client.GetStream()
                                let backendStream = backend.GetStream()

                                do! beforeBackendReceive.Task.WaitAsync(timeout.Token)

                                let first = createPayload 32 239
                                let second = createPayload 32 241
                                let expected = Array.append first second
                                let receive = readExactly clientStream expected.Length timeout.Token

                                do! backendStream.WriteAsync(first, timeout.Token).AsTask()
                                do! retainedFirst.Task.WaitAsync(timeout.Token)

                                // The retained first chunk proves that the initial receive
                                // owner was admitted before the completed direction hands off.
                                client.Client.Shutdown(SocketShutdown.Send)
                                do! expectEndOfStream backendStream timeout.Token
                                do! disposalStarted.Task.WaitAsync(timeout.Token)
                                disposalCompleted.Task.IsCompleted |> should equal false

                                releaseSecondBackendReceive.TrySetResult(()) |> ignore
                                do! backendStream.WriteAsync(second, timeout.Token).AsTask()

                                let! received = receive
                                received |> should equal expected
                                disposalCompleted.Task.IsCompleted |> should equal false

                                backend.Client.Shutdown(SocketShutdown.Send)
                                do! expectEndOfStream clientStream timeout.Token
                                releaseDisposal.TrySetResult(()) |> ignore
                                do! disposalCompleted.Task.WaitAsync(timeout.Token)
                            })
            finally
                releaseDisposal.TrySetResult(()) |> ignore

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``processor failure cancels sibling before processor disposal completes`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let disposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let disposalCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()

            let createPipelines (_: ConnectionState) =
                let failing =
                    GatedDisposalProcessor(
                        (fun input _ _ ->
                            if input.Bytes.IsEmpty then
                                completed 0L
                            else
                                invalidOp "processor failed before cleanup"),
                        disposalStarted,
                        releaseDisposal,
                        disposalCompleted
                    )

                { ClientToBackend = [| failing :> IByteStreamProcessor |]
                  BackendToClient = [| passThroughProcessor () |] }

            try
                do!
                    runWithBoundedObservedProcessorProxyAndLogger
                        createPipelines
                        (ConnectionCapacityLimits(128L, 128L, 64L))
                        (fun value -> observed.TrySetResult(value) |> ignore)
                        (logger :> ILogger)
                        { BeforeReceive = ignore
                          AfterReceive = fun _ _ -> ()
                          PendingSend = ignore
                          CapacityAccounting = None }
                        (endpointPort backendListener)
                        timeout.Token
                        (fun proxy ->
                            task {
                                let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                                use! client = connect proxy.ListenPort timeout.Token
                                use! backend = acceptBackend

                                do! client.GetStream().WriteAsync([| 255uy |], timeout.Token).AsTask()
                                do! disposalStarted.Task.WaitAsync(timeout.Token)
                                disposalCompleted.Task.IsCompleted |> should equal false

                                do! expectDisconnect (client.GetStream()) timeout.Token
                                do! expectDisconnect (backend.GetStream()) timeout.Token
                                proxy.Completion.IsCompleted |> should equal false

                                releaseDisposal.TrySetResult(()) |> ignore
                                do! disposalCompleted.Task.WaitAsync(timeout.Token)
                                let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                                failure.Message |> should haveSubstring "processor failed before cleanup"
                            })
            finally
                releaseDisposal.TrySetResult(()) |> ignore

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``connection abort cleanup failure remains secondary to processor failure`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let logger = RecordingLogger()

            let createPipelines (_: ConnectionState) =
                let failing =
                    processor (fun input _ cancellationToken ->
                        if not input.Bytes.IsEmpty then
                            cancellationToken.Register(
                                Action(fun () -> invalidOp "cancellation callback failed")
                            )
                            |> ignore

                            invalidOp "processor primary failure"

                        completed 0L)

                { ClientToBackend = [| failing |]
                  BackendToClient = [| passThroughProcessor () |] }

            do!
                runWithProcessorProxyAndLogger
                    createPipelines
                    (logger :> ILogger)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            do! client.GetStream().WriteAsync([| 1uy |], timeout.Token).AsTask()
                            do! expectDisconnect (client.GetStream()) timeout.Token
                            do! expectDisconnect (backend.GetStream()) timeout.Token

                            let! failure = logger.FirstFailure.WaitAsync(timeout.Token)
                            failure |> should be ofExactType<AggregateException>
                            let aggregate = failure :?> AggregateException
                            aggregate.InnerExceptions.Count |> should equal 2
                            aggregate.InnerExceptions[0].Message |> should equal "processor primary failure"
                            aggregate.InnerExceptions[1] |> should be ofExactType<AggregateException>
                            let cleanup = aggregate.InnerExceptions[1] :?> AggregateException
                            cleanup.InnerExceptions[0].Message
                            |> should equal "cancellation callback failed"
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
    let ``registered processor failures retain primary ordering while cleanup is gated`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let clientActionReady = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let backendActionReady = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let clientDisposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseClientDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let clientDisposalCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let backendDisposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseBackendDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let backendDisposalCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let observed = TaskCompletionSource<ConnectionRuntimeMetrics>(TaskCreationOptions.RunContinuationsAsynchronously)
            let logger = RecordingLogger()

            let createPipelines (_: ConnectionState) =
                let clientFailure =
                    GatedDisposalProcessor(
                        (fun input _ _ ->
                            if input.Bytes.IsEmpty then
                                completed 0L
                            else
                                clientActionReady.TrySetResult(()) |> ignore

                                ValueTask<ProcessorResult>(
                                    task {
                                        do! backendActionReady.Task
                                        return raise (InvalidOperationException("client direction failed"))
                                    }
                                )),
                        clientDisposalStarted,
                        releaseClientDisposal,
                        clientDisposalCompleted
                    )

                let backendFailure =
                    GatedDisposalProcessor(
                        (fun input _ _ ->
                            if input.Bytes.IsEmpty then
                                completed 0L
                            else
                                backendActionReady.TrySetResult(()) |> ignore

                                ValueTask<ProcessorResult>(
                                    task {
                                        do! clientActionReady.Task
                                        do! clientDisposalStarted.Task
                                        return raise (InvalidOperationException("backend direction failed"))
                                    }
                                )),
                        backendDisposalStarted,
                        releaseBackendDisposal,
                        backendDisposalCompleted
                    )

                releaseBackendDisposal.TrySetResult(()) |> ignore

                { ClientToBackend = [| clientFailure :> IByteStreamProcessor |]
                  BackendToClient = [| backendFailure :> IByteStreamProcessor |] }

            do!
                runWithBoundedObservedProcessorProxyAndLogger
                    createPipelines
                    (ConnectionCapacityLimits(128L, 128L, 64L))
                    (fun value -> observed.TrySetResult(value) |> ignore)
                    (logger :> ILogger)
                    { BeforeReceive = ignore
                      AfterReceive = fun _ _ -> ()
                      PendingSend = ignore
                      CapacityAccounting = None }
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
                            use! client = connect proxy.ListenPort timeout.Token
                            use! backend = acceptBackend

                            do!
                                Task.WhenAll(
                                    client.GetStream().WriteAsync([| 1uy |], timeout.Token).AsTask(),
                                    backend.GetStream().WriteAsync([| 2uy |], timeout.Token).AsTask()
                                )

                            do! clientDisposalStarted.Task.WaitAsync(timeout.Token)
                            do! backendDisposalCompleted.Task.WaitAsync(timeout.Token)
                            proxy.Completion.IsCompleted |> should equal false

                            releaseClientDisposal.TrySetResult(()) |> ignore
                            do! clientDisposalCompleted.Task.WaitAsync(timeout.Token)
                            do! logger.TwoFailures.WaitAsync(timeout.Token)

                            logger.Failures
                            |> Array.map _.Message
                            |> should equal [| "client direction failed"; "backend direction failed" |]
                        })

            let snapshot = observed.Task.Result.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
            snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L
        }

    [<Fact>]
    let ``socket failure releases observed physical capacity and does not stop the listener`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()

            let failedAccounting =
                TaskCompletionSource<ConnectionRuntimeMetrics>(
                    TaskCreationOptions.RunContinuationsAsynchronously
                )

            let mutable observedConnections = 0
            let mutable failedConnectionAccounting = None

            do!
                runWithObservedOptionalProcessorProxy
                    passThroughPipelines
                    (fun accounting ->
                        if Interlocked.Increment(&observedConnections) = 1 then
                            failedAccounting.TrySetResult(accounting) |> ignore)
                    (endpointPort backendListener)
                    timeout.Token
                    (fun proxy ->
                        task {
                            let acceptFailedBackend =
                                backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()

                            use! failedClient = connect proxy.ListenPort timeout.Token
                            use! failedBackend = acceptFailedBackend

                            let! accounting = failedAccounting.Task.WaitAsync(timeout.Token)
                            failedConnectionAccounting <- Some accounting
                            let! active =
                                waitForPhysicalCapacity
                                    accounting
                                    (fun capacity -> capacity > 0L)
                                    timeout.Token

                            active |> should be (greaterThan 0L)

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

            match failedConnectionAccounting with
            | Some accounting ->
                accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            | None -> failwith "The failed connection did not expose its accounting instance."
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

            let directionOutcomes =
                runtimeDiagnosticRecords logger
                |> Array.filter (fun record ->
                    record.Message.StartsWith("Direction ")
                    && not (record.Message.Contains(" stage ")))
                |> Array.choose (property "TerminalOutcome" >> Option.map unbox<string>)
                |> Array.sort

            directionOutcomes |> should equal [| "not-started"; "not-started" |]
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
