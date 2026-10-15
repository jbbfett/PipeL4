namespace PipeL4.Core

open System
open System.Buffers
open System.Collections.Concurrent
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging

type private ForwardingDirection =
    | ClientToBackend
    | BackendToClient

    member this.Name =
        match this with
        | ClientToBackend -> "client-to-backend"
        | BackendToClient -> "backend-to-client"

[<NoEquality; NoComparison>]
type private ForwardingOutcome =
    | EndOfStream of ForwardingDirection
    | Cancelled of ForwardingDirection
    | Failed of ForwardingDirection * exn

[<NoEquality; NoComparison>]
type internal DirectionalProcessorPipelines =
    { ClientToBackend: IByteStreamProcessor array
      BackendToClient: IByteStreamProcessor array }

[<RequireQualifiedAccess>]
module TcpForwarder =
    [<Literal>]
    let private BufferSize = 64 * 1024

    let private tryShutdownSend (socket: Socket) =
        try
            socket.Shutdown SocketShutdown.Send
        with
        | :? SocketException
        | :? ObjectDisposedException -> ()

    type private SocketPipelineSink(destination: Socket) =
        interface IProcessorPipelineSink with
            member _.WriteAsync(bytes, cancellationToken) =
                ValueTask(
                    task {
                        let mutable position = bytes.Start
                        let mutable segment = ReadOnlyMemory<byte>.Empty

                        while bytes.TryGet(&position, &segment) do
                            let mutable sent = 0

                            while sent < segment.Length do
                                let! sentNow =
                                    destination.SendAsync(
                                        segment.Slice sent,
                                        SocketFlags.None,
                                        cancellationToken
                                    )

                                if sentNow = 0 then
                                    raise (SocketException(int SocketError.ConnectionReset))

                                sent <- sent + sentNow
                    }
                )

            member _.CompleteAsync cancellationToken =
                cancellationToken.ThrowIfCancellationRequested()
                tryShutdownSend destination
                ValueTask()

    let private forward
        direction
        (source: Socket)
        (destination: Socket)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = ArrayPool<byte>.Shared.Rent BufferSize

            try
                try
                    let mutable reachedEndOfStream = false

                    while not reachedEndOfStream do
                        let! received =
                            source.ReceiveAsync(
                                buffer.AsMemory(0, BufferSize),
                                SocketFlags.None,
                                cancellationToken
                            )

                        if received = 0 then
                            reachedEndOfStream <- true
                        else
                            let mutable sent = 0

                            while sent < received do
                                let! sentNow =
                                    destination.SendAsync(
                                        buffer.AsMemory(sent, received - sent),
                                        SocketFlags.None,
                                        cancellationToken
                                    )

                                if sentNow = 0 then
                                    raise (SocketException(int SocketError.ConnectionReset))

                                sent <- sent + sentNow

                    tryShutdownSend destination
                    return EndOfStream direction
                with
                | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                    return Cancelled direction
                | :? ObjectDisposedException when cancellationToken.IsCancellationRequested ->
                    return Cancelled direction
                | error ->
                    return Failed(direction, error)
            finally
                ArrayPool<byte>.Shared.Return(buffer, clearArray = false)
        }

    let private forwardWithProcessorPipeline
        direction
        (processors: IByteStreamProcessor array)
        (source: Socket)
        (destination: Socket)
        (cancellationToken: CancellationToken)
        =
        task {
            let sink = SocketPipelineSink destination
            use pipeline = new ProcessorPipeline(processors, sink, MemoryPool<byte>.Shared)

            try
                let mutable reachedEndOfStream = false

                while not reachedEndOfStream do
                    let owner = MemoryPool<byte>.Shared.Rent BufferSize
                    let mutable transferred = false

                    try
                        let! received =
                            source.ReceiveAsync(
                                owner.Memory.Slice(0, BufferSize),
                                SocketFlags.None,
                                cancellationToken
                            )

                        if received = 0 then
                            reachedEndOfStream <- true
                        else
                            transferred <- true
                            do! pipeline.PushAsync(owner, received, cancellationToken)
                    finally
                        if not transferred then
                            owner.Dispose()

                do! pipeline.CompleteAsync cancellationToken
                return EndOfStream direction
            with
            | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                return Cancelled direction
            | :? ObjectDisposedException when cancellationToken.IsCancellationRequested ->
                return Cancelled direction
            | error ->
                return Failed(direction, error)
        }

    let private logForwardingFailure (logger: ILogger) connectionId clientEndpoint backendEndpoint outcome =
        match outcome with
        | Failed(direction, error) ->
            logger.LogWarning(
                error,
                "Connection {ConnectionId} forwarding failed in {Direction} from {ClientEndpoint} to {BackendEndpoint}",
                [| box connectionId
                   box direction.Name
                   box clientEndpoint
                   box backendEndpoint |]
            )
        | EndOfStream _
        | Cancelled _ -> ()

    let private handleConnection
        connectionId
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: (unit -> DirectionalProcessorPipelines) option)
        (logger: ILogger)
        (client: TcpClient)
        (cancellationToken: CancellationToken)
        =
        task {
            use client = client
            use backend = new TcpClient(configuration.BackendEndpoint.AddressFamily)

            let clientEndpoint = client.Client.RemoteEndPoint

            try
                do! backend.ConnectAsync(configuration.BackendEndpoint, cancellationToken)

                logger.LogInformation(
                    "Connection {ConnectionId} established from {ClientEndpoint} to {BackendEndpoint}",
                    [| box connectionId
                       box clientEndpoint
                       box configuration.BackendEndpoint |]
                )

                use connectionCancellation =
                    CancellationTokenSource.CreateLinkedTokenSource cancellationToken

                let clientToBackend, backendToClient =
                    match processorPipelineFactory with
                    | None ->
                        forward
                            ClientToBackend
                            client.Client
                            backend.Client
                            connectionCancellation.Token,
                        forward
                            BackendToClient
                            backend.Client
                            client.Client
                            connectionCancellation.Token
                    | Some createProcessorPipelines ->
                        let processorPipelines = createProcessorPipelines ()

                        forwardWithProcessorPipeline
                            ClientToBackend
                            processorPipelines.ClientToBackend
                            client.Client
                            backend.Client
                            connectionCancellation.Token,
                        forwardWithProcessorPipeline
                            BackendToClient
                            processorPipelines.BackendToClient
                            backend.Client
                            client.Client
                            connectionCancellation.Token

                let! firstCompleted = Task.WhenAny(clientToBackend, backendToClient)
                let! firstOutcome = firstCompleted

                let other =
                    if Object.ReferenceEquals(firstCompleted, clientToBackend) then
                        backendToClient
                    else
                        clientToBackend

                match firstOutcome with
                | Failed _ ->
                    connectionCancellation.Cancel()
                    client.Dispose()
                    backend.Dispose()
                | Cancelled _ when cancellationToken.IsCancellationRequested ->
                    connectionCancellation.Cancel()
                    client.Dispose()
                    backend.Dispose()
                | EndOfStream _
                | Cancelled _ -> ()

                let! otherOutcome = other

                logForwardingFailure
                    logger
                    connectionId
                    clientEndpoint
                    configuration.BackendEndpoint
                    firstOutcome

                logForwardingFailure
                    logger
                    connectionId
                    clientEndpoint
                    configuration.BackendEndpoint
                    otherOutcome

                let completionReason =
                    if cancellationToken.IsCancellationRequested then
                        "shutdown"
                    else
                        match firstOutcome, otherOutcome with
                        | Failed _, _
                        | _, Failed _ -> "failure"
                        | _ -> "completed"

                logger.LogInformation(
                    "Connection {ConnectionId} closed with reason {CompletionReason}",
                    [| box connectionId; box completionReason |]
                )
            with
            | :? OperationCanceledException when cancellationToken.IsCancellationRequested -> ()
            | :? SocketException as error ->
                if not cancellationToken.IsCancellationRequested then
                    logger.LogWarning(
                        error,
                        "Connection {ConnectionId} could not connect from {ClientEndpoint} to {BackendEndpoint}",
                        [| box connectionId
                           box clientEndpoint
                           box configuration.BackendEndpoint |]
                    )
            | error ->
                if not cancellationToken.IsCancellationRequested then
                    logger.LogError(
                        error,
                        "Connection {ConnectionId} failed unexpectedly from {ClientEndpoint} to {BackendEndpoint}",
                        [| box connectionId
                           box clientEndpoint
                           box configuration.BackendEndpoint |]
                    )
        }

    let private trackConnection
        (activeConnections: ConcurrentDictionary<int64, Task>)
        (logger: ILogger)
        connectionId
        (connectionTask: Task)
        =
        activeConnections.TryAdd(connectionId, connectionTask) |> ignore

        connectionTask.ContinueWith(
            (fun (completed: Task) ->
                let mutable removed = Unchecked.defaultof<Task>
                activeConnections.TryRemove(connectionId, &removed) |> ignore

                if completed.IsFaulted then
                    logger.LogError(
                        completed.Exception,
                        "Connection {ConnectionId} task faulted outside its lifecycle handler",
                        [| box connectionId |]
                    )),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        )
        |> ignore

    let private run
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: (unit -> DirectionalProcessorPipelines) option)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        task {
            use runtimeCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)

            let listener = new TcpListener(configuration.ListenEndpoint)
            let activeConnections = ConcurrentDictionary<int64, Task>()
            let mutable nextConnectionId = 0L
            let mutable listenerFailure = None

            try
                try
                    listener.Start()

                    logger.LogInformation(
                        "TCP listener started on {ListenEndpoint} with backend {BackendEndpoint}",
                        [| box configuration.ListenEndpoint
                           box configuration.BackendEndpoint |]
                    )

                    while not runtimeCancellation.IsCancellationRequested do
                        let! client =
                            listener.AcceptTcpClientAsync runtimeCancellation.Token

                        let connectionId = Interlocked.Increment(&nextConnectionId)

                        let connectionTask =
                            handleConnection
                                connectionId
                                configuration
                                processorPipelineFactory
                                logger
                                client
                                runtimeCancellation.Token

                        trackConnection
                            activeConnections
                            logger
                            connectionId
                            connectionTask
                with
                | :? OperationCanceledException when runtimeCancellation.IsCancellationRequested -> ()
                | error -> listenerFailure <- Some error
            finally
                listener.Stop()
                runtimeCancellation.Cancel()

            let remainingConnections = activeConnections.Values |> Seq.toArray

            if remainingConnections.Length > 0 then
                do! Task.WhenAll remainingConnections

            logger.LogInformation(
                "TCP listener stopped on {ListenEndpoint}",
                [| box configuration.ListenEndpoint |]
            )

            match listenerFailure with
            | Some error -> return raise error
            | None -> return ()
        }

    let runAsync
        (configuration: ForwardingConfiguration)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        run configuration None logger cancellationToken

    let internal runWithProcessorPipelinesAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: unit -> DirectionalProcessorPipelines)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        run configuration (Some processorPipelineFactory) logger cancellationToken
