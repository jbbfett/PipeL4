namespace PipeL4.Core

open System
open System.Buffers
open System.Collections.Concurrent
open System.Net.Sockets
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging

type internal ForwardingDirection =
    | ClientToBackend
    | BackendToClient

    member this.Name =
        match this with
        | ClientToBackend -> "client-to-backend"
        | BackendToClient -> "backend-to-client"

[<Struct; NoEquality; NoComparison>]
type internal TcpForwarderTestHooks =
    { BeforeReceive: ForwardingDirection -> unit
      AfterReceive: ForwardingDirection -> int -> unit
      PendingSend: ForwardingDirection -> unit
      // Internal-only seam for deterministic physical-share handoff tests.
      CapacityAccounting: ConnectionCapacityAccountingTestHooks option }

[<NoEquality; NoComparison>]
type private ForwardingOutcome =
    | EndOfStream of ForwardingDirection
    | Cancelled of ForwardingDirection
    | Failed of ForwardingDirection * exn

[<NoEquality; NoComparison>]
type internal DirectionalProcessorPipelines =
    { ClientToBackend: IByteStreamProcessor array
      BackendToClient: IByteStreamProcessor array }

[<Struct; NoEquality; NoComparison>]
type internal DirectionMetricsSnapshot =
    { Direction: ForwardingDirection
      Stages: ProcessorStageMetricsSnapshot list
      InputBytes: int64
      InputBackedOutputBytes: int64
      GeneratedOutputBytes: int64
      InvocationCount: int64
      SuccessfulInvocationCount: int64
      FailedInvocationCount: int64
      CancelledInvocationCount: int64
      ProcessingDuration: TimeSpan
      MaximumInvocationDuration: TimeSpan
      CurrentRetainedBytes: int64
      CurrentLogicalBufferedBytes: int64
      PeakLogicalBufferedBytes: int64
      LogicalCapacity: CapacityUsageSnapshot
      LogicalCapacityLimitBytes: int64
      PhysicalShare: CapacityUsageSnapshot
      PhysicalShareHandoffState: PhysicalShareHandoffState
      ReceiveRentalAttempts: int64
      ReceiveRequestedBytes: int64
      ReceiveReturnedOwnerCapacityBytes: int64
      ReceiveSuccessfullyExposedOwners: int64
      ReceiveOverRentRetries: int64
      LogicalReceiveExhaustionCount: int64
      SynchronousLogicalOutputExhaustionCount: int64
      SynchronousPhysicalOutputExhaustionCount: int64
      PhysicalReceiveExhaustionCount: int64
      PoolOwnerFitFailureCount: int64
      TerminalOutcome: string
      CleanupFailurePresent: bool }

[<Struct; NoEquality; NoComparison>]
type internal ConnectionMetricsSnapshot =
    { ClientToBackend: DirectionMetricsSnapshot
      BackendToClient: DirectionMetricsSnapshot
      CurrentPhysicalBufferCapacityBytes: int64
      PeakPhysicalBufferCapacityBytes: int64
      ConnectionPhysicalCapacity: CapacityUsageSnapshot
      ClientToBackendPhysicalShare: CapacityUsageSnapshot
      BackendToClientPhysicalShare: CapacityUsageSnapshot
      ClientToBackendHandoffState: PhysicalShareHandoffState
      BackendToClientHandoffState: PhysicalShareHandoffState
      CleanupFailurePresent: bool }

type internal ConnectionRuntimeMetrics
    (
        memoryAccounting: ConnectionMemoryAccounting,
        clientToBackendLogical: CapacityBudget,
        backendToClientLogical: CapacityBudget,
        capacityAccounting: ConnectionCapacityAccounting option
    ) =
    let diagnosticsEnabled =
        match capacityAccounting with
        | Some accounting -> accounting.DiagnosticsEnabled
        | None -> true

    let clientToBackendDiagnostics, backendToClientDiagnostics =
        match capacityAccounting with
        | Some accounting -> accounting.ClientToBackendDiagnostics, accounting.BackendToClientDiagnostics
        | None -> DirectionRuntimeDiagnostics(diagnosticsEnabled), DirectionRuntimeDiagnostics(diagnosticsEnabled)

    let emptyMetrics = ProcessorPipelineMetrics(0, diagnosticsEnabled = diagnosticsEnabled)
    let mutable clientToBackendMetrics = emptyMetrics
    let mutable backendToClientMetrics = emptyMetrics

    let toDirectionSnapshot
        direction
        (snapshot: ProcessorPipelineMetricsSnapshot)
        (logical: CapacityUsageSnapshot)
        (physicalShare: CapacityUsageSnapshot)
        handoffState
        (diagnostics: DirectionDiagnosticsSnapshot)
        =
        { Direction = direction
          Stages = snapshot.Stages
          InputBytes = snapshot.InputBytes
          InputBackedOutputBytes = snapshot.InputBackedOutputBytes
          GeneratedOutputBytes = snapshot.GeneratedOutputBytes
          InvocationCount = snapshot.InvocationCount
          SuccessfulInvocationCount = snapshot.SuccessfulInvocationCount
          FailedInvocationCount = snapshot.FailedInvocationCount
          CancelledInvocationCount = snapshot.CancelledInvocationCount
          ProcessingDuration = snapshot.ProcessingDuration
          MaximumInvocationDuration = snapshot.MaximumInvocationDuration
          CurrentRetainedBytes = snapshot.CurrentRetainedBytes
          CurrentLogicalBufferedBytes = logical.CurrentBytes
          PeakLogicalBufferedBytes = logical.PeakBytes
          LogicalCapacity = logical
          LogicalCapacityLimitBytes = logical.LimitBytes
          PhysicalShare = physicalShare
          PhysicalShareHandoffState = handoffState
          ReceiveRentalAttempts = diagnostics.ReceiveRentalAttempts
          ReceiveRequestedBytes = diagnostics.ReceiveRequestedBytes
          ReceiveReturnedOwnerCapacityBytes = diagnostics.ReceiveReturnedOwnerCapacityBytes
          ReceiveSuccessfullyExposedOwners = diagnostics.ReceiveSuccessfullyExposedOwners
          ReceiveOverRentRetries = diagnostics.ReceiveOverRentRetries
          LogicalReceiveExhaustionCount = diagnostics.LogicalReceiveExhaustionCount
          SynchronousLogicalOutputExhaustionCount = diagnostics.SynchronousLogicalOutputExhaustionCount
          SynchronousPhysicalOutputExhaustionCount = diagnostics.SynchronousPhysicalOutputExhaustionCount
          PhysicalReceiveExhaustionCount = diagnostics.PhysicalReceiveExhaustionCount
          PoolOwnerFitFailureCount = diagnostics.PoolOwnerFitFailureCount
          TerminalOutcome = diagnostics.TerminalOutcome
          CleanupFailurePresent = diagnostics.CleanupFailurePresent }

    new(memoryAccounting: ConnectionMemoryAccounting) =
        ConnectionRuntimeMetrics(
            memoryAccounting,
            new CapacityBudget(Int64.MaxValue),
            new CapacityBudget(Int64.MaxValue),
            None
        )

    new(capacityAccounting: ConnectionCapacityAccounting) =
        ConnectionRuntimeMetrics(
            capacityAccounting.Physical,
            capacityAccounting.ClientToBackendLogical,
            capacityAccounting.BackendToClientLogical,
            Some capacityAccounting
        )

    member internal _.Publish(direction, metrics: ProcessorPipelineMetrics) =
        if obj.ReferenceEquals(metrics, null) then
            nullArg (nameof metrics)

        match direction with
        | ClientToBackend -> Interlocked.Exchange(&clientToBackendMetrics, metrics) |> ignore
        | BackendToClient -> Interlocked.Exchange(&backendToClientMetrics, metrics) |> ignore

    member internal _.RecordProgressFailure(direction, category) =
        match direction with
        | ClientToBackend -> clientToBackendDiagnostics.RecordProgressFailure category
        | BackendToClient -> backendToClientDiagnostics.RecordProgressFailure category

    member internal _.RecordTerminalOutcome(direction, outcome: string) =
        match direction with
        | ClientToBackend -> clientToBackendDiagnostics.RecordTerminalOutcome outcome
        | BackendToClient -> backendToClientDiagnostics.RecordTerminalOutcome outcome

    member internal _.RecordCleanupFailure(direction) =
        match direction with
        | ClientToBackend -> clientToBackendDiagnostics.RecordCleanupFailure()
        | BackendToClient -> backendToClientDiagnostics.RecordCleanupFailure()

    member internal _.GetDirectionDiagnostics(direction) =
        match direction with
        | ClientToBackend -> clientToBackendDiagnostics
        | BackendToClient -> backendToClientDiagnostics

    member _.GetSnapshot() =
        let clientPhysicalShare, backendPhysicalShare, clientHandoff, backendHandoff, connectionPhysicalCapacity =
            match capacityAccounting with
            | Some accounting ->
                accounting.ClientToBackendPhysicalShareSnapshot,
                accounting.BackendToClientPhysicalShareSnapshot,
                accounting.GetPhysicalDirectionHandoffState true,
                accounting.GetPhysicalDirectionHandoffState false,
                accounting.PhysicalCapacitySnapshot
            | None ->
                let physical = memoryAccounting.GetSnapshot()

                { LimitBytes = Int64.MaxValue
                  CurrentBytes = physical.CurrentPhysicalBufferCapacityBytes
                  PeakBytes = physical.PeakPhysicalBufferCapacityBytes
                  CurrentWaiterCount = 0L
                  PeakWaiterCount = 0L
                  QueuedWaitCount = 0L
                  TotalWaitDuration = TimeSpan.Zero
                  MaximumWaitDuration = TimeSpan.Zero },
                { LimitBytes = Int64.MaxValue
                  CurrentBytes = 0L
                  PeakBytes = 0L
                  CurrentWaiterCount = 0L
                  PeakWaiterCount = 0L
                  QueuedWaitCount = 0L
                  TotalWaitDuration = TimeSpan.Zero
                  MaximumWaitDuration = TimeSpan.Zero },
                PhysicalShareHandoffState.Open,
                PhysicalShareHandoffState.Open,
                { LimitBytes = Int64.MaxValue
                  CurrentBytes = physical.CurrentPhysicalBufferCapacityBytes
                  PeakBytes = physical.PeakPhysicalBufferCapacityBytes
                  CurrentWaiterCount = 0L
                  PeakWaiterCount = 0L
                  QueuedWaitCount = 0L
                  TotalWaitDuration = TimeSpan.Zero
                  MaximumWaitDuration = TimeSpan.Zero }

        let clientDiagnostics = clientToBackendDiagnostics.GetSnapshot()
        let backendDiagnostics = backendToClientDiagnostics.GetSnapshot()

        let clientToBackend =
            Volatile.Read(&clientToBackendMetrics).GetSnapshot()
            |> fun snapshot ->
                toDirectionSnapshot
                    ClientToBackend
                    snapshot
                    (clientToBackendLogical.GetSnapshot())
                    clientPhysicalShare
                    clientHandoff
                    clientDiagnostics

        let backendToClient =
            Volatile.Read(&backendToClientMetrics).GetSnapshot()
            |> fun snapshot ->
                toDirectionSnapshot
                    BackendToClient
                    snapshot
                    (backendToClientLogical.GetSnapshot())
                    backendPhysicalShare
                    backendHandoff
                    backendDiagnostics

        let physical = memoryAccounting.GetSnapshot()

        { ClientToBackend = clientToBackend
          BackendToClient = backendToClient
          CurrentPhysicalBufferCapacityBytes = physical.CurrentPhysicalBufferCapacityBytes
          PeakPhysicalBufferCapacityBytes = physical.PeakPhysicalBufferCapacityBytes
          ConnectionPhysicalCapacity = connectionPhysicalCapacity
          ClientToBackendPhysicalShare = clientPhysicalShare
          BackendToClientPhysicalShare = backendPhysicalShare
          ClientToBackendHandoffState = clientHandoff
          BackendToClientHandoffState = backendHandoff
          CleanupFailurePresent = clientDiagnostics.CleanupFailurePresent || backendDiagnostics.CleanupFailurePresent }

type internal ConnectionMemoryObserver = ConnectionRuntimeMetrics -> unit

[<NoEquality; NoComparison>]
type private DirectionalPipelineSelection =
    { ClientToBackend: IByteStreamProcessor array option
      BackendToClient: IByteStreamProcessor array option }

[<RequireQualifiedAccess>]
module TcpForwarder =
    let private raisePreserving<'value> (error: exn) : 'value =
        ExceptionDispatchInfo.Capture(error).Throw()
        Unchecked.defaultof<'value>

    let private configuredCapacityLimits (configuration: ForwardingConfiguration) =
        ConnectionCapacityLimits(
            configuration.ClientToServerLogicalCapacityBytes,
            configuration.ServerToClientLogicalCapacityBytes,
            configuration.ConnectionPhysicalCapacityBytes
        )

    [<Literal>]
    let private BufferSize = 64 * 1024

    let private tryShutdownSend (socket: Socket) =
        try
            socket.Shutdown SocketShutdown.Send
        with
        | :? SocketException
        | :? ObjectDisposedException -> ()

    let private aggregateFailure message (primary: exn) (cleanupFailures: exn array) =
        match cleanupFailures.Length with
        | 0 -> primary
        | _ ->
            AggregateException(message, Array.append [| primary |] cleanupFailures)
            :> exn

    let private receiveRequestSize
        (memoryAccounting: ConnectionMemoryAccounting)
        maximumPayload
        oppositeHandoffState
        (diagnostics: DirectionRuntimeDiagnostics)
        =
        let rec sample () =
            let stateBefore = oppositeHandoffState ()
            let admission = memoryAccounting.GetAdmissionSnapshot()
            let available = admission.LimitBytes - admission.CurrentBytes

            if available > 0L then
                min maximumPayload (int (min available (int64 Int32.MaxValue)))
            else
                match oppositeHandoffState () with
                | PhysicalShareHandoffState.Committed ->
                    // A committed handoff is already in progress.  Preserve
                    // the existing asynchronous admission wait so the
                    // sibling can publish its expanded share.
                    1
                | PhysicalShareHandoffState.Completed when stateBefore <> PhysicalShareHandoffState.Completed ->
                    // The completion publication raced the first capacity
                    // sample.  Resample after the release is visible.
                    sample ()
                | PhysicalShareHandoffState.Open
                | PhysicalShareHandoffState.Completed
                | PhysicalShareHandoffState.Failed ->
                    diagnostics.RecordProgressFailure ProgressFailureCategory.PhysicalReceiveExhaustion
                    invalidOp
                        "Physical receive capacity cannot make progress at a quiescent receive boundary."

        sample ()

    type private SocketPipelineSink
        (direction: ForwardingDirection, destination: Socket, testHooks: TcpForwarderTestHooks option)
        =
        let pendingSend =
            match testHooks with
            | Some hooks -> hooks.PendingSend
            | None -> ignore

        interface IProcessorPipelineSink with
            member _.WriteAsync(bytes, cancellationToken) =
                ValueTask(
                    task {
                        let mutable position = bytes.Start
                        let mutable segment = ReadOnlyMemory<byte>.Empty

                        while bytes.TryGet(&position, &segment) do
                            let mutable sent = 0

                            while sent < segment.Length do
                                let send =
                                    destination.SendAsync(
                                        segment.Slice sent,
                                        SocketFlags.None,
                                        cancellationToken
                                    )

                                if not send.IsCompletedSuccessfully then
                                    pendingSend direction

                                let! sentNow = send

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
        (memoryAccounting: ConnectionMemoryAccounting)
        oppositeHandoffState
        completeDirection
        reportFailure
        (diagnostics: DirectionRuntimeDiagnostics)
        testHooks
        (source: Socket)
        (destination: Socket)
        (cancellationToken: CancellationToken)
        =
        task {
            diagnostics.RecordTerminalOutcome "running"

            let beforeReceive =
                match testHooks with
                | Some hooks -> hooks.BeforeReceive
                | None -> ignore

            let afterReceive =
                match testHooks with
                | Some hooks -> hooks.AfterReceive
                | None -> fun _ _ -> ()

            let mutable lease: IMemoryOwner<byte> option = None
            let mutable primaryError: exn option = None
            let mutable outcome: ForwardingOutcome option = None

            try
                let! trackedOwner =
                    memoryAccounting.RentArrayReceiveAsync(
                        ArrayPool<byte>.Shared,
                        receiveRequestSize memoryAccounting BufferSize oppositeHandoffState diagnostics,
                        cancellationToken
                    ).AsTask()

                lease <- Some trackedOwner

                let! forwardingOutcome =
                    task {
                        try
                            let mutable reachedEndOfStream = false

                            while not reachedEndOfStream do
                                beforeReceive direction

                                let! received =
                                    source.ReceiveAsync(
                                        trackedOwner.Memory.Slice(0, min trackedOwner.Memory.Length BufferSize),
                                        SocketFlags.None,
                                        cancellationToken
                                    )

                                afterReceive direction received

                                if received = 0 then
                                    reachedEndOfStream <- true
                                else
                                    let mutable sent = 0

                                    while sent < received do
                                        let! sentNow =
                                            destination.SendAsync(
                                                trackedOwner.Memory.Slice(sent, received - sent),
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
                    }

                outcome <-
                    match forwardingOutcome with
                    | Failed(failedDirection, error) ->
                        Some(Failed(failedDirection, reportFailure error))
                    | value -> Some value
            with
            | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                outcome <- Some(Cancelled direction)
            | :? ObjectDisposedException when cancellationToken.IsCancellationRequested ->
                outcome <- Some(Cancelled direction)
            | error -> primaryError <- Some(reportFailure error)

            let mutable cleanupError: exn option = None

            match lease with
            | Some trackedOwner ->
                try
                    trackedOwner.Dispose()
                with error ->
                    diagnostics.RecordCleanupFailure()
                    cleanupError <- Some(reportFailure error)
            | None -> ()

            let completionError =
                try
                    completeDirection ()
                    None
                with error ->
                    diagnostics.RecordCleanupFailure()
                    Some(reportFailure error)

            match cleanupError, completionError with
            | None, Some error -> cleanupError <- Some error
            | Some cleanup, Some completion ->
                cleanupError <-
                    Some(
                        AggregateException(
                            "Forwarding completion and cleanup both failed.",
                            [| cleanup; completion |]
                        )
                    )
            | _ -> ()

            match primaryError with
            | Some error ->
                match cleanupError with
                | Some cleanup ->
                    return
                        Failed(
                            direction,
                            AggregateException(
                                "Forwarding operation and cleanup both failed.",
                                [| error; cleanup |]
                            )
                        )
                | None -> return Failed(direction, error)
            | None ->
                match cleanupError, outcome with
                | Some error, Some(Failed(failedDirection, primary)) ->
                    return
                        Failed(
                            failedDirection,
                            AggregateException(
                                "Forwarding operation and cleanup both failed.",
                                [| primary; error |]
                            )
                        )
                | Some error, _ -> return Failed(direction, error)
                | None, Some value -> return value
                | None, None ->
                    return
                        Failed(
                            direction,
                            reportFailure (InvalidOperationException("Forwarding completed without an outcome."))
                        )
        }

    let private forwardWithProcessorPipeline
        direction
        (processors: IByteStreamProcessor array)
        (metrics: ProcessorPipelineMetrics)
        (memoryAccounting: ConnectionMemoryAccounting)
        (logicalCapacity: CapacityBudget)
        oppositeHandoffState
        completeDirection
        reportFailure
        (diagnostics: DirectionRuntimeDiagnostics)
        testHooks
        (source: Socket)
        (destination: Socket)
        (cancellationToken: CancellationToken)
        =
        task {
            diagnostics.RecordTerminalOutcome "running"

            let sink = SocketPipelineSink(direction, destination, testHooks)
            let beforeReceive =
                match testHooks with
                | Some hooks -> hooks.BeforeReceive
                | None -> ignore

            let afterReceive =
                match testHooks with
                | Some hooks -> hooks.AfterReceive
                | None -> fun _ _ -> ()

            let pipeline =
                new ProcessorPipeline(
                    processors,
                    sink,
                    MemoryPool<byte>.Shared,
                    memoryAccounting,
                    logicalCapacity,
                    metrics,
                    diagnostics = diagnostics
                )

            let publishedDisposalFailures = ResizeArray<exn>()

            let publishDisposalFailure error =
                diagnostics.RecordCleanupFailure()
                publishedDisposalFailures.Add(reportFailure error)

            let! outcome =
                task {
                    try
                        let mutable reachedEndOfStream = false

                        while not reachedEndOfStream do
                            beforeReceive direction

                            let logicalSnapshot = pipeline.GetLogicalCapacitySnapshot()
                            let logicalHeadroom = logicalSnapshot.LimitBytes - logicalSnapshot.CurrentBytes

                            if logicalHeadroom <= 0L then
                                diagnostics.RecordProgressFailure ProgressFailureCategory.LogicalReceiveExhaustion
                                invalidOp "Logical capacity cannot admit input while retained pipeline data prevents progress."

                            let maximumReceive =
                                min BufferSize (int (min logicalHeadroom (int64 Int32.MaxValue)))

                            let! owner =
                                memoryAccounting.RentReceiveAsync(
                                    MemoryPool<byte>.Shared,
                                    receiveRequestSize memoryAccounting maximumReceive oppositeHandoffState diagnostics,
                                    cancellationToken
                                ).AsTask()

                            let mutable transferred = false

                            try
                                let! received =
                                    source.ReceiveAsync(
                                        owner.Memory.Slice(0, min owner.Memory.Length maximumReceive),
                                        SocketFlags.None,
                                        cancellationToken
                                    )

                                afterReceive direction received

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

            let outcome =
                match outcome with
                | Failed(failedDirection, error) ->
                    Failed(failedDirection, reportFailure error)
                | value -> value

            let! disposalFailure =
                task {
                    try
                        do!
                            pipeline
                                .DisposeAsync(completeDirection, publishDisposalFailure)
                                .AsTask()
                        return None
                    with error ->
                        diagnostics.RecordCleanupFailure()
                        return Some error
                }

            let disposalError =
                if publishedDisposalFailures.Count > 0 then
                    let failures = publishedDisposalFailures.ToArray()

                    if failures.Length = 1 then
                        Some failures[0]
                    else
                        Some(
                            AggregateException(
                                "Multiple processor pipeline resources failed during disposal.",
                                failures
                            )
                            :> exn
                        )
                else
                    match disposalFailure with
                    | Some error -> Some(reportFailure error)
                    | None -> None

            match outcome, disposalError with
            | value, None -> return value
            | Failed(failedDirection, error), Some disposalError ->
                return
                    Failed(
                        failedDirection,
                        AggregateException(
                            "Processor pipeline execution and disposal both failed.",
                            error,
                            disposalError
                        )
                    )
            | EndOfStream completedDirection, Some disposalError
            | Cancelled completedDirection, Some disposalError ->
                return Failed(completedDirection, disposalError)
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

    let private handoffName state =
        match state with
        | PhysicalShareHandoffState.Open -> "open"
        | PhysicalShareHandoffState.Committed -> "committed"
        | PhysicalShareHandoffState.Completed -> "completed"
        | PhysicalShareHandoffState.Failed -> "failed"

    let private logTerminalDiagnostics
        (logger: ILogger)
        connectionId
        (snapshot: ConnectionMetricsSnapshot)
        =
        logger.LogInformation(
            "Connection {ConnectionId} runtime diagnostics: CurrentPhysicalBufferCapacityBytes {CurrentPhysicalBufferCapacityBytes}, PeakPhysicalBufferCapacityBytes {PeakPhysicalBufferCapacityBytes}, ConnectionPhysicalLimitBytes {ConnectionPhysicalLimitBytes}, ConnectionPhysicalCurrentWaiterCount {ConnectionPhysicalCurrentWaiterCount}, ConnectionPhysicalPeakWaiterCount {ConnectionPhysicalPeakWaiterCount}, ConnectionPhysicalQueuedWaitCount {ConnectionPhysicalQueuedWaitCount}, ConnectionPhysicalTotalWaitMilliseconds {ConnectionPhysicalTotalWaitMilliseconds}, ConnectionPhysicalMaximumWaitMilliseconds {ConnectionPhysicalMaximumWaitMilliseconds}, ClientToBackendHandoffState {ClientToBackendHandoffState}, BackendToClientHandoffState {BackendToClientHandoffState}, CleanupFailurePresent {CleanupFailurePresent}",
            [| box connectionId
               box snapshot.CurrentPhysicalBufferCapacityBytes
               box snapshot.PeakPhysicalBufferCapacityBytes
               box snapshot.ConnectionPhysicalCapacity.LimitBytes
               box snapshot.ConnectionPhysicalCapacity.CurrentWaiterCount
               box snapshot.ConnectionPhysicalCapacity.PeakWaiterCount
               box snapshot.ConnectionPhysicalCapacity.QueuedWaitCount
               box snapshot.ConnectionPhysicalCapacity.TotalWaitDuration.TotalMilliseconds
               box snapshot.ConnectionPhysicalCapacity.MaximumWaitDuration.TotalMilliseconds
               box (handoffName snapshot.ClientToBackendHandoffState)
               box (handoffName snapshot.BackendToClientHandoffState)
               box snapshot.CleanupFailurePresent |]
        )

        let logDirection (direction: DirectionMetricsSnapshot) =
            logger.LogInformation(
                "Direction {Direction} Connection {ConnectionId} runtime diagnostics: TerminalOutcome {TerminalOutcome}, CleanupFailurePresent {CleanupFailurePresent}, CurrentLogicalBufferedBytes {CurrentLogicalBufferedBytes}, PeakLogicalBufferedBytes {PeakLogicalBufferedBytes}, LogicalCapacityLimitBytes {LogicalCapacityLimitBytes}, LogicalCurrentWaiterCount {LogicalCurrentWaiterCount}, LogicalPeakWaiterCount {LogicalPeakWaiterCount}, LogicalQueuedWaitCount {LogicalQueuedWaitCount}, LogicalTotalWaitMilliseconds {LogicalTotalWaitMilliseconds}, LogicalMaximumWaitMilliseconds {LogicalMaximumWaitMilliseconds}, PhysicalShareCurrentBytes {PhysicalShareCurrentBytes}, PhysicalSharePeakBytes {PhysicalSharePeakBytes}, PhysicalShareLimitBytes {PhysicalShareLimitBytes}, PhysicalShareCurrentWaiterCount {PhysicalShareCurrentWaiterCount}, PhysicalSharePeakWaiterCount {PhysicalSharePeakWaiterCount}, PhysicalShareQueuedWaitCount {PhysicalShareQueuedWaitCount}, PhysicalShareTotalWaitMilliseconds {PhysicalShareTotalWaitMilliseconds}, PhysicalShareMaximumWaitMilliseconds {PhysicalShareMaximumWaitMilliseconds}, PhysicalShareHandoffState {PhysicalShareHandoffState}, ReceiveRentalAttempts {ReceiveRentalAttempts}, ReceiveRequestedBytes {ReceiveRequestedBytes}, ReceiveReturnedOwnerCapacityBytes {ReceiveReturnedOwnerCapacityBytes}, ReceiveSuccessfullyExposedOwners {ReceiveSuccessfullyExposedOwners}, ReceiveOverRentRetries {ReceiveOverRentRetries}, LogicalReceiveExhaustionCount {LogicalReceiveExhaustionCount}, SynchronousLogicalOutputExhaustionCount {SynchronousLogicalOutputExhaustionCount}, SynchronousPhysicalOutputExhaustionCount {SynchronousPhysicalOutputExhaustionCount}, PhysicalReceiveExhaustionCount {PhysicalReceiveExhaustionCount}, PoolOwnerFitFailureCount {PoolOwnerFitFailureCount}",
                [| box direction.Direction.Name
                   box connectionId
                   box direction.TerminalOutcome
                   box direction.CleanupFailurePresent
                   box direction.CurrentLogicalBufferedBytes
                   box direction.PeakLogicalBufferedBytes
                   box direction.LogicalCapacityLimitBytes
                   box direction.LogicalCapacity.CurrentWaiterCount
                   box direction.LogicalCapacity.PeakWaiterCount
                   box direction.LogicalCapacity.QueuedWaitCount
                   box direction.LogicalCapacity.TotalWaitDuration.TotalMilliseconds
                   box direction.LogicalCapacity.MaximumWaitDuration.TotalMilliseconds
                   box direction.PhysicalShare.CurrentBytes
                   box direction.PhysicalShare.PeakBytes
                   box direction.PhysicalShare.LimitBytes
                   box direction.PhysicalShare.CurrentWaiterCount
                   box direction.PhysicalShare.PeakWaiterCount
                   box direction.PhysicalShare.QueuedWaitCount
                   box direction.PhysicalShare.TotalWaitDuration.TotalMilliseconds
                   box direction.PhysicalShare.MaximumWaitDuration.TotalMilliseconds
                   box (handoffName direction.PhysicalShareHandoffState)
                   box direction.ReceiveRentalAttempts
                   box direction.ReceiveRequestedBytes
                   box direction.ReceiveReturnedOwnerCapacityBytes
                   box direction.ReceiveSuccessfullyExposedOwners
                   box direction.ReceiveOverRentRetries
                   box direction.LogicalReceiveExhaustionCount
                   box direction.SynchronousLogicalOutputExhaustionCount
                   box direction.SynchronousPhysicalOutputExhaustionCount
                   box direction.PhysicalReceiveExhaustionCount
                   box direction.PoolOwnerFitFailureCount |]
            )

            for stage in direction.Stages do
                logger.LogInformation(
                    "Direction {Direction} stage {StageIndex} Connection {ConnectionId} runtime diagnostics: InputBytes {InputBytes}, InputBackedOutputBytes {InputBackedOutputBytes}, GeneratedOutputBytes {GeneratedOutputBytes}, InvocationCount {InvocationCount}, SuccessfulInvocationCount {SuccessfulInvocationCount}, FailedInvocationCount {FailedInvocationCount}, CancelledInvocationCount {CancelledInvocationCount}, ProcessingDurationMilliseconds {ProcessingDurationMilliseconds}, MaximumInvocationDurationMilliseconds {MaximumInvocationDurationMilliseconds}, CurrentRetainedBytes {CurrentRetainedBytes}",
                    [| box direction.Direction.Name
                       box stage.StageIndex
                       box connectionId
                       box stage.InputBytes
                       box stage.InputBackedOutputBytes
                       box stage.GeneratedOutputBytes
                       box stage.InvocationCount
                       box stage.SuccessfulInvocationCount
                       box stage.FailedInvocationCount
                       box stage.CancelledInvocationCount
                       box stage.ProcessingDuration.TotalMilliseconds
                       box stage.MaximumInvocationDuration.TotalMilliseconds
                       box stage.CurrentRetainedBytes |]
                )

        logDirection snapshot.ClientToBackend
        logDirection snapshot.BackendToClient

    let private handleConnection
        connectionId
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: (ConnectionState -> ValueTask<DirectionalPipelineSelection>) option)
        (capacityLimits: ConnectionCapacityLimits)
        (memoryObserver: ConnectionMemoryObserver option)
        (testHooks: TcpForwarderTestHooks option)
        (logger: ILogger)
        (client: TcpClient)
        (cancellationToken: CancellationToken)
        =
        task {
            let connectionState = ConnectionState()
            use capacityAccounting =
                match testHooks with
                | Some hooks ->
                    match hooks.CapacityAccounting with
                    | Some capacityHooks ->
                        new ConnectionCapacityAccounting(
                            capacityLimits,
                            testHooks = capacityHooks,
                            diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                        )
                    | None ->
                        new ConnectionCapacityAccounting(
                            capacityLimits,
                            diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                        )
                | None ->
                    new ConnectionCapacityAccounting(
                        capacityLimits,
                        diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                    )
            let connectionMetrics = ConnectionRuntimeMetrics(capacityAccounting)
            let clientEndpoint = client.Client.RemoteEndPoint

            let runConnection () : Task =
                task {
                    use client = client
                    use backend = new TcpClient(configuration.BackendEndpoint.AddressFamily)

                    do! backend.ConnectAsync(configuration.BackendEndpoint, cancellationToken)

                    logger.LogInformation(
                        "Connection {ConnectionId} established from {ClientEndpoint} to {BackendEndpoint}",
                        [| box connectionId
                           box clientEndpoint
                           box configuration.BackendEndpoint |]
                    )

                    use connectionCancellation =
                        CancellationTokenSource.CreateLinkedTokenSource cancellationToken

                    let mutable abortStarted = 0
                    let failureGate = obj ()
                    let independentFailures = ResizeArray<ForwardingDirection * exn>()

                    let abortConnection () =
                        let failures = ResizeArray<exn>()

                        if Interlocked.CompareExchange(&abortStarted, 1, 0) = 0 then
                            try
                                connectionCancellation.Cancel()
                            with error ->
                                failures.Add error

                            try
                                client.Dispose()
                            with error ->
                                failures.Add error

                            try
                                backend.Dispose()
                            with error ->
                                failures.Add error

                        failures.ToArray()

                    let reportFailure direction error =
                        let shouldAbort =
                            lock failureGate (fun () ->
                                if
                                    independentFailures
                                    |> Seq.exists (fun (_, published) -> Object.ReferenceEquals(published, error))
                                then
                                    false
                                else
                                    independentFailures.Add(direction, error)
                                    independentFailures.Count = 1)

                        let abortFailures =
                            if shouldAbort then
                                abortConnection ()
                            else
                                Array.empty

                        aggregateFailure "Connection failure and abort cleanup failed." error abortFailures

                    let failureOrder direction =
                        lock failureGate (fun () ->
                            independentFailures
                            |> Seq.tryFindIndex (fun (publishedDirection, _) -> publishedDirection = direction)
                            |> Option.defaultValue Int32.MaxValue)

                    let protectDirection
                        (direction: ForwardingDirection)
                        (start: unit -> Task<ForwardingOutcome>)
                        : Task<ForwardingOutcome>
                        =
                        task {
                            try
                                return! start ()
                            with error ->
                                return Failed(direction, reportFailure direction error)
                        }

                    let normalizeTerminalOutcome outcome =
                        match outcome with
                        | Failed _ -> outcome
                        | Cancelled direction when cancellationToken.IsCancellationRequested ->
                            match abortConnection () with
                            | [||] -> outcome
                            | [| error |] -> Failed(direction, error)
                            | failures ->
                                Failed(
                                    direction,
                                    AggregateException(
                                        "Connection cancellation cleanup failed.",
                                        failures
                                    )
                                )
                        | EndOfStream _
                        | Cancelled _ -> outcome

                    let! processorPipelines =
                        match processorPipelineFactory with
                        | None -> Task.FromResult None
                        | Some createProcessorPipelines ->
                            task {
                                let! pipelines =
                                    createProcessorPipelines connectionState
                                    |> _.AsTask()

                                return Some pipelines
                            }

                    let clientToBackend, backendToClient =
                        match processorPipelines with
                        | None ->
                            connectionMetrics.Publish(
                                ClientToBackend,
                                ProcessorPipelineMetrics(
                                    0,
                                    diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                                )
                            )
                            connectionMetrics.Publish(
                                BackendToClient,
                                ProcessorPipelineMetrics(
                                    0,
                                    diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                                )
                            )

                            protectDirection ClientToBackend (fun () ->
                                forward
                                    ClientToBackend
                                    capacityAccounting.ClientToBackendPhysical
                                    (fun () -> capacityAccounting.GetPhysicalDirectionHandoffState false)
                                    (fun () -> capacityAccounting.CompletePhysicalDirection true)
                                    (reportFailure ClientToBackend)
                                    (connectionMetrics.GetDirectionDiagnostics ClientToBackend)
                                    testHooks
                                    client.Client
                                    backend.Client
                                    connectionCancellation.Token),
                            protectDirection BackendToClient (fun () ->
                                forward
                                    BackendToClient
                                    capacityAccounting.BackendToClientPhysical
                                    (fun () -> capacityAccounting.GetPhysicalDirectionHandoffState true)
                                    (fun () -> capacityAccounting.CompletePhysicalDirection false)
                                    (reportFailure BackendToClient)
                                    (connectionMetrics.GetDirectionDiagnostics BackendToClient)
                                    testHooks
                                    backend.Client
                                    client.Client
                                    connectionCancellation.Token)
                        | Some processorPipelines ->
                            let forwardDirection
                                direction
                                (processors: IByteStreamProcessor array option)
                                source
                                destination
                                =
                                protectDirection direction (fun () ->
                                    match processors with
                                    | Some values ->
                                        let metrics =
                                            ProcessorPipelineMetrics(
                                                values.Length,
                                                diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                                            )
                                        connectionMetrics.Publish(direction, metrics)

                                        forwardWithProcessorPipeline
                                            direction
                                            values
                                            metrics
                                            (match direction with
                                             | ClientToBackend -> capacityAccounting.ClientToBackendPhysical
                                             | BackendToClient -> capacityAccounting.BackendToClientPhysical)
                                            (match direction with
                                             | ClientToBackend -> capacityAccounting.ClientToBackendLogical
                                             | BackendToClient -> capacityAccounting.BackendToClientLogical)
                                            (fun () ->
                                                capacityAccounting.GetPhysicalDirectionHandoffState(
                                                    match direction with
                                                    | ClientToBackend -> false
                                                    | BackendToClient -> true
                                                ))
                                            (fun () ->
                                                capacityAccounting.CompletePhysicalDirection(
                                                    match direction with
                                                    | ClientToBackend -> true
                                                    | BackendToClient -> false
                                                ))
                                            (reportFailure direction)
                                            (connectionMetrics.GetDirectionDiagnostics direction)
                                            testHooks
                                            source
                                            destination
                                            connectionCancellation.Token
                                    | None ->
                                        connectionMetrics.Publish(
                                            direction,
                                            ProcessorPipelineMetrics(
                                                0,
                                                diagnosticsEnabled = configuration.RuntimeDiagnosticsEnabled
                                            )
                                        )

                                        forward
                                            direction
                                            (match direction with
                                             | ClientToBackend -> capacityAccounting.ClientToBackendPhysical
                                             | BackendToClient -> capacityAccounting.BackendToClientPhysical)
                                            (fun () ->
                                                capacityAccounting.GetPhysicalDirectionHandoffState(
                                                    match direction with
                                                    | ClientToBackend -> false
                                                    | BackendToClient -> true
                                                ))
                                            (fun () ->
                                                capacityAccounting.CompletePhysicalDirection(
                                                    match direction with
                                                    | ClientToBackend -> true
                                                    | BackendToClient -> false
                                                ))
                                            (reportFailure direction)
                                            (connectionMetrics.GetDirectionDiagnostics direction)
                                            testHooks
                                            source
                                            destination
                                            connectionCancellation.Token)

                            forwardDirection
                                ClientToBackend
                                processorPipelines.ClientToBackend
                                client.Client
                                backend.Client,
                            forwardDirection
                                BackendToClient
                                processorPipelines.BackendToClient
                                backend.Client
                                client.Client

                    let! outcomes = Task.WhenAll [| clientToBackend; backendToClient |]

                    let outcomes =
                        outcomes
                        |> Array.map normalizeTerminalOutcome
                        |> Array.sortBy (fun outcome ->
                            match outcome with
                            | Failed(direction, _) -> failureOrder direction
                            | EndOfStream _
                            | Cancelled _ -> Int32.MaxValue)

                    let firstOutcome = outcomes[0]
                    let otherOutcome = outcomes[1]

                    let recordTerminalOutcome outcome =
                        match outcome with
                        | EndOfStream direction ->
                            connectionMetrics.RecordTerminalOutcome(direction, "completed")
                        | Cancelled direction ->
                            connectionMetrics.RecordTerminalOutcome(direction, "cancelled")
                        | Failed(direction, _) ->
                            connectionMetrics.RecordTerminalOutcome(direction, "failed")

                    recordTerminalOutcome firstOutcome
                    recordTerminalOutcome otherOutcome

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
                }

            let! (connectionFailure: exn option) =
                task {
                    try
                        match memoryObserver with
                        | Some observe ->
                            try
                                observe connectionMetrics
                            with error ->
                                // The accepted client is not yet owned by
                                // runConnection when the observer runs.
                                // Dispose it here while preserving the callback
                                // exception and allowing connection-state cleanup.
                                try
                                    client.Dispose()
                                with _ ->
                                    ()

                                raisePreserving error
                        | None -> ()

                        do! runConnection ()
                        return None
                    with error ->
                        return Some error
                }

            let! (stateDisposalFailure: exn option) =
                task {
                    try
                        do! (connectionState :> IAsyncDisposable).DisposeAsync().AsTask()
                        return None
                    with error ->
                        return Some error
                }

            match stateDisposalFailure with
            | Some _ ->
                connectionMetrics.RecordCleanupFailure ClientToBackend
                connectionMetrics.RecordCleanupFailure BackendToClient
            | None -> ()

            if configuration.RuntimeDiagnosticsEnabled then
                try
                    let snapshot = connectionMetrics.GetSnapshot()
                    logTerminalDiagnostics logger connectionId snapshot
                with _ ->
                    // Runtime diagnostics are optional.  A snapshot or logger
                    // failure must not replace the already-determined
                    // forwarding or cleanup result, and reporting it here
                    // would risk recursive logging failure.
                    ()

            let combineFailures (primaryError: exn) (disposalError: exn option) : exn =
                match disposalError with
                | None -> primaryError
                | Some cleanupError ->
                    AggregateException(
                        "Connection execution and connection-state disposal both failed.",
                        primaryError,
                        cleanupError
                    )
                    :> exn

            let logStateDisposalFailureDuringShutdown () =
                match stateDisposalFailure with
                | Some error ->
                    logger.LogWarning(
                        error,
                        "Connection {ConnectionId} state disposal failed during shutdown",
                        [| box connectionId |]
                    )
                | None -> ()

            match connectionFailure with
            | Some (:? OperationCanceledException) when cancellationToken.IsCancellationRequested ->
                logStateDisposalFailureDuringShutdown ()
            | Some (:? SocketException as error) ->
                if cancellationToken.IsCancellationRequested then
                    logStateDisposalFailureDuringShutdown ()
                else
                    logger.LogWarning(
                        combineFailures error stateDisposalFailure,
                        "Connection {ConnectionId} could not connect from {ClientEndpoint} to {BackendEndpoint}",
                        [| box connectionId
                           box clientEndpoint
                           box configuration.BackendEndpoint |]
                    )
            | Some error ->
                if cancellationToken.IsCancellationRequested then
                    logStateDisposalFailureDuringShutdown ()
                else
                    logger.LogError(
                        combineFailures error stateDisposalFailure,
                        "Connection {ConnectionId} failed unexpectedly from {ClientEndpoint} to {BackendEndpoint}",
                        [| box connectionId
                           box clientEndpoint
                           box configuration.BackendEndpoint |]
                    )
            | None ->
                match stateDisposalFailure with
                | Some error ->
                    logger.LogWarning(
                        error,
                        "Connection {ConnectionId} state disposal failed from {ClientEndpoint} to {BackendEndpoint}",
                        [| box connectionId
                           box clientEndpoint
                           box configuration.BackendEndpoint |]
                    )
                | None -> ()
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
        (processorPipelineFactory: (ConnectionState -> ValueTask<DirectionalPipelineSelection>) option)
        (capacityLimits: ConnectionCapacityLimits)
        (memoryObserver: ConnectionMemoryObserver option)
        (testHooks: TcpForwarderTestHooks option)
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
                                capacityLimits
                                memoryObserver
                                testHooks
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
        run configuration None (configuredCapacityLimits configuration) None None logger cancellationToken

    let internal runWithProcessorPipelinesAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = Some pipelines.ClientToBackend
                  BackendToClient = Some pipelines.BackendToClient }
            )

        run configuration (Some createSelection) (configuredCapacityLimits configuration) None None logger cancellationToken

    let internal runWithOptionalProcessorPipelinesAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let optional processors =
            if Array.isEmpty processors then None else Some processors

        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = optional pipelines.ClientToBackend
                  BackendToClient = optional pipelines.BackendToClient }
            )

        run configuration (Some createSelection) (configuredCapacityLimits configuration) None None logger cancellationToken

    let internal runWithOptionalProcessorPipelineFactoryAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> ValueTask<DirectionalProcessorPipelines>)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let optional processors =
            if Array.isEmpty processors then None else Some processors

        let createSelection connectionState =
            ValueTask<DirectionalPipelineSelection>(
                task {
                    let! pipelines =
                        processorPipelineFactory connectionState
                        |> _.AsTask()

                    return
                        { ClientToBackend = optional pipelines.ClientToBackend
                          BackendToClient = optional pipelines.BackendToClient }
                }
            )

        run configuration (Some createSelection) (configuredCapacityLimits configuration) None None logger cancellationToken

    let internal runWithMemoryObserverAsync
        (configuration: ForwardingConfiguration)
        (memoryObserver: ConnectionMemoryObserver)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        run configuration None (configuredCapacityLimits configuration) (Some memoryObserver) None logger cancellationToken

    let internal runWithProcessorPipelinesAndMemoryObserverAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (memoryObserver: ConnectionMemoryObserver)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = Some pipelines.ClientToBackend
                  BackendToClient = Some pipelines.BackendToClient }
            )

        run
            configuration
            (Some createSelection)
            (configuredCapacityLimits configuration)
            (Some memoryObserver)
            None
            logger
            cancellationToken

    let internal runWithOptionalProcessorPipelinesAndMemoryObserverAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (memoryObserver: ConnectionMemoryObserver)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let optional processors =
            if Array.isEmpty processors then None else Some processors

        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = optional pipelines.ClientToBackend
                  BackendToClient = optional pipelines.BackendToClient }
            )

        run
            configuration
            (Some createSelection)
            (configuredCapacityLimits configuration)
            (Some memoryObserver)
            None
            logger
            cancellationToken

    let internal runWithOptionalProcessorPipelinesAndMemoryObserverAndTestHooksAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (memoryObserver: ConnectionMemoryObserver)
        (testHooks: TcpForwarderTestHooks)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let optional processors =
            if Array.isEmpty processors then None else Some processors

        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = optional pipelines.ClientToBackend
                  BackendToClient = optional pipelines.BackendToClient }
            )

        run
            configuration
            (Some createSelection)
            (configuredCapacityLimits configuration)
            (Some memoryObserver)
            (Some testHooks)
            logger
            cancellationToken

    let internal runWithOptionalProcessorPipelinesAndCapacityLimitsAndMemoryObserverAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (capacityLimits: ConnectionCapacityLimits)
        (memoryObserver: ConnectionMemoryObserver)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let optional processors =
            if Array.isEmpty processors then None else Some processors

        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = optional pipelines.ClientToBackend
                  BackendToClient = optional pipelines.BackendToClient }
            )

        run
            configuration
            (Some createSelection)
            capacityLimits
            (Some memoryObserver)
            None
            logger
            cancellationToken

    let internal runWithOptionalProcessorPipelinesAndCapacityLimitsAndMemoryObserverAndTestHooksAsync
        (configuration: ForwardingConfiguration)
        (processorPipelineFactory: ConnectionState -> DirectionalProcessorPipelines)
        (capacityLimits: ConnectionCapacityLimits)
        (memoryObserver: ConnectionMemoryObserver)
        (testHooks: TcpForwarderTestHooks)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        let optional processors =
            if Array.isEmpty processors then None else Some processors

        let createSelection connectionState =
            let pipelines = processorPipelineFactory connectionState

            ValueTask<DirectionalPipelineSelection>(
                { ClientToBackend = optional pipelines.ClientToBackend
                  BackendToClient = optional pipelines.BackendToClient }
            )

        run
            configuration
            (Some createSelection)
            capacityLimits
            (Some memoryObserver)
            (Some testHooks)
            logger
            cancellationToken
