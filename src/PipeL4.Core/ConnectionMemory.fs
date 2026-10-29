namespace PipeL4.Core

open System
open System.Buffers
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks

module private ConnectionMemoryException =
    let raisePreserving<'value> (error: exn) : 'value =
        ExceptionDispatchInfo.Capture(error).Throw()
        Unchecked.defaultof<'value>

    let faulted<'value> (error: exn) =
        ValueTask<'value>(Task.FromException<'value>(error))

type private ReceiveCapacityOverrunException(error: exn) =
    inherit InvalidOperationException(error.Message, error)

[<Struct; NoEquality; NoComparison>]
type internal ConnectionMemorySnapshot =
    { CurrentPhysicalBufferCapacityBytes: int64
      PeakPhysicalBufferCapacityBytes: int64 }

type internal ConnectionMemoryAccounting private
    (physicalAdmission: ICapacityAdmission, memoryDiagnostics: IConnectionMemoryDiagnostics, _marker: unit)
    as this =
    let physicalBudget = physicalAdmission
    let diagnostics =
        if obj.ReferenceEquals(memoryDiagnostics, null) then
            new NullConnectionMemoryDiagnostics() :> IConnectionMemoryDiagnostics
        else
            memoryDiagnostics

    let aggregate message primary cleanup =
        AggregateException(message, [| primary; cleanup |])

    let tryDispose (owner: IMemoryOwner<byte>) =
        try
            owner.Dispose()
            None
        with error ->
            diagnostics.CleanupFailure()
            Some error

    let tryRelease (reservation: IDisposable) =
        try
            reservation.Dispose()
            None
        with error ->
            Some error

    let recordUncertain capacity =
        if capacity > 0L then
            try
                physicalBudget.RecordUncertain capacity
            with :? InvalidOperationException ->
                ()

        physicalBudget.Close()

    let reserveSynchronously capacity =
        match physicalBudget.TryReserve capacity with
        | ValueSome reservation -> reservation
        | ValueNone ->
            if physicalBudget.IsClosed then
                raise (ObjectDisposedException(nameof CapacityBudget))

            let current = physicalBudget.GetSnapshot().CurrentBytes

            if current > Int64.MaxValue - capacity then
                invalidOp "Physical buffer capacity accounting overflowed."
            else
                invalidOp "Physical buffer capacity budget was exceeded."

    let reserveSynchronouslyForOutput capacity =
        try
            reserveSynchronously capacity
        with error ->
            diagnostics.ProgressFailure ProgressFailureCategory.SynchronousPhysicalOutputExhaustion
            raise error

    let attachRentedOwner
        (owner: IMemoryOwner<byte>)
        (requestedCapacity: int64)
        (provisional: CapacityReservation)
        (retryOverrun: bool)
        (failureCategory: ProgressFailureCategory option)
        (accounting: ConnectionMemoryAccounting)
        : IMemoryOwner<byte> =
        let actualCapacityResult =
            try
                Ok(int64 owner.Memory.Length)
            with error ->
                Error error

        let releaseProvisional primary uncertainAdditionalCapacity =
            match tryDispose owner with
            | Some cleanup ->
                if uncertainAdditionalCapacity > 0L then
                    try
                        physicalBudget.RecordUncertain uncertainAdditionalCapacity
                    with :? InvalidOperationException ->
                        ()
                else
                    physicalBudget.Close()

                raise (aggregate "Physical buffer rental and cleanup both failed." primary cleanup)
            | None ->
                match tryRelease (provisional :> IDisposable) with
                | Some cleanup -> raise (aggregate "Physical buffer rental cleanup failed." primary cleanup)
                | None when retryOverrun && uncertainAdditionalCapacity > 0L ->
                    raise (ReceiveCapacityOverrunException(primary))
                | None -> ConnectionMemoryException.raisePreserving primary

        match actualCapacityResult with
        | Error error ->
            let cleanup = physicalBudget.BeginUnknownCleanup error

            match tryDispose owner with
            | None ->
                match tryRelease (provisional :> IDisposable) with
                | Some releaseError ->
                    cleanup.CompleteSuccessfully()
                    raise (aggregate "Physical buffer rental cleanup failed." error releaseError)
                | None ->
                    cleanup.CompleteSuccessfully()
                    ConnectionMemoryException.raisePreserving error
            | Some disposeError ->
                cleanup.CompleteFailure()
                raise (aggregate "Physical buffer rental and cleanup both failed." error disposeError)
        | Ok actualCapacity when actualCapacity < requestedCapacity ->
            if retryOverrun then
                diagnostics.ReceiveReturnedOwnerCapacity actualCapacity
                diagnostics.ProgressFailure ProgressFailureCategory.PoolOwnerFitFailure

            releaseProvisional
                (InvalidOperationException(
                    "A memory pool returned less capacity than requested."
                ))
                0L
        | Ok actualCapacity ->
            if retryOverrun then
                diagnostics.ReceiveReturnedOwnerCapacity actualCapacity

            let additionalCapacity = actualCapacity - requestedCapacity

            if additionalCapacity = 0L then
                try
                    new TrackedMemoryOwner(owner, accounting, provisional :> IDisposable) :> IMemoryOwner<byte>
                with error ->
                    match tryDispose owner with
                    | Some cleanup ->
                        physicalBudget.Close()
                        raise (aggregate "Physical buffer owner construction and cleanup both failed." error cleanup)
                    | None ->
                        match tryRelease (provisional :> IDisposable) with
                        | Some cleanup ->
                            raise (aggregate "Physical buffer owner construction cleanup failed." error cleanup)
                        | None -> ConnectionMemoryException.raisePreserving error
            else
                let additionalResult =
                    try
                        Ok(physicalBudget.TryReserveExpansion additionalCapacity)
                    with error ->
                        Error error

                match additionalResult with
                | Error error ->
                    match failureCategory with
                    | Some category -> diagnostics.ProgressFailure category
                    | None -> ()

                    releaseProvisional error additionalCapacity
                | Ok(ExpansionReserved additional) ->
                    let reservation =
                        new CapacityReservationGroup(
                            [ provisional :> IDisposable
                              additional :> IDisposable ]
                        ) :> IDisposable

                    try
                        new TrackedMemoryOwner(owner, accounting, reservation) :> IMemoryOwner<byte>
                    with error ->
                        match tryDispose owner with
                        | Some cleanup ->
                            physicalBudget.Close()
                            raise (aggregate "Physical buffer owner construction and cleanup both failed." error cleanup)
                        | None ->
                            match tryRelease reservation with
                            | Some cleanup ->
                                raise (aggregate "Physical buffer owner construction cleanup failed." error cleanup)
                            | None -> ConnectionMemoryException.raisePreserving error
                | Ok(CleanupRequired cleanup) ->
                    match tryDispose owner with
                    | None ->
                        match tryRelease (provisional :> IDisposable) with
                        | Some releaseError ->
                            cleanup.CompleteSuccessfully()
                            raise (aggregate "Physical buffer rental cleanup failed." cleanup.Failure releaseError)
                        | None ->
                            cleanup.CompleteSuccessfully()
                            if retryOverrun then
                                raise (ReceiveCapacityOverrunException(cleanup.Failure))
                            else
                                diagnostics.ProgressFailure ProgressFailureCategory.SynchronousPhysicalOutputExhaustion
                                ConnectionMemoryException.raisePreserving cleanup.Failure
                    | Some disposeError ->
                        cleanup.CompleteFailure()
                        raise (aggregate "Physical buffer rental and cleanup both failed." cleanup.Failure disposeError)

    let rentAsync
        (poolRent: unit -> IMemoryOwner<byte>)
        requestedCapacity
        (cancellationToken: CancellationToken)
        retryOverrun
        failureCategory
        : ValueTask<IMemoryOwner<byte>> =
        ValueTask<IMemoryOwner<byte>>(
            task {
                let recordAdmissionFailure (error: exn) =
                    match error with
                    | :? OperationCanceledException when cancellationToken.IsCancellationRequested -> ()
                    | _ ->
                        match failureCategory with
                        | Some category -> diagnostics.ProgressFailure category
                        | None -> ()

                let reservationTask =
                    task {
                        try
                            return!
                                physicalBudget.ReserveAsync(requestedCapacity, cancellationToken).AsTask()
                        with (error: exn) ->
                            recordAdmissionFailure error

                            return raise error
                    }

                let! provisional = reservationTask
                let mutable rented = false

                try
                    cancellationToken.ThrowIfCancellationRequested()
                    let owner = poolRent ()
                    rented <- true
                    return
                        attachRentedOwner
                            owner
                            requestedCapacity
                            provisional
                            retryOverrun
                            failureCategory
                            this
                with error ->
                    if rented then
                        return ConnectionMemoryException.raisePreserving error
                    else
                        match tryRelease (provisional :> IDisposable) with
                        | Some cleanup ->
                            return raise (aggregate "Physical buffer rental admission cleanup failed." error cleanup)
                        | None -> return ConnectionMemoryException.raisePreserving error
            }
        )

    let rentReceiveAsync
        (poolRent: int -> IMemoryOwner<byte>)
        maximumCapacity
        cancellationToken
        : ValueTask<IMemoryOwner<byte>> =
        ValueTask<IMemoryOwner<byte>>(
            task {
                if maximumCapacity < 1 then
                    invalidArg (nameof maximumCapacity) "A receive capacity must be positive."

                let mutable request = maximumCapacity
                let mutable completed = false
                let mutable result = Unchecked.defaultof<IMemoryOwner<byte>>

                while not completed do
                    try
                        diagnostics.ReceiveRentalAttempt(int64 request)

                        let! owner =
                            rentAsync
                                (fun () -> poolRent request)
                                (int64 request)
                                cancellationToken
                                true
                                (Some ProgressFailureCategory.PhysicalReceiveExhaustion)
                                |> _.AsTask()

                        result <- owner
                        diagnostics.ReceiveSuccessfullyExposedOwner()
                        completed <- true
                    with
                    | :? ReceiveCapacityOverrunException when request > 1 ->
                        diagnostics.ReceiveOverRentRetry()
                        request <- max 1 (request / 2)
                    | :? ReceiveCapacityOverrunException as error ->
                        diagnostics.ProgressFailure ProgressFailureCategory.PoolOwnerFitFailure
                        ConnectionMemoryException.raisePreserving error

                return result
            }
        )

    member internal _.Acquire(capacity: int64) =
        if capacity < 0L then
            invalidArg (nameof capacity) "A buffer capacity cannot be negative."

        if not (physicalBudget.TryAcquire capacity) then
            if physicalBudget.IsClosed then
                raise (ObjectDisposedException(nameof CapacityBudget))

            let current = physicalBudget.GetSnapshot().CurrentBytes

            if current > Int64.MaxValue - capacity then
                invalidOp "Physical buffer capacity accounting overflowed."
            else
                invalidOp "Physical buffer capacity budget was exceeded."

    member internal _.Release(capacity: int64) =
        if capacity < 0L then
            invalidArg (nameof capacity) "A buffer capacity cannot be negative."

        try
            physicalBudget.Release capacity
        with :? InvalidOperationException ->
            invalidOp "Physical buffer capacity accounting became negative."

    member _.GetSnapshot() =
        let snapshot = physicalBudget.GetSnapshot()

        { CurrentPhysicalBufferCapacityBytes = snapshot.CurrentBytes
          PeakPhysicalBufferCapacityBytes = snapshot.PeakBytes }

    member _.GetAdmissionSnapshot() =
        match physicalBudget with
        | :? ICapacityHeadroom as headroom -> headroom.GetHeadroomSnapshot()
        | _ -> physicalBudget.GetSnapshot()

    member internal this.Track(owner: IMemoryOwner<byte>) : IMemoryOwner<byte> =
        if obj.ReferenceEquals(owner, null) then
            nullArg (nameof owner)

        match owner with
        | :? TrackedMemoryOwner as tracked when obj.ReferenceEquals(tracked.Accounting, this) ->
            owner
        | :? TrackedMemoryOwner ->
            invalidOp "A runtime-owned buffer lease cannot move between connections."
        | _ ->
            let capacityResult =
                try
                    Ok(int64 owner.Memory.Length)
                with error ->
                    Error error

            match capacityResult with
            | Error error ->
                let cleanup = physicalBudget.BeginUnknownCleanup error

                match tryDispose owner with
                | None ->
                    cleanup.CompleteSuccessfully()
                    ConnectionMemoryException.raisePreserving error
                | Some disposeError ->
                    cleanup.CompleteFailure()
                    raise (aggregate "Physical buffer inspection and cleanup both failed." error disposeError)
            | Ok capacity ->
                let reservation =
                    try
                        reserveSynchronously capacity
                    with error ->
                        let cleanup = physicalBudget.BeginUnadmittedCleanup(capacity, error)

                        match tryDispose owner with
                        | None ->
                            cleanup.CompleteSuccessfully()
                            ConnectionMemoryException.raisePreserving error
                        | Some disposeError ->
                            cleanup.CompleteFailure()
                            raise (aggregate "Physical buffer admission and cleanup both failed." error disposeError)

                try
                    new TrackedMemoryOwner(owner, this, reservation :> IDisposable) :> IMemoryOwner<byte>
                with error ->
                    match tryDispose owner with
                    | Some cleanup ->
                        recordUncertain capacity
                        raise (aggregate "Physical buffer owner construction and cleanup both failed." error cleanup)
                    | None ->
                        match tryRelease (reservation :> IDisposable) with
                        | Some cleanup ->
                            raise (aggregate "Physical buffer owner construction cleanup failed." error cleanup)
                        | None -> ConnectionMemoryException.raisePreserving error

    member internal this.TrackAsync
        (owner: IMemoryOwner<byte>, cancellationToken: CancellationToken)
        : ValueTask<IMemoryOwner<byte>> =
        if obj.ReferenceEquals(owner, null) then
            nullArg (nameof owner)

        match owner with
        | :? TrackedMemoryOwner as tracked when obj.ReferenceEquals(tracked.Accounting, this) ->
            ValueTask<IMemoryOwner<byte>>(owner)
        | :? TrackedMemoryOwner ->
            invalidOp "A runtime-owned buffer lease cannot move between connections."
        | _ ->
            ValueTask<IMemoryOwner<byte>>(
                task {
                    let mutable capacity: int64 option = None
                    let mutable reservation: CapacityReservation option = None

                    try
                        let actualCapacity = int64 owner.Memory.Length
                        capacity <- Some actualCapacity
                        let! acquired = physicalBudget.ReserveAsync(actualCapacity, cancellationToken).AsTask()
                        reservation <- Some acquired

                        try
                            return new TrackedMemoryOwner(owner, this, acquired :> IDisposable) :> IMemoryOwner<byte>
                        with error ->
                            match tryDispose owner with
                            | Some cleanup ->
                                recordUncertain actualCapacity
                                return raise (aggregate "Physical buffer owner construction and cleanup both failed." error cleanup)
                            | None ->
                                match tryRelease (acquired :> IDisposable) with
                                | Some cleanup ->
                                    return raise (aggregate "Physical buffer owner construction cleanup failed." error cleanup)
                                | None -> return ConnectionMemoryException.raisePreserving error
                    with error ->
                        match reservation with
                        | Some _ -> return ConnectionMemoryException.raisePreserving error
                        | None ->
                            let cleanup =
                                match capacity with
                                | Some value -> physicalBudget.BeginUnadmittedCleanup(value, error)
                                | None -> physicalBudget.BeginUnknownCleanup error

                            match tryDispose owner with
                            | None ->
                                cleanup.CompleteSuccessfully()
                                return ConnectionMemoryException.raisePreserving error
                            | Some disposeError ->
                                cleanup.CompleteFailure()
                                return raise (aggregate "Physical buffer admission and cleanup both failed." error disposeError)
                }
            )

    member internal this.Rent(pool: MemoryPool<byte>, sizeHint: int) : IMemoryOwner<byte> =
        if obj.ReferenceEquals(pool, null) then
            nullArg (nameof pool)

        if sizeHint < 0 then
            invalidArg (nameof sizeHint) "A buffer size hint cannot be negative."

        let requestedCapacity = int64 (max sizeHint 1)
        let provisional = reserveSynchronouslyForOutput requestedCapacity

        let owner =
            try
                pool.Rent(max sizeHint 1)
            with error ->
                match tryRelease (provisional :> IDisposable) with
                | Some cleanup -> raise (aggregate "Physical buffer rental admission cleanup failed." error cleanup)
                | None -> ConnectionMemoryException.raisePreserving error

        attachRentedOwner
            owner
            requestedCapacity
            provisional
            false
            (Some ProgressFailureCategory.SynchronousPhysicalOutputExhaustion)
            this

    member internal this.RentAsync
        (pool: MemoryPool<byte>, sizeHint: int, cancellationToken: CancellationToken)
        : ValueTask<IMemoryOwner<byte>> =
        if obj.ReferenceEquals(pool, null) then
            nullArg (nameof pool)

        if sizeHint < 0 then
            invalidArg (nameof sizeHint) "A buffer size hint cannot be negative."

        rentAsync
            (fun () -> pool.Rent(max sizeHint 1))
            (int64 (max sizeHint 1))
            cancellationToken
            false
            (Some ProgressFailureCategory.SynchronousPhysicalOutputExhaustion)

    member internal this.RentArrayAsync
        (pool: ArrayPool<byte>, minimumLength: int, cancellationToken: CancellationToken)
        : ValueTask<IMemoryOwner<byte>> =
        if obj.ReferenceEquals(pool, null) then
            nullArg (nameof pool)

        if minimumLength < 0 then
            invalidArg (nameof minimumLength) "A buffer length cannot be negative."

        rentAsync
            (fun () -> new ArrayPoolMemoryOwner(pool, pool.Rent(max minimumLength 1)) :> IMemoryOwner<byte>)
            (int64 (max minimumLength 1))
            cancellationToken
            false
            (Some ProgressFailureCategory.SynchronousPhysicalOutputExhaustion)

    member internal this.RentReceiveAsync
        (pool: MemoryPool<byte>, maximumCapacity: int, cancellationToken: CancellationToken)
        : ValueTask<IMemoryOwner<byte>> =
        if obj.ReferenceEquals(pool, null) then
            nullArg (nameof pool)

        if maximumCapacity < 1 then
            invalidArg (nameof maximumCapacity) "A receive capacity must be positive."

        rentReceiveAsync (fun request -> pool.Rent(request)) maximumCapacity cancellationToken

    member internal this.RentArrayReceiveAsync
        (pool: ArrayPool<byte>, maximumCapacity: int, cancellationToken: CancellationToken)
        : ValueTask<IMemoryOwner<byte>> =
        if obj.ReferenceEquals(pool, null) then
            nullArg (nameof pool)

        if maximumCapacity < 1 then
            invalidArg (nameof maximumCapacity) "A receive capacity must be positive."

        rentReceiveAsync
            (fun request -> new ArrayPoolMemoryOwner(pool, pool.Rent(request)) :> IMemoryOwner<byte>)
            maximumCapacity
            cancellationToken

    member internal this.TrackArrayCapacity(capacity: int64) : IDisposable =
        if capacity < 0L then
            invalidArg (nameof capacity) "A buffer capacity cannot be negative."

        let reservation = reserveSynchronously capacity

        try
            new TrackedCapacityLease(reservation) :> IDisposable
        with error ->
            (reservation :> IDisposable).Dispose()
            raise error

    member internal this.TrackArrayCapacityAsync
        (capacity: int64, cancellationToken: CancellationToken)
        : ValueTask<IDisposable> =
        if capacity < 0L then
            invalidArg (nameof capacity) "A buffer capacity cannot be negative."

        let admissionResult =
            try
                Ok(physicalBudget.ReserveAsync(capacity, cancellationToken))
            with error ->
                Error error

        match admissionResult with
        | Error error -> ConnectionMemoryException.faulted<IDisposable> error
        | Ok admission when admission.IsCompletedSuccessfully ->
            try
                let reservation = admission.GetAwaiter().GetResult()

                try
                    ValueTask<IDisposable>(new TrackedCapacityLease(reservation) :> IDisposable)
                with error ->
                    (reservation :> IDisposable).Dispose()
                    ConnectionMemoryException.faulted<IDisposable> error
            with error ->
                ConnectionMemoryException.faulted<IDisposable> error
        | Ok admission ->
            ValueTask<IDisposable>(
                task {
                    let! reservation = admission.AsTask()

                    try
                        return new TrackedCapacityLease(reservation) :> IDisposable
                    with error ->
                        (reservation :> IDisposable).Dispose()
                        return raise error
                }
            )

    member internal _.CloseAdmission() =
        physicalBudget.Close()

    member internal _.RecordCleanupFailure() =
        diagnostics.CleanupFailure()

    member this.Dispose() =
        physicalBudget.Close()

    new() =
        new ConnectionMemoryAccounting(
            (new CapacityBudget(Int64.MaxValue) :> ICapacityAdmission),
            Unchecked.defaultof<IConnectionMemoryDiagnostics>,
            ()
        )

    new(physicalBudget: CapacityBudget) =
        new ConnectionMemoryAccounting(
            physicalBudget :> ICapacityAdmission,
            Unchecked.defaultof<IConnectionMemoryDiagnostics>,
            ()
        )

    new(physicalAdmission: ICapacityAdmission) =
        new ConnectionMemoryAccounting(
            physicalAdmission,
            Unchecked.defaultof<IConnectionMemoryDiagnostics>,
            ()
        )

    new(physicalAdmission: ICapacityAdmission, diagnostics: IConnectionMemoryDiagnostics) =
        new ConnectionMemoryAccounting(physicalAdmission, diagnostics, ())

    interface IDisposable with
        member this.Dispose() = this.Dispose()

and private TrackedMemoryOwner
    (owner: IMemoryOwner<byte>, accounting: ConnectionMemoryAccounting, reservation: IDisposable)
    =
    let mutable disposed = 0

    member _.Accounting = accounting

    interface IMemoryOwner<byte> with
        member _.Memory = owner.Memory

        member _.Dispose() =
            if Interlocked.Exchange(&disposed, 1) = 0 then
                try
                    owner.Dispose()
                with error ->
                    // The underlying owner may have released its storage before
                    // throwing, but the runtime cannot determine that reliably.
                    // Keep the reservation accounted as active and do not retry
                    // an owner whose release state is uncertain.
                    accounting.RecordCleanupFailure()
                    accounting.CloseAdmission()
                    ConnectionMemoryException.raisePreserving error

                reservation.Dispose()

and private CapacityReservationGroup(reservations: IDisposable list) =
    let mutable released = 0

    interface IDisposable with
        member _.Dispose() =
            if Interlocked.CompareExchange(&released, 1, 0) = 0 then
                let failures = ResizeArray<exn>()

                for reservation in reservations do
                    try
                        reservation.Dispose()
                    with error ->
                        failures.Add error

                match failures.Count with
                | 0 -> ()
                | 1 -> ConnectionMemoryException.raisePreserving failures[0]
                | _ -> raise (AggregateException("Multiple physical reservations failed during cleanup.", failures))

and private ArrayPoolMemoryOwner(pool: ArrayPool<byte>, buffer: byte array) =
    let mutable returned = 0

    interface IMemoryOwner<byte> with
        member _.Memory = Memory<byte>(buffer)

        member _.Dispose() =
            if Interlocked.CompareExchange(&returned, 1, 0) = 0 then
                pool.Return(buffer, clearArray = false)

and private TrackedCapacityLease(reservation: CapacityReservation) =
    let mutable disposed = 0

    interface IDisposable with
        member _.Dispose() =
            if Interlocked.Exchange(&disposed, 1) = 0 then
                (reservation :> IDisposable).Dispose()

[<Struct; NoEquality; NoComparison>]
type internal ConnectionCapacitySnapshot =
    { ClientToBackendLogical: CapacityUsageSnapshot
      BackendToClientLogical: CapacityUsageSnapshot
      ClientToBackendPhysicalShare: CapacityUsageSnapshot
      BackendToClientPhysicalShare: CapacityUsageSnapshot
      ClientToBackendHandoffState: PhysicalShareHandoffState
      BackendToClientHandoffState: PhysicalShareHandoffState
      Physical: ConnectionMemorySnapshot }

[<Struct; NoEquality; NoComparison>]
type internal ConnectionCapacityAccountingTestHooks =
    { BeforePhysicalShareExpansion: bool -> unit
      AfterPhysicalShareExpansion: bool -> unit }

type internal ConnectionCapacityAccounting
    (
        limits: ConnectionCapacityLimits,
        ?testHooks: ConnectionCapacityAccountingTestHooks,
        ?diagnosticsEnabled: bool
    ) =
    do
        if obj.ReferenceEquals(limits, null) then
            nullArg (nameof limits)

    let diagnosticsEnabled = defaultArg diagnosticsEnabled true
    let clientToBackendDiagnostics = DirectionRuntimeDiagnostics(diagnosticsEnabled)
    let backendToClientDiagnostics = DirectionRuntimeDiagnostics(diagnosticsEnabled)
    let clientToBackendLogical =
        new CapacityBudget(limits.ClientToBackendLogicalBytes, diagnosticsEnabled = diagnosticsEnabled)
    let backendToClientLogical =
        new CapacityBudget(limits.BackendToClientLogicalBytes, diagnosticsEnabled = diagnosticsEnabled)
    let physicalBudget = new CapacityBudget(limits.ConnectionPhysicalBytes, diagnosticsEnabled = diagnosticsEnabled)
    let directionalLimit =
        if limits.ConnectionPhysicalBytes = Int64.MaxValue then
            Int64.MaxValue
        else
            limits.ConnectionPhysicalBytes / 2L
    let clientToBackendPhysicalBudget = new CapacityBudget(directionalLimit, diagnosticsEnabled = diagnosticsEnabled)
    let backendToClientPhysicalBudget = new CapacityBudget(directionalLimit, diagnosticsEnabled = diagnosticsEnabled)
    let physical = new ConnectionMemoryAccounting(physicalBudget :> ICapacityAdmission)
    let clientToBackendPhysical =
        new ConnectionMemoryAccounting(
            new CompositeCapacityAdmission(
                physicalBudget :> ICapacityAdmission,
                clientToBackendPhysicalBudget :> ICapacityAdmission
            ),
            clientToBackendDiagnostics.AsMemoryDiagnostics()
        )
    let backendToClientPhysical =
        new ConnectionMemoryAccounting(
            new CompositeCapacityAdmission(
                physicalBudget :> ICapacityAdmission,
                backendToClientPhysicalBudget :> ICapacityAdmission
            ),
            backendToClientDiagnostics.AsMemoryDiagnostics()
        )
    let testHooks =
        defaultArg
            testHooks
            { BeforePhysicalShareExpansion = ignore
              AfterPhysicalShareExpansion = ignore }
    // 0 = open, 1 = handoff committed/in progress, 2 = completed, 3 = failed.
    let mutable clientToBackendHandoffState = 0
    let mutable backendToClientHandoffState = 0
    let mutable disposed = 0

    new() = new ConnectionCapacityAccounting(ConnectionCapacityLimits.Unbounded)

    member _.ClientToBackendLogical = clientToBackendLogical
    member _.BackendToClientLogical = backendToClientLogical
    member _.Physical = physical
    member _.ClientToBackendPhysical = clientToBackendPhysical
    member _.BackendToClientPhysical = backendToClientPhysical
    member _.ClientToBackendDiagnostics = clientToBackendDiagnostics
    member _.BackendToClientDiagnostics = backendToClientDiagnostics
    member _.DiagnosticsEnabled = diagnosticsEnabled
    member _.ClientToBackendPhysicalShareSnapshot = clientToBackendPhysicalBudget.GetSnapshot()
    member _.BackendToClientPhysicalShareSnapshot = backendToClientPhysicalBudget.GetSnapshot()
    member _.PhysicalCapacitySnapshot = physicalBudget.GetSnapshot()

    member _.CompletePhysicalDirection(clientToBackend: bool) =
        let shouldComplete =
            if clientToBackend then
                Interlocked.CompareExchange(&clientToBackendHandoffState, 1, 0) = 0
            else
                Interlocked.CompareExchange(&backendToClientHandoffState, 1, 0) = 0

        if shouldComplete then
            try
                testHooks.BeforePhysicalShareExpansion clientToBackend

                if clientToBackend then
                    clientToBackendPhysicalBudget.Dispose()
                    backendToClientPhysicalBudget.IncreaseLimit limits.ConnectionPhysicalBytes
                else
                    backendToClientPhysicalBudget.Dispose()
                    clientToBackendPhysicalBudget.IncreaseLimit limits.ConnectionPhysicalBytes

                testHooks.AfterPhysicalShareExpansion clientToBackend

                if clientToBackend then
                    Volatile.Write(&clientToBackendHandoffState, 2)
                else
                    Volatile.Write(&backendToClientHandoffState, 2)
            with error ->
                if clientToBackend then
                    Volatile.Write(&clientToBackendHandoffState, 3)
                else
                    Volatile.Write(&backendToClientHandoffState, 3)
                raise error

    member _.GetPhysicalDirectionHandoffState(clientToBackend: bool) =
        let state =
            if clientToBackend then
                Volatile.Read(&clientToBackendHandoffState)
            else
                Volatile.Read(&backendToClientHandoffState)

        match state with
        | 0 -> PhysicalShareHandoffState.Open
        | 1 -> PhysicalShareHandoffState.Committed
        | 2 -> PhysicalShareHandoffState.Completed
        | _ -> PhysicalShareHandoffState.Failed

    member _.IsPhysicalDirectionCompleted(clientToBackend: bool) =
        match
            if clientToBackend then
                Volatile.Read(&clientToBackendHandoffState)
            else
                Volatile.Read(&backendToClientHandoffState)
        with
        | 2 -> true
        | _ -> false

    member _.GetSnapshot() =
        { ClientToBackendLogical = clientToBackendLogical.GetSnapshot()
          BackendToClientLogical = backendToClientLogical.GetSnapshot()
          ClientToBackendPhysicalShare = clientToBackendPhysicalBudget.GetSnapshot()
          BackendToClientPhysicalShare = backendToClientPhysicalBudget.GetSnapshot()
          ClientToBackendHandoffState =
              match Volatile.Read(&clientToBackendHandoffState) with
              | 0 -> PhysicalShareHandoffState.Open
              | 1 -> PhysicalShareHandoffState.Committed
              | 2 -> PhysicalShareHandoffState.Completed
              | _ -> PhysicalShareHandoffState.Failed
          BackendToClientHandoffState =
              match Volatile.Read(&backendToClientHandoffState) with
              | 0 -> PhysicalShareHandoffState.Open
              | 1 -> PhysicalShareHandoffState.Committed
              | 2 -> PhysicalShareHandoffState.Completed
              | _ -> PhysicalShareHandoffState.Failed
          Physical = physical.GetSnapshot() }

    member this.Dispose() =
        if Interlocked.Exchange(&disposed, 1) = 0 then
            clientToBackendLogical.Dispose()
            backendToClientLogical.Dispose()
            physicalBudget.Dispose()
            clientToBackendPhysicalBudget.Dispose()
            backendToClientPhysicalBudget.Dispose()

    interface IDisposable with
        member this.Dispose() = this.Dispose()
