namespace PipeL4.Core

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

[<Struct; NoEquality; NoComparison>]
type internal CapacityUsageSnapshot =
    { LimitBytes: int64
      CurrentBytes: int64
      PeakBytes: int64
      CurrentWaiterCount: int64
      PeakWaiterCount: int64
      QueuedWaitCount: int64
      TotalWaitDuration: TimeSpan
      MaximumWaitDuration: TimeSpan }

/// A committed amount of capacity. The release callback is deliberately owned
/// by the budget that issued the reservation so a reservation can move between
/// runtime components without moving the accounting domain itself.
type internal CapacityReservation(amount: int64, release: int64 -> unit) =
    do
        if amount < 0L then
            invalidArg (nameof amount) "A capacity reservation cannot be negative."

    let mutable remaining = amount
    let mutable fullyReleased = 0

    let takePartial amountToRelease throwIfEmpty =
        if amountToRelease < 0L || amountToRelease > amount then
            invalidArg (nameof amountToRelease) "A capacity reservation release is outside the reservation."

        let rec loop () =
            if Volatile.Read(&fullyReleased) <> 0 then
                if throwIfEmpty then
                    invalidOp "A capacity reservation was released more than once."
                else
                    0L
            else
                let current = Volatile.Read(&remaining)
                if amountToRelease > current then
                    if throwIfEmpty then
                        invalidOp "A capacity reservation was released more than once."
                    else
                        0L
                else
                    let updated = current - amountToRelease

                    if Interlocked.CompareExchange(&remaining, updated, current) = current then
                        amountToRelease
                    else
                        loop ()

        loop ()

    member _.Amount = amount
    member _.RemainingAmount = Volatile.Read(&remaining)

    member this.ReleasePartial(amountToRelease: int64) =
        let released = takePartial amountToRelease true

        if released > 0L then
            release released

    member this.Split(amountToSplit: int64) =
        if amountToSplit <= 0L || amountToSplit > amount then
            invalidArg (nameof amountToSplit) "A reservation split must be a positive amount within the reservation."

        let splitAmount = takePartial amountToSplit true
        new CapacityReservation(splitAmount, release)

    member this.Release() =
        if Interlocked.CompareExchange(&fullyReleased, 1, 0) <> 0 then
            invalidOp "A capacity reservation was released more than once."

        let released = Interlocked.Exchange(&remaining, 0L)

        if released > 0L then
            release released

    interface IDisposable with
        member _.Dispose() =
            if Interlocked.CompareExchange(&fullyReleased, 1, 0) <> 0 then
                ()
            else
                let released = Interlocked.Exchange(&remaining, 0L)

                if released > 0L then
                    release released

type internal CapacityCleanup
    (failure: exn, completeSuccess: unit -> unit, completeFailure: unit -> unit)
    =
    let mutable completed = 0

    member _.Failure = failure

    member _.CompleteSuccessfully() =
        if Interlocked.CompareExchange(&completed, 1, 0) = 0 then
            completeSuccess ()
        else
            invalidOp "A capacity cleanup was completed more than once."

    member _.CompleteFailure() =
        if Interlocked.CompareExchange(&completed, 1, 0) = 0 then
            completeFailure ()
        else
            invalidOp "A capacity cleanup was completed more than once."

[<NoEquality; NoComparison>]
type internal CapacityExpansionResult =
    | ExpansionReserved of CapacityReservation
    | CleanupRequired of CapacityCleanup

type internal ICapacityAdmission =
    abstract LimitBytes: int64
    abstract IsClosed: bool
    abstract TryAcquire: amount: int64 -> bool
    abstract TryReserve: amount: int64 -> CapacityReservation voption
    abstract TryReserveExpansion: amount: int64 -> CapacityExpansionResult
    abstract BeginUnknownCleanup: error: exn -> CapacityCleanup
    abstract BeginUnadmittedCleanup: amount: int64 * error: exn -> CapacityCleanup
    abstract RecordUncertain: amount: int64 -> unit
    abstract ReserveAsync: amount: int64 * cancellationToken: CancellationToken -> ValueTask<CapacityReservation>
    abstract Release: amount: int64 -> unit
    abstract GetSnapshot: unit -> CapacityUsageSnapshot
    abstract Close: unit -> unit

type internal ICapacityHeadroom =
    abstract GetHeadroomSnapshot: unit -> CapacityUsageSnapshot

type private CapacityWaiter(amount: int64, cancellationToken: CancellationToken, queuedAt: int64) =
    let completion =
        TaskCompletionSource<CapacityReservation>(TaskCreationOptions.RunContinuationsAsynchronously)

    let mutable completed = 0
    let mutable registration = Unchecked.defaultof<CancellationTokenRegistration>
    let mutable hasRegistration = false

    member _.Amount = amount
    member _.Task = completion.Task
    member _.CancellationToken = cancellationToken
    member _.QueuedAt = queuedAt

    member _.IsWaiting = Volatile.Read(&completed) = 0

    member _.MarkCompleted() =
        Volatile.Write(&completed, 1)

    member _.SetRegistration(value: CancellationTokenRegistration) =
        registration <- value
        hasRegistration <- true

    member _.TakeRegistration() =
        if hasRegistration then
            hasRegistration <- false
            ValueSome registration
        else
            ValueNone

    member _.TrySetResult(value: CapacityReservation) = completion.TrySetResult(value) |> ignore
    member _.TrySetCanceled(token: CancellationToken) = completion.TrySetCanceled(token) |> ignore
    member _.TrySetException(error: exn) = completion.TrySetException(error) |> ignore

[<Struct; NoEquality; NoComparison>]
type internal CapacityBudgetTestHooks =
    { BeforeAdmissionCommit: unit -> unit
      BeforeWaiterEnqueue: unit -> unit
      AfterWaiterCountIncrement: unit -> unit
      BeforeMaximumWaitDurationUpdate: unit -> unit
      AfterReleaseCore: unit -> unit
      BeforeCleanupFailureTransition: unit -> unit }

[<RequireQualifiedAccess>]
type internal PhysicalShareHandoffState =
    | Open
    | Committed
    | Completed
    | Failed

type private CapacityAdmissionState =
    | Open
    | CleanupPending of int
    | ClosedExact
    | ClosedSaturated of pendingOverflowCleanups: int * uncertainRelease: bool

[<Sealed>]
type internal ConnectionCapacityLimits
    (
        clientToBackendLogicalBytes: int64,
        backendToClientLogicalBytes: int64,
        connectionPhysicalBytes: int64
    ) =
    let validate name value =
        if value < 0L then
            invalidArg name "A capacity limit cannot be negative."

    do
        validate (nameof clientToBackendLogicalBytes) clientToBackendLogicalBytes
        validate (nameof backendToClientLogicalBytes) backendToClientLogicalBytes
        validate (nameof connectionPhysicalBytes) connectionPhysicalBytes

    member _.ClientToBackendLogicalBytes = clientToBackendLogicalBytes
    member _.BackendToClientLogicalBytes = backendToClientLogicalBytes
    member _.ConnectionPhysicalBytes = connectionPhysicalBytes

    static member Unbounded =
        ConnectionCapacityLimits(Int64.MaxValue, Int64.MaxValue, Int64.MaxValue)

type internal CapacityBudget(limitBytes: int64, ?testHooks: CapacityBudgetTestHooks, ?diagnosticsEnabled: bool) as this =
    do
        if limitBytes < 0L then
            invalidArg (nameof limitBytes) "A capacity limit cannot be negative."

    let gate = obj ()
    let waiters = ResizeArray<CapacityWaiter>()
    let testHooks =
        defaultArg
            testHooks
            { BeforeAdmissionCommit = ignore
              BeforeWaiterEnqueue = ignore
              AfterWaiterCountIncrement = ignore
              BeforeMaximumWaitDurationUpdate = ignore
              AfterReleaseCore = ignore
              BeforeCleanupFailureTransition = ignore }
    let diagnosticsEnabled = defaultArg diagnosticsEnabled true
    let mutable limitBytes = limitBytes
    let mutable currentBytes = 0L
    let mutable peakBytes = 0L
    let mutable waiterCount = 0
    let mutable peakWaiterCount = 0
    let mutable queuedWaitCount = 0L
    let mutable totalWaitDurationTicks = 0L
    let mutable maximumWaitDurationTicks = 0L
    // Every state transition is published as one atomic reference.  The
    // positive cleanup count is temporary admission blocking; either closed
    // state is terminal and can never reopen.
    let mutable admissionState = Open

    let validateAmount amount =
        if amount < 0L then
            invalidArg (nameof amount) "A capacity reservation cannot be negative."

    let readAdmissionState () = Volatile.Read(&admissionState)

    let publishAdmissionState state = Volatile.Write(&admissionState, state)

    let isClosed = function
        | ClosedExact
        | ClosedSaturated _ -> true
        | Open
        | CleanupPending _ -> false

    let ensureNotClosed () =
        if isClosed (readAdmissionState ()) then
            raise (ObjectDisposedException(nameof CapacityBudget))

    let updatePeak updated =
        let rec publishPeak () =
            let peak = Volatile.Read(&peakBytes)

            if updated <= peak then
                ()
            elif Interlocked.CompareExchange(&peakBytes, updated, peak) <> peak then
                publishPeak ()

        publishPeak ()

    let tryAcquireCore amount =
        let rec acquire () =
            let current = Volatile.Read(&currentBytes)

            if amount > limitBytes - current then
                false
            else
                let updated = current + amount

                if Interlocked.CompareExchange(&currentBytes, updated, current) = current then
                    updatePeak updated
                    true
                else
                    acquire ()

        acquire ()

    let releaseCore amount =
        let rec release () =
            let current = Volatile.Read(&currentBytes)

            if amount > current then
                invalidOp "A capacity release would make usage negative."

            let updated = current - amount

            if Interlocked.CompareExchange(&currentBytes, updated, current) <> current then
                release ()

        release ()

    let tryAddUsageCore amount =
        let rec add () =
            let current = Volatile.Read(&currentBytes)

            let updated =
                try
                    ValueSome(Checked.(+) current amount)
                with :? OverflowException ->
                    ValueNone

            match updated with
            | ValueNone -> false
            | ValueSome updated when Interlocked.CompareExchange(&currentBytes, updated, current) = current ->
                updatePeak updated
                true
            | ValueSome _ -> add ()

        add ()

    let createReservation amount =
        new CapacityReservation(amount, fun released -> this.Release released)

    let recordWaitCompletion (waiter: CapacityWaiter) =
        if diagnosticsEnabled && waiter.QueuedAt <> 0L then
            let elapsed = Stopwatch.GetElapsedTime(waiter.QueuedAt).Ticks

            let currentTotal = Volatile.Read(&totalWaitDurationTicks)
            let updatedTotal =
                if elapsed > Int64.MaxValue - currentTotal then
                    Int64.MaxValue
                else
                    currentTotal + elapsed

            Interlocked.Exchange(&totalWaitDurationTicks, updatedTotal) |> ignore
            testHooks.BeforeMaximumWaitDurationUpdate()

            let rec updateMaximum () =
                let current = Volatile.Read(&maximumWaitDurationTicks)

                if elapsed <= current then
                    ()
                elif Interlocked.CompareExchange(&maximumWaitDurationTicks, elapsed, current) <> current then
                    updateMaximum ()

            updateMaximum ()

    let markWaiterCompletedLocked (waiter: CapacityWaiter) =
        waiter.MarkCompleted()
        waiterCount <- waiterCount - 1
        recordWaitCompletion waiter

    let removeWaiterLocked (waiter: CapacityWaiter) =
        if waiter.IsWaiting && waiters.Remove waiter then
            markWaiterCompletedLocked waiter
            true
        else
            false

    let dispatchWaitersLocked () =
        let completions = ResizeArray<CapacityWaiter * CapacityReservation * CancellationTokenRegistration voption>()
        let mutable index = 0

        while (match readAdmissionState () with | Open -> true | _ -> false) && index < waiters.Count do
            let waiter = waiters[index]

            if waiter.IsWaiting && tryAcquireCore waiter.Amount then
                waiters.RemoveAt index
                markWaiterCompletedLocked waiter
                completions.Add(waiter, createReservation waiter.Amount, waiter.TakeRegistration())
            else
                index <- index + 1

        completions

    let closeWaitersLocked () =
        let cancelled = ResizeArray<CapacityWaiter * CancellationTokenRegistration voption>()

        for waiter in waiters do
            markWaiterCompletedLocked waiter
            cancelled.Add(waiter, waiter.TakeRegistration())

        waiters.Clear()
        cancelled

    let completeClosedWaiters (cancelled: ResizeArray<CapacityWaiter * CancellationTokenRegistration voption>) =
        for waiter, registration in cancelled do
            match registration with
            | ValueSome value -> value.Dispose()
            | ValueNone -> ()

            waiter.TrySetException(ObjectDisposedException(nameof CapacityBudget))

    let overflowFailure error =
        InvalidOperationException("Physical buffer capacity accounting overflowed.", error) :> exn

    let updateOverflowLocked pendingDelta uncertainRelease =
        match readAdmissionState () with
        | ClosedSaturated(pending, uncertain) ->
            let pending =
                try
                    Checked.(+) pending pendingDelta
                with :? OverflowException ->
                    Int32.MaxValue

            publishAdmissionState (ClosedSaturated(pending, uncertain || uncertainRelease))
        | _ -> publishAdmissionState (ClosedSaturated(pendingDelta, uncertainRelease))

        updatePeak Int64.MaxValue

    let closeForOverflowLocked pendingDelta uncertainRelease =
        updateOverflowLocked pendingDelta uncertainRelease
        closeWaitersLocked ()

    let beginCleanupPendingLocked () =
        match readAdmissionState () with
        | Open ->
                publishAdmissionState (CleanupPending 1)
                ResizeArray()
        | CleanupPending count when count < Int32.MaxValue ->
                publishAdmissionState (CleanupPending(count + 1))
                ResizeArray()
        | CleanupPending _ ->
                // A saturated cleanup count cannot be drained reliably.  Close
                // the domain instead of leaving waiters suspended forever.
                publishAdmissionState (ClosedSaturated(0, true))
                closeWaitersLocked ()
        | ClosedExact
        | ClosedSaturated _ -> ResizeArray()

    let beginCleanupLocked amount error =
        let cancelled = beginCleanupPendingLocked ()

        if tryAddUsageCore amount then
            let cleanup =
                new CapacityCleanup(
                    error,
                    (fun () -> this.CompleteCleanupSuccessfully(amount, false)),
                    (fun () -> this.CompleteCleanupFailure(false))
                )

            cleanup, cancelled
        else
            let overflowCancelled = closeForOverflowLocked 1 false

            for waiter in overflowCancelled do
                cancelled.Add waiter

            let cleanup =
                new CapacityCleanup(
                    overflowFailure error,
                    (fun () -> this.CompleteCleanupSuccessfully(0L, true)),
                    (fun () -> this.CompleteCleanupFailure(true))
                )

            cleanup, cancelled

    let beginUnknownCleanupLocked error =
        let cancelled = beginCleanupPendingLocked ()

        let cleanup =
            new CapacityCleanup(
                error,
                (fun () -> this.CompleteCleanupSuccessfully(0L, false)),
                (fun () -> this.CompleteUnknownCleanupFailure())
            )

        cleanup, cancelled

    let beginUnadmittedCleanupLocked amount error =
        let cancelled = beginCleanupPendingLocked ()

        let cleanup =
            new CapacityCleanup(
                error,
                (fun () -> this.CompleteCleanupSuccessfully(0L, false)),
                (fun () -> this.CompleteKnownCleanupFailure amount)
            )

        cleanup, cancelled

    let hasFittingWaiterLocked () =
        let available = limitBytes - Volatile.Read(&currentBytes)
        waiters |> Seq.exists (fun waiter -> waiter.IsWaiting && waiter.Amount <= available)

    let completeSuccesses
        (completions: ResizeArray<CapacityWaiter * CapacityReservation * CancellationTokenRegistration voption>)
        =
        for waiter, reservation, registration in completions do
            match registration with
            | ValueSome value -> value.Dispose()
            | ValueNone -> ()

            waiter.TrySetResult reservation

    member _.LimitBytes = Volatile.Read(&limitBytes)

    member internal _.IsClosed = isClosed (readAdmissionState ())

    member internal _.TryAcquire(amount: int64) =
        validateAmount amount
        ensureNotClosed ()

        let initialState = readAdmissionState ()
        let initialWaiterCount = Volatile.Read(&waiterCount)

        if (match initialState with | Open -> true | _ -> false) && initialWaiterCount = 0 then
            testHooks.BeforeAdmissionCommit()

        lock gate (fun () ->
            ensureNotClosed ()

            if (match readAdmissionState () with | Open -> true | _ -> false)
               && waiterCount = 0 then
                tryAcquireCore amount
            else
                false)

    member internal _.TryReserve(amount: int64) : CapacityReservation voption =
        validateAmount amount
        ensureNotClosed ()

        if amount > limitBytes then
            ValueNone
        else
            let initialState = readAdmissionState ()
            let initialWaiterCount = Volatile.Read(&waiterCount)

            if (match initialState with | Open -> true | _ -> false) && initialWaiterCount = 0 then
                testHooks.BeforeAdmissionCommit()

            lock gate (fun () ->
                ensureNotClosed ()

                if (match readAdmissionState () with | Open -> true | _ -> false)
                   && waiterCount = 0
                   && tryAcquireCore amount then
                    ValueSome(createReservation amount)
                else
                    ValueNone)

    /// Expands an already admitted reservation without registering a new waiter.
    /// The caller already owns provisional physical credit, so this is only used
    /// to reconcile the actual size of storage it has acquired.  A queued waiter
    /// that already fits is left ahead of the expansion.  When reconciliation
    /// cannot proceed, the returned cleanup handle keeps the additional capacity
    /// charged and blocks admission until the owner has been disposed.
    member internal _.TryReserveExpansion(amount: int64) : CapacityExpansionResult =
        validateAmount amount

        let result, cancelled =
            lock gate (fun () ->
                let state = readAdmissionState ()

                match state with
                | Open when not (hasFittingWaiterLocked ()) && tryAcquireCore amount ->
                    ExpansionReserved(createReservation amount), ResizeArray()
                | ClosedExact
                | ClosedSaturated _ ->
                    let cleanup, cancelled =
                        beginCleanupLocked amount (ObjectDisposedException(nameof CapacityBudget))

                    CleanupRequired cleanup, cancelled
                | Open ->
                    let cleanup, cancelled =
                        beginCleanupLocked
                            amount
                            (InvalidOperationException(
                                "The actual pooled buffer capacity exceeded the remaining physical budget."
                            ))

                    CleanupRequired cleanup, cancelled
                | CleanupPending _ ->
                    let cleanup, cancelled =
                        beginCleanupLocked
                            amount
                            (InvalidOperationException(
                                "The actual pooled buffer capacity could not be admitted while cleanup was blocked."
                            ))

                    CleanupRequired cleanup, cancelled
                )

        completeClosedWaiters cancelled
        result

    /// Registers cleanup after capacity inspection failed.  No amount can be
    /// charged because the owner's actual size is unknown, so admission is
    /// blocked by CleanupPending until disposal resolves.  A failed disposal
    /// upgrades the domain to saturated terminal accounting.
    member internal _.BeginUnknownCleanup(error: exn) : CapacityCleanup =
        let cleanup, cancelled = lock gate (fun () -> beginUnknownCleanupLocked error)
        completeClosedWaiters cancelled
        cleanup

    /// Registers cleanup for a supplied owner whose known capacity could not
    /// be admitted.  The owner is already acquired, so admission is blocked
    /// while it is disposed without inflating accounting unless disposal
    /// fails.  A failed disposal then charges the known capacity and closes
    /// exact admission.
    member internal _.BeginUnadmittedCleanup(amount: int64, error: exn) : CapacityCleanup =
        validateAmount amount

        let cleanup, cancelled = lock gate (fun () -> beginUnadmittedCleanupLocked amount error)
        completeClosedWaiters cancelled
        cleanup

    member internal _.RecordUncertain(amount: int64) =
        validateAmount amount
        let cancelled =
            lock gate (fun () ->
                // Publish the terminal exact state before changing the charge.
                // This closes lock-free admission for the whole uncertain-release
                // transition; an overflow can then replace it with saturation
                // without exposing an open intermediate state.
                match readAdmissionState () with
                | ClosedSaturated _ -> ()
                | Open
                | CleanupPending _
                | ClosedExact -> publishAdmissionState ClosedExact

                let cancelled = closeWaitersLocked ()

                if tryAddUsageCore amount then
                    cancelled
                else
                    updateOverflowLocked 0 true
                    cancelled)

        completeClosedWaiters cancelled

    member internal _.RecordUnknown() =
        let cancelled =
            lock gate (fun () ->
                updateOverflowLocked 0 true
                closeWaitersLocked ())

        completeClosedWaiters cancelled

    member private _.CompleteCleanupSuccessfully(amount: int64, overflowPending: bool) =
        let completions =
            lock gate (fun () ->
                if overflowPending then
                    match readAdmissionState () with
                    | ClosedSaturated(pending, uncertain) when pending > 1 ->
                        publishAdmissionState (ClosedSaturated(pending - 1, uncertain))
                    | ClosedSaturated(_, true) ->
                        publishAdmissionState (ClosedSaturated(0, true))
                    | ClosedSaturated(_, false) ->
                        publishAdmissionState ClosedExact
                    | _ -> ()

                    ResizeArray()
                else
                    releaseCore amount

                    match readAdmissionState () with
                    | CleanupPending count when count > 1 ->
                        publishAdmissionState (CleanupPending(count - 1))
                        ResizeArray()
                    | CleanupPending _ ->
                        publishAdmissionState Open
                        dispatchWaitersLocked ()
                    | Open
                    | ClosedExact
                    | ClosedSaturated _ ->
                        ResizeArray())

        completeSuccesses completions

    member private _.CompleteCleanupFailure(overflowPending: bool) =
        let cancelled =
            lock gate (fun () ->
                if overflowPending then
                    updateOverflowLocked 0 true
                    ResizeArray()
                else
                    match readAdmissionState () with
                    | ClosedExact
                    | ClosedSaturated _ -> ResizeArray()
                    | Open
                    | CleanupPending _ ->
                        testHooks.BeforeCleanupFailureTransition()
                        publishAdmissionState ClosedExact
                        closeWaitersLocked ())

        completeClosedWaiters cancelled

    member private _.CompleteUnknownCleanupFailure() =
        let cancelled =
            lock gate (fun () ->
                match readAdmissionState () with
                | ClosedSaturated _ ->
                    closeWaitersLocked ()
                | Open
                | CleanupPending _
                | ClosedExact ->
                    testHooks.BeforeCleanupFailureTransition()
                    updateOverflowLocked 0 true
                    closeWaitersLocked ())

        completeClosedWaiters cancelled

    member private _.CompleteKnownCleanupFailure(amount: int64) =
        let cancelled =
            lock gate (fun () ->
                match readAdmissionState () with
                | ClosedSaturated _ ->
                    closeWaitersLocked ()
                | ClosedExact ->
                    if tryAddUsageCore amount then
                        closeWaitersLocked ()
                    else
                        updateOverflowLocked 0 true
                        closeWaitersLocked ()
                | Open
                | CleanupPending _ ->
                    testHooks.BeforeCleanupFailureTransition()
                    publishAdmissionState ClosedExact

                    if tryAddUsageCore amount then
                        closeWaitersLocked ()
                    else
                        updateOverflowLocked 0 true
                        closeWaitersLocked ())

        completeClosedWaiters cancelled

    member internal _.ReserveAsync(amount: int64, cancellationToken: CancellationToken) : ValueTask<CapacityReservation> =
        validateAmount amount

        if amount > limitBytes then
            raise (ArgumentOutOfRangeException(nameof amount, amount, "The requested capacity exceeds the budget limit."))

        let reserve () =
            ensureNotClosed ()
            cancellationToken.ThrowIfCancellationRequested()

            let initialState = readAdmissionState ()
            let initialWaiterCount = Volatile.Read(&waiterCount)

            if (match initialState with | Open -> true | _ -> false) && initialWaiterCount = 0 then
                testHooks.BeforeAdmissionCommit()

            let mutable waiterResult: CapacityWaiter option = None
            let mutable immediateResult: CapacityReservation voption = ValueNone
            let mutable registrationToDispose: CancellationTokenRegistration voption = ValueNone
            let mutable completions: ResizeArray<CapacityWaiter * CapacityReservation * CancellationTokenRegistration voption> voption =
                ValueNone

            lock gate (fun () ->
                ensureNotClosed ()
                cancellationToken.ThrowIfCancellationRequested()

                if (match readAdmissionState () with | Open -> true | _ -> false)
                   && waiterCount = 0
                   && tryAcquireCore amount then
                    immediateResult <- ValueSome(createReservation amount)
                else
                    let queuedAt = if diagnosticsEnabled then Stopwatch.GetTimestamp() else 0L
                    let waiter = CapacityWaiter(amount, cancellationToken, queuedAt)
                    testHooks.BeforeWaiterEnqueue()
                    waiters.Add waiter
                    waiterCount <- waiterCount + 1
                    testHooks.AfterWaiterCountIncrement()

                    if diagnosticsEnabled then
                        if queuedWaitCount < Int64.MaxValue then
                            queuedWaitCount <- queuedWaitCount + 1L

                        peakWaiterCount <- max peakWaiterCount waiterCount

                    waiterResult <- Some waiter

                    completions <- ValueSome(dispatchWaitersLocked ()))

            match waiterResult with
            | Some waiter ->
                try
                    let registration =
                        cancellationToken.Register(Action(fun () -> this.CancelWaiter waiter))

                    lock gate (fun () ->
                        if waiter.IsWaiting then
                            waiter.SetRegistration registration
                        else
                            registrationToDispose <- ValueSome registration)
                with error ->
                    let removed =
                        lock gate (fun () -> removeWaiterLocked waiter)

                    if removed then
                        waiter.TrySetException error
            | None -> ()

            match registrationToDispose with
            | ValueSome registration -> registration.Dispose()
            | ValueNone -> ()

            match completions with
            | ValueSome values -> completeSuccesses values
            | ValueNone -> ()

            match immediateResult, waiterResult with
            | ValueSome reservation, _ ->
                // The reservation was committed while holding gate.  A later
                // terminal transition does not revoke that already-owned
                // capacity; it only closes future admission.
                ValueTask<CapacityReservation>(reservation)
            | ValueNone, Some waiter -> ValueTask<CapacityReservation>(waiter.Task)
            | ValueNone, None ->
                invalidOp "Capacity admission did not produce a reservation or waiter."

        reserve ()

    member internal _.Release(amount: int64) =
        validateAmount amount
        releaseCore amount
        testHooks.AfterReleaseCore()

        let completions =
            lock gate (fun () ->
                if (match readAdmissionState () with | Open -> true | _ -> false) then
                    dispatchWaitersLocked ()
                else
                    ResizeArray())

        completeSuccesses completions

    member internal _.IncreaseLimit(newLimit: int64) =
        if newLimit < 0L then
            invalidArg (nameof newLimit) "A capacity limit cannot be negative."

        let completions =
            lock gate (fun () ->
                if newLimit > limitBytes then
                    limitBytes <- newLimit

                if (match readAdmissionState () with | Open -> true | _ -> false) then
                    dispatchWaitersLocked ()
                else
                    ResizeArray())

        completeSuccesses completions

    member _.GetSnapshot() =
        lock gate (fun () ->
            let current = Volatile.Read(&currentBytes)
            let peak = Volatile.Read(&peakBytes)
            let saturated =
                match readAdmissionState () with
                | ClosedSaturated(pending, uncertain) -> pending > 0 || uncertain
                | Open
                | CleanupPending _
                | ClosedExact -> false

            let current = if saturated then Int64.MaxValue else current

            { LimitBytes = Volatile.Read(&limitBytes)
              CurrentBytes = current
              PeakBytes = max current peak
              CurrentWaiterCount = if diagnosticsEnabled then int64 waiterCount else 0L
              PeakWaiterCount = if diagnosticsEnabled then int64 peakWaiterCount else 0L
              QueuedWaitCount = if diagnosticsEnabled then queuedWaitCount else 0L
              TotalWaitDuration =
                  if diagnosticsEnabled then
                      TimeSpan.FromTicks totalWaitDurationTicks
                  else
                      TimeSpan.Zero
              MaximumWaitDuration =
                  if diagnosticsEnabled then
                      TimeSpan.FromTicks maximumWaitDurationTicks
                  else
                      TimeSpan.Zero })

    member private _.CancelWaiter(waiter: CapacityWaiter) =
        let registration, cancelled =
            lock gate (fun () ->
                if removeWaiterLocked waiter then
                    waiter.TakeRegistration(), true
                else
                    ValueNone, false)

        if cancelled then
            match registration with
            | ValueSome value -> value.Dispose()
            | ValueNone -> ()

            waiter.TrySetCanceled waiter.CancellationToken

    member this.Dispose() =
        let cancelled =
            lock gate (fun () ->
                match readAdmissionState () with
                | ClosedExact
                | ClosedSaturated _ -> ResizeArray()
                | Open
                | CleanupPending _ ->
                    publishAdmissionState ClosedExact
                    closeWaitersLocked ())

        completeClosedWaiters cancelled

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    interface ICapacityAdmission with
        member _.LimitBytes = Volatile.Read(&limitBytes)
        member _.IsClosed = isClosed (readAdmissionState ())
        member this.TryAcquire(amount) = this.TryAcquire(amount)
        member this.TryReserve(amount) = this.TryReserve(amount)
        member this.TryReserveExpansion(amount) = this.TryReserveExpansion(amount)
        member this.BeginUnknownCleanup(error) = this.BeginUnknownCleanup(error)
        member this.BeginUnadmittedCleanup(amount, error) = this.BeginUnadmittedCleanup(amount, error)
        member this.RecordUncertain(amount) = this.RecordUncertain(amount)
        member this.ReserveAsync(amount, cancellationToken) = this.ReserveAsync(amount, cancellationToken)
        member this.Release(amount) = this.Release(amount)
        member this.GetSnapshot() = this.GetSnapshot()
        member this.Close() = this.Dispose()

    interface ICapacityHeadroom with
        member this.GetHeadroomSnapshot() = this.GetSnapshot()

type internal CompositeCapacityAdmission
    (primary: ICapacityAdmission, secondary: ICapacityAdmission)
    =
    do
        if obj.ReferenceEquals(primary, null) then
            nullArg (nameof primary)

        if obj.ReferenceEquals(secondary, null) then
            nullArg (nameof secondary)

    // Admit against the directional share before the connection-wide budget.
    // A direction at its share must not consume the sibling's remaining
    // global credit while waiting for a share handoff.
    let domains = [| secondary; primary |]

    let aggregate message failures =
        match failures with
        | [] -> ()
        | [ error ] -> raise error
        | values -> raise (AggregateException(message, values |> List.toArray))

    let releaseReservations (reservations: ResizeArray<CapacityReservation>) =
        let failures = ResizeArray<exn>()

        for reservation in reservations do
            try
                (reservation :> IDisposable).Dispose()
            with error ->
                failures.Add error

        if failures.Count > 0 then
            aggregate "Composite capacity reservation cleanup failed." (List.ofSeq failures)

    let createReservation amount (reservations: CapacityReservation list) =
        new CapacityReservation(
            amount,
            fun released ->
                let failures = ResizeArray<exn>()

                for reservation in reservations do
                    try
                        reservation.ReleasePartial released
                    with error ->
                        failures.Add error

                if failures.Count > 0 then
                    aggregate "Composite capacity reservation release failed." (List.ofSeq failures)
        )

    let reserveSynchronously amount =
        let acquired = ResizeArray<CapacityReservation>()
        let mutable failure: exn option = None

        for domain in domains do
            if failure.IsNone then
                try
                    match domain.TryReserve amount with
                    | ValueSome reservation -> acquired.Add reservation
                    | ValueNone ->
                        failure <-
                            Some(
                                InvalidOperationException(
                                    "The composite capacity admission could not reserve the requested amount."
                                )
                            )
                with error ->
                    failure <- Some error

        match failure with
        | None -> ValueSome(createReservation amount (List.ofSeq acquired))
        | Some error ->
            releaseReservations acquired
            ValueNone

    let reserveAsync amount cancellationToken =
        ValueTask<CapacityReservation>(
            task {
                let acquired = ResizeArray<CapacityReservation>()

                try
                    for domain in domains do
                        let! reservation = domain.ReserveAsync(amount, cancellationToken).AsTask()
                        acquired.Add reservation

                    return createReservation amount (List.ofSeq acquired)
                with error ->
                    releaseReservations acquired
                    return raise error
            }
        )

    let beginCompositeCleanup (createCleanup: ICapacityAdmission -> CapacityCleanup) =
        let cleanups = domains |> Array.map createCleanup |> Array.toList
        let failure =
            cleanups
            |> List.map (fun cleanup -> cleanup.Failure)
            |> function
                | [ error ] -> error
                | errors -> AggregateException("Composite capacity cleanup is required.", errors |> List.toArray) :> exn

        new CapacityCleanup(
            failure,
            (fun () ->
                let failures = ResizeArray<exn>()

                for cleanup in cleanups do
                    try
                        cleanup.CompleteSuccessfully()
                    with error ->
                        failures.Add error

                if failures.Count > 0 then
                    aggregate "Composite capacity cleanup failed." (List.ofSeq failures)),
            (fun () ->
                let failures = ResizeArray<exn>()

                for cleanup in cleanups do
                    try
                        cleanup.CompleteFailure()
                    with error ->
                        failures.Add error

                if failures.Count > 0 then
                    aggregate "Composite capacity cleanup failed." (List.ofSeq failures))
        )

    interface ICapacityAdmission with
        member _.LimitBytes = min primary.LimitBytes secondary.LimitBytes
        member _.IsClosed = primary.IsClosed || secondary.IsClosed

        member _.TryAcquire(amount) =
            let mutable acquired = 0
            let mutable succeeded = true

            for domain in domains do
                if succeeded then
                    if domain.TryAcquire amount then
                        acquired <- acquired + 1
                    else
                        succeeded <- false

            if not succeeded then
                for index in 0 .. acquired - 1 do
                    domains[index].Release amount

            succeeded

        member _.TryReserve(amount) = reserveSynchronously amount

        member _.TryReserveExpansion(amount) =
            let acquired = ResizeArray<CapacityReservation>()
            let cleanups = ResizeArray<CapacityCleanup>()
            let mutable failure: exn option = None

            for domain in domains do
                if failure.IsNone then
                    try
                        match domain.TryReserveExpansion amount with
                        | ExpansionReserved reservation -> acquired.Add reservation
                        | CleanupRequired cleanup -> cleanups.Add cleanup
                    with error ->
                        failure <- Some error

            match failure with
            | Some error ->
                releaseReservations acquired
                raise error
            | None when cleanups.Count = 0 ->
                ExpansionReserved(createReservation amount (List.ofSeq acquired))
            | None ->
                let failure =
                    cleanups
                    |> Seq.map (fun cleanup -> cleanup.Failure)
                    |> Seq.toList
                    |> function
                        | [ error ] -> error
                        | errors -> AggregateException("Composite capacity expansion cleanup is required.", errors |> List.toArray) :> exn

                CleanupRequired(
                    new CapacityCleanup(
                        failure,
                        (fun () ->
                            let failures = ResizeArray<exn>()

                            for cleanup in cleanups do
                                try
                                    cleanup.CompleteSuccessfully()
                                with error ->
                                    failures.Add error

                            try
                                releaseReservations acquired
                            with error ->
                                failures.Add error

                            if failures.Count > 0 then
                                aggregate "Composite capacity expansion cleanup failed." (List.ofSeq failures)),
                        (fun () ->
                            let failures = ResizeArray<exn>()

                            for cleanup in cleanups do
                                try
                                    cleanup.CompleteFailure()
                                with error ->
                                    failures.Add error

                            if failures.Count > 0 then
                                aggregate "Composite capacity expansion cleanup failed." (List.ofSeq failures)))
                )

        member _.BeginUnknownCleanup(error) = beginCompositeCleanup (fun domain -> domain.BeginUnknownCleanup error)
        member _.BeginUnadmittedCleanup(amount, error) =
            beginCompositeCleanup (fun domain -> domain.BeginUnadmittedCleanup(amount, error))

        member _.RecordUncertain(amount) =
            for domain in domains do
                domain.RecordUncertain amount

        member _.ReserveAsync(amount, cancellationToken) = reserveAsync amount cancellationToken

        member _.Release(amount) =
            let failures = ResizeArray<exn>()

            for domain in domains do
                try
                    domain.Release amount
                with error ->
                    failures.Add error

            if failures.Count > 0 then
                aggregate "Composite capacity release failed." (List.ofSeq failures)

        member _.GetSnapshot() = primary.GetSnapshot()
        member _.Close() =
            primary.Close()
            secondary.Close()

    interface ICapacityHeadroom with
        member _.GetHeadroomSnapshot() = secondary.GetSnapshot()
