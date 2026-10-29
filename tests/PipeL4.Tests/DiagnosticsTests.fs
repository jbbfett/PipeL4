namespace PipeL4.Tests

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open Xunit

module DiagnosticsTests =
    type private FixedOwner(size: int) =
        let buffer = Array.zeroCreate<byte> size

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = ()

    type private ThrowingOwner(size: int) =
        let buffer = Array.zeroCreate<byte> size

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = raise (InvalidOperationException("owner disposal failed"))

    type private AlwaysOverRentPool(?throwOnDispose: bool) =
        inherit MemoryPool<byte>()

        let throwOnDispose = defaultArg throwOnDispose false
        let mutable rentCount = 0

        member _.RentCount = Volatile.Read(&rentCount)

        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            Interlocked.Increment(&rentCount) |> ignore

            if throwOnDispose then
                new ThrowingOwner(sizeHint + 4) :> IMemoryOwner<byte>
            else
                new FixedOwner(sizeHint + 4) :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    type private RetryingPool() =
        inherit MemoryPool<byte>()

        let mutable rentCount = 0

        member _.RentCount = Volatile.Read(&rentCount)

        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            Interlocked.Increment(&rentCount) |> ignore
            let capacity = if sizeHint > 8 then 64 else 16
            new FixedOwner(capacity) :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    let private validArguments =
        [| "--listen-address"
           "127.0.0.1"
           "--listen-port"
           "8080"
           "--backend-host"
           "127.0.0.1"
           "--backend-port"
           "8081" |]

    let private parse arguments =
        match ForwardingConfiguration.tryParseArguments arguments with
        | Ok configuration -> configuration
        | Error error -> failwithf "Expected valid arguments, but parsing failed: %s" error

    [<Fact>]
    let ``runtime diagnostics are enabled by default and can be disabled`` () =
        let enabled = parse validArguments

        enabled.RuntimeDiagnosticsEnabled |> should equal true

        let disabledArguments = Array.append validArguments [| "--disable-runtime-diagnostics" |]
        let disabled = parse disabledArguments

        disabled.RuntimeDiagnosticsEnabled |> should equal false

        let duplicate = Array.append disabledArguments [| "--disable-runtime-diagnostics" |]

        match ForwardingConfiguration.tryParseArguments duplicate with
        | Error message -> message |> should equal "Argument '--disable-runtime-diagnostics' was specified more than once."
        | Ok _ -> failwith "A duplicate valueless diagnostics option was accepted."

    [<Fact>]
    let ``queued wait metrics count grant cancellation and disposal`` () =
        task {
            let budget = new CapacityBudget(1L, diagnosticsEnabled = true)
            let held =
                match budget.TryReserve 1L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "The initial reservation was not admitted."

            let granted = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            let queued = budget.GetSnapshot()

            queued.CurrentWaiterCount |> should equal 1L
            queued.PeakWaiterCount |> should equal 1L
            queued.QueuedWaitCount |> should equal 1L

            held.Release()
            let! grantedReservation = granted
            grantedReservation.Release()

            let afterGrant = budget.GetSnapshot()
            afterGrant.CurrentWaiterCount |> should equal 0L
            afterGrant.QueuedWaitCount |> should equal 1L
            afterGrant.TotalWaitDuration |> should be (greaterThanOrEqualTo TimeSpan.Zero)
            afterGrant.MaximumWaitDuration |> should be (greaterThanOrEqualTo TimeSpan.Zero)
            afterGrant.TotalWaitDuration |> should be (greaterThanOrEqualTo afterGrant.MaximumWaitDuration)

            let secondHeld =
                match budget.TryReserve 1L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "The second reservation was not admitted."

            use cancellation = new CancellationTokenSource()
            let cancelled = budget.ReserveAsync(1L, cancellation.Token).AsTask()
            cancellation.Cancel()
            let! _ = Assert.ThrowsAsync<TaskCanceledException>(fun () -> cancelled)

            let afterCancellation = budget.GetSnapshot()
            afterCancellation.CurrentWaiterCount |> should equal 0L
            afterCancellation.QueuedWaitCount |> should equal 2L
            afterCancellation.TotalWaitDuration
            |> should be (greaterThanOrEqualTo afterGrant.TotalWaitDuration)

            let disposedWait = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            budget.Dispose()
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> disposedWait)

            let afterDisposal = budget.GetSnapshot()
            afterDisposal.CurrentWaiterCount |> should equal 0L
            afterDisposal.QueuedWaitCount |> should equal 3L
            secondHeld.Release()
        }

    [<Fact>]
    let ``concurrent capacity snapshots stay consistent while waiters drain`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            let budget = new CapacityBudget(1L, diagnosticsEnabled = true)

            let held =
                match budget.TryReserve 1L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "The initial reservation was not admitted."

            let workerCount = 8

            let workers =
                Array.init workerCount (fun _ ->
                    task {
                        let! reservation = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
                        reservation.Release()
                    })

            let allWorkers = Task.WhenAll workers

            while budget.GetSnapshot().CurrentWaiterCount < int64 workerCount do
                do! Task.Delay(1, timeout.Token)

            held.Release()

            while not allWorkers.IsCompleted do
                let snapshot = budget.GetSnapshot()
                snapshot.CurrentWaiterCount |> should be (greaterThanOrEqualTo 0L)
                snapshot.PeakWaiterCount |> should be (greaterThanOrEqualTo snapshot.CurrentWaiterCount)
                snapshot.TotalWaitDuration |> should be (greaterThanOrEqualTo snapshot.MaximumWaitDuration)
                do! Task.Delay(1, timeout.Token)

            let! _ = allWorkers

            let finalSnapshot = budget.GetSnapshot()
            finalSnapshot.CurrentWaiterCount |> should equal 0L
            finalSnapshot.PeakWaiterCount |> should equal (int64 workerCount)
            finalSnapshot.QueuedWaitCount |> should equal (int64 workerCount)
            finalSnapshot.TotalWaitDuration
            |> should be (greaterThanOrEqualTo finalSnapshot.MaximumWaitDuration)
        }

    [<Fact>]
    let ``snapshot waits for the complete waiter registration diagnostic update`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use waiterCancellation = new CancellationTokenSource()
            let registrationEntered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowRegistration = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue = ignore
                  AfterWaiterCountIncrement =
                      fun () ->
                          registrationEntered.TrySetResult(()) |> ignore
                          allowRegistration.Task.GetAwaiter().GetResult()
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore = ignore
                  BeforeCleanupFailureTransition = ignore }

            let budget = new CapacityBudget(1L, hooks)
            let held =
                match budget.TryReserve 1L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "The initial reservation was not admitted."

            let waiting =
                Task.Run<CapacityReservation>(Func<Task<CapacityReservation>>(fun () ->
                    budget.ReserveAsync(1L, waiterCancellation.Token).AsTask()))

            try
                do! registrationEntered.Task.WaitAsync(timeout.Token)

                let snapshotStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let snapshotTask =
                    Task.Run<CapacityUsageSnapshot>(Func<CapacityUsageSnapshot>(fun () ->
                        snapshotStarted.TrySetResult(()) |> ignore
                        budget.GetSnapshot()))

                do! snapshotStarted.Task.WaitAsync(timeout.Token)
                let! winner = Task.WhenAny(snapshotTask :> Task, Task.Delay(100, timeout.Token))
                obj.ReferenceEquals(winner, snapshotTask) |> should equal false

                allowRegistration.TrySetResult(()) |> ignore
                let! snapshot = snapshotTask.WaitAsync(timeout.Token)
                snapshot.CurrentWaiterCount |> should equal 1L
                snapshot.QueuedWaitCount |> should equal 1L
                snapshot.PeakWaiterCount |> should equal 1L
            finally
                allowRegistration.TrySetResult(()) |> ignore
                waiterCancellation.Cancel()
                (held :> IDisposable).Dispose()
        }

    [<Fact>]
    let ``snapshot waits for the complete wait duration diagnostic update`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use waiterCancellation = new CancellationTokenSource()
            let waiterRegistered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let completionEntered = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowCompletion = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue = ignore
                  AfterWaiterCountIncrement = fun () -> waiterRegistered.TrySetResult(()) |> ignore
                  BeforeMaximumWaitDurationUpdate =
                      fun () ->
                          completionEntered.TrySetResult(()) |> ignore
                          allowCompletion.Task.GetAwaiter().GetResult()
                  AfterReleaseCore = ignore
                  BeforeCleanupFailureTransition = ignore }

            let budget = new CapacityBudget(1L, hooks)
            let held =
                match budget.TryReserve 1L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "The initial reservation was not admitted."

            let waiting =
                Task.Run<CapacityReservation>(Func<Task<CapacityReservation>>(fun () ->
                    budget.ReserveAsync(1L, waiterCancellation.Token).AsTask()))

            try
                do! waiterRegistered.Task.WaitAsync(timeout.Token)
                do! Task.Delay(5, timeout.Token)

                let releaseTask = Task.Run(fun () -> held.Release())
                do! completionEntered.Task.WaitAsync(timeout.Token)

                let snapshotStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let snapshotTask =
                    Task.Run<CapacityUsageSnapshot>(Func<CapacityUsageSnapshot>(fun () ->
                        snapshotStarted.TrySetResult(()) |> ignore
                        budget.GetSnapshot()))

                do! snapshotStarted.Task.WaitAsync(timeout.Token)
                let! winner = Task.WhenAny(snapshotTask :> Task, Task.Delay(100, timeout.Token))
                obj.ReferenceEquals(winner, snapshotTask) |> should equal false

                allowCompletion.TrySetResult(()) |> ignore
                do! releaseTask
                let! reservation = waiting.WaitAsync(timeout.Token)
                reservation.Release()

                let! snapshot = snapshotTask.WaitAsync(timeout.Token)
                snapshot.TotalWaitDuration |> should be (greaterThan TimeSpan.Zero)
                snapshot.TotalWaitDuration |> should equal snapshot.MaximumWaitDuration
            finally
                allowCompletion.TrySetResult(()) |> ignore
                waiterCancellation.Cancel()
                (held :> IDisposable).Dispose()
        }

    [<Fact>]
    let ``controlled queued waits report coherent diagnostics with positive duration`` () =
        task {
            let budget = new CapacityBudget(1L, diagnosticsEnabled = true)

            let held =
                match budget.TryReserve 1L with
                | ValueSome reservation -> reservation
                | ValueNone -> failwith "The initial reservation was not admitted."

            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()

            while budget.GetSnapshot().CurrentWaiterCount <> 1L do
                do! Task.Yield()

            do! Task.Delay(5)
            held.Release()
            let! reservation = waiting
            reservation.Release()

            let snapshot = budget.GetSnapshot()
            snapshot.PeakWaiterCount |> should be (greaterThanOrEqualTo snapshot.CurrentWaiterCount)
            snapshot.QueuedWaitCount |> should be (greaterThanOrEqualTo snapshot.CurrentWaiterCount)
            snapshot.TotalWaitDuration |> should be (greaterThan TimeSpan.Zero)
            snapshot.TotalWaitDuration |> should be (greaterThanOrEqualTo snapshot.MaximumWaitDuration)
        }

    [<Fact>]
    let ``disabled diagnostics preserve accounting but omit optional counters`` () =
        let accounting =
            new ConnectionCapacityAccounting(
                ConnectionCapacityLimits(8L, 8L, 16L),
                diagnosticsEnabled = false
            )

        let logical =
            match accounting.ClientToBackendLogical.TryReserve 4L with
            | ValueSome reservation -> reservation
            | ValueNone -> failwith "The logical reservation was not admitted."

        let physical = accounting.Physical.TrackArrayCapacity 5L
        let snapshot = ConnectionRuntimeMetrics(accounting).GetSnapshot()

        snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 4L
        snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 5L
        snapshot.ClientToBackend.LogicalCapacity.QueuedWaitCount |> should equal 0L
        snapshot.ClientToBackend.ReceiveRentalAttempts |> should equal 0L
        snapshot.ClientToBackend.Stages |> should be Empty
        snapshot.ClientToBackend.TerminalOutcome |> should equal "disabled"

        let disabledStageMetrics = ProcessorPipelineMetrics(1, diagnosticsEnabled = false)
        disabledStageMetrics.BeginInvocation 0
        disabledStageMetrics.CommitSuccessful(0, 4L, 2L, 9L, 1L)
        let disabledStage = disabledStageMetrics.GetSnapshot().Stages[0]
        disabledStage.InvocationCount |> should equal 0L
        disabledStage.ProcessingDuration |> should equal TimeSpan.Zero
        disabledStage.MaximumInvocationDuration |> should equal TimeSpan.Zero

        physical.Dispose()
        logical.Release()

    [<Fact>]
    let ``receive rental metrics distinguish retries from exposed owners`` () =
        let accounting = new ConnectionCapacityAccounting(ConnectionCapacityLimits(16L, 16L, 64L))
        let pool = new RetryingPool()
        let owner =
            accounting.ClientToBackendPhysical.RentReceiveAsync(
                pool,
                32,
                CancellationToken.None
            ).AsTask()
            |> fun task -> task.GetAwaiter().GetResult()

        let diagnostics = accounting.ClientToBackendDiagnostics.GetSnapshot()
        diagnostics.ReceiveRentalAttempts |> should equal 3L
        diagnostics.ReceiveRequestedBytes |> should equal 56L
        diagnostics.ReceiveReturnedOwnerCapacityBytes |> should equal (64L + 64L + 16L)
        diagnostics.ReceiveSuccessfullyExposedOwners |> should equal 1L
        diagnostics.ReceiveOverRentRetries |> should equal 2L
        pool.RentCount |> should equal 3

        owner.Dispose()

    [<Fact>]
    let ``final receive over-rent failure records one pool-fit progress failure`` () =
        let diagnostics = DirectionRuntimeDiagnostics(true)
        let accounting =
            new ConnectionMemoryAccounting(
                new CapacityBudget(4L),
                diagnostics.AsMemoryDiagnostics()
            )

        let pool = new AlwaysOverRentPool()

        let observed =
            try
                accounting.RentReceiveAsync(pool, 4, CancellationToken.None).AsTask().GetAwaiter().GetResult()
                |> ignore
                failwith "The terminal receive over-run was not raised."
            with error ->
                error

        observed.Message |> should equal "The actual pooled buffer capacity exceeded the remaining physical budget."
        pool.RentCount |> should equal 3

        let snapshot = diagnostics.GetSnapshot()
        snapshot.ReceiveRentalAttempts |> should equal 3L
        snapshot.ReceiveRequestedBytes |> should equal 7L
        snapshot.ReceiveReturnedOwnerCapacityBytes |> should equal 19L
        snapshot.ReceiveOverRentRetries |> should equal 2L
        snapshot.ReceiveSuccessfullyExposedOwners |> should equal 0L
        snapshot.PoolOwnerFitFailureCount |> should equal 1L
        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L

    [<Fact>]
    let ``physical owner cleanup failures update direction diagnostics`` () =
        let diagnostics = DirectionRuntimeDiagnostics(true)
        let accounting =
            new ConnectionMemoryAccounting(
                new CapacityBudget(8L),
                diagnostics.AsMemoryDiagnostics()
            )

        let tracked = accounting.Track(new ThrowingOwner(7))
        let observed = Assert.Throws<InvalidOperationException>(fun () -> tracked.Dispose())

        observed.Message |> should equal "owner disposal failed"
        diagnostics.GetSnapshot().CleanupFailurePresent |> should equal true
        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 7L

    [<Fact>]
    let ``rejected over-rent owner cleanup is diagnosed and remains conservatively charged`` () =
        let diagnostics = DirectionRuntimeDiagnostics(true)
        let accounting =
            new ConnectionMemoryAccounting(
                new CapacityBudget(4L),
                diagnostics.AsMemoryDiagnostics()
            )

        let pool = new AlwaysOverRentPool(throwOnDispose = true)

        let observed =
            Assert.Throws<AggregateException>(fun () ->
                accounting.RentReceiveAsync(pool, 4, CancellationToken.None).AsTask().GetAwaiter().GetResult()
                |> ignore)

        observed.InnerExceptions |> Seq.exists (fun error -> error.Message = "owner disposal failed") |> should equal true
        diagnostics.GetSnapshot().CleanupFailurePresent |> should equal true
        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L
        pool.RentCount |> should equal 1

    [<Fact>]
    let ``physical share snapshots publish handoff after expansion is usable`` () =
        let accounting = new ConnectionCapacityAccounting(ConnectionCapacityLimits(8L, 8L, 16L))
        let lease = accounting.ClientToBackendPhysical.TrackArrayCapacity 4L
        let before = ConnectionRuntimeMetrics(accounting).GetSnapshot()

        before.ClientToBackend.PhysicalShare.CurrentBytes |> should equal 4L
        before.ClientToBackend.PhysicalShare.LimitBytes |> should equal 8L
        before.ClientToBackend.PhysicalShareHandoffState |> should equal PhysicalShareHandoffState.Open

        lease.Dispose()
        accounting.CompletePhysicalDirection true

        let after = ConnectionRuntimeMetrics(accounting).GetSnapshot()
        after.ClientToBackend.PhysicalShare.CurrentBytes |> should equal 0L
        after.ClientToBackend.PhysicalShareHandoffState |> should equal PhysicalShareHandoffState.Completed
        after.BackendToClient.PhysicalShare.LimitBytes |> should equal 16L
        after.BackendToClient.PhysicalShareHandoffState |> should equal PhysicalShareHandoffState.Open

    [<Fact>]
    let ``stage maximum duration includes failed and cancelled completions`` () =
        let metrics = ProcessorPipelineMetrics(1)
        metrics.BeginInvocation 0
        metrics.CommitSuccessful(0, 0L, 0L, 7L, 0L)
        metrics.BeginInvocation 0
        metrics.CommitFailed(0, 11L, false)
        metrics.BeginInvocation 0
        metrics.CommitFailed(0, 5L, true)

        let snapshot = metrics.GetSnapshot().Stages[0]
        snapshot.InvocationCount |> should equal 3L
        snapshot.SuccessfulInvocationCount |> should equal 1L
        snapshot.FailedInvocationCount |> should equal 1L
        snapshot.CancelledInvocationCount |> should equal 1L
        snapshot.ProcessingDuration |> should equal (TimeSpan.FromTicks 23L)
        snapshot.MaximumInvocationDuration |> should equal (TimeSpan.FromTicks 11L)

    [<Fact>]
    let ``progress failure categories are stable and detached`` () =
        let diagnostics = DirectionRuntimeDiagnostics(true)

        for category in
            [ ProgressFailureCategory.LogicalReceiveExhaustion
              ProgressFailureCategory.SynchronousLogicalOutputExhaustion
              ProgressFailureCategory.SynchronousPhysicalOutputExhaustion
              ProgressFailureCategory.PhysicalReceiveExhaustion
              ProgressFailureCategory.PoolOwnerFitFailure ] do
            diagnostics.RecordProgressFailure category

        diagnostics.RecordTerminalOutcome "failed"
        diagnostics.RecordCleanupFailure()
        let snapshot = diagnostics.GetSnapshot()

        snapshot.LogicalReceiveExhaustionCount |> should equal 1L
        snapshot.SynchronousLogicalOutputExhaustionCount |> should equal 1L
        snapshot.SynchronousPhysicalOutputExhaustionCount |> should equal 1L
        snapshot.PhysicalReceiveExhaustionCount |> should equal 1L
        snapshot.PoolOwnerFitFailureCount |> should equal 1L
        snapshot.TerminalOutcome |> should equal "failed"
        snapshot.CleanupFailurePresent |> should equal true
