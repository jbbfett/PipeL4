namespace PipeL4.Tests

open System
open System.Buffers
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open Xunit

module CapacityAccountingTests =
    type private TrackingOwner(size: int) =
        let buffer = Array.zeroCreate<byte> size
        let mutable disposeCount = 0

        member _.DisposeCount = Volatile.Read(&disposeCount)

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore

    type private ThrowingOwner(size: int) =
        let buffer = Array.zeroCreate<byte> size
        let mutable disposeCount = 0

        member _.DisposeCount = Volatile.Read(&disposeCount)

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore
                raise (InvalidOperationException("owner disposal failed"))

    type private BlockingOwner(size: int, throwOnDispose: bool) =
        let buffer = Array.zeroCreate<byte> size
        let disposeStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowDispose = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable disposeCount = 0

        member _.DisposeStarted = disposeStarted.Task
        member _.DisposeCount = Volatile.Read(&disposeCount)
        member _.AllowDispose() = allowDispose.TrySetResult(()) |> ignore

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore
                disposeStarted.TrySetResult(()) |> ignore
                allowDispose.Task.GetAwaiter().GetResult()

                if throwOnDispose then
                    raise (InvalidOperationException("owner disposal failed"))

    type private MemoryInspectionOwner(size: int, throwOnDispose: bool) =
        let buffer = Array.zeroCreate<byte> size
        let memoryStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowMemory = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable disposeCount = 0

        member _.MemoryStarted = memoryStarted.Task
        member _.DisposeCount = Volatile.Read(&disposeCount)
        member _.AllowMemory() = allowMemory.TrySetResult(()) |> ignore

        interface IMemoryOwner<byte> with
            member _.Memory =
                memoryStarted.TrySetResult(()) |> ignore
                allowMemory.Task.GetAwaiter().GetResult()
                raise (InvalidOperationException("memory inspection failed"))

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore

                if throwOnDispose then
                    raise (InvalidOperationException("owner disposal failed"))

    type private BlockingUnknownCapacityOwner(throwOnDispose: bool) =
        let memoryStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowMemory = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let disposeStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowDispose = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable disposeCount = 0

        member _.MemoryStarted = memoryStarted.Task
        member _.DisposeStarted = disposeStarted.Task
        member _.DisposeCount = Volatile.Read(&disposeCount)
        member _.AllowMemory() = allowMemory.TrySetResult(()) |> ignore
        member _.AllowDispose() = allowDispose.TrySetResult(()) |> ignore

        interface IMemoryOwner<byte> with
            member _.Memory =
                memoryStarted.TrySetResult(()) |> ignore
                allowMemory.Task.GetAwaiter().GetResult()
                raise (InvalidOperationException("memory inspection failed"))

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore
                disposeStarted.TrySetResult(()) |> ignore
                allowDispose.Task.GetAwaiter().GetResult()

                if throwOnDispose then
                    raise (InvalidOperationException("owner disposal failed"))

    type private SuppliedOwnerPool(owner: IMemoryOwner<byte>) =
        inherit MemoryPool<byte>()

        let mutable rentCount = 0

        member _.RentCount = Volatile.Read(&rentCount)

        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(_sizeHint) =
            Interlocked.Increment(&rentCount) |> ignore
            owner

        override _.Dispose(_disposing) = ()

    type private BlockingRentPool(owner: IMemoryOwner<byte>) =
        inherit MemoryPool<byte>()

        let rentStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowRent = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable rentCount = 0

        member _.RentStarted = rentStarted.Task
        member _.RentCount = Volatile.Read(&rentCount)
        member _.AllowRent() = allowRent.TrySetResult(()) |> ignore

        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(_sizeHint) =
            Interlocked.Increment(&rentCount) |> ignore
            rentStarted.TrySetResult(()) |> ignore
            allowRent.Task.GetAwaiter().GetResult()
            owner

        override _.Dispose(_disposing) = ()

    type private InstrumentedPool(capacity: int) =
        inherit MemoryPool<byte>()

        let mutable rentCount = 0
        let owners = ResizeArray<IMemoryOwner<byte>>()

        member _.RentCount = Volatile.Read(&rentCount)
        member _.Owners = owners

        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(_sizeHint) =
            Interlocked.Increment(&rentCount) |> ignore
            let owner = new TrackingOwner(capacity) :> IMemoryOwner<byte>
            owners.Add owner
            owner

        override _.Dispose(_disposing) = ()

    type private RetryingReceivePool() =
        inherit MemoryPool<byte>()

        let mutable rentCount = 0
        let owners = ResizeArray<TrackingOwner>()

        member _.RentCount = Volatile.Read(&rentCount)
        member _.Owners = owners

        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            Interlocked.Increment(&rentCount) |> ignore
            let capacity = if sizeHint > 8 then 64 else 16
            let owner = new TrackingOwner(capacity)
            owners.Add owner
            owner :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    type private ImmediateSink() =
        interface IProcessorPipelineSink with
            member _.WriteAsync(_, _) = ValueTask()
            member _.CompleteAsync _ = ValueTask()

    type private AdmissionProcessor(started: TaskCompletionSource<unit>, generated: bool) =
        interface IByteStreamProcessor with
            member _.ProcessAsync(input, output, _) =
                started.TrySetResult(()) |> ignore

                if generated then
                    let span = output.GetSpan(1)
                    span[0] <- 1uy
                    output.Advance(1)
                else
                    output.AppendInput(0L, input.Bytes.Length)

                ValueTask<ProcessorResult>({ RetainedLength = 0L })

    let private reserve (budget: CapacityBudget) amount =
        match budget.TryReserve amount with
        | ValueSome reservation -> reservation
        | ValueNone -> failwith $"Could not reserve {amount} bytes."

    let private snapshot (budget: CapacityBudget) = budget.GetSnapshot()

    let private runUnknownCleanupSuccess
        (start: ConnectionMemoryAccounting -> BlockingUnknownCapacityOwner -> Task)
        =
        task {
            use accounting = new ConnectionMemoryAccounting(new CapacityBudget 2L)
            let owner = new BlockingUnknownCapacityOwner(false)
            let first = start accounting owner

            do! owner.MemoryStarted
            owner.AllowMemory()
            do! owner.DisposeStarted

            let secondPool = new InstrumentedPool(1)
            let second = accounting.RentAsync(secondPool, 1, CancellationToken.None).AsTask()
            second.IsCompleted |> should equal false
            secondPool.RentCount |> should equal 0

            owner.AllowDispose()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            let! secondOwner = second

            secondPool.RentCount |> should equal 1
            secondOwner.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            owner.DisposeCount |> should equal 1
        }

    let private runUnknownCleanupFailure
        (start: ConnectionMemoryAccounting -> BlockingUnknownCapacityOwner -> Task)
        =
        task {
            use accounting = new ConnectionMemoryAccounting(new CapacityBudget 2L)
            let owner = new BlockingUnknownCapacityOwner(true)
            let first = start accounting owner

            do! owner.MemoryStarted
            owner.AllowMemory()
            do! owner.DisposeStarted

            let secondPool = new InstrumentedPool(1)
            let second = accounting.RentAsync(secondPool, 1, CancellationToken.None).AsTask()
            second.IsCompleted |> should equal false
            secondPool.RentCount |> should equal 0

            owner.AllowDispose()
            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message |> should equal "memory inspection failed"
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> second)
            secondPool.RentCount |> should equal 0

            let futurePool = new InstrumentedPool(1)
            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(futurePool, 1, CancellationToken.None).AsTask())

            futurePool.RentCount |> should equal 0
            let saturated = accounting.GetSnapshot()
            saturated.CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            saturated.PeakPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            owner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``reservation and release track current and peak usage`` () =
        let budget = new CapacityBudget 10L
        let first = reserve budget 4L
        let second = reserve budget 6L

        let full = snapshot budget
        full.CurrentBytes |> should equal 10L
        full.PeakBytes |> should equal 10L

        first.Release()

        let afterRelease = snapshot budget
        afterRelease.CurrentBytes |> should equal 6L
        afterRelease.PeakBytes |> should equal 10L

        (second :> IDisposable).Dispose()

        let empty = snapshot budget
        empty.CurrentBytes |> should equal 0L
        empty.PeakBytes |> should equal 10L

    [<Fact>]
    let ``reservations partition and release proportional logical credit`` () =
        let budget = new CapacityBudget 10L
        let parent = reserve budget 10L
        let first = parent.Split 4L
        let second = parent.Split 3L

        parent.RemainingAmount |> should equal 3L
        first.RemainingAmount |> should equal 4L
        second.RemainingAmount |> should equal 3L
        (snapshot budget).CurrentBytes |> should equal 10L

        first.ReleasePartial 2L
        (snapshot budget).CurrentBytes |> should equal 8L
        first.RemainingAmount |> should equal 2L

        second.Release()
        first.Release()
        parent.Release()
        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``zero and exact boundary reservations are valid`` () =
        let zero = new CapacityBudget 0L
        let empty = reserve zero 0L
        (snapshot zero).CurrentBytes |> should equal 0L
        empty.Release()

        let budget = new CapacityBudget 8L
        let reservation = reserve budget 8L
        (snapshot budget).CurrentBytes |> should equal 8L

        match budget.TryReserve 1L with
        | ValueSome _ -> failwith "A reservation beyond the exact boundary succeeded."
        | ValueNone -> ()

        reservation.Release()
        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``failed reservations do not change current or peak and recover after release`` () =
        let budget = new CapacityBudget 5L
        let held = reserve budget 5L
        let before = snapshot budget

        match budget.TryReserve 1L with
        | ValueSome _ -> failwith "A reservation over the budget succeeded."
        | ValueNone -> ()

        let afterFailure = snapshot budget
        afterFailure.CurrentBytes |> should equal before.CurrentBytes
        afterFailure.PeakBytes |> should equal before.PeakBytes

        held.Release()
        let recovered = reserve budget 5L
        (snapshot budget).CurrentBytes |> should equal 5L
        recovered.Release()

    [<Fact>]
    let ``limits and amounts reject negative values`` () =
        Assert.Throws<ArgumentException>(fun () -> new CapacityBudget(-1L) |> ignore)
        |> ignore

        let budget = new CapacityBudget 1L
        Assert.Throws<ArgumentException>(fun () -> budget.TryReserve(-1L) |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> budget.Release(-1L)) |> ignore

        Assert.Throws<ArgumentException>(fun () -> ConnectionCapacityLimits(-1L, 1L, 1L) |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> ConnectionCapacityLimits(1L, -1L, 1L) |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> ConnectionCapacityLimits(1L, 1L, -1L) |> ignore)
        |> ignore

    [<Fact>]
    let ``maximum int64 capacity cannot overflow`` () =
        let budget = new CapacityBudget(Int64.MaxValue)
        let held = reserve budget Int64.MaxValue

        match budget.TryReserve 1L with
        | ValueSome _ -> failwith "A reservation overflowed the maximum capacity."
        | ValueNone -> ()

        let full = snapshot budget
        full.CurrentBytes |> should equal Int64.MaxValue
        full.PeakBytes |> should equal Int64.MaxValue

        held.Release()
        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``repeated release and underflow fail without changing usage`` () =
        let budget = new CapacityBudget 3L
        let reservation = reserve budget 3L
        reservation.Release()

        Assert.Throws<InvalidOperationException>(fun () -> reservation.Release())
        |> ignore

        Assert.Throws<InvalidOperationException>(fun () -> budget.Release 1L) |> ignore
        (snapshot budget).CurrentBytes |> should equal 0L

        // IDisposable cleanup remains idempotent for runtime-owned wrappers.
        (reservation :> IDisposable).Dispose()
        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``concurrent reservations never exceed the shared limit`` () =
        let limit = 64L
        let budget = new CapacityBudget(limit)
        let successful = ConcurrentBag<CapacityReservation>()

        Parallel.For(
            0,
            256,
            Action<int>(fun _ ->
                match budget.TryReserve 1L with
                | ValueSome reservation -> successful.Add reservation
                | ValueNone -> ())
        )
        |> ignore

        successful.Count |> should equal (int limit)
        let held = snapshot budget
        held.CurrentBytes |> should equal limit
        held.PeakBytes |> should equal limit

        Parallel.ForEach(successful, Action<CapacityReservation>(fun reservation -> reservation.Release()))
        |> ignore

        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``directional and connection budgets remain separate`` () =
        let limits = ConnectionCapacityLimits(3L, 5L, 7L)
        let accounting = new ConnectionCapacityAccounting(limits)
        let clientToBackend = reserve accounting.ClientToBackendLogical 3L
        let backendToClient = reserve accounting.BackendToClientLogical 5L
        let firstPhysical = accounting.Physical.TrackArrayCapacity 4L
        let secondPhysical = accounting.Physical.TrackArrayCapacity 3L

        match accounting.ClientToBackendLogical.TryReserve 1L with
        | ValueSome _ -> failwith "The client-to-backend budget was exceeded."
        | ValueNone -> ()

        match accounting.BackendToClientLogical.TryReserve 1L with
        | ValueSome _ -> failwith "The backend-to-client budget was exceeded."
        | ValueNone -> ()

        match accounting.Physical.GetSnapshot().CurrentPhysicalBufferCapacityBytes with
        | 7L -> ()
        | value -> failwith $"Unexpected physical usage: {value}."

        Assert.Throws<InvalidOperationException>(fun () ->
            accounting.Physical.TrackArrayCapacity 1L |> ignore)
        |> ignore

        clientToBackend.Release()
        backendToClient.Release()
        firstPhysical.Dispose()
        secondPhysical.Dispose()

        let final = accounting.GetSnapshot()
        final.ClientToBackendLogical.CurrentBytes |> should equal 0L
        final.BackendToClientLogical.CurrentBytes |> should equal 0L
        final.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        final.Physical.PeakPhysicalBufferCapacityBytes |> should equal 7L

    [<Fact>]
    let ``retracking a shared physical owner does not double count`` () =
        let accounting = new ConnectionMemoryAccounting()
        let owner = new TrackingOwner(17)
        let tracked = accounting.Track(owner)
        let moved = accounting.Track(tracked)

        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 17L
        accounting.GetSnapshot().PeakPhysicalBufferCapacityBytes |> should equal 17L
        Object.ReferenceEquals(tracked, moved) |> should equal true

        tracked.Dispose()
        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        owner.DisposeCount |> should equal 1

        moved.Dispose()
        owner.DisposeCount |> should equal 1

    [<Fact>]
    let ``reservation ownership moves without transient accounting changes`` () =
        let budget = new CapacityBudget 6L
        let original = reserve budget 6L

        let transfer (reservation: CapacityReservation) = reservation
        let moved = transfer original

        (snapshot budget).CurrentBytes |> should equal 6L
        moved.Release()
        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``physical registration failure disposes the owner without changing accounting`` () =
        let limits = ConnectionCapacityLimits(Int64.MaxValue, Int64.MaxValue, 4L)
        let accounting = new ConnectionCapacityAccounting(limits)
        let owner = new TrackingOwner(5)

        Assert.Throws<InvalidOperationException>(fun () -> accounting.Physical.Track(owner) |> ignore)
        |> ignore

        let observed = accounting.Physical.GetSnapshot()
        observed.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        observed.PeakPhysicalBufferCapacityBytes |> should equal 0L
        owner.DisposeCount |> should equal 1

    [<Fact>]
    let ``throwing disposal keeps physical capacity conservatively accounted`` () =
        let accounting = new ConnectionMemoryAccounting()
        let owner = new ThrowingOwner(7)
        let tracked = accounting.Track(owner)

        Assert.Throws<InvalidOperationException>(fun () -> tracked.Dispose())
        |> ignore

        let afterFailure = accounting.GetSnapshot()
        afterFailure.CurrentPhysicalBufferCapacityBytes |> should equal 7L
        afterFailure.PeakPhysicalBufferCapacityBytes |> should equal 7L
        owner.DisposeCount |> should equal 1

        tracked.Dispose()
        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 7L

    [<Fact>]
    let ``mixed usage preserves the lifetime peak`` () =
        let budget = new CapacityBudget 20L
        let first = reserve budget 8L
        first.Release()
        let second = reserve budget 13L
        let failedBefore = snapshot budget

        match budget.TryReserve 8L with
        | ValueSome _ -> failwith "A reservation beyond the limit succeeded."
        | ValueNone -> ()

        let failedAfter = snapshot budget
        failedAfter.CurrentBytes |> should equal failedBefore.CurrentBytes
        failedAfter.PeakBytes |> should equal failedBefore.PeakBytes

        second.Release()
        let final = snapshot budget
        final.CurrentBytes |> should equal 0L
        final.PeakBytes |> should equal 13L

    [<Fact>]
    let ``connection snapshots retain physical peak after release`` () =
        let memory = new ConnectionMemoryAccounting()
        let metrics = ConnectionRuntimeMetrics(memory)
        let lease = memory.TrackArrayCapacity 9L

        let active = metrics.GetSnapshot()
        active.CurrentPhysicalBufferCapacityBytes |> should equal 9L
        active.PeakPhysicalBufferCapacityBytes |> should equal 9L

        lease.Dispose()

        let released = metrics.GetSnapshot()
        released.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        released.PeakPhysicalBufferCapacityBytes |> should equal 9L

    [<Fact>]
    let ``async reservation waits and resumes after release`` () =
        task {
            let budget = new CapacityBudget 4L
            let held = reserve budget 4L
            let waiting = budget.ReserveAsync(2L, CancellationToken.None).AsTask()

            waiting.IsCompleted |> should equal false
            held.Release()

            let! acquired = waiting
            acquired.Amount |> should equal 2L
            acquired.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``work conserving waiter scan grants every fitting request`` () =
        task {
            let budget = new CapacityBudget 10L
            let held = reserve budget 7L
            let large = budget.ReserveAsync(5L, CancellationToken.None).AsTask()
            let small = budget.ReserveAsync(2L, CancellationToken.None).AsTask()

            large.IsCompleted |> should equal false
            small.IsCompleted |> should equal true

            let! smallReservation = small
            smallReservation.Release()
            held.Release()

            let! largeReservation = large
            largeReservation.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``concurrent waiters and repeated release cycles do not overcommit`` () =
        task {
            let budget = new CapacityBudget 1L
            let held = reserve budget 1L

            let waiters =
                Array.init 64 (fun _ ->
                    budget.ReserveAsync(1L, CancellationToken.None).AsTask())

            waiters |> Array.forall (fun waiter -> not waiter.IsCompleted) |> should equal true
            held.Release()

            let! reservations =
                waiters
                |> Array.map (fun waiter ->
                    task {
                        let! reservation = waiter
                        reservation.Release()
                        return reservation.Amount
                    })
                |> Task.WhenAll

            reservations |> Array.forall (fun amount -> amount = 1L) |> should equal true
            (snapshot budget).CurrentBytes |> should equal 0L
            (snapshot budget).PeakBytes |> should equal 1L
        }

    [<Fact>]
    let ``repeated asynchronous reservation cycles remain balanced`` () =
        task {
            let budget = new CapacityBudget 3L

            for _ in 1 .. 100 do
                let! reservation = budget.ReserveAsync(3L, CancellationToken.None).AsTask()
                reservation.Release()
                (snapshot budget).CurrentBytes |> should equal 0L

            (snapshot budget).PeakBytes |> should equal 3L
        }

    [<Fact>]
    let ``queued cancellation removes the waiter without changing usage`` () =
        task {
            let budget = new CapacityBudget 2L
            let held = reserve budget 2L
            use cancellation = new CancellationTokenSource()
            let waiting = budget.ReserveAsync(1L, cancellation.Token).AsTask()

            cancellation.Cancel()
            let! _ = Assert.ThrowsAsync<TaskCanceledException>(fun () -> waiting)
            (snapshot budget).CurrentBytes |> should equal 2L

            held.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``cancellation before admission is observed synchronously`` () =
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()

        Assert.Throws<OperationCanceledException>(fun () ->
            (new CapacityBudget(1L)).ReserveAsync(1L, cancellation.Token).AsTask() |> ignore)
        |> ignore

    [<Fact>]
    let ``cancellation racing release leaves either one completed reservation or cancellation`` () =
        task {
            for _ in 1 .. 40 do
                let budget = new CapacityBudget 1L
                let held = reserve budget 1L
                use cancellation = new CancellationTokenSource()
                let waiting = budget.ReserveAsync(1L, cancellation.Token).AsTask()
                use barrier = new Barrier(2)

                let releaseTask =
                    Task.Run(fun () ->
                        barrier.SignalAndWait() |> ignore
                        held.Release())

                let cancelTask =
                    Task.Run(fun () ->
                        barrier.SignalAndWait() |> ignore
                        cancellation.Cancel())

                do! Task.WhenAll(releaseTask, cancelTask)

                try
                    let! reservation = waiting
                    reservation.Release()
                with :? OperationCanceledException ->
                    ()

                (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``disposing a budget terminates outstanding waits and preserves releases`` () =
        task {
            let budget = new CapacityBudget 1L
            let held = reserve budget 1L
            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()

            (budget :> IDisposable).Dispose()
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)

            Assert.Throws<ObjectDisposedException>(fun () ->
                budget.ReserveAsync(1L, CancellationToken.None).AsTask() |> ignore)
            |> ignore

            held.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``oversized async reservations fail without registering a waiter`` () =
        let budget = new CapacityBudget 4L

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            budget.ReserveAsync(5L, CancellationToken.None).AsTask() |> ignore)
        |> ignore

        let state = snapshot budget
        state.CurrentBytes |> should equal 0L
        state.PeakBytes |> should equal 0L

    [<Fact>]
    let ``connection budgets keep logical directions independent and physical usage shared`` () =
        task {
            let limits = ConnectionCapacityLimits(2L, 3L, 4L)
            use accounting = new ConnectionCapacityAccounting(limits)
            let firstLogical = reserve accounting.ClientToBackendLogical 2L
            let secondLogical = accounting.BackendToClientLogical.ReserveAsync(3L, CancellationToken.None).AsTask()
            let physical = accounting.Physical.TrackArrayCapacity 4L
            let waitingPhysical =
                accounting.Physical.TrackArrayCapacityAsync(1L, CancellationToken.None).AsTask()

            secondLogical.IsCompletedSuccessfully |> should equal true
            waitingPhysical.IsCompleted |> should equal false

            let! secondLogicalReservation = secondLogical
            secondLogicalReservation.Release()
            physical.Dispose()

            let! resumedPhysical = waitingPhysical
            resumedPhysical.Dispose()
            firstLogical.Release()

            let final = accounting.GetSnapshot()
            final.ClientToBackendLogical.CurrentBytes |> should equal 0L
            final.BackendToClientLogical.CurrentBytes |> should equal 0L
            final.Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``logical waiters are resumed only by releases in their own direction`` () =
        task {
            use accounting = new ConnectionCapacityAccounting(ConnectionCapacityLimits(4L, 4L, 8L))
            let clientHeld = reserve accounting.ClientToBackendLogical 4L
            let backendHeld = reserve accounting.BackendToClientLogical 4L
            let waiting = accounting.ClientToBackendLogical.ReserveAsync(4L, CancellationToken.None).AsTask()

            waiting.IsCompleted |> should equal false
            backendHeld.Release()
            waiting.IsCompleted |> should equal false

            clientHeld.Release()
            let! clientReservation = waiting
            clientReservation.Release()
        }

    [<Fact>]
    let ``async owner admission disposes an owner when the request cannot fit`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 4L)
            let owner = new TrackingOwner(5)

            let! _ =
                Assert.ThrowsAsync<ArgumentOutOfRangeException>(fun () ->
                    accounting.TrackAsync(owner, CancellationToken.None).AsTask())

            owner.DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``async owner cancellation releases no capacity and disposes the owner`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 4L)
            let held = accounting.TrackArrayCapacity 4L
            let owner = new TrackingOwner(1)
            use cancellation = new CancellationTokenSource()
            let waiting = accounting.TrackAsync(owner, cancellation.Token).AsTask()

            cancellation.Cancel()
            let! _ = Assert.ThrowsAsync<TaskCanceledException>(fun () -> waiting)
            owner.DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 4L

            held.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``release dispatches a waiter registered at the release boundary`` () =
        task {
            let beforeEnqueue = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowEnqueue = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseCore = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue =
                      fun () ->
                          beforeEnqueue.TrySetResult(()) |> ignore
                          allowEnqueue.Task.GetAwaiter().GetResult()
                  AfterWaiterCountIncrement = ignore
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore = fun () -> releaseCore.TrySetResult(()) |> ignore
                  BeforeCleanupFailureTransition = ignore }

            let budget = new CapacityBudget(1L, hooks)
            let held = reserve budget 1L
            let waiting : Task<CapacityReservation> =
                Task.Run<CapacityReservation>(
                    Func<Task<CapacityReservation>>(fun () ->
                        budget.ReserveAsync(1L, CancellationToken.None).AsTask())
                )
            do! beforeEnqueue.Task

            let releaseTask = Task.Run(fun () -> held.Release())
            do! releaseCore.Task
            releaseTask.IsCompleted |> should equal false

            allowEnqueue.TrySetResult(()) |> ignore
            let! acquired = waiting
            do! releaseTask
            acquired.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``locked fast admission cannot bypass a waiter observed after its initial check`` () =
        task {
            let admissionPaused = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowAdmission = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let waiterEnqueued = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseCore = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowRelease = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable admissionHookUsed = 0
            let mutable pauseAdmission = 0

            let hooks =
                { BeforeAdmissionCommit =
                      fun () ->
                          if Volatile.Read(&pauseAdmission) = 1
                             && Interlocked.CompareExchange(&admissionHookUsed, 1, 0) = 0 then
                              admissionPaused.TrySetResult(()) |> ignore
                              allowAdmission.Task.GetAwaiter().GetResult()
                  BeforeWaiterEnqueue = fun () -> waiterEnqueued.TrySetResult(()) |> ignore
                  AfterWaiterCountIncrement = ignore
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore =
                      fun () ->
                          releaseCore.TrySetResult(()) |> ignore
                          allowRelease.Task.GetAwaiter().GetResult()
                  BeforeCleanupFailureTransition = ignore }

            let budget = new CapacityBudget(1L, hooks)
            let held = reserve budget 1L
            Volatile.Write(&pauseAdmission, 1)
            let admission = TaskCompletionSource<ValueOption<CapacityReservation>>(TaskCreationOptions.RunContinuationsAsynchronously)
            let admissionThread =
                Thread(ThreadStart(fun () ->
                    try
                        admission.TrySetResult(budget.TryReserve 1L) |> ignore
                    with error ->
                        admission.TrySetException(error) |> ignore))
            admissionThread.Start()

            do! admissionPaused.Task
            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            do! waiterEnqueued.Task
            waiting.IsCompleted |> should equal false

            // Pause after the release CAS but before the release-driven waiter
            // scan, then let the failed fast path revalidate first.
            let releaseThread =
                Thread(ThreadStart(fun () ->
                    held.Release()))
            releaseThread.Start()
            do! releaseCore.Task

            allowAdmission.TrySetResult(()) |> ignore
            let! fastResult = admission.Task
            admissionThread.Join()

            match fastResult with
            | ValueSome reservation ->
                reservation.Release()
                failwith "The paused fast admission bypassed the queued waiter."
            | ValueNone -> ()

            allowRelease.TrySetResult(()) |> ignore
            let! waiterReservation = waiting
            releaseThread.Join()
            waiterReservation.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``controller disposal racing release and cancellation leaves balanced usage`` () =
        task {
            for _ in 1 .. 40 do
                let budget = new CapacityBudget 1L
                let held = reserve budget 1L
                use cancellation = new CancellationTokenSource()
                let waiting = budget.ReserveAsync(1L, cancellation.Token).AsTask()
                use barrier = new Barrier(3)

                let releaseTask =
                    Task.Run(fun () ->
                        barrier.SignalAndWait() |> ignore
                        held.Release())

                let cancelTask =
                    Task.Run(fun () ->
                        barrier.SignalAndWait() |> ignore
                        cancellation.Cancel())

                let disposeTask =
                    Task.Run(fun () ->
                        barrier.SignalAndWait() |> ignore
                        (budget :> IDisposable).Dispose())

                do! Task.WhenAll([| releaseTask; cancelTask; disposeTask |])

                try
                    let! acquired = waiting
                    acquired.Release()
                with
                | :? OperationCanceledException
                | :? ObjectDisposedException -> ()

                (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``reservation aware rental waits before touching the pool`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 2L)
            let pool = new InstrumentedPool(2)
            let first = accounting.RentAsync(pool, 2, CancellationToken.None).AsTask()
            let! firstOwner = first
            let second = accounting.RentAsync(pool, 2, CancellationToken.None).AsTask()

            second.IsCompleted |> should equal false
            pool.RentCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 2L

            firstOwner.Dispose()
            let! secondOwner = second
            pool.RentCount |> should equal 2
            secondOwner.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``rental with unknown memory and disposal failures saturates capacity`` () =
        task {
            let budget = new CapacityBudget 1L
            let accounting = new ConnectionMemoryAccounting(budget)
            let owner = new MemoryInspectionOwner(1, true)
            let pool = new SuppliedOwnerPool(owner :> IMemoryOwner<byte>)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(pool, 1, CancellationToken.None).AsTask())
                )

            do! owner.MemoryStarted
            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            waiting.IsCompleted |> should equal false

            owner.AllowMemory()
            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message |> should equal "memory inspection failed"
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            owner.DisposeCount |> should equal 1

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)
            let saturated = accounting.GetSnapshot()
            saturated.CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            saturated.PeakPhysicalBufferCapacityBytes |> should equal Int64.MaxValue

            accounting.Dispose()
            accounting.Dispose()
            let futurePool = new InstrumentedPool(1)
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> accounting.RentAsync(futurePool, 1, CancellationToken.None).AsTask())
            futurePool.RentCount |> should equal 0
            (snapshot budget).CurrentBytes |> should equal Int64.MaxValue
        }

    [<Fact>]
    let ``synchronous tracking with unknown memory and disposal failures saturates capacity`` () =
        task {
            let budget = new CapacityBudget 1L
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = accounting.TrackArrayCapacity 1L
            let owner = new MemoryInspectionOwner(1, true)
            let first = Task.Run<IMemoryOwner<byte>>(Func<IMemoryOwner<byte>>(fun () -> accounting.Track(owner)))

            do! owner.MemoryStarted
            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            waiting.IsCompleted |> should equal false

            owner.AllowMemory()
            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message |> should equal "memory inspection failed"
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            owner.DisposeCount |> should equal 1

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)
            let saturated = accounting.GetSnapshot()
            saturated.CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            saturated.PeakPhysicalBufferCapacityBytes |> should equal Int64.MaxValue

            held.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            accounting.Dispose()
            accounting.Dispose()
            Assert.Throws<ObjectDisposedException>(fun () -> budget.TryReserve 1L |> ignore) |> ignore
        }

    [<Fact>]
    let ``asynchronous tracking with unknown memory and disposal failures saturates capacity`` () =
        task {
            let budget = new CapacityBudget 1L
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = accounting.TrackArrayCapacity 1L
            let owner = new MemoryInspectionOwner(1, true)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.TrackAsync(owner, CancellationToken.None).AsTask())
                )

            do! owner.MemoryStarted
            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            waiting.IsCompleted |> should equal false

            owner.AllowMemory()
            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message |> should equal "memory inspection failed"
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            owner.DisposeCount |> should equal 1

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)
            let saturated = accounting.GetSnapshot()
            saturated.CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            saturated.PeakPhysicalBufferCapacityBytes |> should equal Int64.MaxValue

            held.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue
            accounting.Dispose()
            accounting.Dispose()
            Assert.Throws<ObjectDisposedException>(fun () -> budget.TryReserve 1L |> ignore) |> ignore
        }

    [<Fact>]
    let ``reservation-aware unknown cleanup blocks admission until successful disposal`` () =
        runUnknownCleanupSuccess (fun accounting owner ->
            Task.Run(
                Func<Task>(fun () ->
                    task {
                        let! _ = accounting.RentAsync(new SuppliedOwnerPool(owner :> IMemoryOwner<byte>), 1, CancellationToken.None).AsTask()
                        return ()
                    })
            ))

    [<Fact>]
    let ``reservation-aware unknown cleanup failure saturates before admission`` () =
        runUnknownCleanupFailure (fun accounting owner ->
            Task.Run(
                Func<Task>(fun () ->
                    task {
                        let! _ = accounting.RentAsync(new SuppliedOwnerPool(owner :> IMemoryOwner<byte>), 1, CancellationToken.None).AsTask()
                        return ()
                    })
            ))

    [<Fact>]
    let ``synchronous tracking unknown cleanup blocks admission until successful disposal`` () =
        runUnknownCleanupSuccess (fun accounting owner ->
            Task.Run(
                Func<Task>(fun () ->
                    task {
                        accounting.Track(owner) |> ignore
                    })
            ))

    [<Fact>]
    let ``synchronous tracking unknown cleanup failure saturates before admission`` () =
        runUnknownCleanupFailure (fun accounting owner ->
            Task.Run(
                Func<Task>(fun () ->
                    task {
                        accounting.Track(owner) |> ignore
                    })
            ))

    [<Fact>]
    let ``asynchronous tracking unknown cleanup blocks admission until successful disposal`` () =
        runUnknownCleanupSuccess (fun accounting owner ->
            Task.Run(
                Func<Task>(fun () ->
                    task {
                        let! _ = accounting.TrackAsync(owner, CancellationToken.None).AsTask()
                        return ()
                    })
            ))

    [<Fact>]
    let ``asynchronous tracking unknown cleanup failure saturates before admission`` () =
        runUnknownCleanupFailure (fun accounting owner ->
            Task.Run(
                Func<Task>(fun () ->
                    task {
                        let! _ = accounting.TrackAsync(owner, CancellationToken.None).AsTask()
                        return ()
                    })
            ))

    [<Fact>]
    let ``tracked owner disposal failure closes physical admission terminally`` () =
        task {
            let waiterEnqueued = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue = fun () -> waiterEnqueued.TrySetResult(()) |> ignore
                  AfterWaiterCountIncrement = ignore
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore = ignore
                  BeforeCleanupFailureTransition = ignore }

            use accounting = new ConnectionMemoryAccounting(new CapacityBudget(8L, hooks))
            let held = accounting.TrackArrayCapacity 4L
            let owner = new ThrowingOwner(4)
            let tracked = accounting.Track(owner)

            let waitingPool = new InstrumentedPool(1)
            let waiting = accounting.RentAsync(waitingPool, 1, CancellationToken.None).AsTask()
            do! waiterEnqueued.Task
            waiting.IsCompleted |> should equal false

            let observed = Assert.Throws<InvalidOperationException>(fun () -> tracked.Dispose())
            observed.Message |> should equal "owner disposal failed"

            held.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 4L

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)
            waitingPool.RentCount |> should equal 0

            Assert.Throws<ObjectDisposedException>(fun () -> accounting.TrackArrayCapacity 1L |> ignore)
            |> ignore

            let futurePool = new InstrumentedPool(1)
            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(futurePool, 1, CancellationToken.None).AsTask())

            futurePool.RentCount |> should equal 0
            tracked.Dispose()
            owner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``rental with memory inspection failure and successful disposal releases provisional capacity`` () =
        task {
            let budget = new CapacityBudget 1L
            let accounting = new ConnectionMemoryAccounting(budget)
            let owner = new MemoryInspectionOwner(1, false)
            let pool = new SuppliedOwnerPool(owner :> IMemoryOwner<byte>)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(pool, 1, CancellationToken.None).AsTask())
                )

            do! owner.MemoryStarted
            owner.AllowMemory()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            owner.DisposeCount |> should equal 1
            let snapshot = accounting.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.PeakPhysicalBufferCapacityBytes |> should equal 1L
            let reservation = reserve budget 1L
            reservation.Release()
        }

    [<Fact>]
    let ``synchronous tracking with memory inspection failure and successful disposal stays open`` () =
        task {
            let budget = new CapacityBudget 1L
            let accounting = new ConnectionMemoryAccounting(budget)
            let owner = new MemoryInspectionOwner(1, false)
            let first = Task.Run<IMemoryOwner<byte>>(Func<IMemoryOwner<byte>>(fun () -> accounting.Track(owner)))

            do! owner.MemoryStarted
            owner.AllowMemory()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            owner.DisposeCount |> should equal 1
            let snapshot = accounting.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.PeakPhysicalBufferCapacityBytes |> should equal 0L
            let reservation = reserve budget 1L
            reservation.Release()
        }

    [<Fact>]
    let ``asynchronous tracking with memory inspection failure and successful disposal stays open`` () =
        task {
            let budget = new CapacityBudget 1L
            let accounting = new ConnectionMemoryAccounting(budget)
            let owner = new MemoryInspectionOwner(1, false)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.TrackAsync(owner, CancellationToken.None).AsTask())
                )

            do! owner.MemoryStarted
            owner.AllowMemory()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            owner.DisposeCount |> should equal 1
            let snapshot = accounting.GetSnapshot()
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            snapshot.PeakPhysicalBufferCapacityBytes |> should equal 0L
            let reservation = reserve budget 1L
            reservation.Release()
        }

    [<Fact>]
    let ``over rent expansion succeeds while an older larger waiter remains queued`` () =
        task {
            let budget = new CapacityBudget 100L
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = reserve budget 50L
            let largeWaiter = budget.ReserveAsync(60L, CancellationToken.None).AsTask()
            let pool = new InstrumentedPool(20)

            match budget.TryReserve 1L with
            | ValueSome reservation ->
                reservation.Release()
                failwith "A normal reservation bypassed the queued waiter."
            | ValueNone -> ()

            let! owner = accounting.RentAsync(pool, 10, CancellationToken.None).AsTask()

            largeWaiter.IsCompleted |> should equal false
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 70L
            pool.RentCount |> should equal 1

            owner.Dispose()
            owner.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 50L
            (pool.Owners[0] :?> TrackingOwner).DisposeCount |> should equal 1

            held.Release()
            let! largeReservation = largeWaiter
            largeReservation.Release()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``over rent expansion leaves a fitting waiter ahead of reconciliation`` () =
        task {
            let releaseStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowRelease = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue = ignore
                  AfterWaiterCountIncrement = ignore
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore =
                      fun () ->
                          releaseStarted.TrySetResult(()) |> ignore
                          allowRelease.Task.GetAwaiter().GetResult()
                  BeforeCleanupFailureTransition = ignore }

            let budget = new CapacityBudget(10L, hooks)
            let held = reserve budget 10L
            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            let releaseTask = Task.Run(fun () -> held.Release())
            do! releaseStarted.Task

            match budget.TryReserveExpansion 1L with
            | ExpansionReserved reservation ->
                reservation.Release()
                failwith "Expansion bypassed a waiter that already fit."
            | CleanupRequired cleanup -> cleanup.CompleteSuccessfully()

            allowRelease.TrySetResult(()) |> ignore
            do! releaseTask
            let! reservation = waiting
            reservation.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``reservation aware rental charges actual over rent`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 8L)
            let pool = new InstrumentedPool(8)
            let! owner = accounting.RentAsync(pool, 4, CancellationToken.None).AsTask()

            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L
            owner.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``failed over rent reconciliation cleans up without retrying`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 6L)
            let pool = new InstrumentedPool(8)

            let! _ =
                Assert.ThrowsAnyAsync<InvalidOperationException>(fun () ->
                    accounting.RentAsync(pool, 4, CancellationToken.None).AsTask())

            pool.RentCount |> should equal 1
            (pool.Owners[0] :?> TrackingOwner).DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L

            let lease = accounting.TrackArrayCapacity 6L
            lease.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``throwing cleanup during failed over rent preserves the actual uncertain capacity`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 6L)
            let owners = ResizeArray<ThrowingOwner>()

            let pool =
                { new MemoryPool<byte>() with
                    override _.MaxBufferSize = Int32.MaxValue

                    override _.Rent(_sizeHint) =
                        let owner = new ThrowingOwner(8)
                        owners.Add owner
                        owner :> IMemoryOwner<byte>

                    override _.Dispose(_disposing) = () }

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () ->
                    accounting.RentAsync(pool, 4, CancellationToken.None).AsTask())

            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message
            |> should equal "The actual pooled buffer capacity exceeded the remaining physical budget."
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            owners[0].DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L

            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(pool, 1, CancellationToken.None).AsTask())

            owners[0].DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L
        }

    [<Fact>]
    let ``blocking over rent cleanup fails closed before disposal throws`` () =
        task {
            let budget = new CapacityBudget 6L
            let accounting = new ConnectionMemoryAccounting(budget)
            let owner = new BlockingOwner(8, true)
            let firstPool = new SuppliedOwnerPool(owner :> IMemoryOwner<byte>)
            let secondPool = new InstrumentedPool(2)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(firstPool, 4, CancellationToken.None).AsTask())
                )

            do! owner.DisposeStarted
            let pending = accounting.GetSnapshot()
            pending.CurrentPhysicalBufferCapacityBytes |> should equal 8L
            pending.PeakPhysicalBufferCapacityBytes |> should equal 8L

            let second = accounting.RentAsync(secondPool, 2, CancellationToken.None).AsTask()
            second.IsCompleted |> should equal false
            secondPool.RentCount |> should equal 0

            owner.AllowDispose()

            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message
            |> should equal "The actual pooled buffer capacity exceeded the remaining physical budget."
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> second)
            firstPool.RentCount |> should equal 1
            owner.DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L

            let thirdPool = new InstrumentedPool(1)

            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(thirdPool, 1, CancellationToken.None).AsTask())

            secondPool.RentCount |> should equal 0
            thirdPool.RentCount |> should equal 0
            owner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``failed cleanup publishes closure without an intermediate open admission`` () =
        task {
            let transitionStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowTransition = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue = ignore
                  AfterWaiterCountIncrement = ignore
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore = ignore
                  BeforeCleanupFailureTransition =
                      fun () ->
                          transitionStarted.TrySetResult(()) |> ignore
                          allowTransition.Task.GetAwaiter().GetResult() }

            let budget = new CapacityBudget(10L, hooks)
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = reserve budget 6L
            let owner = new BlockingOwner(8, true)
            let firstPool = new SuppliedOwnerPool(owner :> IMemoryOwner<byte>)
            let secondPool = new InstrumentedPool(2)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(firstPool, 4, CancellationToken.None).AsTask())
                )

            do! owner.DisposeStarted
            held.Release()
            let second = accounting.RentAsync(secondPool, 2, CancellationToken.None).AsTask()
            second.IsCompleted |> should equal false
            secondPool.RentCount |> should equal 0

            owner.AllowDispose()
            do! transitionStarted.Task

            // Admission now takes the same gate as the terminal transition;
            // it must remain pending until the held failure publishes closure.
            let acquireTask = Task.Run(fun () -> budget.TryAcquire 1L)
            let reserveTask = Task.Run(fun () -> budget.TryReserve 1L)
            acquireTask.IsCompleted |> should equal false
            reserveTask.IsCompleted |> should equal false

            second.IsCompleted |> should equal false
            allowTransition.TrySetResult(()) |> ignore

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> acquireTask)
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> reserveTask)

            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message
            |> should equal "The actual pooled buffer capacity exceeded the remaining physical budget."
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"

            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> second)
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L
            firstPool.RentCount |> should equal 1
            secondPool.RentCount |> should equal 0
            owner.DisposeCount |> should equal 1

            let thirdPool = new InstrumentedPool(1)

            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(thirdPool, 1, CancellationToken.None).AsTask())

            thirdPool.RentCount |> should equal 0
        }

    [<Fact>]
    let ``overflow cleanup with successful disposal closes and releases provisional usage`` () =
        task {
            let budget = new CapacityBudget(Int64.MaxValue)
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = reserve budget (Int64.MaxValue - 4L)
            let owner = new TrackingOwner(8)
            let pool = new BlockingRentPool(owner :> IMemoryOwner<byte>)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(pool, 4, CancellationToken.None).AsTask())
                )
            do! pool.RentStarted

            let waitingPool = new InstrumentedPool(1)
            let waiting = accounting.RentAsync(waitingPool, 1, CancellationToken.None).AsTask()
            waiting.IsCompleted |> should equal false
            waitingPool.RentCount |> should equal 0

            pool.AllowRent()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)

            owner.DisposeCount |> should equal 1
            pool.RentCount |> should equal 1
            waitingPool.RentCount |> should equal 0
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes
            |> should equal (Int64.MaxValue - 4L)

            held.Release()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L

            let futurePool = new InstrumentedPool(1)

            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(futurePool, 1, CancellationToken.None).AsTask())

            futurePool.RentCount |> should equal 0

            let futureOwner = new TrackingOwner(1)
            Assert.Throws<ObjectDisposedException>(fun () -> accounting.Track(futureOwner) |> ignore) |> ignore
            futureOwner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``overflow cleanup with failed disposal preserves saturated uncertainty`` () =
        task {
            let budget = new CapacityBudget(Int64.MaxValue)
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = reserve budget (Int64.MaxValue - 4L)
            let owner = new ThrowingOwner(8)
            let pool = new BlockingRentPool(owner :> IMemoryOwner<byte>)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(pool, 4, CancellationToken.None).AsTask())
                )
            do! pool.RentStarted

            let waitingPool = new InstrumentedPool(1)
            let waiting = accounting.RentAsync(waitingPool, 1, CancellationToken.None).AsTask()
            waiting.IsCompleted |> should equal false

            pool.AllowRent()
            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> first)
            observed.InnerExceptions.Count |> should equal 2
            observed.InnerExceptions[0].Message |> should equal "Physical buffer capacity accounting overflowed."
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)

            owner.DisposeCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue

            held.Release()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal Int64.MaxValue

            let futurePool = new InstrumentedPool(1)

            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(futurePool, 1, CancellationToken.None).AsTask())

            futurePool.RentCount |> should equal 0
        }

    [<Fact>]
    let ``concurrent cleanup successes only reopen after the final handle`` () =
        task {
            let budget = new CapacityBudget 10L
            let held = reserve budget 10L

            let first =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The first cleanup unexpectedly reserved capacity."

            let second =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The second cleanup unexpectedly reserved capacity."

            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            waiting.IsCompleted |> should equal false

            use start = new Barrier(2)
            let firstTask =
                Task.Run(fun () ->
                    start.SignalAndWait() |> ignore
                    first.CompleteSuccessfully())
            let secondTask =
                Task.Run(fun () ->
                    start.SignalAndWait() |> ignore
                    second.CompleteSuccessfully())

            do! Task.WhenAll([| firstTask; secondTask |])
            waiting.IsCompleted |> should equal false
            (snapshot budget).CurrentBytes |> should equal 10L
            held.Release()
            let! reservation = waiting
            (snapshot budget).CurrentBytes |> should equal 1L
            reservation.Release()
            (snapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``cleanup failure remains terminal after another cleanup succeeds`` () =
        task {
            let budget = new CapacityBudget 10L
            let held = reserve budget 10L

            let failed =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The cleanup unexpectedly reserved capacity."

            let succeeding =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The cleanup unexpectedly reserved capacity."

            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            failed.CompleteFailure()
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)

            succeeding.CompleteSuccessfully()
            held.Release()
            (snapshot budget).CurrentBytes |> should equal 1L

            Assert.Throws<ObjectDisposedException>(fun () -> budget.TryReserve 1L |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``unknown capacity transition is idempotent and preserves overflow cleanup`` () =
        let budget = new CapacityBudget(Int64.MaxValue)
        let held = reserve budget (Int64.MaxValue - 1L)

        let cleanup =
            match budget.TryReserveExpansion 2L with
            | CleanupRequired cleanup -> cleanup
            | ExpansionReserved reservation ->
                reservation.Release()
                failwith "The overflow cleanup unexpectedly reserved capacity."

        budget.RecordUnknown()
        budget.RecordUnknown()
        cleanup.CompleteSuccessfully()
        held.Release()

        let snapshot = budget.GetSnapshot()
        snapshot.CurrentBytes |> should equal Int64.MaxValue
        snapshot.PeakBytes |> should equal Int64.MaxValue
        Assert.Throws<ObjectDisposedException>(fun () -> budget.TryReserve 1L |> ignore) |> ignore

    [<Fact>]
    let ``successful cleanup followed by failure remains terminal and conservatively charged`` () =
        task {
            let budget = new CapacityBudget 10L
            let held = reserve budget 10L

            let succeeding =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The cleanup unexpectedly reserved capacity."

            let failing =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The cleanup unexpectedly reserved capacity."

            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            succeeding.CompleteSuccessfully()
            waiting.IsCompleted |> should equal false
            (snapshot budget).CurrentBytes |> should equal 11L

            failing.CompleteFailure()
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)
            (snapshot budget).CurrentBytes |> should equal 11L
            held.Release()
            (snapshot budget).CurrentBytes |> should equal 1L
            Assert.Throws<ObjectDisposedException>(fun () -> budget.TryReserve 1L |> ignore) |> ignore
        }

    [<Fact>]
    let ``overlapping cleanup failure and success never expose an open admission`` () =
        task {
            let transitionStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let allowTransition = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforeAdmissionCommit = ignore
                  BeforeWaiterEnqueue = ignore
                  AfterWaiterCountIncrement = ignore
                  BeforeMaximumWaitDurationUpdate = ignore
                  AfterReleaseCore = ignore
                  BeforeCleanupFailureTransition =
                      fun () ->
                          transitionStarted.TrySetResult(()) |> ignore
                          allowTransition.Task.GetAwaiter().GetResult() }

            let budget = new CapacityBudget(10L, hooks)
            let held = reserve budget 10L

            let failing =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The cleanup unexpectedly reserved capacity."

            let succeeding =
                match budget.TryReserveExpansion 1L with
                | CleanupRequired cleanup -> cleanup
                | ExpansionReserved reservation ->
                    reservation.Release()
                    failwith "The cleanup unexpectedly reserved capacity."

            let waiting = budget.ReserveAsync(1L, CancellationToken.None).AsTask()
            let failureTask = Task.Run(fun () -> failing.CompleteFailure())
            do! transitionStarted.Task

            let successTask = Task.Run(fun () -> succeeding.CompleteSuccessfully())
            let admissionTask = Task.Run(fun () -> budget.TryReserve 1L)

            allowTransition.TrySetResult(()) |> ignore
            do! Task.WhenAll([| failureTask; successTask |])
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> waiting)
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> admissionTask)

            (snapshot budget).CurrentBytes |> should equal 11L
            held.Release()
            (snapshot budget).CurrentBytes |> should equal 1L
            Assert.Throws<ObjectDisposedException>(fun () -> budget.TryReserve 1L |> ignore) |> ignore
        }

    [<Fact>]
    let ``cleanup handles remain single completion objects`` () =
        let budget = new CapacityBudget 10L
        let held = reserve budget 10L

        let cleanup =
            match budget.TryReserveExpansion 1L with
            | CleanupRequired cleanup -> cleanup
            | ExpansionReserved reservation ->
                reservation.Release()
                failwith "The cleanup unexpectedly reserved capacity."

        cleanup.CompleteSuccessfully()
        Assert.Throws<InvalidOperationException>(fun () -> cleanup.CompleteSuccessfully()) |> ignore
        Assert.Throws<InvalidOperationException>(fun () -> cleanup.CompleteFailure()) |> ignore
        held.Release()
        (snapshot budget).CurrentBytes |> should equal 0L

    [<Fact>]
    let ``blocking successful over rent cleanup resumes waiters after release`` () =
        task {
            let budget = new CapacityBudget 6L
            let accounting = new ConnectionMemoryAccounting(budget)
            let held = reserve budget 2L
            let owner = new BlockingOwner(8, false)
            let firstPool = new SuppliedOwnerPool(owner :> IMemoryOwner<byte>)
            let secondPool = new InstrumentedPool(2)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(firstPool, 4, CancellationToken.None).AsTask())
                )

            do! owner.DisposeStarted
            let pending = accounting.GetSnapshot()
            pending.CurrentPhysicalBufferCapacityBytes |> should equal 10L
            pending.PeakPhysicalBufferCapacityBytes |> should equal 10L

            let second = accounting.RentAsync(secondPool, 2, CancellationToken.None).AsTask()
            second.IsCompleted |> should equal false
            secondPool.RentCount |> should equal 0

            let cancellation = new CancellationTokenSource()
            let cancelled = accounting.RentAsync(secondPool, 1, cancellation.Token).AsTask()
            cancellation.Cancel()
            let! _ = Assert.ThrowsAsync<TaskCanceledException>(fun () -> cancelled)

            held.Release()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L
            second.IsCompleted |> should equal false

            owner.AllowDispose()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            let! secondOwner = second

            secondPool.RentCount |> should equal 1
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 2L
            secondOwner.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            owner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``controller shutdown during blocking over rent cleanup does not reopen admission`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 6L)
            let owner = new BlockingOwner(8, false)
            let firstPool = new SuppliedOwnerPool(owner :> IMemoryOwner<byte>)
            let secondPool = new InstrumentedPool(2)

            let first =
                Task.Run<IMemoryOwner<byte>>(
                    Func<Task<IMemoryOwner<byte>>>(fun () ->
                        accounting.RentAsync(firstPool, 4, CancellationToken.None).AsTask())
                )

            do! owner.DisposeStarted
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 8L
            let second = accounting.RentAsync(secondPool, 2, CancellationToken.None).AsTask()
            (accounting :> IDisposable).Dispose()
            let! _ = Assert.ThrowsAsync<ObjectDisposedException>(fun () -> second)

            owner.AllowDispose()
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> first)
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L

            let thirdPool = new InstrumentedPool(1)

            let! _ =
                Assert.ThrowsAsync<ObjectDisposedException>(fun () ->
                    accounting.RentAsync(thirdPool, 1, CancellationToken.None).AsTask())

            thirdPool.RentCount |> should equal 0
            owner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``cancellation before reservation aware rental does not touch the pool`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 4L)
            let pool = new InstrumentedPool(2)
            use cancellation = new CancellationTokenSource()
            cancellation.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    accounting.RentAsync(pool, 2, cancellation.Token).AsTask())

            pool.RentCount |> should equal 0
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``throwing supplied owner cleanup remains secondary after cancellation`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 4L)
            let held = accounting.TrackArrayCapacity 4L
            let owner = new ThrowingOwner(1)
            use cancellation = new CancellationTokenSource()
            let waiting = accounting.TrackAsync(owner, cancellation.Token).AsTask()
            cancellation.Cancel()

            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> waiting)
            observed.InnerExceptions.Count |> should equal 2
            (observed.InnerExceptions[0] :? OperationCanceledException) |> should equal true
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 5L

            held.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 1L
        }

    [<Fact>]
    let ``throwing supplied owner cleanup remains secondary after controller shutdown`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 4L)
            let held = accounting.TrackArrayCapacity 4L
            let owner = new ThrowingOwner(1)
            let waiting = accounting.TrackAsync(owner, CancellationToken.None).AsTask()
            (accounting :> IDisposable).Dispose()

            let! observed = Assert.ThrowsAsync<AggregateException>(fun () -> waiting)
            observed.InnerExceptions.Count |> should equal 2
            (observed.InnerExceptions[0] :? ObjectDisposedException) |> should equal true
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 5L

            held.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 1L
        }

    [<Fact>]
    let ``throwing supplied owner cleanup is preserved after immediate admission failure`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 4L)
            let owner = new ThrowingOwner(5)

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () ->
                    accounting.TrackAsync(owner, CancellationToken.None).AsTask())

            observed.InnerExceptions.Count |> should equal 2
            (observed.InnerExceptions[0] :? ArgumentOutOfRangeException) |> should equal true
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 5L
        }

    [<Fact>]
    let ``processor push waits for physical admission before invoking a stage`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 2L)
            let held = accounting.TrackArrayCapacity 2L
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let pipeline =
                new ProcessorPipeline(
                    [| AdmissionProcessor(started, false) :> IByteStreamProcessor |],
                    ImmediateSink(),
                    MemoryPool<byte>.Shared,
                    accounting
                )

            let owner = new TrackingOwner(1)
            let pushing = pipeline.PushAsync(owner, 1, CancellationToken.None)
            pushing.IsCompleted |> should equal false
            started.Task.IsCompleted |> should equal false

            held.Dispose()
            do! pushing
            do! started.Task
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
        }

    [<Fact>]
    let ``synchronous generated output fails atomically when physical capacity is exhausted`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 1L)
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let pipeline =
                new ProcessorPipeline(
                    [| AdmissionProcessor(started, true) :> IByteStreamProcessor |],
                    ImmediateSink(),
                    MemoryPool<byte>.Shared,
                    accounting
                )

            let owner = new TrackingOwner(1)

            let! _ =
                Assert.ThrowsAnyAsync<InvalidOperationException>(fun () ->
                    pipeline.PushAsync(owner, 1, CancellationToken.None))

            started.Task.IsCompleted |> should equal true
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
        }

    [<Fact>]
    let ``connection physical shares hand off after one direction completes`` () =
        task {
            use accounting =
                new ConnectionCapacityAccounting(ConnectionCapacityLimits(64L, 64L, 65L))

            let clientLease = accounting.ClientToBackendPhysical.TrackArrayCapacity 32L
            let backendLease = accounting.BackendToClientPhysical.TrackArrayCapacity 32L

            Assert.Throws<InvalidOperationException>(fun () ->
                accounting.ClientToBackendPhysical.TrackArrayCapacity 1L |> ignore)
            |> ignore

            clientLease.Dispose()
            accounting.CompletePhysicalDirection true

            let reclaimed = accounting.BackendToClientPhysical.TrackArrayCapacity 33L
            accounting.GetSnapshot().Physical.CurrentPhysicalBufferCapacityBytes |> should equal 65L

            reclaimed.Dispose()
            backendLease.Dispose()
            accounting.GetSnapshot().Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``concurrent direction completion transfers its physical share once`` () =
        task {
            let expansionStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseExpansion = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable expansionCount = 0

            let hooks =
                { BeforePhysicalShareExpansion =
                      (fun clientToBackend ->
                          clientToBackend |> should equal true
                          Interlocked.Increment(&expansionCount) |> ignore
                          expansionStarted.TrySetResult(()) |> ignore
                          releaseExpansion.Task.GetAwaiter().GetResult())
                  AfterPhysicalShareExpansion = ignore }

            use accounting =
                new ConnectionCapacityAccounting(
                    ConnectionCapacityLimits(64L, 64L, 64L),
                    testHooks = hooks
                )

            let first = Task.Run(fun () -> accounting.CompletePhysicalDirection true)
            do! expansionStarted.Task

            let second = Task.Run(fun () -> accounting.CompletePhysicalDirection true)
            do! second
            Volatile.Read(&expansionCount) |> should equal 1
            accounting.GetPhysicalDirectionHandoffState true
            |> should equal PhysicalShareHandoffState.Committed

            releaseExpansion.TrySetResult(()) |> ignore
            do! first

            Volatile.Read(&expansionCount) |> should equal 1
            accounting.GetPhysicalDirectionHandoffState true
            |> should equal PhysicalShareHandoffState.Completed

            let fullShare = accounting.BackendToClientPhysical.TrackArrayCapacity 64L
            fullShare.Dispose()
            accounting.GetSnapshot().Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``committed physical share handoff wakes a waiting sibling without a false completion snapshot`` () =
        task {
            let committed =
                new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let hooks =
                { BeforePhysicalShareExpansion =
                      (fun clientToBackend ->
                          if not clientToBackend then
                              committed.TrySetResult(()) |> ignore
                              release.Task.GetAwaiter().GetResult())
                  AfterPhysicalShareExpansion = ignore }

            use accounting =
                new ConnectionCapacityAccounting(
                    ConnectionCapacityLimits(64L, 64L, 65L),
                    testHooks = hooks
                )

            let clientLease = accounting.ClientToBackendPhysical.TrackArrayCapacity 32L
            let backendLease = accounting.BackendToClientPhysical.TrackArrayCapacity 32L
            backendLease.Dispose()

            let completion =
                Task.Run(fun () -> accounting.CompletePhysicalDirection false)

            do! committed.Task
            accounting.GetPhysicalDirectionHandoffState false
            |> should equal PhysicalShareHandoffState.Committed

            let waiting =
                accounting.ClientToBackendPhysical.TrackArrayCapacityAsync(1L, CancellationToken.None).AsTask()

            waiting.IsCompleted |> should equal false
            accounting.IsPhysicalDirectionCompleted false |> should equal false

            release.TrySetResult(()) |> ignore
            do! completion
            let! transferred = waiting

            accounting.IsPhysicalDirectionCompleted false |> should equal true
            accounting.GetPhysicalDirectionHandoffState false
            |> should equal PhysicalShareHandoffState.Completed

            transferred.Dispose()
            clientLease.Dispose()
            accounting.GetSnapshot().Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``failed physical share handoff never publishes completion`` () =
        task {
            let hooks =
                { BeforePhysicalShareExpansion = ignore
                  AfterPhysicalShareExpansion =
                      (fun _ -> raise (InvalidOperationException("handoff publication failed"))) }

            use accounting =
                new ConnectionCapacityAccounting(
                    ConnectionCapacityLimits(64L, 64L, 65L),
                    testHooks = hooks
                )

            let observed =
                Assert.Throws<InvalidOperationException>(fun () ->
                    accounting.CompletePhysicalDirection false)

            observed.Message |> should equal "handoff publication failed"
            accounting.IsPhysicalDirectionCompleted false |> should equal false
            accounting.GetPhysicalDirectionHandoffState false
            |> should equal PhysicalShareHandoffState.Failed
        }

    [<Fact>]
    let ``receive rental retries a pooled over rent within directional headroom`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 32L)
            let pool = new RetryingReceivePool()

            let! owner = accounting.RentReceiveAsync(pool, 32, CancellationToken.None).AsTask()
            owner.Memory.Length |> should equal 16
            pool.RentCount |> should equal 3
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 16L

            owner.Dispose()
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Fact>]
    let ``receive rental fails closed when every pooled over rent is too large`` () =
        task {
            let accounting = new ConnectionMemoryAccounting(new CapacityBudget 32L)
            let pool = new InstrumentedPool(64)

            let! _ =
                Assert.ThrowsAnyAsync<InvalidOperationException>(fun () ->
                    accounting.RentReceiveAsync(pool, 32, CancellationToken.None).AsTask())

            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            pool.Owners
            |> Seq.forall (fun owner -> (owner :?> TrackingOwner).DisposeCount = 1)
            |> should equal true
        }

    [<Fact>]
    let ``directional receive admission cancellation releases provisional global credit`` () =
        task {
            use accounting =
                new ConnectionCapacityAccounting(ConnectionCapacityLimits(64L, 64L, 64L))

            let held = accounting.ClientToBackendPhysical.TrackArrayCapacity 32L
            use cancellation = new CancellationTokenSource()
            let waiting =
                accounting.ClientToBackendPhysical.RentReceiveAsync(
                    MemoryPool<byte>.Shared,
                    1,
                    cancellation.Token
                ).AsTask()

            cancellation.Cancel()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> waiting)

            accounting.GetSnapshot().Physical.CurrentPhysicalBufferCapacityBytes |> should equal 32L
            held.Dispose()
            accounting.GetSnapshot().Physical.CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }
