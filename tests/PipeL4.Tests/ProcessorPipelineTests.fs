namespace PipeL4.Tests

open System
open System.Buffers
open System.Diagnostics
open System.IO
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open Xunit

module ProcessorPipelineTests =
    let private bytes (text: string) = Encoding.ASCII.GetBytes(text)
    let private text (value: byte array) = Encoding.ASCII.GetString(value)

    let private assertDurationIncludes (held: TimeSpan) (recorded: TimeSpan) =
        let tolerance = TimeSpan.FromMilliseconds 20.0
        recorded |> should be (greaterThanOrEqualTo (held - tolerance))

    let private sequenceBytes (sequence: ReadOnlySequence<byte>) =
        let result = Array.zeroCreate<byte> (int sequence.Length)
        sequence.CopyTo(result.AsSpan())
        result

    let private sequenceSegmentCount (sequence: ReadOnlySequence<byte>) =
        let mutable position = sequence.Start
        let mutable segment = ReadOnlyMemory<byte>.Empty
        let mutable count = 0

        while sequence.TryGet(&position, &segment) do
            count <- count + 1

        count

    let private sequenceSegments (sequence: ReadOnlySequence<byte>) =
        let segments = ResizeArray<ReadOnlyMemory<byte>>()
        let mutable position = sequence.Start
        let mutable segment = ReadOnlyMemory<byte>.Empty

        while sequence.TryGet(&position, &segment) do
            segments.Add(segment)

        segments.ToArray()

    type private TrackingOwner(buffer: byte array) =
        let mutable disposeCount = 0

        member _.Buffer = buffer
        member _.DisposeCount = Volatile.Read(&disposeCount)

        interface IMemoryOwner<byte> with
            member _.Memory =
                if Volatile.Read(&disposeCount) <> 0 then
                    raise (ObjectDisposedException(nameof TrackingOwner))

                Memory<byte>(buffer)

            member _.Dispose() =
                if Interlocked.Increment(&disposeCount) = 1 then
                    buffer.AsSpan().Fill(221uy)

    type private ThrowingDisposeOwner(buffer: byte array) =
        let mutable disposeCount = 0

        member _.Buffer = buffer
        member _.DisposeCount = Volatile.Read(&disposeCount)

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore
                raise (InvalidOperationException("owner disposal failed"))

    type private BlockingDisposeOwner(buffer: byte array) =
        let disposeStarted =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let releaseDispose =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let mutable disposeCount = 0

        member _.Buffer = buffer
        member _.DisposeStarted = disposeStarted.Task
        member _.DisposeCount = Volatile.Read(&disposeCount)
        member _.ReleaseDispose() = releaseDispose.TrySetResult(()) |> ignore

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                if Interlocked.Increment(&disposeCount) = 1 then
                    disposeStarted.SetResult(())
                    releaseDispose.Task.GetAwaiter().GetResult()
                    buffer.AsSpan().Fill(221uy)

    type private TrackingPool(?minimumCapacity: int) =
        inherit MemoryPool<byte>()

        let owners = ResizeArray<TrackingOwner>()
        let minimumCapacity = defaultArg minimumCapacity 0

        member _.Owners = owners
        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            let owner =
                new TrackingOwner(
                    Array.zeroCreate<byte> (max minimumCapacity (max sizeHint 1))
                )

            owners.Add(owner)
            owner :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    type private BlockingPool(?minimumCapacity: int) =
        inherit MemoryPool<byte>()

        let owners = ResizeArray<BlockingDisposeOwner>()
        let rented = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let minimumCapacity = defaultArg minimumCapacity 0

        member _.Owners = owners
        member _.Rented = rented.Task
        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            let owner =
                new BlockingDisposeOwner(
                    Array.zeroCreate<byte> (max minimumCapacity (max sizeHint 1))
                )

            owners.Add(owner)
            rented.TrySetResult(()) |> ignore
            owner :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    type private RecordingSink() =
        let writes = ResizeArray<byte array>()
        let mutable completions = 0
        let mutable onWrite = ignore

        member _.Writes = writes
        member _.Bytes = writes |> Seq.toArray |> Array.concat
        member _.Completions = completions
        member _.OnWrite with set callback = onWrite <- callback

        interface IProcessorPipelineSink with
            member _.WriteAsync(value, _) =
                onWrite value
                writes.Add(sequenceBytes value)
                ValueTask()

            member _.CompleteAsync(_) =
                completions <- completions + 1
                ValueTask()

    let private owner textValue = new TrackingOwner(bytes textValue)

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

    type private BothDisposableProcessor
        (
            name: string,
            events: ResizeArray<string>,
            disposeFailure: exn option
        ) =
        interface IByteStreamProcessor with
            member _.ProcessAsync(input, output, _) =
                output.AppendInput(0L, input.Bytes.Length)
                completed 0L

        interface IDisposable with
            member _.Dispose() = events.Add($"sync:{name}")

        interface IAsyncDisposable with
            member _.DisposeAsync() =
                events.Add($"async:{name}")

                match disposeFailure with
                | Some error -> ValueTask(Task.FromException(error))
                | None -> ValueTask()

    type private ControlledDisposalProcessor
        (
            name: string,
            events: ResizeArray<string>,
            disposalStarted: TaskCompletionSource<unit>,
            releaseDisposal: TaskCompletionSource<unit>,
            disposalFailure: exn option
        ) =
        interface IByteStreamProcessor with
            member _.ProcessAsync(input, _, _) = completed input.Bytes.Length

        interface IAsyncDisposable with
            member _.DisposeAsync() =
                ValueTask(
                    task {
                        events.Add($"dispose:{name}")
                        disposalStarted.TrySetResult(()) |> ignore
                        do! releaseDisposal.Task

                        match disposalFailure with
                        | Some error -> return raise error
                        | None -> return ()
                    }
                )

    type private SyncDisposableProcessor(name: string, events: ResizeArray<string>) =
        interface IByteStreamProcessor with
            member _.ProcessAsync(input, output, _) =
                output.AppendInput(0L, input.Bytes.Length)
                completed 0L

        interface IDisposable with
            member _.Dispose() = events.Add($"sync:{name}")

    let private push (pipeline: ProcessorPipeline) (input: TrackingOwner) =
        pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None)

    let private retainedBytes (pipeline: ProcessorPipeline) =
        pipeline.GetStageMetricsSnapshot()
        |> List.map (fun snapshot -> snapshot.CurrentRetainedBytes)
        |> List.toArray

    let private physicalBytes (accounting: ConnectionMemoryAccounting) =
        accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes

    let private logicalSnapshot (budget: CapacityBudget) = budget.GetSnapshot()

    type private BlockingSink() =
        let started =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let release =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        member _.Started = started.Task
        member _.Release() = release.TrySetResult(()) |> ignore

        interface IProcessorPipelineSink with
            member _.WriteAsync(_, _) =
                ValueTask(
                    task {
                        started.TrySetResult(()) |> ignore
                        do! release.Task
                    }
                )

            member _.CompleteAsync(_) = ValueTask()

    type private BlockingRecordingSink() =
        let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable bytes = Array.empty<byte>
        let mutable completions = 0

        member _.Started = started.Task
        member _.Release() = release.TrySetResult(()) |> ignore
        member _.Bytes = bytes
        member _.Completions = Volatile.Read(&completions)

        interface IProcessorPipelineSink with
            member _.WriteAsync(value, _) =
                ValueTask(
                    task {
                        bytes <- sequenceBytes value
                        started.TrySetResult(()) |> ignore
                        do! release.Task
                    }
                )

            member _.CompleteAsync(_) =
                Interlocked.Increment(&completions) |> ignore
                ValueTask()

    [<NoEquality; NoComparison>]
    type private DetachedSnapshotProbe =
        { Snapshot: ConnectionMetricsSnapshot
          CapacityAfterTeardown: int64
          Pipeline: WeakReference
          Processor: WeakReference
          Owner: WeakReference }

    let private captureDetachedSnapshot () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let connection = ConnectionRuntimeMetrics(accounting)
            let metrics = ProcessorPipelineMetrics(1)
            connection.Publish(ClientToBackend, metrics)
            let sink = BlockingSink()

            let mutable stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            let mutable input = new TrackingOwner(Array.zeroCreate<byte> 32)
            let mutable pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, metrics)

            let mutable operation = Some(push pipeline input)
            do! sink.Started

            let snapshot = connection.GetSnapshot()
            let pipelineReference = WeakReference(pipeline)
            let processorReference = WeakReference(stage)
            let ownerReference = WeakReference(input)

            sink.Release()
            do! operation.Value
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()

            let result =
                { Snapshot = snapshot
                  CapacityAfterTeardown = accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes
                  Pipeline = pipelineReference
                  Processor = processorReference
                  Owner = ownerReference }

            operation <- None
            pipeline <- Unchecked.defaultof<_>
            stage <- Unchecked.defaultof<_>
            input <- Unchecked.defaultof<_>
            return result
        }

    let private collectUntilDead (references: WeakReference array) =
        let mutable attempt = 0

        while attempt < 20 && references |> Array.exists _.IsAlive do
            GC.Collect()
            GC.WaitForPendingFinalizers()
            GC.Collect()
            attempt <- attempt + 1

    [<Fact>]
    let ``physical capacity starts at zero and tracks actual lease capacity once`` () =
        let accounting = new ConnectionMemoryAccounting()
        let owner = new TrackingOwner(Array.zeroCreate<byte> 32)
        let tracked = accounting.Track(owner)

        physicalBytes accounting |> should equal 32L

        tracked.Dispose()
        physicalBytes accounting |> should equal 0L
        owner.DisposeCount |> should equal 1

        tracked.Dispose()
        physicalBytes accounting |> should equal 0L
        owner.DisposeCount |> should equal 1

    [<Fact>]
    let ``failed owner tracking disposes the rented owner once and preserves overflow`` () =
        let accounting = new ConnectionMemoryAccounting()
        accounting.Acquire Int64.MaxValue

        let owner = new ThrowingDisposeOwner(Array.zeroCreate<byte> 1)

        let error =
            Assert.Throws<AggregateException>(fun () ->
                accounting.Track(owner) |> ignore)

        error.InnerExceptions.Count |> should equal 2
        error.InnerExceptions[0].Message |> should equal "Physical buffer capacity accounting overflowed."
        error.InnerExceptions[1].Message |> should equal "owner disposal failed"
        owner.DisposeCount |> should equal 1
        physicalBytes accounting |> should equal Int64.MaxValue

        accounting.Release Int64.MaxValue
        physicalBytes accounting |> should equal Int64.MaxValue

    [<Fact>]
    let ``failed raw capacity registration does not create a lease`` () =
        let accounting = new ConnectionMemoryAccounting()
        accounting.Acquire Int64.MaxValue
        let buffer = ArrayPool<byte>.Shared.Rent 1

        try
            Assert.Throws<InvalidOperationException>(fun () ->
                accounting.TrackArrayCapacity(int64 buffer.LongLength) |> ignore)
            |> ignore

            physicalBytes accounting |> should equal Int64.MaxValue
        finally
            ArrayPool<byte>.Shared.Return(buffer, clearArray = false)
            accounting.Release Int64.MaxValue

        physicalBytes accounting |> should equal 0L

    [<Fact>]
    let ``owner disposal failure preserves conservative capacity accounting`` () =
        let accounting = new ConnectionMemoryAccounting()
        let owner = new ThrowingDisposeOwner(Array.zeroCreate<byte> 7)
        let tracked = accounting.Track(owner)

        let error = Assert.Throws<InvalidOperationException>(fun () -> tracked.Dispose())
        error.Message |> should equal "owner disposal failed"
        owner.DisposeCount |> should equal 1
        // A throwing owner leaves its physical release state ambiguous. The
        // runtime keeps the lease accounted and never retries that owner.
        physicalBytes accounting |> should equal 7L

        tracked.Dispose()
        owner.DisposeCount |> should equal 1
        physicalBytes accounting |> should equal 7L

    [<Fact>]
    let ``pipeline cleanup attempts every pending owner after the first release fails`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let sink = RecordingSink()
            let first = new ThrowingDisposeOwner(Array.zeroCreate<byte> 2)
            let middle = owner "middle"
            let last = owner "last"

            let retaining =
                processor (fun input _ _ -> completed input.Bytes.Length)

            let pipeline =
                new ProcessorPipeline([| retaining |], sink, MemoryPool<byte>.Shared, accounting)

            do! pipeline.PushAsync(first, first.Buffer.Length, CancellationToken.None)
            do! push pipeline middle
            do! push pipeline last

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())

            observed.Message |> should equal "owner disposal failed"
            first.DisposeCount |> should equal 1
            middle.DisposeCount |> should equal 1
            last.DisposeCount |> should equal 1
            physicalBytes accounting |> should equal 2L

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            first.DisposeCount |> should equal 1
            middle.DisposeCount |> should equal 1
            last.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``runtime cleanup publishes before external processor disposal`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let sink = RecordingSink()
            let input = new ThrowingDisposeOwner(Array.zeroCreate<byte> 4)
            let disposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releasePublication = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let published = TaskCompletionSource<exn>(TaskCreationOptions.RunContinuationsAsynchronously)
            let siblingCancelled = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            use siblingCancellation = new CancellationTokenSource()
            let events = ResizeArray<string>()

            let processor =
                ControlledDisposalProcessor(
                    "external",
                    events,
                    disposalStarted,
                    releaseDisposal,
                    None
                )

            let pipeline =
                new ProcessorPipeline(
                    [| processor :> IByteStreamProcessor |],
                    sink,
                    MemoryPool<byte>.Shared,
                    accounting
                )

            let sibling =
                task {
                    try
                        do! Task.Delay(Timeout.Infinite, siblingCancellation.Token)
                    with :? OperationCanceledException ->
                        siblingCancelled.TrySetResult(()) |> ignore
                }

            do! pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None)

            let disposal =
                Task.Run(
                    Func<Task>(fun () ->
                        pipeline
                            .DisposeAsync(
                                ignore,
                                fun error ->
                                    published.TrySetResult(error) |> ignore
                                    siblingCancellation.Cancel()
                                    releasePublication.Task.GetAwaiter().GetResult()
                            )
                            .AsTask())
                )

            let! publishedError = published.Task
            do! siblingCancelled.Task
            disposalStarted.Task.IsCompleted |> should equal false
            publishedError.Message |> should equal "owner disposal failed"

            releasePublication.TrySetResult(()) |> ignore
            releaseDisposal.TrySetResult(()) |> ignore
            let! observed = Assert.ThrowsAsync<InvalidOperationException>(fun () -> disposal)
            observed.Message |> should equal "owner disposal failed"
            events |> Seq.toList |> should equal [ "dispose:external" ]
            input.DisposeCount |> should equal 1
            physicalBytes accounting |> should equal 4L
            do! sibling
        }

    [<Fact>]
    let ``processor disposal publishes each failure before later disposal`` () =
        task {
            let sink = RecordingSink()
            let events = ResizeArray<string>()
            let firstStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseFirst = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releasePublication = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let laterStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseLater = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let firstFailure = InvalidOperationException("first processor disposal failed")
            let notifications = ResizeArray<exn>()
            let firstPublished = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let later =
                ControlledDisposalProcessor(
                    "later",
                    events,
                    laterStarted,
                    releaseLater,
                    None
                )

            let first =
                ControlledDisposalProcessor(
                    "first",
                    events,
                    firstStarted,
                    releaseFirst,
                    Some firstFailure
                )

            let pipeline =
                new ProcessorPipeline(
                    [| later :> IByteStreamProcessor; first :> IByteStreamProcessor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            releaseFirst.TrySetResult(()) |> ignore

            let disposal =
                Task.Run(
                    Func<Task>(fun () ->
                        pipeline
                            .DisposeAsync(
                                ignore,
                                fun error ->
                                    notifications.Add(error)
                                    firstPublished.TrySetResult(()) |> ignore
                                    releasePublication.Task.GetAwaiter().GetResult()
                            )
                            .AsTask())
                )

            do! firstPublished.Task
            laterStarted.Task.IsCompleted |> should equal false
            notifications |> Seq.map _.Message |> Seq.toList |> should equal [ firstFailure.Message ]

            releasePublication.TrySetResult(()) |> ignore
            releaseLater.TrySetResult(()) |> ignore
            let! observed = Assert.ThrowsAsync<InvalidOperationException>(fun () -> disposal)
            Object.ReferenceEquals(observed, firstFailure) |> should equal true
            events |> Seq.toList |> should equal [ "dispose:first"; "dispose:later" ]
            notifications.Count |> should equal 1

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            events |> Seq.toList |> should equal [ "dispose:first"; "dispose:later" ]
        }

    [<Fact>]
    let ``pipeline aggregates every pending cleanup failure`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let budget = new CapacityBudget 16L
            let sink = RecordingSink()
            let first = new ThrowingDisposeOwner(Array.zeroCreate<byte> 2)
            let second = new ThrowingDisposeOwner(Array.zeroCreate<byte> 3)
            let third = new ThrowingDisposeOwner(Array.zeroCreate<byte> 5)

            let retaining =
                processor (fun input _ _ -> completed input.Bytes.Length)

            let pipeline =
                new ProcessorPipeline([| retaining |], sink, MemoryPool<byte>.Shared, accounting, budget)

            do! pipeline.PushAsync(first, first.Buffer.Length, CancellationToken.None)
            do! pipeline.PushAsync(second, second.Buffer.Length, CancellationToken.None)
            do! pipeline.PushAsync(third, third.Buffer.Length, CancellationToken.None)

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () ->
                    (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())

            observed.InnerExceptions.Count |> should equal 3
            observed.InnerExceptions
            |> Seq.forall (fun error -> error.Message = "owner disposal failed")
            |> should equal true
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
            third.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 10L

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
            third.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 10L
        }

    [<Fact>]
    let ``processor failure preserves its error when intermediate cleanup also fails`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let budget = new CapacityBudget 16L
            let sink = RecordingSink()
            let first = owner "first"
            let failingOwner = new ThrowingDisposeOwner(Array.zeroCreate<byte> 3)
            let last = owner "last"
            let expected = InvalidOperationException("processor failed")

            let mutable invocation = 0

            let stage =
                processor (fun input _ _ ->
                    invocation <- invocation + 1

                    if invocation < 3 then
                        completed input.Bytes.Length
                    else
                        raise expected)

            let pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            do! push pipeline first
            do! pipeline.PushAsync(failingOwner, failingOwner.Buffer.Length, CancellationToken.None)

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () ->
                    pipeline.PushAsync(last, last.Buffer.Length, CancellationToken.None))

            observed.InnerExceptions
            |> Seq.exists (fun error -> obj.ReferenceEquals(error, expected))
            |> should equal true

            observed.InnerExceptions
            |> Seq.exists (fun error -> error.Message = "owner disposal failed")
            |> should equal true

            first.DisposeCount |> should equal 1
            failingOwner.DisposeCount |> should equal 1
            last.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 3L

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            first.DisposeCount |> should equal 1
            failingOwner.DisposeCount |> should equal 1
            last.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 3L
        }

    [<Fact>]
    let ``array-pool capacity returns to zero across repeated rental cycles`` () =
        let accounting = new ConnectionMemoryAccounting()

        for _ in 1 .. 5 do
            let buffer = ArrayPool<byte>.Shared.Rent 1024
            let capacity = int64 buffer.LongLength
            let lease = accounting.TrackArrayCapacity capacity

            physicalBytes accounting |> should equal capacity
            ArrayPool<byte>.Shared.Return(buffer, clearArray = false)
            lease.Dispose()
            physicalBytes accounting |> should equal 0L

    [<Fact>]
    let ``physical capacity sums independent leases and array-pool leases`` () =
        let accounting = new ConnectionMemoryAccounting()
        let first = accounting.Track(new TrackingOwner(Array.zeroCreate<byte> 17))
        let second = accounting.Track(new TrackingOwner(Array.zeroCreate<byte> 29))
        let pooledBuffer = ArrayPool<byte>.Shared.Rent 4096
        let pooledCapacity = int64 pooledBuffer.LongLength
        let pooled = accounting.TrackArrayCapacity pooledCapacity
        let mutable returned = false

        try
            physicalBytes accounting |> should equal (46L + pooledCapacity)

            first.Dispose()
            physicalBytes accounting |> should equal (29L + pooledCapacity)
            second.Dispose()
            physicalBytes accounting |> should equal pooledCapacity

            ArrayPool<byte>.Shared.Return(pooledBuffer, clearArray = false)
            returned <- true
            pooled.Dispose()
            physicalBytes accounting |> should equal 0L
        finally
            if not returned then
                ArrayPool<byte>.Shared.Return(pooledBuffer, clearArray = false)

            pooled.Dispose()

    [<Fact>]
    let ``a tracked lease cannot move between connection accounting instances`` () =
        let firstAccounting = new ConnectionMemoryAccounting()
        let secondAccounting = new ConnectionMemoryAccounting()
        let owner = new TrackingOwner(Array.zeroCreate<byte> 13)
        let tracked = firstAccounting.Track(owner)

        Assert.Throws<InvalidOperationException>(fun () ->
            secondAccounting.Track(tracked) |> ignore)
        |> ignore

        physicalBytes firstAccounting |> should equal 13L
        physicalBytes secondAccounting |> should equal 0L

        tracked.Dispose()
        physicalBytes firstAccounting |> should equal 0L

    [<Fact>]
    let ``pipeline physical snapshot keeps full retained backing capacity`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let stage =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 1L)

            let input = new TrackingOwner(Array.zeroCreate<byte> 64)
            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting)

            do! pipeline.PushAsync(input, 8, CancellationToken.None)
            physicalBytes accounting |> should equal 64L
            retainedBytes pipeline |> should equal [| 1L |]

            do! pipeline.CompleteAsync(CancellationToken.None)
            physicalBytes accounting |> should equal 0L
        }

    [<Fact>]
    let ``mixed borrowed and generated output counts distinct active leases`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let sink = new BlockingSink()
            let input = new TrackingOwner(Array.zeroCreate<byte> 32)
            let pool = new TrackingPool(minimumCapacity = 16)

            let stage =
                processor (fun currentInput output _ ->
                    output.AppendInput(0L, 1L)
                    emit output (bytes "XYZ")
                    output.AppendInput(1L, 1L)
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting)

            let operation = pipeline.PushAsync(input, 2, CancellationToken.None)
            do! sink.Started

            physicalBytes accounting |> should equal 48L
            input.DisposeCount |> should equal 0
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 0

            sink.Release()
            do! operation

            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``logical occupancy charges transactional output and releases after sink consumption`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new BlockingSink()
            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            let operation = push pipeline (owner "ABC")
            do! sink.Started

            let pending = logicalSnapshot budget
            pending.CurrentBytes |> should equal 3L
            pending.PeakBytes |> should equal 6L

            sink.Release()
            do! operation
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``logical ownership transfers between stages without duplicate current charge`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let started = new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let first =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            let second =
                processor (fun input output _ ->
                    ValueTask<ProcessorResult>(
                        task {
                            started.SetResult(())
                            do! release.Task
                            output.AppendInput(0L, input.Bytes.Length)
                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline =
                new ProcessorPipeline([| first; second |], sink, MemoryPool<byte>.Shared, accounting, budget)

            let operation = push pipeline (owner "ABC")
            do! started.Task

            (logicalSnapshot budget).CurrentBytes |> should equal 3L
            (logicalSnapshot budget).PeakBytes |> should equal 6L

            release.SetResult(())
            do! operation
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            sink.Bytes |> text |> should equal "ABC"
        }

    [<Fact>]
    let ``partial retained input releases only the consumed logical prefix`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let mutable calls = 0

            let stage =
                processor (fun input output _ ->
                    calls <- calls + 1

                    if calls = 1 then
                        completed 2L
                    else
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            do! pipeline.PushAsync(owner "ABCD", 4, CancellationToken.None)
            (logicalSnapshot budget).CurrentBytes |> should equal 2L

            do! pipeline.PushAsync(owner "EF", 2, CancellationToken.None)
            let final = logicalSnapshot budget
            final.CurrentBytes |> should equal 0L
            final.PeakBytes |> should equal 8L
            sink.Bytes |> text |> should equal "CDEF"
        }

    [<Fact>]
    let ``logical expansion remains charged independently of physical storage`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new BlockingSink()
            let pool = new TrackingPool()
            let invocationStarted = new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let invocationRelease = new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable duringAdmission = 0L

            let stage =
                processor (fun _ output _ ->
                    duringAdmission <- (logicalSnapshot budget).CurrentBytes
                    emit output (bytes "XYZ")
                    duringAdmission <- (logicalSnapshot budget).CurrentBytes
                    ValueTask<ProcessorResult>(
                        task {
                            invocationStarted.SetResult(())
                            do! invocationRelease.Task
                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting, budget)

            let operation = push pipeline (owner "AB")
            do! invocationStarted.Task

            duringAdmission |> should equal 5L
            let pending = logicalSnapshot budget
            pending.CurrentBytes |> should equal 5L
            pending.PeakBytes |> should equal 5L

            invocationRelease.SetResult(())
            do! sink.Started
            (logicalSnapshot budget).CurrentBytes |> should equal 3L
            physicalBytes accounting |> should equal (int64 pool.Owners[0].Buffer.Length)

            sink.Release()
            do! operation
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``logical contraction charges only generated output while the sink is pending`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new BlockingSink()
            let started = new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = new TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let stage =
                processor (fun _ output _ ->
                    emit output (bytes "X")

                    ValueTask<ProcessorResult>(
                        task {
                            started.SetResult(())
                            do! release.Task
                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            let operation = push pipeline (owner "ABCD")
            do! started.Task
            (logicalSnapshot budget).CurrentBytes |> should equal 5L
            (logicalSnapshot budget).PeakBytes |> should equal 5L

            release.SetResult(())
            do! sink.Started
            (logicalSnapshot budget).CurrentBytes |> should equal 1L
            sink.Release()
            do! operation
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``repeated borrowed ranges charge each logical occurrence but share physical storage`` () =
        task {
            let budget = new CapacityBudget 32L
            let accounting = new ConnectionMemoryAccounting()
            let sink = BlockingSink()
            let input = owner "ABC"

            let stage =
                processor (fun currentInput output _ ->
                    for _ in 1 .. 3 do
                        output.AppendInput(0L, currentInput.Bytes.Length)

                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            let operation = push pipeline input
            do! sink.Started

            (logicalSnapshot budget).CurrentBytes |> should equal 9L
            physicalBytes accounting |> should equal 3L

            sink.Release()
            do! operation
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``logical input admission waits for viable capacity and resumes`` () =
        task {
            let budget = new CapacityBudget 8L
            let held = budget.ReserveAsync(6L, CancellationToken.None).AsTask()
            let! heldReservation = held
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            let operation = push pipeline (owner "ABC")
            operation.IsCompleted |> should equal false

            heldReservation.Release()
            do! operation
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            sink.Bytes |> text |> should equal "ABC"
        }

    [<Fact>]
    let ``cancellation during logical input admission cleans the owner`` () =
        task {
            let budget = new CapacityBudget 8L
            let! held = budget.ReserveAsync(6L, CancellationToken.None).AsTask()
            use cancellation = new CancellationTokenSource()
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let stage = processor (fun input output _ -> output.AppendInput(0L, input.Bytes.Length); completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            let input = owner "ABC"
            let operation = pipeline.PushAsync(input, input.Buffer.Length, cancellation.Token)
            operation.IsCompleted |> should equal false
            cancellation.Cancel()

            let! observed = Assert.ThrowsAsync<TaskCanceledException>(fun () -> operation)
            observed |> should not' (be null)
            input.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 6L

            held.Release()
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``retained input that cannot admit another occurrence faults instead of waiting`` () =
        task {
            let budget = new CapacityBudget 5L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let stage = processor (fun _ _ _ -> completed 3L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            do! pipeline.PushAsync(owner "ABC", 3, CancellationToken.None)
            (logicalSnapshot budget).CurrentBytes |> should equal 3L

            let second = owner "DEF"
            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.PushAsync(second, second.Buffer.Length, CancellationToken.None))

            observed.Message |> should haveSubstring "prevents progress"
            second.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``synchronous generated output exhaustion is atomic`` () =
        task {
            let budget = new CapacityBudget 4L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let pool = new TrackingPool()
            let stage =
                processor (fun _ output _ ->
                    emit output (bytes "WXYZ")
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting, budget)

            let input = owner "AB"
            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None))

            observed.Message |> should haveSubstring "generated output occurrence"
            input.DisposeCount |> should equal 1
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 0L
            sink.Bytes |> should be Empty
        }

    [<Fact>]
    let ``shared physical input remains counted once across repeated ranges and stages`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let input = new TrackingOwner(Array.zeroCreate<byte> 48)

            let first =
                processor (fun currentInput output _ ->
                    if currentInput.IsCompleted then
                        completed 0L
                    else
                        output.AppendInput(0L, currentInput.Bytes.Length)
                        output.AppendInput(0L, currentInput.Bytes.Length)
                        completed currentInput.Bytes.Length)

            let second =
                processor (fun currentInput output _ ->
                    if currentInput.IsCompleted then
                        completed 0L
                    else
                        output.AppendInput(0L, currentInput.Bytes.Length)
                        completed 0L)

            use pipeline =
                new ProcessorPipeline([| first; second |], sink, MemoryPool<byte>.Shared, accounting)

            do! pipeline.PushAsync(input, 5, CancellationToken.None)
            physicalBytes accounting |> should equal 48L
            input.DisposeCount |> should equal 0

            do! pipeline.CompleteAsync(CancellationToken.None)
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``physical capacity remains while asynchronous invocation owns input`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let started =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let release =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let sink = new RecordingSink()
            let input = new TrackingOwner(Array.zeroCreate<byte> 40)

            let stage =
                processor (fun currentInput _ _ ->
                    ValueTask<ProcessorResult>(
                        task {
                            started.SetResult(())
                            do! release.Task
                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting)

            let operation = pipeline.PushAsync(input, 7, CancellationToken.None)
            do! started.Task

            physicalBytes accounting |> should equal 40L
            input.DisposeCount |> should equal 0

            release.SetResult(())
            do! operation
            physicalBytes accounting |> should equal 0L
        }

    [<Fact>]
    let ``shared connection accounting survives one direction completing`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let firstSink = new RecordingSink()
            let secondSink = new BlockingSink()
            let firstInput = new TrackingOwner(Array.zeroCreate<byte> 19)
            let secondInput = new TrackingOwner(Array.zeroCreate<byte> 23)

            let passThrough =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use first =
                new ProcessorPipeline([| passThrough |], firstSink, MemoryPool<byte>.Shared, accounting)

            use second =
                new ProcessorPipeline([| passThrough |], secondSink, MemoryPool<byte>.Shared, accounting)

            let firstOperation = first.PushAsync(firstInput, 3, CancellationToken.None)
            let secondOperation = second.PushAsync(secondInput, 3, CancellationToken.None)
            do! secondSink.Started
            do! firstOperation

            physicalBytes accounting |> should equal 23L
            firstInput.DisposeCount |> should equal 1
            secondInput.DisposeCount |> should equal 0

            secondSink.Release()
            do! secondOperation
            physicalBytes accounting |> should equal 0L
            secondInput.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``concurrent accounting updates and snapshots remain non-negative`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()

            let leases =
                [| for index in 1 .. 32 do
                       yield accounting.Track(new TrackingOwner(Array.zeroCreate<byte> (index + 1))) |]

            let readers =
                [| for _ in 1 .. 4 do
                       yield
                           Task.Run(
                               Func<Task>(fun () ->
                                   task {
                                       for _ in 1 .. 500 do
                                           physicalBytes accounting |> should be (greaterThanOrEqualTo 0L)
                                   })
                           ) |]

            let releasers =
                leases
                |> Array.map (fun lease ->
                    Task.Run(
                        Action(fun () -> lease.Dispose())
                    ))

            do! Task.WhenAll(Array.append readers releasers)
            physicalBytes accounting |> should equal 0L
        }

    [<Fact>]
    let ``physical capacity is cleared after processor failure`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let input = new TrackingOwner(Array.zeroCreate<byte> 27)
            let expected = InvalidOperationException("processor failed")

            let stage =
                processor (fun currentInput output _ ->
                    output.AppendInput(0L, currentInput.Bytes.Length)
                    emit output (bytes "generated")
                    raise expected)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool, accounting)

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.PushAsync(input, 4, CancellationToken.None))

            Object.ReferenceEquals(expected, observed) |> should equal true
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``unadvanced generated output is released from physical capacity`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let input = new TrackingOwner(Array.zeroCreate<byte> 25)

            let stage =
                processor (fun _ output _ ->
                    output.GetMemory(11) |> ignore
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool, accounting)

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.PushAsync(input, 3, CancellationToken.None))

            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``processor failure releases logical output and input before generated owner disposal`` () =
        task {
            let expected = InvalidOperationException("processor failed")
            let budget = new CapacityBudget 32L
            let accounting = new ConnectionMemoryAccounting()
            let pool = new BlockingPool()
            let sink = new RecordingSink()

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    emit output (bytes "XYZ")
                    raise expected)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting, budget)

            let input = owner "AB"
            let operation =
                Task.Run(
                    Func<Task>(fun () ->
                        pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None))
                )

            do! pool.Rented
            do! pool.Owners[0].DisposeStarted
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 5L
            input.DisposeCount |> should equal 0

            pool.Owners[0].ReleaseDispose()
            let! observed = Assert.ThrowsAsync<InvalidOperationException>(fun () -> operation)
            Object.ReferenceEquals(expected, observed) |> should equal true
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``multi-stage disposal releases every logical range before an earlier owner unblocks`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let input = new BlockingDisposeOwner(bytes "A")

            let first =
                processor (fun currentInput output _ ->
                    emit output (bytes "X")
                    completed currentInput.Bytes.Length)

            let second = processor (fun currentInput _ _ -> completed currentInput.Bytes.Length)

            let pipeline =
                new ProcessorPipeline([| first; second |], sink, pool, accounting, budget)

            do! pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None)
            (logicalSnapshot budget).CurrentBytes |> should equal 2L

            let disposal =
                Task.Run(
                    Func<Task>(fun () ->
                        (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())
                )

            do! input.DisposeStarted
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 2L
            pool.Owners[0].DisposeCount |> should equal 0

            input.ReleaseDispose()
            do! disposal
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``cancellation releases logical state before generated owner disposal`` () =
        task {
            use cancellation = new CancellationTokenSource()
            use timeout = new CancellationTokenSource(5000: int)
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let budget = new CapacityBudget 32L
            let accounting = new ConnectionMemoryAccounting()
            let pool = new BlockingPool()
            let sink = new RecordingSink()

            let stage =
                processor (fun _ output cancellationToken ->
                    emit output (bytes "XYZ")

                    ValueTask<ProcessorResult>(
                        task {
                            started.SetResult(())
                            do! Task.Delay(Timeout.Infinite, cancellationToken)
                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting, budget)

            let input = owner "AB"
            let operation = pipeline.PushAsync(input, input.Buffer.Length, cancellation.Token)
            do! started.Task.WaitAsync(timeout.Token)
            cancellation.Cancel()
            do! pool.Rented.WaitAsync(timeout.Token)
            do! pool.Owners[0].DisposeStarted.WaitAsync(timeout.Token)

            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 5L

            pool.Owners[0].ReleaseDispose()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``segmented partial consumption releases logical prefix before first owner disposal`` () =
        task {
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let first = new BlockingDisposeOwner(bytes "A")
            let second = owner "BC"
            let mutable invocation = 0

            let stage =
                processor (fun input output _ ->
                    invocation <- invocation + 1

                    match invocation with
                    | 1 -> completed input.Bytes.Length
                    | 2 -> completed 1L
                    | 3 ->
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L
                    | _ -> failwith "Unexpected processor invocation.")

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting, budget)

            do! pipeline.PushAsync(first, first.Buffer.Length, CancellationToken.None)
            let operation =
                Task.Run(
                    Func<Task>(fun () ->
                        pipeline.PushAsync(second, second.Buffer.Length, CancellationToken.None))
                )

            do! first.DisposeStarted
            (logicalSnapshot budget).CurrentBytes |> should equal 1L
            physicalBytes accounting |> should equal 3L

            first.ReleaseDispose()
            do! operation
            let fourth = owner "D"
            do! pipeline.PushAsync(fourth, fourth.Buffer.Length, CancellationToken.None)

            sink.Bytes |> text |> should equal "CD"
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 0L
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
            fourth.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``physical capacity is cleared after cancellation`` () =
        task {
            use cancellation = new CancellationTokenSource()
            let accounting = new ConnectionMemoryAccounting()
            let started =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let sink = new RecordingSink()
            let input = new TrackingOwner(Array.zeroCreate<byte> 31)

            let stage =
                processor (fun _ _ cancellationToken ->
                    ValueTask<ProcessorResult>(
                        task {
                            started.SetResult(())
                            do! Task.Delay(Timeout.Infinite, cancellationToken)
                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting)

            let operation = pipeline.PushAsync(input, 6, cancellation.Token)
            do! started.Task
            cancellation.Cancel()

            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``empty pipeline passes through the owned input and completes`` () =
        task {
            let input = owner "ABC"
            let sink = new RecordingSink()
            let mutable sameBuffer = false

            sink.OnWrite <- fun sequence ->
                let mutable segment = Unchecked.defaultof<ArraySegment<byte>>
                MemoryMarshal.TryGetArray(sequence.First, &segment) |> should equal true
                sameBuffer <- Object.ReferenceEquals(segment.Array, input.Buffer)
                input.DisposeCount |> should equal 0

            use pipeline = new ProcessorPipeline([||], sink, MemoryPool<byte>.Shared)
            do! push pipeline input
            do! pipeline.CompleteAsync(CancellationToken.None)

            sameBuffer |> should equal true
            sink.Bytes |> text |> should equal "ABC"
            sink.Completions |> should equal 1
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``stage metrics start at zero and remain zero for a pass-through stage`` () =
        task {
            let sink = new RecordingSink()

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)

            let initial = pipeline.GetStageMetricsSnapshot()
            initial |> List.map (fun snapshot -> snapshot.StageIndex) |> should equal [ 0 ]
            retainedBytes pipeline |> should equal [| 0L |]
            initial[0].InputBytes |> should equal 0L
            initial[0].InputBackedOutputBytes |> should equal 0L
            initial[0].GeneratedOutputBytes |> should equal 0L
            initial[0].InvocationCount |> should equal 0L
            initial[0].SuccessfulInvocationCount |> should equal 0L
            initial[0].FailedInvocationCount |> should equal 0L
            initial[0].CancelledInvocationCount |> should equal 0L
            initial[0].ProcessingDuration |> should equal TimeSpan.Zero

            do! push pipeline (owner "ABC")
            retainedBytes pipeline |> should equal [| 0L |]

            let afterInput = pipeline.GetStageMetricsSnapshot()[0]
            afterInput.InputBytes |> should equal 3L
            afterInput.InputBackedOutputBytes |> should equal 3L
            afterInput.GeneratedOutputBytes |> should equal 0L
            afterInput.InvocationCount |> should equal 1L
            afterInput.SuccessfulInvocationCount |> should equal 1L
            afterInput.FailedInvocationCount |> should equal 0L
            afterInput.CancelledInvocationCount |> should equal 0L
            afterInput.ProcessingDuration |> should be (greaterThanOrEqualTo TimeSpan.Zero)

            do! pipeline.CompleteAsync(CancellationToken.None)
            retainedBytes pipeline |> should equal [| 0L |]

            let afterEof = pipeline.GetStageMetricsSnapshot()[0]
            afterEof.InputBytes |> should equal 3L
            afterEof.InputBackedOutputBytes |> should equal 3L
            afterEof.InvocationCount |> should equal 2L
            afterEof.SuccessfulInvocationCount |> should equal 2L
        }

    [<Fact>]
    let ``empty pipeline returns an empty stage metrics snapshot`` () =
        let sink = new RecordingSink()
        use pipeline = new ProcessorPipeline([||], sink, MemoryPool<byte>.Shared)
        pipeline.GetStageMetricsSnapshot() |> should be Empty

    [<Fact>]
    let ``empty connection directions produce valid zero snapshots`` () =
        let connection = new ConnectionRuntimeMetrics(new ConnectionMemoryAccounting())
        let snapshot = connection.GetSnapshot()

        snapshot.ClientToBackend.Stages |> should be Empty
        snapshot.BackendToClient.Stages |> should be Empty
        snapshot.ClientToBackend.InputBytes |> should equal 0L
        snapshot.BackendToClient.InputBytes |> should equal 0L
        snapshot.ClientToBackend.InvocationCount |> should equal 0L
        snapshot.BackendToClient.InvocationCount |> should equal 0L
        snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L

    [<Fact>]
    let ``connection snapshots expose independent directional logical occupancy`` () =
        use accounting = new ConnectionCapacityAccounting(ConnectionCapacityLimits(8L, 12L, 32L))
        let connection = new ConnectionRuntimeMetrics(accounting)
        let clientReservation =
            match accounting.ClientToBackendLogical.TryReserve 3L with
            | ValueSome reservation -> reservation
            | ValueNone -> failwith "Could not reserve client-to-backend logical credit."

        let backendReservation =
            match accounting.BackendToClientLogical.TryReserve 5L with
            | ValueSome reservation -> reservation
            | ValueNone -> failwith "Could not reserve backend-to-client logical credit."

        let snapshot = connection.GetSnapshot()
        snapshot.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 3L
        snapshot.ClientToBackend.PeakLogicalBufferedBytes |> should equal 3L
        snapshot.BackendToClient.CurrentLogicalBufferedBytes |> should equal 5L
        snapshot.BackendToClient.PeakLogicalBufferedBytes |> should equal 5L

        clientReservation.Release()
        backendReservation.Release()
        let final = connection.GetSnapshot()
        final.ClientToBackend.CurrentLogicalBufferedBytes |> should equal 0L
        final.BackendToClient.CurrentLogicalBufferedBytes |> should equal 0L

    [<Fact>]
    let ``stage metrics replace the committed retained suffix instead of accumulating`` () =
        task {
            let sink = new RecordingSink()
            let mutable invocation = 0

            let stage =
                processor (fun input _ _ ->
                    invocation <- invocation + 1

                    match invocation with
                    | 1 -> completed input.Bytes.Length
                    | 2 -> completed input.Bytes.Length
                    | 3 -> completed 2L
                    | 4 -> completed 0L
                    | _ -> failwith "Unexpected processor invocation.")

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)

            do! push pipeline (owner "ABC")
            retainedBytes pipeline |> should equal [| 3L |]

            do! push pipeline (owner "DE")
            retainedBytes pipeline |> should equal [| 5L |]

            do! push pipeline (owner "FG")
            retainedBytes pipeline |> should equal [| 2L |]

            do! push pipeline (owner "HI")
            retainedBytes pipeline |> should equal [| 0L |]
        }

    [<Fact>]
    let ``input metrics count newly admitted bytes once across retained re-presentation`` () =
        task {
            let sink = new RecordingSink()
            let mutable invocation = 0

            let stage =
                processor (fun input output _ ->
                    invocation <- invocation + 1

                    match invocation with
                    | 1 -> completed input.Bytes.Length
                    | 2 ->
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L
                    | _ -> completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            do! push pipeline (owner "DEF")

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InputBytes |> should equal 6L
            snapshot.InputBackedOutputBytes |> should equal 6L
            snapshot.InvocationCount |> should equal 2L
            snapshot.SuccessfulInvocationCount |> should equal 2L
            sink.Bytes |> text |> should equal "ABCDEF"
        }

    [<Fact>]
    let ``segmented retained input has admission accounting independent of physical segments`` () =
        task {
            let sink = new RecordingSink()
            let mutable invocation = 0
            let mutable sawMultipleSegments = false

            let stage =
                processor (fun input output _ ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed 1L
                    else
                        sawMultipleSegments <- sequenceSegmentCount input.Bytes > 1
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "AB")
            do! push pipeline (owner "CD")

            sawMultipleSegments |> should equal true
            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InputBytes |> should equal 4L
            snapshot.InputBackedOutputBytes |> should equal 3L
            sink.Bytes |> text |> should equal "BCD"
        }

    [<Fact>]
    let ``mixed and repeated output records logical provenance after commit`` () =
        task {
            let sink = new BlockingSink()
            let pool = new TrackingPool(minimumCapacity = 64)
            let accounting = new ConnectionMemoryAccounting()

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, 2L)
                    output.AppendInput(0L, 2L)
                    emit output (bytes "XYZ")
                    emit output (bytes "Q")
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting)

            let operation = push pipeline (owner "ABCD")
            do! sink.Started

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InputBytes |> should equal 4L
            snapshot.InputBackedOutputBytes |> should equal 4L
            snapshot.GeneratedOutputBytes |> should equal 4L
            snapshot.InvocationCount |> should equal 1L
            snapshot.SuccessfulInvocationCount |> should equal 1L
            snapshot.ProcessingDuration |> should be (greaterThanOrEqualTo TimeSpan.Zero)
            physicalBytes accounting |> should be (greaterThan snapshot.GeneratedOutputBytes)

            sink.Release()
            do! operation
        }

    [<Fact>]
    let ``failed invocation does not publish tentative output metrics`` () =
        task {
            let sink = new RecordingSink()
            let expected = InvalidOperationException("processor failed")

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    emit output (bytes "XYZ")
                    raise expected)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)

            let! observed = Assert.ThrowsAsync<InvalidOperationException>(fun () -> push pipeline (owner "ABC"))
            Object.ReferenceEquals(expected, observed) |> should equal true

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InputBytes |> should equal 3L
            snapshot.InputBackedOutputBytes |> should equal 0L
            snapshot.GeneratedOutputBytes |> should equal 0L
            snapshot.InvocationCount |> should equal 1L
            snapshot.SuccessfulInvocationCount |> should equal 0L
            snapshot.FailedInvocationCount |> should equal 1L
            snapshot.CancelledInvocationCount |> should equal 0L
            snapshot.ProcessingDuration |> should be (greaterThanOrEqualTo TimeSpan.Zero)
            sink.Writes.Count |> should equal 0
        }

    [<Fact>]
    let ``emitted input and retained input are accounted independently`` () =
        task {
            let sink = new RecordingSink()

            let first =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        output.AppendInput(0L, input.Bytes.Length)
                        completed input.Bytes.Length)

            let second =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| first; second |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")

            sink.Bytes |> text |> should equal "ABC"
            retainedBytes pipeline |> should equal [| 3L; 0L |]

            do! pipeline.CompleteAsync(CancellationToken.None)
            retainedBytes pipeline |> should equal [| 0L; 0L |]
        }

    [<Fact>]
    let ``stage metrics keep independent values and indexes across pipelines`` () =
        task {
            let firstSink = new RecordingSink()
            let secondSink = new RecordingSink()

            let retainingStage retainedLength =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed (min retainedLength input.Bytes.Length))

            use first =
                new ProcessorPipeline(
                    [| retainingStage 3L; retainingStage 2L |],
                    firstSink,
                    MemoryPool<byte>.Shared
                )

            use second =
                new ProcessorPipeline([| retainingStage 1L |], secondSink, MemoryPool<byte>.Shared)

            let! _ =
                Task.WhenAll(
                    push first (owner "ABCDE"),
                    push second (owner "XY")
                )

            first.GetStageMetricsSnapshot()
            |> List.map (fun snapshot -> snapshot.StageIndex, snapshot.CurrentRetainedBytes)
            |> should equal [ 0, 3L; 1, 2L ]

            second.GetStageMetricsSnapshot()
            |> List.map (fun snapshot -> snapshot.StageIndex, snapshot.CurrentRetainedBytes)
            |> should equal [ 0, 1L ]
        }

    [<Fact>]
    let ``connection snapshots aggregate independent directions and retain no runtime owners`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let connection = new ConnectionRuntimeMetrics(accounting)
            let clientMetrics = new ProcessorPipelineMetrics(2)
            let backendMetrics = new ProcessorPipelineMetrics(1)
            connection.Publish(ClientToBackend, clientMetrics)
            connection.Publish(BackendToClient, backendMetrics)

            let clientSink = new RecordingSink()
            let backendSink = new RecordingSink()
            let passThrough () =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            let clientPipeline =
                new ProcessorPipeline(
                    [| passThrough (); passThrough () |],
                    clientSink,
                    MemoryPool<byte>.Shared,
                    accounting,
                    clientMetrics
                )

            let backendPipeline =
                new ProcessorPipeline(
                    [| passThrough () |],
                    backendSink,
                    MemoryPool<byte>.Shared,
                    accounting,
                    backendMetrics
                )

            do! clientPipeline.PushAsync(owner "AB", 2, CancellationToken.None)
            do! backendPipeline.PushAsync(owner "XYZ", 3, CancellationToken.None)

            let snapshot = connection.GetSnapshot()
            snapshot.ClientToBackend.Direction |> should equal ClientToBackend
            snapshot.BackendToClient.Direction |> should equal BackendToClient
            snapshot.ClientToBackend.Stages |> List.map _.StageIndex |> should equal [ 0; 1 ]
            snapshot.BackendToClient.Stages |> List.map _.StageIndex |> should equal [ 0 ]
            snapshot.ClientToBackend.InputBytes |> should equal 4L
            snapshot.ClientToBackend.InputBackedOutputBytes |> should equal 4L
            snapshot.BackendToClient.InputBytes |> should equal 3L
            snapshot.BackendToClient.InputBackedOutputBytes |> should equal 3L
            snapshot.ClientToBackend.InvocationCount |> should equal 2L
            snapshot.BackendToClient.InvocationCount |> should equal 1L
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L

            let transformedStages =
                snapshot.ClientToBackend.Stages
                |> List.map (fun stage -> { stage with InputBytes = 999L })

            (List.head transformedStages).InputBytes |> should equal 999L
            (List.head snapshot.ClientToBackend.Stages).InputBytes |> should equal 2L

            let successive = connection.GetSnapshot()
            obj.ReferenceEquals(box snapshot.ClientToBackend.Stages, box successive.ClientToBackend.Stages)
            |> should equal false
            successive.ClientToBackend.Stages |> List.map _.StageIndex |> should equal [ 0; 1 ]

            do! (clientPipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (backendPipeline :> IAsyncDisposable).DisposeAsync().AsTask()

            snapshot.ClientToBackend.InputBytes |> should equal 4L
            snapshot.BackendToClient.InputBytes |> should equal 3L
            snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 0L
            connection.GetSnapshot().ClientToBackend.CurrentRetainedBytes |> should equal 0L
            connection.GetSnapshot().BackendToClient.CurrentRetainedBytes |> should equal 0L
        }

    [<Fact>]
    let ``active snapshots remain detached from torn down runtime resources`` () =
        task {
            let! probe = captureDetachedSnapshot ()
            do! Task.Yield()

            probe.Snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 32L
            probe.CapacityAfterTeardown |> should equal 0L

            collectUntilDead [| probe.Pipeline; probe.Processor; probe.Owner |]
            probe.Pipeline.IsAlive |> should equal false
            probe.Processor.IsAlive |> should equal false
            probe.Owner.IsAlive |> should equal false
            probe.Snapshot.CurrentPhysicalBufferCapacityBytes |> should equal 32L
        }

    [<Fact>]
    let ``concurrent direction snapshots stay non-negative while sink writes are pending`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let connection = new ConnectionRuntimeMetrics(accounting)
            let clientMetrics = new ProcessorPipelineMetrics(1)
            let backendMetrics = new ProcessorPipelineMetrics(1)
            connection.Publish(ClientToBackend, clientMetrics)
            connection.Publish(BackendToClient, backendMetrics)

            let clientSink = new BlockingSink()
            let backendSink = new BlockingSink()
            let passThrough () =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            let clientPipeline =
                new ProcessorPipeline(
                    [| passThrough () |],
                    clientSink,
                    MemoryPool<byte>.Shared,
                    accounting,
                    clientMetrics
                )

            let backendPipeline =
                new ProcessorPipeline(
                    [| passThrough () |],
                    backendSink,
                    MemoryPool<byte>.Shared,
                    accounting,
                    backendMetrics
                )

            let clientPush = clientPipeline.PushAsync(owner "AB", 2, CancellationToken.None)
            let backendPush = backendPipeline.PushAsync(owner "CD", 2, CancellationToken.None)
            do!
                Task.WhenAll(
                    [| clientSink.Started :> Task
                       backendSink.Started :> Task |]
                )

            for _ in 1 .. 100 do
                let snapshot = connection.GetSnapshot()
                snapshot.ClientToBackend.InputBytes |> should be (greaterThanOrEqualTo 0L)
                snapshot.BackendToClient.InputBytes |> should be (greaterThanOrEqualTo 0L)
                snapshot.CurrentPhysicalBufferCapacityBytes |> should be (greaterThanOrEqualTo 0L)

            clientSink.Release()
            backendSink.Release()
            do! Task.WhenAll([| clientPush :> Task; backendPush :> Task |])
            do! (clientPipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (backendPipeline :> IAsyncDisposable).DisposeAsync().AsTask()
        }

    [<Fact>]
    let ``asynchronous invocation snapshots retain the previous committed value`` () =
        task {
            use timeout = new CancellationTokenSource(5000: int)
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let mutable invocation = 0

            let stage =
                processor (fun input _ _ ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed input.Bytes.Length
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                started.SetResult(())
                                do! release.Task
                                return { RetainedLength = 2L }
                            }
                        ))

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            let priorSnapshot = pipeline.GetStageMetricsSnapshot()[0]

            let operation = push pipeline (owner "D")
            do! started.Task.WaitAsync(timeout.Token)
            retainedBytes pipeline |> should equal [| 3L |]
            let pendingSnapshot = pipeline.GetStageMetricsSnapshot()[0]
            pendingSnapshot.InvocationCount |> should equal 2L
            pendingSnapshot.SuccessfulInvocationCount |> should equal 1L
            pendingSnapshot.ProcessingDuration |> should equal priorSnapshot.ProcessingDuration

            let holdStartedAt = Stopwatch.GetTimestamp()
            do! Task.Delay(TimeSpan.FromMilliseconds 75.0, timeout.Token)
            let held = Stopwatch.GetElapsedTime(holdStartedAt)
            release.SetResult(())
            do! operation
            retainedBytes pipeline |> should equal [| 2L |]

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InvocationCount |> should equal 2L
            snapshot.SuccessfulInvocationCount |> should equal 2L
            snapshot.FailedInvocationCount |> should equal 0L
            snapshot.CancelledInvocationCount |> should equal 0L
            assertDurationIncludes held snapshot.ProcessingDuration
            snapshot.ProcessingDuration |> should be (greaterThanOrEqualTo pendingSnapshot.ProcessingDuration)
        }

    [<Fact>]
    let ``successful retention disposal does not hold the metrics monitor`` () =
        task {
            use timeout = new CancellationTokenSource(5000: int)
            let accounting = new ConnectionMemoryAccounting()
            let sink = new RecordingSink()
            let firstInput = new BlockingDisposeOwner(Array.zeroCreate<byte> 2)
            let secondInput = owner "CD"
            let mutable invocation = 0

            let stage =
                processor (fun input output _ ->
                    invocation <- invocation + 1

                    if input.IsCompleted then
                        completed 0L
                    elif invocation = 1 then
                        completed input.Bytes.Length
                    else
                        output.AppendInput(2L, 2L)
                        completed 2L)

            let pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared, accounting)

            do! pipeline.PushAsync(firstInput, firstInput.Buffer.Length, CancellationToken.None)
            let operation =
                Task.Run(
                    Func<Task>(fun () ->
                        push pipeline secondInput)
                )
            try
                do! firstInput.DisposeStarted.WaitAsync(timeout.Token)

                let snapshotTask =
                    Task.Run(
                        Func<ProcessorStageMetricsSnapshot>(fun () ->
                            pipeline.GetStageMetricsSnapshot()[0])
                    )

                let! pendingSnapshot = snapshotTask.WaitAsync(timeout.Token)
                pendingSnapshot.InvocationCount |> should equal 2L
                pendingSnapshot.SuccessfulInvocationCount |> should equal 1L

                firstInput.ReleaseDispose()
                do! operation
                do! pipeline.CompleteAsync(CancellationToken.None)
                firstInput.DisposeCount |> should equal 1
                secondInput.DisposeCount |> should equal 1
            finally
                firstInput.ReleaseDispose()
        }

    [<Fact>]
    let ``invalid result does not publish a speculative metric and teardown clears the old value`` () =
        task {
            use timeout = new CancellationTokenSource(5000: int)
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let mutable invocation = 0

            let stage =
                processor (fun input _ _ ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed input.Bytes.Length
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                started.SetResult(())
                                do! release.Task
                                return { RetainedLength = input.Bytes.Length + 1L }
                            }
                        ))

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            let operation = push pipeline (owner "D")
            do! started.Task.WaitAsync(timeout.Token)
            retainedBytes pipeline |> should equal [| 3L |]

            let holdStartedAt = Stopwatch.GetTimestamp()
            do! Task.Delay(TimeSpan.FromMilliseconds 75.0, timeout.Token)
            let held = Stopwatch.GetElapsedTime(holdStartedAt)
            release.SetResult(())

            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> operation)
            retainedBytes pipeline |> should equal [| 0L |]

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InvocationCount |> should equal 2L
            snapshot.SuccessfulInvocationCount |> should equal 1L
            snapshot.FailedInvocationCount |> should equal 1L
            snapshot.CancelledInvocationCount |> should equal 0L
            snapshot.InputBackedOutputBytes |> should equal 0L
            snapshot.GeneratedOutputBytes |> should equal 0L
            assertDurationIncludes held snapshot.ProcessingDuration
        }

    [<Fact>]
    let ``asynchronous processor failure includes awaited duration and preserves the error`` () =
        task {
            use timeout = new CancellationTokenSource(5000: int)
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let expected = InvalidOperationException("asynchronous processor failed")

            let stage =
                processor (fun _ _ _ ->
                    ValueTask<ProcessorResult>(
                        task {
                            started.SetResult(())
                            do! release.Task
                            return raise expected
                        }
                    ))

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            let operation = push pipeline (owner "ABC")
            do! started.Task.WaitAsync(timeout.Token)

            let holdStartedAt = Stopwatch.GetTimestamp()
            do! Task.Delay(TimeSpan.FromMilliseconds 75.0, timeout.Token)
            let held = Stopwatch.GetElapsedTime(holdStartedAt)
            release.SetResult(())

            let! observed = Assert.ThrowsAsync<InvalidOperationException>(fun () -> operation)
            Object.ReferenceEquals(expected, observed) |> should equal true

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InputBackedOutputBytes |> should equal 0L
            snapshot.GeneratedOutputBytes |> should equal 0L
            snapshot.InvocationCount |> should equal 1L
            snapshot.SuccessfulInvocationCount |> should equal 0L
            snapshot.FailedInvocationCount |> should equal 1L
            snapshot.CancelledInvocationCount |> should equal 0L
            assertDurationIncludes held snapshot.ProcessingDuration
            sink.Writes.Count |> should equal 0
        }

    [<Fact>]
    let ``processor exception clears committed stage retention`` () =
        task {
            let sink = new RecordingSink()
            let mutable invocation = 0

            let stage =
                processor (fun input _ _ ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed input.Bytes.Length
                    else
                        raise (InvalidOperationException("processor failed")))

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> push pipeline (owner "D"))
            retainedBytes pipeline |> should equal [| 0L |]

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InvocationCount |> should equal 2L
            snapshot.SuccessfulInvocationCount |> should equal 1L
            snapshot.FailedInvocationCount |> should equal 1L
            snapshot.ProcessingDuration |> should be (greaterThanOrEqualTo TimeSpan.Zero)
        }

    [<Fact>]
    let ``abort clears stage retention before pending owner disposal finishes`` () =
        task {
            let input = new BlockingDisposeOwner(bytes "ABC")
            let nextInput = owner "D"
            let sink = new RecordingSink()
            let expected = InvalidOperationException("processor failed")
            let mutable invocation = 0

            let stage =
                processor (fun currentInput _ _ ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed currentInput.Bytes.Length
                    else
                        raise expected)

            let logicalCapacity = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()

            use pipeline =
                new ProcessorPipeline(
                    [| stage |],
                    sink,
                    MemoryPool<byte>.Shared,
                    accounting,
                    logicalCapacity
                )

            do!
                pipeline.PushAsync(
                    input,
                    input.Buffer.Length,
                    CancellationToken.None
                )

            retainedBytes pipeline |> should equal [| 3L |]

            let operation =
                Task.Run(
                    Func<Task>(fun () ->
                        pipeline.PushAsync(
                            nextInput,
                            nextInput.Buffer.Length,
                            CancellationToken.None
                        ))
                )

            do! input.DisposeStarted

            try
                operation.IsCompleted |> should equal false
                retainedBytes pipeline |> should equal [| 0L |]
                (logicalSnapshot logicalCapacity).CurrentBytes |> should equal 0L
            finally
                input.ReleaseDispose()

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> operation)

            Object.ReferenceEquals(expected, observed) |> should equal true
            retainedBytes pipeline |> should equal [| 0L |]
            input.DisposeCount |> should equal 1
            nextInput.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``cancellation clears stage retention after the active invocation is torn down`` () =
        task {
            use timeout = new CancellationTokenSource(5000: int)
            use cancellation = new CancellationTokenSource()
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let mutable invocation = 0

            let stage =
                processor (fun input _ cancellationToken ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed input.Bytes.Length
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                started.SetResult(())
                                do! Task.Delay(Timeout.Infinite, cancellationToken)
                                return { RetainedLength = 0L }
                            }
                        ))

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            let operation = pipeline.PushAsync(owner "D", 1, cancellation.Token)
            do! started.Task.WaitAsync(timeout.Token)
            retainedBytes pipeline |> should equal [| 3L |]

            let holdStartedAt = Stopwatch.GetTimestamp()
            do! Task.Delay(TimeSpan.FromMilliseconds 75.0, timeout.Token)
            let held = Stopwatch.GetElapsedTime(holdStartedAt)
            cancellation.Cancel()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)
            retainedBytes pipeline |> should equal [| 0L |]

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InvocationCount |> should equal 2L
            snapshot.SuccessfulInvocationCount |> should equal 1L
            snapshot.FailedInvocationCount |> should equal 0L
            snapshot.CancelledInvocationCount |> should equal 1L
            assertDurationIncludes held snapshot.ProcessingDuration
        }

    [<Fact>]
    let ``disposal leaves stable stage indexes with zero retention`` () =
        task {
            let sink = new RecordingSink()
            let stage () =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed input.Bytes.Length)

            let pipeline = new ProcessorPipeline([| stage (); stage () |], sink, MemoryPool<byte>.Shared)

            do! push pipeline (owner "ABC")
            retainedBytes pipeline |> should equal [| 3L; 3L |]
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()

            pipeline.GetStageMetricsSnapshot()
            |> List.map (fun snapshot -> snapshot.StageIndex, snapshot.CurrentRetainedBytes)
            |> should equal [ 0, 0L; 1, 0L ]
        }

    [<Fact>]
    let ``disposal clears stage retention before pending owner disposal finishes`` () =
        task {
            let input = new BlockingDisposeOwner(bytes "ABC")
            let sink = new RecordingSink()

            let stage =
                processor (fun currentInput _ _ -> completed currentInput.Bytes.Length)

            let pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)

            do!
                pipeline.PushAsync(
                    input,
                    input.Buffer.Length,
                    CancellationToken.None
                )

            retainedBytes pipeline |> should equal [| 3L |]

            let disposal =
                Task.Run(
                    Func<Task>(fun () ->
                        (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())
                )

            do! input.DisposeStarted

            try
                disposal.IsCompleted |> should equal false
                retainedBytes pipeline |> should equal [| 0L |]
            finally
                input.ReleaseDispose()

            do! disposal
            retainedBytes pipeline |> should equal [| 0L |]
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``final EOF snapshot keeps prior retention until the final commit`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()

            let stage =
                processor (fun input _ _ ->
                    if input.IsCompleted then
                        ValueTask<ProcessorResult>(
                            task {
                                started.SetResult(())
                                do! release.Task
                                return { RetainedLength = 0L }
                            }
                        )
                    else
                        completed input.Bytes.Length)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            let completion = pipeline.CompleteAsync(CancellationToken.None)
            do! started.Task

            completion.IsCompleted |> should equal false
            retainedBytes pipeline |> should equal [| 3L |]

            release.SetResult(())
            do! completion
            retainedBytes pipeline |> should equal [| 0L |]
        }

    [<Fact>]
    let ``one stage borrows its input without copying`` () =
        task {
            let input = owner "ABCDEF"
            let sink = new RecordingSink()
            let mutable sameBuffer = false

            sink.OnWrite <- fun sequence ->
                let mutable segment = Unchecked.defaultof<ArraySegment<byte>>
                MemoryMarshal.TryGetArray(sequence.First, &segment) |> should equal true
                sameBuffer <- Object.ReferenceEquals(segment.Array, input.Buffer)
                input.DisposeCount |> should equal 0

            let stage =
                processor (fun _ output _ ->
                    output.AppendInput(1L, 3L)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline input

            sameBuffer |> should equal true
            sink.Bytes |> text |> should equal "BCD"
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``eight pass-through stages preserve the original input backing`` () =
        task {
            let input = owner "ABCDEF"
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let mutable sameBuffer = false

            sink.OnWrite <- fun sequence ->
                let mutable segment = Unchecked.defaultof<ArraySegment<byte>>
                MemoryMarshal.TryGetArray(sequence.First, &segment) |> should equal true
                sameBuffer <- Object.ReferenceEquals(segment.Array, input.Buffer)
                input.DisposeCount |> should equal 0

            let passThrough =
                Array.init 8 (fun _ ->
                    processor (fun input output _ ->
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L))

            use pipeline = new ProcessorPipeline(passThrough, sink, pool)
            do! push pipeline input

            sameBuffer |> should equal true
            sink.Bytes |> text |> should equal "ABCDEF"
            input.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 0
        }

    [<Fact>]
    let ``multiple stages run in order on one logical output stream`` () =
        task {
            let calls = ResizeArray<string>()
            let sink = new RecordingSink()

            let first =
                processor (fun input output _ ->
                    calls.Add("A:" + (sequenceBytes input.Bytes |> text))
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            let second =
                processor (fun input output _ ->
                    calls.Add("B:" + (sequenceBytes input.Bytes |> text))
                    emit output (bytes "X")
                    output.AppendInput(1L, 2L)
                    completed 0L)

            let third =
                processor (fun input output _ ->
                    calls.Add("C:" + (sequenceBytes input.Bytes |> text))
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| first; second; third |], sink, MemoryPool<byte>.Shared)

            do! push pipeline (owner "ABC")

            calls |> Seq.toArray |> should equal [| "A:ABC"; "B:ABC"; "C:XBC" |]
            sink.Bytes |> text |> should equal "XBC"
        }

    [<Fact>]
    let ``stage and arrival boundaries do not become message boundaries`` () =
        task {
            let secondStageInputs = ResizeArray<string>()
            let thirdStageInputs = ResizeArray<string>()
            let sink = new RecordingSink()

            let first =
                processor (fun input output _ ->
                    for offset in 0L .. input.Bytes.Length - 1L do
                        output.AppendInput(offset, 1L)

                    completed 0L)

            let second =
                processor (fun input output _ ->
                    let value = sequenceBytes input.Bytes |> text
                    secondStageInputs.Add(value)

                    if input.Bytes.Length < 4L then
                        completed input.Bytes.Length
                    else
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

            let third =
                processor (fun input output _ ->
                    thirdStageInputs.Add(sequenceBytes input.Bytes |> text)
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| first; second; third |], sink, MemoryPool<byte>.Shared)

            do! push pipeline (owner "AB")
            do! push pipeline (owner "CD")

            secondStageInputs |> Seq.toArray |> should equal [| "AB"; "ABCD" |]
            thirdStageInputs |> Seq.toArray |> should equal [| "ABCD" |]
            sink.Bytes |> text |> should equal "ABCD"
        }

    [<Fact>]
    let ``generated output can be shorter or longer and mixed with borrowed ranges`` () =
        task {
            let sink = new RecordingSink()
            let pool = new TrackingPool()

            let stage =
                processor (fun input output _ ->
                    let value = sequenceBytes input.Bytes |> text

                    match value with
                    | "ABC" -> emit output (bytes "X")
                    | "DE" -> emit output (bytes "WXYZ123")
                    | "FG" ->
                        output.AppendInput(0L, 1L)
                        emit output (bytes "X")
                        output.AppendInput(1L, 1L)
                        emit output (bytes "Y")
                    | _ -> failwith "Unexpected input"

                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool)
            do! push pipeline (owner "ABC")
            do! push pipeline (owner "DE")
            do! push pipeline (owner "FG")

            sink.Writes |> Seq.map text |> Seq.toArray
            |> should equal [| "X"; "WXYZ123"; "FXGY" |]

            pool.Owners.Count |> should equal 4
            pool.Owners |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``borrowed and generated ranges form the exact requested stream`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let inputOwner = owner "ABCDEF"
            let pool = new TrackingPool()
            let mutable observed = Array.empty<byte>
            let mutable segments = Array.empty<ReadOnlyMemory<byte>>

            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(value, _) =
                        ValueTask(
                            task {
                                segments <- sequenceSegments value
                                started.SetResult(())
                                do! release.Task
                                observed <- sequenceBytes value
                            }
                        )

                    member _.CompleteAsync(_) = ValueTask() }

            let stage =
                processor (fun _ output _ ->
                    output.AppendInput(0L, 2L)
                    emit output (bytes "XYZ")
                    output.AppendInput(4L, 2L)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool)
            let operation = push pipeline inputOwner
            do! started.Task

            segments.Length |> should equal 3

            let mutable first = Unchecked.defaultof<ArraySegment<byte>>
            let mutable middle = Unchecked.defaultof<ArraySegment<byte>>
            let mutable last = Unchecked.defaultof<ArraySegment<byte>>

            MemoryMarshal.TryGetArray(segments[0], &first) |> should equal true
            MemoryMarshal.TryGetArray(segments[1], &middle) |> should equal true
            MemoryMarshal.TryGetArray(segments[2], &last) |> should equal true

            Object.ReferenceEquals(first.Array, inputOwner.Buffer) |> should equal true
            Object.ReferenceEquals(last.Array, inputOwner.Buffer) |> should equal true
            pool.Owners.Count |> should equal 1
            Object.ReferenceEquals(middle.Array, pool.Owners[0].Buffer) |> should equal true
            inputOwner.DisposeCount |> should equal 0
            pool.Owners[0].DisposeCount |> should equal 0

            release.SetResult(())
            do! operation

            observed |> text |> should equal "ABXYZEF"
            inputOwner.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``zero output does not invoke downstream until its upstream changes`` () =
        task {
            let sink = new RecordingSink()
            let mutable downstreamCalls = 0

            let first = processor (fun _ _ _ -> completed 0L)

            let second =
                processor (fun _ _ _ ->
                    downstreamCalls <- downstreamCalls + 1
                    completed 0L)

            use pipeline =
                new ProcessorPipeline([| first; second |], sink, MemoryPool<byte>.Shared)

            do! push pipeline (owner "ABC")
            downstreamCalls |> should equal 0
            sink.Bytes |> should be Empty
            do! pipeline.CompleteAsync(CancellationToken.None)
            downstreamCalls |> should equal 1
        }

    [<Fact>]
    let ``partial retained suffix combines with later input`` () =
        task {
            let seen = ResizeArray<string>()
            let sink = new RecordingSink()

            let stage =
                processor (fun input output _ ->
                    let value = sequenceBytes input.Bytes |> text
                    seen.Add(value)

                    if not input.IsCompleted then
                        if value = "ABCD" then
                            output.AppendInput(0L, 2L)
                            completed 2L
                        else
                            output.AppendInput(0L, input.Bytes.Length)
                            completed 0L
                    else
                        completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            let first = owner "ABCD"
            let second = owner "EF"
            do! push pipeline first
            first.DisposeCount |> should equal 0
            do! push pipeline second

            seen |> Seq.toArray |> should equal [| "ABCD"; "CDEF" |]
            sink.Bytes |> text |> should equal "ABCDEF"
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``generated output reaches B while A retains its unrelated input`` () =
        task {
            let aInputs = ResizeArray<string>()
            let bInputs = ResizeArray<string>()
            let firstOwner = owner "ABC"
            let secondOwner = owner "DEF"
            let pool = new TrackingPool()
            let sink = new RecordingSink()

            let a =
                processor (fun input output _ ->
                    let value = sequenceBytes input.Bytes |> text
                    aInputs.Add(value)

                    if value = "ABC" then
                        emit output (bytes "XYZ")
                        completed input.Bytes.Length
                    else
                        completed 0L)

            let b =
                processor (fun input output _ ->
                    bInputs.Add(sequenceBytes input.Bytes |> text)
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| a; b |], sink, pool)
            do! push pipeline firstOwner

            aInputs |> Seq.toArray |> should equal [| "ABC" |]
            bInputs |> Seq.toArray |> should equal [| "XYZ" |]
            sink.Bytes |> text |> should equal "XYZ"
            firstOwner.DisposeCount |> should equal 0
            pool.Owners[0].DisposeCount |> should equal 1

            do! push pipeline secondOwner
            aInputs |> Seq.toArray |> should equal [| "ABC"; "ABCDEF" |]
            bInputs |> Seq.toArray |> should equal [| "XYZ" |]
            firstOwner.DisposeCount |> should equal 1
            secondOwner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``generated output remains owned while a downstream stage retains it`` () =
        task {
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let seen = ResizeArray<string>()

            let a =
                processor (fun input output _ ->
                    if sequenceBytes input.Bytes |> text = "A" then
                        emit output (bytes "X")
                    else
                        emit output (bytes "Y")

                    completed 0L)

            let b =
                processor (fun input output _ ->
                    let value = sequenceBytes input.Bytes |> text
                    seen.Add(value)

                    if value = "X" then
                        completed input.Bytes.Length
                    else
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L)

            use pipeline = new ProcessorPipeline([| a; b |], sink, pool)
            do! push pipeline (owner "A")
            seen |> Seq.toArray |> should equal [| "X" |]
            pool.Owners[0].DisposeCount |> should equal 0

            do! push pipeline (owner "B")
            seen |> Seq.toArray |> should equal [| "X"; "XY" |]
            sink.Bytes |> text |> should equal "XY"
            pool.Owners |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``upstream retained input and downstream retained generated output release independently`` () =
        task {
            let firstOwner = owner "ABC"
            let secondOwner = owner "D"
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let downstreamInputs = ResizeArray<string * bool>()

            let a =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    elif sequenceBytes input.Bytes |> text = "ABC" then
                        emit output (bytes "XYZ")
                        completed input.Bytes.Length
                    else
                        completed 0L)

            let b =
                processor (fun input _ _ ->
                    downstreamInputs.Add((sequenceBytes input.Bytes |> text, input.IsCompleted))

                    if input.IsCompleted then
                        completed 0L
                    else
                        completed input.Bytes.Length)

            use pipeline = new ProcessorPipeline([| a; b |], sink, pool)
            do! push pipeline firstOwner

            downstreamInputs |> Seq.toArray |> should equal [| "XYZ", false |]
            firstOwner.DisposeCount |> should equal 0
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 0

            do! push pipeline secondOwner

            firstOwner.DisposeCount |> should equal 1
            secondOwner.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 0
            downstreamInputs |> Seq.toArray |> should equal [| "XYZ", false |]

            do! pipeline.CompleteAsync(CancellationToken.None)

            downstreamInputs |> Seq.toArray
            |> should equal [| "XYZ", false; "XYZ", true |]

            pool.Owners[0].DisposeCount |> should equal 1
            sink.Bytes |> should be Empty
        }

    [<Fact>]
    let ``input storage remains alive until the awaited sink write finishes`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let inputOwner = owner "ABC"
            let mutable observed = Array.empty<byte>

            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(value, _) =
                        ValueTask(
                            task {
                                started.SetResult(())
                                do! release.Task
                                observed <- sequenceBytes value
                            }
                        )

                    member _.CompleteAsync(_) = ValueTask() }

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            let operation = push pipeline inputOwner
            do! started.Task
            inputOwner.DisposeCount |> should equal 0
            release.SetResult(())
            do! operation
            observed |> text |> should equal "ABC"
            inputOwner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``generated storage remains alive until the awaited sink write finishes`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let pool = new TrackingPool()
            let inputOwner = owner "ABC"
            let mutable observed = Array.empty<byte>

            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(value, _) =
                        ValueTask(
                            task {
                                started.SetResult(())
                                do! release.Task
                                observed <- sequenceBytes value
                            }
                        )

                    member _.CompleteAsync(_) = ValueTask() }

            let stage =
                processor (fun _ output _ ->
                    emit output (bytes "XYZ123")
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool)
            let operation = push pipeline inputOwner
            do! started.Task

            inputOwner.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 0

            release.SetResult(())
            do! operation

            observed |> text |> should equal "XYZ123"
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``borrowed output survives downstream release while A retains it`` () =
        task {
            let firstOwner = owner "ABC"
            let secondOwner = owner "D"
            let seen = ResizeArray<string>()
            let sink = RecordingSink()

            let a =
                processor (fun input output _ ->
                    let value = sequenceBytes input.Bytes |> text
                    seen.Add(value)

                    if value = "ABC" then
                        output.AppendInput(0L, input.Bytes.Length)
                        completed input.Bytes.Length
                    else
                        completed 0L)

            let b = processor (fun _ _ _ -> completed 0L)
            use pipeline = new ProcessorPipeline([| a; b |], sink, MemoryPool<byte>.Shared)
            do! push pipeline firstOwner
            firstOwner.DisposeCount |> should equal 0
            do! push pipeline secondOwner

            seen |> Seq.toArray |> should equal [| "ABC"; "ABCD" |]
            firstOwner.DisposeCount |> should equal 1
            secondOwner.DisposeCount |> should equal 1
            sink.Bytes |> should be Empty
        }

    [<Fact>]
    let ``downstream retention keeps borrowed input alive after upstream releases it`` () =
        task {
            let firstOwner = owner "ABC"
            let secondOwner = owner "D"
            let sink = RecordingSink()
            let downstreamInputs = ResizeArray<string * bool>()

            let a =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    elif sequenceBytes input.Bytes |> text = "ABC" then
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L
                    else
                        completed 0L)

            let b =
                processor (fun input _ _ ->
                    downstreamInputs.Add((sequenceBytes input.Bytes |> text, input.IsCompleted))

                    if input.IsCompleted then
                        completed 0L
                    else
                        completed input.Bytes.Length)

            use pipeline = new ProcessorPipeline([| a; b |], sink, MemoryPool<byte>.Shared)
            do! push pipeline firstOwner

            downstreamInputs |> Seq.toArray |> should equal [| "ABC", false |]
            firstOwner.DisposeCount |> should equal 0

            do! push pipeline secondOwner

            firstOwner.DisposeCount |> should equal 0
            secondOwner.DisposeCount |> should equal 1
            downstreamInputs |> Seq.toArray |> should equal [| "ABC", false |]

            do! pipeline.CompleteAsync(CancellationToken.None)

            downstreamInputs |> Seq.toArray
            |> should equal [| "ABC", false; "ABC", true |]

            firstOwner.DisposeCount |> should equal 1
            sink.Bytes |> should be Empty
        }

    [<Fact>]
    let ``repeated borrowed ranges stay alive until asynchronous sink consumption completes`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let inputOwner = owner "ABC"
            let mutable observed = Array.empty<byte>
            let mutable segmentCount = 0
            let mutable allRangesUseInputBacking = false

            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(value, _) =
                        ValueTask(
                            task {
                                let segments = sequenceSegments value
                                segmentCount <- segments.Length

                                allRangesUseInputBacking <-
                                    segments
                                    |> Array.forall (fun segment ->
                                        let mutable arraySegment =
                                            Unchecked.defaultof<ArraySegment<byte>>

                                        MemoryMarshal.TryGetArray(segment, &arraySegment)
                                        && Object.ReferenceEquals(arraySegment.Array, inputOwner.Buffer))

                                started.SetResult(())
                                do! release.Task
                                observed <- sequenceBytes value
                            }
                        )

                    member _.CompleteAsync(_) = ValueTask() }

            let stage =
                processor (fun input output _ ->
                    for _ in 1 .. 3 do
                        output.AppendInput(0L, input.Bytes.Length)

                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            let operation = push pipeline inputOwner
            do! started.Task

            segmentCount |> should equal 3
            allRangesUseInputBacking |> should equal true
            inputOwner.DisposeCount |> should equal 0

            release.SetResult(())
            do! operation

            observed |> text |> should equal "ABCABCABC"
            inputOwner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``repeated borrowed ranges release independently from upstream retention`` () =
        task {
            let firstOwner = owner "ABC"
            let secondOwner = owner "D"
            let sink = RecordingSink()

            let stage =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    elif sequenceBytes input.Bytes |> text = "ABC" then
                        for _ in 1 .. 3 do
                            output.AppendInput(0L, input.Bytes.Length)

                        completed input.Bytes.Length
                    else
                        completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline firstOwner

            sink.Bytes |> text |> should equal "ABCABCABC"
            firstOwner.DisposeCount |> should equal 0

            do! push pipeline secondOwner

            firstOwner.DisposeCount |> should equal 1
            secondOwner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``EOF reaches every stage after upstream final output`` () =
        task {
            let calls = ResizeArray<string>()
            let sink = RecordingSink()

            let a =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        calls.Add("A:EOF:" + (sequenceBytes input.Bytes |> text))
                        emit output (bytes "Z")
                    else
                        calls.Add("A:data")

                    completed 0L)

            let b =
                processor (fun input output _ ->
                    calls.Add("B:" + string input.IsCompleted + ":" + (sequenceBytes input.Bytes |> text))
                    output.AppendInput(0L, input.Bytes.Length)
                    completed 0L)

            use pipeline = new ProcessorPipeline([| a; b |], sink, MemoryPool<byte>.Shared)
            do! push pipeline (owner "ABC")
            do! pipeline.CompleteAsync(CancellationToken.None)

            calls |> Seq.toArray
            |> should equal [| "A:data"; "A:EOF:"; "B:True:Z" |]

            sink.Bytes |> text |> should equal "Z"
            sink.Completions |> should equal 1
        }

    [<Fact>]
    let ``EOF resolves retained input before downstream completion`` () =
        task {
            let sink = RecordingSink()
            let inputOwner = owner "AB"
            let mutable sawFinal = false

            let stage =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        sawFinal <- true
                        sequenceBytes input.Bytes |> text |> should equal "AB"
                        output.AppendInput(0L, input.Bytes.Length)
                        completed 0L
                    else
                        completed input.Bytes.Length)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline inputOwner
            inputOwner.DisposeCount |> should equal 0
            do! pipeline.CompleteAsync(CancellationToken.None)

            sawFinal |> should equal true
            sink.Bytes |> text |> should equal "AB"
            sink.Completions |> should equal 1
            inputOwner.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``EOF output remains owned until its pending sink write completes`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let logical = new CapacityBudget(16L)
            let pool = new TrackingPool()
            let sink = BlockingRecordingSink()
            let inputOwner = owner "ABC"

            let stage =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        output.AppendInput(0L, input.Bytes.Length)
                        emit output (bytes "!")
                        completed 0L
                    else
                        completed input.Bytes.Length)

            let pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting, logical)

            do! push pipeline inputOwner
            let completion = pipeline.CompleteAsync(CancellationToken.None)
            do! sink.Started

            completion.IsCompleted |> should equal false
            sink.Bytes |> text |> should equal "ABC!"
            sink.Completions |> should equal 0
            (logicalSnapshot logical).CurrentBytes |> should equal 4L
            physicalBytes accounting |> should equal 4L
            inputOwner.DisposeCount |> should equal 0

            sink.Release()
            do! completion

            sink.Completions |> should equal 1
            (logicalSnapshot logical).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 0L
            inputOwner.DisposeCount |> should equal 1
            pool.Owners |> Seq.forall (fun outputOwner -> outputOwner.DisposeCount = 1)
            |> should equal true

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
        }

    [<Fact>]
    let ``EOF with unresolved retained input is terminal and releases it`` () =
        task {
            let sink = RecordingSink()
            let inputOwner = owner "AB"

            let stage =
                processor (fun input _ _ -> completed input.Bytes.Length)

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline inputOwner
            inputOwner.DisposeCount |> should equal 0

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.CompleteAsync(CancellationToken.None))

            inputOwner.DisposeCount |> should equal 1
            sink.Completions |> should equal 0
        }

    [<Fact>]
    let ``failure discards staged output and releases all owners`` () =
        task {
            let inputOwner = owner "ABC"
            let pool = new TrackingPool()
            let sink = RecordingSink()
            let expected = InvalidOperationException("processor failed")

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    emit output (bytes "XYZ")
                    raise expected)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool)

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> push pipeline inputOwner)

            Object.ReferenceEquals(expected, observed) |> should equal true
            sink.Writes.Count |> should equal 0
            inputOwner.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``asynchronous failure discards staged output and releases all owners`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let inputOwner = owner "ABC"
            let pool = new TrackingPool()
            let sink = RecordingSink()
            let expected = InvalidOperationException("processor failed asynchronously")

            let stage =
                processor (fun input output _ ->
                    ValueTask<ProcessorResult>(
                        task {
                            output.AppendInput(0L, input.Bytes.Length)
                            emit output (bytes "XYZ")
                            started.SetResult(())
                            do! release.Task
                            return raise expected
                        }
                    ))

            use pipeline = new ProcessorPipeline([| stage |], sink, pool)
            let operation = push pipeline inputOwner
            do! started.Task

            sink.Writes.Count |> should equal 0
            inputOwner.DisposeCount |> should equal 0
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 0

            release.SetResult(())

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> operation)

            Object.ReferenceEquals(expected, observed) |> should equal true
            sink.Writes.Count |> should equal 0
            inputOwner.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InvocationCount |> should equal 1L
            snapshot.SuccessfulInvocationCount |> should equal 0L
            snapshot.FailedInvocationCount |> should equal 1L
            snapshot.CancelledInvocationCount |> should equal 0L
            snapshot.InputBackedOutputBytes |> should equal 0L
            snapshot.GeneratedOutputBytes |> should equal 0L
        }

    [<Fact>]
    let ``cancellation remains primary when pending cleanup also fails`` () =
        task {
            use cancellation = new CancellationTokenSource()
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = RecordingSink()
            let first = new ThrowingDisposeOwner(Array.zeroCreate<byte> 3)
            let second = owner "second"
            let mutable invocation = 0

            let stage =
                processor (fun input _ cancellationToken ->
                    invocation <- invocation + 1

                    if invocation = 1 then
                        completed input.Bytes.Length
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                started.SetResult(())
                                do! Task.Delay(Timeout.Infinite, cancellationToken)
                                return { RetainedLength = 0L }
                            }
                        ))

            let pipeline =
                new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)

            do! pipeline.PushAsync(first, first.Buffer.Length, CancellationToken.None)
            let operation = pipeline.PushAsync(second, second.Buffer.Length, cancellation.Token)
            do! started.Task
            cancellation.Cancel()

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () -> operation)

            observed.InnerExceptions.Count |> should equal 2
            (observed.InnerExceptions[0] :? OperationCanceledException) |> should equal true
            observed.InnerExceptions[1].Message |> should equal "owner disposal failed"
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``sink failure releases borrowed and generated output`` () =
        task {
            let expected = IOException("sink failed")
            let inputOwner = owner "ABC"
            let pool = new TrackingPool()

            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(_, _) = ValueTask(Task.FromException(expected))
                    member _.CompleteAsync(_) = ValueTask() }

            let stage =
                processor (fun input output _ ->
                    output.AppendInput(0L, input.Bytes.Length)
                    emit output (bytes "XYZ")
                    completed 0L)

            use pipeline = new ProcessorPipeline([| stage |], sink, pool)

            let! observed =
                Assert.ThrowsAsync<IOException>(fun () -> push pipeline inputOwner)

            Object.ReferenceEquals(expected, observed) |> should equal true
            inputOwner.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1

            let snapshot = pipeline.GetStageMetricsSnapshot()[0]
            snapshot.InputBytes |> should equal 3L
            snapshot.InputBackedOutputBytes |> should equal 3L
            snapshot.GeneratedOutputBytes |> should equal 3L
            snapshot.InvocationCount |> should equal 1L
            snapshot.SuccessfulInvocationCount |> should equal 1L
            snapshot.FailedInvocationCount |> should equal 0L
        }

    [<Fact>]
    let ``sink failure releases retained logical state before generated owner disposal`` () =
        task {
            let expected = IOException("sink failed")
            let budget = new CapacityBudget 16L
            let accounting = new ConnectionMemoryAccounting()
            let pool = new BlockingPool()
            let input = owner "AB"

            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(_, _) = ValueTask(Task.FromException(expected))
                    member _.CompleteAsync(_) = ValueTask() }

            let stage =
                processor (fun currentInput output _ ->
                    output.AppendInput(0L, currentInput.Bytes.Length)
                    emit output (bytes "XYZ")
                    completed currentInput.Bytes.Length)

            use pipeline =
                new ProcessorPipeline([| stage |], sink, pool, accounting, budget)

            let operation =
                Task.Run(
                    Func<Task>(fun () ->
                        pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None))
                )

            do! pool.Rented
            do! pool.Owners[0].DisposeStarted
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 5L

            pool.Owners[0].ReleaseDispose()
            let! observed = Assert.ThrowsAsync<IOException>(fun () -> operation)
            Object.ReferenceEquals(expected, observed) |> should equal true
            (logicalSnapshot budget).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 0L
            input.DisposeCount |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``pre-cancelled push releases its valid input owner`` () =
        task {
            use cancellation = new CancellationTokenSource()
            cancellation.Cancel()
            let inputOwner = owner "ABC"
            let sink = new RecordingSink()
            use pipeline = new ProcessorPipeline([||], sink, MemoryPool<byte>.Shared)

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    pipeline.PushAsync(inputOwner, inputOwner.Buffer.Length, cancellation.Token))

            inputOwner.DisposeCount |> should equal 1
            sink.Writes.Count |> should equal 0
        }

    [<Fact>]
    let ``invalid retained length and ambiguous writer order are terminal`` () =
        task {
            let pool = new TrackingPool()
            let sink = new RecordingSink()

            let invalidRetention =
                processor (fun input _ _ -> completed (input.Bytes.Length + 1L))

            use first = new ProcessorPipeline([| invalidRetention |], sink, pool)
            let inputOwner = owner "ABC"

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> push first inputOwner)

            inputOwner.DisposeCount |> should equal 1

            let ambiguous =
                processor (fun _ output _ ->
                    output.GetMemory(1) |> ignore

                    try
                        output.AppendInput(0L, 1L)
                    with :? InvalidOperationException -> ()

                    completed 0L)

            use second = new ProcessorPipeline([| ambiguous |], sink, pool)
            let secondOwner = owner "D"

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> push second secondOwner)

            secondOwner.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
            sink.Writes.Count |> should equal 0
        }

    [<Fact>]
    let ``cancellation releases retained shared input and generated output`` () =
        task {
            use cancellation = new CancellationTokenSource()
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let pool = new TrackingPool()
            let sink = new RecordingSink()
            let firstOwner = owner "ABC"
            let secondOwner = owner "D"

            let a =
                processor (fun input output cancellationToken ->
                    if sequenceBytes input.Bytes |> text = "ABC" then
                        output.AppendInput(0L, input.Bytes.Length)
                        emit output (bytes "XYZ")
                        completed input.Bytes.Length
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                started.SetResult(())
                                do! Task.Delay(Timeout.Infinite, cancellationToken)
                                return { RetainedLength = 0L }
                            }
                        ))

            let b = processor (fun input _ _ -> completed input.Bytes.Length)

            use pipeline = new ProcessorPipeline([| a; b |], sink, pool)
            do! push pipeline firstOwner
            firstOwner.DisposeCount |> should equal 0
            pool.Owners[0].DisposeCount |> should equal 0

            let operation = pipeline.PushAsync(secondOwner, secondOwner.Buffer.Length, cancellation.Token)
            do! started.Task
            secondOwner.DisposeCount |> should equal 0
            cancellation.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)

            firstOwner.DisposeCount |> should equal 1
            secondOwner.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
            sink.Writes.Count |> should equal 0
        }

    [<Fact>]
    let ``null input owner reports the ownership contract error`` () =
        let sink = new RecordingSink()
        use pipeline = new ProcessorPipeline([||], sink, MemoryPool<byte>.Shared)
        let missingOwner = Unchecked.defaultof<IMemoryOwner<byte>>

        let error =
            Assert.Throws<ArgumentNullException>(fun () ->
                pipeline.PushAsync(missingOwner, 0, CancellationToken.None) |> ignore)

        error.ParamName |> should equal "owner"

    [<Fact>]
    let ``retained suffix boundaries are correct across physical segments`` () =
        task {
            let cases =
                [| 9L, "ABCDEFGHI"
                   8L, "BCDEFGHI"
                   6L, "DEFGHI"
                   5L, "EFGHI"
                   4L, "FGHI"
                   2L, "HI"
                   0L, "" |]

            for retainedLength, retainedText in cases do
                let sink = new RecordingSink()
                let first = owner "ABC"
                let second = owner "DE"
                let third = owner "FGHI"
                let fourth = owner "J"
                let mutable invocation = 0
                let mutable observed = ""

                let stage =
                    processor (fun input _ _ ->
                        if input.IsCompleted then
                            completed 0L
                        else
                            invocation <- invocation + 1

                            match invocation with
                            | 1
                            | 2 -> completed input.Bytes.Length
                            | 3 ->
                                sequenceBytes input.Bytes |> text |> should equal "ABCDEFGHI"
                                sequenceSegmentCount input.Bytes |> should equal 3
                                completed retainedLength
                            | 4 ->
                                observed <- sequenceBytes input.Bytes |> text
                                completed 0L
                            | _ -> failwith "Unexpected processor invocation.")

                use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
                do! push pipeline first
                do! push pipeline second
                do! push pipeline third
                retainedBytes pipeline |> should equal [| retainedLength |]

                let retainedContains (value: string) =
                    retainedText.IndexOfAny(value.ToCharArray()) >= 0

                first.DisposeCount |> should equal (if retainedContains "ABC" then 0 else 1)
                second.DisposeCount |> should equal (if retainedContains "DE" then 0 else 1)
                third.DisposeCount |> should equal (if retainedContains "FGHI" then 0 else 1)

                do! push pipeline fourth
                observed |> should equal (retainedText + "J")
                retainedBytes pipeline |> should equal [| 0L |]
                first.DisposeCount |> should equal 1
                second.DisposeCount |> should equal 1
                third.DisposeCount |> should equal 1
                fourth.DisposeCount |> should equal 1
                do! pipeline.CompleteAsync(CancellationToken.None)
        }

    [<Fact>]
    let ``multi-segment retained input remains valid across asynchronous suspension`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let first = owner "AB"
            let second = owner "CD"
            let third = owner "E"
            let sink = new RecordingSink()
            let mutable invocation = 0
            let mutable suspendedInput = ""
            let mutable releasedInput = ""

            let stage =
                processor (fun input _ _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        invocation <- invocation + 1

                        match invocation with
                        | 1 -> completed input.Bytes.Length
                        | 2 ->
                            ValueTask<ProcessorResult>(
                                task {
                                    sequenceSegmentCount input.Bytes |> should equal 2
                                    started.SetResult(())
                                    do! release.Task
                                    suspendedInput <- sequenceBytes input.Bytes |> text
                                    return { RetainedLength = input.Bytes.Length }
                                }
                            )
                        | 3 ->
                            sequenceSegmentCount input.Bytes |> should equal 3
                            releasedInput <- sequenceBytes input.Bytes |> text
                            completed 0L
                        | _ -> failwith "Unexpected processor invocation.")

            use pipeline = new ProcessorPipeline([| stage |], sink, MemoryPool<byte>.Shared)
            do! push pipeline first

            let operation = push pipeline second
            do! started.Task

            first.DisposeCount |> should equal 0
            second.DisposeCount |> should equal 0

            release.SetResult(())
            do! operation

            suspendedInput |> should equal "ABCD"
            first.DisposeCount |> should equal 0
            second.DisposeCount |> should equal 0

            do! push pipeline third

            releasedInput |> should equal "ABCDE"
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
            third.DisposeCount |> should equal 1
            do! pipeline.CompleteAsync(CancellationToken.None)
        }

    [<Fact>]
    let ``segmented borrowed output remains valid while overlapping input is retained`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let first = owner "ABC"
            let second = owner "DE"
            let third = owner "FGHI"
            let fourth = owner "J"
            let mutable invocation = 0
            let mutable downstreamBytes = ""

            let a =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        invocation <- invocation + 1

                        match invocation with
                        | 1
                        | 2 -> completed input.Bytes.Length
                        | 3 ->
                            sequenceSegmentCount input.Bytes |> should equal 3
                            output.AppendInput(1L, 1L)
                            output.AppendInput(2L, 4L)
                            output.AppendInput(4L, 3L)
                            completed 5L
                        | 4 -> completed 0L
                        | _ -> failwith "Unexpected processor invocation.")

            let b =
                processor (fun input _ _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                sequenceSegmentCount input.Bytes |> should be (greaterThan 1)
                                started.SetResult(())
                                do! release.Task
                                downstreamBytes <- sequenceBytes input.Bytes |> text
                                return { RetainedLength = 0L }
                            }
                        ))

            use pipeline = new ProcessorPipeline([| a; b |], sink, MemoryPool<byte>.Shared)
            do! push pipeline first
            do! push pipeline second
            let operation = push pipeline third
            do! started.Task

            first.DisposeCount |> should equal 0
            second.DisposeCount |> should equal 0
            third.DisposeCount |> should equal 0
            release.SetResult(())
            do! operation

            downstreamBytes |> should equal "BCDEFEFG"
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 0
            third.DisposeCount |> should equal 0

            do! push pipeline fourth
            second.DisposeCount |> should equal 1
            third.DisposeCount |> should equal 1
            fourth.DisposeCount |> should equal 1
            do! pipeline.CompleteAsync(CancellationToken.None)
        }

    [<Fact>]
    let ``slow asynchronous downstream applies backpressure without queuing another push`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let first = owner "A"
            let rejected = owner "B"
            let third = owner "C"
            let mutable upstreamCalls = 0
            let mutable downstreamCalls = 0
            let mutable activeDownstreamCalls = 0
            let mutable maximumDownstreamCalls = 0

            let a =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        upstreamCalls <- upstreamCalls + 1

                        if upstreamCalls = 1 then
                            output.AppendInput(0L, input.Bytes.Length)
                            completed input.Bytes.Length
                        else
                            output.AppendInput(input.Bytes.Length - 1L, 1L)
                            completed 0L)

            let b =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        completed 0L
                    else
                        ValueTask<ProcessorResult>(
                            task {
                                let active = Interlocked.Increment(&activeDownstreamCalls)
                                maximumDownstreamCalls <- max maximumDownstreamCalls active

                                try
                                    downstreamCalls <- downstreamCalls + 1

                                    if downstreamCalls = 1 then
                                        started.SetResult(())
                                        do! release.Task
                                    else
                                        do! Task.Yield()

                                    output.AppendInput(0L, input.Bytes.Length)
                                    return { RetainedLength = 0L }
                                finally
                                    Interlocked.Decrement(&activeDownstreamCalls) |> ignore
                            }
                        ))

            use pipeline = new ProcessorPipeline([| a; b |], sink, MemoryPool<byte>.Shared)
            let firstOperation = push pipeline first
            do! started.Task

            firstOperation.IsCompleted |> should equal false
            first.DisposeCount |> should equal 0

            let! _ =
                Assert.ThrowsAsync<InvalidOperationException>(fun () -> push pipeline rejected)

            rejected.DisposeCount |> should equal 1
            upstreamCalls |> should equal 1
            downstreamCalls |> should equal 1

            release.SetResult(())
            do! firstOperation
            first.DisposeCount |> should equal 0

            do! push pipeline third
            do! pipeline.CompleteAsync(CancellationToken.None)

            sink.Bytes |> text |> should equal "AC"
            upstreamCalls |> should equal 2
            downstreamCalls |> should equal 2
            maximumDownstreamCalls |> should equal 1
            first.DisposeCount |> should equal 1
            third.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``multi-stage EOF resolves retained borrowed and generated data before completion`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let inputOwner = owner "AB"
            let pool = new TrackingPool()
            let sink = new RecordingSink()

            let a =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        output.AppendInput(0L, input.Bytes.Length)
                        emit output (bytes "C")
                        completed 0L
                    else
                        output.AppendInput(0L, 1L)
                        completed 1L)

            let b =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        ValueTask<ProcessorResult>(
                            task {
                                sequenceBytes input.Bytes |> text |> should equal "ABC"
                                started.SetResult(())
                                do! release.Task
                                output.AppendInput(0L, input.Bytes.Length)
                                return { RetainedLength = 0L }
                            }
                        )
                    else
                        completed input.Bytes.Length)

            let c =
                processor (fun input output _ ->
                    if input.IsCompleted then
                        emit output (bytes "[")
                        output.AppendInput(0L, input.Bytes.Length)
                        emit output (bytes "]")
                    else
                        output.AppendInput(0L, input.Bytes.Length)

                    completed 0L)

            use pipeline = new ProcessorPipeline([| a; b; c |], sink, pool)
            do! push pipeline inputOwner
            inputOwner.DisposeCount |> should equal 0

            let completion = pipeline.CompleteAsync(CancellationToken.None)
            do! started.Task
            completion.IsCompleted |> should equal false
            sink.Writes.Count |> should equal 0
            sink.Completions |> should equal 0
            inputOwner.DisposeCount |> should equal 0
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 0

            release.SetResult(())
            do! completion

            sink.Bytes |> text |> should equal "[ABC]"
            sink.Completions |> should equal 1
            inputOwner.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 3
            pool.Owners |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``fragmentation invariant processor produces the same logical stream`` () =
        task {
            let patterns =
                [| [| "ABCDEF" |]
                   [| "A"; "BC"; "DEF" |]
                   [| "ABCD"; "E"; "F" |]
                   [| "A"; "B"; "C"; "D"; "E"; "F" |] |]

            for chunks in patterns do
                let sink = new RecordingSink()
                let pool = new TrackingPool()
                let inputs = ResizeArray<TrackingOwner>()

                let stage =
                    processor (fun input output _ ->
                        let available = sequenceBytes input.Bytes
                        let pairBytes = available.Length - available.Length % 2

                        for offset in 0 .. 2 .. pairBytes - 1 do
                            emit output [| available[offset + 1]; available[offset] |]

                        if input.IsCompleted && available.Length % 2 <> 0 then
                            emit output [| available[available.Length - 1] |]

                        let retained =
                            if input.IsCompleted then 0L else int64 (available.Length - pairBytes)

                        completed retained)

                use pipeline = new ProcessorPipeline([| stage |], sink, pool)

                for chunk in chunks do
                    let input = owner chunk
                    inputs.Add(input)
                    do! push pipeline input

                do! pipeline.CompleteAsync(CancellationToken.None)
                sink.Bytes |> text |> should equal "BADCFE"
                inputs |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
                pool.Owners |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``deterministic stress preserves stream and releases every owner`` () =
        task {
            let transform frameSize split marker (input: byte array) =
                let result = ResizeArray<byte>()
                let mutable offset = 0

                while offset < input.Length do
                    let count = min frameSize (input.Length - offset)
                    let prefix = min split count
                    result.AddRange(input.AsSpan(offset, prefix).ToArray())
                    result.Add(marker)
                    result.AddRange(input.AsSpan(offset + prefix, count - prefix).ToArray())
                    offset <- offset + count

                result.ToArray()

            let framedProcessor frameSize split marker asynchronous =
                processor (fun input output _ ->
                    let run () =
                        let length = int input.Bytes.Length

                        let resolved =
                            if input.IsCompleted then length else length - length % frameSize

                        let mutable offset = 0

                        while offset < resolved do
                            let count = min frameSize (resolved - offset)
                            let prefix = min split count

                            if prefix > 0 then
                                output.AppendInput(int64 offset, int64 prefix)

                            emit output [| marker |]

                            if count > prefix then
                                output.AppendInput(
                                    int64 (offset + prefix),
                                    int64 (count - prefix)
                                )

                            offset <- offset + count

                        { RetainedLength = int64 (length - resolved) }

                    if asynchronous then
                        ValueTask<ProcessorResult>(
                            task {
                                do! Task.Yield()
                                return run ()
                            }
                        )
                    else
                        ValueTask<ProcessorResult>(run ()))

            for seed in [| 17; 1701; 65537 |] do
                let random = Random(seed)
                let payload = Array.init 257 (fun _ -> byte (random.Next(0, 251)))
                let expected = payload |> transform 5 2 241uy |> transform 7 1 242uy
                let sink = new RecordingSink()
                let pool = new TrackingPool()
                let inputs = ResizeArray<TrackingOwner>()

                let passThrough =
                    processor (fun input output _ ->
                        ValueTask<ProcessorResult>(
                            task {
                                do! Task.Yield()
                                output.AppendInput(0L, input.Bytes.Length)
                                return { RetainedLength = 0L }
                            }
                        ))

                use pipeline =
                    new ProcessorPipeline(
                        [| framedProcessor 5 2 241uy false
                           framedProcessor 7 1 242uy true
                           passThrough |],
                        sink,
                        pool
                    )

                let mutable offset = 0

                while offset < payload.Length do
                    let count = min (random.Next(1, 18)) (payload.Length - offset)
                    let input = new TrackingOwner(payload.AsSpan(offset, count).ToArray())
                    inputs.Add(input)
                    do! push pipeline input
                    offset <- offset + count

                do! pipeline.CompleteAsync(CancellationToken.None)
                sink.Bytes |> should equal expected
                inputs |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
                pool.Owners |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``deterministic stress cancellation discards the active invocation`` () =
        task {
            use cancellation = new CancellationTokenSource()
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let sink = new RecordingSink()
            let pool = new TrackingPool()
            let inputs = ResizeArray<TrackingOwner>()
            let mutable calls = 0

            let a =
                processor (fun input output _ ->
                    output.AppendInput(0L, 1L)
                    emit output [| 241uy |]
                    output.AppendInput(1L, input.Bytes.Length - 1L)
                    completed 0L)

            let b =
                processor (fun input output cancellationToken ->
                    ValueTask<ProcessorResult>(
                        task {
                            calls <- calls + 1
                            output.AppendInput(0L, input.Bytes.Length)
                            emit output [| 242uy |]

                            if calls = 4 then
                                started.SetResult(())
                                do! Task.Delay(Timeout.Infinite, cancellationToken)
                            else
                                do! Task.Yield()

                            return { RetainedLength = 0L }
                        }
                    ))

            use pipeline = new ProcessorPipeline([| a; b |], sink, pool)

            for value in [| "AAAA"; "BBBB"; "CCCC" |] do
                let input = owner value
                inputs.Add(input)
                do! pipeline.PushAsync(input, input.Buffer.Length, cancellation.Token)

            let finalInput = owner "DDDD"
            inputs.Add(finalInput)

            let operation =
                pipeline.PushAsync(finalInput, finalInput.Buffer.Length, cancellation.Token)

            do! started.Task
            cancellation.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)

            let expected =
                [| [| 65uy; 241uy; 65uy; 65uy; 65uy; 242uy |]
                   [| 66uy; 241uy; 66uy; 66uy; 66uy; 242uy |]
                   [| 67uy; 241uy; 67uy; 67uy; 67uy; 242uy |] |]
                |> Array.concat

            sink.Bytes |> should equal expected
            inputs |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
            pool.Owners |> Seq.forall (fun item -> item.DisposeCount = 1) |> should equal true
        }

    [<Fact>]
    let ``runtime memory release callback precedes processor disposal and runs once`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let logical = new CapacityBudget(16L)
            let disposalStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let releaseDisposal = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let disposalCompleted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let memoryReleased = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable callbackCount = 0

            let retaining =
                { new IByteStreamProcessor with
                    member _.ProcessAsync(input, _, _) =
                        completed input.Bytes.Length

                  interface IAsyncDisposable with
                      member _.DisposeAsync() =
                          ValueTask(
                              task {
                                  disposalStarted.TrySetResult(()) |> ignore
                                  do! releaseDisposal.Task
                                  disposalCompleted.TrySetResult(()) |> ignore
                              }
                          ) }

            let pipeline =
                new ProcessorPipeline(
                    [| retaining |],
                    RecordingSink(),
                    MemoryPool<byte>.Shared,
                    accounting,
                    logical
                )

            let input = owner "retained"
            do! push pipeline input
            (logicalSnapshot logical).CurrentBytes |> should equal 8L
            physicalBytes accounting |> should equal 8L

            let disposing =
                pipeline.DisposeAsync(fun () ->
                    Interlocked.Increment(&callbackCount) |> ignore
                    (logicalSnapshot logical).CurrentBytes |> should equal 0L
                    physicalBytes accounting |> should equal 0L
                    input.DisposeCount |> should equal 1
                    memoryReleased.TrySetResult(()) |> ignore)
                |> _.AsTask()

            do! memoryReleased.Task
            do! disposalStarted.Task
            disposing.IsCompleted |> should equal false
            disposalCompleted.Task.IsCompleted |> should equal false

            releaseDisposal.TrySetResult(()) |> ignore
            do! disposing
            do! disposalCompleted.Task

            do!
                pipeline.DisposeAsync(fun () -> Interlocked.Increment(&callbackCount) |> ignore).AsTask()

            Volatile.Read(&callbackCount) |> should equal 1
        }

    [<Fact>]
    let ``runtime memory callback observes conservative physical cleanup failure`` () =
        task {
            let accounting = new ConnectionMemoryAccounting()
            let logical = new CapacityBudget(8L)
            let input = new ThrowingDisposeOwner(bytes "ABC")
            let mutable callbackCount = 0

            let retaining =
                processor (fun processorInput _ _ -> completed processorInput.Bytes.Length)

            let pipeline =
                new ProcessorPipeline(
                    [| retaining |],
                    RecordingSink(),
                    MemoryPool<byte>.Shared,
                    accounting,
                    logical
                )

            do!
                pipeline.PushAsync(
                    input :> IMemoryOwner<byte>,
                    input.Buffer.Length,
                    CancellationToken.None
                )

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    let operation =
                        pipeline.DisposeAsync(fun () ->
                            Interlocked.Increment(&callbackCount) |> ignore
                            (logicalSnapshot logical).CurrentBytes |> should equal 0L
                            physicalBytes accounting |> should equal 3L)

                    operation.AsTask())

            observed.Message |> should equal "owner disposal failed"
            Volatile.Read(&callbackCount) |> should equal 1
            input.DisposeCount |> should equal 1
            (logicalSnapshot logical).CurrentBytes |> should equal 0L
            physicalBytes accounting |> should equal 3L
        }

    [<Fact>]
    let ``runtime memory callback failure does not skip processor disposal`` () =
        task {
            let events = ResizeArray<string>()
            let processorFailure = InvalidOperationException("processor disposal failed")
            let processor = new BothDisposableProcessor("processor", events, Some processorFailure)
            let pipeline =
                new ProcessorPipeline(
                    [| processor :> IByteStreamProcessor |],
                    RecordingSink(),
                    MemoryPool<byte>.Shared
                )

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () ->
                    pipeline.DisposeAsync(fun () -> invalidOp "physical share handoff failed").AsTask())

            observed.InnerExceptions
            |> Seq.map _.Message
            |> Seq.toArray
            |> should equal [| "physical share handoff failed"; "processor disposal failed" |]

            events |> Seq.toArray |> should equal [| "async:processor" |]
        }

    [<Fact>]
    let ``pipeline disposes processors once in reverse order and prefers async disposal`` () =
        task {
            let events = ResizeArray<string>()
            let sink = new RecordingSink()

            let first =
                new BothDisposableProcessor("first", events, None)
                :> IByteStreamProcessor

            let second =
                new SyncDisposableProcessor("second", events)
                :> IByteStreamProcessor

            let third =
                new BothDisposableProcessor("third", events, None)
                :> IByteStreamProcessor

            let pipeline =
                new ProcessorPipeline(
                    [| first; second; third |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            do! push pipeline (owner "ABC")
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()

            events
            |> Seq.toArray
            |> should equal [| "async:third"; "sync:second"; "async:first" |]
        }

    [<Fact>]
    let ``pipeline continues reverse disposal after a processor cleanup failure`` () =
        task {
            let events = ResizeArray<string>()
            let sink = new RecordingSink()
            let expected = InvalidOperationException("dispose failed")

            let processors =
                [| new BothDisposableProcessor("first", events, None)
                   new BothDisposableProcessor("second", events, Some expected)
                   new BothDisposableProcessor("third", events, None) |]
                |> Array.map (fun value -> value :> IByteStreamProcessor)

            let pipeline =
                new ProcessorPipeline(processors, sink, MemoryPool<byte>.Shared)

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())

            Object.ReferenceEquals(expected, observed) |> should equal true

            events
            |> Seq.toArray
            |> should equal [| "async:third"; "async:second"; "async:first" |]

            do! (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()
            events.Count |> should equal 3
        }

    [<Fact>]
    let ``pipeline aggregates multiple processor cleanup failures`` () =
        task {
            let events = ResizeArray<string>()
            let sink = new RecordingSink()
            let firstError = InvalidOperationException("first dispose failed")
            let secondError = InvalidOperationException("second dispose failed")

            let processors =
                [| new BothDisposableProcessor("first", events, Some firstError)
                   new BothDisposableProcessor("second", events, Some secondError)
                   new BothDisposableProcessor("third", events, None) |]
                |> Array.map (fun value -> value :> IByteStreamProcessor)

            let pipeline =
                new ProcessorPipeline(processors, sink, MemoryPool<byte>.Shared)

            let! observed =
                Assert.ThrowsAsync<AggregateException>(fun () ->
                    (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())

            observed.InnerExceptions.Count |> should equal 2
            Object.ReferenceEquals(secondError, observed.InnerExceptions[0]) |> should equal true
            Object.ReferenceEquals(firstError, observed.InnerExceptions[1]) |> should equal true
            events |> Seq.toArray |> should equal [| "async:third"; "async:second"; "async:first" |]
        }
