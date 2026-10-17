namespace PipeL4.Tests

open System
open System.Buffers
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open Xunit

module ScriptProcessorTests =
    let private bytes (text: string) = Encoding.ASCII.GetBytes text
    let private text (value: byte array) = Encoding.ASCII.GetString value

    let private sequenceBytes (sequence: ReadOnlySequence<byte>) =
        let result = Array.zeroCreate<byte> (int sequence.Length)
        sequence.CopyTo(result.AsSpan())
        result

    let private sequenceSegments (sequence: ReadOnlySequence<byte>) =
        let segments = ResizeArray<ReadOnlyMemory<byte>>()
        let mutable position = sequence.Start
        let mutable segment = ReadOnlyMemory<byte>.Empty

        while sequence.TryGet(&position, &segment) do
            segments.Add segment

        segments.ToArray()

    let private sequenceSegmentCount (sequence: ReadOnlySequence<byte>) =
        sequenceSegments sequence |> Array.length

    type private TrackingOwner(buffer: byte array) =
        let mutable disposeCount = 0

        member _.Buffer = buffer
        member _.DisposeCount = Volatile.Read(&disposeCount)

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)

            member _.Dispose() =
                Interlocked.Increment(&disposeCount) |> ignore

    type private TrackingPool() =
        inherit MemoryPool<byte>()

        let owners = ResizeArray<TrackingOwner>()

        member _.Owners = owners
        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            let owner = new TrackingOwner(Array.zeroCreate<byte> (max sizeHint 1))
            owners.Add owner
            owner :> IMemoryOwner<byte>

        override _.Dispose(_disposing) = ()

    type private RecordingSink() =
        let writes = ResizeArray<byte array>()
        let mutable completions = 0
        let mutable onWrite = ignore

        member _.Bytes = writes |> Seq.toArray |> Array.concat
        member _.Completions = completions
        member _.OnWrite with set callback = onWrite <- callback

        interface IProcessorPipelineSink with
            member _.WriteAsync(value, cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                onWrite value
                writes.Add(sequenceBytes value)
                ValueTask()

            member _.CompleteAsync(cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()
                completions <- completions + 1
                ValueTask()

    let private completed retainedLength =
        let result: PipeL4.Plugins.ProcessorResult =
            { RetainedLength = retainedLength }

        ValueTask<PipeL4.Plugins.ProcessorResult>(result)

    let private adapt (processor: PipeL4.Plugins.IByteStreamProcessor) =
        PipeL4.Plugins.ScriptProcessorAdapter(processor) :> IByteStreamProcessor

    let private push (pipeline: ProcessorPipeline) (owner: TrackingOwner) cancellationToken =
        pipeline.PushAsync(owner, owner.Buffer.Length, cancellationToken)

    type private MixedOutputProcessor() =
        let mutable sawSegmentedInput = false
        let mutable sawCompletion = false

        member _.SawSegmentedInput = sawSegmentedInput
        member _.SawCompletion = sawCompletion

        interface PipeL4.Plugins.IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                cancellationToken.ThrowIfCancellationRequested()

                if input.IsCompleted then
                    sawCompletion <- true

                if not input.IsCompleted && input.Bytes.Length < 6L then
                    completed input.Bytes.Length
                elif input.Bytes.IsEmpty then
                    completed 0L
                else
                    sawSegmentedInput <- sequenceSegmentCount input.Bytes > 1
                    output.AppendInput(0L, 2L)

                    let generated = bytes "XYZ"
                    generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                    output.Advance(generated.Length)
                    generated.AsSpan().Clear()

                    output.AppendInput(4L, 2L)
                    completed 0L

    type private SuspendingProcessor
        (
            started: TaskCompletionSource<unit>,
            release: TaskCompletionSource<unit>
        ) =
        interface PipeL4.Plugins.IByteStreamProcessor with
            member _.ProcessAsync(input, output, cancellationToken) =
                ValueTask<PipeL4.Plugins.ProcessorResult>(
                    task {
                        started.TrySetResult(()) |> ignore
                        do! release.Task.WaitAsync(cancellationToken)
                        output.AppendInput(0L, input.Bytes.Length)
                        return ({ RetainedLength = 0L }: PipeL4.Plugins.ProcessorResult)
                    }
                )

    [<Fact>]
    let ``adapter preserves segmented input and ordered mixed output ownership`` () =
        task {
            let firstInput = new TrackingOwner(bytes "AB")
            let secondInput = new TrackingOwner(bytes "CDEF")
            let pool = new TrackingPool()
            let sink = RecordingSink()
            let processor = MixedOutputProcessor()
            let mutable preservedInputBacking = false

            sink.OnWrite <- fun sequence ->
                let segments = sequenceSegments sequence
                segments.Length |> should equal 3

                let mutable firstSegment = Unchecked.defaultof<ArraySegment<byte>>
                let mutable lastSegment = Unchecked.defaultof<ArraySegment<byte>>
                MemoryMarshal.TryGetArray(segments[0], &firstSegment) |> should equal true
                MemoryMarshal.TryGetArray(segments[2], &lastSegment) |> should equal true

                preservedInputBacking <-
                    Object.ReferenceEquals(firstSegment.Array, firstInput.Buffer)
                    && Object.ReferenceEquals(lastSegment.Array, secondInput.Buffer)

                firstInput.DisposeCount |> should equal 0
                secondInput.DisposeCount |> should equal 0
                pool.Owners[0].DisposeCount |> should equal 0

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    pool
                )

            do! push pipeline firstInput CancellationToken.None
            firstInput.DisposeCount |> should equal 0
            sink.Bytes |> should be Empty

            do! push pipeline secondInput CancellationToken.None
            do! pipeline.CompleteAsync CancellationToken.None

            processor.SawSegmentedInput |> should equal true
            processor.SawCompletion |> should equal true
            preservedInputBacking |> should equal true
            sink.Bytes |> text |> should equal "ABXYZEF"
            sink.Completions |> should equal 1
            firstInput.DisposeCount |> should equal 1
            secondInput.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter keeps invocation memory valid across asynchronous suspension`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let input = new TrackingOwner(bytes "ABC")
            let sink = RecordingSink()
            let processor = SuspendingProcessor(started, release)

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            let operation = push pipeline input CancellationToken.None
            do! started.Task

            operation.IsCompleted |> should equal false
            input.DisposeCount |> should equal 0

            release.SetResult(())
            do! operation

            sink.Bytes |> text |> should equal "ABC"
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter passes cancellation to an asynchronous processor and releases input`` () =
        task {
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let input = new TrackingOwner(bytes "ABC")
            let sink = RecordingSink()
            let processor = SuspendingProcessor(started, release)
            use cancellation = new CancellationTokenSource()

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            let operation = push pipeline input cancellation.Token
            do! started.Task
            cancellation.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation :> Task)

            input.DisposeCount |> should equal 1
            sink.Bytes |> should be Empty
        }

    [<Fact>]
    let ``adapter preserves borrowed range validation`` () =
        task {
            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(input, output, _) =
                        output.AppendInput(0L, input.Bytes.Length + 1L)
                        completed 0L }

            let input = new TrackingOwner(bytes "A")
            let sink = RecordingSink()

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            let! error =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    push pipeline input CancellationToken.None :> Task)

            error.Message.Contains("output range", StringComparison.Ordinal)
            |> should equal true

            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter preserves retained suffix validation`` () =
        task {
            for retainedLength in [ -1L; 2L ] do
                let processor =
                    { new PipeL4.Plugins.IByteStreamProcessor with
                        member _.ProcessAsync(_, _, _) = completed retainedLength }

                let input = new TrackingOwner(bytes "A")
                let sink = RecordingSink()

                use pipeline =
                    new ProcessorPipeline(
                        [| adapt processor |],
                        sink,
                        MemoryPool<byte>.Shared
                    )

                let! error =
                    Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                        push pipeline input CancellationToken.None :> Task)

                error.Message.Contains("retained", StringComparison.Ordinal)
                |> should equal true

                input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter rejects retained input after upstream completion`` () =
        task {
            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(input, _, _) = completed input.Bytes.Length }

            let input = new TrackingOwner(bytes "A")
            let sink = RecordingSink()

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            do! push pipeline input CancellationToken.None
            input.DisposeCount |> should equal 0

            let! error =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.CompleteAsync(CancellationToken.None) :> Task)

            error.Message.Contains("after upstream completion", StringComparison.Ordinal)
            |> should equal true

            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter output cannot be reused after its invocation`` () =
        task {
            let mutable capturedOutput: PipeL4.Plugins.IProcessorOutput option = None

            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(_, output, _) =
                        capturedOutput <- Some output
                        completed 0L }

            let input = new TrackingOwner(bytes "A")
            let sink = RecordingSink()

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            do! push pipeline input CancellationToken.None

            let output = capturedOutput |> Option.get

            let error =
                Assert.Throws<InvalidOperationException>(fun () ->
                    output.AppendInput(0L, 0L))

            error.Message.Contains("after its invocation", StringComparison.Ordinal)
            |> should equal true

            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter failure after mixed output is atomic and releases every owner`` () =
        task {
            let expected = InvalidOperationException("script failed after output")
            let input = new TrackingOwner(bytes "ABC")
            let pool = new TrackingPool()
            let sink = RecordingSink()

            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(input, output, _) =
                        output.AppendInput(0L, 1L)
                        let generated = bytes "XY"
                        generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                        output.Advance(generated.Length)
                        output.AppendInput(2L, 1L)
                        raise expected }

            use pipeline =
                new ProcessorPipeline([| adapt processor |], sink, pool)

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    push pipeline input CancellationToken.None :> Task)

            Object.ReferenceEquals(expected, observed) |> should equal true
            sink.Bytes |> should be Empty
            input.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter asynchronous failure after retained input releases shared memory`` () =
        task {
            let expected = InvalidOperationException("script failed asynchronously")
            let first = new TrackingOwner(bytes "AB")
            let second = new TrackingOwner(bytes "C")
            let pool = new TrackingPool()
            let sink = RecordingSink()
            let mutable calls = 0

            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(input, output, _) =
                        calls <- calls + 1

                        if calls = 1 then
                            completed input.Bytes.Length
                        else
                            ValueTask<PipeL4.Plugins.ProcessorResult>(
                                task {
                                    output.AppendInput(0L, 1L)
                                    let generated = bytes "Z"
                                    generated.AsSpan().CopyTo(output.GetSpan(1))
                                    output.Advance(1)
                                    output.AppendInput(input.Bytes.Length - 1L, 1L)
                                    do! Task.Yield()
                                    return raise expected
                                }
                            ) }

            use pipeline =
                new ProcessorPipeline([| adapt processor |], sink, pool)

            do! push pipeline first CancellationToken.None
            first.DisposeCount |> should equal 0

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    push pipeline second CancellationToken.None :> Task)

            Object.ReferenceEquals(expected, observed) |> should equal true
            sink.Bytes |> should be Empty
            first.DisposeCount |> should equal 1
            second.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter asynchronous failure before output releases input and preserves exception`` () =
        task {
            let expected = InvalidOperationException("script failed before output")
            let input = new TrackingOwner(bytes "ABC")
            let sink = RecordingSink()

            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(_, _, _) =
                        ValueTask<PipeL4.Plugins.ProcessorResult>(
                            task {
                                do! Task.Yield()
                                return raise expected
                            }
                        ) }

            use pipeline =
                new ProcessorPipeline(
                    [| adapt processor |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    push pipeline input CancellationToken.None :> Task)

            Object.ReferenceEquals(expected, observed) |> should equal true
            sink.Bytes |> should be Empty
            input.DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter final EOF failure discards final output and retained input`` () =
        task {
            let expected = InvalidOperationException("script failed at EOF")
            let input = new TrackingOwner(bytes "AB")
            let pool = new TrackingPool()
            let sink = RecordingSink()

            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(input, output, _) =
                        if input.IsCompleted then
                            output.AppendInput(0L, input.Bytes.Length)
                            let generated = bytes "!"
                            generated.AsSpan().CopyTo(output.GetSpan(1))
                            output.Advance(1)
                            raise expected
                        else
                            completed input.Bytes.Length }

            use pipeline =
                new ProcessorPipeline([| adapt processor |], sink, pool)

            do! push pipeline input CancellationToken.None
            input.DisposeCount |> should equal 0

            let! observed =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    pipeline.CompleteAsync(CancellationToken.None) :> Task)

            Object.ReferenceEquals(expected, observed) |> should equal true
            sink.Bytes |> should be Empty
            input.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }

    [<Fact>]
    let ``adapter rejects unadvanced generated output and releases its owner`` () =
        task {
            let input = new TrackingOwner(bytes "A")
            let pool = new TrackingPool()
            let sink = RecordingSink()

            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(_, output, _) =
                        output.GetMemory(8) |> ignore
                        completed 0L }

            use pipeline =
                new ProcessorPipeline([| adapt processor |], sink, pool)

            let! error =
                Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                    push pipeline input CancellationToken.None :> Task)

            error.Message.Contains("not advanced", StringComparison.Ordinal)
            |> should equal true

            sink.Bytes |> should be Empty
            input.DisposeCount |> should equal 1
            pool.Owners.Count |> should equal 1
            pool.Owners[0].DisposeCount |> should equal 1
        }
