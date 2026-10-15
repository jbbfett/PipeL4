namespace PipeL4.Tests

open System
open System.Buffers
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

    type private TrackingPool() =
        inherit MemoryPool<byte>()

        let owners = ResizeArray<TrackingOwner>()

        member _.Owners = owners
        override _.MaxBufferSize = Int32.MaxValue

        override _.Rent(sizeHint) =
            let owner = new TrackingOwner(Array.zeroCreate<byte> (max sizeHint 1))
            owners.Add(owner)
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

    let private push (pipeline: ProcessorPipeline) (input: TrackingOwner) =
        pipeline.PushAsync(input, input.Buffer.Length, CancellationToken.None)

    [<Fact>]
    let ``empty pipeline passes through the owned input and completes`` () =
        task {
            let input = owner "ABC"
            let sink = RecordingSink()
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
    let ``one stage borrows its input without copying`` () =
        task {
            let input = owner "ABCDEF"
            let sink = RecordingSink()
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
            let sink = RecordingSink()
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
            let sink = RecordingSink()

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
            let sink = RecordingSink()

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
            let sink = RecordingSink()
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
            let sink = RecordingSink()
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
            let sink = RecordingSink()

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
            let sink = RecordingSink()

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
            let sink = RecordingSink()
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
            let sink = RecordingSink()
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
        }

    [<Fact>]
    let ``pre-cancelled push releases its valid input owner`` () =
        task {
            use cancellation = new CancellationTokenSource()
            cancellation.Cancel()
            let inputOwner = owner "ABC"
            let sink = RecordingSink()
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
            let sink = RecordingSink()

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
            let sink = RecordingSink()
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
        let sink = RecordingSink()
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
                let sink = RecordingSink()
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

                let retainedContains (value: string) =
                    retainedText.IndexOfAny(value.ToCharArray()) >= 0

                first.DisposeCount |> should equal (if retainedContains "ABC" then 0 else 1)
                second.DisposeCount |> should equal (if retainedContains "DE" then 0 else 1)
                third.DisposeCount |> should equal (if retainedContains "FGHI" then 0 else 1)

                do! push pipeline fourth
                observed |> should equal (retainedText + "J")
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
            let sink = RecordingSink()
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
            let sink = RecordingSink()
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
            let sink = RecordingSink()
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
            let sink = RecordingSink()

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
                let sink = RecordingSink()
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
                let sink = RecordingSink()
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
            let sink = RecordingSink()
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
