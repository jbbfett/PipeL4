namespace PipeL4.Tests

open System
open System.Buffers
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open PipeL4.PluginHost
open PipeL4.Plugins
open Xunit

module FunctionalProcessorAdapterTests =
    let private bytes (value: string) = Encoding.ASCII.GetBytes value

    let private text (value: ReadOnlySequence<byte>) =
        let buffer = Array.zeroCreate<byte> (int value.Length)
        value.CopyTo(buffer.AsSpan())
        Encoding.ASCII.GetString buffer

    type private Owner(buffer: byte array) =
        let mutable disposals = 0
        member _.Buffer = buffer
        member _.Disposals = disposals

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() =
                disposals <- disposals + 1
                buffer.AsSpan().Fill(221uy)

    type private Pool() =
        inherit MemoryPool<byte>()
        let owners = ResizeArray<Owner>()
        member _.Owners = owners
        override _.MaxBufferSize = Int32.MaxValue
        override _.Rent(sizeHint) =
            let owner = new Owner(Array.zeroCreate<byte> (max 16 sizeHint))
            owners.Add owner
            owner :> IMemoryOwner<byte>
        override _.Dispose(_) = ()

    type private Sink(check: ReadOnlySequence<byte> -> unit) =
        let writes = ResizeArray<string>()
        let mutable completions = 0
        member _.Text = String.concat "" writes
        member _.Completions = completions

        interface IProcessorPipelineSink with
            member _.WriteAsync(value, _) =
                check value
                writes.Add(text value)
                ValueTask()
            member _.CompleteAsync(_) =
                completions <- completions + 1
                ValueTask()

    let private handle scenario (ctx: ProcessContext) =
        match scenario with
        | "suffix" ->
            let retained = if ctx.IsCompleted then 0L else 1L
            emitInput ctx 0L (ctx.Input.Length - retained)
            retainSuffix retained
        | "simultaneous" ->
            emitInput ctx 0L ctx.Input.Length
            retainSuffix (if ctx.IsCompleted then 0L else 1L)
        | "overlap" when not ctx.IsCompleted ->
            emitInput ctx (ctx.Input.Length - 1L) 1L
            emitInput ctx (ctx.Input.Length - 1L) 1L
            retainSuffix 1L
        | "segmented" when not ctx.IsCompleted && ctx.Input.Length < 6L ->
            retainSuffix ctx.Input.Length
        | "eof" when not ctx.IsCompleted -> retainSuffix ctx.Input.Length
        | _ when ctx.Input.IsEmpty -> releaseAll
        | "drop" -> releaseAll
        | "generated" ->
            emitBytes ctx (ReadOnlySpan<byte>(bytes "XYZ"))
            releaseAll
        | "mixed" ->
            emitInput ctx 0L 1L
            let generated = bytes "!"
            emitBytes ctx (ReadOnlySpan<byte>(generated))
            generated.AsSpan().Clear()
            emitInput ctx (ctx.Input.Length - 1L) 1L
            releaseAll
        | "ordered" ->
            emitInput ctx 0L 1L
            emitInput ctx (ctx.Input.Length - 1L) 1L
            emitInput ctx 0L 1L
            releaseAll
        | "repeated" ->
            emitInput ctx 0L ctx.Input.Length
            emitInput ctx 0L ctx.Input.Length
            releaseAll
        | _ ->
            emitInput ctx 0L ctx.Input.Length
            releaseAll

    // Independent class implementation exercises the established public adapter path.
    type private EquivalentProcessor(scenario, observe) =
        interface PipeL4.Plugins.IByteStreamProcessor with
            member _.ProcessAsync(input, output, token) =
                observe input.Bytes input.IsCompleted token
                let mutable retained = 0L
                let emitGenerated value =
                    let generated = bytes value
                    generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                    output.Advance(generated.Length)
                    generated.AsSpan().Clear()

                match scenario with
                | "suffix" ->
                    retained <- if input.IsCompleted then 0L else 1L
                    output.AppendInput(0L, input.Bytes.Length - retained)
                | "simultaneous" ->
                    output.AppendInput(0L, input.Bytes.Length)
                    retained <- if input.IsCompleted then 0L else 1L
                | "overlap" when not input.IsCompleted ->
                    output.AppendInput(input.Bytes.Length - 1L, 1L)
                    output.AppendInput(input.Bytes.Length - 1L, 1L)
                    retained <- 1L
                | "segmented" when not input.IsCompleted && input.Bytes.Length < 6L ->
                    retained <- input.Bytes.Length
                | "eof" when not input.IsCompleted -> retained <- input.Bytes.Length
                | _ when input.Bytes.IsEmpty -> ()
                | "drop" -> ()
                | "generated" -> emitGenerated "XYZ"
                | "mixed" ->
                    output.AppendInput(0L, 1L)
                    emitGenerated "!"
                    output.AppendInput(input.Bytes.Length - 1L, 1L)
                | "ordered" ->
                    output.AppendInput(0L, 1L)
                    output.AppendInput(input.Bytes.Length - 1L, 1L)
                    output.AppendInput(0L, 1L)
                | "repeated" ->
                    output.AppendInput(0L, input.Bytes.Length)
                    output.AppendInput(0L, input.Bytes.Length)
                | _ -> output.AppendInput(0L, input.Bytes.Length)

                ValueTask<PipeL4.Plugins.ProcessorResult>({ RetainedLength = retained })

    let private run functional scenario =
        task {
            use state = new ConnectionState()
            let publicState = ConnectionStateAdapter(state) :> IConnectionState
            use cancellation = new CancellationTokenSource()
            use pool = new Pool()
            let inputs = [| new Owner(bytes "AB"); new Owner(bytes "CDEF") |]
            let invocations = ResizeArray<string * bool * int>()
            let observe (input: ReadOnlySequence<byte>) completed token =
                token |> should equal cancellation.Token
                let mutable segments = 0
                for segment in input do
                    if not segment.IsEmpty then
                        segments <- segments + 1
                invocations.Add(text input, completed, segments)

            let processor =
                if functional then
                    FunctionalProcessorAdapter(
                        (fun ctx ->
                            Assert.Same(publicState, ctx.ConnectionState)
                            observe ctx.Input ctx.IsCompleted ctx.CancellationToken
                            handle scenario ctx),
                        publicState
                    ) :> PipeL4.Core.IByteStreamProcessor
                else
                    ScriptProcessorAdapter(EquivalentProcessor(scenario, observe))
                    :> PipeL4.Core.IByteStreamProcessor

            let sink =
                Sink(fun sequence ->
                    for segment in sequence do
                        let mutable backing = Unchecked.defaultof<ArraySegment<byte>>
                        MemoryMarshal.TryGetArray(segment, &backing) |> should equal true
                        // Every output segment must still refer to a live runtime owner.
                        let owner =
                            Seq.append inputs pool.Owners
                            |> Seq.find (fun owner -> Object.ReferenceEquals(backing.Array, owner.Buffer))
                        owner.Disposals |> should equal 0)

            use pipeline = new ProcessorPipeline([| processor |], sink, pool)
            let samples = ResizeArray<int64 * int64 * int64>()
            let sample () =
                let metrics = pipeline.GetStageMetricsSnapshot() |> List.exactlyOne
                samples.Add(
                    metrics.CurrentRetainedBytes,
                    pipeline.GetLogicalCapacitySnapshot().CurrentBytes,
                    pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes
                )

            for input in inputs do
                do! pipeline.PushAsync(input, input.Buffer.Length, cancellation.Token)
                sample ()

            do! pipeline.CompleteAsync cancellation.Token
            sample ()
            let metrics = pipeline.GetStageMetricsSnapshot() |> List.exactlyOne
            metrics.SuccessfulInvocationCount |> should equal 3L
            sink.Completions |> should equal 1
            samples[samples.Count - 1] |> should equal (0L, 0L, 0L)
            for owner in Seq.append inputs pool.Owners do
                owner.Disposals |> should equal 1

            return
                (sink.Text, invocations.ToArray(), samples.ToArray(),
                 metrics.InputBackedOutputBytes, metrics.GeneratedOutputBytes,
                 pipeline.GetLogicalCapacitySnapshot().PeakBytes,
                 pipeline.GetConnectionMemorySnapshot().PeakPhysicalBufferCapacityBytes)
        }

    [<Theory>]
    [<InlineData("passthrough", "ABCDEF")>]
    [<InlineData("drop", "")>]
    [<InlineData("generated", "XYZXYZ")>]
    [<InlineData("mixed", "A!BC!F")>]
    [<InlineData("ordered", "ABACFC")>]
    [<InlineData("repeated", "ABABCDEFCDEF")>]
    [<InlineData("suffix", "ABCDEF")>]
    [<InlineData("simultaneous", "ABBCDEFF")>]
    [<InlineData("overlap", "BBFFF")>]
    [<InlineData("segmented", "ABCDEF")>]
    [<InlineData("eof", "ABCDEF")>]
    let ``functional and class processors have equivalent executor outcomes`` scenario (expected: string) =
        task {
            let! functional = run true scenario
            let! classBased = run false scenario
            functional |> should equal classBased
            let output, invocations, samples, borrowed, generated, _, _ = functional
            output |> should equal expected
            borrowed + generated |> should equal (int64 expected.Length)
            match scenario with
            | "suffix"
            | "simultaneous"
            | "overlap" ->
                invocations |> should equal [| ("AB", false, 1); ("BCDEF", false, 2); ("F", true, 1) |]
                samples[0] |> should equal (1L, 1L, 2L)
                samples[1] |> should equal (1L, 1L, 4L)
            | "segmented" ->
                invocations |> should equal [| ("AB", false, 1); ("ABCDEF", false, 2); ("", true, 0) |]
            | "eof" ->
                invocations |> should equal [| ("AB", false, 1); ("ABCDEF", false, 2); ("ABCDEF", true, 2) |]
            | _ -> ()
        }

    let private adapt functional state (action: ProcessContext -> PipeL4.Plugins.ProcessorResult) =
        if functional then
            FunctionalProcessorAdapter(action, state) :> PipeL4.Core.IByteStreamProcessor
        else
            let processor =
                { new PipeL4.Plugins.IByteStreamProcessor with
                    member _.ProcessAsync(input, output, token) =
                        let result =
                            action
                                { Input = input.Bytes
                                  IsCompleted = input.IsCompleted
                                  Output = output
                                  CancellationToken = token
                                  ConnectionState = state }
                        ValueTask<PipeL4.Plugins.ProcessorResult>(result) }
            ScriptProcessorAdapter(processor) :> PipeL4.Core.IByteStreamProcessor

    [<Theory>]
    [<InlineData("negative")>]
    [<InlineData("too-long")>]
    [<InlineData("eof")>]
    [<InlineData("range")>]
    [<InlineData("uncommitted")>]
    [<InlineData("throw")>]
    [<InlineData("cancel")>]
    let ``functional failures preserve class validation atomicity and cleanup`` scenario =
        task {
            let outcomes = ResizeArray<string * string * int64 * int64>()
            for functional in [ true; false ] do
                use state = new ConnectionState()
                use pool = new Pool()
                use cancellation = new CancellationTokenSource()
                let publicState = ConnectionStateAdapter(state) :> IConnectionState
                let expected = InvalidOperationException("handler failed")
                let mutable calls = 0
                let action ctx =
                    calls <- calls + 1
                    ctx.CancellationToken |> should equal cancellation.Token
                    if scenario = "eof" && not ctx.IsCompleted then
                        retainSuffix ctx.Input.Length
                    else
                        emitInput ctx 0L 1L
                        emitBytes ctx (ReadOnlySpan<byte>(bytes "!"))
                        match scenario with
                        | "negative" -> retainSuffix -1L
                        | "too-long" -> retainSuffix (ctx.Input.Length + 1L)
                        | "eof" -> retainSuffix 1L
                        | "range" ->
                            emitInput ctx 0L (ctx.Input.Length + 1L)
                            releaseAll
                        | "uncommitted" ->
                            ctx.Output.GetMemory(1) |> ignore
                            releaseAll
                        | "cancel" ->
                            cancellation.Cancel()
                            ctx.CancellationToken.ThrowIfCancellationRequested()
                            releaseAll
                        | _ -> raise expected

                let sink = Sink(ignore)
                let input = new Owner(bytes "AB")
                use pipeline = new ProcessorPipeline([| adapt functional publicState action |], sink, pool)
                let operation =
                    task {
                        do! pipeline.PushAsync(input, 2, cancellation.Token)
                        do! pipeline.CompleteAsync cancellation.Token
                    }
                let! error = Assert.ThrowsAnyAsync<Exception>(fun () -> operation :> Task)
                if scenario = "cancel" then
                    let cancelled = Assert.IsAssignableFrom<OperationCanceledException>(error)
                    cancelled.CancellationToken |> should equal cancellation.Token
                elif scenario = "throw" then
                    Assert.Same(expected, error)
                else
                    Assert.IsType<InvalidOperationException>(error) |> ignore
                calls |> should equal (if scenario = "eof" then 2 else 1)
                sink.Text |> should equal ""
                sink.Completions |> should equal 0
                for owner in Seq.append [ input ] pool.Owners do
                    owner.Disposals |> should equal 1
                let logical = pipeline.GetLogicalCapacitySnapshot().CurrentBytes
                let physical = pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes
                logical |> should equal 0L
                physical |> should equal 0L
                outcomes.Add(error.GetType().Name, error.Message, logical, physical)
            outcomes[0] |> should equal outcomes[1]
        }

    [<Fact>]
    let ``functional and class output preserve sink backpressure and invocation lifetime`` () =
        task {
            for functional in [ true; false ] do
                use state = new ConnectionState()
                use pool = new Pool()
                use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
                let input = new Owner(bytes "AB")
                let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let mutable capturedOutput = None
                let action ctx =
                    capturedOutput <- Some ctx.Output
                    emitInput ctx 0L ctx.Input.Length
                    emitBytes ctx (ReadOnlySpan<byte>(bytes "!"))
                    releaseAll
                let sink =
                    { new IProcessorPipelineSink with
                        member _.WriteAsync(value, token) =
                            text value |> should equal "AB!"
                            input.Disposals |> should equal 0
                            pool.Owners[0].Disposals |> should equal 0
                            started.SetResult(())
                            ValueTask(release.Task.WaitAsync(token))
                        member _.CompleteAsync(_) = ValueTask() }
                let processor = adapt functional (ConnectionStateAdapter(state)) action
                use pipeline = new ProcessorPipeline([| processor |], sink, pool)
                let operation = pipeline.PushAsync(input, 2, timeout.Token)
                try
                    do! started.Task.WaitAsync(timeout.Token)
                    operation.IsCompleted |> should equal false
                    input.Disposals |> should equal 0
                    pool.Owners[0].Disposals |> should equal 0
                    pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 3L
                    pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes
                    |> should equal 18L
                    // The writer expires when the handler returns, even while sink consumption waits.
                    let output = capturedOutput |> Option.get
                    Assert.Throws<InvalidOperationException>(fun () -> output.AppendInput(0L, 1L))
                    |> ignore
                finally
                    release.TrySetResult(()) |> ignore
                do! operation
                input.Disposals |> should equal 1
                pool.Owners[0].Disposals |> should equal 1
                pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
                pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes
                |> should equal 0L
        }

    [<Fact>]
    let ``stage factory isolates adapters and shares only the connection state view`` () =
        task {
            let seen = ResizeArray<IConnectionState>()
            let stage =
                ConfiguredScriptStage("typed-handle", fun ctx ->
                    seen.Add ctx.ConnectionState
                    ctx.ConnectionState.GetOrAdd("shared", fun () -> obj()) |> ignore
                    emitInput ctx 0L ctx.Input.Length
                    releaseAll)
            let definition = ScriptPipelineDefinition([| stage; stage |], [| stage |])
            use firstState = new ConnectionState()
            use secondState = new ConnectionState()
            let! first = definition.CreateProcessorPipelinesAsync(firstState).AsTask()
            let! second = definition.CreateProcessorPipelinesAsync(secondState).AsTask()
            let processors =
                Array.concat [| first.ClientToBackend; first.BackendToClient; second.ClientToBackend; second.BackendToClient |]
            for i in 0 .. processors.Length - 1 do
                for j in i + 1 .. processors.Length - 1 do
                    Assert.NotSame(processors[i], processors[j])
                use pipeline = new ProcessorPipeline([| processors[i] |], Sink(ignore), MemoryPool<byte>.Shared)
                do! pipeline.PushAsync(new Owner(bytes "A"), 1, CancellationToken.None)
            seen.Count |> should equal 6
            for i in 0 .. 2 do
                Assert.Same(seen[0], seen[i])
                Assert.Same(seen[3], seen[i + 3])
            Assert.NotSame(seen[0], seen[3])
            Assert.Same(firstState.TryGet<obj>("shared") |> Option.get, seen[0].TryGet<obj>("shared") |> Option.get)
            Assert.NotSame(firstState.TryGet<obj>("shared") |> Option.get, secondState.TryGet<obj>("shared") |> Option.get)
        }

    [<Fact>]
    let ``adapter returns a synchronously completed value task and forwards invocation objects`` () =
        let state = new ConnectionState()
        let publicState = ConnectionStateAdapter(state) :> IConnectionState
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()
        let input = ReadOnlySequence<byte>(bytes "AB")
        let mutable calls = 0
        let mutable appended = false
        let output =
            { new PipeL4.Core.IProcessorOutput with
                member _.AppendInput(offset, length) =
                    (offset, length) |> should equal (0L, 1L)
                    appended <- true
                member _.GetMemory(_) = failwith "Unexpected generated output"
                member _.GetSpan(_) = Span<byte>.Empty
                member _.Advance(_) = failwith "Unexpected generated output" }
        let processor =
            FunctionalProcessorAdapter(
                (fun ctx ->
                    calls <- calls + 1
                    ctx.Input.Start.Equals(input.Start) |> should equal true
                    ctx.Input.End.Equals(input.End) |> should equal true
                    ctx.IsCompleted |> should equal false
                    ctx.CancellationToken |> should equal cancellation.Token
                    Assert.Same(publicState, ctx.ConnectionState)
                    emitInput ctx 0L 1L
                    retainSuffix 1L),
                publicState
            ) :> PipeL4.Core.IByteStreamProcessor
        let result = processor.ProcessAsync({ Bytes = input; IsCompleted = false }, output, cancellation.Token)
        result.IsCompletedSuccessfully |> should equal true
        result.Result.RetainedLength |> should equal 1L
        calls |> should equal 1
        appended |> should equal true
        (state :> IAsyncDisposable).DisposeAsync().GetAwaiter().GetResult()
