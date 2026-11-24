namespace PipeL4.Tests

open System
open System.Buffers
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources
open FsUnit.Xunit
open PipeL4.Core
open PipeL4.PluginHost
open PipeL4.Plugins
open Xunit

module FunctionalProcessorAsyncTests =
    let private signal () = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let private bytes (value: string) = Encoding.ASCII.GetBytes value
    let private text (value: ReadOnlySequence<byte>) = Encoding.ASCII.GetString(value.ToArray())

    type private ResultSource() =
        let mutable source = ManualResetValueTaskSourceCore<PipeL4.Plugins.ProcessorResult>()
        let mutable consumptions = 0
        do source.RunContinuationsAsynchronously <- true
        member this.Operation = ValueTask<PipeL4.Plugins.ProcessorResult>(this, source.Version)
        member _.Complete() = source.SetResult(releaseAll)
        member _.Consumptions = consumptions
        interface IValueTaskSource<PipeL4.Plugins.ProcessorResult> with
            member _.GetResult(token) =
                consumptions <- consumptions + 1
                if consumptions <> 1 then failwith "ValueTask consumed more than once"
                source.GetResult(token)
            member _.GetStatus(token) = source.GetStatus(token)
            member _.OnCompleted(continuation, state, token, flags) = source.OnCompleted(continuation, state, token, flags)

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
        override _.Rent(size) =
            let owner = new Owner(Array.zeroCreate<byte> (max size 16))
            owners.Add owner
            owner :> IMemoryOwner<byte>
        override _.Dispose(_) = ()

    type private Sink(check: ReadOnlySequence<byte> -> unit) =
        let writes = ResizeArray<string>()
        let mutable completions = 0
        member _.Text = String.concat "" writes
        member _.Writes = writes.Count
        member _.Completions = completions
        interface IProcessorPipelineSink with
            member _.WriteAsync(value, _) =
                check value
                writes.Add(text value)
                ValueTask()
            member _.CompleteAsync(_) =
                completions <- completions + 1
                ValueTask()

    let private dispose (pipeline: ProcessorPipeline) =
        (pipeline :> IAsyncDisposable).DisposeAsync().AsTask()

    let private passthrough =
        ConfiguredScriptStage("sync", fun ctx ->
            emitInput ctx 0L ctx.Input.Length
            releaseAll)

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``value task sources are consumed exactly once with typed value state`` suspended =
        task {
            use connection = new ConnectionState()
            let source = ResultSource()
            if not suspended then source.Complete()
            let handler = FunctionalHandler<struct (int * int)>.Create(fun _ (struct (value, marker)) ->
                value |> should equal 42
                marker |> should equal 7
                source.Operation)
            let processor =
                new FunctionalProcessorAdapter<struct (int * int)>(
                    (fun _ -> struct (42, 7)), handler, None, ConnectionStateAdapter(connection))
                :> PipeL4.Core.IByteStreamProcessor
            let output =
                { new PipeL4.Core.IProcessorOutput with
                    member _.AppendInput(_, _) = failwith "Unexpected output"
                    member _.GetMemory(_) = failwith "Unexpected output"
                    member _.GetSpan(_) = Span<byte>.Empty
                    member _.Advance(_) = failwith "Unexpected output" }
            let operation = processor.ProcessAsync(
                                { Bytes = ReadOnlySequence<byte>.Empty; IsCompleted = false }, output, CancellationToken.None)
            if suspended then
                Assert.False(operation.IsCompleted)
                source.Consumptions |> should equal 0
                source.Complete()
            else Assert.True(operation.IsCompletedSuccessfully)
            let! result = operation
            result.RetainedLength |> should equal 0L
            source.Consumptions |> should equal 1
            let! failures = ProcessorLifetime.disposeAllAsync [| processor |]
            Assert.Empty(failures)
        }

    [<Theory>]
    [<InlineData("stateless-task", false)>]
    [<InlineData("stateless-task", true)>]
    [<InlineData("stateless-value-task", false)>]
    [<InlineData("stateless-value-task", true)>]
    [<InlineData("stateful-task", false)>]
    [<InlineData("stateful-task", true)>]
    [<InlineData("stateful-value-task", false)>]
    [<InlineData("stateful-value-task", true)>]
    [<InlineData("class", false)>]
    [<InlineData("class", true)>]
    let ``awaited handlers preserve segmented input state emission retention and EOF`` kind suspended =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use connection = new ConnectionState()
            use pool = new Pool()
            let local = ref 0
            let started = Array.init 3 (fun _ -> signal ())
            let resume = Array.init 3 (fun _ -> signal ())
            let outputs = ResizeArray<PipeL4.Plugins.IProcessorOutput>()
            let inputs = [| new Owner(bytes "AB"); new Owner(bytes "CD") |]
            let mutable calls = 0
            let mutable created = 0
            let mutable cleaned = 0
            let mutable registry = None
            let handle (ctx: ProcessContext) (state: int ref) =
                task {
                    Assert.Same(local, state)
                    state.Value |> should equal calls
                    state.Value <- state.Value + 1
                    let index = calls
                    calls <- calls + 1
                    ctx.CancellationToken |> should equal timeout.Token
                    match registry with
                    | Some previous -> Assert.Same(previous, ctx.ConnectionState)
                    | None -> registry <- Some ctx.ConnectionState
                    let expected = [| "AB"; "BCD"; "D" |][index]
                    ctx.IsCompleted |> should equal (index = 2)
                    text ctx.Input |> should equal expected
                    ctx.Input.IsSingleSegment |> should equal (index <> 1)
                    outputs.Add ctx.Output
                    emitInput ctx 0L 1L
                    // Uncommitted runtime-owned writable memory must survive suspension too.
                    let writable = ctx.Output.GetMemory(1)
                    started[index].SetResult(())
                    if suspended then do! resume[index].Task.WaitAsync(timeout.Token)
                    text ctx.Input |> should equal expected
                    writable.Span[0] <- byte '!'
                    ctx.Output.Advance(1)
                    emitInput ctx (ctx.Input.Length - 1L) 1L
                    return retainSuffix (if ctx.IsCompleted then 0L else 1L)
                }
            let create (_: CreateContext) =
                created <- created + 1
                local
            let cleanup = FunctionalDisposal<int ref>.Create(fun state ->
                Assert.Same(local, state)
                state.Value |> should equal 3
                cleaned <- cleaned + 1
                ValueTask())
            let stage =
                match kind with
                | "stateless-task" -> ConfiguredScriptStage("async", fun ctx -> handle ctx local)
                | "stateless-value-task" ->
                    ConfiguredScriptStage("async", fun ctx -> ValueTask<PipeL4.Plugins.ProcessorResult>(handle ctx local))
                | "stateful-task" ->
                    ConfiguredScriptStage.CreateFunctional("async", create,
                        FunctionalHandler<int ref>.Create(handle), dispose = cleanup)
                | "stateful-value-task" ->
                    ConfiguredScriptStage.CreateFunctional("async", create,
                        FunctionalHandler<int ref>.Create(fun ctx state -> ValueTask<PipeL4.Plugins.ProcessorResult>(handle ctx state)),
                        dispose = cleanup)
                | _ ->
                    // Replaced with the established class adapter below.
                    ConfiguredScriptStage("unused", fun _ -> releaseAll)
            let! processors = ScriptPipelineDefinition([| passthrough; stage; passthrough |], Array.empty)
                                  .CreateProcessorPipelinesAsync(connection).AsTask()
            let processors =
                if kind = "class" then
                    let view = ConnectionStateAdapter(connection) :> IConnectionState
                    let processor =
                        { new PipeL4.Plugins.IByteStreamProcessor with
                            member _.ProcessAsync(input, output, token) =
                                ValueTask<PipeL4.Plugins.ProcessorResult>(handle
                                    { Input = input.Bytes; IsCompleted = input.IsCompleted; Output = output
                                      CancellationToken = token; ConnectionState = view } local) }
                    let result = Array.copy processors.ClientToBackend
                    result[1] <- ScriptProcessorAdapter(processor)
                    result
                else processors.ClientToBackend
            let sink = Sink(fun sequence ->
                for segment in sequence do
                    let mutable backing = Unchecked.defaultof<ArraySegment<byte>>
                    Assert.True(MemoryMarshal.TryGetArray(segment, &backing))
                    let owner = Seq.append inputs pool.Owners |> Seq.find (fun owner -> obj.ReferenceEquals(owner.Buffer, backing.Array))
                    owner.Disposals |> should equal 0)
            use pipeline = new ProcessorPipeline(processors, sink, pool)
            let checkSuspended index (operation: Task) =
                Assert.False(operation.IsCompleted)
                sink.Writes |> should equal index
                // The previous retained suffix stays published until this result completes.
                let metrics = pipeline.GetStageMetricsSnapshot()[1]
                let retained = if index = 0 then 0L else 1L
                metrics.CurrentRetainedBytes |> should equal retained
                inputs[min index 1].Disposals |> should equal 0
            for index = 0 to 2 do
                let operation =
                    if index = 2 then pipeline.CompleteAsync timeout.Token
                    else pipeline.PushAsync(inputs[index], 2, timeout.Token)
                try
                    do! started[index].Task.WaitAsync(timeout.Token)
                    if suspended then checkSuspended index operation
                finally
                    resume[index].TrySetResult(()) |> ignore
                do! operation
                Assert.Throws<InvalidOperationException>(fun () -> outputs[index].AppendInput(0L, 1L)) |> ignore
            sink.Text |> should equal "A!BB!DD!D"
            sink.Completions |> should equal 1
            do! dispose pipeline
            let stateful = kind.StartsWith("stateful", StringComparison.Ordinal)
            created |> should equal (if stateful then 1 else 0)
            cleaned |> should equal (if stateful then 1 else 0)
            for owner in Seq.append inputs pool.Owners do owner.Disposals |> should equal 1
            pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
            pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            let metrics = pipeline.GetStageMetricsSnapshot()[1]
            metrics.InputBackedOutputBytes |> should equal 6L
            metrics.GeneratedOutputBytes |> should equal 3L
        }

    [<Theory>]
    [<InlineData("before-await")>]
    [<InlineData("after-await")>]
    [<InlineData("cancel-suspended")>]
    [<InlineData("cancel-staged")>]
    [<InlineData("invalid-retention")>]
    [<InlineData("uncommitted")>]
    let ``failed async invocations discard staged output and release all owners`` outcome =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use cancellation = new CancellationTokenSource()
            use connection = new ConnectionState()
            use pool = new Pool()
            let started, resume = signal (), signal ()
            let expected = InvalidOperationException("handler failure")
            let mutable output = None
            let mutable calls = 0
            let handle (ctx: ProcessContext) : Task<PipeL4.Plugins.ProcessorResult> =
                if outcome = "before-await" then raise expected
                task {
                    calls <- calls + 1
                    Assert.False(ctx.IsCompleted)
                    ctx.CancellationToken |> should equal cancellation.Token
                    output <- Some ctx.Output
                    emitInput ctx 0L ctx.Input.Length
                    emitBytes ctx (ReadOnlySpan<byte>(bytes "!"))
                    started.SetResult(())
                    if outcome = "cancel-suspended" then
                        do! resume.Task.WaitAsync(ctx.CancellationToken)
                    else
                        do! resume.Task.WaitAsync(timeout.Token)
                    match outcome with
                    | "after-await" -> return raise expected
                    | "cancel-staged" -> return releaseAll // Executor checks cancellation after await.
                    | "invalid-retention" -> return retainSuffix (ctx.Input.Length + 1L)
                    | "uncommitted" ->
                        ctx.Output.GetMemory(1) |> ignore
                        return releaseAll
                    | _ -> return releaseAll
                }
            let! processors = ScriptPipelineDefinition([| ConfiguredScriptStage("failure", handle) |], Array.empty)
                                  .CreateProcessorPipelinesAsync(connection).AsTask()
            let sink = Sink(ignore)
            let input = new Owner(bytes "AB")
            use pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, pool)
            let operation = pipeline.PushAsync(input, 2, cancellation.Token)
            try
                if outcome <> "before-await" then
                    do! started.Task.WaitAsync(timeout.Token)
                    sink.Writes |> should equal 0
                    input.Disposals |> should equal 0
                    pool.Owners[0].Disposals |> should equal 0
                    if outcome.StartsWith("cancel", StringComparison.Ordinal) then cancellation.Cancel()
            finally
                resume.TrySetResult(()) |> ignore
            let! error = Assert.ThrowsAnyAsync<Exception>(fun () -> operation)
            if outcome.StartsWith("cancel", StringComparison.Ordinal) then
                let cancelled = Assert.IsAssignableFrom<OperationCanceledException>(error)
                cancelled.CancellationToken |> should equal cancellation.Token
            else
                let context = Assert.IsType<ScriptProcessorFailureException>(error)
                if outcome.EndsWith("await", StringComparison.Ordinal) then Assert.Same(expected, context.InnerException)
                else Assert.IsType<InvalidOperationException>(context.InnerException) |> ignore
            do! dispose pipeline
            calls |> should equal (if outcome = "before-await" then 0 else 1)
            sink.Writes |> should equal 0
            sink.Completions |> should equal 0
            for owner in Seq.append [input] pool.Owners do owner.Disposals |> should equal 1
            pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
            pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            output |> Option.iter (fun writer ->
                Assert.Throws<InvalidOperationException>(fun () -> writer.GetMemory(1) |> ignore) |> ignore)
        }

    [<Fact>]
    let ``pre-cancelled invocation never enters async handler`` () =
        task {
            use connection = new ConnectionState()
            use cancellation = new CancellationTokenSource()
            cancellation.Cancel()
            let handle (_: ProcessContext) : Task<PipeL4.Plugins.ProcessorResult> = failwith "Must not execute"
            let! processors = ScriptPipelineDefinition([| ConfiguredScriptStage("cancel", handle) |], Array.empty)
                                  .CreateProcessorPipelinesAsync(connection).AsTask()
            let input = new Owner(bytes "A")
            let sink = Sink(ignore)
            use pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> pipeline.PushAsync(input, 1, cancellation.Token))
            do! dispose pipeline
            input.Disposals |> should equal 1
            sink.Completions |> should equal 0
        }

    [<Theory>]
    [<InlineData("task", false, false)>]
    [<InlineData("task", false, true)>]
    [<InlineData("task", true, false)>]
    [<InlineData("task", true, true)>]
    [<InlineData("value-task", false, false)>]
    [<InlineData("value-task", false, true)>]
    [<InlineData("value-task", true, false)>]
    [<InlineData("value-task", true, true)>]
    [<InlineData("value-task-unit", false, false)>]
    [<InlineData("value-task-unit", false, true)>]
    [<InlineData("value-task-unit", true, false)>]
    [<InlineData("value-task-unit", true, true)>]
    let ``explicit async cleanup is awaited once even without invocations`` kind invokeHandler suspended =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use connection = new ConnectionState()
            let started, resume = signal (), signal ()
            let local = ref 0
            let mutable creations = 0
            let mutable cleanups = 0
            let cleanup (state: int ref) =
                task {
                    Assert.Same(local, state)
                    cleanups <- cleanups + 1
                    started.SetResult(())
                    if suspended then do! resume.Task.WaitAsync(timeout.Token)
                    state.Value |> should equal (if invokeHandler then 1 else 0)
                }
            let disposer =
                match kind with
                | "task" -> FunctionalDisposal<int ref>.Create(cleanup)
                | "value-task" -> FunctionalDisposal<int ref>.Create(fun state -> ValueTask(cleanup state :> Task))
                | _ -> FunctionalDisposal<int ref>.Create(fun state -> ValueTask<unit>(cleanup state))
            let stage = ConfiguredScriptStage.CreateFunctional("cleanup",
                            (fun () -> creations <- creations + 1; local),
                            FunctionalHandler<int ref>.Create(fun _ state -> state.Value <- state.Value + 1; releaseAll),
                            dispose = disposer)
            let! processors = ScriptPipelineDefinition([| stage |], Array.empty).CreateProcessorPipelinesAsync(connection).AsTask()
            creations |> should equal 1
            use pipeline = new ProcessorPipeline(processors.ClientToBackend, Sink(ignore), MemoryPool<byte>.Shared)
            if invokeHandler then do! pipeline.CompleteAsync timeout.Token
            let operation = dispose pipeline
            try
                do! started.Task.WaitAsync(timeout.Token)
                if suspended then Assert.False(operation.IsCompleted)
            finally
                resume.TrySetResult(()) |> ignore
            do! operation
            do! dispose pipeline
            cleanups |> should equal 1
        }

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``async rollback awaits reverse cleanup and aggregates each failure after construction`` fails =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use connection = new ConnectionState()
            let started, resume = signal (), signal ()
            let events = ResizeArray<int>()
            let construction = InvalidOperationException("construction")
            let failures = [| InvalidOperationException("first cleanup"); InvalidOperationException("second cleanup") |]
            let stage index = ConfiguredScriptStage.CreateFunctional(string index, (fun () -> index),
                                FunctionalHandler<int>.Create(fun _ _ -> ValueTask<PipeL4.Plugins.ProcessorResult>(releaseAll)),
                                dispose = FunctionalDisposal<int>.Create(fun state -> task {
                                    state |> should equal index
                                    events.Add index
                                    if index = 1 then
                                        started.SetResult(())
                                        do! resume.Task.WaitAsync(timeout.Token)
                                    if fails then raise failures[index]
                                }))
            let broken = ConfiguredScriptStage.CreateFunctional("broken",
                            (fun () -> raise construction : int), (fun _ _ -> releaseAll))
            let operation = ScriptPipelineDefinition([| stage 0; stage 1 |], [| broken |])
                                .CreateProcessorPipelinesAsync(connection).AsTask()
            try
                do! started.Task.WaitAsync(timeout.Token)
                Assert.False(operation.IsCompleted)
                events.ToArray() |> should equal [| 1 |]
                connection.GetOrAdd("alive", fun () -> 42) |> should equal 42
            finally
                resume.TrySetResult(()) |> ignore
            let! error = Assert.ThrowsAnyAsync<Exception>(fun () -> operation :> Task)
            let errors =
                if fails then Assert.IsType<AggregateException>(error).InnerExceptions |> Seq.toArray
                else [| error |]
            let contextual = Assert.IsType<ScriptProcessorFailureException>(errors[0])
            Assert.Same(construction, contextual.InnerException)
            if fails then
                errors.Length |> should equal 3
                for index = 1 to 2 do
                    let contextual = Assert.IsType<ScriptProcessorFailureException>(errors[index])
                    Assert.Same(failures[2 - index], contextual.InnerException)
            events.ToArray() |> should equal [| 1; 0 |]
        }

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``suspended functional output preserves bounded admission and downstream backpressure`` exhaust =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use connection = new ConnectionState()
            use pool = new Pool()
            use budget = new CapacityBudget(if exhaust then 3L else 5L)
            let accounting = new ConnectionMemoryAccounting()
            let handlerStarted, resumeHandler = signal (), signal ()
            let sinkStarted, resumeSink = signal (), signal ()
            let input = new Owner(bytes "AB")
            let mutable captured = None
            let mutable sinkCalls = 0
            let handler (ctx: ProcessContext) = task {
                captured <- Some ctx.Output
                emitBytes ctx (ReadOnlySpan<byte>(bytes "!"))
                handlerStarted.SetResult(())
                do! resumeHandler.Task.WaitAsync(timeout.Token)
                emitInput ctx 0L ctx.Input.Length
                return releaseAll
            }
            let sink =
                { new IProcessorPipelineSink with
                    member _.WriteAsync(value, token) =
                        sinkCalls <- sinkCalls + 1
                        text value |> should equal "!AB"
                        sinkStarted.SetResult(())
                        ValueTask(resumeSink.Task.WaitAsync(token))
                    member _.CompleteAsync(_) = ValueTask() }
            let processor =
                FunctionalProcessorAdapter(handler, ConnectionStateAdapter(connection))
                :> PipeL4.Core.IByteStreamProcessor
            use pipeline = new ProcessorPipeline([| processor |], sink, pool, accounting, budget)
            let operation = pipeline.PushAsync(input, 2, timeout.Token)
            try
                do! handlerStarted.Task.WaitAsync(timeout.Token)
                Assert.False(operation.IsCompleted)
                sinkCalls |> should equal 0
                budget.GetSnapshot().CurrentBytes |> should equal 3L
                accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 18L
                input.Disposals |> should equal 0
                pool.Owners[0].Disposals |> should equal 0
                resumeHandler.SetResult(())
                if exhaust then
                    let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> operation)
                    sinkCalls |> should equal 0
                else
                    do! sinkStarted.Task.WaitAsync(timeout.Token)
                    Assert.False(operation.IsCompleted)
                    budget.GetSnapshot().CurrentBytes |> should equal 3L
                    input.Disposals |> should equal 0
                    pool.Owners[0].Disposals |> should equal 0
                    // Invocation is complete; the push is still held by the awaited sink.
                    Assert.Throws<InvalidOperationException>(fun () -> captured.Value.Advance(0)) |> ignore
                    resumeSink.SetResult(())
                    do! operation
            finally
                resumeHandler.TrySetResult(()) |> ignore
                resumeSink.TrySetResult(()) |> ignore
            do! dispose pipeline
            input.Disposals |> should equal 1
            pool.Owners[0].Disposals |> should equal 1
            budget.GetSnapshot().CurrentBytes |> should equal 0L
            accounting.GetSnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Theory>]
    [<InlineData("task-before")>]
    [<InlineData("task-after")>]
    [<InlineData("value-task")>]
    [<InlineData("value-task-unit")>]
    let ``async cleanup failures retain original exception identity and stage context`` kind =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use connection = new ConnectionState()
            let expected = InvalidOperationException("cleanup")
            let started, resume = signal (), signal ()
            let mutable calls = 0
            let cleanup (_: int) : Task<unit> =
                calls <- calls + 1
                if kind = "task-before" then raise expected
                task {
                    started.SetResult(())
                    do! resume.Task.WaitAsync(timeout.Token)
                    return raise expected
                }
            let disposer =
                match kind with
                | "value-task" -> FunctionalDisposal<int>.Create(fun state -> ValueTask(cleanup state :> Task))
                | "value-task-unit" -> FunctionalDisposal<int>.Create(fun state -> ValueTask<unit>(cleanup state))
                | _ -> FunctionalDisposal<int>.Create(cleanup)
            let stage = ConfiguredScriptStage.CreateFunctional("cleanup-failure", (fun () -> 42),
                            FunctionalHandler<int>.Create(fun _ state ->
                                state |> should equal 42
                                ValueTask<PipeL4.Plugins.ProcessorResult>(releaseAll)), dispose = disposer)
            let! processors = ScriptPipelineDefinition(Array.empty, [| stage |]).CreateProcessorPipelinesAsync(connection).AsTask()
            use pipeline = new ProcessorPipeline(processors.BackendToClient, Sink(ignore), MemoryPool<byte>.Shared)
            let operation = dispose pipeline
            try
                if kind <> "task-before" then
                    do! started.Task.WaitAsync(timeout.Token)
                    Assert.False(operation.IsCompleted)
            finally
                resume.TrySetResult(()) |> ignore
            let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> operation)
            Assert.Same(expected, error.InnerException)
            error.ScriptPath |> should equal "cleanup-failure"
            error.Direction |> should equal "server-to-client"
            error.StageNumber |> should equal 1
            do! dispose pipeline
            calls |> should equal 1
        }

    [<Theory>]
    [<InlineData("normal")>]
    [<InlineData("failure")>]
    [<InlineData("cancellation")>]
    let ``terminal processing precedes reverse async cleanup with multiple failures`` outcome =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use cancellation = new CancellationTokenSource()
            use connection = new ConnectionState()
            let active, resumeHandler = signal (), signal ()
            let cleaning, resumeCleanup = signal (), signal ()
            let handlerFailure = InvalidOperationException("handle")
            let cleanupFailures = [| InvalidOperationException("cleanup 0"); InvalidOperationException("cleanup 1") |]
            let mutable handlerFinished = false
            let events = ResizeArray<int>()
            let stage index = ConfiguredScriptStage.CreateFunctional(string index, (fun () -> index),
                FunctionalHandler<int>.Create(fun ctx state -> task {
                    state |> should equal index
                    if index = 0 then
                        active.SetResult(())
                        // Deliberately cooperative completion after cancellation; memory remains live.
                        do! resumeHandler.Task.WaitAsync(timeout.Token)
                        handlerFinished <- true
                        if outcome = "failure" then raise handlerFailure
                    emitInput ctx 0L ctx.Input.Length
                    return releaseAll
                }),
                dispose = FunctionalDisposal<int>.Create(fun state -> task {
                    Assert.True(handlerFinished)
                    events.Add state
                    if index = 1 then
                        cleaning.SetResult(())
                        do! resumeCleanup.Task.WaitAsync(timeout.Token)
                    raise cleanupFailures[index]
                }))
            let! processors = ScriptPipelineDefinition([| stage 0; stage 1 |], Array.empty)
                                  .CreateProcessorPipelinesAsync(connection).AsTask()
            let sink = Sink(ignore)
            let input = new Owner(bytes "AB")
            use pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
            let operation = pipeline.PushAsync(input, 2, cancellation.Token)
            try
                do! active.Task.WaitAsync(timeout.Token)
                if outcome = "cancellation" then cancellation.Cancel()
                // Core rejects concurrent lifecycle calls; it must not start cleanup of an active stage.
                let! _ = Assert.ThrowsAsync<InvalidOperationException>(fun () -> dispose pipeline)
                Assert.Empty(events)
                input.Disposals |> should equal 0
            finally
                resumeHandler.TrySetResult(()) |> ignore
            match outcome with
            | "failure" ->
                let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> operation)
                Assert.Same(handlerFailure, error.InnerException)
            | "cancellation" ->
                let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)
                error.CancellationToken |> should equal cancellation.Token
            | _ -> do! operation
            let cleanup = dispose pipeline
            try
                do! cleaning.Task.WaitAsync(timeout.Token)
                Assert.False(cleanup.IsCompleted)
                events.ToArray() |> should equal [| 1 |]
            finally
                resumeCleanup.TrySetResult(()) |> ignore
            let! aggregate = Assert.ThrowsAsync<AggregateException>(fun () -> cleanup)
            aggregate.InnerExceptions.Count |> should equal 2
            for index = 0 to 1 do
                let error = Assert.IsType<ScriptProcessorFailureException>(aggregate.InnerExceptions[index])
                Assert.Same(cleanupFailures[1 - index], error.InnerException)
                error.StageNumber |> should equal (2 - index)
            events.ToArray() |> should equal [| 1; 0 |]
            do! dispose pipeline
            input.Disposals |> should equal 1
            if outcome <> "normal" then sink.Completions |> should equal 0
        }
