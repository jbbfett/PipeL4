namespace PipeL4.Tests

open System
open System.Buffers
open System.Collections.Concurrent
open System.IO
open System.Reflection
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Microsoft.FSharp.Reflection
open PipeL4.Core
open PipeL4.PluginHost
open PipeL4.Plugins
open Xunit

module FunctionalScriptTests =
    type private Workspace() =
        let root = Path.Combine(Path.GetTempPath(), "PipeL4.Tests", Guid.NewGuid().ToString("N"))
        let compiler = ScriptCompiler(Path.Combine(root, "compiler"))
        do Directory.CreateDirectory(root) |> ignore
        member _.Compiler = compiler
        member _.Write(name: string, source: string) =
            let path = Path.Combine(root, name + ".fsx")
            File.WriteAllText(path, source)
            path
        interface IDisposable with
            member _.Dispose() = Directory.Delete(root, true)

    type private Owner(data: byte array) =
        member _.Data = data
        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(data)
            member _.Dispose() = data.AsSpan().Fill(221uy)

    type private Sink(?inspect: ReadOnlySequence<byte> -> unit) =
        let output = ResizeArray<byte>()
        member _.Text = Encoding.ASCII.GetString(output.ToArray())
        interface IProcessorPipelineSink with
            member _.WriteAsync(data, _) =
                inspect |> Option.iter (fun inspect -> inspect data)
                for segment in data do output.AddRange(segment.ToArray())
                ValueTask()
            member _.CompleteAsync(_) = ValueTask()

    let private prelude = "open System\nopen System.Threading.Tasks\nopen PipeL4.Plugins\n"
    let private bytes (value: string) = Encoding.ASCII.GetBytes(value)
    let private signal () = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let private diagnosticsText diagnostics = diagnostics |> Array.map _.Message |> String.concat "\n"
    let private compiled result =
        match result with
        | Compiled(plugin, diagnostics) ->
            diagnostics |> Array.exists (fun d -> d.CompilerCode = Some "FS0046") |> should equal false
            plugin
        | Failed diagnostics -> failwith (diagnosticsText diagnostics)
    let private failed result =
        match result with
        | Failed diagnostics -> diagnostics
        | Compiled _ -> failwith "Expected script validation to fail"
    let private compile (workspace: Workspace) name source =
        task {
            let path = workspace.Write(name, source)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            return path, compiled result
        }
    let private publicState state = ConnectionStateAdapter(state) :> IConnectionState
    let private push (pipeline: ProcessorPipeline) value =
        let data = bytes value
        pipeline.PushAsync(new Owner(data), data.Length, CancellationToken.None)
    let private required value = value |> Option.ofObj |> Option.defaultWith (fun () -> failwith "Missing reflection metadata or value")
    let private property<'T> (plugin: CompiledScriptProcessor) name =
        let info = plugin.ProcessorType.GetProperty(name, BindingFlags.Public ||| BindingFlags.Static) |> required
        info.GetValue(null) |> required :?> 'T
    let private load (workspace: Workspace) client server =
        ScriptPipelineLoader.loadWithCompilerAsync
            (fun path token -> workspace.Compiler.CompileAndLoadAsync(path, token)) client server CancellationToken.None

    [<Theory>]
    [<InlineData("sync")>]
    [<InlineData("task")>]
    [<InlineData("value-task")>]
    let ``real stateless scripts bind and run without module name assumptions`` resultForm =
        task {
            use workspace = new Workspace()
            let body = "emitInput ctx 0L ctx.Input.Length; releaseAll"
            let source =
                match resultForm with
                | "task" -> "let handle ctx = task {\n    do! Task.Yield()\n    return (" + body + ")\n}"
                | "value-task" -> "let handle ctx = ValueTask<ProcessorResult>(" + body + ")"
                | _ -> "let handle ctx = " + body
            let! _, plugin = compile workspace "odd-script-name" (prelude + source)
            FSharpType.IsModule(plugin.ProcessorType) |> should equal true
            plugin.ProcessorType.IsNested |> should equal false
            let method = plugin.ProcessorType.GetMethod("handle") |> required
            method.GetParameters() |> Array.map _.ParameterType |> should equal [| typeof<ProcessContext> |]
            use state = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState state) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "AB"
            do! push pipeline "CDEF"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "ABCDEF"
            let metrics = pipeline.GetStageMetricsSnapshot() |> List.exactlyOne
            metrics.GeneratedOutputBytes |> should equal 0L
            pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Theory>]
    [<InlineData("generated", "XYZXYZ")>]
    [<InlineData("mixed", "A!BABC!FCDEF")>]
    [<InlineData("suffix", "ABCDEF")>]
    [<InlineData("eof", "ABCDEF")>]
    [<InlineData("segmented", "AB!EFAB")>]
    let ``real scripts preserve emission retention EOF and borrowed backing`` scenario expected =
        task {
            use workspace = new Workspace()
            let body =
                match scenario with
                | "generated" -> "if not ctx.Input.IsEmpty then emitBytes ctx (ReadOnlySpan<byte>([| 88uy; 89uy; 90uy |]))\n    releaseAll"
                | "mixed" -> "if not ctx.Input.IsEmpty then\n        emitInput ctx 0L 1L\n        emitBytes ctx (ReadOnlySpan<byte>([| 33uy |]))\n        emitInput ctx (ctx.Input.Length - 1L) 1L\n        emitInput ctx 0L ctx.Input.Length\n    releaseAll"
                | "suffix" -> "let retained = if ctx.IsCompleted then 0L else 1L\n    emitInput ctx 0L (ctx.Input.Length - retained)\n    retainSuffix retained"
                | "eof" -> "if not ctx.IsCompleted then retainSuffix ctx.Input.Length\n    else\n        emitInput ctx 0L ctx.Input.Length\n        releaseAll"
                | _ -> "if not ctx.IsCompleted && ctx.Input.Length < 6L then retainSuffix ctx.Input.Length\n    else\n        if not ctx.Input.IsEmpty then\n            emitInput ctx 0L 2L\n            emitBytes ctx (ReadOnlySpan<byte>([| 33uy |]))\n            emitInput ctx 4L 2L\n            emitInput ctx 0L 2L\n        releaseAll"
            let! _, plugin = compile workspace "Emissions" (prelude + "let handle (ctx: ProcessContext) =\n    " + body)
            use state = new ConnectionState()
            let first, second = new Owner(bytes "AB"), new Owner(bytes "CDEF")
            let mutable borrowedSegments = 0
            let sink = Sink(fun sequence ->
                for segment in sequence do
                    let mutable array = Unchecked.defaultof<ArraySegment<byte>>
                    if MemoryMarshal.TryGetArray(segment, &array)
                       && (obj.ReferenceEquals(array.Array, first.Data) || obj.ReferenceEquals(array.Array, second.Data)) then
                        borrowedSegments <- borrowedSegments + 1)
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState state) |], sink, MemoryPool<byte>.Shared)
            do! pipeline.PushAsync(first, 2, CancellationToken.None)
            do! pipeline.PushAsync(second, 4, CancellationToken.None)
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal expected
            if scenario <> "generated" then borrowedSegments |> should be (greaterThan 0)
            let metrics = pipeline.GetStageMetricsSnapshot() |> List.exactlyOne
            let generated = match scenario with "generated" -> 6L | "mixed" -> 2L | "segmented" -> 1L | _ -> 0L
            metrics.GeneratedOutputBytes |> should equal generated
            pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
            pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Theory>]
    [<InlineData("missing", "let process (ctx: ProcessContext) = releaseAll", "handle")>]
    [<InlineData("zero", "let handle () = releaseAll", "handle")>]
    [<InlineData("three", "let handle (ctx: ProcessContext) (a: int) (b: int) = releaseAll", "handle")>]
    [<InlineData("result", "let handle (ctx: ProcessContext) = 42", "handle")>]
    [<InlineData("no-create", "let handle (ctx: ProcessContext) (state: int ref) = releaseAll", "handle")>]
    [<InlineData("stateless-create", "let create () = ref 0\nlet handle (ctx: ProcessContext) = releaseAll", "create")>]
    [<InlineData("state-mismatch", "type StateA = A\ntype StateB = B\nlet create () = A\nlet handle (ctx: ProcessContext) (state: StateB) = releaseAll", "create")>]
    [<InlineData("dispose-alone", "let handle (ctx: ProcessContext) = releaseAll\nlet dispose (state: int) = ()", "dispose")>]
    [<InlineData("dispose-state", "let create () = ref 0\nlet handle (ctx: ProcessContext) (state: int ref) = releaseAll\nlet dispose (state: int) = ()", "dispose")>]
    [<InlineData("dispose-result", "let create () = ref 0\nlet handle (ctx: ProcessContext) (state: int ref) = releaseAll\nlet dispose (state: int ref) = 42", "dispose")>]
    [<InlineData("dispose-task", "let create () = ref 0\nlet handle (ctx: ProcessContext) (state: int ref) = releaseAll\nlet dispose (state: int ref) = Task.CompletedTask", "dispose")>]
    [<InlineData("async-create", "let create () = Task.FromResult(0)\nlet handle (ctx: ProcessContext) (state: Task<int>) = releaseAll", "create")>]
    [<InlineData("vt-create", "let create () = ValueTask<int>(0)\nlet handle (ctx: ProcessContext) (state: ValueTask<int>) = releaseAll", "create")>]
    [<InlineData("fsharp-async-create", "let create () = async { return 0 }\nlet handle (ctx: ProcessContext) (state: Async<int>) = releaseAll", "create")>]
    [<InlineData("byref-like-state", "let create () = Span<byte>.Empty\nlet handle (ctx: ProcessContext) (state: Span<byte>) = releaseAll", "handle")>]
    [<InlineData("dispose-arguments", "let create () = 0\nlet handle (ctx: ProcessContext) (state: int) = releaseAll\nlet dispose (state: int) (extra: int) = ()", "dispose")>]
    [<InlineData("context-type", "let handle (ctx: string) = releaseAll", "handle")>]
    [<InlineData("create-argument", "let create (value: int) = value\nlet handle (ctx: ProcessContext) (state: int) = releaseAll", "create")>]
    [<InlineData("tupled", "let create () = 0\nlet handle (ctx: ProcessContext, state: int) = releaseAll", "handle")>]
    [<InlineData("generic", "let handle (ctx: ProcessContext) (state: 'T) = releaseAll", "handle")>]
    [<InlineData("value", "let handle = 42", "handle")>]
    [<InlineData("create-value", "let create = 42\nlet handle (ctx: ProcessContext) (state: int) = releaseAll", "create")>]
    let ``invalid functional signatures fail statically with contextual diagnostics`` name source (functionName: string) =
        task {
            use workspace = new Workspace()
            let path = workspace.Write(name, prelude + source)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let diagnostics = failed result
            let error = diagnostics |> Array.find (fun d -> d.Severity = ScriptDiagnosticSeverity.Error)
            error.SourcePath |> should equal (Some path)
            error.CompilerCode |> should equal None
            error.Message.Contains(functionName, StringComparison.Ordinal) |> should equal true
            error.Message.Contains("expected", StringComparison.Ordinal) |> should equal true
            if name <> "missing" then error.Message.Contains("actual", StringComparison.Ordinal) |> should equal true
        }

    [<Fact>]
    let ``ambiguous root modules fail deterministically while nested helpers are ignored`` () =
        task {
            use workspace = new Workspace()
            let ambiguous = "namespace Ambiguous\nmodule First =\n    open PipeL4.Plugins\n    let handle (ctx: ProcessContext) = releaseAll\nmodule Second =\n    open PipeL4.Plugins\n    let handle (ctx: ProcessContext) = releaseAll"
            let path = workspace.Write("Ambiguous", ambiguous)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let message = failed result |> diagnosticsText
            message.Contains("Ambiguous functional 'handle'") |> should equal true
            message.Contains("First") |> should equal true
            message.Contains("Second") |> should equal true
            let source = prelude + "type IHelper =\n    inherit IByteStreamProcessor\nmodule Helper =\n    let handle () = 42\nlet handle ctx = emitInput ctx 0L ctx.Input.Length; releaseAll"
            let! _, _ = compile workspace "NestedHelper" source
            let path = workspace.Write("OnlyNested", prelude + "module Helper =\n    let handle ctx = emitInput ctx 0L ctx.Input.Length; releaseAll")
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            (failed result |> diagnosticsText).Contains("missing top-level") |> should equal true
        }

    [<Theory>]
    [<InlineData("unit")>]
    [<InlineData("context")>]
    let ``typed local states are created once per stage and connection with shared connection view`` creation =
        task {
            use workspace = new Workspace()
            let creationSource =
                if creation = "unit" then
                    "let create () =\n    let state = { Count = ref 0; Shared = None }\n"
                else
                    "let create (ctx: CreateContext) =\n    let state = { Count = ref 0; Shared = Some ctx.ConnectionState }\n"
            let declarations =
                "type State = { Count: int ref; Shared: IConnectionState option }\n" +
                "let created = System.Collections.Concurrent.ConcurrentQueue<State>()\n" +
                "let observed = System.Collections.Concurrent.ConcurrentQueue<State * IConnectionState>()\n"
            let source = prelude + declarations + creationSource + """    created.Enqueue state
    state
let handle ctx (state: State) =
    match state.Shared with
    | Some shared when not (obj.ReferenceEquals(shared, ctx.ConnectionState)) -> failwith "wrong connection view"
    | _ -> ()
    state.Count.Value <- state.Count.Value + 1
    observed.Enqueue(state, ctx.ConnectionState)
    emitInput ctx 0L ctx.Input.Length
    releaseAll
"""
            let! path, plugin = compile workspace "TypedState" source
            let created = property<obj> plugin "created"
            // Reading the queue initializes the module, but neither compilation nor discovery calls create.
            (created.GetType().GetProperty("Count") |> required).GetValue(created) |> should equal (box 0)
            let mutable compilations = 0
            let! definition = ScriptPipelineLoader.loadWithCompilerAsync
                                (fun _ _ -> compilations <- compilations + 1; Task.FromResult(Compiled(plugin, [||])))
                                [| path; Path.Combine(Path.GetDirectoryName(path) |> required, ".", Path.GetFileName(path) |> required) |]
                                [| path |] CancellationToken.None
            compilations |> should equal 1
            for _ in 1 .. 2 do
                use state = new ConnectionState()
                let! pipelines = definition.CreateProcessorPipelinesAsync(state)
                for processors in [ pipelines.ClientToBackend; pipelines.BackendToClient ] do
                    use pipeline = new ProcessorPipeline(processors, Sink(), MemoryPool<byte>.Shared)
                    do! push pipeline "A"
                    do! push pipeline "B"
                    do! pipeline.CompleteAsync(CancellationToken.None)
            let observed = property<obj> plugin "observed"
            let entries = (observed.GetType().GetMethod("ToArray") |> required).Invoke(observed, [||]) |> required :?> Array
            entries.Length |> should equal 18
            let field (index: int) = FSharpValue.GetTupleFields(entries.GetValue(index) |> required)
            let states = (created.GetType().GetMethod("ToArray") |> required).Invoke(created, [||]) |> required :?> Array
            for stage in 0 .. 5 do
                let local = states.GetValue(stage) |> required
                let invocations =
                    [| for index in 0 .. entries.Length - 1 do
                           let entry = field index
                           if obj.ReferenceEquals(local, entry[0]) then yield entry |]
                invocations.Length |> should equal 3
                for invocation in invocations do Assert.Same(invocations[0][1], invocation[1])
                for previous in 0 .. stage - 1 do Assert.NotSame(local, states.GetValue(previous))
                let count = ((FSharpValue.GetRecordFields(local))[0] |> required) :?> int ref
                count.Value |> should equal 3
            Assert.Same((field 0)[1], (field 6)[1])
            Assert.NotSame((field 0)[1], (field 9)[1])
            (created.GetType().GetProperty("Count") |> required).GetValue(created) |> should equal (box 6)
        }

    [<Theory>]
    [<InlineData("sync", "sync")>]
    [<InlineData("task", "task")>]
    [<InlineData("value-task", "value-task")>]
    [<InlineData("sync", "value-task-unit")>]
    [<InlineData("task", "none")>]
    let ``closed generic state handlers and explicit disposal use existing typed adapters`` resultForm disposalForm =
        task {
            use workspace = new Workspace()
            let handleBody = "state.Add(1); emitInput ctx 0L ctx.Input.Length; releaseAll"
            let handle =
                match resultForm with
                | "task" -> "task {\n        do! Task.Yield()\n        return (" + handleBody + ")\n    }"
                | "value-task" -> "ValueTask<ProcessorResult>(task {\n        do! Task.Yield()\n        return (" + handleBody + ")\n    })"
                | _ -> handleBody
            let dispose =
                match disposalForm with
                | "task" -> "let dispose (state: ResizeArray<int>) = task {\n    do! Task.Yield()\n    disposed.Enqueue(state.Count)\n}"
                | "value-task" -> "let dispose (state: ResizeArray<int>) = ValueTask(task {\n    do! Task.Yield()\n    disposed.Enqueue(state.Count)\n} :> Task)"
                | "value-task-unit" -> "let dispose (state: ResizeArray<int>) = ValueTask<unit>(task {\n    do! Task.Yield()\n    disposed.Enqueue(state.Count)\n})"
                | "none" -> ""
                | _ -> "let dispose (state: ResizeArray<int>) = disposed.Enqueue(state.Count)"
            let source = prelude + "let disposed = System.Collections.Concurrent.ConcurrentQueue<int>()\nlet create () = ResizeArray<int>()\nlet handle ctx (state: ResizeArray<int>) =\n    " + handle + "\n" + dispose
            let! _, plugin = compile workspace "GenericState" source
            let method = plugin.ProcessorType.GetMethod("handle") |> required
            (method.GetCustomAttribute<CompilationArgumentCountsAttribute>() |> required).Counts |> Seq.toArray |> should equal [| 1; 1 |]
            use connection = new ConnectionState()
            let pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], Sink(), MemoryPool<byte>.Shared)
            do! push pipeline "A"
            do! push pipeline "B"
            do! pipeline.CompleteAsync(CancellationToken.None)
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            let disposed = property<ConcurrentQueue<int>> plugin "disposed"
            disposed.ToArray() |> should equal (if disposalForm = "none" then [||] else [| 3 |])
        }

    [<Fact>]
    let ``local state implementing IDisposable is not implicitly disposed`` () =
        task {
            use workspace = new Workspace()
            let source = prelude + """
let disposals = ref 0
type State() =
    interface IDisposable with
        member _.Dispose() = disposals.Value <- disposals.Value + 1
let create () = new State()
let handle ctx (state: State) = emitInput ctx 0L ctx.Input.Length; releaseAll
"""
            let! _, plugin = compile workspace "NoImplicitDisposal" source
            use connection = new ConnectionState()
            let processor = plugin.CreateProcessor(publicState connection)
            let! failures = ProcessorLifetime.disposeAllAsync [| processor |]
            failures |> should be Empty
            (property<int ref> plugin "disposals").Value |> should equal 0
        }

    [<Fact>]
    let ``script compilation errors remain compiler diagnostics`` () =
        task {
            use workspace = new Workspace()
            let path = workspace.Write("CompilerError", prelude + "let handle ctx =\n    let x: int = \"wrong\"\n    releaseAll")
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            failed result |> Array.exists (fun d -> d.Severity = ScriptDiagnosticSeverity.Error && d.CompilerCode.IsSome && d.Range.IsSome) |> should equal true
        }

    [<Theory>]
    [<InlineData("sync")>]
    [<InlineData("task")>]
    [<InlineData("value-task")>]
    let ``real handler failures keep context and explicit cleanup`` resultForm =
        task {
            use workspace = new Workspace()
            let body =
                match resultForm with
                | "sync" -> "invalidOp \"handler failed\""
                | "task" -> "task {\n        do! Task.Yield()\n        return invalidOp \"handler failed\"\n    }"
                | _ -> "ValueTask<ProcessorResult>(task {\n        do! Task.Yield()\n        return invalidOp \"handler failed\"\n    })"
            let source = prelude + "let cleaned = ref false\nlet create () = ref 0\nlet handle (ctx: ProcessContext) (state: int ref): " +
                         (match resultForm with "sync" -> "ProcessorResult" | "task" -> "Task<ProcessorResult>" | _ -> "ValueTask<ProcessorResult>") +
                         " =\n    " + body + "\nlet dispose (state: int ref) = cleaned.Value <- true"
            let! path, plugin = compile workspace "HandlerFailure" source
            let! definition = ScriptPipelineLoader.loadWithCompilerAsync
                                (fun _ _ -> Task.FromResult(Compiled(plugin, [||]))) [| path |] [||] CancellationToken.None
            use connection = new ConnectionState()
            let! processors = definition.CreateProcessorPipelinesAsync(connection)
            let pipeline = new ProcessorPipeline(processors.ClientToBackend, Sink(), MemoryPool<byte>.Shared)
            let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> push pipeline "A")
            error.ScriptPath |> should equal path
            error.Direction |> should equal "client-to-server"
            error.StageNumber |> should equal 1
            (error.InnerException |> required).Message |> should equal "handler failed"
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            (property<bool ref> plugin "cleaned").Value |> should equal true
            pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
            pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
        }

    [<Theory>]
    [<InlineData("sync")>]
    [<InlineData("task")>]
    [<InlineData("value-task")>]
    [<InlineData("value-task-unit")>]
    let ``real disposal failures are awaited contextualized and do not skip other stages`` resultForm =
        task {
            use workspace = new Workspace()
            let cleanup = "events.Enqueue(state); if state = 2 then invalidOp \"cleanup failed\""
            let dispose =
                match resultForm with
                | "sync" -> cleanup
                | "task" -> "task {\n        do! Task.Yield()\n        " + cleanup + "\n    }"
                | "value-task" -> "ValueTask(task {\n        do! Task.Yield()\n        " + cleanup + "\n    } :> Task)"
                | _ -> "ValueTask<unit>(task {\n        do! Task.Yield()\n        " + cleanup + "\n    })"
            let source = prelude + "let next = ref 0\nlet events = System.Collections.Concurrent.ConcurrentQueue<int>()\nlet create () = next.Value <- next.Value + 1; next.Value\nlet handle (ctx: ProcessContext) (state: int) = releaseAll\nlet dispose (state: int) =\n    " + dispose
            let! path, plugin = compile workspace "DisposalFailure" source
            let! definition = ScriptPipelineLoader.loadWithCompilerAsync
                                (fun _ _ -> Task.FromResult(Compiled(plugin, [||]))) [| path; path |] [||] CancellationToken.None
            use connection = new ConnectionState()
            let! processors = definition.CreateProcessorPipelinesAsync(connection)
            let pipeline = new ProcessorPipeline(processors.ClientToBackend, Sink(), MemoryPool<byte>.Shared)
            let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())
            error.StageNumber |> should equal 2
            (error.InnerException |> required).Message |> should equal "cleanup failed"
            (property<ConcurrentQueue<int>> plugin "events").ToArray() |> should equal [| 2; 1 |]
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            (property<ConcurrentQueue<int>> plugin "events").Count |> should equal 2
        }

    [<Fact>]
    let ``creation exceptions happen at construction and rollback awaits explicit cleanup`` () =
        task {
            use workspace = new Workspace()
            let source = prelude + """
let next = ref 0
let events = System.Collections.Concurrent.ConcurrentQueue<int>()
let create () =
    next.Value <- next.Value + 1
    if next.Value = 3 then invalidOp "create failed"
    next.Value
let handle (ctx: ProcessContext) (state: int) = releaseAll
let dispose (state: int) = task {
    do! Task.Yield()
    events.Enqueue(state)
    if state = 2 then invalidOp "rollback cleanup failed"
}
"""
            let! path, plugin = compile workspace "Rollback" source
            (property<int ref> plugin "next").Value |> should equal 0
            let! definition = ScriptPipelineLoader.loadWithCompilerAsync
                                (fun _ _ -> Task.FromResult(Compiled(plugin, [||]))) [| path; path |] [| path |] CancellationToken.None
            use connection = new ConnectionState()
            let! error = Assert.ThrowsAsync<AggregateException>(fun () -> definition.CreateProcessorPipelinesAsync(connection).AsTask() :> Task)
            error.InnerExceptions.Count |> should equal 2
            let construction = Assert.IsType<ScriptProcessorFailureException>(error.InnerExceptions[0])
            construction.Direction |> should equal "server-to-client"
            construction.StageNumber |> should equal 1
            (construction.InnerException |> required).Message |> should equal "create failed"
            let cleanup = Assert.IsType<ScriptProcessorFailureException>(error.InnerExceptions[1])
            cleanup.StageNumber |> should equal 2
            (property<ConcurrentQueue<int>> plugin "events").ToArray() |> should equal [| 2; 1 |]
            let! processors = definition.CreateProcessorPipelinesAsync(connection)
            let! failures = ProcessorLifetime.disposeAllAsync(Array.append processors.ClientToBackend processors.BackendToClient)
            failures |> should be Empty
            (property<int ref> plugin "next").Value |> should equal 6
        }

    [<Fact>]
    let ``real asynchronous handler cancellation releases memory before explicit cleanup`` () =
        task {
            use timeout = new CancellationTokenSource(10_000: int)
            use workspace = new Workspace()
            let source = prelude + """
let create (ctx: CreateContext) = ctx.ConnectionState
let handle ctx (state: IConnectionState) = task {
    state.GetOrAdd("entered", fun () -> TaskCompletionSource<unit>()).TrySetResult(()) |> ignore
    do! Task.Delay(-1, ctx.CancellationToken)
    return releaseAll
}
let dispose (state: IConnectionState) = task {
    do! Task.Yield()
    state.GetOrAdd("cleaned", fun () -> TaskCompletionSource<unit>()).TrySetResult(()) |> ignore
}
"""
            let! _, plugin = compile workspace "Cancellation" source
            use connection = new ConnectionState()
            let entered, cleaned = signal (), signal ()
            connection.GetOrAdd("entered", fun () -> entered) |> ignore
            connection.GetOrAdd("cleaned", fun () -> cleaned) |> ignore
            use cancellation = new CancellationTokenSource()
            let pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], Sink(), MemoryPool<byte>.Shared)
            let operation = pipeline.PushAsync(new Owner(bytes "A"), 1, cancellation.Token)
            do! entered.Task.WaitAsync(timeout.Token)
            cancellation.Cancel()
            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> operation)
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            do! cleaned.Task.WaitAsync(timeout.Token)
            pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
        }

    [<Fact>]
    let ``class selection retains precedence and malformed classes cannot fall back to functions`` () =
        task {
            use workspace = new Workspace()
            let functional = "let handle (ctx: ProcessContext) = invalidOp \"functional must not run\"\n"
            let classSource declaration = prelude + functional + declaration + "\n    interface IByteStreamProcessor with\n        member _.ProcessAsync(input, output, _) =\n            output.AppendInput(0L, input.Bytes.Length)\n            ValueTask<ProcessorResult>(releaseAll)"
            let! _, plugin = compile workspace "ClassPrecedence" (classSource "type Processor() =")
            plugin.ProcessorType.Name |> should equal "Processor"
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            sink.Text |> should equal "A"
            for name, declaration in [ "private", "type private Processor() ="; "constructor", "type Processor(value: int) ="; "generic", "type Processor<'T>() =" ] do
                let path = workspace.Write(name, classSource declaration)
                let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
                match result with
                | Failed diagnostics -> (diagnosticsText diagnostics).Contains("no usable implementation") |> should equal true
                | _ -> failwith "Malformed class fell back to functional discovery"
            let path = workspace.Write("MultipleClasses", classSource "type Processor() =" + "\ntype Other() =\n    interface IByteStreamProcessor with\n        member _.ProcessAsync(_, _, _) = ValueTask<ProcessorResult>(releaseAll)")
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            match result with
            | Failed diagnostics -> (diagnosticsText diagnostics).Contains("multiple implementations") |> should equal true
            | _ -> failwith "Class ambiguity masked by functional discovery"
        }

    [<Fact>]
    let ``mixed class and functional scripts compose in configured order`` () =
        task {
            use workspace = new Workspace()
            let first = workspace.Write("First", prelude + "let handle ctx =\n    if not ctx.Input.IsEmpty then emitBytes ctx (ReadOnlySpan<byte>([| 65uy |]))\n    emitInput ctx 0L ctx.Input.Length\n    releaseAll")
            let second = workspace.Write("Second", prelude + "let handle ctx =\n    emitInput ctx 0L ctx.Input.Length\n    if not ctx.Input.IsEmpty then emitBytes ctx (ReadOnlySpan<byte>([| 66uy |]))\n    releaseAll")
            let classPath = Path.Combine(TestRepository.repositoryRoot, "examples", "AsciiUppercase.fsx")
            let! definition = load workspace [| first; classPath; second |] [||]
            use connection = new ConnectionState()
            let! processors = definition.CreateProcessorPipelinesAsync(connection)
            let sink = Sink()
            use pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
            do! push pipeline "x"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "AXB"
        }

    [<Theory>]
    [<InlineData("Passthrough")>]
    [<InlineData("Counter")>]
    [<InlineData("ExplicitUnit")>]
    [<InlineData("UnitState")>]
    [<InlineData("TupleState")>]
    [<InlineData("StructTupleState")>]
    [<InlineData("FunctionValues")>]
    [<InlineData("ClosureHandler")>]
    [<InlineData("TupleIdentityRepro")>]

    let ``guarded authoring fixtures compile in the actual host`` name =
        task {
            use workspace = new Workspace()
            let path = Path.Combine(TestRepository.repositoryRoot, "tests", "fixtures", "functional", name + ".fsx")
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let plugin = compiled result
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "AB"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "AB"
        }

    [<Fact>]
    let ``private definitions and compiled source names bind without public state requirements`` () =
        task {
            use workspace = new Workspace()
            let source = prelude + """
type private State = { Count: int ref }
[<CompiledName("MakeLocal")>]
let private create () = { Count = ref 0 }
[<CompiledName("HandleBytes")>]
let private handle ctx (state: State) =
    state.Count.Value <- state.Count.Value + 1
    emitInput ctx 0L ctx.Input.Length
    releaseAll
[<CompiledName("CleanupLocal")>]
let private dispose (state: State) = ()
"""
            let! _, plugin = compile workspace "PrivateDefinitions" source
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            sink.Text |> should equal "A"
        }

    [<Fact>]
    let ``shared record fields need annotation when used before a context helper`` () =
        task {
            use workspace = new Workspace()
            let path = Path.Combine(TestRepository.repositoryRoot, "tests", "fixtures", "functional", "ContextInferenceRepro.fsx")
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let errors = failed result
            errors |> Array.exists (fun d -> d.CompilerCode.IsSome && d.Message.Contains("ProcessorInput")) |> should equal true
            let source = File.ReadAllText(path).Replace("let handle ctx =", "let handle (ctx: ProcessContext) =")
            let! _, _ = compile workspace "ContextInferenceAnnotated" source
            return ()
        }

    [<Theory>]
    [<InlineData("create")>]
    [<InlineData("dispose")>]
    let ``lifecycle ambiguity and definitions in other modules are rejected`` functionName =
        task {
            use workspace = new Workspace()
            let source =
                "namespace LifecycleModules\nmodule Main =\n    open PipeL4.Plugins\n    let create () = ref 0\n    let handle (ctx: ProcessContext) (state: int ref) = releaseAll\n    let dispose (state: int ref) = ()\n" +
                "module Other =\n    let " + functionName + (if functionName = "create" then " () = ref 0" else " (state: int ref) = ()")
            let path = workspace.Write("AmbiguousLifecycle", source)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            (failed result |> diagnosticsText).Contains("Ambiguous functional '" + functionName + "'") |> should equal true
            let source = "namespace SplitModules\nmodule Main =\n    open PipeL4.Plugins\n    let handle (ctx: ProcessContext) (state: int ref) = releaseAll\nmodule Other =\n    let create () = ref 0"
            let path = workspace.Write("SplitLifecycle", source)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            (failed result |> diagnosticsText).Contains("same top-level module") |> should equal true
        }

    [<Theory>]
    [<InlineData("unit", "let create () = ()")>]
    [<InlineData("context", "let create (ctx: CreateContext) = ()")>]
    let ``unit state uses emitted void creation without boxing`` name creation =
        task {
            use workspace = new Workspace()
            let! _, plugin = compile workspace name (prelude + creation + "\nlet handle ctx (state: unit) = emitInput ctx 0L ctx.Input.Length; releaseAll\nlet dispose (state: unit) = ()")
            use connection = new ConnectionState()
            let sink = Sink()
            let pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            sink.Text |> should equal "A"
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
        }

    [<Theory>]
    [<InlineData("()", "()", "sync")>]
    [<InlineData("(_: unit)", "()", "task")>]
    [<InlineData("(ignored: unit)", "(state: unit)", "value-task")>]
    [<InlineData("()", "(state: unit)", "value-task-unit")>]
    [<InlineData("(_: unit)", "(state: unit)", "sync")>]
    [<InlineData("(ctx: CreateContext)", "()", "value-task")>]
    let ``unit argument representations normalize in both validation and invocation`` creationArgument disposalArgument resultForm =
        task {
            use workspace = new Workspace()
            let cleanup = "disposed.Value <- disposed.Value + 1"
            let disposal =
                match resultForm with
                | "task" -> "task {\n    do! Task.Yield()\n    " + cleanup + "\n}"
                | "value-task" -> "ValueTask(task {\n    do! Task.Yield()\n    " + cleanup + "\n} :> Task)"
                | "value-task-unit" -> "ValueTask<unit>(task {\n    do! Task.Yield()\n    " + cleanup + "\n})"
                | _ -> cleanup
            let source = prelude + "let created = ref 0\nlet disposed = ref 0\nlet calls = ref 0\nlet create " + creationArgument + " =\n    created.Value <- created.Value + 1\nlet handle ctx " + disposalArgument + " =\n    calls.Value <- calls.Value + 1\n    emitInput ctx 0L ctx.Input.Length\n    releaseAll\nlet dispose " + disposalArgument + " = " + disposal
            let! _, plugin = compile workspace "UnitArguments" source
            (property<int ref> plugin "created").Value |> should equal 0
            let creation = plugin.ProcessorType.GetMethod("create") |> required
            let expected = if creationArgument = "()" then [||] elif creationArgument = "(ctx: CreateContext)" then [| typeof<CreateContext> |] else [| typeof<unit> |]
            creation.GetParameters() |> Array.map _.ParameterType |> should equal expected
            let cleanup = plugin.ProcessorType.GetMethod("dispose") |> required
            cleanup.GetParameters() |> Array.map _.ParameterType |> should equal (if disposalArgument = "()" then [||] else [| typeof<unit> |])
            use connection = new ConnectionState()
            let! processors = ScriptPipelineDefinition([| ConfiguredScriptStage("unit", plugin); ConfiguredScriptStage("unit", plugin) |], [||]).CreateProcessorPipelinesAsync(connection)
            (property<int ref> plugin "created").Value |> should equal 2
            let sink = Sink()
            let pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            do! push pipeline "B"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "AB"
            (property<int ref> plugin "calls").Value |> should equal 6
            (property<int ref> plugin "disposed").Value |> should equal 0
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            (property<int ref> plugin "disposed").Value |> should equal 2
        }

    [<Fact>]
    let ``explicit unit creation can return non-unit state`` () =
        task {
            use workspace = new Workspace()
            let! _, plugin = compile workspace "ExplicitUnit" (prelude + "let create (_: unit) = ref 0\nlet handle ctx (state: int ref) =\n    state.Value <- state.Value + 1\n    emitInput ctx 0L ctx.Input.Length\n    releaseAll")
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "A"
        }

    [<Theory>]
    [<InlineData(false, "sync", false)>]
    [<InlineData(false, "task", false)>]
    [<InlineData(false, "value-task", false)>]
    [<InlineData(true, "sync", false)>]
    [<InlineData(true, "task", false)>]
    [<InlineData(true, "value-task", false)>]
    [<InlineData(false, "sync", true)>]
    [<InlineData(false, "task", true)>]
    [<InlineData(false, "value-task", true)>]
    [<InlineData(true, "value-task", true)>]
    let ``tuple state retains typed components across calls stages and cleanup`` structTuple resultForm aliases =
        let verifyRepresentation (plugin: CompiledScriptProcessor) =
            if aliases then
                let getter = (plugin.ProcessorType.GetProperty("handle") |> required).GetGetMethod() |> required
                FSharpType.IsFunction(getter.ReturnType) |> should equal true
            else
                let handle = plugin.ProcessorType.GetMethod("handle") |> required
                let counts = (handle.GetCustomAttribute<CompilationArgumentCountsAttribute>() |> required).Counts |> Seq.toArray
                counts |> should equal (if structTuple then [| 1; 1 |] else [| 1; 2 |])
                handle.GetParameters().Length |> should equal (if structTuple then 2 else 3)
        task {
            use workspace = new Workspace()
            let stateType = if structTuple then "struct (int ref * int ref)" else "int ref * int ref"
            let stateValue = if structTuple then "struct (ref 0, ref 0)" else "(ref 0, ref 0)"
            let pattern = if structTuple then "struct (first, second)" else "first, second"
            let body = "let " + pattern + " = state\n    first.Value <- first.Value + 1\n    second.Value <- second.Value + 10\n    seen.Enqueue(first)\n    emitInput ctx 0L ctx.Input.Length\n    releaseAll"
            let handler =
                match resultForm with
                | "task" -> "task {\n        do! Task.Yield()\n        " + body.Replace("\n    releaseAll", "\n    return releaseAll").Replace("\n    ", "\n        ") + "\n    }"
                | "value-task" -> "ValueTask<ProcessorResult>(task {\n        do! Task.Yield()\n        " + body.Replace("\n    releaseAll", "\n    return releaseAll").Replace("\n    ", "\n        ") + "\n    })"
                | _ -> body
            let source = prelude + "let created = System.Collections.Concurrent.ConcurrentQueue<int ref>()\nlet seen = System.Collections.Concurrent.ConcurrentQueue<int ref>()\nlet disposed = System.Collections.Concurrent.ConcurrentQueue<int ref>()\nlet create () =\n    let state = " + stateValue + "\n    let " + pattern + " = state\n    created.Enqueue(first)\n    state\nlet handle ctx (state: " + stateType + ") =\n    " + handler + "\nlet dispose (state: " + stateType + ") =\n    let " + pattern + " = state\n    if first.Value <> 3 || second.Value <> 30 then invalidOp \"wrong state\"\n    disposed.Enqueue(first)"
            let source =
                if aliases then
                    source.Replace("let create ()", "let make ()").Replace("let handle ctx", "let implementation ctx").Replace("let dispose (state:", "let cleanup (state:") +
                    "\nlet create = make\nlet handle = implementation\nlet dispose = cleanup"
                else source
            let! path, plugin = compile workspace "TupleState" source
            verifyRepresentation plugin
            let created = property<ConcurrentQueue<int ref>> plugin "created"
            created.Count |> should equal 0
            let! definition = ScriptPipelineLoader.loadWithCompilerAsync (fun _ _ -> Task.FromResult(Compiled(plugin, [||]))) [| path; path |] [||] CancellationToken.None
            for _ in 1 .. 2 do
                use connection = new ConnectionState()
                let! processors = definition.CreateProcessorPipelinesAsync(connection)
                let sink = Sink()
                let pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
                do! push pipeline "A"
                do! push pipeline "B"
                do! pipeline.CompleteAsync(CancellationToken.None)
                sink.Text |> should equal "AB"
                do! (pipeline :> IAsyncDisposable).DisposeAsync()
                pipeline.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
                pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            let states = created.ToArray()
            states.Length |> should equal 4
            for index in 0 .. 3 do
                for previous in 0 .. index - 1 do Assert.NotSame(states[index], states[previous])
            let seen = (property<ConcurrentQueue<int ref>> plugin "seen").ToArray()
            for index in 0 .. 3 do seen |> Array.filter (fun value -> obj.ReferenceEquals(value, states[index])) |> Array.length |> should equal 3
            let disposed = (property<ConcurrentQueue<int ref>> plugin "disposed").ToArray()
            for index, state in [ 0, states[1]; 1, states[0]; 2, states[3]; 3, states[2] ] do Assert.Same(state, disposed[index])
        }

    [<Theory>]
    [<InlineData("sync", false)>]
    [<InlineData("task", false)>]
    [<InlineData("value-task", false)>]
    [<InlineData("sync", true)>]
    [<InlineData("task", true)>]
    [<InlineData("value-task", true)>]
    let ``stateless handler aliases and captured closures execute through compiled getters`` resultForm closure =
        task {
            use workspace = new Workspace()
            let body = "emitInput ctx 0L ctx.Input.Length; releaseAll"
            let result =
                match resultForm with
                | "task" -> "task {\n        do! Task.Yield()\n        return (" + body + ")\n    }"
                | "value-task" -> "ValueTask<ProcessorResult>(" + body + ")"
                | _ -> body
            let binding =
                if closure then
                    "let handle =\n    let count = ref 0\n    fun ctx ->\n        count.Value <- count.Value + 1\n        " + result.Replace("\n", "\n    ")
                else
                    "let implementation ctx =\n    " + result + "\nlet handle = implementation"
            let! _, plugin = compile workspace "FunctionValue" (prelude + binding)
            let getter = (plugin.ProcessorType.GetProperty("handle") |> required).GetGetMethod() |> required
            getter.IsSpecialName |> should equal true
            FSharpType.IsFunction(getter.ReturnType) |> should equal true
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "AB"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "AB"
        }

    [<Theory>]
    [<InlineData("sync", "sync", false)>]
    [<InlineData("task", "task", true)>]
    [<InlineData("value-task", "value-task", false)>]
    [<InlineData("sync", "value-task-unit", true)>]
    let ``stateful lifecycle aliases are reusable functions with fresh local states`` resultForm disposalForm contextCreation =
        task {
            use workspace = new Workspace()
            let body = "state.Value <- state.Value + 1\n    seen.Enqueue(state)\n    emitInput ctx 0L ctx.Input.Length\n    releaseAll"
            let handler =
                match resultForm with
                | "task" -> "task {\n        do! Task.Yield()\n        " + body.Replace("\n    releaseAll", "\n    return releaseAll").Replace("\n    ", "\n        ") + "\n    }"
                | "value-task" -> "ValueTask<ProcessorResult>(task {\n        do! Task.Yield()\n        " + body.Replace("\n    releaseAll", "\n    return releaseAll").Replace("\n    ", "\n        ") + "\n    })"
                | _ -> body
            let cleanup = "disposed.Enqueue(state)"
            let disposal =
                match disposalForm with
                | "task" -> "task {\n    do! Task.Yield()\n    " + cleanup + "\n}"
                | "value-task" -> "ValueTask(task {\n    do! Task.Yield()\n    " + cleanup + "\n} :> Task)"
                | "value-task-unit" -> "ValueTask<unit>(task {\n    do! Task.Yield()\n    " + cleanup + "\n})"
                | _ -> cleanup
            let argument = if contextCreation then "(ctx: CreateContext)" else "(_: unit)"
            let source = prelude + "let created = System.Collections.Concurrent.ConcurrentQueue<int ref>()\nlet seen = System.Collections.Concurrent.ConcurrentQueue<int ref>()\nlet disposed = System.Collections.Concurrent.ConcurrentQueue<int ref>()\nlet make " + argument + " =\n    let state = ref 0\n    created.Enqueue(state)\n    state\nlet create = make\nlet implementation ctx (state: int ref) =\n    " + handler + "\nlet handle = implementation\nlet cleanup (state: int ref) = " + disposal + "\nlet dispose = cleanup"
            let! path, plugin = compile workspace "LifecycleAliases" source
            for name in [ "create"; "handle"; "dispose" ] do
                let getter = (plugin.ProcessorType.GetProperty(name) |> required).GetGetMethod() |> required
                FSharpType.IsFunction(getter.ReturnType) |> should equal true
            let created = property<ConcurrentQueue<int ref>> plugin "created"
            created.Count |> should equal 0
            let! definition = ScriptPipelineLoader.loadWithCompilerAsync (fun _ _ -> Task.FromResult(Compiled(plugin, [||]))) [| path; path |] [||] CancellationToken.None
            for _ in 1 .. 2 do
                use connection = new ConnectionState()
                let! processors = definition.CreateProcessorPipelinesAsync(connection)
                let sink = Sink()
                let pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
                do! push pipeline "A"
                do! push pipeline "B"
                do! pipeline.CompleteAsync(CancellationToken.None)
                sink.Text |> should equal "AB"
                do! (pipeline :> IAsyncDisposable).DisposeAsync()
            let states = created.ToArray()
            states.Length |> should equal 4
            for index in 0 .. 3 do
                states[index].Value |> should equal 3
                for previous in 0 .. index - 1 do Assert.NotSame(states[index], states[previous])
                (property<ConcurrentQueue<int ref>> plugin "seen").ToArray() |> Array.filter (fun value -> obj.ReferenceEquals(value, states[index])) |> Array.length |> should equal 3
            let disposed = (property<ConcurrentQueue<int ref>> plugin "disposed").ToArray()
            for index, state in [ 0, states[1]; 1, states[0]; 2, states[3]; 3, states[2] ] do Assert.Same(state, disposed[index])
        }

    [<Theory>]
    [<InlineData("creation-type", "let implementation (value: int) = ref value\nlet create = implementation\nlet handle (ctx: ProcessContext) (state: int ref) = releaseAll", "create")>]
    [<InlineData("creation-mismatch", "let make () = (ref 0, ref \"x\")\nlet create = make\nlet handle (ctx: ProcessContext) (state: int ref * int ref) = releaseAll", "create")>]
    [<InlineData("disposal-mismatch", "let create () = (ref 0, ref 0)\nlet handle (ctx: ProcessContext) (state: int ref * int ref) = releaseAll\nlet cleanup (state: int ref * string ref) = ()\nlet dispose = cleanup", "dispose")>]
    [<InlineData("tuple-kind", "let create () = struct (ref 0, ref 0)\nlet handle (ctx: ProcessContext) (state: int ref * int ref) = releaseAll", "create")>]
    [<InlineData("tupled-method", "let create () = (ref 0, ref 0)\nlet handle (ctx: ProcessContext, state: int ref * int ref) = releaseAll", "handle")>]
    [<InlineData("tupled-alias", "let create () = ref 0\nlet implementation (ctx: ProcessContext, state: int ref) = releaseAll\nlet handle = implementation", "handle")>]
    [<InlineData("literal-handle", "[<Literal>]\nlet handle = 42", "handle")>]
    [<InlineData("literal-create", "[<Literal>]\nlet create = 42\nlet handle (ctx: ProcessContext) (state: int) = releaseAll", "create")>]
    [<InlineData("literal-dispose", "let create () = ref 0\nlet handle (ctx: ProcessContext) (state: int ref) = releaseAll\n[<Literal>]\nlet dispose = 42", "dispose")>]
    [<InlineData("value-dispose", "let create () = ref 0\nlet handle (ctx: ProcessContext) (state: int ref) = releaseAll\nlet dispose = 42", "dispose")>]
    [<InlineData("alias-result", "let implementation (ctx: ProcessContext) = 42\nlet handle = implementation", "handle")>]
    [<InlineData("alias-async-create", "let make () = Task.FromResult(ref 0)\nlet create = make\nlet handle (ctx: ProcessContext) (state: Task<int ref>) = releaseAll", "create")>]
    [<InlineData("alias-three-arguments", "let implementation (ctx: ProcessContext) (state: int) (extra: int) = releaseAll\nlet handle = implementation", "handle")>]
    let ``invalid callable values and argument groups remain static signature errors`` name source (functionName: string) =
        task {
            use workspace = new Workspace()
            let path = workspace.Write(name, prelude + source)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let error = failed result |> Array.find (fun d -> d.Severity = ScriptDiagnosticSeverity.Error)
            error.CompilerCode |> should equal None
            error.SourcePath |> should equal (Some path)
            error.Message.Contains("Invalid functional '" + functionName + "'") |> should equal true
            error.Message.Contains("expected") |> should equal true
            error.Message.Contains("actual") |> should equal true
        }

    [<Fact>]
    let ``function getter participates in deterministic method ambiguity`` () =
        task {
            use workspace = new Workspace()
            let source = "namespace MixedDefinitions\nmodule First =\n    open PipeL4.Plugins\n    let implementation ctx = emitInput ctx 0L ctx.Input.Length; releaseAll\n    let handle = implementation\nmodule Second =\n    open PipeL4.Plugins\n    let handle (ctx: ProcessContext) = releaseAll"
            let path = workspace.Write("MixedDefinitions", source)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let text = failed result |> diagnosticsText
            text.Contains("Ambiguous functional 'handle'") |> should equal true
            text.IndexOf("First", StringComparison.Ordinal) < text.IndexOf("Second", StringComparison.Ordinal) |> should equal true
        }

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``large and nested tuple states use logical elements beyond CLR Rest`` nested =
        task {
            use workspace = new Workspace()
            let stateType = if nested then "(int ref * int ref) * int ref" else String.concat " * " (List.replicate 9 "int ref")
            let stateValue = if nested then "((ref 0, ref 0), ref 0)" else "(" + String.concat ", " (List.replicate 9 "ref 0") + ")"
            let pattern = if nested then "(first, _), last" else "first, _, _, _, _, _, _, _, last"
            let source = prelude + "let create () = " + stateValue + "\nlet handle ctx (state: " + stateType + ") =\n    let " + pattern + " = state\n    first.Value <- first.Value + 1\n    last.Value <- last.Value + 1\n    emitInput ctx 0L ctx.Input.Length\n    releaseAll\nlet dispose (state: " + stateType + ") =\n    let " + pattern + " = state\n    if first.Value <> 3 || last.Value <> 3 then invalidOp \"wrong tuple components\""
            let! _, plugin = compile workspace "LargeTuple" source
            use connection = new ConnectionState()
            let sink = Sink()
            let pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            do! push pipeline "B"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "AB"
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
        }

    [<Fact>]
    let ``flattened reference tuple identity matches native FSharp without changing its components`` () =
        task {
            use workspace = new Workspace()
            let path = Path.Combine(TestRepository.repositoryRoot, "tests", "fixtures", "functional", "TupleIdentityRepro.fsx")
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let plugin = compiled result
            let native = property<bool> plugin "nativeIdentity"
            native |> should equal false
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "A"
            do! push pipeline "B"
            do! pipeline.CompleteAsync(CancellationToken.None)
            (property<ResizeArray<bool>> plugin "observed").ToArray() |> should equal [| native; native; native |]
            sink.Text |> should equal "AB"
        }

    [<Theory>]
    [<InlineData("create")>]
    [<InlineData("handle")>]
    [<InlineData("dispose")>]
    let ``function alias failures retain construction processing and cleanup context`` phase =
        task {
            use workspace = new Workspace()
            let creation = if phase = "create" then "invalidOp \"alias create failed\"" else "ref 0"
            let handler = if phase = "handle" then "invalidOp \"alias handle failed\"" else "releaseAll"
            let cleanup = if phase = "dispose" then "invalidOp \"alias dispose failed\"" else "()"
            let source = prelude + "let make () : int ref = " + creation + "\nlet create = make\nlet implementation (ctx: ProcessContext) (state: int ref) : Task<ProcessorResult> = task {\n    do! Task.Yield()\n    return " + handler + "\n}\nlet handle = implementation\nlet cleanup (state: int ref) = ValueTask<unit>(task {\n    do! Task.Yield()\n    return " + cleanup + "\n})\nlet dispose = cleanup"
            let! path, plugin = compile workspace "AliasFailure" source
            let definition = ScriptPipelineDefinition([| ConfiguredScriptStage(path, plugin) |], [||])
            use connection = new ConnectionState()
            if phase = "create" then
                let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> definition.CreateProcessorPipelinesAsync(connection).AsTask() :> Task)
                error.StageNumber |> should equal 1
                (error.InnerException |> required).Message |> should equal "alias create failed"
            else
                let! processors = definition.CreateProcessorPipelinesAsync(connection)
                let pipeline = new ProcessorPipeline(processors.ClientToBackend, Sink(), MemoryPool<byte>.Shared)
                let! error =
                    if phase = "handle" then
                        Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> push pipeline "A")
                    else
                        Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> (pipeline :> IAsyncDisposable).DisposeAsync().AsTask())
                error.ScriptPath |> should equal path
                error.Direction |> should equal "client-to-server"
                error.StageNumber |> should equal 1
                (error.InnerException |> required).Message |> should equal ("alias " + phase + " failed")
                do! (pipeline :> IAsyncDisposable).DisposeAsync()
        }

    [<Fact>]
    let ``normalized synchronous and completed ValueTask bindings add no per-call allocations`` () =
        task {
            use workspace = new Workspace()
            use connection = new ConnectionState()
            let output =
                { new PipeL4.Core.IProcessorOutput with
                    member _.AppendInput(_, _) = ()
                    member _.Advance(_) = ()
                    member _.GetMemory(_) = Memory<byte>.Empty
                    member _.GetSpan(_) = Span<byte>.Empty }
            let input: PipeL4.Core.ProcessorInput =
                { Bytes = ReadOnlySequence<byte>([| 1uy |]); IsCompleted = false }
            let measure (processor: PipeL4.Core.IByteStreamProcessor) =
                let invoke () =
                    let operation = processor.ProcessAsync(input, output, CancellationToken.None)
                    if not operation.IsCompletedSuccessfully || operation.Result.RetainedLength <> 0L then
                        invalidOp "Expected synchronous completion"
                for _ in 1 .. 4096 do invoke ()
                let before = GC.GetAllocatedBytesForCurrentThread()
                for _ in 1 .. 4096 do invoke ()
                GC.GetAllocatedBytesForCurrentThread() - before
            let baselineSource = prelude + "let handle ctx = emitInput ctx 0L ctx.Input.Length; releaseAll"
            let! _, baseline = compile workspace "AllocationBaseline" baselineSource
            let control = baseline.CreateProcessor(publicState connection)
            let cases =
                [ "tuple", "let create () = (ref 0, ref 0)\nlet handle ctx (state: int ref * int ref) = emitInput ctx 0L ctx.Input.Length; releaseAll"
                  "tuple-value-task", "let create () = (ref 0, ref 0)\nlet handle ctx (state: int ref * int ref) = emitInput ctx 0L ctx.Input.Length; ValueTask<ProcessorResult>(releaseAll)"
                  "unit", "let create (_: unit) = ()\nlet handle ctx () = emitInput ctx 0L ctx.Input.Length; releaseAll"
                  "struct-tuple", "let create () = struct (ref 0, ref 0)\nlet handle ctx (state: struct (int ref * int ref)) = emitInput ctx 0L ctx.Input.Length; releaseAll"
                  "tuple-alias", "let create () = (ref 0, ref 0)\nlet implementation ctx (state: int ref * int ref) = emitInput ctx 0L ctx.Input.Length; releaseAll\nlet handle = implementation"
                  "stateful-alias", "let create () = ref 0\nlet implementation ctx (state: int ref) = emitInput ctx 0L ctx.Input.Length; releaseAll\nlet handle = implementation"
                  "stateless-alias", "let implementation ctx = emitInput ctx 0L ctx.Input.Length; releaseAll\nlet handle = implementation" ]
            for name, source in cases do
                let! _, plugin = compile workspace name (prelude + source)
                let processor = plugin.CreateProcessor(publicState connection)
                // Interleave controls with cases to sample the same warmed execution period.
                // The median rejects isolated measurement spikes without allowing any fixed
                // byte allowance: a recurring per-invocation allocation raises the median.
                let controls, allocated =
                    Array.init 7 (fun _ -> measure control, measure processor)
                    |> Array.unzip
                let median values = (Array.sort values)[values.Length / 2]
                let expected, actual = median controls, median allocated
                let samples values = values |> Array.map string |> String.concat ", "
                let delta = actual - expected
                Assert.True((actual = expected),
                    $"Allocation case '{name}': 7 samples, each 4096 warmup + 4096 measured invocations; control=[{samples controls}] bytes; case=[{samples allocated}] bytes; medians control={expected}, case={actual}; delta={delta} bytes ({float delta / 4096.0} bytes/invocation).")
                let! failures = ProcessorLifetime.disposeAllAsync [| processor |]
                failures |> should be Empty
        }

    [<Theory>]
    [<InlineData("direct", "sync")>]
    [<InlineData("direct", "task")>]
    [<InlineData("direct", "value-task")>]
    [<InlineData("getter", "sync")>]
    [<InlineData("getter", "task")>]
    [<InlineData("getter", "value-task")>]
    [<InlineData("mixed", "sync")>]
    [<InlineData("mixed", "task")>]
    [<InlineData("mixed", "value-task")>]
    [<InlineData("mixed", "completed-value-task")>]
    let ``equivalent handler representations evaluate each context during processing`` representation resultForm =
        let verifyRepresentation (plugin: CompiledScriptProcessor) =
            if representation = "mixed" then
                let method = plugin.ProcessorType.GetMethod("handle") |> required
                method.GetParameters() |> Array.map _.ParameterType |> should equal [| typeof<ProcessContext> |]
                FSharpType.IsFunction(method.ReturnType) |> should equal true
                FSharpType.GetFunctionElements(method.ReturnType) |> fst |> should equal typeof<int ref>
        task {
            use workspace = new Workspace()
            let body = """state.Value <- state.Value + int n
seen.Enqueue(n, completed, state.Value)
emitInput ctx 0L n
releaseAll"""
            let indent count (text: string) =
                text.Split('\n') |> Array.map (fun line -> String.replicate count " " + line) |> String.concat "\n"
            let result =
                match resultForm with
                | "task" -> "task {\n    do! Task.Yield()\n" + indent 4 (body.Replace("\nreleaseAll", "\nreturn releaseAll")) + "\n}"
                | "value-task" -> "ValueTask<ProcessorResult>(task {\n    do! Task.Yield()\n" + indent 4 (body.Replace("\nreleaseAll", "\nreturn releaseAll")) + "\n})"
                | "completed-value-task" -> "ValueTask<ProcessorResult>(\n" + indent 4 body + "\n)"
                | _ -> body
            let declaration =
                if representation = "mixed" then
                    "let handle (ctx: ProcessContext) =\n    evaluations.Value <- evaluations.Value + 1\n    let n = ctx.Input.Length\n    let completed = ctx.IsCompleted\n    fun (state: int ref) ->\n" + indent 8 result
                else
                    let name = if representation = "getter" then "implementation" else "handle"
                    "let " + name + " (ctx: ProcessContext) (state: int ref) =\n    evaluations.Value <- evaluations.Value + 1\n    let n = ctx.Input.Length\n    let completed = ctx.IsCompleted\n" + indent 4 result +
                    (if representation = "getter" then "\nlet handle = implementation" else "")
            let declarations = prelude + """
let evaluations = ref 0
let created = System.Collections.Concurrent.ConcurrentQueue<int ref>()
let disposed = System.Collections.Concurrent.ConcurrentQueue<int ref>()
let seen = System.Collections.Concurrent.ConcurrentQueue<int64 * bool * int>()
let create () =
    let state = ref 0
    created.Enqueue(state)
    state
let dispose (state: int ref) = disposed.Enqueue(state)
"""
            let source = declarations + declaration
            let! path, plugin = compile workspace "Representations" source
            verifyRepresentation plugin
            let evaluations = property<int ref> plugin "evaluations"
            evaluations.Value |> should equal 0
            (property<ConcurrentQueue<int ref>> plugin "created").Count |> should equal 0
            let definition = ScriptPipelineDefinition([| ConfiguredScriptStage(path, plugin) |], [||])
            for index in 1 .. 2 do
                use connection = new ConnectionState()
                let! processors = definition.CreateProcessorPipelinesAsync(connection)
                evaluations.Value |> should equal ((index - 1) * 3)
                let sink = Sink()
                let pipeline = new ProcessorPipeline(processors.ClientToBackend, sink, MemoryPool<byte>.Shared)
                do! push pipeline "AB"
                do! push pipeline "C"
                do! pipeline.CompleteAsync(CancellationToken.None)
                sink.Text |> should equal "ABC"
                evaluations.Value |> should equal (index * 3)
                do! (pipeline :> IAsyncDisposable).DisposeAsync()
                let metrics = pipeline.GetStageMetricsSnapshot() |> List.exactlyOne
                metrics.GeneratedOutputBytes |> should equal 0L
                pipeline.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            let created = (property<ConcurrentQueue<int ref>> plugin "created").ToArray()
            let disposed = (property<ConcurrentQueue<int ref>> plugin "disposed").ToArray()
            Assert.NotSame(created[0], created[1])
            for index in 0 .. 1 do Assert.Same(created[index], disposed[index])
            (property<ConcurrentQueue<int64 * bool * int>> plugin "seen").ToArray()
            |> should equal [| 2L, false, 2; 1L, false, 3; 0L, true, 3; 2L, false, 2; 1L, false, 3; 0L, true, 3 |]
        }

    [<Theory>]
    [<InlineData("unit")>]
    [<InlineData("tuple")>]
    [<InlineData("struct-tuple")>]
    let ``mixed handlers keep their returned function state argument intact`` stateForm =
        task {
            use workspace = new Workspace()
            let stateType, creation, update =
                match stateForm with
                | "unit" -> "unit", "()", "()"
                | "tuple" -> "int ref * int ref", "(ref 0, ref 0)", "let first, second = state in first.Value <- first.Value + int n; second.Value <- second.Value + 1"
                | _ -> "struct (int ref * int ref)", "struct (ref 0, ref 0)", "let struct (first, second) = state in first.Value <- first.Value + int n; second.Value <- second.Value + 1"
            let source = prelude + "let create () = " + creation + "\nlet handle (ctx: ProcessContext) =\n    let n = ctx.Input.Length\n    fun (state: " + stateType + ") ->\n        " + update + "\n        emitInput ctx 0L n\n        releaseAll"
            let! _, plugin = compile workspace "MixedState" source
            use connection = new ConnectionState()
            let sink = Sink()
            use pipeline = new ProcessorPipeline([| plugin.CreateProcessor(publicState connection) |], sink, MemoryPool<byte>.Shared)
            do! push pipeline "AB"
            do! push pipeline "C"
            do! pipeline.CompleteAsync(CancellationToken.None)
            sink.Text |> should equal "ABC"
        }

    [<Theory>]
    [<InlineData("tupled", "let handle (ctx: ProcessContext, ignored: int) =\n    let n = ctx.Input.Length\n    fun (state: int ref) -> releaseAll", "handle")>]
    [<InlineData("excess-returned", "let handle (ctx: ProcessContext) =\n    let n = ctx.Input.Length\n    fun (state: int ref) (extra: int) -> releaseAll", "handle")>]
    [<InlineData("excess-method", "let handle (ctx: ProcessContext) (state: int ref) =\n    let n = ctx.Input.Length\n    fun (extra: int) -> releaseAll", "handle")>]
    [<InlineData("wrong-context", "let handle (ctx: string) =\n    let n = ctx.Length\n    fun (state: int ref) -> releaseAll", "handle")>]
    [<InlineData("wrong-state", "let handle (ctx: ProcessContext) =\n    let n = ctx.Input.Length\n    fun (state: string ref) -> releaseAll", "create")>]
    [<InlineData("wrong-result", "let handle (ctx: ProcessContext) =\n    let n = ctx.Input.Length\n    fun (state: int ref) -> 42", "handle")>]
    let ``mixed representations still reject invalid logical signatures`` name declaration functionName =
        task {
            use workspace = new Workspace()
            let path = workspace.Write(name, prelude + "let create () = ref 0\n" + declaration)
            let! result = workspace.Compiler.CompileAndLoadAsync(path, CancellationToken.None)
            let error = failed result |> Array.find (fun diagnostic -> diagnostic.Severity = ScriptDiagnosticSeverity.Error)
            error.CompilerCode |> should equal None
            error.SourcePath |> should equal (Some path)
            error.Message.Contains("Invalid functional '" + functionName + "'") |> should equal true
        }

    [<Theory>]
    [<InlineData("outer", "sync")>]
    [<InlineData("inner", "sync")>]
    [<InlineData("inner", "task")>]
    [<InlineData("inner", "value-task")>]
    let ``mixed handler failures retain processing context and cleanup`` failureLocation resultForm =
        task {
            use workspace = new Workspace()
            let returnType, body =
                match resultForm with
                | "task" -> "Task<ProcessorResult>", "task {\n            do! Task.Yield()\n            return invalidOp \"mixed failure\"\n        }"
                | "value-task" -> "ValueTask<ProcessorResult>", "ValueTask<ProcessorResult>(task {\n            do! Task.Yield()\n            return invalidOp \"mixed failure\"\n        })"
                | _ -> "ProcessorResult", "invalidOp \"mixed failure\""
            let outer = if failureLocation = "outer" then "    if n > 0L then invalidOp \"mixed failure\"\n" else ""
            let source = prelude + "let cleaned = ref false\nlet create () = ref 0\nlet dispose (state: int ref) = cleaned.Value <- true\nlet handle (ctx: ProcessContext) : int ref -> " + returnType + " =\n    let n = ctx.Input.Length\n" + outer + "    fun (state: int ref) ->\n        " + body
            let! path, plugin = compile workspace "MixedFailure" source
            use connection = new ConnectionState()
            let! processors = ScriptPipelineDefinition([| ConfiguredScriptStage(path, plugin) |], [||]).CreateProcessorPipelinesAsync(connection)
            let pipeline = new ProcessorPipeline(processors.ClientToBackend, Sink(), MemoryPool<byte>.Shared)
            let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> push pipeline "A")
            error.ScriptPath |> should equal path
            error.Direction |> should equal "client-to-server"
            error.StageNumber |> should equal 1
            (error.InnerException |> required).Message |> should equal "mixed failure"
            do! (pipeline :> IAsyncDisposable).DisposeAsync()
            (property<bool ref> plugin "cleaned").Value |> should equal true
        }
