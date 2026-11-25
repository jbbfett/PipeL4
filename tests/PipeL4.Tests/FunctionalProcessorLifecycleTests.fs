namespace PipeL4.Tests

open System
open System.Buffers
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open PipeL4.PluginHost
open PipeL4.Plugins
open Xunit

module FunctionalProcessorLifecycleTests =
    type private Owner() =
        let buffer = [| 1uy |]
        let mutable disposals = 0
        member _.Disposals = disposals
        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = disposals <- disposals + 1

    type private Resource() =
        let mutable syncDisposals = 0
        let mutable asyncDisposals = 0
        member _.SyncDisposals = syncDisposals
        member _.AsyncDisposals = asyncDisposals
        interface IDisposable with
            member _.Dispose() = syncDisposals <- syncDisposals + 1
        interface IAsyncDisposable with
            member _.DisposeAsync() =
                asyncDisposals <- asyncDisposals + 1
                ValueTask()

    [<NoEquality; NoComparison>]
    type private LocalState =
        { Registry: IConnectionState
          Shared: Resource
          mutable Calls: int }

    let private sink =
        { new IProcessorPipelineSink with
            member _.WriteAsync(_, token) =
                token.ThrowIfCancellationRequested()
                ValueTask()
            member _.CompleteAsync token =
                token.ThrowIfCancellationRequested()
                ValueTask() }

    let private pipeline processors =
        new ProcessorPipeline(processors, sink, MemoryPool<byte>.Shared)

    let private dispose (value: ProcessorPipeline) =
        (value :> IAsyncDisposable).DisposeAsync().AsTask()

    let private assertContext path direction number (cause: exn) (error: exn) =
        let contextual = Assert.IsType<ScriptProcessorFailureException>(error)
        contextual.ScriptPath |> should equal path
        contextual.Direction |> should equal direction
        contextual.StageNumber |> should equal number
        Assert.Same(cause, contextual.InnerException)

    [<Fact>]
    let ``parameterless create is eager and cleanup receives its state without any invocation`` () =
        task {
            use connection = new ConnectionState()
            let events = ResizeArray<string>()
            let created = ResizeArray<int64 ref>()
            let disposed = ResizeArray<int64 ref>()
            let stage =
                ConfiguredScriptStage.CreateFunctional(
                    "counter.fsx",
                    (fun () ->
                        events.Add "create"
                        let state = ref 0L
                        created.Add state
                        state),
                    (fun _ (state: int64 ref) ->
                        state.Value <- state.Value + 1L
                        events.Add "handle"
                        releaseAll),
                    dispose = (fun state ->
                        events.Add "dispose"
                        disposed.Add state)
                )
            // Repeating a definition must construct distinct instances, even with no input.
            let definition = ScriptPipelineDefinition([| stage; stage |], Array.empty)
            events |> should be Empty
            let! processors = definition.CreateProcessorPipelinesAsync(connection).AsTask()
            events.ToArray() |> should equal [| "create"; "create" |]
            Assert.NotSame(created[0], created[1])
            use executor = pipeline processors.ClientToBackend
            do! dispose executor
            do! dispose executor
            events.ToArray() |> should equal [| "create"; "create"; "dispose"; "dispose" |]
            Assert.Same(created[1], disposed[0])
            Assert.Same(created[0], disposed[1])
        }

    [<Fact>]
    let ``local state is isolated while both directions share only their connection registry`` () =
        task {
            use first = new ConnectionState()
            use second = new ConnectionState()
            let created = ResizeArray<LocalState>()
            let disposed = ResizeArray<LocalState>()
            let stage =
                ConfiguredScriptStage.CreateFunctional(
                    "shared.fsx",
                    (fun (ctx: CreateContext) ->
                        let state =
                            { Registry = ctx.ConnectionState
                              Shared = ctx.ConnectionState.GetOrAdd("resource", fun () -> new Resource())
                              Calls = 0 }
                        created.Add state
                        state),
                    (fun ctx state ->
                        Assert.True(created |> Seq.exists (fun original -> obj.ReferenceEquals(original, state)))
                        Assert.Same(state.Registry, ctx.ConnectionState)
                        Assert.Same(state.Shared, ctx.ConnectionState.TryGet<Resource>("resource").Value)
                        state.Calls <- state.Calls + 1
                        emitInput ctx 0L ctx.Input.Length
                        releaseAll),
                    dispose = (fun state ->
                        // Connection resources are still live during stage cleanup.
                        Assert.Same(state.Shared, state.Registry.TryGet<Resource>("resource").Value)
                        state.Shared.AsyncDisposals |> should equal 0
                        disposed.Add state)
                )
            let definition = ScriptPipelineDefinition([| stage; stage |], [| stage |])
            let! firstProcessors = definition.CreateProcessorPipelinesAsync(first).AsTask()
            let! secondProcessors = definition.CreateProcessorPipelinesAsync(second).AsTask()
            created.Count |> should equal 6
            for index = 0 to created.Count - 1 do
                created[index].Calls |> should equal 0
                for previous = 0 to index - 1 do
                    Assert.NotSame(created[previous], created[index])
                let connectionStart = if index < 3 then 0 else 3
                Assert.Same(created[connectionStart].Registry, created[index].Registry)
                Assert.Same(created[connectionStart].Shared, created[index].Shared)
            Assert.NotSame(created[0].Registry, created[3].Registry)
            Assert.NotSame(created[0].Shared, created[3].Shared)

            for processors in [ firstProcessors; secondProcessors ] do
                for direction in [ processors.ClientToBackend; processors.BackendToClient ] do
                    use executor = pipeline direction
                    for _ = 1 to 2 do
                        let owner = new Owner()
                        do! executor.PushAsync(owner, 1, CancellationToken.None)
                        owner.Disposals |> should equal 1
                    do! executor.CompleteAsync CancellationToken.None
                    do! dispose executor
                    do! dispose executor

            disposed.Count |> should equal 6
            let expectedOrder = [| 1; 0; 2; 4; 3; 5 |]
            for index = 0 to disposed.Count - 1 do
                Assert.Same(created[expectedOrder[index]], disposed[index])
                disposed[index].Calls |> should equal 3
            for connection in [ first; second ] do
                do! (connection :> IAsyncDisposable).DisposeAsync().AsTask()
            for local in created do
                local.Shared.AsyncDisposals |> should equal 1
                local.Shared.SyncDisposals |> should equal 0
        }

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``state resources are disposed only by explicit user cleanup`` explicitCleanup =
        task {
            use connection = new ConnectionState()
            let resource = new Resource()
            let mutable callbacks = 0
            let stage =
                ConfiguredScriptStage.CreateFunctional(
                    "resource.fsx",
                    (fun () -> resource),
                    (fun _ state ->
                        Assert.Same(resource, state)
                        releaseAll),
                    ?dispose =
                        (if explicitCleanup then
                             Some(fun state ->
                                 callbacks <- callbacks + 1
                                 (state :> IDisposable).Dispose())
                         else None)
                )
            let! processors =
                ScriptPipelineDefinition([| stage |], Array.empty)
                    .CreateProcessorPipelinesAsync(connection).AsTask()
            use executor = pipeline processors.ClientToBackend
            do! executor.CompleteAsync CancellationToken.None
            do! dispose executor
            callbacks |> should equal (if explicitCleanup then 1 else 0)
            resource.SyncDisposals |> should equal callbacks
            resource.AsyncDisposals |> should equal 0
        }

    [<Fact>]
    let ``cleanup neither recurses into local state nor disposes a connection registry returned as state`` () =
        task {
            use connection = new ConnectionState()
            let nested = new Resource()
            let recordStage =
                ConfiguredScriptStage.CreateFunctional(
                    "nested.fsx",
                    (fun (ctx: CreateContext) ->
                        { Registry = ctx.ConnectionState; Shared = nested; Calls = 0 }),
                    (fun _ _ -> releaseAll),
                    dispose = (fun state -> state.Calls <- state.Calls + 1)
                )
            let registryStage =
                ConfiguredScriptStage.CreateFunctional(
                    "registry.fsx",
                    (fun (ctx: CreateContext) -> ctx.ConnectionState),
                    (fun ctx state ->
                        Assert.Same(state, ctx.ConnectionState)
                        releaseAll)
                )
            let! processors =
                ScriptPipelineDefinition([| recordStage; registryStage |], Array.empty)
                    .CreateProcessorPipelinesAsync(connection).AsTask()
            use executor = pipeline processors.ClientToBackend
            do! executor.CompleteAsync CancellationToken.None
            do! dispose executor
            nested.SyncDisposals |> should equal 0
            nested.AsyncDisposals |> should equal 0
            connection.GetOrAdd("still-live", fun () -> 42) |> should equal 42
        }

    [<Fact>]
    let ``value type state has the same typed value at creation handling and cleanup`` () =
        task {
            use connection = new ConnectionState()
            let mutable handled = 0
            let mutable disposed = 0
            let stage =
                ConfiguredScriptStage.CreateFunctional(
                    "value.fsx",
                    (fun () -> struct (42L, 7)),
                    (fun _ (struct (count, marker)) ->
                        count |> should equal 42L
                        marker |> should equal 7
                        handled <- handled + 1
                        releaseAll),
                    dispose = (fun (struct (count, marker)) ->
                        count |> should equal 42L
                        marker |> should equal 7
                        disposed <- disposed + 1)
                )
            let! processors =
                ScriptPipelineDefinition([| stage |], Array.empty)
                    .CreateProcessorPipelinesAsync(connection).AsTask()
            use executor = pipeline processors.ClientToBackend
            do! executor.CompleteAsync CancellationToken.None
            do! dispose executor
            handled |> should equal 1
            disposed |> should equal 1
        }

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``creation failure rolls back completed directions and preserves cleanup failures`` cleanupFails =
        task {
            use connection = new ConnectionState()
            let creationFailure = InvalidOperationException("create failed")
            let cleanupFailure = InvalidOperationException("cleanup failed")
            let published = new Resource()
            let events = ResizeArray<string>()
            let created = ResizeArray<int ref>()
            let cleaned = ResizeArray<int ref>()
            let mutable failCreation = true
            let successful =
                ConfiguredScriptStage.CreateFunctional(
                    "successful.fsx",
                    (fun () ->
                        let state = ref created.Count
                        created.Add state
                        state),
                    (fun _ _ -> failwith "Rollback must not process input."),
                    dispose = (fun state ->
                        cleaned.Add state
                        events.Add $"dispose {state.Value}"
                        if cleanupFails then raise cleanupFailure)
                )
            let failing =
                ConfiguredScriptStage.CreateFunctional(
                    "failing.fsx",
                    (fun (ctx: CreateContext) ->
                        ctx.ConnectionState.GetOrAdd("owned-before-failure", fun () -> published) |> ignore
                        events.Add "create"
                        if failCreation then raise creationFailure
                        ref 42),
                    (fun _ _ -> failwith "Failed creation must not process input."),
                    dispose = (fun _ -> events.Add "dispose final")
                )
            let definition = ScriptPipelineDefinition([| successful |], [| successful; failing |])
            let! error =
                Assert.ThrowsAnyAsync<Exception>(fun () ->
                    definition.CreateProcessorPipelinesAsync(connection).AsTask() :> Task)
            if cleanupFails then
                let aggregate = Assert.IsType<AggregateException>(error)
                aggregate.InnerExceptions.Count |> should equal 3
                assertContext "failing.fsx" "server-to-client" 2 creationFailure aggregate.InnerExceptions[0]
                assertContext "successful.fsx" "server-to-client" 1 cleanupFailure aggregate.InnerExceptions[1]
                assertContext "successful.fsx" "client-to-server" 1 cleanupFailure aggregate.InnerExceptions[2]
            else
                assertContext "failing.fsx" "server-to-client" 2 creationFailure error
            events.ToArray() |> should equal [| "create"; "dispose 1"; "dispose 0" |]
            Assert.Same(created[1], cleaned[0])
            Assert.Same(created[0], cleaned[1])
            Assert.Same(published, connection.TryGet<Resource>("owned-before-failure").Value)
            published.AsyncDisposals |> should equal 0

            // A failed instance does not invalidate the reusable definition.
            failCreation <- false
            let! processors = definition.CreateProcessorPipelinesAsync(connection).AsTask()
            created.Count |> should equal 4
            let! failures =
                ProcessorLifetime.disposeAllAsync (Array.append processors.ClientToBackend processors.BackendToClient)
                |> _.AsTask()
            failures.Length |> should equal (if cleanupFails then 2 else 0)
            events.ToArray()
            |> should equal [| "create"; "dispose 1"; "dispose 0"; "create"; "dispose final"; "dispose 3"; "dispose 2" |]
            do! (connection :> IAsyncDisposable).DisposeAsync().AsTask()
            published.AsyncDisposals |> should equal 1
            published.SyncDisposals |> should equal 0
        }

    [<Theory>]
    [<InlineData("failure", false)>]
    [<InlineData("failure", true)>]
    [<InlineData("cancellation", false)>]
    [<InlineData("cancellation", true)>]
    let ``handler termination retains its error and cleanup attempts every stage once`` outcome cleanupFails =
        task {
            use connection = new ConnectionState()
            use cancellation = new CancellationTokenSource()
            let handlerFailure = InvalidOperationException("handle failed")
            let cleanupFailure = InvalidOperationException("dispose failed")
            let created = ResizeArray<int ref>()
            let disposed = ResizeArray<int ref>()
            let stage =
                ConfiguredScriptStage.CreateFunctional(
                    "termination.fsx",
                    (fun () ->
                        let state = ref created.Count
                        created.Add state
                        state),
                    (fun ctx state ->
                        Assert.Same(created[0], state)
                        if outcome = "cancellation" then
                            cancellation.Cancel()
                            ctx.CancellationToken.ThrowIfCancellationRequested()
                        raise handlerFailure),
                    dispose = (fun state ->
                        disposed.Add state
                        if cleanupFails then raise cleanupFailure)
                )
            let! processors =
                ScriptPipelineDefinition([| stage; stage |], Array.empty)
                    .CreateProcessorPipelinesAsync(connection).AsTask()
            use executor = pipeline processors.ClientToBackend
            let owner = new Owner()
            let! error =
                Assert.ThrowsAnyAsync<Exception>(fun () -> executor.PushAsync(owner, 1, cancellation.Token) :> Task)
            if outcome = "cancellation" then
                let cancelled = Assert.IsAssignableFrom<OperationCanceledException>(error)
                cancelled.CancellationToken |> should equal cancellation.Token
            else
                assertContext "termination.fsx" "client-to-server" 1 handlerFailure error
            owner.Disposals |> should equal 1
            executor.GetLogicalCapacitySnapshot().CurrentBytes |> should equal 0L
            executor.GetConnectionMemorySnapshot().CurrentPhysicalBufferCapacityBytes |> should equal 0L
            if cleanupFails then
                let! aggregate = Assert.ThrowsAsync<AggregateException>(fun () -> dispose executor)
                aggregate.InnerExceptions.Count |> should equal 2
                assertContext "termination.fsx" "client-to-server" 2 cleanupFailure aggregate.InnerExceptions[0]
                assertContext "termination.fsx" "client-to-server" 1 cleanupFailure aggregate.InnerExceptions[1]
            else
                do! dispose executor
            do! dispose executor
            disposed.Count |> should equal 2
            Assert.Same(created[1], disposed[0])
            Assert.Same(created[0], disposed[1])
        }

    [<Fact>]
    let ``successful processing propagates a single synchronous cleanup failure with context`` () =
        task {
            use connection = new ConnectionState()
            let failure = InvalidOperationException("dispose failed")
            let stage =
                ConfiguredScriptStage.CreateFunctional(
                    "dispose.fsx", (fun () -> ref 0), (fun _ _ -> releaseAll),
                    dispose = (fun _ -> raise failure)
                )
            let! processors =
                ScriptPipelineDefinition(Array.empty, [| stage |])
                    .CreateProcessorPipelinesAsync(connection).AsTask()
            use executor = pipeline processors.BackendToClient
            do! executor.CompleteAsync CancellationToken.None
            let! error = Assert.ThrowsAsync<ScriptProcessorFailureException>(fun () -> dispose executor)
            assertContext "dispose.fsx" "server-to-client" 1 failure error
            do! dispose executor
        }
