namespace PipeL4.Tests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open Xunit

module ConnectionStateTests =
    type private SyncDisposable(name: string, events: ResizeArray<string>, failure: exn option) =
        interface IDisposable with
            member _.Dispose() =
                events.Add($"sync:{name}")

                match failure with
                | Some error -> raise error
                | None -> ()

    type private BothDisposable(name: string, events: ResizeArray<string>, failure: exn option) =
        let mutable syncDisposals = 0
        let mutable asyncDisposals = 0

        member _.SyncDisposals = Volatile.Read(&syncDisposals)
        member _.AsyncDisposals = Volatile.Read(&asyncDisposals)

        interface IDisposable with
            member _.Dispose() =
                Interlocked.Increment(&syncDisposals) |> ignore
                events.Add($"sync:{name}")

        interface IAsyncDisposable with
            member _.DisposeAsync() =
                Interlocked.Increment(&asyncDisposals) |> ignore
                events.Add($"async:{name}")

                match failure with
                | Some error -> ValueTask(Task.FromException error)
                | None -> ValueTask()

    type private GatedAsyncDisposable
        (
            started: TaskCompletionSource<unit>,
            release: TaskCompletionSource<unit>,
            completed: TaskCompletionSource<unit>
        ) =
        interface IAsyncDisposable with
            member _.DisposeAsync() =
                ValueTask(
                    task {
                        started.TrySetResult(()) |> ignore
                        do! release.Task
                        completed.TrySetResult(()) |> ignore
                    }
                )

    let private dispose (state: ConnectionState) =
        (state :> IAsyncDisposable).DisposeAsync().AsTask()

    [<Fact>]
    let ``same typed key returns the published instance`` () =
        task {
            let state = ConnectionState()
            let expected = obj()
            let mutable factoryCalls = 0

            let first =
                state.GetOrAdd(
                    "shared",
                    fun () ->
                        Interlocked.Increment(&factoryCalls) |> ignore
                        expected
                )

            let second = state.GetOrAdd("shared", fun () -> obj())
            let found = state.TryGet<obj>("shared")

            Object.ReferenceEquals(expected, first) |> should equal true
            Object.ReferenceEquals(first, second) |> should equal true
            found.IsSome |> should equal true
            Object.ReferenceEquals(expected, found.Value) |> should equal true
            factoryCalls |> should equal 1
            do! dispose state
        }

    [<Fact>]
    let ``keys support different types and typed null values`` () =
        task {
            let state = ConnectionState()
            state.GetOrAdd("number", fun () -> 42) |> should equal 42
            state.GetOrAdd<string>("nullable", fun () -> Unchecked.defaultof<string>)
            |> should be Null

            state.TryGet<int>("number") |> should equal (Some 42)

            match state.TryGet<string>("nullable") with
            | Some value -> value |> should be Null
            | None -> failwith "The typed null value was not found."

            do! dispose state
        }

    [<Fact>]
    let ``type conflicts fail without running the competing factory`` () =
        task {
            let state = ConnectionState()
            state.GetOrAdd("typed", fun () -> 42) |> ignore
            let mutable competingCalls = 0

            let getError =
                Assert.Throws<InvalidOperationException>(fun () ->
                    state.GetOrAdd(
                        "typed",
                        fun () ->
                            Interlocked.Increment(&competingCalls) |> ignore
                            "wrong"
                    )
                    |> ignore)

            let tryGetError =
                Assert.Throws<InvalidOperationException>(fun () ->
                    state.TryGet<string>("typed") |> ignore)

            getError.Message.Contains("typed", StringComparison.Ordinal) |> should equal true
            tryGetError.Message.Contains("System.Int32", StringComparison.Ordinal)
            |> should equal true

            competingCalls |> should equal 0
            do! dispose state
        }

    [<Fact>]
    let ``missing and invalid keys are handled consistently`` () =
        task {
            let state = ConnectionState()
            state.TryGet<obj>("missing") |> should equal None

            Assert.Throws<ArgumentNullException>(fun () ->
                state.TryGet<obj>(Unchecked.defaultof<string>) |> ignore)
            |> ignore

            for key in [| ""; "   " |] do
                Assert.Throws<ArgumentException>(fun () ->
                    state.GetOrAdd(key, fun () -> obj()) |> ignore)
                |> ignore

            let nullFactory = Unchecked.defaultof<unit -> obj>

            Assert.Throws<ArgumentNullException>(fun () ->
                state.GetOrAdd("key", nullFactory) |> ignore)
            |> ignore

            do! dispose state
        }

    [<Fact>]
    let ``concurrent first access executes one factory and publishes one value`` () =
        task {
            let state = ConnectionState()
            let start = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let events = ResizeArray<string>()
            let expected = new BothDisposable("shared", events, None)
            let mutable factoryCalls = 0

            let operations =
                Array.init 32 (fun _ ->
                    task {
                        do! start.Task

                        return
                            state.GetOrAdd(
                                "shared",
                                fun () ->
                                    Interlocked.Increment(&factoryCalls) |> ignore
                                    expected
                            )
                    })

            start.TrySetResult(()) |> ignore
            let! values = Task.WhenAll operations
            factoryCalls |> should equal 1
            values |> Array.forall (fun value -> Object.ReferenceEquals(expected, value))
            |> should equal true

            let retrievals =
                Array.init 32 (fun index ->
                    task {
                        if index % 2 = 0 then
                            return
                                state.GetOrAdd(
                                    "shared",
                                    fun () -> failwith "The published value should be reused."
                                )
                        else
                            return
                                state.TryGet<BothDisposable>("shared")
                                |> Option.defaultWith (fun () -> failwith "The value was missing.")
                    })

            let! retrieved = Task.WhenAll retrievals

            retrieved
            |> Array.forall (fun value -> Object.ReferenceEquals(expected, value))
            |> should equal true

            do! dispose state
            expected.AsyncDisposals |> should equal 1
            expected.SyncDisposals |> should equal 0
        }

    [<Fact>]
    let ``concurrent incompatible access does not run the losing factory`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use release = new ManualResetEventSlim(false)
            let state = ConnectionState()
            let winningStarted =
                TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let mutable winningCalls = 0
            let mutable losingCalls = 0

            let winning =
                Task.Run(fun () ->
                    state.GetOrAdd(
                        "typed-race",
                        fun () ->
                            Interlocked.Increment(&winningCalls) |> ignore
                            winningStarted.TrySetResult(()) |> ignore
                            release.Wait(timeout.Token)
                            42
                    ))

            try
                do! winningStarted.Task.WaitAsync(timeout.Token)

                let losing =
                    Task.Run(fun () ->
                        state.GetOrAdd(
                            "typed-race",
                            fun () ->
                                Interlocked.Increment(&losingCalls) |> ignore
                                "wrong"
                        ))

                release.Set()
                let! value = winning.WaitAsync(timeout.Token)
                value |> should equal 42

                let! error =
                    Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                        losing.WaitAsync(timeout.Token) :> Task)

                error.Message.Contains("typed-race", StringComparison.Ordinal)
                |> should equal true

                winningCalls |> should equal 1
                losingCalls |> should equal 0
                state.TryGet<int>("typed-race") |> should equal (Some 42)
                do! dispose state
            finally
                release.Set()
        }

    [<Fact>]
    let ``concurrent factory failures are cached and never published`` () =
        task {
            let state = ConnectionState()
            let start = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let expected = InvalidOperationException("creation failed")
            let mutable factoryCalls = 0

            let operations =
                Array.init 32 (fun _ ->
                    task {
                        do! start.Task

                        return
                            Assert.Throws<InvalidOperationException>(fun () ->
                                state.GetOrAdd<obj>(
                                    "failed",
                                    fun () ->
                                        Interlocked.Increment(&factoryCalls) |> ignore
                                        raise expected
                                )
                                |> ignore)
                    })

            start.TrySetResult(()) |> ignore
            let! observed = Task.WhenAll operations

            observed
            |> Array.forall (fun error -> Object.ReferenceEquals(expected, error))
            |> should equal true

            factoryCalls |> should equal 1

            let subsequentGet =
                Assert.Throws<InvalidOperationException>(fun () ->
                    state.GetOrAdd<obj>("failed", fun () -> obj()) |> ignore)

            let subsequentTryGet =
                Assert.Throws<InvalidOperationException>(fun () ->
                    state.TryGet<obj>("failed") |> ignore)

            Object.ReferenceEquals(expected, subsequentGet) |> should equal true
            Object.ReferenceEquals(expected, subsequentTryGet) |> should equal true
            state.GetOrAdd("unrelated", fun () -> 7) |> should equal 7
            do! dispose state
        }

    [<Fact>]
    let ``different keys initialize concurrently`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use releaseFirst = new ManualResetEventSlim(false)
            let state = ConnectionState()
            let firstStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let secondStarted = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let first =
                Task.Run(fun () ->
                    state.GetOrAdd(
                        "first",
                        fun () ->
                            firstStarted.TrySetResult(()) |> ignore
                            releaseFirst.Wait(timeout.Token)
                            obj()
                    ))

            do! firstStarted.Task.WaitAsync(timeout.Token)

            let second =
                Task.Run(fun () ->
                    state.GetOrAdd(
                        "second",
                        fun () ->
                            secondStarted.TrySetResult(()) |> ignore
                            obj()
                    ))

            do! secondStarted.Task.WaitAsync(timeout.Token)
            let! _ = second.WaitAsync(timeout.Token)
            releaseFirst.Set()
            let! _ = first.WaitAsync(timeout.Token)
            do! dispose state
        }

    [<Fact>]
    let ``disposal drains successful initialization and rejects later access`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use release = new ManualResetEventSlim(false)
            let state = ConnectionState()
            let events = ResizeArray<string>()
            let value = new BothDisposable("owned", events, None)
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let initialization =
                Task.Run(fun () ->
                    state.GetOrAdd(
                        "owned",
                        fun () ->
                            started.TrySetResult(()) |> ignore
                            release.Wait(timeout.Token)
                            value
                    ))

            try
                do! started.Task.WaitAsync(timeout.Token)
                let disposal = dispose state
                disposal.IsCompleted |> should equal false

                Assert.Throws<ObjectDisposedException>(fun () ->
                    state.TryGet<BothDisposable>("owned") |> ignore)
                |> ignore

                let mutable lateFactoryCalls = 0

                Assert.Throws<ObjectDisposedException>(fun () ->
                    state.GetOrAdd(
                        "late",
                        fun () ->
                            Interlocked.Increment(&lateFactoryCalls) |> ignore
                            obj()
                    )
                    |> ignore)
                |> ignore

                lateFactoryCalls |> should equal 0
                release.Set()
                let! publishedValue = initialization.WaitAsync(timeout.Token)
                Object.ReferenceEquals(value, publishedValue) |> should equal true
                do! disposal.WaitAsync(timeout.Token)
                value.AsyncDisposals |> should equal 1
                value.SyncDisposals |> should equal 0
            finally
                release.Set()
        }

    [<Fact>]
    let ``disposal drains failed initialization without publishing a value`` () =
        task {
            use timeout = new CancellationTokenSource(10_000)
            use release = new ManualResetEventSlim(false)
            let state = ConnectionState()
            let expected = InvalidOperationException("initialization failed")
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            let initialization =
                Task.Run(fun () ->
                    state.GetOrAdd<obj>(
                        "failed",
                        fun () ->
                            started.TrySetResult(()) |> ignore
                            release.Wait(timeout.Token)
                            raise expected
                    ))

            try
                do! started.Task.WaitAsync(timeout.Token)
                let disposal = dispose state
                disposal.IsCompleted |> should equal false

                Assert.Throws<ObjectDisposedException>(fun () ->
                    state.TryGet<obj>("failed") |> ignore)
                |> ignore

                release.Set()

                let! observed =
                    Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                        initialization.WaitAsync(timeout.Token) :> Task)

                Object.ReferenceEquals(expected, observed) |> should equal true
                do! disposal.WaitAsync(timeout.Token)

                Assert.Throws<ObjectDisposedException>(fun () ->
                    state.GetOrAdd("failed", fun () -> obj()) |> ignore)
                |> ignore
            finally
                release.Set()
        }

    [<Fact>]
    let ``disposal is reverse ordered async preferred and reference deduplicated`` () =
        task {
            let events = ResizeArray<string>()
            let state = ConnectionState()
            let first = new BothDisposable("first", events, None)
            let second = new SyncDisposable("second", events, None)
            let third = new BothDisposable("third", events, None)

            state.GetOrAdd("first", fun () -> first) |> ignore
            state.GetOrAdd("second", fun () -> second) |> ignore
            state.GetOrAdd("first-alias", fun () -> first) |> ignore
            state.GetOrAdd("third", fun () -> third) |> ignore

            do! dispose state
            do! dispose state

            events
            |> Seq.toArray
            |> should equal [| "async:third"; "async:first"; "sync:second" |]

            first.AsyncDisposals |> should equal 1
            first.SyncDisposals |> should equal 0
            third.AsyncDisposals |> should equal 1
            third.SyncDisposals |> should equal 0
        }

    [<Fact>]
    let ``disposal awaits asynchronous values`` () =
        task {
            let state = ConnectionState()
            let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            let completed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
            state.GetOrAdd("async", fun () -> GatedAsyncDisposable(started, release, completed))
            |> ignore

            let operation = dispose state
            let repeated = dispose state
            Object.ReferenceEquals(operation, repeated) |> should equal true
            do! started.Task
            operation.IsCompleted |> should equal false
            completed.Task.IsCompleted |> should equal false

            Assert.Throws<ObjectDisposedException>(fun () ->
                state.TryGet<GatedAsyncDisposable>("async") |> ignore)
            |> ignore

            let mutable factoryCalls = 0

            Assert.Throws<ObjectDisposedException>(fun () ->
                state.GetOrAdd(
                    "late",
                    fun () ->
                        Interlocked.Increment(&factoryCalls) |> ignore
                        obj()
                )
                |> ignore)
            |> ignore

            factoryCalls |> should equal 0
            release.TrySetResult(()) |> ignore
            do! Task.WhenAll(operation, repeated)
            completed.Task.IsCompletedSuccessfully |> should equal true
        }

    [<Fact>]
    let ``disposal continues after failures and aggregates them`` () =
        task {
            let events = ResizeArray<string>()
            let state = ConnectionState()
            let firstFailure = InvalidOperationException("first failed")
            let secondFailure = InvalidOperationException("second failed")
            state.GetOrAdd("first", fun () -> new SyncDisposable("first", events, Some firstFailure))
            |> ignore

            state.GetOrAdd("middle", fun () -> new SyncDisposable("middle", events, None))
            |> ignore

            state.GetOrAdd("second", fun () -> new BothDisposable("second", events, Some secondFailure))
            |> ignore

            let! error = Assert.ThrowsAsync<AggregateException>(fun () -> dispose state)
            error.InnerExceptions.Count |> should equal 2

            events
            |> Seq.toArray
            |> should equal [| "async:second"; "sync:middle"; "sync:first" |]
        }

    [<Fact>]
    let ``disposed state cannot be resurrected`` () =
        task {
            let state = ConnectionState()
            do! dispose state
            do! dispose state

            Assert.Throws<ObjectDisposedException>(fun () ->
                state.GetOrAdd("new", fun () -> obj()) |> ignore)
            |> ignore

            Assert.Throws<ObjectDisposedException>(fun () ->
                state.TryGet<obj>("new") |> ignore)
            |> ignore
        }
