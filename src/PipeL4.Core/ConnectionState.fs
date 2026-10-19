namespace PipeL4.Core

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks

[<NoEquality; NoComparison>]
type private ConnectionStateEntry =
    { DeclaredType: Type
      Value: objnull }

/// Runtime-owned typed state shared by all processor stages for one connection.
[<Sealed>]
type internal ConnectionState() =
    let entries =
        ConcurrentDictionary<string, Lazy<ConnectionStateEntry>> StringComparer.Ordinal

    let published = ConcurrentQueue<ConnectionStateEntry>()
    let lifetimeGate = obj()
    let mutable activeAccesses = 0
    let mutable disposalStarted = false
    let mutable drained: TaskCompletionSource<unit> option = None
    let mutable disposalTask: Task option = None

    let disposedError () =
        ObjectDisposedException(nameof ConnectionState)

    let validateKey key =
        if isNull key then
            nullArg (nameof key)

        if String.IsNullOrWhiteSpace key then
            invalidArg (nameof key) "A connection-state key cannot be empty or whitespace."

    let enterAccess () =
        lock lifetimeGate (fun () ->
            if disposalStarted then
                raise (disposedError ())

            activeAccesses <- activeAccesses + 1)

    let exitAccess () =
        let signal =
            lock lifetimeGate (fun () ->
                activeAccesses <- activeAccesses - 1

                if disposalStarted && activeAccesses = 0 then
                    drained
                else
                    None)

        match signal with
        | Some completion -> completion.TrySetResult(()) |> ignore
        | None -> ()

    let ensureType key requestedType (entry: ConnectionStateEntry) =
        if entry.DeclaredType <> requestedType then
            invalidOp
                $"Connection-state key '{key}' contains '{entry.DeclaredType}', not the requested type '{requestedType}'."

    let raisePreserving (error: exn) =
        ExceptionDispatchInfo.Capture(error).Throw()

    let disposeCore () =
        task {
            let snapshot = published.ToArray()
            entries.Clear()
            published.Clear()

            let failures = ResizeArray<exn>()
            let disposedReferences = HashSet<objnull> ReferenceEqualityComparer.Instance

            for index = snapshot.Length - 1 downto 0 do
                let value = snapshot[index].Value

                let shouldDispose =
                    if isNull value then
                        false
                    elif value.GetType().IsValueType then
                        true
                    else
                        disposedReferences.Add value

                if shouldDispose then
                    try
                        match value with
                        | :? IAsyncDisposable as disposable ->
                            do! disposable.DisposeAsync().AsTask()
                        | :? IDisposable as disposable -> disposable.Dispose()
                        | _ -> ()
                    with error ->
                        failures.Add error

            match failures.Count with
            | 0 -> ()
            | 1 -> raisePreserving failures[0]
            | _ ->
                raise (
                    AggregateException(
                        "Multiple connection-state values failed during disposal.",
                        failures
                    )
                )
        }

    let completeDisposal (waitForAccesses: Task) (completion: TaskCompletionSource<unit>) =
        task {
            try
                do! waitForAccesses
                do! disposeCore ()
                completion.TrySetResult(()) |> ignore
            with error ->
                completion.TrySetException(error) |> ignore
        }
        |> ignore

    let getDisposalTask () =
        let mutable start: (Task * TaskCompletionSource<unit>) option = None

        let operation =
            lock lifetimeGate (fun () ->
                match disposalTask with
                | Some operation -> operation
                | None ->
                    disposalStarted <- true

                    let waitForAccesses =
                        if activeAccesses = 0 then
                            Task.CompletedTask
                        else
                            let completion =
                                TaskCompletionSource<unit>
                                    TaskCreationOptions.RunContinuationsAsynchronously

                            drained <- Some completion
                            completion.Task :> Task

                    let completion =
                        TaskCompletionSource<unit>
                            TaskCreationOptions.RunContinuationsAsynchronously

                    let operation = completion.Task :> Task
                    disposalTask <- Some operation
                    start <- Some(waitForAccesses, completion)
                    operation)

        match start with
        | Some(waitForAccesses, completion) ->
            completeDisposal waitForAccesses completion
        | None -> ()

        operation

    member _.GetOrAdd<'T>(key: string, factory: unit -> 'T) =
        validateKey key

        if obj.ReferenceEquals(factory, null) then
            nullArg (nameof factory)

        enterAccess ()

        try
            let entry =
                match entries.TryGetValue key with
                | true, existing -> existing.Value
                | false, _ ->
                    let candidate =
                        Lazy<ConnectionStateEntry>(
                            (fun () ->
                                let entry =
                                    { DeclaredType = typeof<'T>
                                      Value = box (factory ()) }

                                published.Enqueue entry
                                entry),
                            LazyThreadSafetyMode.ExecutionAndPublication
                        )

                    entries.GetOrAdd(key, candidate).Value

            ensureType key typeof<'T> entry
            unbox<'T> entry.Value
        finally
            exitAccess ()

    member _.TryGet<'T>(key: string) =
        validateKey key
        enterAccess ()

        try
            match entries.TryGetValue key with
            | true, value ->
                let entry = value.Value
                ensureType key typeof<'T> entry
                Some(unbox<'T> entry.Value)
            | false, _ -> None
        finally
            exitAccess ()

    interface IAsyncDisposable with
        member _.DisposeAsync() = ValueTask(getDisposalTask ())
