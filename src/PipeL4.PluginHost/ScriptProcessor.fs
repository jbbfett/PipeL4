namespace PipeL4.PluginHost

open System
open System.Reflection
open System.Runtime.ExceptionServices
open System.Threading.Tasks
open PipeL4.Plugins

type internal ConnectionStateAdapter(state: PipeL4.Core.ConnectionState) =
    do
        if obj.ReferenceEquals(state, null) then
            nullArg (nameof state)

    interface IConnectionState with
        member _.GetOrAdd<'T>(key, factory) = state.GetOrAdd<'T>(key, factory)
        member _.TryGet<'T>(key) = state.TryGet<'T>(key)

type private ProcessorOutputAdapter(output: PipeL4.Core.IProcessorOutput) =
    interface IProcessorOutput with
        member _.AppendInput(offset, length) = output.AppendInput(offset, length)
        member _.Advance(count) = output.Advance(count)
        member _.GetMemory(sizeHint) = output.GetMemory(sizeHint)
        member _.GetSpan(sizeHint) = output.GetSpan(sizeHint)

type internal ScriptProcessorAdapter(processor: IByteStreamProcessor) =
    do
        if obj.ReferenceEquals(processor, null) then
            nullArg (nameof processor)

    interface PipeL4.Core.IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            let adaptResult (result: ProcessorResult) : PipeL4.Core.ProcessorResult =
                { RetainedLength = result.RetainedLength }

            let scriptInput =
                { Bytes = input.Bytes
                  IsCompleted = input.IsCompleted }

            let scriptOutput = ProcessorOutputAdapter(output) :> IProcessorOutput
            let operation = processor.ProcessAsync(scriptInput, scriptOutput, cancellationToken)

            if operation.IsCompletedSuccessfully then
                ValueTask<PipeL4.Core.ProcessorResult>(adaptResult operation.Result)
            else
                ValueTask<PipeL4.Core.ProcessorResult>(
                    task {
                        let! result = operation.AsTask()
                        return adaptResult result
                    }
                )

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            match processor with
            | :? IAsyncDisposable as disposable -> disposable.DisposeAsync()
            | :? IDisposable as disposable ->
                disposable.Dispose()
                ValueTask()
            | _ -> ValueTask()

/// Construction-time normalization; the synchronous case never creates a task.
[<RequireQualifiedAccess; NoEquality; NoComparison>]
type internal FunctionalHandler<'State> =
    private
    | Synchronous of OptimizedClosures.FSharpFunc<ProcessContext, 'State, ProcessorResult>
    | Asynchronous of OptimizedClosures.FSharpFunc<ProcessContext, 'State, ValueTask<ProcessorResult>>

    static member Create(handle: ProcessContext -> 'State -> ProcessorResult) =
        if obj.ReferenceEquals(handle, null) then nullArg (nameof handle)
        Synchronous(OptimizedClosures.FSharpFunc<_, _, _>.Adapt handle)

    static member Create(handle: ProcessContext -> 'State -> ValueTask<ProcessorResult>) =
        if obj.ReferenceEquals(handle, null) then nullArg (nameof handle)
        Asynchronous(OptimizedClosures.FSharpFunc<_, _, _>.Adapt handle)

    static member Create(handle: ProcessContext -> 'State -> Task<ProcessorResult>) =
        if obj.ReferenceEquals(handle, null) then nullArg (nameof handle)
        let invoke = OptimizedClosures.FSharpFunc<_, _, _>.Adapt handle
        FunctionalHandler<'State>.Create(fun ctx state -> ValueTask<ProcessorResult>(invoke.Invoke(ctx, state)))

    member this.Invoke(context, state) =
        let adapt (result: ProcessorResult) : PipeL4.Core.ProcessorResult =
            { RetainedLength = result.RetainedLength }

        match this with
        | Synchronous invoke ->
            ValueTask<PipeL4.Core.ProcessorResult>(adapt (invoke.Invoke(context, state)))
        | Asynchronous invoke ->
            let operation = invoke.Invoke(context, state)
            if operation.IsCompletedSuccessfully then
                ValueTask<PipeL4.Core.ProcessorResult>(adapt operation.Result)
            else
                ValueTask<PipeL4.Core.ProcessorResult>(task {
                    let! result = operation.AsTask()
                    return adapt result
                })

/// Only explicitly supplied cleanup is normalized, once, to the existing async lifecycle.
type internal FunctionalDisposal<'State> private (invoke: 'State -> ValueTask) =
    member _.Invoke(state) = invoke state

    static member Create(dispose: 'State -> unit) =
        if obj.ReferenceEquals(dispose, null) then nullArg (nameof dispose)
        FunctionalDisposal(fun state -> dispose state; ValueTask())

    static member Create(dispose: 'State -> ValueTask) =
        if obj.ReferenceEquals(dispose, null) then nullArg (nameof dispose)
        FunctionalDisposal(dispose)

    static member Create(dispose: 'State -> Task<unit>) =
        if obj.ReferenceEquals(dispose, null) then nullArg (nameof dispose)
        FunctionalDisposal(fun state -> ValueTask(dispose state :> Task))

    static member Create(dispose: 'State -> ValueTask<unit>) =
        if obj.ReferenceEquals(dispose, null) then nullArg (nameof dispose)
        FunctionalDisposal(fun state -> ValueTask(task { do! dispose state }))

/// Bridges typed handlers to the same executor and writer used by class processors.
type internal FunctionalProcessorAdapter
    private (handle: FunctionalHandler<unit>, connectionState: IConnectionState) =
    do
        if obj.ReferenceEquals(handle, null) then
            nullArg (nameof handle)

        if obj.ReferenceEquals(connectionState, null) then
            nullArg (nameof connectionState)

    new(handle: ProcessContext -> ProcessorResult, connectionState) =
        if obj.ReferenceEquals(handle, null) then nullArg (nameof handle)
        FunctionalProcessorAdapter(FunctionalHandler<unit>.Create(fun ctx () -> handle ctx), connectionState)

    new(handle: ProcessContext -> Task<ProcessorResult>, connectionState) =
        if obj.ReferenceEquals(handle, null) then nullArg (nameof handle)
        FunctionalProcessorAdapter(FunctionalHandler<unit>.Create(fun ctx () -> handle ctx), connectionState)

    new(handle: ProcessContext -> ValueTask<ProcessorResult>, connectionState) =
        if obj.ReferenceEquals(handle, null) then nullArg (nameof handle)
        FunctionalProcessorAdapter(FunctionalHandler<unit>.Create(fun ctx () -> handle ctx), connectionState)

    interface PipeL4.Core.IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            let context =
                { Input = input.Bytes
                  IsCompleted = input.IsCompleted
                  Output = ProcessorOutputAdapter(output) :> IProcessorOutput
                  CancellationToken = cancellationToken
                  ConnectionState = connectionState }

            handle.Invoke(context, ())

/// Owns one eagerly created, strongly typed local state value. Only explicit user cleanup runs.
type internal FunctionalProcessorAdapter<'State>
    (
        create: CreateContext -> 'State,
        handle: FunctionalHandler<'State>,
        dispose: FunctionalDisposal<'State> option,
        connectionState: IConnectionState
    ) =
    do
        if obj.ReferenceEquals(create, null) then
            nullArg (nameof create)

        if obj.ReferenceEquals(handle, null) then
            nullArg (nameof handle)

        if obj.ReferenceEquals(connectionState, null) then
            nullArg (nameof connectionState)

        match dispose with
        | Some cleanup when obj.ReferenceEquals(cleanup, null) -> nullArg (nameof dispose)
        | _ -> ()

    let state = create { ConnectionState = connectionState }

    new(create, handle: ProcessContext -> 'State -> ProcessorResult, dispose: ('State -> unit) option, connectionState) =
        new FunctionalProcessorAdapter<'State>(
            create, FunctionalHandler<'State>.Create(handle),
            Option.map (fun (cleanup: 'State -> unit) -> FunctionalDisposal<'State>.Create(cleanup)) dispose, connectionState)

    interface PipeL4.Core.IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            let context =
                { Input = input.Bytes
                  IsCompleted = input.IsCompleted
                  Output = ProcessorOutputAdapter(output) :> IProcessorOutput
                  CancellationToken = cancellationToken
                  ConnectionState = connectionState }

            handle.Invoke(context, state)

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            match dispose with
            | Some cleanup -> cleanup.Invoke(state)
            | None -> ValueTask()

[<NoEquality; NoComparison>]
type internal ProcessorConstructor =
    | Parameterless of ConstructorInfo
    | ConnectionStateAware of ConstructorInfo
    | Functional of moduleType: Type * factory: (IConnectionState -> PipeL4.Core.IByteStreamProcessor)

type internal CompiledScriptProcessor internal (assembly: Assembly, constructor: ProcessorConstructor) =
    member _.Assembly = assembly

    member _.ProcessorType =
        match constructor with
        | Functional(moduleType, _) -> moduleType
        | Parameterless value
        | ConnectionStateAware value ->
            match value.DeclaringType with
            | null -> invalidOp "The discovered processor constructor has no declaring type."
            | processorType -> processorType

    member _.CreateProcessor(connectionState: IConnectionState) : PipeL4.Core.IByteStreamProcessor =
        if obj.ReferenceEquals(connectionState, null) then
            nullArg (nameof connectionState)

        match constructor with
        | Functional(_, factory) -> factory connectionState
        | _ ->
            let instance =
                try
                    match constructor with
                    | Parameterless value -> value.Invoke(Array.empty)
                    | ConnectionStateAware value -> value.Invoke([| box connectionState |])
                    | Functional _ -> invalidOp "Expected a class processor constructor."
                with :? TargetInvocationException as error ->
                    match Option.ofObj error.InnerException with
                    | Some innerError ->
                        ExceptionDispatchInfo.Capture(innerError).Throw()
                        Unchecked.defaultof<obj>
                    | None -> raise error

            let processor =
                match instance with
                | :? IByteStreamProcessor as value -> value
                | _ -> invalidOp "The discovered processor type did not create an IByteStreamProcessor instance."

            ScriptProcessorAdapter(processor) :> PipeL4.Core.IByteStreamProcessor
