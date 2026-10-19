namespace PipeL4.Plugins

open System
open System.Reflection
open System.Runtime.ExceptionServices
open System.Threading.Tasks

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

[<NoEquality; NoComparison>]
type internal ProcessorConstructor =
    | Parameterless of ConstructorInfo
    | ConnectionStateAware of ConstructorInfo

type internal CompiledScriptProcessor internal (assembly: Assembly, constructor: ProcessorConstructor) =
    member _.Assembly = assembly

    member _.ProcessorType =
        let constructorInfo =
            match constructor with
            | Parameterless value
            | ConnectionStateAware value -> value

        match constructorInfo.DeclaringType with
        | null -> invalidOp "The discovered processor constructor has no declaring type."
        | processorType -> processorType

    member _.CreateProcessor(connectionState: IConnectionState) : PipeL4.Core.IByteStreamProcessor =
        if obj.ReferenceEquals(connectionState, null) then
            nullArg (nameof connectionState)

        let instance =
            try
                match constructor with
                | Parameterless value -> value.Invoke(Array.empty)
                | ConnectionStateAware value -> value.Invoke([| box connectionState |])
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

[<RequireQualifiedAccess>]
module internal ScriptProcessorDiscovery =
    let private displayName (processorType: Type) =
        processorType.FullName
        |> Option.ofObj
        |> Option.filter (String.IsNullOrWhiteSpace >> not)
        |> Option.defaultValue processorType.Name

    let discover (assembly: Assembly) =
        try
            let candidates =
                assembly.GetTypes()
                |> Array.choose (fun processorType ->
                    if
                        processorType.IsVisible
                        && processorType.IsClass
                        && not processorType.IsAbstract
                        && not processorType.ContainsGenericParameters
                        && typeof<IByteStreamProcessor>.IsAssignableFrom processorType
                    then
                        match processorType.GetConstructor([| typeof<IConnectionState> |]) with
                        | null ->
                            match processorType.GetConstructor(Type.EmptyTypes) with
                            | null -> None
                            | constructor -> Some(processorType, Parameterless constructor)
                        | constructor -> Some(processorType, ConnectionStateAware constructor)
                    else
                        None)
                |> Array.sortBy (fun (processorType, _) -> displayName processorType)

            match candidates with
            | [| _, constructor |] -> Ok(CompiledScriptProcessor(assembly, constructor))
            | [||] ->
                Error
                    "The script must define exactly one public, concrete, non-generic IByteStreamProcessor class with a public parameterless or IConnectionState constructor; no usable implementation was found."
            | values ->
                let names =
                    values
                    |> Array.map (fun (processorType, _) -> displayName processorType)
                    |> String.concat ", "

                Error
                    $"The script must define exactly one usable IByteStreamProcessor implementation, but multiple implementations were found: {names}."
        with :? ReflectionTypeLoadException as error ->
            let details =
                error.LoaderExceptions
                |> Array.choose Option.ofObj
                |> Array.map _.Message
                |> Array.distinct
                |> String.concat "; "

            if String.IsNullOrWhiteSpace details then
                Error "The compiled script types could not be inspected for a processor implementation."
            else
                Error $"The compiled script types could not be inspected for a processor implementation: {details}"
