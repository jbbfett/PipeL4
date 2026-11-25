namespace PipeL4.PluginHost

open System
open System.Linq.Expressions
open System.Reflection
open System.Runtime.ExceptionServices
open System.Threading.Tasks
open Microsoft.FSharp.Reflection
open PipeL4.Plugins

/// One source-level callable; CLR argument groups may erase unit or flatten reference tuples.
[<NoEquality; NoComparison>]
type private FunctionalDefinition =
    { Name: string
      Member: MemberInfo
      IsFunctionValue: bool
      Groups: Type array array option
      ResultType: Type }

    member this.Method =
        match this.Member with
        | :? MethodInfo as method -> method
        | _ -> invalidOp "A nonfunction definition cannot be bound."

[<RequireQualifiedAccess>]
module private FunctionalCallable =
    let private unitType (value: Type) = if value = typeof<Void> then typeof<unit> else value

    let describe name isFunctionValue (method: MethodInfo) =
        let parameters = method.GetParameters() |> Array.map _.ParameterType
        let groups, result =
            if method.ContainsGenericParameters then
                None, method.ReturnType
            elif isFunctionValue then
                if parameters.Length <> 0 || not (FSharpType.IsFunction method.ReturnType) then
                    None, method.ReturnType
                else
                    let argument, result = FSharpType.GetFunctionElements method.ReturnType
                    if name = "handle" && FSharpType.IsFunction result then
                        let state, result = FSharpType.GetFunctionElements result
                        Some [| [| argument |]; [| state |] |], result
                    else
                        Some [| [| argument |] |], result
            else
                match method.GetCustomAttribute<CompilationArgumentCountsAttribute>() |> Option.ofObj with
                | None -> Some [| parameters |], method.ReturnType
                | Some attribute ->
                    let counts = Seq.toArray attribute.Counts
                    if counts.Length = 0 || Array.exists (fun n -> n < 0) counts || Array.sum counts <> parameters.Length then
                        None, method.ReturnType
                    else
                        let mutable offset = 0
                        let groups = counts |> Array.map (fun count ->
                            let group = Array.sub parameters offset count
                            offset <- offset + count
                            group)
                        Some groups, method.ReturnType
        let groups, result =
            match groups with
            | Some [| context |] when name = "handle" && not isFunctionValue && FSharpType.IsFunction result ->
                let state, result = FSharpType.GetFunctionElements result
                Some [| context; [| state |] |], result
            | _ -> groups, result
        { Name = name
          Member = method
          IsFunctionValue = isFunctionValue
          Groups = groups
          ResultType = unitType result }

    let matches (expected: Type) (group: Type array) =
        match group with
        | [||] -> expected = typeof<unit>
        | [| actual |] -> actual = expected
        | _ -> FSharpType.IsTuple expected && not expected.IsValueType && FSharpType.GetTupleElements expected = group

    // These expression trees are compiled once. Tuple fields are read directly from the
    // original typed state; there are no object arrays, reflection or casts during invocation.
    let rec private tupleElement (value: Expression) index : Expression =
        if index < 7 then
            Expression.PropertyOrField(value, $"Item{index + 1}")
        else
            tupleElement (Expression.PropertyOrField(value, "Rest")) (index - 7)

    let private methodBody definition (arguments: ParameterExpression array) resultType =
        let groups = definition.Groups |> Option.defaultWith (fun () -> invalidOp "Expected a validated callable.")
        let parameters =
            Array.map2 (fun group (argument: ParameterExpression) ->
                match group with
                | [||] -> Array.empty
                | [| _ |] -> [| argument :> Expression |]
                | values -> values |> Array.mapi (fun index _ -> tupleElement argument index)) groups arguments
            |> Array.concat
        let call = Expression.Call(definition.Method, parameters) :> Expression
        if definition.Method.ReturnType = typeof<Void> then
            Expression.Block(call, Expression.Default(resultType)) :> Expression
        else
            call

    let unary<'Argument, 'Result> definition : 'Argument -> 'Result =
        if definition.IsFunctionValue then
            // Fetch the function value once; this does not call its create/handle/dispose body.
            definition.Method.CreateDelegate<Func<'Argument -> 'Result>>().Invoke()
        else
            let invoke =
                if (definition.Method.GetParameters() |> Array.map _.ParameterType) = [| typeof<'Argument> |]
                   && definition.Method.ReturnType = typeof<'Result> then
                    definition.Method.CreateDelegate<Func<'Argument, 'Result>>()
                else
                    let argument = Expression.Parameter(typeof<'Argument>, "argument")
                    Expression.Lambda<Func<'Argument, 'Result>>(
                        methodBody definition [| argument |] typeof<'Result>, argument).Compile()
            fun argument -> invoke.Invoke(argument)

    let binary<'First, 'Second, 'Result> definition : 'First -> 'Second -> 'Result =
        if definition.IsFunctionValue then
            definition.Method.CreateDelegate<Func<'First -> 'Second -> 'Result>>().Invoke()
        elif definition.Method.ReturnType = typeof<'Second -> 'Result> then
            let invoke = definition.Method.CreateDelegate<Func<'First, 'Second -> 'Result>>()
            // The returned function can capture this invocation's context. Only the method
            // delegate is cached; evaluating it and applying state belong to processing.
            fun first second -> invoke.Invoke(first) second
        else
            let invoke =
                if (definition.Method.GetParameters() |> Array.map _.ParameterType) = [| typeof<'First>; typeof<'Second> |] then
                    definition.Method.CreateDelegate<Func<'First, 'Second, 'Result>>()
                else
                    let first = Expression.Parameter(typeof<'First>, "first")
                    let second = Expression.Parameter(typeof<'Second>, "second")
                    Expression.Lambda<Func<'First, 'Second, 'Result>>(
                        methodBody definition [| first; second |] typeof<'Result>, first, second).Compile()
            fun first second -> invoke.Invoke(first, second)

// Reflection closes this method once at load time. State remains strongly typed in the factory,
// the existing adapter, and every handler and explicitly supplied cleanup invocation.
type private FunctionalScriptBinding =
    static member BindState<'State>(create: FunctionalDefinition, handle: FunctionalDefinition, dispose: FunctionalDefinition option) =
        let createState: CreateContext -> 'State =
            match create.Groups with
            | Some [| group |] when FunctionalCallable.matches typeof<unit> group ->
                let invoke = FunctionalCallable.unary<unit, 'State> create
                fun _ -> invoke ()
            | _ -> FunctionalCallable.unary<CreateContext, 'State> create

        let handler =
            if handle.ResultType = typeof<ProcessorResult> then
                FunctionalHandler<'State>.Create(FunctionalCallable.binary<ProcessContext, 'State, ProcessorResult> handle)
            elif handle.ResultType = typeof<Task<ProcessorResult>> then
                FunctionalHandler<'State>.Create(FunctionalCallable.binary<ProcessContext, 'State, Task<ProcessorResult>> handle)
            else
                FunctionalHandler<'State>.Create(FunctionalCallable.binary<ProcessContext, 'State, ValueTask<ProcessorResult>> handle)

        let cleanup =
            dispose |> Option.map (fun definition ->
                if definition.ResultType = typeof<unit> then
                    FunctionalDisposal<'State>.Create(FunctionalCallable.unary<'State, unit> definition)
                elif definition.ResultType = typeof<Task<unit>> then
                    FunctionalDisposal<'State>.Create(FunctionalCallable.unary<'State, Task<unit>> definition)
                elif definition.ResultType = typeof<ValueTask> then
                    FunctionalDisposal<'State>.Create(FunctionalCallable.unary<'State, ValueTask> definition)
                else
                    FunctionalDisposal<'State>.Create(FunctionalCallable.unary<'State, ValueTask<unit>> definition))

        fun connectionState ->
            new FunctionalProcessorAdapter<'State>(createState, handler, cleanup, connectionState)
            :> PipeL4.Core.IByteStreamProcessor

[<RequireQualifiedAccess>]
module private FunctionalScriptDiscovery =
    let private flags = BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.DeclaredOnly

    let private sourceName (memberInfo: MemberInfo) =
        match memberInfo.GetCustomAttribute<CompilationSourceNameAttribute>() |> Option.ofObj with
        | Some attribute -> attribute.SourceName
        | None -> memberInfo.Name

    let private signature definition = $"{definition.Member.DeclaringType}.{definition.Member}"
    let private expectedHandle = "handle: ProcessContext -> ProcessorResult | Task<ProcessorResult> | ValueTask<ProcessorResult>, optionally with a second curried 'State argument and synchronous create: unit | CreateContext -> 'State"
    let private expectedCreate = "create: unit -> 'State or CreateContext -> 'State (synchronous)"
    let private expectedDispose = "dispose: 'State -> unit | Task<unit> | ValueTask | ValueTask<unit>"
    let private invalid name expected definition =
        Error $"Invalid functional '{name}'; expected {expected}; actual {signature definition}."

    let private declaringType definition =
        definition.Member.DeclaringType |> Option.ofObj
        |> Option.defaultWith (fun () -> invalidOp "The discovered function has no declaring module.")

    let private asyncState (stateType: Type) =
        typeof<Task>.IsAssignableFrom stateType
        || stateType = typeof<ValueTask>
        || (stateType.IsGenericType
            && (stateType.GetGenericTypeDefinition() = typedefof<ValueTask<_>>
                || stateType.GetGenericTypeDefinition() = typedefof<Async<_>>))

    let private bindStateless handle =
        if handle.ResultType = typeof<ProcessorResult> then
            let invoke = FunctionalCallable.unary<ProcessContext, ProcessorResult> handle
            fun state -> FunctionalProcessorAdapter(invoke, state) :> PipeL4.Core.IByteStreamProcessor
        elif handle.ResultType = typeof<Task<ProcessorResult>> then
            let invoke = FunctionalCallable.unary<ProcessContext, Task<ProcessorResult>> handle
            fun state -> FunctionalProcessorAdapter(invoke, state) :> PipeL4.Core.IByteStreamProcessor
        else
            let invoke = FunctionalCallable.unary<ProcessContext, ValueTask<ProcessorResult>> handle
            fun state -> FunctionalProcessorAdapter(invoke, state) :> PipeL4.Core.IByteStreamProcessor

    let private bindStateful stateType create handle dispose =
        let binding =
            let method =
                typeof<FunctionalScriptBinding>.GetMethod("BindState", BindingFlags.Static ||| BindingFlags.Public ||| BindingFlags.NonPublic)
                |> Option.ofObj
                |> Option.defaultWith (fun () -> invalidOp "The typed functional binding method is missing.")
            method.MakeGenericMethod([| stateType |])
        try
            match binding.Invoke(null, [| box create; box handle; box dispose |]) with
            | null -> invalidOp "The typed functional binding returned no factory."
            | factory -> factory :?> (IConnectionState -> PipeL4.Core.IByteStreamProcessor)
        with :? TargetInvocationException as error ->
            match Option.ofObj error.InnerException with
            | Some inner ->
                ExceptionDispatchInfo.Capture(inner).Throw()
                Unchecked.defaultof<_>
            | None -> raise error

    let private validate handle create dispose =
        let validResult = List.contains handle.ResultType [ typeof<ProcessorResult>; typeof<Task<ProcessorResult>>; typeof<ValueTask<ProcessorResult>> ]
        match handle.Groups with
        | Some groups when validResult && (groups.Length = 1 || groups.Length = 2)
                           && FunctionalCallable.matches typeof<ProcessContext> groups[0] ->
            match create, dispose, groups.Length with
            | None, Some cleanup, _ -> invalid "dispose" (expectedDispose + "; requires create") cleanup
            | None, None, 1 -> Ok(bindStateless handle)
            | None, None, _ -> invalid "handle" (expectedHandle + "; stateful handle requires create") handle
            | Some creation, _, 1 -> invalid "create" (expectedCreate + "; requires stateful handle") creation
            | Some creation, cleanup, _ ->
                let stateType = creation.ResultType
                if stateType.IsByRef || stateType.IsByRefLike || stateType.IsPointer then
                    invalid "handle" (expectedHandle + "; 'State must be storable in a typed adapter") handle
                elif asyncState stateType
                     || (match creation.Groups with
                         | Some [| group |] -> not (FunctionalCallable.matches typeof<unit> group || FunctionalCallable.matches typeof<CreateContext> group)
                         | _ -> true)
                     || not (FunctionalCallable.matches stateType groups[1]) then
                    invalid "create" (expectedCreate + "; 'State must match handle") creation
                else
                    match cleanup with
                    | Some definition when
                        (match definition.Groups with
                         | Some [| group |] -> not (FunctionalCallable.matches stateType group)
                         | _ -> true)
                        || not (List.contains definition.ResultType [ typeof<unit>; typeof<Task<unit>>; typeof<ValueTask>; typeof<ValueTask<unit>> ]) ->
                        invalid "dispose" ($"{expectedDispose}; 'State must be {stateType}") definition
                    | _ -> Ok(bindStateful stateType creation handle cleanup)
        | _ -> invalid "handle" expectedHandle handle

    let discover assembly (types: Type array) =
        let isEntryPoint name = name = "handle" || name = "create" || name = "dispose"
        // Module/property metadata identifies definitions without generated module or getter names.
        // Only this script's top-level modules participate; nested modules remain helpers.
        let definitions =
            types
            |> Array.filter (fun t -> not t.IsNested && FSharpType.IsModule t)
            |> Array.collect (fun t ->
                let methods =
                    t.GetMethods(flags) |> Array.choose (fun method ->
                        let name = sourceName method
                        if not method.IsSpecialName && isEntryPoint name then
                            Some(FunctionalCallable.describe name false method)
                        else None)
                let values =
                    t.GetProperties(flags) |> Array.choose (fun property ->
                        let name = sourceName property
                        match property.GetGetMethod(true) |> Option.ofObj with
                        | Some getter when isEntryPoint name -> Some(FunctionalCallable.describe name true getter)
                        | _ -> None)
                let constants =
                    t.GetFields(flags) |> Array.choose (fun field ->
                        let name = sourceName field
                        if field.IsLiteral && isEntryPoint name then
                            Some { Name = name; Member = field; IsFunctionValue = false; Groups = None; ResultType = field.FieldType }
                        else None)
                Array.concat [| methods; values; constants |])
            |> Array.sortBy signature
        let named name = definitions |> Array.filter (fun definition -> definition.Name = name)
        let handles, creates, disposes = named "handle", named "create", named "dispose"
        let ambiguous = [ "handle", handles; "create", creates; "dispose", disposes ] |> List.tryFind (fun (_, methods) -> methods.Length > 1)
        match ambiguous with
        | Some(name, methods) ->
            let actual = methods |> Array.map signature |> String.concat "; "
            Error $"Ambiguous functional '{name}'; expected one top-level {name} definition; actual {actual}."
        | None when handles.Length = 0 ->
            Error $"The script has no usable implementation of IByteStreamProcessor and is missing top-level functional 'handle'; expected {expectedHandle}."
        | None ->
            let handle = handles[0]
            if definitions |> Array.exists (fun definition -> declaringType definition <> declaringType handle) then
                let actual = definitions |> Array.map signature |> String.concat "; "
                Error $"Functional 'create' and 'dispose' must be in the same top-level module as 'handle'; expected {expectedHandle}; actual {actual}."
            else
                validate handle (Array.tryHead creates) (Array.tryHead disposes)
                |> Result.map (fun factory -> CompiledScriptProcessor(assembly, Functional(declaringType handle, factory)))

[<RequireQualifiedAccess>]
module internal ScriptProcessorDiscovery =
    let private displayName (processorType: Type) =
        processorType.FullName
        |> Option.ofObj
        |> Option.filter (String.IsNullOrWhiteSpace >> not)
        |> Option.defaultValue processorType.Name

    let discover (assembly: Assembly) =
        try
            let types = assembly.GetTypes()
            let candidates =
                types
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
            | [||] when not (types |> Array.exists (fun t -> not t.IsInterface && typeof<IByteStreamProcessor>.IsAssignableFrom t)) ->
                FunctionalScriptDiscovery.discover assembly types
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
