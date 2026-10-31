namespace PipeL4.Plugins

open System
open System.Collections.Generic
open System.IO
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open PipeL4.Core

type internal ScriptPluginConfigurationException(message: string) =
    inherit Exception(message)

type internal ScriptProcessorFailureException(
    scriptPath: string,
    direction: string,
    stageNumber: int,
    innerException: exn
) =
    inherit Exception(
        $"F# script plugin '{scriptPath}' failed in {direction} stage {stageNumber}.",
        innerException
    )

    member _.ScriptPath = scriptPath
    member _.Direction = direction
    member _.StageNumber = stageNumber

[<RequireQualifiedAccess>]
module private ExceptionHelpers =
    let raisePreserving<'value> (error: exn) =
        ExceptionDispatchInfo.Capture(error).Throw()
        Unchecked.defaultof<'value>

type private ContextualScriptProcessor(
    scriptPath: string,
    direction: string,
    stageNumber: int,
    processor: PipeL4.Core.IByteStreamProcessor
) =
    let contextualize (cancellationToken: CancellationToken) (error: exn) =
        match error with
        | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
            error
        | :? ScriptProcessorFailureException as contextualized
            when contextualized.ScriptPath = scriptPath
                 && contextualized.Direction = direction
                 && contextualized.StageNumber = stageNumber ->
            contextualized
        | _ ->
            ScriptProcessorFailureException(
                scriptPath,
                direction,
                stageNumber,
                error
            )

    let raiseContextualized cancellationToken error =
        contextualize cancellationToken error
        |> ExceptionHelpers.raisePreserving

    interface PipeL4.Core.IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            try
                let operation = processor.ProcessAsync(input, output, cancellationToken)

                if operation.IsCompletedSuccessfully then
                    operation
                else
                    ValueTask<PipeL4.Core.ProcessorResult>(
                        task {
                            try
                                return! operation.AsTask()
                            with error ->
                                return raiseContextualized cancellationToken error
                        }
                    )
            with error ->
                raiseContextualized cancellationToken error

    interface IProcessorFailureContext with
        member _.ContextualizeFailure(error, cancellationToken) =
            contextualize cancellationToken error

    interface IAsyncDisposable with
        member _.DisposeAsync() =
            ValueTask(
                task {
                    let! failures =
                        ProcessorLifetime.disposeAllAsync [| processor |]
                        |> _.AsTask()

                    match failures with
                    | [||] -> return ()
                    | [| error |] ->
                        return raiseContextualized CancellationToken.None error
                    | errors ->
                        return
                            raiseContextualized
                                CancellationToken.None
                                (AggregateException(
                                    "Multiple processor resources failed during disposal.",
                                    errors
                                ))
                }
            )

type internal ConfiguredScriptStage(scriptPath: string, plugin: CompiledScriptProcessor) =
    member _.ScriptPath = scriptPath
    member _.Plugin = plugin

    member _.CreateProcessor(connectionState, direction, stageNumber) =
        try
            let processor = plugin.CreateProcessor(connectionState)

            ContextualScriptProcessor(
                scriptPath,
                direction,
                stageNumber,
                processor
            )
            :> PipeL4.Core.IByteStreamProcessor
        with error ->
            raise (
                ScriptProcessorFailureException(
                    scriptPath,
                    direction,
                    stageNumber,
                    error
                )
            )

type internal ScriptPipelineDefinition internal (
    clientToServerStages: ConfiguredScriptStage array,
    serverToClientStages: ConfiguredScriptStage array
) =
    member _.HasPlugins =
        clientToServerStages.Length > 0 || serverToClientStages.Length > 0

    member internal _.ClientToServerPaths =
        clientToServerStages |> Array.map _.ScriptPath

    member internal _.ServerToClientPaths =
        serverToClientStages |> Array.map _.ScriptPath

    member _.CreateProcessorPipelinesAsync(connectionState: ConnectionState) =
        ValueTask<DirectionalProcessorPipelines>(
            task {
                let created = ResizeArray<PipeL4.Core.IByteStreamProcessor>()
                let publicConnectionState =
                    ConnectionStateAdapter(connectionState) :> IConnectionState

                let create direction (stages: ConfiguredScriptStage array) =
                    stages
                    |> Array.mapi (fun index stage ->
                        let processor =
                            stage.CreateProcessor(
                                publicConnectionState,
                                direction,
                                index + 1
                            )

                        created.Add processor
                        processor)

                try
                    return
                        { ClientToBackend = create "client-to-server" clientToServerStages
                          BackendToClient = create "server-to-client" serverToClientStages }
                with constructionError ->
                    let! disposalFailures =
                        ProcessorLifetime.disposeAllAsync (created.ToArray())
                        |> _.AsTask()

                    match disposalFailures with
                    | [||] ->
                        return ExceptionHelpers.raisePreserving constructionError
                    | _ ->
                        let failures =
                            Array.append [| constructionError |] disposalFailures

                        return
                            raise (
                                AggregateException(
                                    "Processor pipeline construction failed and cleanup also failed.",
                                    failures
                                )
                            )
            }
        )

[<RequireQualifiedAccess>]
module internal ScriptPipelineLoader =
    type Compiler = string -> CancellationToken -> Task<ScriptCompilationResult>

    let private pathComparer =
        if OperatingSystem.IsWindows() then
            StringComparer.OrdinalIgnoreCase
        else
            StringComparer.Ordinal

    let private severityName severity =
        match severity with
        | ScriptDiagnosticSeverity.Information -> "info"
        | ScriptDiagnosticSeverity.Warning -> "warning"
        | ScriptDiagnosticSeverity.Error -> "error"

    let private formatDiagnostic (diagnostic: ScriptDiagnostic) =
        let location =
            match diagnostic.SourcePath, diagnostic.Range with
            | Some path, Some range ->
                $"{path}({range.Start.Line},{range.Start.Column + 1})"
            | Some path, None -> path
            | None, _ -> "plugin"

        let compilerCode =
            diagnostic.CompilerCode
            |> Option.map (fun code -> $" {code}")
            |> Option.defaultValue ""

        $"{location}: {severityName diagnostic.Severity}{compilerCode}: {diagnostic.Message}"

    let private reportCompilerWarning (logger: ILogger) scriptPath diagnostic =
        if diagnostic.Severity = ScriptDiagnosticSeverity.Warning then
            let formattedDiagnostic = formatDiagnostic diagnostic

            try
                logger.LogWarning(
                    "F# script plugin {ScriptPath}: {Diagnostic}",
                    [| box scriptPath; box formattedDiagnostic |]
                )
            with _ ->
                ()

    let private compilationFailure scriptPath diagnostics =
        let details =
            diagnostics
            |> Array.map formatDiagnostic
            |> String.concat Environment.NewLine

        if String.IsNullOrWhiteSpace details then
            ScriptPluginConfigurationException(
                $"F# script plugin '{scriptPath}' could not be compiled and loaded."
            )
        else
            ScriptPluginConfigurationException(
                $"F# script plugin '{scriptPath}' could not be compiled and loaded:{Environment.NewLine}{details}"
            )

    let private canonicalizePath suppliedPath =
        if String.IsNullOrWhiteSpace suppliedPath then
            suppliedPath
        else
            try
                Path.GetFullPath suppliedPath
            with error ->
                raise (
                    ScriptPluginConfigurationException(
                        $"F# script plugin path '{suppliedPath}' is invalid: {error.Message}"
                    )
                )

    let private loadWithOptionalLoggerAsync
        (compiler: Compiler)
        (clientToServerPaths: IReadOnlyList<string>)
        (serverToClientPaths: IReadOnlyList<string>)
        (logger: ILogger option)
        (cancellationToken: CancellationToken)
        =
        task {
            let compiled = Dictionary<string, CompiledScriptProcessor>(pathComparer)

            let loadDirection (paths: IReadOnlyList<string>) =
                task {
                    let stages = ResizeArray<ConfiguredScriptStage>(paths.Count)

                    for suppliedPath in paths do
                        cancellationToken.ThrowIfCancellationRequested()
                        let scriptPath = canonicalizePath suppliedPath

                        let! plugin =
                            task {
                                match compiled.TryGetValue scriptPath with
                                | true, existing -> return existing
                                | false, _ ->
                                    let! result = compiler scriptPath cancellationToken

                                    match result with
                                    | Compiled(value, diagnostics) ->
                                        cancellationToken.ThrowIfCancellationRequested()
                                        compiled.Add(scriptPath, value)

                                        match logger with
                                        | Some logger ->
                                            diagnostics
                                            |> Array.iter (reportCompilerWarning logger scriptPath)
                                        | None -> ()

                                        cancellationToken.ThrowIfCancellationRequested()
                                        return value
                                    | Failed diagnostics ->
                                        return raise (compilationFailure scriptPath diagnostics)
                            }

                        cancellationToken.ThrowIfCancellationRequested()
                        stages.Add(ConfiguredScriptStage(scriptPath, plugin))

                    return stages.ToArray()
                }

            let! clientToServerStages = loadDirection clientToServerPaths
            let! serverToClientStages = loadDirection serverToClientPaths
            cancellationToken.ThrowIfCancellationRequested()

            return
                ScriptPipelineDefinition(
                    clientToServerStages,
                    serverToClientStages
                )
        }

    let loadWithCompilerAsync
        (compiler: Compiler)
        (clientToServerPaths: IReadOnlyList<string>)
        (serverToClientPaths: IReadOnlyList<string>)
        (cancellationToken: CancellationToken)
        =
        loadWithOptionalLoggerAsync
            compiler
            clientToServerPaths
            serverToClientPaths
            None
            cancellationToken

    let loadWithCompilerAndLoggerAsync
        (compiler: Compiler)
        (clientToServerPaths: IReadOnlyList<string>)
        (serverToClientPaths: IReadOnlyList<string>)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        loadWithOptionalLoggerAsync
            compiler
            clientToServerPaths
            serverToClientPaths
            (Some logger)
            cancellationToken

    let loadAsync
        (configuration: ForwardingConfiguration)
        (cancellationToken: CancellationToken)
        =
        let compiler = ScriptCompiler()

        loadWithCompilerAsync
            (fun path token -> compiler.CompileAndLoadAsync(path, token))
            configuration.ClientToServerPluginPaths
            configuration.ServerToClientPluginPaths
            cancellationToken

[<RequireQualifiedAccess>]
module internal ScriptTcpForwarder =
    let runWithCompilerAsync
        (compiler: ScriptPipelineLoader.Compiler)
        (configuration: ForwardingConfiguration)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        task {
            let! definition =
                ScriptPipelineLoader.loadWithCompilerAndLoggerAsync
                    compiler
                    configuration.ClientToServerPluginPaths
                    configuration.ServerToClientPluginPaths
                    logger
                    cancellationToken

            if definition.HasPlugins then
                return!
                    TcpForwarder.runWithOptionalProcessorPipelineFactoryAsync
                        configuration
                        definition.CreateProcessorPipelinesAsync
                        logger
                        cancellationToken
            else
                return! TcpForwarder.runAsync configuration logger cancellationToken
        }

    let runAsync
        (configuration: ForwardingConfiguration)
        (logger: ILogger)
        (cancellationToken: CancellationToken)
        =
        if
            configuration.ClientToServerPluginPaths.Count = 0
            && configuration.ServerToClientPluginPaths.Count = 0
        then
            TcpForwarder.runAsync configuration logger cancellationToken
        else
            let compiler = ScriptCompiler()

            runWithCompilerAsync
                (fun path token -> compiler.CompileAndLoadAsync(path, token))
                configuration
                logger
                cancellationToken
