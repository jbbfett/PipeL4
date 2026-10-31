namespace PipeL4.Plugins

open System
open System.IO
open System.Runtime.Loader
open System.Threading
open System.Threading.Tasks
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Diagnostics
open FSharp.Compiler.Tokenization

[<RequireQualifiedAccess>]
type internal ScriptDiagnosticSeverity =
    | Information
    | Warning
    | Error

[<Struct>]
type internal ScriptSourcePosition =
    { Line: int
      Column: int }

[<Struct>]
type internal ScriptSourceRange =
    { Start: ScriptSourcePosition
      End: ScriptSourcePosition }

type internal ScriptDiagnostic =
    { Severity: ScriptDiagnosticSeverity
      CompilerCode: string option
      Message: string
      SourcePath: string option
      Range: ScriptSourceRange option }

[<NoEquality; NoComparison>]
type internal ScriptCompilationResult =
    | Compiled of plugin: CompiledScriptProcessor * diagnostics: ScriptDiagnostic array
    | Failed of diagnostics: ScriptDiagnostic array

type internal ScriptCompiler internal (temporaryRoot: string) =
    let checker = FSharpChecker.Create()

    do
        if String.IsNullOrWhiteSpace temporaryRoot then
            invalidArg (nameof temporaryRoot) "A temporary root directory is required."

    let diagnostic severity sourcePath range message =
        { Severity = severity
          CompilerCode = None
          Message = message
          SourcePath = sourcePath
          Range = range }

    let error sourcePath message =
        diagnostic ScriptDiagnosticSeverity.Error sourcePath None message

    let compilerDiagnostic (value: FSharpDiagnostic) =
        let severity =
            match value.Severity with
            | FSharpDiagnosticSeverity.Error -> ScriptDiagnosticSeverity.Error
            | FSharpDiagnosticSeverity.Warning -> ScriptDiagnosticSeverity.Warning
            | _ -> ScriptDiagnosticSeverity.Information

        let sourceRange = value.Range

        let range =
            if sourceRange.StartLine > 0 then
                Some
                    { Start =
                        { Line = sourceRange.StartLine
                          Column = sourceRange.StartColumn }
                      End =
                        { Line = sourceRange.EndLine
                          Column = sourceRange.EndColumn } }
            else
                None

        let sourcePath =
            if String.IsNullOrWhiteSpace sourceRange.FileName then
                None
            else
                Some sourceRange.FileName

        { Severity = severity
          CompilerCode = Some value.ErrorNumberText
          Message = value.Message
          SourcePath = sourcePath
          Range = range }

    let isDirectiveBoundary (line: string) index =
        index = line.Length
        || Char.IsWhiteSpace line[index]
        || line[index] = '"'
        || line[index] = '@'

    let compilerBaseArguments outputPath =
        [| "fsc.dll"
           "--target:library"
           "--targetprofile:netcore"
           "--noframework"
           "--deterministic+"
           "--debug-"
           "--warn:5"
           "--fullpaths"
           "--utf8output"
           "--nologo"
           $"--out:{outputPath}" |]

    let forbiddenDirectiveDiagnostics sourcePath (source: string) =
        let parsingOptions, _ =
            Array.append
                (compilerBaseArguments "PipeL4.DirectiveValidation.dll")
                [| sourcePath |]
            |> Array.toList
            |> fun arguments ->
                checker.GetParsingOptionsFromCommandLineArgs(
                    arguments,
                    isInteractive = false,
                    isEditing = false
                )

        let tokenizer =
            FSharpSourceTokenizer(
                parsingOptions.ConditionalDefines,
                Some sourcePath,
                Some parsingOptions.LangVersionText,
                parsingOptions.StrictIndentation
            )
        let lines = source.Split '\n'

        let isConditionalControl (line: string) (firstNonWhitespace: int) =
            if firstNonWhitespace >= line.Length || line[firstNonWhitespace] <> '#' then
                false
            else
                let afterHash = line.Substring(firstNonWhitespace + 1).TrimStart(' ', '\t')

                [| "if"; "elif"; "else"; "endif" |]
                |> Array.exists (fun name ->
                    afterHash.StartsWith(name, StringComparison.Ordinal)
                    && isDirectiveBoundary afterHash name.Length)

        // Use the tokenizer's real preprocessor state to find branch boundaries. In
        // active code it distinguishes controls from literal text. In inactive code,
        // FCS returns INACTIVECODE for the whole line (starting at column zero), even
        // when the directive is indented. Recognize conditional spelling on those
        // lines, where the compiler itself skips lexical constructs.
        let mutable controlLexState = FSharpTokenizerLexState.Initial

        let conditionalControlLines =
            lines
            |> Array.mapi (fun _ sourceLine ->
                let line = sourceLine.TrimEnd '\r'
                let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length
                let lineTokenizer = tokenizer.CreateLineTokenizer line
                let mutable scanning = true
                let mutable isControl = false

                while scanning do
                    let token, nextState = lineTokenizer.ScanToken controlLexState
                    controlLexState <- nextState

                    match token with
                    | Some info
                        when info.TokenName = "INACTIVECODE"
                             || (info.LeftColumn = firstNonWhitespace
                                 && info.TokenName = "HASH_IF") ->
                        isControl <-
                            isConditionalControl line firstNonWhitespace
                    | None -> scanning <- false
                    | Some _ -> ()

                isControl)

        let mutable lexState = FSharpTokenizerLexState.Initial

        lines
        |> Array.mapi (fun lineIndex sourceLine ->
            let line = sourceLine.TrimEnd '\r'
            let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length

            if conditionalControlLines[lineIndex] then
                // Each conditional arm is checked independently. This is essential
                // because the compiler skips inactive text before lexing the next arm.
                lexState <- FSharpTokenizerLexState.Initial
                None
            else
                let candidate =
                    if firstNonWhitespace >= line.Length || line[firstNonWhitespace] <> '#' then
                        None
                    else
                        let mutable directiveStart = firstNonWhitespace + 1

                        while directiveStart < line.Length
                              && (line[directiveStart] = ' ' || line[directiveStart] = '\t') do
                            directiveStart <- directiveStart + 1

                        let remaining = line.Substring directiveStart

                        [| "load"; "r"; "i" |]
                        |> Array.tryFind (fun name ->
                            remaining.StartsWith(name, StringComparison.OrdinalIgnoreCase)
                            && isDirectiveBoundary remaining name.Length)
                        |> Option.map (fun name -> name, directiveStart)

                let lineTokenizer = tokenizer.CreateLineTokenizer line
                let mutable scanning = true
                let mutable dependencyHashIsCode = false

                while scanning do
                    let token, nextState = lineTokenizer.ScanToken lexState
                    lexState <- nextState

                    match token with
                    | Some info
                        when info.LeftColumn = firstNonWhitespace
                             && info.TokenName = "HASH" ->
                        dependencyHashIsCode <- true
                    | None -> scanning <- false
                    | Some _ -> ()

                match candidate with
                | Some(name, directiveStart) when dependencyHashIsCode ->
                    let range =
                        { Start =
                            { Line = lineIndex + 1
                              Column = firstNonWhitespace }
                          End =
                            { Line = lineIndex + 1
                              Column = directiveStart + name.Length } }

                    Some(
                        diagnostic
                            ScriptDiagnosticSeverity.Error
                            (Some sourcePath)
                            (Some range)
                            $"Script dependency directive '#{name}' is not supported; references are supplied by PipeL4."
                    )
                | _ -> None
        )
        |> Array.choose id

    let controlledReferencePaths () =
        let directoryName (path: string) =
            match Path.GetDirectoryName path with
            | null -> invalidOp $"The assembly path '{path}' does not have a directory."
            | directory -> Path.GetFullPath directory

        let runtimeDirectory =
            typeof<obj>.Assembly.Location
            |> directoryName

        let trustedPlatformAssemblies =
            match AppContext.GetData "TRUSTED_PLATFORM_ASSEMBLIES" with
            | :? string as paths when not (String.IsNullOrWhiteSpace paths) ->
                paths.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            | _ ->
                invalidOp "The runtime did not provide its trusted platform assembly list."

        let pathComparison =
            if OperatingSystem.IsWindows() then
                StringComparison.OrdinalIgnoreCase
            else
                StringComparison.Ordinal

        let runtimeReferences =
            trustedPlatformAssemblies
            |> Array.filter (fun path ->
                let directory =
                    path
                    |> directoryName

                String.Equals(directory, runtimeDirectory, pathComparison))

        Array.append
            runtimeReferences
            [| typeof<int list>.Assembly.Location
               typeof<IByteStreamProcessor>.Assembly.Location |]
        |> Array.distinct
        |> Array.sort

    let compilerArguments scriptPath outputPath =
        Array.concat
            [| compilerBaseArguments outputPath
               controlledReferencePaths ()
               |> Array.map (fun referencePath -> $"-r:{referencePath}")
               [| scriptPath |] |]

    new() = ScriptCompiler(Path.Combine(Path.GetTempPath(), "PipeL4", "fsx"))

    member _.CompileAndLoadAsync(
        scriptPath: string,
        cancellationToken: CancellationToken
    ) : Task<ScriptCompilationResult> =
        task {
            cancellationToken.ThrowIfCancellationRequested()

            let suppliedPath = Some scriptPath

            try
                if String.IsNullOrWhiteSpace scriptPath then
                    return Failed [| error suppliedPath "An F# script path is required." |]
                elif not (String.Equals(Path.GetExtension scriptPath, ".fsx", StringComparison.OrdinalIgnoreCase)) then
                    return Failed [| error suppliedPath "The plugin source path must have the .fsx extension." |]
                else
                    let fullScriptPath = Path.GetFullPath scriptPath
                    let! source = File.ReadAllTextAsync(fullScriptPath, cancellationToken)
                    cancellationToken.ThrowIfCancellationRequested()
                    let forbiddenDirectives = forbiddenDirectiveDiagnostics fullScriptPath source
                    cancellationToken.ThrowIfCancellationRequested()

                    if forbiddenDirectives.Length > 0 then
                        return Failed forbiddenDirectives
                    else
                        let compilationId = Guid.NewGuid().ToString "N"
                        let workspacePath = Path.Combine(temporaryRoot, compilationId)
                        let outputPath = Path.Combine(workspacePath, $"PipeL4.Plugin.{compilationId}.dll")
                        let mutable workspaceDeleted = false

                        try
                            try
                                Directory.CreateDirectory workspacePath |> ignore
                                cancellationToken.ThrowIfCancellationRequested()

                                let! compilerDiagnostics, compilerFailure =
                                    checker.Compile(compilerArguments fullScriptPath outputPath)
                                    |> fun compilation ->
                                        Async.StartAsTask(
                                            compilation,
                                            cancellationToken = cancellationToken
                                        )

                                cancellationToken.ThrowIfCancellationRequested()

                                let diagnostics =
                                    compilerDiagnostics |> Array.map compilerDiagnostic

                                let hasErrors =
                                    diagnostics
                                    |> Array.exists (fun value ->
                                        value.Severity = ScriptDiagnosticSeverity.Error)

                                match compilerFailure with
                                | Some(:? OperationCanceledException) when cancellationToken.IsCancellationRequested ->
                                    return raise (OperationCanceledException cancellationToken)
                                | Some(:? OperationCanceledException as cancellation) ->
                                    return raise cancellation
                                | Some failure ->
                                    let failureDiagnostic =
                                        error
                                            (Some fullScriptPath)
                                            $"The F# compiler could not complete the script compilation: {failure.Message}"

                                    return Failed(Array.append diagnostics [| failureDiagnostic |])
                                | None when hasErrors ->
                                    return Failed diagnostics
                                | None when not (File.Exists outputPath) ->
                                    let missingOutput =
                                        error
                                            (Some fullScriptPath)
                                            "The F# compiler reported success but did not produce an assembly."

                                    return Failed(Array.append diagnostics [| missingOutput |])
                                | None ->
                                    let! assemblyBytes =
                                        File.ReadAllBytesAsync(outputPath, cancellationToken)

                                    cancellationToken.ThrowIfCancellationRequested()
                                    Directory.Delete(workspacePath, recursive = true)
                                    workspaceDeleted <- true

                                    use assemblyStream = new MemoryStream(assemblyBytes, writable = false)

                                    let assembly =
                                        AssemblyLoadContext.Default.LoadFromStream assemblyStream

                                    cancellationToken.ThrowIfCancellationRequested()

                                    match ScriptProcessorDiscovery.discover assembly with
                                    | Ok plugin ->
                                        cancellationToken.ThrowIfCancellationRequested()
                                        return Compiled(plugin, diagnostics)
                                    | Error message ->
                                        let discoveryDiagnostic = error (Some fullScriptPath) message
                                        return Failed(Array.append diagnostics [| discoveryDiagnostic |])
                            finally
                                if not workspaceDeleted && Directory.Exists workspacePath then
                                    try
                                        Directory.Delete(workspacePath, recursive = true)
                                    with _ ->
                                        ()
                        with
                        | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                            return raise (OperationCanceledException cancellationToken)
                        | :? OperationCanceledException as cancellation ->
                            return raise cancellation
                        | failure ->
                            return
                                Failed
                                    [| error
                                           (Some fullScriptPath)
                                           $"The script could not be compiled and loaded: {failure.Message}" |]
            with
            | :? OperationCanceledException when cancellationToken.IsCancellationRequested ->
                return raise (OperationCanceledException cancellationToken)
            | :? OperationCanceledException as cancellation ->
                return raise cancellation
            | failure ->
                return
                    Failed
                        [| error
                               suppliedPath
                               $"The script source could not be read: {failure.Message}" |]
        }
