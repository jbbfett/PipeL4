namespace PipeL4.PluginHost

open System
open System.IO
open System.Runtime.Loader
open System.Threading
open System.Threading.Tasks
open FSharp.Compiler.CodeAnalysis
open FSharp.Compiler.Diagnostics
open FSharp.Compiler.Tokenization
open PipeL4.Plugins

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

    let isSemVer (version: string) =
        let isAsciiDigit value = value >= '0' && value <= '9'

        let isIdentifierCharacter value =
            isAsciiDigit value
            || (value >= 'A' && value <= 'Z')
            || (value >= 'a' && value <= 'z')
            || value = '-'

        let hasValidIdentifiers allowNumericLeadingZero (value: string) =
            let identifiers = value.Split '.'

            identifiers.Length > 0
            && (identifiers
                |> Array.forall (fun identifier ->
                    identifier.Length > 0
                    && (identifier |> Seq.forall isIdentifierCharacter)
                    && (allowNumericLeadingZero
                        || not (identifier |> Seq.forall isAsciiDigit)
                        || identifier.Length = 1
                        || identifier[0] <> '0')))

        let versionAndMetadata = version.Split '+'

        if versionAndMetadata.Length > 2 || String.IsNullOrEmpty versionAndMetadata[0] then
            false
        else
            let versionAndPrerelease = versionAndMetadata[0]
            let prereleaseSeparator = versionAndPrerelease.IndexOf '-'

            let core, prerelease =
                if prereleaseSeparator < 0 then
                    versionAndPrerelease, None
                else
                    versionAndPrerelease.Substring(0, prereleaseSeparator),
                    Some(versionAndPrerelease.Substring(prereleaseSeparator + 1))

            let coreParts = core.Split '.'

            let validCore =
                coreParts.Length = 3
                && (coreParts
                    |> Array.forall (fun part ->
                        part.Length > 0
                        && (part |> Seq.forall isAsciiDigit)
                        && (part.Length = 1 || part[0] <> '0')))

            let validPrerelease =
                prerelease
                |> Option.forall (hasValidIdentifiers false)

            let validMetadata =
                versionAndMetadata.Length = 1
                || hasValidIdentifiers true versionAndMetadata[1]

            validCore && validPrerelease && validMetadata

    let isSupportedEditorReference (line: string) (directiveStart: int) (name: string) =
        let directiveEnd = directiveStart + name.Length

        if name <> "r"
           || directiveEnd > line.Length
           || not (String.Equals(line.Substring(directiveStart, name.Length), "r", StringComparison.Ordinal)) then
            false
        else
            let mutable argumentStart = directiveEnd

            while argumentStart < line.Length
                  && (line[argumentStart] = ' ' || line[argumentStart] = '\t') do
                argumentStart <- argumentStart + 1

            let argument = line.Substring(argumentStart).TrimEnd(' ', '\t')

            if argument.Length < 2 || argument[0] <> '"' || argument[argument.Length - 1] <> '"' then
                false
            else
                let reference = argument.Substring(1, argument.Length - 2)
                let packagePrefix = "nuget: PipeL4.Plugins, "

                reference.StartsWith(packagePrefix, StringComparison.Ordinal)
                && isSemVer (reference.Substring packagePrefix.Length)

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

        let conditionalControl (line: string) (firstNonWhitespace: int) =
            if firstNonWhitespace >= line.Length || line[firstNonWhitespace] <> '#' then
                None
            else
                let afterHash = line.Substring(firstNonWhitespace + 1).TrimStart(' ', '\t')

                [| "if"; "elif"; "else"; "endif" |]
                |> Array.tryPick (fun name ->
                    if afterHash.StartsWith(name, StringComparison.Ordinal)
                       && isDirectiveBoundary afterHash name.Length then
                        Some(name, afterHash.Substring(name.Length).Trim(' ', '\t'))
                    else
                        None)

        // Use the tokenizer's real preprocessor state to find branch boundaries. In
        // active code it distinguishes controls from literal text. In inactive code,
        // FCS returns INACTIVECODE for the whole line (starting at column zero), even
        // when the directive is indented. Recognize conditional spelling on those
        // lines, where the compiler itself skips lexical constructs.
        let mutable controlLexState = FSharpTokenizerLexState.Initial

        let conditionalControlLines, inactiveCodeLines =
            let inactiveCodeLines = Array.zeroCreate<bool> lines.Length

            let conditionalControlLines =
                lines
                |> Array.mapi (fun lineIndex sourceLine ->
                    let line = sourceLine.TrimEnd '\r'
                    let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length
                    let lineTokenizer = tokenizer.CreateLineTokenizer line
                    let mutable scanning = true
                    let mutable isControl = false

                    while scanning do
                        let token, nextState = lineTokenizer.ScanToken controlLexState
                        controlLexState <- nextState

                        match token with
                        | Some info when info.TokenName = "INACTIVECODE" ->
                            inactiveCodeLines[lineIndex] <- true
                            isControl <- conditionalControl line firstNonWhitespace |> Option.isSome
                        | Some info
                            when info.LeftColumn = firstNonWhitespace
                                 && info.TokenName = "HASH_IF" ->
                            isControl <- conditionalControl line firstNonWhitespace |> Option.isSome
                        | None -> scanning <- false
                        | Some _ -> ()

                    if isControl then
                        conditionalControl line firstNonWhitespace
                    else
                        None)

            conditionalControlLines, inactiveCodeLines

        // The only permitted editor reference is one canonical PipeL4.Plugins NuGet
        // reference in a top-level INTERACTIVE branch. FCS supplies the branch state;
        // this small structural check deliberately rejects nesting and alternate arms.
        let allowedEditorReferenceLines = Array.zeroCreate<bool> lines.Length
        let mutable conditionalDepth = 0
        let mutable isInteractiveBlock = false
        let mutable isSimpleInteractiveBlock = false
        let mutable editorReferenceLine = -1
        let mutable hasAcceptedEditorReference = false

        let isExactConditionalDirective (line: string) firstNonWhitespace name argument =
            if firstNonWhitespace >= line.Length || line[firstNonWhitespace] <> '#' then
                false
            else
                let afterHash = line.Substring(firstNonWhitespace + 1)

                afterHash.StartsWith(name, StringComparison.Ordinal)
                && isDirectiveBoundary afterHash name.Length
                && String.Equals(
                    afterHash.Substring(name.Length).Trim(' ', '\t'),
                    argument,
                    StringComparison.Ordinal
                )

        for lineIndex in 0 .. lines.Length - 1 do
            match conditionalControlLines[lineIndex] with
            | Some("if", condition) ->
                if conditionalDepth = 0 then
                    let line = lines[lineIndex].TrimEnd '\r'
                    let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length

                    isInteractiveBlock <-
                        condition = "INTERACTIVE"
                        && isExactConditionalDirective line firstNonWhitespace "if" "INTERACTIVE"

                    isSimpleInteractiveBlock <- isInteractiveBlock
                    editorReferenceLine <- -1
                else
                    isSimpleInteractiveBlock <- false

                conditionalDepth <- conditionalDepth + 1
            | Some(("elif" | "else"), _) when conditionalDepth > 0 ->
                isSimpleInteractiveBlock <- false
            | Some("endif", _) when conditionalDepth > 0 ->
                let line = lines[lineIndex].TrimEnd '\r'
                let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length

                if conditionalDepth = 1
                   && not (isExactConditionalDirective line firstNonWhitespace "endif" "") then
                    isSimpleInteractiveBlock <- false

                if conditionalDepth = 1
                   && isInteractiveBlock
                   && isSimpleInteractiveBlock
                   && editorReferenceLine >= 0
                   && not hasAcceptedEditorReference then
                    allowedEditorReferenceLines[editorReferenceLine] <- true
                    hasAcceptedEditorReference <- true

                conditionalDepth <- conditionalDepth - 1

                if conditionalDepth = 0 then
                    isInteractiveBlock <- false
                    isSimpleInteractiveBlock <- false
                    editorReferenceLine <- -1
            | _ when conditionalDepth = 1 && isInteractiveBlock && isSimpleInteractiveBlock ->
                let line = lines[lineIndex].TrimEnd '\r'
                let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length

                if firstNonWhitespace < line.Length then
                    let dependencyCandidate =
                        if line[firstNonWhitespace] <> '#' then
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

                    match dependencyCandidate with
                    | Some("r", directiveStart)
                        when inactiveCodeLines[lineIndex]
                             && directiveStart = firstNonWhitespace + 1
                             && isSupportedEditorReference line directiveStart "r" ->
                        if editorReferenceLine < 0 then
                            editorReferenceLine <- lineIndex
                        else
                            isSimpleInteractiveBlock <- false
                    | _ -> isSimpleInteractiveBlock <- false
            | _ -> ()

        let mutable lexState = FSharpTokenizerLexState.Initial

        lines
        |> Array.mapi (fun lineIndex sourceLine ->
            let line = sourceLine.TrimEnd '\r'
            let firstNonWhitespace = line.Length - line.TrimStart(' ', '\t').Length

            if conditionalControlLines[lineIndex].IsSome then
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
                | Some(name, directiveStart)
                    when dependencyHashIsCode && not allowedEditorReferenceLines[lineIndex] ->
                    let range =
                        { Start =
                            { Line = lineIndex + 1
                              Column = firstNonWhitespace }
                          End =
                            { Line = lineIndex + 1
                              Column = directiveStart + name.Length } }

                    let message =
                        if name = "r" && line.Contains("PipeL4.Plugins", StringComparison.Ordinal) then
                            "The PipeL4.Plugins editor reference must be a single versioned NuGet reference inside a top-level '#if INTERACTIVE' block without nested or alternate branches."
                        else
                            $"Script dependency directive '#{name}' is not supported; references are supplied by PipeL4."

                    Some(
                        diagnostic
                            ScriptDiagnosticSeverity.Error
                            (Some sourcePath)
                            (Some range)
                            message
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
