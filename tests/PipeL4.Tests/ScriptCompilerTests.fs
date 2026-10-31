namespace PipeL4.Tests

open System
open System.Buffers
open System.Collections.Concurrent
open System.IO
open System.Runtime.Loader
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open PipeL4.Core
open PipeL4.Plugins
open Xunit

module ScriptCompilerTests =
    let private unusedConnectionState =
        { new IConnectionState with
            member _.GetOrAdd<'T>(_: string, factory: unit -> 'T) : 'T = factory ()
            member _.TryGet<'T>(_: string) : 'T option = None }

    let private bytes (text: string) = Encoding.ASCII.GetBytes text

    let private sequenceBytes (sequence: ReadOnlySequence<byte>) =
        let result = Array.zeroCreate<byte> (int sequence.Length)
        sequence.CopyTo(result.AsSpan())
        result

    type private TestOwner(buffer: byte array) =
        member _.Buffer = buffer

        interface IMemoryOwner<byte> with
            member _.Memory = Memory<byte>(buffer)
            member _.Dispose() = ()

    type private TestWorkspace() =
        let rootPath =
            Path.Combine(Path.GetTempPath(), "PipeL4.Tests", Guid.NewGuid().ToString "N")

        let sourceRoot = Path.Combine(rootPath, "sources")
        let compilerRoot = Path.Combine(rootPath, "compiler")
        let compiler = ScriptCompiler compilerRoot

        do Directory.CreateDirectory(sourceRoot) |> ignore

        member _.Compiler = compiler

        member _.CompilerRoot = compilerRoot

        member _.WriteSource(fileName: string, source: string) =
            let path = Path.Combine(sourceRoot, fileName)
            File.WriteAllText(path, source)
            path

        interface IDisposable with
            member _.Dispose() =
                if Directory.Exists rootPath then
                    Directory.Delete(rootPath, recursive = true)

    let private compile (compiler: ScriptCompiler) path =
        compiler.CompileAndLoadAsync(path, CancellationToken.None)

    let private withIdentityProcessor source =
        source
        + """

open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

    let private processorType declaration extraMembers =
        String.concat
            Environment.NewLine
            [ declaration
              extraMembers
              "    interface IByteStreamProcessor with"
              "        member _.ProcessAsync(input, output, _) ="
              "            output.AppendInput(0L, input.Bytes.Length)"
              "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ]

    let private processorSource types =
        String.concat
            Environment.NewLine
            [ "module ProcessorCandidates"
              ""
              "open System.Threading.Tasks"
              "open PipeL4.Plugins"
              ""
              String.concat (Environment.NewLine + Environment.NewLine) types ]

    let private diagnosticText diagnostics =
        diagnostics
        |> Array.map (fun value -> value.Message)
        |> String.concat Environment.NewLine

    let private expectCompiledPlugin result =
        match result with
        | Compiled(plugin, diagnostics) -> plugin, diagnostics
        | Failed diagnostics ->
            failwithf "Expected compilation to succeed:%s%s" Environment.NewLine (diagnosticText diagnostics)

    let private expectCompiled result =
        let plugin, diagnostics = expectCompiledPlugin result
        plugin.Assembly, diagnostics

    let private expectFailed result =
        match result with
        | Failed diagnostics -> diagnostics
        | Compiled(plugin, _) ->
            failwithf "Expected compilation to fail, but assembly '%s' was loaded." plugin.Assembly.FullName

    let private compilerArtifacts (workspace: TestWorkspace) =
        if Directory.Exists workspace.CompilerRoot then
            Directory.EnumerateFileSystemEntries workspace.CompilerRoot
            |> Seq.toArray
        else
            Array.empty

    [<Fact>]
    let ``compiles an ordinary script against standard NET and FSharp libraries`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "StandardLibraries.fsx",
                    withIdentityProcessor
                        """module StandardLibraries

let joined = System.String.Join("-", [ "fsharp"; "script" ])
let count = [ 1; 2; 3 ] |> List.length
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let assembly, diagnostics = expectCompiled result

            let assemblyName =
                assembly.GetName().Name
                |> Option.ofObj
                |> Option.defaultWith (fun () -> failwith "The loaded assembly has no name.")

            assemblyName.StartsWith("PipeL4.Plugin.", StringComparison.Ordinal)
            |> should equal true

            diagnostics
            |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)
            |> should equal false
        }

    [<Fact>]
    let ``loads the compiled assembly for ordinary reflection and use`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "InspectionFixture.fsx",
                    withIdentityProcessor
                        """namespace InspectionFixture

type Probe() =
    member _.Value = System.String.Concat("load", "ed")
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let assembly, _ = expectCompiled result

            AssemblyLoadContext.GetLoadContext assembly
            |> should be (sameAs AssemblyLoadContext.Default)

            let probeType =
                match assembly.GetType("InspectionFixture.Probe", throwOnError = true) with
                | null -> failwith "The declared probe type was not present in the loaded assembly."
                | value -> value

            let probe = Activator.CreateInstance probeType

            let property =
                match probeType.GetProperty "Value" with
                | null -> failwith "The declared probe property was not present in the loaded assembly."
                | value -> value

            let value = property.GetValue probe

            value |> should equal (box "loaded")
        }

    [<Fact>]
    let ``returns compiler errors with source positions and no assembly`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "InvalidScript.fsx",
                    """module InvalidScript

let value: int = "wrong"
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let compilerError =
                diagnostics
                |> Array.find (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)

            compilerError.CompilerCode.IsSome |> should equal true
            compilerError.SourcePath |> should equal (Some(Path.GetFullPath scriptPath))
            compilerError.Range.IsSome |> should equal true
            compilerError.Range.Value.Start.Line |> should be (greaterThan 0)
        }

    [<Fact>]
    let ``preserves warnings separately from errors`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "WarningScript.fsx",
                    withIdentityProcessor
                        """module WarningScript

let describe value =
    match value with
    | 0 -> "zero"
"""
                )

            let! result = compile workspace.Compiler scriptPath
            let _, diagnostics = expectCompiled result

            diagnostics
            |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Warning)
            |> should equal true

            diagnostics
            |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)
            |> should equal false
        }

    [<Fact>]
    let ``returns controlled failures for missing and non-fsx sources`` () =
        task {
            use workspace = new TestWorkspace()
            let missingPath = Path.Combine(workspace.CompilerRoot, "Missing.fsx")
            let! missingResult = compile workspace.Compiler missingPath
            let missingDiagnostics = expectFailed missingResult

            missingDiagnostics
            |> Array.exists (fun value ->
                value.Severity = ScriptDiagnosticSeverity.Error
                && value.Message.Contains("could not be read", StringComparison.OrdinalIgnoreCase))
            |> should equal true

            let sourcePath = workspace.WriteSource("NotAScript.fs", "module NotAScript")
            let! extensionResult = compile workspace.Compiler sourcePath
            let extensionDiagnostics = expectFailed extensionResult

            extensionDiagnostics
            |> Array.exists (fun value ->
                value.Severity = ScriptDiagnosticSeverity.Error
                && value.Message.Contains(".fsx", StringComparison.Ordinal))
            |> should equal true
        }

    [<Fact>]
    let ``rejects dependency-expanding script directives before compilation`` () =
        task {
            use workspace = new TestWorkspace()

            let directives =
                [| "#r \"nuget: Newtonsoft.Json\""
                   "#r \"local-library.dll\""
                   "#I \"/tmp/arbitrary-assemblies\""
                   "#i \"nuget: https://example.invalid/v3/index.json\""
                   "#load \"Other.fsx\"" |]

            for index, directive in Array.indexed directives do
                let scriptPath =
                    workspace.WriteSource(
                        $"Forbidden{index}.fsx",
                        $"""{directive}
module Forbidden{index}
"""
                    )

                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                diagnostics
                |> Array.exists (fun value ->
                    value.Severity = ScriptDiagnosticSeverity.Error
                    && value.Message.Contains("not supported", StringComparison.OrdinalIgnoreCase)
                    && value.Range |> Option.exists (fun range -> range.Start.Line = 1))
                |> should equal true
        }

    [<Fact>]
    let ``accepts directive-like text in FSharp strings and comments`` () =
        task {
            use workspace = new TestWorkspace()
            let source =
                [| "module DirectiveTextFixtures"
                   "let label = \"fixture\""
                   "let documentation = \"\"\""
                   "#r \"example-only.dll\""
                   "   #load indented-string-text.fsx"
                   "#if PH01_LITERAL_TEXT"
                   "#else"
                   "#endif"
                   "\"\"\""
                   "let verbatimDocumentation = @\""
                   "#load \"\"example-only.fsx\"\""
                   "\""
                   "let interpolatedDocumentation = $\"\"\""
                   "Documentation for {label}"
                   "#I \"example-only-directory\""
                   "\"\"\""
                   "let interpolatedVerbatimDocumentation = $@\"{label}"
                   "#r \"\"example-only.dll\"\""
                   "\""
                   "let ordinaryString = \"#r ordinary-string-text\""
                   "let ordinaryMultilineString = \"ordinary\\"
                   "#r ordinary-multiline-string-text"
                   "continued\""
                   "// #load line-comment-text.fsx"
                   "(* outer comment"
                   " \t#r indented-comment-text.dll"
                   "#if PH01_COMMENT_TEXT"
                   "#else"
                   "#endif"
                   "   (* nested comment"
                   "#I comment-only-directory"
                   "   *)"
                   "*)" |]
                |> String.concat Environment.NewLine
                |> withIdentityProcessor

            let scriptPath = workspace.WriteSource("DirectiveTextFixtures.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result

            plugin.ProcessorType.Name |> should equal "Processor"
        }

    [<Fact>]
    let ``uses compiler conditional defines when scanning active literals`` () =
        task {
            use workspace = new TestWorkspace()
            let source =
                [| "module CompiledConditionalLiteral"
                   "#if COMPILED"
                   "let selected = \"compiled\""
                   "let documentation = \"\"\""
                   "#r \"compiled-branch-string.dll\""
                   "#else"
                   "#load \"compiled-branch-string.fsx\""
                   "#endif"
                   "\"\"\""
                   "#else"
                   "let selected = \"other\""
                   "#endif" |]
                |> String.concat Environment.NewLine
                |> withIdentityProcessor

            let scriptPath = workspace.WriteSource("CompiledConditionalLiteral.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result

            let moduleType =
                match plugin.Assembly.GetType("CompiledConditionalLiteral", throwOnError = true) with
                | null -> failwith "The compiled conditional module was not present."
                | value -> value

            let selectedProperty =
                match moduleType.GetProperty "selected" with
                | null -> failwith "The compiled conditional value was not present."
                | value -> value

            let selected = selectedProperty.GetValue null

            selected |> should equal (box "compiled")
        }

    [<Fact>]
    let ``accepts directive-like text in inactive conditional strings and comments`` () =
        task {
            use workspace = new TestWorkspace()
            let source =
                [| "module ConditionalDirectiveTextFixtures"
                   "#if PH01_SYMBOL_NOT_DEFINED"
                   "let documentation = \"\"\""
                   "#r \"conditional-string-text.dll\""
                   "\"\"\""
                   "(* conditional block comment"
                   "#load \"conditional-comment-text.fsx\""
                   "*)"
                   "#else"
                   "let value = 1"
                   "#endif" |]
                |> String.concat Environment.NewLine
                |> withIdentityProcessor

            let scriptPath = workspace.WriteSource("ConditionalDirectiveTextFixtures.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result

            plugin.ProcessorType.Name |> should equal "Processor"
        }

    [<Fact>]
    let ``rejects dependency directives in inactive conditional branches`` () =
        task {
            use workspace = new TestWorkspace()
            let source =
                "#if PH01_SYMBOL_NOT_DEFINED"
                + Environment.NewLine
                + "#r \"inactive-branch.dll\""
                + Environment.NewLine
                + "#endif"

            let scriptPath = workspace.WriteSource("InactiveDirective.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let directiveDiagnostic =
                diagnostics
                |> Array.find (fun value -> value.Message.Contains("not supported", StringComparison.Ordinal))

            directiveDiagnostic.Range.Value.Start.Line |> should equal 2
            directiveDiagnostic.Range.Value.Start.Column |> should equal 0
            directiveDiagnostic.Range.Value.End.Column |> should equal 2
        }

    [<Fact>]
    let ``rejects directives in indented nested inactive conditionals`` () =
        task {
            use workspace = new TestWorkspace()

            let cases =
                [| "ExactReproducer", "   ", ""
                   "OneSpace", " ", " "
                   "ThreeSpaces", "   ", "   "
                   "Tab", "\t", "\t"
                   "MixedIndent", " \t", " \t" |]

            for name, nestedIndent, directiveIndent in cases do
                let source =
                    [| "#if OUTER_MISSING"
                       nestedIndent + "#if INNER_MISSING"
                       directiveIndent + "#r \"nested.dll\""
                       nestedIndent + "#endif"
                       "#endif" |]
                    |> String.concat Environment.NewLine
                    |> withIdentityProcessor

                let scriptPath = workspace.WriteSource($"{name}.fsx", source)
                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                let directiveDiagnostic =
                    diagnostics
                    |> Array.tryFind (fun value ->
                        value.Message.Contains("not supported", StringComparison.Ordinal))
                    |> Option.defaultWith (fun () ->
                        failwithf "The nested #r directive in %s was not rejected." name)

                directiveDiagnostic.Range.Value.Start.Line |> should equal 3
                directiveDiagnostic.Range.Value.Start.Column |> should equal directiveIndent.Length
                directiveDiagnostic.Range.Value.End.Column |> should equal (directiveIndent.Length + 2)
        }

    [<Fact>]
    let ``rejects active directives after malformed inactive conditional text`` () =
        task {
            use workspace = new TestWorkspace()

            let cases =
                [| "UnclosedString",
                   [| "#if PH01_SYMBOL_NOT_DEFINED"
                      "let text = \"\"\""
                      "#else"
                      "#r \"dependency.dll\""
                      "#endif" |]
                   "UnclosedComment",
                   [| "#if PH01_SYMBOL_NOT_DEFINED"
                      "(*"
                      "#else"
                      "#r \"dependency.dll\""
                      "#endif" |] |]

            for name, lines in cases do
                let scriptPath =
                    workspace.WriteSource(
                        $"{name}.fsx",
                        String.concat Environment.NewLine lines
                        |> withIdentityProcessor
                    )

                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                let directiveDiagnostic =
                    diagnostics
                    |> Array.tryFind (fun value ->
                        value.Message.Contains("not supported", StringComparison.Ordinal))
                    |> Option.defaultWith (fun () ->
                        failwithf
                            "The active #r after %s was not rejected. Compiler diagnostics: %s"
                            name
                            (diagnosticText diagnostics))

                directiveDiagnostic.Range.Value.Start.Line |> should equal 4
                directiveDiagnostic.Range.Value.Start.Column |> should equal 0
                directiveDiagnostic.Range.Value.End.Column |> should equal 2
        }

    [<Fact>]
    let ``rejects directives across nested conditional elif and endif transitions`` () =
        task {
            use workspace = new TestWorkspace()
            let source =
                [| "#if PH01_OUTER_SYMBOL_NOT_DEFINED"
                   "let text = \"\"\""
                   "   #if PH01_INNER_SYMBOL_NOT_DEFINED"
                   "#r \"nested-inactive.dll\""
                   "\t#elif PH01_INNER_ELIF_SYMBOL_NOT_DEFINED"
                   "#r \"nested-elif.dll\""
                   " #else"
                   "#r \"nested-else.dll\""
                   " \t#endif"
                   "#else"
                   "#r \"outer-else.dll\""
                   "#endif" |]
                |> String.concat Environment.NewLine
                |> withIdentityProcessor

            let scriptPath = workspace.WriteSource("NestedConditionals.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let directiveDiagnostics =
                diagnostics
                |> Array.filter (fun value ->
                    value.Message.Contains("not supported", StringComparison.Ordinal))

            directiveDiagnostics.Length |> should equal 4

            directiveDiagnostics
            |> Array.map (fun value -> value.Range.Value.Start.Line)
            |> should equal [| 4; 6; 8; 11 |]
        }

    [<Fact>]
    let ``preserves directive casing whitespace and source positions`` () =
        task {
            use workspace = new TestWorkspace()
            let scriptPath =
                workspace.WriteSource(
                    "DirectivePosition.fsx",
                    "module DirectivePosition"
                    + Environment.NewLine
                    + " \t# R \"restricted.dll\""
                )

            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let directiveDiagnostic =
                diagnostics
                |> Array.find (fun value -> value.Message.Contains("not supported", StringComparison.Ordinal))

            directiveDiagnostic.SourcePath |> should equal (Some(Path.GetFullPath scriptPath))
            directiveDiagnostic.CompilerCode |> should equal None
            directiveDiagnostic.Range.Value.Start.Line |> should equal 2
            directiveDiagnostic.Range.Value.Start.Column |> should equal 2
            directiveDiagnostic.Range.Value.End.Line |> should equal 2
            directiveDiagnostic.Range.Value.End.Column |> should equal 5
            directiveDiagnostic.Message
            |> should equal "Script dependency directive '#r' is not supported; references are supplied by PipeL4."
        }

    [<Fact>]
    let ``does not expose incidental PipeL4 or compiler assemblies`` () =
        task {
            use workspace = new TestWorkspace()

            let sources =
                [| """module CompilerAssemblyLeak
open FSharp.Compiler.CodeAnalysis
let checkerType = typeof<FSharpChecker>
"""
                   """module PipeL4AssemblyLeak
open PipeL4.Core
let configurationType = typeof<ForwardingConfiguration>
""" |]

            for index, source in Array.indexed sources do
                let scriptPath = workspace.WriteSource($"AssemblyLeak{index}.fsx", source)
                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                diagnostics
                |> Array.exists (fun value -> value.Severity = ScriptDiagnosticSeverity.Error)
                |> should equal true
        }

    [<Fact>]
    let ``removes temporary compiler artifacts after success and failure`` () =
        task {
            use workspace = new TestWorkspace()
            let validPath =
                workspace.WriteSource(
                    "Valid.fsx",
                    withIdentityProcessor "module Valid\nlet value = 42"
                )
            let invalidPath = workspace.WriteSource("Invalid.fsx", "module Invalid\nlet value: int = \"wrong\"")

            let! validResult = compile workspace.Compiler validPath
            expectCompiled validResult |> ignore
            compilerArtifacts workspace |> should be Empty

            let! invalidResult = compile workspace.Compiler invalidPath
            expectFailed invalidResult |> ignore
            compilerArtifacts workspace |> should be Empty
        }

    [<Fact>]
    let ``pre-cancelled compilation observes cancellation without creating compiler artifacts`` () =
        task {
            use workspace = new TestWorkspace()
            let scriptPath =
                workspace.WriteSource(
                    "PreCancelled.fsx",
                    withIdentityProcessor "module PreCancelled"
                )

            use cancellation = new CancellationTokenSource()
            cancellation.Cancel()

            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    workspace.Compiler.CompileAndLoadAsync(scriptPath, cancellation.Token))

            compilerArtifacts workspace |> should be Empty
        }

    [<Fact>]
    let ``supports concurrent reuse with unique assembly identities`` () =
        task {
            use workspace = new TestWorkspace()
            let firstPath =
                workspace.WriteSource(
                    "First.fsx",
                    withIdentityProcessor "module First\nlet value = 1"
                )

            let secondPath =
                workspace.WriteSource(
                    "Second.fsx",
                    withIdentityProcessor "module Second\nlet value = 2"
                )

            let! results =
                Task.WhenAll(
                    compile workspace.Compiler firstPath,
                    compile workspace.Compiler secondPath
                )

            let firstAssembly, _ = expectCompiled results[0]
            let secondAssembly, _ = expectCompiled results[1]

            firstAssembly.FullName |> should not' (equal secondAssembly.FullName)
            compilerArtifacts workspace |> should be Empty
        }

    [<Fact>]
    let ``discovers one processor and defers construction`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                processorSource
                    [ processorType
                          "type DeferredProcessor() ="
                          "    do invalidOp \"constructor failure\"" ]

            let scriptPath = workspace.WriteSource("DeferredProcessor.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result

            plugin.ProcessorType.Name |> should equal "DeferredProcessor"

            let error =
                Assert.Throws<InvalidOperationException>(fun () ->
                    plugin.CreateProcessor(unusedConnectionState) |> ignore)

            error.Message |> should equal "constructor failure"
        }

    [<Fact>]
    let ``creates a fresh adapted processor for each request`` () =
        task {
            use workspace = new TestWorkspace()

            let scriptPath =
                workspace.WriteSource(
                    "FreshProcessor.fsx",
                    processorSource [ processorType "type FreshProcessor() =" "" ]
                )

            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result
            let first = plugin.CreateProcessor(unusedConnectionState)
            let second = plugin.CreateProcessor(unusedConnectionState)

            Object.ReferenceEquals(first, second) |> should equal false
        }

    [<Fact>]
    let ``state-aware constructor receives the public connection state and is preferred`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                """module StatefulConstructor

open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Selection private () =
    static let mutable selected = ""
    static member Selected
        with get () = selected
        and set value = selected <- value

type Processor private (selection: string, connectionState: IConnectionState option) =
    do
        Selection.Selected <- selection

        match connectionState with
        | Some state ->
            state.GetOrAdd(
                "requests",
                fun () -> ConcurrentDictionary<string, int64>()
            )
            |> ignore
        | None -> ()

    new() = Processor("parameterless", None)
    new(connectionState: IConnectionState) = Processor("connection-state", Some connectionState)

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("StatefulConstructor.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result
            let state = ConnectionState()
            let publicState = ConnectionStateAdapter(state) :> IConnectionState
            let processor = plugin.CreateProcessor(publicState)

            let selectionType =
                plugin.Assembly.GetTypes()
                |> Array.find (fun value -> value.Name = "Selection")

            let selectedProperty =
                match selectionType.GetProperty "Selected" with
                | null -> failwith "The constructor selection property was not found."
                | value -> value

            selectedProperty.GetValue(null)
            |> should equal (box "connection-state")

            state.TryGet<ConcurrentDictionary<string, int64>>("requests").IsSome
            |> should equal true

            let! failures =
                ProcessorLifetime.disposeAllAsync [| processor |]
                |> _.AsTask()

            failures |> should be Empty
            do! (state :> IAsyncDisposable).DisposeAsync().AsTask()
        }

    [<Fact>]
    let ``compiled processor runs through retained segmented input and mixed output`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                """module CompiledMixedProcessor

open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, cancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()

            if not input.IsCompleted && input.Bytes.Length < 6L then
                ValueTask<ProcessorResult>({ RetainedLength = input.Bytes.Length })
            elif input.Bytes.IsEmpty then
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
            else
                output.AppendInput(0L, 2L)
                let generated = [| 88uy; 89uy; 90uy |]
                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))
                output.Advance(generated.Length)
                generated.AsSpan().Clear()
                output.AppendInput(4L, 2L)
                ValueTask<ProcessorResult>({ RetainedLength = 0L })
"""

            let scriptPath = workspace.WriteSource("CompiledMixedProcessor.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let plugin, _ = expectCompiledPlugin result
            let writes = ResizeArray<byte array>()
            let mutable preservedInputBacking = false
            let first = new TestOwner(bytes "AB")
            let second = new TestOwner(bytes "CDEF")

            let sink =
                { new PipeL4.Core.IProcessorPipelineSink with
                    member _.WriteAsync(value, cancellationToken) =
                        cancellationToken.ThrowIfCancellationRequested()

                        let segments = ResizeArray<ReadOnlyMemory<byte>>()
                        let mutable position = value.Start
                        let mutable segment = ReadOnlyMemory<byte>.Empty

                        while value.TryGet(&position, &segment) do
                            segments.Add segment

                        segments.Count |> should equal 3

                        let mutable firstSegment = Unchecked.defaultof<ArraySegment<byte>>
                        let mutable lastSegment = Unchecked.defaultof<ArraySegment<byte>>

                        MemoryMarshal.TryGetArray(segments[0], &firstSegment)
                        |> should equal true

                        MemoryMarshal.TryGetArray(segments[2], &lastSegment)
                        |> should equal true

                        preservedInputBacking <-
                            Object.ReferenceEquals(firstSegment.Array, first.Buffer)
                            && Object.ReferenceEquals(lastSegment.Array, second.Buffer)

                        writes.Add(sequenceBytes value)
                        ValueTask()

                    member _.CompleteAsync(cancellationToken) =
                        cancellationToken.ThrowIfCancellationRequested()
                        ValueTask() }

            use pipeline =
                new PipeL4.Core.ProcessorPipeline(
                    [| plugin.CreateProcessor(unusedConnectionState) |],
                    sink,
                    MemoryPool<byte>.Shared
                )

            do! pipeline.PushAsync(first, 2, CancellationToken.None)
            writes |> should be Empty
            do! pipeline.PushAsync(second, 4, CancellationToken.None)
            do! pipeline.CompleteAsync CancellationToken.None

            writes
            |> Seq.toArray
            |> Array.concat
            |> Encoding.ASCII.GetString
            |> should equal "ABXYZEF"

            preservedInputBacking |> should equal true
        }

    [<Fact>]
    let ``rejects scripts without a usable processor entry point`` () =
        task {
            use workspace = new TestWorkspace()

            let candidates =
                [| "NoProcessor", "module NoProcessor\nlet value = 42"
                   "PrivateProcessor",
                   processorSource [ processorType "type private PrivateProcessor() =" "" ]
                   "AbstractProcessor",
                   processorSource
                       [ processorType
                             "[<AbstractClass>]\ntype AbstractProcessor() ="
                             "    abstract Marker: unit -> unit" ]
                   "GenericProcessor",
                   processorSource [ processorType "type GenericProcessor<'T>() =" "" ]
                   "ConstructedProcessor",
                   processorSource [ processorType "type ConstructedProcessor(value: int) =" "" ] |]

            for name, source in candidates do
                let scriptPath = workspace.WriteSource($"{name}.fsx", source)
                let! result = compile workspace.Compiler scriptPath
                let diagnostics = expectFailed result

                diagnostics
                |> Array.exists (fun value ->
                    value.Severity = ScriptDiagnosticSeverity.Error
                    && value.Message.Contains("no usable implementation", StringComparison.Ordinal))
                |> should equal true
        }

    [<Fact>]
    let ``rejects ambiguous processor entry points and names the candidates`` () =
        task {
            use workspace = new TestWorkspace()

            let source =
                processorSource
                    [ processorType "type FirstProcessor() =" ""
                      processorType "type SecondProcessor() =" "" ]

            let scriptPath = workspace.WriteSource("AmbiguousProcessors.fsx", source)
            let! result = compile workspace.Compiler scriptPath
            let diagnostics = expectFailed result

            let diagnostic =
                diagnostics
                |> Array.find (fun value ->
                    value.Severity = ScriptDiagnosticSeverity.Error
                    && value.Message.Contains("multiple implementations", StringComparison.Ordinal))

            diagnostic.Message.Contains("FirstProcessor", StringComparison.Ordinal)
            |> should equal true

            diagnostic.Message.Contains("SecondProcessor", StringComparison.Ordinal)
            |> should equal true
        }
