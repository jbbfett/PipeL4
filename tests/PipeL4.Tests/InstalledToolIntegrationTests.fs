namespace PipeL4.Tests

open System
open System.ComponentModel
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Sockets
open System.Runtime.InteropServices
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Xunit

open CliAcceptanceTests

module InstalledToolIntegrationTests =
    let private loopback = IPAddress.Loopback

    type private TestWorkspace() =
        let rootPath =
            Path.Combine(Path.GetTempPath(), "PipeL4.InstalledToolTests", Guid.NewGuid().ToString "N")

        let scriptPath = Path.Combine(rootPath, "Increment.fsx")

        do Directory.CreateDirectory rootPath |> ignore

        member _.RootPath = rootPath
        member _.ScriptPath = scriptPath

        member _.WriteScript() =
            let source =
                String.concat
                    Environment.NewLine
                    [| "module InstalledToolIncrement"
                       ""
                       "open System"
                       "open System.Threading.Tasks"
                       "open PipeL4.Plugins"
                       ""
                       "type Processor() ="
                       "    interface IByteStreamProcessor with"
                       "        member _.ProcessAsync(input, output, cancellationToken) ="
                       "            cancellationToken.ThrowIfCancellationRequested()"
                       "            if not input.Bytes.IsEmpty then"
                       "                let generated = Array.zeroCreate<byte> (int input.Bytes.Length)"
                       "                let mutable offset = 0"
                       "                for segment in input.Bytes do"
                       "                    segment.Span.CopyTo(generated.AsSpan(offset))"
                       "                    offset <- offset + segment.Length"
                       "                for index in 0 .. generated.Length - 1 do"
                       "                    generated[index] <- generated[index] + 1uy"
                       "                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))"
                       "                output.Advance(generated.Length)"
                       "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" |]

            File.WriteAllText(scriptPath, source)

        member _.WriteScript(fileName: string, source: string) =
            let path = Path.Combine(rootPath, fileName)
            File.WriteAllText(path, source)
            path

        interface IDisposable with
            member _.Dispose() =
                if Directory.Exists rootPath then
                    Directory.Delete(rootPath, recursive = true)

    module private UnixSignals =
        [<DllImport("libc", EntryPoint = "kill", SetLastError = true)>]
        extern int private sendSignal (int processId, int signal)

        let interrupt processId =
            if sendSignal(processId, 2) <> 0 then
                raise (Win32Exception(Marshal.GetLastWin32Error()))

    let private repositoryRoot () =
        let rec find (directory: DirectoryInfo option) =
            match directory with
            | None -> failwith "Could not find the PipeL4 repository root."
            | Some current ->
                let solutionPath = Path.Combine(current.FullName, "PipeL4.sln")
                let projectPath = Path.Combine(current.FullName, "src", "PipeL4", "PipeL4.fsproj")

                if File.Exists solutionPath && File.Exists projectPath then
                    current.FullName
                else
                    find (current.Parent |> Option.ofObj)

        find (Some(DirectoryInfo(AppContext.BaseDirectory)))

    let private commandFailure name (result: ProcessResult) =
        String.concat
            Environment.NewLine
            [ $"{name} exited with code {result.ExitCode}."
              "--- stdout ---"
              result.StandardOutput
              "--- stderr ---"
              result.StandardError ]

    let private requireSuccessfulCommand name (result: ProcessResult) =
        if result.ExitCode <> 0 then
            failwith (commandFailure name result)

        if not result.OutputCaptureCompleted then
            failwith (commandFailure ($"{name} output capture") result)

    let private packageVersion packagePath =
        if not (File.Exists packagePath) then
            failwithf "The release candidate package does not exist at '%s'." packagePath

        let packageName =
            Path.GetFileNameWithoutExtension packagePath
            |> Option.ofObj
            |> Option.defaultWith (fun () -> failwithf "Could not read package name from '%s'." packagePath)
        let prefix = "PipeL4."

        if not (packageName.StartsWith(prefix, StringComparison.Ordinal)) then
            failwithf "Unexpected generated package name '%s'." packageName

        packageName.Substring(prefix.Length)

    let private generatedPackage packageDirectory =
        let packages = Directory.GetFiles(packageDirectory, "PipeL4.*.nupkg")

        if packages.Length <> 1 then
            failwithf
                "Expected exactly one generated PipeL4 package in '%s', found %d."
                packageDirectory
                packages.Length

        let packagePath = packages[0]
        packagePath, packageVersion packagePath

    let private installToolAsync
        (repository: string)
        (workspace: TestWorkspace)
        (cancellationToken: CancellationToken)
        =
        task {
            let packageDirectory = Path.Combine(workspace.RootPath, "package")
            let installDirectory = Path.Combine(workspace.RootPath, "tool")
            Directory.CreateDirectory packageDirectory |> ignore
            Directory.CreateDirectory installDirectory |> ignore

            let projectPath = Path.Combine(repository, "src", "PipeL4", "PipeL4.fsproj")

            let packArguments =
                [| "pack"
                   projectPath
                   "--configuration"
                   "Release"
                   "--no-restore"
                   "-p:PaketDisableCliTest=true"
                   "-p:BuildInParallel=false"
                   "--output"
                   packageDirectory |]

            let! packagePath, packageVersion =
                task {
                    match Environment.GetEnvironmentVariable("PIPEL4_PACKAGE_PATH") with
                    | null
                    | "" ->
                        use pack = ChildProcess.Start("dotnet", packArguments)
                        let! packResult = pack.WaitForExitAsync(TimeSpan.FromSeconds 60.0)
                        cancellationToken.ThrowIfCancellationRequested()
                        requireSuccessfulCommand "Release dotnet pack" packResult
                        return generatedPackage packageDirectory
                    | candidatePath ->
                        let candidatePath =
                            if Path.IsPathRooted candidatePath then
                                candidatePath
                            else
                                Path.Combine(repository, candidatePath)
                            |> Path.GetFullPath

                        return candidatePath, packageVersion candidatePath
                }

            let packageSource =
                Path.GetDirectoryName packagePath
                |> Option.ofObj
                |> Option.defaultWith (fun () ->
                    failwithf "Could not determine the local package source for '%s'." packagePath)

            let installArguments =
                [| "tool"
                   "install"
                   "PipeL4"
                   "--tool-path"
                   installDirectory
                   "--source"
                   packageSource
                   "--version"
                   packageVersion
                   "--no-cache" |]

            use install = ChildProcess.Start("dotnet", installArguments)
            let! installResult = install.WaitForExitAsync(TimeSpan.FromSeconds 60.0)
            cancellationToken.ThrowIfCancellationRequested()
            requireSuccessfulCommand "local PipeL4 tool installation" installResult

            let executableName = if OperatingSystem.IsWindows() then "pipel4.exe" else "pipel4"
            let executablePath = Path.Combine(installDirectory, executableName)

            if not (File.Exists executablePath) then
                failwithf "The installed PipeL4 command was not created at '%s'." executablePath

            return executablePath, packagePath, packageVersion
        }

    let private reservePort () =
        use listener = new TcpListener(loopback, 0)
        listener.Start()
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let private readExactly
        (stream: NetworkStream)
        length
        (cancellationToken: CancellationToken)
        =
        task {
            let result = Array.zeroCreate<byte> length
            let mutable offset = 0

            while offset < length do
                let! read = stream.ReadAsync(result.AsMemory(offset, length - offset), cancellationToken)

                if read = 0 then
                    failwithf "Stream ended after %d of %d expected bytes." offset length

                offset <- offset + read

            return result
        }

    let private expectEndOfStream
        (stream: NetworkStream)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> 1
            let! read = stream.ReadAsync(buffer.AsMemory(), cancellationToken)
            read |> should equal 0
        }

    let private connectEventually
        (child: ChildProcess)
        port
        (cancellationToken: CancellationToken)
        =
        task {
            let deadline = Stopwatch.StartNew()
            let timeout = TimeSpan.FromSeconds 15.0
            let mutable connected: TcpClient option = None

            while
                connected.IsNone
                && not child.HasExited
                && not cancellationToken.IsCancellationRequested
                && deadline.Elapsed < timeout do
                let candidate = new TcpClient(AddressFamily.InterNetwork)

                try
                    use attempt = new CancellationTokenSource(TimeSpan.FromMilliseconds 250.0)
                    do! candidate.ConnectAsync(loopback, port, attempt.Token)
                    connected <- Some candidate
                with
                | :? SocketException -> candidate.Dispose()
                | :? OperationCanceledException -> candidate.Dispose()

                if connected.IsNone then
                    do! Task.Delay(20, cancellationToken)

            match connected with
            | Some client -> return client
            | None when cancellationToken.IsCancellationRequested ->
                return raise (OperationCanceledException cancellationToken)
            | None when child.HasExited ->
                let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 5.0)
                return raise (InvalidOperationException(commandFailure "Installed PipeL4" result))
            | None ->
                return
                    raise (
                        TimeoutException(
                            String.concat
                                Environment.NewLine
                                [ $"The installed PipeL4 listener on port {port} did not become reachable."
                                  child.CapturedStandardOutput
                                  child.CapturedStandardError ]
                        )
                    )
        }

    let private stopToolAsync (child: ChildProcess) =
        task {
            if OperatingSystem.IsLinux() then
                UnixSignals.interrupt child.Id
                return! child.WaitForExitAsync(TimeSpan.FromSeconds 15.0)
            else
                return! child.KillAndWaitAsync()
        }

    let private boundedOutput (output: string) =
        let maximumCharacters = 16 * 1024

        if output.Length <= maximumCharacters then
            output
        else
            "[earlier output omitted]\n" + output.Substring(output.Length - maximumCharacters)

    let private childProcessFailure context (original: exn) (diagnostics: ProcessFailureDiagnostics) =
        let status =
            match diagnostics.NaturalExitCode, diagnostics.ForcedTerminationRequested, diagnostics.ProcessExited with
            | Some exitCode, _, _ -> $"Child process naturally exited with status code {exitCode}."
            | None, true, _ -> "Test infrastructure requested forced termination; its exit status is not reported as natural."
            | None, false, true -> "Child process exited, but no natural application exit status was captured."
            | None, false, false -> "Child process exit status is unavailable after bounded cleanup."

        let secondaryErrors =
            if diagnostics.SecondaryErrors.IsEmpty then
                []
            else
                [ "--- diagnostic or cleanup errors ---"
                  String.concat Environment.NewLine diagnostics.SecondaryErrors ]

        let message =
            String.concat
                Environment.NewLine
                ([ context
                   "The original exception and stack trace are preserved below and as the inner exception."
                   "Original exception:"
                   original.ToString()
                   status
                   $"Child process cleanup observed exit: {diagnostics.ProcessExited}."
                   $"Output capture completed: {diagnostics.OutputCaptureCompleted}."
                   "--- child stdout (tail, at most 16 KiB) ---"
                   boundedOutput diagnostics.StandardOutput
                   "--- child stderr (tail, at most 16 KiB) ---"
                   boundedOutput diagnostics.StandardError ]
                 @ secondaryErrors)

        InvalidOperationException(message, original)

    let private runFailingTrafficAsync
        (installedToolPath: string)
        (scriptPath: string)
        (scenarioName: string)
        : Task<InvalidOperationException>
        =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)
            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let backendPort = (backendListener.LocalEndpoint :?> IPEndPoint).Port
            let listenPort = reservePort ()

            let arguments =
                [| "--listen-address"
                   "127.0.0.1"
                   "--listen-port"
                   string listenPort
                   "--backend-host"
                   "127.0.0.1"
                   "--backend-port"
                   string backendPort
                   "--client-to-server-plugin"
                   scriptPath
                   "--disable-runtime-diagnostics" |]

            use proxy = ChildProcess.Start(installedToolPath, arguments)

            try
                use! client = connectEventually proxy listenPort timeout.Token
                use! backend = backendListener.AcceptTcpClientAsync(timeout.Token)
                let clientStream = client.GetStream()
                let backendStream = backend.GetStream()
                let payload = [| 1uy; 2uy; 3uy |]

                do! clientStream.WriteAsync(payload, timeout.Token)
                client.Client.Shutdown(SocketShutdown.Send)
                let! _ = readExactly backendStream payload.Length timeout.Token
                return failwith "The fault-injection processor unexpectedly forwarded the full payload."
            with original ->
                let! diagnostics = proxy.CollectFailureDiagnosticsAndCleanupAsync()

                return
                    childProcessFailure
                        $"Installed-tool scenario '{scenarioName}' failed after listener startup."
                        original
                        diagnostics
        }

    let private failureScript processorBody =
        String.concat
            Environment.NewLine
            [| "module InstalledToolFailure"
               ""
               "open System"
               "open System.Threading.Tasks"
               "open PipeL4.Plugins"
               ""
               "type Processor() ="
               "    interface IByteStreamProcessor with"
               "        member _.ProcessAsync(input, output, cancellationToken) ="
               "            cancellationToken.ThrowIfCancellationRequested()"
               processorBody
               "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" |]

    [<Fact>]
    let installedToolCompilesScriptAndTransformsLoopbackTcpTraffic () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 60.0)
            let repository = repositoryRoot ()
            use workspace = new TestWorkspace()
            workspace.WriteScript()

            let! installedToolPath, packagePath, packageVersion =
                installToolAsync repository workspace timeout.Token

            File.Exists packagePath |> should equal true
            installedToolPath.StartsWith(repository, StringComparison.Ordinal)
            |> should equal false

            use help = ChildProcess.Start(installedToolPath, [| "--help" |])
            let! helpResult = help.WaitForExitAsync(TimeSpan.FromSeconds 10.0)
            helpResult.ExitCode |> should equal 0
            helpResult.OutputCaptureCompleted |> should equal true
            helpResult.StandardError |> should equal ""
            helpResult.StandardOutput.Contains("Usage: pipel4 [options]", StringComparison.Ordinal)
            |> should equal true
            helpResult.StandardOutput.Contains("--version", StringComparison.Ordinal) |> should equal true

            use version = ChildProcess.Start(installedToolPath, [| "--version" |])
            let! versionResult = version.WaitForExitAsync(TimeSpan.FromSeconds 10.0)
            versionResult.ExitCode |> should equal 0
            versionResult.OutputCaptureCompleted |> should equal true
            versionResult.StandardError |> should equal ""
            versionResult.StandardOutput.Trim() |> should equal $"PipeL4 {packageVersion}"

            use backendListener = new TcpListener(loopback, 0)
            backendListener.Start()
            let backendPort = (backendListener.LocalEndpoint :?> IPEndPoint).Port
            let listenPort = reservePort ()

            let arguments =
                [| "--listen-address"
                   "127.0.0.1"
                   "--listen-port"
                   string listenPort
                   "--backend-host"
                   "127.0.0.1"
                   "--backend-port"
                   string backendPort
                   "--client-to-server-plugin"
                   workspace.ScriptPath
                   "--server-to-client-plugin"
                   workspace.ScriptPath
                   "--disable-runtime-diagnostics" |]

            use proxy = ChildProcess.Start(installedToolPath, arguments)
            try
                use! client = connectEventually proxy listenPort timeout.Token
                use! backend = backendListener.AcceptTcpClientAsync(timeout.Token)

                let clientStream = client.GetStream()
                let backendStream = backend.GetStream()
                let clientPayload = [| 1uy; 2uy; 3uy |]
                let backendReceive = readExactly backendStream clientPayload.Length timeout.Token

                do! clientStream.WriteAsync(clientPayload, timeout.Token)
                client.Client.Shutdown(SocketShutdown.Send)

                let! receivedAtBackend = backendReceive
                receivedAtBackend |> should equal [| 2uy; 3uy; 4uy |]
                do! expectEndOfStream backendStream timeout.Token

                let backendPayload = [| 10uy; 20uy; 30uy |]
                let clientReceive = readExactly clientStream backendPayload.Length timeout.Token
                do! backendStream.WriteAsync(backendPayload, timeout.Token)
                backend.Client.Shutdown(SocketShutdown.Send)

                let! receivedAtClient = clientReceive
                receivedAtClient |> should equal [| 11uy; 21uy; 31uy |]
                do! expectEndOfStream clientStream timeout.Token
                proxy.HasExited |> should equal false

                let! shutdownResult = stopToolAsync proxy

                if OperatingSystem.IsLinux() then
                    shutdownResult.ExitCode |> should equal 0

                shutdownResult.OutputCaptureCompleted |> should equal true
                shutdownResult.StandardError.Contains("Plugin configuration error:", StringComparison.Ordinal)
                |> should equal false
            with original ->
                let! diagnostics = proxy.CollectFailureDiagnosticsAndCleanupAsync()

                return
                    raise (
                        childProcessFailure
                            "Installed-tool TCP forwarding scenario failed after proxy startup."
                            original
                            diagnostics
                    )

            do!
                task {
                    let listenPort = reservePort ()
                    let examplePath = Path.Combine(repository, "examples", "AsciiUppercase.fsx")

                    let arguments =
                        [| "--listen-address"
                           "127.0.0.1"
                           "--listen-port"
                           string listenPort
                           "--backend-host"
                           "127.0.0.1"
                           "--backend-port"
                           string backendPort
                           "--client-to-server-plugin"
                           examplePath
                           "--disable-runtime-diagnostics" |]

                    use exampleProxy = ChildProcess.Start(installedToolPath, arguments)

                    try
                        use! client = connectEventually exampleProxy listenPort timeout.Token
                        use! backend = backendListener.AcceptTcpClientAsync(timeout.Token)

                        let clientStream = client.GetStream()
                        let backendStream = backend.GetStream()
                        let payload = Encoding.UTF8.GetBytes("Hello, PipeL4!\n")
                        let backendReceive = readExactly backendStream payload.Length timeout.Token

                        do! clientStream.WriteAsync(payload, timeout.Token)
                        client.Client.Shutdown(SocketShutdown.Send)

                        let! receivedAtBackend = backendReceive
                        receivedAtBackend
                        |> should equal (Encoding.UTF8.GetBytes("HELLO, PIPEL4!\n"))
                        do! expectEndOfStream backendStream timeout.Token

                        let response = Encoding.UTF8.GetBytes("backend reply\n")
                        let clientReceive = readExactly clientStream response.Length timeout.Token
                        do! backendStream.WriteAsync(response, timeout.Token)
                        backend.Client.Shutdown(SocketShutdown.Send)

                        let! receivedAtClient = clientReceive
                        receivedAtClient |> should equal response
                        do! expectEndOfStream clientStream timeout.Token

                        let! shutdownResult = stopToolAsync exampleProxy

                        if OperatingSystem.IsLinux() then
                            shutdownResult.ExitCode |> should equal 0

                        shutdownResult.OutputCaptureCompleted |> should equal true
                    with original ->
                        let! diagnostics = exampleProxy.CollectFailureDiagnosticsAndCleanupAsync()

                        return
                            raise (
                                childProcessFailure
                                    "Installed-tool AsciiUppercase example scenario failed."
                                    original
                                    diagnostics
                            )
                }
        }

    [<Fact>]
    let installedToolPostStartupFailuresIncludeChildDiagnostics () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 60.0)
            let repository = repositoryRoot ()
            use workspace = new TestWorkspace()

            let! installedToolPath, _, _ = installToolAsync repository workspace timeout.Token

            let processorExceptionScript =
                failureScript "            raise (InvalidOperationException(\"DT02 controlled processor failure\"))"

            let processorExceptionPath = workspace.WriteScript("ProcessorException.fsx", processorExceptionScript)

            let! processorException =
                runFailingTrafficAsync installedToolPath processorExceptionPath "processor exception"

            processorException.Message.Contains("Stream ended after 0 of 3 expected bytes.", StringComparison.Ordinal)
            |> should equal true

            processorException.Message.Contains("DT02 controlled processor failure", StringComparison.Ordinal)
            |> should equal true

            processorException.Message.Contains("Original exception:", StringComparison.Ordinal)
            |> should equal true

            processorException.Message.Contains("at ", StringComparison.Ordinal) |> should equal true

            processorException.Message.Contains("scenario 'processor exception'", StringComparison.Ordinal)
            |> should equal true

            processorException.Message.Contains("--- child stdout", StringComparison.Ordinal) |> should equal true
            processorException.Message.Contains("--- child stderr", StringComparison.Ordinal) |> should equal true

            processorException.Message.Contains("Test infrastructure requested forced termination; its exit status is not reported as natural.", StringComparison.Ordinal)
            |> should equal true

            processorException.Message.Contains("Child process cleanup observed exit: True.", StringComparison.Ordinal)
            |> should equal true

            processorException.Message.Contains("Output capture completed: True.", StringComparison.Ordinal)
            |> should equal true

            let exitScript =
                failureScript
                    (String.concat
                        Environment.NewLine
                        [ "            if not input.Bytes.IsEmpty then"
                          "                Console.Error.WriteLine(\"DT02 child stderr marker\")"
                          "                Environment.Exit(23)" ])

            let exitScriptPath = workspace.WriteScript("ProcessExit.fsx", exitScript)
            let! processExit = runFailingTrafficAsync installedToolPath exitScriptPath "explicit process exit"

            processExit.Message.Contains("Stream ended after 0 of 3 expected bytes.", StringComparison.Ordinal)
            |> should equal true

            processExit.Message.Contains("DT02 child stderr marker", StringComparison.Ordinal)
            |> should equal true

            processExit.Message.Contains("scenario 'explicit process exit'", StringComparison.Ordinal)
            |> should equal true

            processExit.Message.Contains("naturally exited with status code 23", StringComparison.Ordinal)
            |> should equal true

            processExit.Message.Contains("Output capture completed: True.", StringComparison.Ordinal)
            |> should equal true

            let workspacePath = workspace.RootPath
            (workspace :> IDisposable).Dispose()
            Directory.Exists workspacePath |> should equal false
        }
