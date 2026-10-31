namespace PipeL4.Tests

open System
open System.ComponentModel
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Sockets
open System.Runtime.InteropServices
open System.Runtime.ExceptionServices
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Xunit

module CliAcceptanceTests =
    [<NoEquality; NoComparison>]
    type private ProcessResult =
        { ExitCode: int
          StandardOutput: string
          StandardError: string
          OutputCaptureCompleted: bool }

    [<Sealed>]
    type private ProcessOutputCapture() =
        let gate = obj ()
        let buffer = StringBuilder()
        let lines = ResizeArray<string>()
        let completed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        member _.Receive(line: string | null) =
            match line with
            | null -> lock gate (fun () -> completed.TrySetResult(()) |> ignore)
            | capturedLine ->
                lock gate (fun () ->
                    buffer.AppendLine(capturedLine) |> ignore
                    lines.Add(capturedLine))

        member _.Text = lock gate (fun () -> buffer.ToString())
        member _.Completion = completed.Task

        member _.HasJsonStringProperties(expectedProperties: (string * string) list) =
            lock gate (fun () ->
                let rec hasProperty (propertyName: string) (expectedValue: string) (element: JsonElement) =
                    match element.ValueKind with
                    | JsonValueKind.Object ->
                        element.EnumerateObject()
                        |> Seq.exists (fun property ->
                            (property.NameEquals(propertyName)
                             && property.Value.ValueKind = JsonValueKind.String
                             && property.Value.ValueEquals(expectedValue))
                            || hasProperty propertyName expectedValue property.Value)
                    | JsonValueKind.Array ->
                        element.EnumerateArray()
                        |> Seq.exists (hasProperty propertyName expectedValue)
                    | _ -> false

                lines
                |> Seq.exists (fun line ->
                    try
                        use document = JsonDocument.Parse(line)
                        expectedProperties
                        |> List.forall (fun (propertyName, expectedValue) ->
                            hasProperty propertyName expectedValue document.RootElement)
                    with :? JsonException ->
                        false))

        member this.HasJsonStringProperty(propertyName: string, expectedValue: string) =
            this.HasJsonStringProperties([ propertyName, expectedValue ])

    [<Sealed>]
    type private ChildProcess private (
        childProcess: Process,
        standardOutput: ProcessOutputCapture,
        standardError: ProcessOutputCapture
    ) =
        let mutable outputReadCancelled = false

        let cancelOutputRead cancel =
            try
                cancel ()
                outputReadCancelled <- true
            with
            | :? InvalidOperationException -> outputReadCancelled <- true

        let waitForOutputCaptureAsync (timeout: TimeSpan) =
            task {
                let completion = Task.WhenAll(standardOutput.Completion, standardError.Completion)
                let! finished = Task.WhenAny(completion, Task.Delay timeout)

                if obj.ReferenceEquals(finished, completion) then
                    let! _ = completion
                    return true
                else
                    cancelOutputRead (fun () -> childProcess.CancelOutputRead())
                    cancelOutputRead (fun () -> childProcess.CancelErrorRead())
                    return false
            }

        let killProcessTree () =
            if not childProcess.HasExited then
                try
                    childProcess.Kill(entireProcessTree = true)
                with
                | :? InvalidOperationException -> ()
                | :? Win32Exception when childProcess.HasExited -> ()

        let waitForProcessExitAsync (timeout: TimeSpan) : Task<bool> =
            Task.Run(fun () ->
                if childProcess.HasExited then
                    true
                else
                    childProcess.WaitForExit(int timeout.TotalMilliseconds))

        let killAndReapAsync (timeout: TimeSpan) =
            task {
                killProcessTree ()
                let! exited = waitForProcessExitAsync timeout

                if exited then
                    let! captured = waitForOutputCaptureAsync (TimeSpan.FromSeconds 2.0)
                    return exited, captured
                else
                    cancelOutputRead (fun () -> childProcess.CancelOutputRead())
                    cancelOutputRead (fun () -> childProcess.CancelErrorRead())
                    return false, false
            }

        let result outputCaptureCompleted =
            { ExitCode = childProcess.ExitCode
              StandardOutput = standardOutput.Text
              StandardError = standardError.Text
              OutputCaptureCompleted = outputCaptureCompleted }

        member _.Id = childProcess.Id
        member _.HasExited = childProcess.HasExited
        member _.HasListenerStartedRecord(endpoint: string) =
            standardOutput.HasJsonStringProperties(
                [ "ListenEndpoint", endpoint
                  "{OriginalFormat}", "TCP listener started on {ListenEndpoint} with backend {BackendEndpoint}" ]
            )
        member _.CapturedStandardOutput = standardOutput.Text
        member _.CapturedStandardError = standardError.Text

        member _.WaitForExitAsync(timeout: TimeSpan) =
            task {
                let! exited = waitForProcessExitAsync timeout

                if not exited then
                    let! exited, captured = killAndReapAsync (TimeSpan.FromSeconds 5.0)
                    let outputState =
                        if captured then "redirected output was fully captured"
                        else "redirected output capture was bounded and cancelled"

                    let details =
                        String.concat
                            Environment.NewLine
                            [ sprintf
                                  "The PipeL4 subprocess did not exit before the timeout. Cleanup exited=%b; %s."
                                  exited
                                  outputState
                              standardOutput.Text
                              standardError.Text ]

                    raise (TimeoutException(details))

                let! captured = waitForOutputCaptureAsync (TimeSpan.FromSeconds 3.0)
                return result captured
            }

        member _.KillAndWaitAsync() =
            task {
                let! exited, captured = killAndReapAsync (TimeSpan.FromSeconds 5.0)

                if not exited then
                    raise (TimeoutException("The PipeL4 subprocess remained alive after process-tree cleanup."))

                return result captured
            }

        static member private StartWithReadSetup(
            executablePath: string,
            arguments: string array,
            beginOutputRead: Process -> unit,
            beginErrorRead: Process -> unit
        ) =
            let startInfo = ProcessStartInfo()
            startInfo.FileName <- executablePath
            startInfo.WorkingDirectory <- Path.GetTempPath()
            startInfo.UseShellExecute <- false
            startInfo.RedirectStandardOutput <- true
            startInfo.RedirectStandardError <- true

            for argument in arguments do
                startInfo.ArgumentList.Add argument

            let childProcess = new Process(StartInfo = startInfo)
            let standardOutput = ProcessOutputCapture()
            let standardError = ProcessOutputCapture()
            let mutable outputReadAttempted = false
            let mutable errorReadAttempted = false

            let started =
                try
                    childProcess.Start()
                with startError ->
                    try
                        childProcess.Dispose()
                    with _ -> ()

                    ExceptionDispatchInfo.Capture(startError).Throw()
                    false

            if not started then
                childProcess.Dispose()
                failwith "The PipeL4 subprocess could not be started."

            try
                childProcess.OutputDataReceived.Add(fun event -> standardOutput.Receive(event.Data))
                childProcess.ErrorDataReceived.Add(fun event -> standardError.Receive(event.Data))
                outputReadAttempted <- true
                beginOutputRead childProcess
                errorReadAttempted <- true
                beginErrorRead childProcess
                new ChildProcess(childProcess, standardOutput, standardError)
            with setupError ->
                let attemptCleanup cleanup =
                    try
                        cleanup ()
                    with _ -> ()

                attemptCleanup (fun () ->
                    if not childProcess.HasExited then
                        childProcess.Kill(entireProcessTree = true))
                attemptCleanup (fun () -> childProcess.WaitForExit(3000) |> ignore)

                if outputReadAttempted then
                    attemptCleanup (fun () -> childProcess.CancelOutputRead())

                if errorReadAttempted then
                    attemptCleanup (fun () -> childProcess.CancelErrorRead())

                attemptCleanup (fun () -> childProcess.Dispose())
                ExceptionDispatchInfo.Capture(setupError).Throw()
                Unchecked.defaultof<ChildProcess>

        static member Start(executablePath: string, arguments: string array) =
            ChildProcess.StartWithReadSetup(
                executablePath,
                arguments,
                (fun childToRead -> childToRead.BeginOutputReadLine()),
                (fun childToRead -> childToRead.BeginErrorReadLine())
            )

        static member StartWithReadSetupForTest(
            executablePath: string,
            arguments: string array,
            beginOutputRead: Process -> unit,
            beginErrorRead: Process -> unit
        ) =
            ChildProcess.StartWithReadSetup(executablePath, arguments, beginOutputRead, beginErrorRead)

        interface IDisposable with
            member _.Dispose() =
                try
                    killProcessTree ()

                    if childProcess.WaitForExit(3000) then
                        let completion = Task.WhenAll(standardOutput.Completion, standardError.Completion)

                        if not (completion.Wait(TimeSpan.FromSeconds 2.0)) then
                            cancelOutputRead (fun () -> childProcess.CancelOutputRead())
                            cancelOutputRead (fun () -> childProcess.CancelErrorRead())
                    elif not outputReadCancelled then
                        cancelOutputRead (fun () -> childProcess.CancelOutputRead())
                        cancelOutputRead (fun () -> childProcess.CancelErrorRead())
                    else
                        ()
                finally
                    childProcess.Dispose()

    type private TestWorkspace() =
        let rootPath =
            Path.Combine(Path.GetTempPath(), "PipeL4.CliAcceptanceTests", Guid.NewGuid().ToString "N")

        do Directory.CreateDirectory rootPath |> ignore

        member _.WriteSource(fileName: string, source: string) =
            let path = Path.Combine(rootPath, fileName)
            File.WriteAllText(path, source)
            path

        member _.GetPath(fileName: string) = Path.Combine(rootPath, fileName)

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

    module private UnixFifo =
        [<DllImport("libc", EntryPoint = "mkfifo", SetLastError = true, CharSet = CharSet.Ansi)>]
        extern int private createFifo (string path, uint32 mode)

        [<DllImport("libc", EntryPoint = "open", SetLastError = true, CharSet = CharSet.Ansi)>]
        extern int private openFile (string path, int flags)

        [<DllImport("libc", EntryPoint = "close", SetLastError = true)>]
        extern int private closeFile (int fileDescriptor)

        let create path =
            if createFifo(path, 0x180u) <> 0 then
                raise (Win32Exception(Marshal.GetLastWin32Error()))

        let openWriterWhenReaderIsReadyAsync
            (child: ChildProcess)
            path
            timeout
            =
            task {
                let stopwatch = Stopwatch.StartNew()
                let mutable fileDescriptor = -1

                while
                    fileDescriptor < 0
                    && not child.HasExited
                    && stopwatch.Elapsed < timeout do
                    // O_WRONLY | O_NONBLOCK returns ENXIO until the child has opened
                    // the FIFO for reading. This is the startup coordination point.
                    let candidate = openFile(path, 1 ||| 0x800)

                    if candidate >= 0 then
                        fileDescriptor <- candidate
                    else
                        let errorCode = Marshal.GetLastWin32Error()

                        if errorCode <> 6 then
                            raise (Win32Exception(errorCode))

                        do! Task.Delay(TimeSpan.FromMilliseconds 10.0)

                if fileDescriptor < 0 then
                    raise (
                        TimeoutException(
                            $"The child did not open script FIFO '{path}' for reading."
                            + Environment.NewLine
                            + child.CapturedStandardOutput
                            + child.CapturedStandardError
                        )
                    )

                return fileDescriptor
            }

        let closeWriter fileDescriptor =
            if closeFile fileDescriptor <> 0 then
                raise (Win32Exception(Marshal.GetLastWin32Error()))

    module private LinuxPidFd =
        [<Struct; StructLayout(LayoutKind.Sequential)>]
        type private PollDescriptor =
            { mutable FileDescriptor: int
              mutable Events: int16
              mutable ReturnedEvents: int16 }

        [<DllImport("libc", EntryPoint = "pidfd_open", SetLastError = true)>]
        extern int private openPidFd (int processId, uint32 flags)

        [<DllImport("libc", EntryPoint = "pidfd_send_signal", SetLastError = true)>]
        extern int private sendPidFdSignal (int fileDescriptor, int signal, IntPtr signalInfo, uint32 flags)

        [<DllImport("libc", EntryPoint = "poll", SetLastError = true)>]
        extern int private poll (PollDescriptor& descriptors, uint32 count, int timeoutMilliseconds)

        [<DllImport("libc", EntryPoint = "close", SetLastError = true)>]
        extern int private closeFileDescriptor (int fileDescriptor)

        let private hasExited fileDescriptor timeoutMilliseconds =
            let mutable descriptor =
                { FileDescriptor = fileDescriptor
                  Events = 1s
                  ReturnedEvents = 0s }

            let result = poll (&descriptor, 1u, timeoutMilliseconds)

            if result < 0 then
                raise (Win32Exception(Marshal.GetLastWin32Error()))

            result > 0 && descriptor.ReturnedEvents <> 0s

        [<Sealed>]
        type PidFd private (fileDescriptor: int) =
            let mutable disposed = false

            member _.CanSendSignalZero =
                if disposed then
                    raise (ObjectDisposedException(nameof PidFd))

                sendPidFdSignal(fileDescriptor, 0, IntPtr.Zero, 0u) = 0

            member _.HasExited =
                if disposed then
                    raise (ObjectDisposedException(nameof PidFd))

                hasExited fileDescriptor 0

            member this.StopAndWait(timeout: TimeSpan) =
                if disposed then
                    raise (ObjectDisposedException(nameof PidFd))

                if not this.HasExited then
                    let result = sendPidFdSignal(fileDescriptor, 9, IntPtr.Zero, 0u)

                    if result < 0 then
                        let errorCode = Marshal.GetLastWin32Error()

                        if errorCode <> 3 then
                            raise (Win32Exception(errorCode))

                let timeoutMilliseconds =
                    int (Math.Clamp(timeout.TotalMilliseconds, 0.0, float Int32.MaxValue))

                hasExited fileDescriptor timeoutMilliseconds

            member _.Dispose() =
                if not disposed then
                    disposed <- true
                    closeFileDescriptor fileDescriptor |> ignore

            static member TryOpen(processId: int) =
                try
                    let fileDescriptor = openPidFd(processId, 0u)

                    if fileDescriptor < 0 then
                        None
                    else
                        Some(PidFd(fileDescriptor))
                with
                | :? EntryPointNotFoundException -> None
                | :? DllNotFoundException -> None

        let isSupported () =
            try
                match PidFd.TryOpen(Environment.ProcessId) with
                | None -> false
                | Some ownProcess ->
                    try
                        ownProcess.CanSendSignalZero
                    finally
                        ownProcess.Dispose()
            with
            | :? EntryPointNotFoundException -> false
            | :? DllNotFoundException -> false

    let private applicationHostPath () =
        let rec findRepositoryRoot (directory: DirectoryInfo option) =
            match directory with
            | None ->
                failwithf
                    "Could not find the PipeL4 repository root above test output '%s'."
                    AppContext.BaseDirectory
            | Some current ->
                let solutionPath = Path.Combine(current.FullName, "PipeL4.sln")
                let applicationProjectPath = Path.Combine(current.FullName, "src", "PipeL4", "PipeL4.fsproj")

                if File.Exists solutionPath && File.Exists applicationProjectPath then
                    current.FullName
                else
                    findRepositoryRoot (current.Parent |> Option.ofObj)

        let outputDirectory = DirectoryInfo(AppContext.BaseDirectory)
        let targetFramework = outputDirectory.Name

        let configuration =
            outputDirectory.Parent
            |> Option.ofObj
            |> Option.map _.Name
            |> Option.defaultWith (fun () -> failwith "The test output path has no build configuration directory.")

        let repositoryRoot = findRepositoryRoot (Some outputDirectory)
        let executableName = if OperatingSystem.IsWindows() then "PipeL4.exe" else "PipeL4"

        let executablePath =
            Path.Combine(
                repositoryRoot,
                "src",
                "PipeL4",
                "bin",
                configuration,
                targetFramework,
                executableName
            )

        if not (File.Exists executablePath) then
            failwithf
                "The PipeL4 application host was not built at the expected project output path '%s'."
                executablePath

        executablePath

    let private reservePort () =
        use listener = new TcpListener(IPAddress.Loopback, 0)
        listener.Start()
        (listener.LocalEndpoint :?> IPEndPoint).Port

    let private reserveListenerPort () =
        let listener = new TcpListener(IPAddress.Loopback, 0)
        listener.Start()
        listener

    let private requiredArguments listenPort backendPort =
        [| "--listen-address"
           "127.0.0.1"
           "--listen-port"
           string listenPort
           "--backend-host"
           "127.0.0.1"
           "--backend-port"
           string backendPort |]

    let private repeatedArguments optionName paths =
        paths |> Array.collect (fun path -> [| optionName; path |])

    let private transformSource moduleName expression =
        String.concat
            Environment.NewLine
            [ $"module {moduleName}"
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
              $"                for index in 0 .. generated.Length - 1 do generated[index] <- {expression}"
              "                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))"
              "                output.Advance(generated.Length)"
              "            ValueTask<ProcessorResult>({ RetainedLength = 0L })" ]

    let private fsharpStringLiteral (value: string) =
        "\"" + value.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\""

    let private waitForListenerAsync
        (child: ChildProcess)
        (listenEndpoint: IPEndPoint)
        (timeout: TimeSpan)
        =
        task {
            let stopwatch = Stopwatch.StartNew()
            let mutable earlyExit: ProcessResult option = None
            let mutable lastConnectionError: string option = None
            let expectedEndpoint = listenEndpoint.ToString()
            let mutable startupRecordObserved = false

            let captureExit () =
                task {
                    let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 5.0)
                    earlyExit <- Some result
                }

            let processExitError (result: ProcessResult) =
                let details =
                    String.concat
                        Environment.NewLine
                        [ $"PipeL4 exited during listener readiness (code {result.ExitCode})."
                          result.StandardOutput
                          result.StandardError ]

                InvalidOperationException(details)

            // Wait for the child-owned structured log record before opening any socket.
            while not startupRecordObserved && earlyExit.IsNone && stopwatch.Elapsed < timeout do
                if child.HasExited then
                    do! captureExit ()
                else
                    startupRecordObserved <-
                        child.HasListenerStartedRecord(expectedEndpoint)

                    if not startupRecordObserved then
                        let remainingMilliseconds = (timeout - stopwatch.Elapsed).TotalMilliseconds

                        if remainingMilliseconds > 0.0 then
                            do!
                                Task.Delay(
                                    TimeSpan.FromMilliseconds(min 25.0 remainingMilliseconds)
                                )

            if earlyExit.IsNone && child.HasExited then
                do! captureExit ()

            match earlyExit with
            | Some result -> return raise (processExitError result)
            | None when not startupRecordObserved ->
                let details =
                    String.concat
                        Environment.NewLine
                        [ $"PipeL4 did not report listener {expectedEndpoint} within {timeout}."
                          child.CapturedStandardOutput
                          child.CapturedStandardError ]

                return raise (TimeoutException(details))
            | None ->
                let mutable connection: TcpClient option = None

                while connection.IsNone && earlyExit.IsNone && stopwatch.Elapsed < timeout do
                    if child.HasExited then
                        do! captureExit ()
                    else
                        let candidate = new TcpClient(AddressFamily.InterNetwork)
                        let remainingMilliseconds = (timeout - stopwatch.Elapsed).TotalMilliseconds

                        try
                            if remainingMilliseconds <= 0.0 then
                                raise (OperationCanceledException("Readiness deadline expired."))

                            use connectTimeout =
                                new CancellationTokenSource(
                                    TimeSpan.FromMilliseconds(min 300.0 remainingMilliseconds)
                                )

                            do!
                                candidate.ConnectAsync(
                                    listenEndpoint.Address,
                                    listenEndpoint.Port,
                                    connectTimeout.Token
                                )

                            if child.HasExited then
                                candidate.Dispose()
                                do! captureExit ()
                            else
                                connection <- Some candidate
                        with
                        | :? SocketException as error ->
                            candidate.Dispose()
                            lastConnectionError <- Some error.Message
                        | :? OperationCanceledException as error ->
                            candidate.Dispose()
                            lastConnectionError <- Some error.Message

                        if connection.IsNone && earlyExit.IsNone then
                            let remainingMilliseconds = (timeout - stopwatch.Elapsed).TotalMilliseconds

                            if remainingMilliseconds > 0.0 then
                                do!
                                    Task.Delay(
                                        TimeSpan.FromMilliseconds(min 25.0 remainingMilliseconds)
                                    )

                if earlyExit.IsNone && child.HasExited then
                    do! captureExit ()

                match earlyExit, connection with
                | Some result, Some client ->
                    client.Dispose()
                    return raise (processExitError result)
                | Some result, None ->
                    return raise (processExitError result)
                | None, Some client -> return client
                | None, None ->
                    let detail = lastConnectionError |> Option.defaultValue "no connection attempt completed"
                    let readinessDiagnostic =
                        $"Child confirmed listener {expectedEndpoint}; TCP readiness timed out after {timeout}. "
                        + $"Last connection error: {detail}."

                    return
                        raise (
                            TimeoutException(
                                String.concat
                                    Environment.NewLine
                                    [ readinessDiagnostic
                                      child.CapturedStandardOutput
                                      child.CapturedStandardError ]
                            )
                        )
        }

    let private connectToListenerAsync port (cancellationToken: CancellationToken) =
        task {
            let client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(IPAddress.Loopback, port, cancellationToken)
                return client
            with error ->
                client.Dispose()
                return raise error
        }

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

    let private expectConnectionClosed
        (stream: NetworkStream)
        (cancellationToken: CancellationToken)
        =
        task {
            let buffer = Array.zeroCreate<byte> 1

            try
                let! read = stream.ReadAsync(buffer.AsMemory(), cancellationToken)
                read |> should equal 0
            with
            | :? IOException -> ()
            | :? SocketException -> ()
            | :? ObjectDisposedException -> ()
        }

    let private expectConnectionFailure port (cancellationToken: CancellationToken) =
        task {
            use client = new TcpClient(AddressFamily.InterNetwork)

            try
                do! client.ConnectAsync(IPAddress.Loopback, port, cancellationToken)
                failwith "Expected the PipeL4 listener to remain stopped."
            with :? SocketException -> ()
        }

    let private shellQuote (value: string) =
        "'" + value.Replace("'", "'\\''") + "'"

    let private processIdFromFile path =
        File.ReadAllText(path).Trim() |> Int32.Parse

    let private waitForPidFdAsync (child: ChildProcess) path timeout =
        task {
            let stopwatch = Stopwatch.StartNew()
            let mutable processIdentity: LinuxPidFd.PidFd option = None

            // Callers keep the spawning shell alive while this PID is written; all later signals use the pidfd.
            while
                processIdentity.IsNone
                && not child.HasExited
                && stopwatch.Elapsed < timeout do
                if File.Exists path && not child.HasExited then
                    try
                        processIdentity <- LinuxPidFd.PidFd.TryOpen(processIdFromFile path)
                    with
                    | :? IOException -> ()
                    | :? FormatException -> ()
                    | :? OverflowException -> ()

                if processIdentity.IsNone then
                    do! Task.Delay(20)

            match processIdentity with
            | Some identity when not identity.HasExited -> return identity
            | Some identity ->
                identity.Dispose()
                return
                    raise (
                        InvalidOperationException(
                            "The test-owned descendant exited before its pidfd was retained."
                        )
                    )
            | None ->
                let diagnostic =
                    String.concat
                        Environment.NewLine
                        [ $"Could not retain a pidfd for the test-owned descendant within {timeout}."
                          child.CapturedStandardOutput
                          child.CapturedStandardError ]

                return
                    raise (TimeoutException(diagnostic))
        }

    let private cleanupPidFd (processIdentity: LinuxPidFd.PidFd option) =
        processIdentity
        |> Option.iter (fun identity ->
            try
                identity.StopAndWait(TimeSpan.FromSeconds 3.0) |> ignore
            with _ ->
                ()

            try
                identity.Dispose()
            with _ ->
                ())

    let private assertStandardErrorContains (expected: string) (result: ProcessResult) =
        result.StandardError.Contains(expected, StringComparison.Ordinal)
        |> should equal true

    let private assertStandardOutputContains (expected: string) (result: ProcessResult) =
        result.StandardOutput.Contains(expected, StringComparison.Ordinal)
        |> should equal true

    let private assertOutputCaptureCompleted (result: ProcessResult) =
        if not result.OutputCaptureCompleted then
            failwithf
                "The subprocess output streams did not finish within their bound. Captured stdout:%sCaptured stderr:%s"
                result.StandardOutput
                result.StandardError

    [<Fact>]
    let ``built CLI applies ordered directional scripts with finite capacities and propagates EOF`` () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(IPAddress.Loopback, 0)
            backendListener.Start()

            let addOnePath =
                workspace.WriteSource("AddOne.fsx", transformSource "AddOne" "generated[index] + 1uy")

            let doublePath =
                workspace.WriteSource("Double.fsx", transformSource "Double" "generated[index] * 2uy")

            let addFivePath =
                workspace.WriteSource("AddFive.fsx", transformSource "AddFive" "generated[index] + 5uy")

            let listenPort = reservePort ()
            let backendPort = (backendListener.LocalEndpoint :?> IPEndPoint).Port

            let arguments =
                Array.concat
                    [| requiredArguments listenPort backendPort
                       repeatedArguments "--client-to-server-plugin" [| addOnePath; doublePath |]
                       repeatedArguments "--server-to-client-plugin" [| addFivePath |]
                       [| "--client-to-server-logical-capacity-bytes"
                          "64"
                          "--server-to-client-logical-capacity-bytes"
                          "64"
                          "--connection-physical-capacity-bytes"
                          "131072"
                          "--disable-runtime-diagnostics" |] |]

            use child = ChildProcess.Start(applicationHostPath (), arguments)
            let listenEndpoint = IPEndPoint(IPAddress.Loopback, listenPort)
            use! client = waitForListenerAsync child listenEndpoint (TimeSpan.FromSeconds 10.0)

            use! backend = backendListener.AcceptTcpClientAsync(timeout.Token)
            let clientStream = client.GetStream()
            let backendStream = backend.GetStream()

            let clientPayload = [| 2uy; 3uy; 4uy |]
            let receiveAtBackend = readExactly backendStream clientPayload.Length timeout.Token
            do! clientStream.WriteAsync(clientPayload, timeout.Token)
            client.Client.Shutdown(SocketShutdown.Send)

            let! receivedAtBackend = receiveAtBackend
            receivedAtBackend |> should equal [| 6uy; 8uy; 10uy |]
            do! expectEndOfStream backendStream timeout.Token

            let backendPayload = [| 10uy; 20uy; 30uy |]
            let receiveAtClient = readExactly clientStream backendPayload.Length timeout.Token
            do! backendStream.WriteAsync(backendPayload, timeout.Token)
            backend.Client.Shutdown(SocketShutdown.Send)

            let! receivedAtClient = receiveAtClient
            receivedAtClient |> should equal [| 15uy; 25uy; 35uy |]
            do! expectEndOfStream clientStream timeout.Token
            child.HasExited |> should equal false
        }

    [<Fact>]
    let ``invalid CLI arguments return the established error code and usage`` () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
            let listenPort = reservePort ()
            let backendPort = reservePort ()

            let arguments =
                Array.append (requiredArguments listenPort backendPort) [| "--unknown-option" |]

            use child = ChildProcess.Start(applicationHostPath (), arguments)
            let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 10.0)

            result.ExitCode |> should equal 2
            assertOutputCaptureCompleted result
            assertStandardErrorContains "Error: Unknown argument '--unknown-option'." result
            assertStandardErrorContains "Usage: PipeL4" result
            do! expectConnectionFailure listenPort timeout.Token
        }

    [<Fact>]
    let ``plugin compilation failures return configuration error and do not start the listener`` () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
            use workspace = new TestWorkspace()

            let invalidScript =
                workspace.WriteSource(
                    "InvalidCliPlugin.fsx",
                    "module InvalidCliPlugin\nlet value: int = \"wrong\""
                )

            use reservedListener = reserveListenerPort ()
            let listenPort = (reservedListener.LocalEndpoint :?> IPEndPoint).Port
            let backendPort = reservePort ()

            let arguments =
                Array.concat
                    [| requiredArguments listenPort backendPort
                       [| "--client-to-server-plugin"; invalidScript |] |]

            use child = ChildProcess.Start(applicationHostPath (), arguments)
            let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 15.0)

            result.ExitCode |> should equal 2
            assertOutputCaptureCompleted result
            assertStandardErrorContains "Plugin configuration error:" result
            assertStandardErrorContains "InvalidCliPlugin.fsx" result
            assertStandardErrorContains "FS" result
            result.StandardError.Contains("Runtime error:", StringComparison.Ordinal) |> should equal false
            reservedListener.Stop()
            do! expectConnectionFailure listenPort timeout.Token
        }

    [<Fact>]
    let ``listener readiness requires the child startup record for its endpoint`` () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 15.0)
            use workspace = new TestWorkspace()
            use foreignListener = reserveListenerPort ()
            let listenEndpoint = foreignListener.LocalEndpoint :?> IPEndPoint
            let backendPort = reservePort ()

            let pluginPath =
                workspace.WriteSource(
                    "ReadinessProbe.fsx",
                    transformSource "ReadinessProbe" "generated[index]"
                )

            let arguments =
                Array.concat
                    [| requiredArguments listenEndpoint.Port backendPort
                       [| "--client-to-server-plugin"; pluginPath |] |]

            use child = ChildProcess.Start(applicationHostPath (), arguments)

            let! readinessFailure =
                task {
                    try
                        let! foreignConnection =
                            waitForListenerAsync child listenEndpoint (TimeSpan.FromSeconds 10.0)

                        foreignConnection.Dispose()
                        return None
                    with :? InvalidOperationException as error ->
                        return Some error.Message
                }

            match readinessFailure with
            | Some message ->
                message.Contains("code 1", StringComparison.Ordinal) |> should equal true
                message.Contains("Runtime error:", StringComparison.Ordinal) |> should equal true
            | None ->
                failwith "A foreign listener was mistaken for the PipeL4 child listener."

            child.HasExited |> should equal true
            foreignListener.Stop()
            do! expectConnectionFailure listenEndpoint.Port timeout.Token
        }

    [<Fact>]
    let ``listener startup record compares the complete structured endpoint`` () =
        let output = ProcessOutputCapture()
        let stoppedRecord =
            """{"State":{"ListenEndpoint":"127.0.0.1:12345","{OriginalFormat}":"""
            + "\"TCP listener stopped on {ListenEndpoint}\"}}"

        output.Receive(stoppedRecord)

        output.HasJsonStringProperty("ListenEndpoint", "127.0.0.1:1234") |> should equal false
        output.HasJsonStringProperty("ListenEndpoint", "127.0.0.1:12345") |> should equal true
        let startedEventTemplate =
            "TCP listener started on {ListenEndpoint} with backend {BackendEndpoint}"

        let startupRecord =
            [ "ListenEndpoint", "127.0.0.1:12345"
              "{OriginalFormat}", startedEventTemplate ]

        output.HasJsonStringProperties(startupRecord) |> should equal false
        let startedRecord =
            """{"State":{"ListenEndpoint":"127.0.0.1:12345","{OriginalFormat}":"""
            + "\"" + startedEventTemplate + "\"}}"

        output.Receive(startedRecord)
        output.HasJsonStringProperties(startupRecord) |> should equal true

    [<Fact>]
    let ``listener readiness waits for child startup across a transient foreign listener`` () =
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "This listener handoff test uses /bin/sh and a test-owned start gate."
        )

        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 20.0)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(IPAddress.Loopback, 0)
            backendListener.Start()

            use foreignListener = new TcpListener(IPAddress.Loopback, 0)
            foreignListener.Start()
            let listenEndpoint = foreignListener.LocalEndpoint :?> IPEndPoint
            let backendPort = (backendListener.LocalEndpoint :?> IPEndPoint).Port
            let pluginPath =
                workspace.WriteSource(
                    "TransientHandoff.fsx",
                    transformSource "TransientHandoff" "generated[index]"
                )

            let releaseGatePath = workspace.GetPath("release-host")
            let gatedHostPath =
                workspace.WriteSource(
                    "gated-host.sh",
                    String.concat
                        Environment.NewLine
                        [ "#!/bin/sh"
                          "while [ ! -e " + shellQuote releaseGatePath + " ]; do"
                          "  sleep 0.1"
                          "done"
                          "exec " + shellQuote (applicationHostPath ()) + " \"$@\"" ]
                )

            let arguments =
                Array.concat
                    [| [| gatedHostPath |]
                       requiredArguments listenEndpoint.Port backendPort
                       [| "--client-to-server-plugin"; pluginPath |] |]

            use child = ChildProcess.Start("/bin/sh", arguments)
            let readiness = waitForListenerAsync child listenEndpoint (TimeSpan.FromSeconds 10.0)
            let foreignAccept = foreignListener.AcceptTcpClientAsync()

            let! firstForeignAttempt =
                Task.WhenAny(foreignAccept, Task.Delay(TimeSpan.FromMilliseconds 750.0, timeout.Token))

            let foreignListenerAcceptedBeforeChildStartup =
                obj.ReferenceEquals(firstForeignAttempt, foreignAccept)

            if foreignListenerAcceptedBeforeChildStartup then
                try
                    let! foreignConnection = foreignAccept
                    foreignConnection.Dispose()
                with
                | :? SocketException -> ()
                | :? ObjectDisposedException -> ()

            foreignListener.Stop()

            if not foreignAccept.IsCompleted then
                try
                    let! foreignConnection = foreignAccept.WaitAsync(TimeSpan.FromSeconds 2.0)
                    foreignConnection.Dispose()
                with
                | :? SocketException -> ()
                | :? ObjectDisposedException -> ()
                | :? TimeoutException -> ()

            File.WriteAllText(releaseGatePath, "continue")

            use! client = readiness
            foreignListenerAcceptedBeforeChildStartup |> should equal false

            use! backend = backendListener.AcceptTcpClientAsync(timeout.Token)
            let payload = [| 0x51uy |]
            let receiveAtBackend = readExactly (backend.GetStream()) payload.Length timeout.Token
            do! client.GetStream().WriteAsync(payload, timeout.Token)
            let! forwarded = receiveAtBackend

            forwarded |> should equal payload
        }

    [<Fact>]
    let ``listener bind failures retain the runtime error exit code`` () =
        task {
            use reservedListener = reserveListenerPort ()
            let listenPort = (reservedListener.LocalEndpoint :?> IPEndPoint).Port
            let backendPort = reservePort ()

            use child =
                ChildProcess.Start(
                    applicationHostPath (),
                    requiredArguments listenPort backendPort
                )

            let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 10.0)

            result.ExitCode |> should equal 1
            assertOutputCaptureCompleted result
            assertStandardErrorContains "Runtime error:" result
            reservedListener.Stop()
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 5.0)
            do! expectConnectionFailure listenPort timeout.Token
        }

    [<Fact>]
    let ``output capture setup failures clean up the started child and preserve the setup exception`` () =
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "This setup-failure test uses /bin/sleep and is run on Linux."
        )
        Assert.SkipUnless(
            LinuxPidFd.isSupported (),
            "This setup-failure test requires Linux pidfd support for safe fallback cleanup."
        )

        task {
            let mutable ownedChild: LinuxPidFd.PidFd option = None
            let setupFailure = InvalidOperationException("controlled output setup failure")

            let beginOutputRead (startedProcess: Process) =
                ownedChild <- LinuxPidFd.PidFd.TryOpen(startedProcess.Id)

                if ownedChild.IsNone then
                    raise (InvalidOperationException("Could not retain a pidfd for the started test process."))

                startedProcess.BeginOutputReadLine()

            let beginErrorRead (_: Process) = raise setupFailure

            try
                let actualFailure =
                    try
                        ChildProcess.StartWithReadSetupForTest(
                            "/bin/sleep",
                            [| "60" |],
                            beginOutputRead,
                            beginErrorRead
                        )
                        |> ignore

                        None
                    with error ->
                        Some error

                match actualFailure, ownedChild with
                | Some error, Some ownedProcess ->
                    obj.ReferenceEquals(error, setupFailure) |> should equal true
                    ownedProcess.HasExited |> should equal true
                | _ -> failwith "The controlled output setup failure was not observed after child creation."
            finally
                cleanupPidFd ownedChild
        }

    [<Fact>]
    let ``subprocess output capture stays bounded when a descendant retains redirected pipes`` () =
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "This process-tree test uses /bin/sh and is run on Linux."
        )
        Assert.SkipUnless(
            LinuxPidFd.isSupported (),
            "This process-tree test requires Linux pidfd support for safe fallback cleanup."
        )

        task {
            use workspace = new TestWorkspace()
            let processIdPath = workspace.GetPath("pipe-holder.pid")
            let releaseGatePath = workspace.GetPath("release-pipe-holder")

            let command =
                String.concat
                    Environment.NewLine
                    [ "/bin/sleep 600 &"
                      "child_pid=$!"
                      "echo \"$child_pid\""
                      "echo \"$child_pid\" > " + shellQuote processIdPath
                      "while [ ! -e " + shellQuote releaseGatePath + " ]; do /bin/sleep 0.05; done"
                      "exec /bin/true" ]

            use child = ChildProcess.Start("/bin/sh", [| "-c"; command |])
            let mutable ownedDescendant: LinuxPidFd.PidFd option = None

            try
                let! descendant =
                    waitForPidFdAsync child processIdPath (TimeSpan.FromSeconds 5.0)

                ownedDescendant <- Some descendant
                File.WriteAllText(releaseGatePath, "release")

                let drain = child.WaitForExitAsync(TimeSpan.FromSeconds 10.0)
                let! boundedWait = Task.WhenAny(drain, Task.Delay(TimeSpan.FromSeconds 6.0))
                let completedWithinBound = obj.ReferenceEquals(boundedWait, drain)

                if not completedWithinBound then
                    descendant.StopAndWait(TimeSpan.FromSeconds 3.0) |> should equal true

                let! afterCleanup = Task.WhenAny(drain, Task.Delay(TimeSpan.FromSeconds 3.0))

                if not (obj.ReferenceEquals(afterCleanup, drain)) then
                    failwith "Subprocess output capture did not complete after the pipe holder was stopped."

                let! result = drain

                descendant.StopAndWait(TimeSpan.FromSeconds 3.0) |> should equal true
                completedWithinBound |> should equal true
                result.ExitCode |> should equal 0
                result.OutputCaptureCompleted |> should equal false

                let processId = processIdFromFile processIdPath
                result.StandardOutput.Contains(string processId, StringComparison.Ordinal)
                |> should equal true
            finally
                cleanupPidFd ownedDescendant
        }

    [<Fact>]
    let ``subprocess timeout kills its owned process tree`` () =
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "This process-tree timeout test uses /bin/sh and is run on Linux."
        )
        Assert.SkipUnless(
            LinuxPidFd.isSupported (),
            "This process-tree timeout test requires Linux pidfd support for safe fallback cleanup."
        )

        task {
            use workspace = new TestWorkspace()
            let processIdPath = workspace.GetPath("timeout-child.pid")
            let command =
                String.concat
                    Environment.NewLine
                    [ "/bin/sleep 600 &"
                      "echo $! > " + shellQuote processIdPath
                      "wait" ]

            use child = ChildProcess.Start("/bin/sh", [| "-c"; command |])
            let mutable ownedDescendant: LinuxPidFd.PidFd option = None

            try
                let! descendant =
                    waitForPidFdAsync child processIdPath (TimeSpan.FromSeconds 5.0)

                ownedDescendant <- Some descendant
                let timedWait = child.WaitForExitAsync(TimeSpan.FromMilliseconds 250.0)
                let! boundedWait = Task.WhenAny(timedWait, Task.Delay(TimeSpan.FromSeconds 8.0))
                let completedWithinBound = obj.ReferenceEquals(boundedWait, timedWait)

                if not completedWithinBound then
                    descendant.StopAndWait(TimeSpan.FromSeconds 3.0) |> should equal true

                let! afterCleanup = Task.WhenAny(timedWait, Task.Delay(TimeSpan.FromSeconds 3.0))

                if not (obj.ReferenceEquals(afterCleanup, timedWait)) then
                    failwith "The subprocess timeout did not finish after its owned process tree was stopped."

                let mutable timeoutMessage: string option = None

                try
                    let! _ = timedWait
                    ()
                with :? TimeoutException as error ->
                    timeoutMessage <- Some error.Message

                completedWithinBound |> should equal true

                timeoutMessage
                |> Option.exists (fun message -> message.Contains("Cleanup exited=true", StringComparison.Ordinal))
                |> should equal true

                descendant.HasExited |> should equal true
            finally
                cleanupPidFd ownedDescendant
        }

    [<Fact>]
    let ``built CLI enforces logical capacity and continues with a healthy connection`` () =
        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(IPAddress.Loopback, 0)
            backendListener.Start()

            let scriptPath =
                workspace.WriteSource(
                    "GeneratedOutputCapacity.fsx",
                    String.concat
                        Environment.NewLine
                        [| "module GeneratedOutputCapacity"
                           ""
                           "open System"
                           "open System.Threading.Tasks"
                           "open PipeL4.Plugins"
                           ""
                           "type Processor() ="
                           "    interface IByteStreamProcessor with"
                           "        member _.ProcessAsync(input, output, _) ="
                           "            if input.IsCompleted then"
                           "                ValueTask<ProcessorResult>({ RetainedLength = 0L })"
                           "            elif input.Bytes.FirstSpan[0] = 255uy then"
                           "                let generated = Array.create 16 255uy"
                           "                generated.AsSpan().CopyTo(output.GetSpan(generated.Length))"
                           "                output.Advance(generated.Length)"
                           "                ValueTask<ProcessorResult>({ RetainedLength = 0L })"
                           "            else"
                           "                for segment in input.Bytes do"
                           "                    segment.Span.CopyTo(output.GetSpan(segment.Length))"
                           "                    output.Advance(segment.Length)"
                           "                ValueTask<ProcessorResult>({ RetainedLength = 0L })" |]
                )

            let backendPort = (backendListener.LocalEndpoint :?> IPEndPoint).Port

            let arguments listenPort logicalCapacity =
                Array.concat
                    [| requiredArguments listenPort backendPort
                       [| "--client-to-server-plugin"
                          scriptPath
                          "--client-to-server-logical-capacity-bytes"
                          string logicalCapacity
                          "--server-to-client-logical-capacity-bytes"
                          "32"
                          "--connection-physical-capacity-bytes"
                          "131072" |] |]

            let constrainedPort = reservePort ()
            use constrainedChild = ChildProcess.Start(applicationHostPath (), arguments constrainedPort 8)
            let constrainedEndpoint = IPEndPoint(IPAddress.Loopback, constrainedPort)
            use! failedClient =
                waitForListenerAsync constrainedChild constrainedEndpoint (TimeSpan.FromSeconds 10.0)

            let acceptFailedBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
            use! failedBackend = acceptFailedBackend
            let failedClientStream = failedClient.GetStream()
            let failedBackendStream = failedBackend.GetStream()
            do! failedClientStream.WriteAsync([| 255uy |], timeout.Token)
            do! expectConnectionClosed failedClientStream timeout.Token
            do! expectConnectionClosed failedBackendStream timeout.Token

            let acceptHealthyBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
            // The constrained host remains on the same port after a connection-level capacity failure.
            use! healthyClient = connectToListenerAsync constrainedPort timeout.Token
            use! healthyBackend = acceptHealthyBackend

            let healthyClientStream = healthyClient.GetStream()
            let healthyBackendStream = healthyBackend.GetStream()
            let safePayload = [| 1uy; 2uy; 3uy |]
            let receiveSafePayload = readExactly healthyBackendStream safePayload.Length timeout.Token
            do! healthyClientStream.WriteAsync(safePayload, timeout.Token)
            let! receivedSafePayload = receiveSafePayload
            receivedSafePayload |> should equal safePayload

            healthyClient.Client.Shutdown(SocketShutdown.Send)
            healthyBackend.Client.Shutdown(SocketShutdown.Send)
            do! expectEndOfStream healthyClientStream timeout.Token
            do! expectEndOfStream healthyBackendStream timeout.Token

            let! constrainedResult = constrainedChild.KillAndWaitAsync()
            assertOutputCaptureCompleted constrainedResult
            constrainedResult.StandardOutput.Contains(
                "logical generated output occurrence",
                StringComparison.OrdinalIgnoreCase
            )
            |> should equal true

            let sufficientPort = reservePort ()
            use sufficientChild = ChildProcess.Start(applicationHostPath (), arguments sufficientPort 32)
            let sufficientEndpoint = IPEndPoint(IPAddress.Loopback, sufficientPort)
            use! sufficientClient =
                waitForListenerAsync sufficientChild sufficientEndpoint (TimeSpan.FromSeconds 10.0)

            let acceptSufficientBackend = backendListener.AcceptTcpClientAsync(timeout.Token).AsTask()
            use! sufficientBackend = acceptSufficientBackend
            let sufficientClientStream = sufficientClient.GetStream()
            let sufficientBackendStream = sufficientBackend.GetStream()
            do! sufficientClientStream.WriteAsync([| 255uy |], timeout.Token)

            let! generated =
                readExactly sufficientBackendStream 16 timeout.Token

            generated |> should equal (Array.create 16 255uy)
            sufficientClient.Client.Shutdown(SocketShutdown.Send)
            do! expectEndOfStream sufficientBackendStream timeout.Token
            sufficientBackend.Client.Shutdown(SocketShutdown.Send)
            do! expectEndOfStream sufficientClientStream timeout.Token

            let! sufficientResult = sufficientChild.KillAndWaitAsync()
            assertOutputCaptureCompleted sufficientResult
        }

    [<Fact>]
    let ``SIGINT during plugin source loading exits cleanly before listener startup`` () =
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "The startup cancellation test uses a Linux FIFO to coordinate script loading."
        )

        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 15.0)
            use workspace = new TestWorkspace()
            let scriptPath = workspace.GetPath("BlockedStartup.fsx")
            UnixFifo.create scriptPath

            let listenPort = reservePort ()
            let backendPort = reservePort ()
            let arguments =
                Array.concat
                    [| requiredArguments listenPort backendPort
                       [| "--client-to-server-plugin"; scriptPath |] |]

            use child = ChildProcess.Start(applicationHostPath (), arguments)
            let! writer =
                UnixFifo.openWriterWhenReaderIsReadyAsync
                    child
                    scriptPath
                    (TimeSpan.FromSeconds 10.0)

            try
                UnixSignals.interrupt child.Id
            finally
                UnixFifo.closeWriter writer

            let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 10.0)
            result.ExitCode |> should equal 0
            assertOutputCaptureCompleted result
            result.StandardError.Contains("Runtime error:", StringComparison.Ordinal)
            |> should equal false
            result.StandardError.Contains("Plugin configuration error:", StringComparison.Ordinal)
            |> should equal false
            result.StandardOutput.Contains("TCP listener started", StringComparison.Ordinal)
            |> should equal false
            do! expectConnectionFailure listenPort timeout.Token
        }

    [<Fact>]
    let ``SIGINT cooperatively stops the built CLI`` () =
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "The SIGINT process test runs on Linux. "
            + "Windows console control delivery requires a console host."
        )

        task {
            use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 15.0)
            use workspace = new TestWorkspace()
            use backendListener = new TcpListener(IPAddress.Loopback, 0)
            backendListener.Start()

            let processedMarker = workspace.GetPath("processor-executed.txt")
            let disposedMarker = workspace.GetPath("processor-disposed.txt")

            let scriptPath =
                workspace.WriteSource(
                    "DisposalMarker.fsx",
                    String.concat
                        Environment.NewLine
                        [| "module DisposalMarker"
                           ""
                           "open System"
                           "open System.IO"
                           "open System.Threading.Tasks"
                           "open PipeL4.Plugins"
                           ""
                           "type Processor() ="
                           "    interface IByteStreamProcessor with"
                           "        member _.ProcessAsync(input, output, _) ="
                           "            if not input.Bytes.IsEmpty then"
                           $"                File.WriteAllText({fsharpStringLiteral processedMarker}, \"processed\")"
                           "                for segment in input.Bytes do"
                           "                    segment.Span.CopyTo(output.GetSpan(segment.Length))"
                           "                    output.Advance(segment.Length)"
                           "            ValueTask<ProcessorResult>({ RetainedLength = 0L })"
                           "    interface IDisposable with"
                           "        member _.Dispose() ="
                           $"            File.WriteAllText({fsharpStringLiteral disposedMarker}, \"disposed\")" |]
                )

            let listenPort = reservePort ()
            let backendPort = (backendListener.LocalEndpoint :?> IPEndPoint).Port
            use child =
                ChildProcess.Start(
                    applicationHostPath (),
                    Array.concat
                        [| requiredArguments listenPort backendPort
                           [| "--client-to-server-plugin"; scriptPath |] |]
                )

            let listenEndpoint = IPEndPoint(IPAddress.Loopback, listenPort)
            use! client = waitForListenerAsync child listenEndpoint (TimeSpan.FromSeconds 10.0)
            use! backend = backendListener.AcceptTcpClientAsync(timeout.Token)

            let payload = [| 18uy; 24uy; 36uy |]
            let receive = readExactly (backend.GetStream()) payload.Length timeout.Token
            do! client.GetStream().WriteAsync(payload, timeout.Token)
            let! received = receive
            received |> should equal payload
            File.Exists processedMarker |> should equal true

            UnixSignals.interrupt child.Id

            let! result = child.WaitForExitAsync(TimeSpan.FromSeconds 10.0)
            result.ExitCode |> should equal 0
            assertOutputCaptureCompleted result
            assertStandardOutputContains "TCP listener stopped" result
            File.Exists disposedMarker |> should equal true
            File.ReadAllText(disposedMarker) |> should equal "disposed"
            do! expectConnectionClosed (client.GetStream()) timeout.Token
            do! expectConnectionClosed (backend.GetStream()) timeout.Token
        }
