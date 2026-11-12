open System
open System.Diagnostics
open System.IO

type ScriptResult =
    { ExitCode: int
      StandardOutput: string
      StandardError: string }

let private childWorkingDirectory =
    Path.Combine(Path.GetTempPath(), $"PipeL4 validation harness {Guid.NewGuid():N}")

do
    Directory.CreateDirectory childWorkingDirectory |> ignore
    let globalJsonPath = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "..", "global.json"))
    File.Copy(globalJsonPath, Path.Combine(childWorkingDirectory, "global.json"))

let runScript (scriptPath: string) (arguments: string list) injectFailure =
    let startInfo = new ProcessStartInfo("dotnet")
    startInfo.ArgumentList.Add "fsi"
    startInfo.ArgumentList.Add scriptPath
    for argument in arguments do
        startInfo.ArgumentList.Add argument
    startInfo.WorkingDirectory <- childWorkingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.Environment["PIPEL4_TEST_VALIDATE_PACKAGE_INJECT_FAILURE"] <- ""
    if injectFailure then
        startInfo.Environment["PIPEL4_TEST_VALIDATE_PACKAGE_INJECT_FAILURE"] <- "1"

    use childProcess = new Process(StartInfo = startInfo)
    if not (childProcess.Start()) then
        failwith $"Could not start 'dotnet fsi {scriptPath}'."
    let standardOutput = childProcess.StandardOutput.ReadToEnd()
    let standardError = childProcess.StandardError.ReadToEnd()
    childProcess.WaitForExit()

    { ExitCode = childProcess.ExitCode
      StandardOutput = standardOutput
      StandardError = standardError }

let reportChildFailure label result =
    eprintfn "FAIL %s: child exited %d." label result.ExitCode
    if not (String.IsNullOrWhiteSpace result.StandardOutput) then
        eprintfn "stdout:\n%s" result.StandardOutput
    if not (String.IsNullOrWhiteSpace result.StandardError) then
        eprintfn "stderr:\n%s" result.StandardError

let runHarness () =
    let arguments = fsi.CommandLineArgs |> Array.skip 1
    if arguments.Length <> 1 then
        eprintfn "Usage: dotnet fsi scripts/test_validation_exit_codes.fsx <package-or-output-directory>"
        2
    else
        let testScript = Path.Combine(__SOURCE_DIRECTORY__, "test_validate_package.fsx")
        let packagePath = Path.GetFullPath arguments[0]
        let mutable passed = 0
        let mutable failed = 0

        let check label predicate result =
            if predicate result then
                passed <- passed + 1
                printfn "PASS %s (exit %d)" label result.ExitCode
            else
                failed <- failed + 1
                reportChildFailure label result

        let healthy = runScript testScript [ packagePath ] false
        check "healthy package validation suite exits successfully"
            (fun result ->
                result.ExitCode = 0
                && result.StandardOutput.Contains("Package validation tests:", StringComparison.Ordinal)
                && result.StandardOutput.Contains("0 failed.", StringComparison.Ordinal))
            healthy

        let injectedFailure = runScript testScript [ packagePath ] true
        check "a controlled negative-test assertion failure exits nonzero"
            (fun result ->
                result.ExitCode <> 0
                && result.StandardError.Contains("FAIL controlled exit-code regression failure", StringComparison.Ordinal)
                && result.StandardOutput.Contains("failed.", StringComparison.Ordinal)
                && result.StandardOutput.Contains("PASS temporary test artifacts cleaned up", StringComparison.Ordinal))
            injectedFailure

        let invalidArguments = runScript testScript [] false
        check "invalid invocation arguments exit nonzero"
            (fun result ->
                result.ExitCode <> 0
                && result.StandardError.Contains("Usage: dotnet fsi", StringComparison.Ordinal))
            invalidArguments

        let missingPath = Path.Combine(Path.GetTempPath(), $"PipeL4 missing package {Guid.NewGuid():N}")
        let setupFailure = runScript testScript [ missingPath ] false
        check "package setup failure exits nonzero"
            (fun result ->
                result.ExitCode <> 0
                && result.StandardError.Contains("Package validation test setup failed", StringComparison.Ordinal))
            setupFailure

        let unexpectedException = runScript testScript [ "--probe-unexpected-exception" ] false
        check "unexpected exceptions exit nonzero"
            (fun result ->
                result.ExitCode <> 0
                && result.StandardError.Contains("Unexpected package validation test error", StringComparison.Ordinal))
            unexpectedException

        let validatorScript = Path.Combine(__SOURCE_DIRECTORY__, "validate_package.fsx")
        let validatorUsage = runScript validatorScript [] false
        check "validator usage errors exit nonzero"
            (fun result ->
                result.ExitCode <> 0
                && result.StandardError.Contains("Usage: dotnet fsi", StringComparison.Ordinal))
            validatorUsage

        let validatorSetup = runScript validatorScript [ missingPath ] false
        check "validator package-path failures exit nonzero"
            (fun result ->
                result.ExitCode <> 0
                && result.StandardError.Contains("Package validation failed", StringComparison.Ordinal))
            validatorSetup

        printfn "F# script exit-code checks: %d passed, %d failed." passed failed
        if failed = 0 then 0 else 1

let exitStatus =
    try
        try
            runHarness ()
        with error ->
            eprintfn "Unexpected exit-code harness error: %s" error.Message
            2
    finally
        if Directory.Exists childWorkingDirectory then
            Directory.Delete(childWorkingDirectory, true)

Environment.Exit exitStatus
