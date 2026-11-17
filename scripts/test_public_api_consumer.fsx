open System
open System.Diagnostics
open System.IO

let private fail message = raise (InvalidOperationException message)

let arguments = fsi.CommandLineArgs |> Array.skip 1

let run () =
    if arguments.Length <> 1 then
        fail "Usage: dotnet fsi scripts/test_public_api_consumer.fsx <PipeL4.Plugins-package.nupkg>"

    let packagePath = Path.GetFullPath arguments[0]
    if not (File.Exists packagePath) then
        fail $"Public API package is missing: {packagePath}"

    let validatorPath = Path.Combine(__SOURCE_DIRECTORY__, "validate_public_api_package.fsx")
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- Environment.CurrentDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.ArgumentList.Add "fsi"
    startInfo.ArgumentList.Add validatorPath
    startInfo.ArgumentList.Add packagePath
    startInfo.ArgumentList.Add "--test-invalid-consumer"

    use child = new Process(StartInfo = startInfo)
    if not (child.Start()) then
        fail "Could not start the public API validator's invalid-consumer regression."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()
    if not (child.WaitForExit(180000)) then
        child.Kill(true)
        fail "The invalid-consumer regression timed out."

    let output = outputTask.GetAwaiter().GetResult()
    let error = errorTask.GetAwaiter().GetResult()
    let diagnostics = output + Environment.NewLine + error

    if child.ExitCode = 0 then
        fail "Invalid Consumer.fs unexpectedly passed the public API package validator."
    if not (diagnostics.Contains("Restored ", StringComparison.Ordinal)
            && diagnostics.Contains("Consumer.fs", StringComparison.Ordinal)
            && diagnostics.Contains("FS0001", StringComparison.Ordinal)
            && diagnostics.Contains("dotnet build", StringComparison.Ordinal)) then
        fail
            $"The validator failed for a reason other than compiling invalid Consumer.fs. Exit code: {child.ExitCode}.{Environment.NewLine}{diagnostics}"

    printfn "PASS invalid Consumer.fs failed during F# compilation (FS0001); validator exited %d." child.ExitCode

let exitStatus =
    try
        run ()
        0
    with error ->
        eprintfn "Public API consumer regression failed: %s" error.Message
        1

Environment.Exit exitStatus
