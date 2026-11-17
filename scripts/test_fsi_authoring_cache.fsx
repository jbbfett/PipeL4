open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Xml.Linq

type private ProcessResult =
    { ExitCode: int
      StandardOutput: string
      StandardError: string }

exception private RegressionException of string

let private fail message = raise (RegressionException message)

let private createPrivateDirectory path =
    Directory.CreateDirectory path |> ignore

    if not (OperatingSystem.IsWindows()) then
        File.SetUnixFileMode(
            path,
            UnixFileMode.UserRead ||| UnixFileMode.UserWrite ||| UnixFileMode.UserExecute)

let private runDotnet workingDirectory (environment: (string * string) list) arguments =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true

    for name, value in environment do
        startInfo.Environment[name] <- value

    for argument in arguments do
        startInfo.ArgumentList.Add argument

    use child = new Process(StartInfo = startInfo)

    if not (child.Start()) then
        fail "Could not start dotnet for the FSI authoring cache regression."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()

    if not (child.WaitForExit(180000)) then
        child.Kill(true)
        fail "An FSI authoring cache regression process timed out."

    { ExitCode = child.ExitCode
      StandardOutput = outputTask.GetAwaiter().GetResult()
      StandardError = errorTask.GetAwaiter().GetResult() }

let private packageDetails packagePath =
    use package = ZipFile.OpenRead packagePath

    let nuspecs =
        package.Entries
        |> Seq.filter (fun entry -> entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
        |> Seq.toArray

    if nuspecs.Length <> 1 then
        fail $"Expected one nuspec in '{packagePath}', found {nuspecs.Length}."

    use stream = nuspecs[0].Open()
    let document = XDocument.Load stream

    let fsharpCoreDependency =
        document.Descendants()
        |> Seq.tryFind (fun element ->
            element.Name.LocalName = "dependency"
            && (element.Attribute(XName.Get "id")
                |> Option.ofObj
                |> Option.exists (fun attribute ->
                    attribute.Value.Equals("FSharp.Core", StringComparison.OrdinalIgnoreCase))))
        |> Option.defaultWith (fun () -> fail "The candidate package does not declare FSharp.Core.")

    let range =
        fsharpCoreDependency.Attribute(XName.Get "version")
        |> Option.ofObj
        |> Option.map (fun attribute -> attribute.Value.Trim().TrimStart('[', '(').TrimEnd(']', ')'))
        |> Option.defaultWith (fun () -> fail "The candidate package has no FSharp.Core version range.")

    let bounds = range.Split([| ',' |], StringSplitOptions.None)
    let version = bounds[0].Trim()

    if String.IsNullOrWhiteSpace version then
        fail "The candidate package has no usable FSharp.Core minimum version."

    version

let private findGlobalPackagesPath () =
    let result = runDotnet Environment.CurrentDirectory [] [ "nuget"; "locals"; "global-packages"; "--list" ]

    if result.ExitCode <> 0 then
        fail $"Could not locate NuGet's global package cache. {result.StandardError}"

    result.StandardOutput.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
    |> Array.tryPick (fun line ->
        let separator = line.IndexOf ':'

        if separator >= 0 then
            Some(line.Substring(separator + 1).Trim())
        else
            None)
    |> Option.defaultWith (fun () -> fail "Could not parse NuGet's global package cache path.")

let private writeNuGetConfig (path: string) (feed: string) =
    let config =
        XDocument(
            XElement(
                "configuration",
                XElement(
                    "packageSources",
                    XElement("clear"),
                    XElement("add", XAttribute("key", "PipeL4 local candidate"), XAttribute("value", feed)))))

    config.Save path

let private makeIncompatiblePackage path =
    use archive = ZipFile.Open(path, ZipArchiveMode.Create)
    let entry = archive.CreateEntry("Incompatible.nuspec")
    use stream = entry.Open()
    use writer = new StreamWriter(stream)
    writer.Write(
        "<package xmlns=\"http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd\"><metadata><id>Other.Package</id><version>1.0.0</version></metadata></package>")

let private expectSuccess (label: string) (expectedText: string) (result: ProcessResult) =
    let diagnostics = result.StandardOutput + Environment.NewLine + result.StandardError

    if result.ExitCode <> 0 || not (diagnostics.Contains(expectedText, StringComparison.Ordinal)) then
        fail $"{label} failed (exit {result.ExitCode}).{Environment.NewLine}{diagnostics}"

    printfn "PASS %s" label

let private expectFailure (label: string) (expectedText: string) (result: ProcessResult) =
    let diagnostics = result.StandardOutput + Environment.NewLine + result.StandardError

    if result.ExitCode = 0 || not (diagnostics.Contains(expectedText, StringComparison.Ordinal)) then
        fail $"{label} did not fail as expected (exit {result.ExitCode}).{Environment.NewLine}{diagnostics}"

    printfn "PASS %s (exit %d)" label result.ExitCode

let private runRegression packageArgument sourceArgument =
    let packagePath = Path.GetFullPath packageArgument
    let sourcePath = Path.GetFullPath sourceArgument

    if not (File.Exists packagePath) then
        fail $"Public API package does not exist: {packagePath}"

    if not (File.Exists sourcePath) then
        fail $"Authoring example does not exist: {sourcePath}"

    let validatorPath = Path.Combine(__SOURCE_DIRECTORY__, "validate_fsi_authoring.fsx")
    let fsharpCoreVersion = packageDetails packagePath
    let globalPackages = findGlobalPackagesPath ()
    let fsharpCorePackage =
        Path.Combine(globalPackages, "fsharp.core", fsharpCoreVersion.ToLowerInvariant(), $"fsharp.core.{fsharpCoreVersion.ToLowerInvariant()}.nupkg")

    if not (File.Exists fsharpCorePackage) then
        fail $"Could not stage FSharp.Core {fsharpCoreVersion} for the regression at '{fsharpCorePackage}'."

    let root = Path.Combine(Path.GetTempPath(), $"PipeL4 FSI authoring cache regression {Guid.NewGuid():N}")
    let coldHome = Path.Combine(root, "cold-home")
    let seededHome = Path.Combine(root, "seeded-home")
    let candidate = Path.Combine(root, "candidate", Path.GetFileName packagePath)
    let source = Path.Combine(root, "AsciiUppercase.fsx")
    let seedWorkingDirectory = Path.Combine(root, "direct-fsi")
    let seedFeed = Path.Combine(seedWorkingDirectory, "feed")
    let seedPackages = Path.Combine(seedWorkingDirectory, "packages")
    let seedHttpCache = Path.Combine(seedWorkingDirectory, "http-cache")

    let profileEnvironment profile =
        [ "HOME", profile
          "USERPROFILE", profile
          "DOTNET_CLI_HOME", profile
          "XDG_RUNTIME_DIR", profile
          "NUGET_PACKAGES", globalPackages ]

    let validate profile package source =
        runDotnet
            root
            (profileEnvironment profile)
            [ "fsi"; validatorPath; package; source ]

    try
        Directory.CreateDirectory(Path.GetDirectoryName candidate) |> ignore
        Directory.CreateDirectory seedFeed |> ignore
        createPrivateDirectory coldHome
        createPrivateDirectory seededHome
        File.Copy(packagePath, candidate)
        File.Copy(sourcePath, source)
        File.Copy(candidate, Path.Combine(seedFeed, Path.GetFileName candidate))
        File.Copy(fsharpCorePackage, Path.Combine(seedFeed, Path.GetFileName fsharpCorePackage))
        writeNuGetConfig (Path.Combine(seedWorkingDirectory, "NuGet.Config")) seedFeed

        let cold = validate coldHome candidate source
        expectSuccess "cold candidate validation restored from the isolated local feed" "restored only from the local candidate feed" cold

        let directEnvironment =
            [ "HOME", seededHome
              "USERPROFILE", seededHome
              "DOTNET_CLI_HOME", seededHome
              "XDG_RUNTIME_DIR", seededHome
              "NUGET_PACKAGES", seedPackages
              "NUGET_HTTP_CACHE_PATH", seedHttpCache
              "RestoreSources", seedFeed
              "NuGetAudit", "false"
              "FSharpCoreImplicitPackageVersion", fsharpCoreVersion ]

        let direct = runDotnet seedWorkingDirectory directEnvironment [ "fsi"; "--exec"; source ]
        expectSuccess "direct FSI authoring succeeded and populated the normal temporary dependency-manager cache" "" direct

        let managerCache = Path.Combine(seededHome, ".packagemanagement", "nuget", "Cache")

        if not (Directory.Exists managerCache) then
            fail $"Direct FSI did not create its dependency-manager cache at '{managerCache}'."

        let seededResolution =
            Directory.GetFiles(managerCache, "*.resolvedReferences.paths")
            |> Array.tryFind (fun path ->
                let contents = File.ReadAllText path
                contents.Contains(seedPackages, StringComparison.Ordinal)
                && contents.Contains("PipeL4.Plugins", StringComparison.Ordinal))

        if seededResolution.IsNone then
            fail "Direct FSI did not cache PipeL4.Plugins assembly paths from the temporary normal NuGet package cache."

        printfn "PASS direct FSI seeded a conflicting PipeL4.Plugins resolution under HOME/.packagemanagement/nuget/Cache"

        let warm = validate seededHome candidate source
        expectSuccess "warm candidate validation ignored the seeded FSI resolution and restored from its local feed" "restored only from the local candidate feed" warm

        let repeated = validate seededHome candidate source
        expectSuccess "repeated candidate validation remained successful" "restored only from the local candidate feed" repeated

        let invalidSource = Path.Combine(root, "Invalid.fsx")
        File.WriteAllText(invalidSource, File.ReadAllText(source) + Environment.NewLine + "let invalidValue: int = \"not an integer\"")
        let invalidCompilation = validate seededHome candidate invalidSource
        expectFailure "invalid F# source still fails compilation" "FS0001" invalidCompilation

        let throwingSource = Path.Combine(root, "Throws.fsx")
        File.WriteAllText(
            throwingSource,
            File.ReadAllText(source)
            + Environment.NewLine
            + "do raise (System.InvalidOperationException(\"controlled FSI runtime failure\"))")
        let runtimeFailure = validate seededHome candidate throwingSource
        expectFailure "FSI runtime exceptions still propagate as failures" "controlled FSI runtime failure" runtimeFailure

        let missingPackage = Path.Combine(root, "missing", "PipeL4.Plugins.nupkg")
        let missing = validate seededHome missingPackage source
        expectFailure "missing candidate packages still fail" "Public API package does not exist" missing

        let incompatiblePackage = Path.Combine(root, "incompatible.nupkg")
        makeIncompatiblePackage incompatiblePackage
        let incompatible = validate seededHome incompatiblePackage source
        expectFailure "incompatible candidate packages still fail" "Expected PackageId 'PipeL4.Plugins'" incompatible
    finally
        if Directory.Exists root then
            Directory.Delete(root, true)

let arguments = fsi.CommandLineArgs |> Array.skip 1

let exitStatus =
    try
        match arguments with
        | [| packagePath; sourcePath |] ->
            runRegression packagePath sourcePath
            0
        | _ ->
            eprintfn "Usage: dotnet fsi scripts/test_fsi_authoring_cache.fsx <PipeL4.Plugins-package.nupkg> <processor.fsx>"
            2
    with
    | RegressionException message ->
        eprintfn "FSI authoring cache regression failed: %s" message
        1
    | error ->
        eprintfn "Unexpected FSI authoring cache regression error: %s" error.Message
        2

Environment.Exit exitStatus
