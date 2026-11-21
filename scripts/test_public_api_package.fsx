open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Security.Cryptography
open System.Text.Json
open System.Xml.Linq

let private fail message = raise (InvalidOperationException message)
let private root = Path.Combine(Path.GetTempPath(), $"PipeL4 public API regressions {Guid.NewGuid():N}")

let private runValidator packagePath (environment: (string * string) list) =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- Environment.CurrentDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    for argument in [ "fsi"; Path.Combine(__SOURCE_DIRECTORY__, "validate_public_api_package.fsx"); packagePath ] do
        startInfo.ArgumentList.Add argument
    for name, value in environment do
        startInfo.Environment[name] <- value
    use child = new Process(StartInfo = startInfo)
    if not (child.Start()) then fail "Could not start public API validator."
    let output = child.StandardOutput.ReadToEndAsync()
    let error = child.StandardError.ReadToEndAsync()
    if not (child.WaitForExit(180000)) then
        child.Kill(true)
        fail "Public API regression timed out."
    child.ExitCode, output.GetAwaiter().GetResult() + error.GetAwaiter().GetResult()

let private check label predicate (exitCode: int, diagnostics: string) =
    if not (predicate exitCode diagnostics) then
        fail $"{label}: validator exited {exitCode}.\n{diagnostics}"
    printfn "PASS %s (validator exit %d)" label exitCode

let private rewriteRange (packagePath: string) label (range: string option) =
    let directory = Path.Combine(root, label)
    Directory.CreateDirectory directory |> ignore
    let target = Path.Combine(directory, Path.GetFileName packagePath)
    File.Copy(packagePath, target)
    use archive = ZipFile.Open(target, ZipArchiveMode.Update)
    let entry = archive.Entries |> Seq.find (fun entry -> entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
    let document =
        use stream = entry.Open()
        XDocument.Load stream
    let dependency = document.Descendants() |> Seq.find (fun element -> element.Name.LocalName = "dependency")
    match range with
    | Some value -> dependency.SetAttributeValue(XName.Get "version", value)
    | None -> dependency.Remove()
    use stream = entry.Open()
    stream.SetLength 0L
    document.Save stream
    target

let private property (name: string) (value: string) = XElement(name, value)

let private policyFiles label extraTargets =
    let directory = Path.Combine(root, label)
    Directory.CreateDirectory directory |> ignore
    let props = Path.Combine(directory, "policy.props")
    XElement("Project",
        XElement("PropertyGroup",
            property "WarningsAsErrors" "$(WarningsAsErrors);44;4242",
            property "NoWarn" "$(NoWarn);1182; nu1605 ,NU1901;NU1605",
            property "RestoreNoWarn" "NU1902, Nu1605 ;NU1605"))
        .Save props
    let targets = Path.Combine(directory, "probe.targets")
    let probe =
        XElement("Target", XAttribute("Name", "ObserveConsumerPolicy"),
            XAttribute("BeforeTargets", "_GenerateRestoreGraphProjectEntry;CoreCompile"),
            XElement("WriteLinesToFile", XAttribute("File", Path.Combine(directory, "evaluated-policy.txt")),
                XAttribute("Lines", "POLICY|$(WarningsAsErrors.Replace(';', ','))|$(NoWarn.Replace(';', ','))|$(RestoreNoWarn.Replace(';', ','))")))
    let evidence =
        XElement("Target", XAttribute("Name", "CaptureConsumerEvidence"), XAttribute("AfterTargets", "Build"),
            XElement("Copy", XAttribute("SourceFiles", "$(MSBuildProjectFullPath)"),
                XAttribute("DestinationFiles", Path.Combine(directory, "consumer-$(FSharpCoreImplicitPackageVersion).fsproj"))),
            XElement("Copy", XAttribute("SourceFiles", "$(ProjectAssetsFile)"),
                XAttribute("DestinationFiles", Path.Combine(directory, "assets-$(FSharpCoreImplicitPackageVersion).json"))),
            XElement("ItemGroup",
                XElement("_CandidateReference", XAttribute("Include", "@(ReferencePath)"),
                    XAttribute("Condition", "'%(ReferencePath.Filename)' == 'PipeL4.Plugins'"))),
            XElement("Copy", XAttribute("SourceFiles", "@(_CandidateReference)"),
                XAttribute("DestinationFiles", Path.Combine(directory, "api-$(FSharpCoreImplicitPackageVersion).dll"))))
    XElement("Project", probe, evidence, extraTargets).Save targets
    directory, [ "DirectoryBuildPropsPath", props; "CustomAfterMicrosoftCommonTargets", targets; "NoWarn", "NU1903" ]

let private verifyPolicy directory =
    let policies =
        File.ReadAllLines(Path.Combine(directory, "evaluated-policy.txt"))
        |> Array.map (fun line -> line.Trim())
        |> Array.filter (fun line -> line.StartsWith("POLICY|", StringComparison.Ordinal))
    if policies.Length = 0 then fail "No evaluated consumer policy was observed."
    let entries (value: string) = value.Split([| ';'; ',' |], StringSplitOptions.RemoveEmptyEntries ||| StringSplitOptions.TrimEntries)
    for policy in policies do
        let parts = policy.Split('|')
        for code in [ "3239"; "44"; "4242"; "NU1605" ] do
            if not (entries parts[1] |> Array.contains code) then fail $"Missing warning-as-error {code}: {policy}"
        for code in [ "1182"; "NU1901"; "NU1903" ] do
            if not (entries parts[2] |> Array.contains code) then fail $"Lost NoWarn entry {code}: {policy}"
        if not (entries parts[3] |> Array.contains "NU1902") then fail $"Lost RestoreNoWarn entry: {policy}"
        if [ parts[2]; parts[3] ] |> List.exists (fun value -> entries value |> Array.exists (fun code -> code.Equals("NU1605", StringComparison.OrdinalIgnoreCase))) then
            fail $"NU1605 is still suppressed: {policy}"
    printfn "Evaluated policy: %s" policies[0]

let private warningTarget code =
    XElement("Target", XAttribute("Name", "ExistingWarningControl"), XAttribute("BeforeTargets", "PrepareForBuild"),
        XElement("Warning", XAttribute("Code", code), XAttribute("Text", "Existing warning policy negative control")))

let private lateSuppression directory =
    let path = Path.Combine(directory, "Directory.Build.targets")
    XElement("Project",
        XElement("PropertyGroup",
            property "NoWarn" "$(NoWarn);NU1605;NU1904",
            property "RestoreNoWarn" "$(RestoreNoWarn);NU1605;NU1905"))
        .Save path
    path

let private fixturePackage directory (id: string) (version: string) (dependency: string option) =
    let path = Path.Combine(directory, $"{id}.{version}.nupkg")
    use archive = ZipFile.Open(path, ZipArchiveMode.Create)
    let ns = XNamespace.Get "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd"
    let metadata = XElement(ns + "metadata",
                        XElement(ns + "id", id), XElement(ns + "version", version),
                        XElement(ns + "authors", "PipeL4 tests"), XElement(ns + "description", "Downgrade regression fixture"))
    match dependency with
    | Some dependencyId ->
        metadata.Add(XElement(ns + "dependencies", XElement(ns + "dependency",
            XAttribute("id", dependencyId), XAttribute("version", "[2.0.0,)"))))
    | None -> ()
    use stream = archive.CreateEntry(id + ".nuspec").Open()
    XElement(ns + "package", metadata).Save stream
    path

let private run packagePath =
    Directory.CreateDirectory root |> ignore
    let accepted code (output: string) = code = 0 && output.Contains("SDK-default and minimum-version F# consumers compiled.", StringComparison.Ordinal)
    let directory, environment = policyFiles "preserved-policy" [||]
    let result = runValidator packagePath environment
    check "SDK-default and explicit minimum consumers with preserved policy" accepted result
    verifyPolicy directory
    use package = ZipFile.OpenRead packagePath
    use dll = package.GetEntry("lib/net10.0/PipeL4.Plugins.dll").Open()
    let candidateHash = SHA256.HashData dll
    let assetsFiles = Directory.GetFiles(directory, "assets-*.json")
    if assetsFiles.Length <> 2 then fail "Expected evidence from both generated consumers."
    for assetsPath in assetsFiles do
        use assets = JsonDocument.Parse(File.ReadAllText assetsPath)
        let restore = assets.RootElement.GetProperty("project").GetProperty("restore")
        let sources = restore.GetProperty("sources").EnumerateObject() |> Seq.map (fun source -> source.Name) |> Seq.toArray
        if sources.Length <> 1 || not (sources[0].EndsWith("feed", StringComparison.Ordinal)) then
            fail $"Consumer did not use only its isolated feed: {assetsPath}"
        let cache = restore.GetProperty("packagesPath").GetString()
        if not (cache.StartsWith(Path.GetTempPath(), StringComparison.Ordinal)) then fail "Consumer cache was not isolated."
        let versions = assets.RootElement.GetProperty("libraries").EnumerateObject() |> Seq.map (fun library -> library.Name) |> Seq.toArray
        if versions.Length <> 2 || not (versions |> Array.exists (fun name -> name.StartsWith("FSharp.Core/", StringComparison.Ordinal))) then
            fail $"Unexpected resolved consumer dependencies: {assetsPath}"
        printfn "Resolved consumer dependencies: %s" (String.Join(", ", versions))
    let sdkAssets =
        assetsFiles
        |> Array.tryFind (fun path -> Path.GetFileName path <> "assets-10.0.100.json")
        |> Option.defaultWith (fun () -> fail "SDK-default consumer assets were not captured.")
    use sdkDocument = JsonDocument.Parse(File.ReadAllText sdkAssets)
    let sdkDefaultVersion =
        sdkDocument.RootElement.GetProperty("libraries").EnumerateObject()
        |> Seq.find (fun library -> library.Name.StartsWith("FSharp.Core/", StringComparison.Ordinal))
        |> fun library -> library.Name.Substring("FSharp.Core/".Length)
    let dllFiles = Directory.GetFiles(directory, "api-*.dll")
    if dllFiles.Length <> 2 then fail "Expected resolved DLL evidence from both consumers."
    for dllPath in dllFiles do
        if not (SHA256.HashData(File.ReadAllBytes dllPath).AsSpan().SequenceEqual(candidateHash)) then
            fail "Consumer reference DLL differs from the local candidate."
    let projectFiles = Directory.GetFiles(directory, "consumer-*.fsproj")
    if projectFiles.Length <> 2 then fail "Expected both generated consumer projects."
    for project in projectFiles do
        let document = XDocument.Load project
        if document.Descendants() |> Seq.exists (fun element -> element.Name.LocalName = "Import") then
            fail "Generated consumer unexpectedly imports extra infrastructure."
    printfn "PASS generated projects, isolated sources/caches, resolved dependencies, and candidate reference DLLs verified."

    for code in [ "3239"; "4242" ] do
        let directory, environment = policyFiles ("warning-" + code) [| warningTarget code |]
        let result = runValidator packagePath environment
        check $"existing MSBuild warning {code} remains fatal"
            (fun exitCode output -> exitCode = 1 && output.Contains("error " + code, StringComparison.Ordinal)) result
        verifyPolicy directory

    let obsoleteSource = "namespace WarningControl\nmodule Values =\n    [<System.Obsolete(\"warning control\")>]\n    let oldValue = 1\n    let value = oldValue\n"
    let fsharpTarget =
        XElement("Target", XAttribute("Name", "CompilerWarningControl"), XAttribute("BeforeTargets", "CoreCompile"),
            XElement("WriteLinesToFile", XAttribute("File", "Consumer.fs"), XAttribute("Overwrite", "true"), XAttribute("Lines", obsoleteSource)))
    let _, environment = policyFiles "compiler-warning" [| fsharpTarget |]
    runValidator packagePath environment
    |> check "existing F# warning-as-error remains fatal"
        (fun code output -> code = 1 && output.Contains("error FS0044", StringComparison.Ordinal))

    let fixtures = Path.Combine(root, "downgrade-fixtures")
    Directory.CreateDirectory fixtures |> ignore
    let packages =
        [ fixturePackage fixtures "PipeL4.Validation.Dependency" "1.0.0" None
          fixturePackage fixtures "PipeL4.Validation.Dependency" "2.0.0" None
          fixturePackage fixtures "PipeL4.Validation.Parent" "1.0.0" (Some "PipeL4.Validation.Dependency") ]
    let downgradeTarget =
        XElement("Target", XAttribute("Name", "StageDowngradeFixtures"), XAttribute("BeforeTargets", "_GenerateRestoreGraphProjectEntry"),
            XElement("Copy", XAttribute("SourceFiles", String.Join(";", packages)),
                XAttribute("DestinationFolder", "$(MSBuildProjectDirectory)/../feed")))
    let references =
        XElement("ItemGroup",
            XElement("PackageReference", XAttribute("Include", "PipeL4.Validation.Parent"), XAttribute("Version", "1.0.0")),
            XElement("PackageReference", XAttribute("Include", "PipeL4.Validation.Dependency"), XAttribute("Version", "1.0.0")))
    let directory, environment = policyFiles "downgrade" [| downgradeTarget; references |]
    let result = runValidator packagePath environment
    check "genuine NU1605 downgrade fails restore"
        (fun code output -> code = 1 && output.Contains("error NU1605", StringComparison.Ordinal)
                            && output.Contains("PipeL4.Validation.Dependency from 2.0.0 to 1.0.0", StringComparison.Ordinal)
                            && output.Contains("dotnet restore", StringComparison.Ordinal)) result
    verifyPolicy directory

    let directory, environment = policyFiles "late-downgrade" [| downgradeTarget; references |]
    let lateTargets = lateSuppression directory
    let assetsDirectory = Path.Combine(directory, "restore-obj")
    let environment =
        [ "DirectoryBuildTargetsPath", lateTargets
          "BaseIntermediateOutputPath", assetsDirectory + string Path.DirectorySeparatorChar ] @ environment
    let result = runValidator packagePath environment
    check "late Directory.Build.targets cannot suppress a genuine NU1605 downgrade"
        (fun code output -> code = 1 && output.Contains("error NU1605", StringComparison.Ordinal)
                            && output.Contains("PipeL4.Validation.Dependency from 2.0.0 to 1.0.0", StringComparison.Ordinal)) result
    verifyPolicy directory
    let assetsPath = Path.Combine(assetsDirectory, "project.assets.json")
    if not (File.Exists assetsPath) then fail "Late downgrade restore assets were not retained."
    use downgradeAssets = JsonDocument.Parse(File.ReadAllText assetsPath)
    let warningProperties = downgradeAssets.RootElement.GetProperty("project").GetProperty("restore").GetProperty("warningProperties")
    let codes = warningProperties.GetProperty("noWarn").EnumerateArray() |> Seq.map (fun code -> code.GetString()) |> Seq.toArray
    if codes |> Array.exists (fun code -> code.Equals("NU1605", StringComparison.OrdinalIgnoreCase)) then
        fail "Restore assets retained NU1605 suppression."
    for code in [ "NU1901"; "NU1903"; "NU1904" ] do
        if not (codes |> Array.contains code) then fail $"Restore assets lost unrelated suppression {code}."
    let fatalCodes = warningProperties.GetProperty("warnAsError").EnumerateArray() |> Seq.map (fun code -> code.GetString()) |> Seq.toArray
    if not (fatalCodes |> Array.contains "NU1605") then fail "Restore assets did not make NU1605 fatal."

    let infoTarget =
        XElement("Target", XAttribute("Name", "BenignNugetInformation"), XAttribute("BeforeTargets", "CoreCompile"),
            XElement("Message", XAttribute("Importance", "high"), XAttribute("Text", "Information: NU1605 downgrade check completed.")))
    let _, environment = policyFiles "informational-code" [| infoTarget |]
    runValidator packagePath environment
    |> check "informational NU1605 output is accepted" accepted

    let diagnosticTarget =
        XElement("Target", XAttribute("Name", "NugetWarningControl"), XAttribute("BeforeTargets", "PrepareForBuild"),
            XElement("Warning", XAttribute("Code", "NU1605"), XAttribute("Text", "Injected downgrade warning")))
    let _, environment = policyFiles "diagnostic-warning" [| diagnosticTarget |]
    runValidator packagePath environment
    |> check "real NU1605 warning is rejected"
        (fun code output -> code = 1 && output.Contains("error NU1605", StringComparison.Ordinal))

    let compatibleRanges = [ "bare", "10.0.100"; "inclusive", "[10.0.100,)"; "bounded", "[10.0.100,11.0.0)" ]
    let compatibleRanges = if sdkDefaultVersion = "10.0.100" then ("exact", "[10.0.100]") :: compatibleRanges else compatibleRanges
    for label, range in compatibleRanges do
        let candidate = rewriteRange packagePath label (Some range)
        runValidator candidate [] |> check $"NuGet range {range} accepted" accepted
    let incompatibleRanges =
        [ "exclusive", Some "(10.0.100,)"
          "higher", Some "[10.0.101,)"
          "lower", Some "[9.0.100,)"
          "unbounded", Some "(,10.0.100]"
          "empty", Some "[10.0.100,10.0.100)"
          "invalid", Some "not-a-version"
          "missing", None ]
    let incompatibleRanges =
        if sdkDefaultVersion = "10.0.100" then incompatibleRanges
        else [ "exact", Some "[10.0.100]"
               "bounded-sdk", Some $"[10.0.100,{sdkDefaultVersion})" ] @ incompatibleRanges
    for label, range in incompatibleRanges do
        let candidate = rewriteRange packagePath label range
        runValidator candidate []
        |> check $"NuGet range {label} rejected before restore"
            (fun code output -> code = 1 && output.Contains("FSharp.Core", StringComparison.Ordinal) && not (output.Contains("Restored ", StringComparison.Ordinal)))

let exitStatus =
    try
        try
            match fsi.CommandLineArgs |> Array.skip 1 with
            | [| packagePath |] -> run (Path.GetFullPath packagePath); 0
            | _ -> fail "Usage: dotnet fsi scripts/test_public_api_package.fsx <PipeL4.Plugins-package.nupkg>"
        with error ->
            eprintfn "Public API package regressions failed: %s" error.Message
            1
    finally
        if Directory.Exists root then Directory.Delete(root, true)

Environment.Exit exitStatus
