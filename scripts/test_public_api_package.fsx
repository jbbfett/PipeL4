open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Security.Cryptography
open System.Text.Json
open System.Xml.Linq

let private fail message = raise (InvalidOperationException message)
let private root = Path.Combine(Path.GetTempPath(), $"PipeL4 public API regressions {Guid.NewGuid():N}")

let private runValidatorFrom packagePath (environment: (string * string) list) workingDirectory fsiDll =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    let validatorPath = Path.Combine(__SOURCE_DIRECTORY__, "validate_public_api_package.fsx")
    let arguments =
        match fsiDll with
        | Some path -> [ path; validatorPath; packagePath ]
        | None -> [ "fsi"; validatorPath; packagePath ]
    for argument in arguments do
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

let private runValidator packagePath environment =
    runValidatorFrom packagePath environment Environment.CurrentDirectory None

let private runDotnet workingDirectory environment arguments =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    for argument in arguments do startInfo.ArgumentList.Add argument
    for name, value in environment do startInfo.Environment[name] <- value
    use child = new Process(StartInfo = startInfo)
    if not (child.Start()) then fail "Could not start dotnet for SDK selection regression."
    let output = child.StandardOutput.ReadToEndAsync()
    let error = child.StandardError.ReadToEndAsync()
    if not (child.WaitForExit(30000)) then
        child.Kill(true)
        fail "SDK selection regression timed out."
    child.ExitCode, output.GetAwaiter().GetResult() + error.GetAwaiter().GetResult()

let private globalPackageRoot environment =
    let code, output = runDotnet Environment.CurrentDirectory environment [ "nuget"; "locals"; "global-packages"; "--list" ]
    if code <> 0 then fail $"Could not locate NuGet package cache: {output}"
    output.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
    |> Array.tryPick (fun line ->
        let separator = line.IndexOf(':')
        if separator >= 0 then Some(line.Substring(separator + 1).Trim()) else None)
    |> Option.defaultWith (fun () -> fail "Could not parse NuGet global package cache path.")

let private sdkBasePath workingDirectory =
    let code, output = runDotnet workingDirectory [] [ "--info" ]
    if code <> 0 then fail $"Could not read selected SDK information: {output}"
    let matchResult = System.Text.RegularExpressions.Regex.Match(output, @"(?m)^\s*Base Path:\s*(.+?)\s*$")
    if not matchResult.Success then fail "Could not parse selected SDK base path."
    Path.GetFullPath(matchResult.Groups[1].Value.Trim())

let private installedSdkFsiDlls () =
    let code, output = runDotnet Environment.CurrentDirectory [] [ "--list-sdks" ]
    if code <> 0 then fail $"Could not list installed SDKs: {output}"
    output.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
    |> Array.choose (fun line ->
        let matchResult = System.Text.RegularExpressions.Regex.Match(line, @"^(\S+)\s+\[(.+)\]$")
        if not matchResult.Success then None
        else
            let sdkRoot = Path.Combine(matchResult.Groups[2].Value, matchResult.Groups[1].Value)
            let fsiDll = Path.Combine(sdkRoot, "FSharp", "fsi.dll")
            if File.Exists fsiDll then Some(matchResult.Groups[1].Value, fsiDll) else None)
    |> Array.toList

let private packageEntryHash packagePath entryPath =
    use archive = ZipFile.OpenRead packagePath
    let entry = archive.GetEntry(entryPath) |> Option.ofObj |> Option.defaultWith (fun () -> fail $"Package asset is missing: {entryPath}")
    use stream = entry.Open()
    Convert.ToHexString(SHA256.HashData stream)

let private packageNuspecIdentity packagePath =
    use archive = ZipFile.OpenRead packagePath
    let nuspec = archive.Entries |> Seq.find (fun entry -> entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
    use stream = nuspec.Open()
    let document = XDocument.Load stream
    let metadata = document.Descendants() |> Seq.find (fun element -> element.Name.LocalName = "metadata")
    let field name = metadata.Elements() |> Seq.find (fun element -> element.Name.LocalName = name) |> fun element -> element.Value.Trim()
    field "id", field "version"

let private fsharpCoreCompileAsset assetsPath version =
    use assets = JsonDocument.Parse(File.ReadAllText assetsPath)
    let target = assets.RootElement.GetProperty("targets").GetProperty("net10.0")
    let library =
        target.EnumerateObject()
        |> Seq.tryFind (fun item -> item.Name.Equals("FSharp.Core/" + version, StringComparison.OrdinalIgnoreCase))
        |> Option.defaultWith (fun () -> fail $"Captured assets do not contain FSharp.Core {version}.")
    let compile = library.Value.GetProperty("compile")
    let paths =
        compile.EnumerateObject()
        |> Seq.map (fun item -> item.Name.Replace('\\', '/'))
        |> Seq.filter (fun path -> Path.GetFileName(path).Equals("FSharp.Core.dll", StringComparison.OrdinalIgnoreCase))
        |> Seq.toArray
    if paths.Length <> 1 then fail $"Expected one FSharp.Core compile asset, found {paths.Length}."
    paths[0]

let private verifySdkReference output sdkPath assetPath version =
    let matches =
        System.Text.RegularExpressions.Regex.Matches(output, @"Verified actual Fsc reference '([^']+)' asset '([^']+)' SHA-256 ([0-9A-F]+) against selected SDK library-pack\.")
    if matches.Count <> 1 then fail $"Expected one verified SDK-default FSharp.Core compiler reference, found {matches.Count}."
    let matchResult = matches[0]
    if matchResult.Groups[2].Value <> assetPath
       || not (matchResult.Groups[1].Value.EndsWith(assetPath.Replace('/', Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase)) then
        fail $"Validator referenced FSharp.Core '{matchResult.Groups[1].Value}' / '{matchResult.Groups[2].Value}', expected asset '{assetPath}'."
    let package = Path.Combine(sdkPath, "FSharp", "library-packs", $"FSharp.Core.{version}.nupkg")
    let expectedHash = packageEntryHash package assetPath
    if not (String.Equals(matchResult.Groups[3].Value, expectedHash, StringComparison.Ordinal)) then
        fail "Validator's actual FSharp.Core compiler-reference hash differs from the selected SDK library-pack."
    printfn "PASS actual SDK-default FSharp.Core reference %s has SDK-pack SHA-256 %s." assetPath expectedHash

let private expectedFSharpCore100AssetHash assetPath =
    match assetPath with
    | "lib/netstandard2.0/FSharp.Core.dll" -> "B907C420D77CA09731659EFBB43B7F55477EBC2068CAA80ED7E581B66E508F88"
    | "lib/netstandard2.1/FSharp.Core.dll" -> "09730E5BA269E2A255588DA8839FB5CFF43899DFAFBED0D64E905A6F4171EC5B"
    | _ -> fail $"No independent fingerprint is recorded for minimum asset '{assetPath}'."

let private verifyMinimumReference output assetsPath =
    let assetPath = fsharpCoreCompileAsset assetsPath "10.0.100"
    let matches =
        System.Text.RegularExpressions.Regex.Matches(output, @"Verified actual Fsc reference '([^']+)' asset '([^']+)' SHA-256 ([0-9A-F]+) against the signed NuGet\.org FSharp\.Core 10\.0\.100 package\.")
    if matches.Count <> 1 then fail $"Expected one actual Fsc reference verified against canonical FSharp.Core 10.0.100, found {matches.Count}."
    let matchResult = matches[0]
    let expectedHash = expectedFSharpCore100AssetHash assetPath
    if matchResult.Groups[2].Value <> assetPath
       || not (matchResult.Groups[1].Value.EndsWith(assetPath.Replace('/', Path.DirectorySeparatorChar), StringComparison.OrdinalIgnoreCase))
       || not (String.Equals(matchResult.Groups[3].Value, expectedHash, StringComparison.Ordinal)) then
        fail "Validator's actual minimum-version Fsc reference differs from the signed NuGet.org FSharp.Core 10.0.100 asset."
    printfn "PASS actual minimum-version Fsc reference %s has official SHA-256 %s." assetPath expectedHash

let private makeVersionedFSharpCorePackage sourcePath targetPath version =
    use input = ZipFile.OpenRead sourcePath
    use output = new ZipArchive(new FileStream(targetPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None), ZipArchiveMode.Create)
    for entry in input.Entries do
        let target = output.CreateEntry(entry.FullName, CompressionLevel.Optimal)
        use targetStream = target.Open()
        if entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase) then
            use sourceStream = entry.Open()
            let document = XDocument.Load sourceStream
            let versionElement = document.Descendants() |> Seq.find (fun element -> element.Name.LocalName = "version")
            versionElement.Value <- version
            document.Save(targetStream, SaveOptions.DisableFormatting)
        else
            use sourceStream = entry.Open()
            sourceStream.CopyTo targetStream

let private extractPackageEntry (packagePath: string) (entryPath: string) (targetPath: string) =
    use archive = ZipFile.OpenRead packagePath
    let entry = archive.GetEntry(entryPath) |> Option.ofObj |> Option.defaultWith (fun () -> fail $"Package asset is missing: {entryPath}")
    Directory.CreateDirectory(Path.GetDirectoryName targetPath) |> ignore
    use source = entry.Open()
    use target = File.Create targetPath
    source.CopyTo target

let private makePoisonedMinimumFSharpCorePackage canonicalPath sdkPackPath targetPath =
    use canonical = ZipFile.OpenRead canonicalPath
    use sdkPack = ZipFile.OpenRead sdkPackPath
    use output = new ZipArchive(new FileStream(targetPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None), ZipArchiveMode.Create)
    let poisonedAssets = set [ "lib/netstandard2.0/FSharp.Core.dll"; "lib/netstandard2.1/FSharp.Core.dll" ]
    for entry in canonical.Entries do
        let target = output.CreateEntry(entry.FullName, CompressionLevel.Optimal)
        use targetStream = target.Open()
        if poisonedAssets.Contains entry.FullName then
            let replacement = sdkPack.GetEntry(entry.FullName) |> Option.ofObj |> Option.defaultWith (fun () -> fail $"SDK pack is missing '{entry.FullName}'.")
            use replacementStream = replacement.Open()
            replacementStream.CopyTo targetStream
        else
            use sourceStream = entry.Open()
            sourceStream.CopyTo targetStream

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
            XElement("Copy", XAttribute("SourceFiles", "$(MSBuildProjectDirectory)/global.json"),
                XAttribute("DestinationFiles", Path.Combine(directory, "sdk-$(FSharpCoreImplicitPackageVersion).json")),
                XAttribute("SkipUnchangedFiles", "true")),
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
    let selectedSdkCode, selectedSdk = runDotnet Environment.CurrentDirectory [] [ "--version" ]
    if selectedSdkCode <> 0 then fail $"Could not determine expected SDK: {selectedSdk}"
    let selectedSdk = selectedSdk.Trim()
    let sdkPath = sdkBasePath Environment.CurrentDirectory
    let selectedSdkFsiDll = Path.Combine(sdkPath, "FSharp", "fsi.dll")
    let sdkLines = result |> snd |> fun output -> output.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
    if not (sdkLines |> Array.contains $"Validator selected .NET SDK {selectedSdk}.") then
        fail $"Validator did not report the parent SDK selected by dotnet ({selectedSdk})."
    if not (sdkLines |> Array.contains $"Running FSI SDK {selectedSdk} matches workspace-selected SDK {selectedSdk}.") then
        fail "Validator did not identify and match the running FSI SDK to the workspace-selected SDK."
    let currentGlobalJson = Path.Combine(Environment.CurrentDirectory, "global.json")
    if not ((snd result).Contains($"Effective global.json: {currentGlobalJson}.", StringComparison.Ordinal)) then
        fail "Validator did not discover the current-directory global.json."
    let consumerSdkLines = sdkLines |> Array.filter (fun line -> line.StartsWith("Temporary consumer selected .NET SDK ", StringComparison.Ordinal))
    if consumerSdkLines.Length <> 2 || consumerSdkLines |> Array.exists (fun line -> not (line.EndsWith(selectedSdk + ".", StringComparison.Ordinal))) then
        fail "Both temporary consumers must verify the exact SDK selected by the validator."
    let sdkListCode, sdkList = runDotnet Environment.CurrentDirectory [] [ "--list-sdks" ]
    if sdkListCode <> 0 then fail $"Could not list installed SDKs: {sdkList}"
    let selectedVersion = Version.Parse selectedSdk
    let newerSdks =
        sdkList.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.choose (fun line ->
            let matchResult = System.Text.RegularExpressions.Regex.Match(line, @"^(\S+)\s+\[")
            if matchResult.Success then
                match Version.TryParse matchResult.Groups[1].Value with
                | true, version when version > selectedVersion -> Some version
                | _ -> None
            else None)
    if newerSdks.Length > 0 then printfn "PASS newer installed SDKs (%s) did not change temporary consumer selection." (String.Join(", ", newerSdks))
    else printfn "No newer SDK is installed locally; exact SDK pin was verified."
    printfn "PASS both temporary consumers pinned SDK %s with rollForward disabled." selectedSdk
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
    let sdkAssets = Path.Combine(directory, "assets-.json")
    if not (File.Exists sdkAssets) then fail "SDK-default consumer assets were not captured."
    use sdkDocument = JsonDocument.Parse(File.ReadAllText sdkAssets)
    let sdkDefaultVersion =
        sdkDocument.RootElement.GetProperty("libraries").EnumerateObject()
        |> Seq.find (fun library -> library.Name.StartsWith("FSharp.Core/", StringComparison.Ordinal))
        |> fun library -> library.Name.Substring("FSharp.Core/".Length)
    let sdkDefaultAsset = fsharpCoreCompileAsset sdkAssets sdkDefaultVersion
    verifySdkReference (snd result) sdkPath sdkDefaultAsset sdkDefaultVersion
    let minimumAssets = Path.Combine(directory, "assets-10.0.100.json")
    if not (File.Exists minimumAssets) then fail "Minimum-version consumer assets were not captured."
    verifyMinimumReference (snd result) minimumAssets
    let dllFiles = Directory.GetFiles(directory, "api-*.dll")
    if dllFiles.Length <> 2 then fail "Expected resolved DLL evidence from both consumers."
    for dllPath in dllFiles do
        if not (SHA256.HashData(File.ReadAllBytes dllPath).AsSpan().SequenceEqual(candidateHash)) then
            fail "Consumer reference DLL differs from the local candidate."
    let projectFiles = Directory.GetFiles(directory, "consumer-*.fsproj")
    if projectFiles.Length <> 2 then fail "Expected both generated consumer projects."
    let sdkConfigurationFiles = Directory.GetFiles(directory, "sdk-*.json")
    if sdkConfigurationFiles.Length <> 2 then fail "Expected captured global.json files from both consumers."
    for sdkConfiguration in sdkConfigurationFiles do
        use configuration = JsonDocument.Parse(File.ReadAllText sdkConfiguration)
        let sdk = configuration.RootElement.GetProperty("sdk")
        if sdk.GetProperty("version").GetString() <> selectedSdk
           || sdk.GetProperty("rollForward").GetString() <> "disable" then
            fail $"Temporary consumer global.json did not pin exact SDK {selectedSdk}: {sdkConfiguration}"
        let mutable configuredAllowPrerelease = Unchecked.defaultof<JsonElement>
        if sdk.TryGetProperty("allowPrerelease", &configuredAllowPrerelease) then
            fail "A temporary consumer added allowPrerelease when the effective repository policy did not specify it."
    for project in projectFiles do
        let document = XDocument.Load project
        if document.Descendants() |> Seq.exists (fun element -> element.Name.LocalName = "Import") then
            fail "Generated consumer unexpectedly imports extra infrastructure."
    printfn "PASS generated projects, isolated sources/caches, resolved dependencies, and candidate reference DLLs verified."

    let fallbackCache = Path.Combine(root, "fallback-package-cache")
    let minimumPackage = Path.Combine(globalPackageRoot [], "fsharp.core", "10.0.100", "fsharp.core.10.0.100.nupkg")
    if sdkDefaultVersion <> "10.0.100" then
        if not (File.Exists minimumPackage) then fail $"Minimum-version package is unavailable for isolated regression setup: {minimumPackage}"
        let minimumCacheDirectory = Path.Combine(fallbackCache, "fsharp.core", "10.0.100")
        Directory.CreateDirectory minimumCacheDirectory |> ignore
        File.Copy(minimumPackage, Path.Combine(minimumCacheDirectory, "fsharp.core.10.0.100.nupkg"), true)
        let defaultCacheDirectory = Path.Combine(fallbackCache, "fsharp.core", sdkDefaultVersion)
        Directory.CreateDirectory defaultCacheDirectory |> ignore
        let incorrectPayload = Path.Combine(defaultCacheDirectory, $"fsharp.core.{sdkDefaultVersion}.nupkg")
        makeVersionedFSharpCorePackage minimumPackage incorrectPayload sdkDefaultVersion
        let incorrectId, incorrectVersion = packageNuspecIdentity incorrectPayload
        if incorrectId <> "FSharp.Core" || incorrectVersion <> sdkDefaultVersion then
            fail $"Wrong-payload negative control has unexpected nuspec identity {incorrectId} {incorrectVersion}."
        let incorrectHash = packageEntryHash incorrectPayload sdkDefaultAsset
        let expectedHash = packageEntryHash (Path.Combine(sdkPath, "FSharp", "library-packs", $"FSharp.Core.{sdkDefaultVersion}.nupkg")) sdkDefaultAsset
        if String.Equals(incorrectHash, expectedHash, StringComparison.Ordinal) then
            fail "The wrong-payload negative control did not contain different FSharp.Core DLL bytes."
        printfn "Constructed wrong-payload control: FSharp.Core %s metadata with 10.0.100 DLL payload (%s)." sdkDefaultVersion incorrectHash
    let fallbackResult = runValidator packagePath [ "NUGET_PACKAGES", fallbackCache ]
    check "SDK-default compatibility survives an exact-version wrong-DLL cache package" accepted fallbackResult
    let fallbackDiagnostics = snd fallbackResult
    if not (fallbackDiagnostics.Contains($"Staged FSharp.Core {sdkDefaultVersion} from selected SDK library-pack.", StringComparison.Ordinal)) then
        fail "The wrong-payload SDK-default package was not replaced by the selected SDK library-pack."
    if sdkDefaultVersion <> "10.0.100"
       && not (fallbackDiagnostics.Contains("Staged FSharp.Core 10.0.100 from exact package cache entry.", StringComparison.Ordinal)) then
        fail "The minimum FSharp.Core package was not staged from the exact isolated cache entry."
    verifySdkReference fallbackDiagnostics sdkPath sdkDefaultAsset sdkDefaultVersion
    verifyMinimumReference fallbackDiagnostics minimumAssets
    printfn "PASS wrong-version metadata cannot make an incorrect FSharp.Core DLL payload pass SDK-default validation."

    let sdkDefaultPackage = Path.Combine(sdkPath, "FSharp", "library-packs", $"FSharp.Core.{sdkDefaultVersion}.nupkg")
    let minimumPoisonCache = Path.Combine(root, "minimum-poison-cache")
    let minimumPoisonPackage = Path.Combine(minimumPoisonCache, "fsharp.core", "10.0.100", "fsharp.core.10.0.100.nupkg")
    Directory.CreateDirectory(Path.GetDirectoryName minimumPoisonPackage) |> ignore
    makePoisonedMinimumFSharpCorePackage minimumPackage sdkDefaultPackage minimumPoisonPackage
    let poisonId, poisonVersion = packageNuspecIdentity minimumPoisonPackage
    if poisonId <> "FSharp.Core" || poisonVersion <> "10.0.100" then
        fail $"Minimum poison control changed package identity to {poisonId} {poisonVersion}."
    for assetPath in [ "lib/netstandard2.0/FSharp.Core.dll"; "lib/netstandard2.1/FSharp.Core.dll" ] do
        let poisonedHash = packageEntryHash minimumPoisonPackage assetPath
        let sdkHash = packageEntryHash sdkDefaultPackage assetPath
        if not (String.Equals(poisonedHash, sdkHash, StringComparison.Ordinal))
           || String.Equals(poisonedHash, expectedFSharpCore100AssetHash assetPath, StringComparison.Ordinal) then
            fail $"Minimum poison control did not substitute the FSharp.Core 10.0.115 payload at '{assetPath}'."
    let minimumPoisonCode, minimumPoisonOutput = runValidator packagePath [ "NUGET_PACKAGES", minimumPoisonCache ]
    if minimumPoisonCode = 0
       || not (minimumPoisonOutput.Contains("Consumer scenario requires FSharp.Core 10.0.100.", StringComparison.Ordinal))
       || not (minimumPoisonOutput.Contains("FSharp.Core 10.0.100 package SHA-512 does not match the trusted NuGet.org package fingerprint", StringComparison.Ordinal))
       || not (minimumPoisonOutput.Contains("Verified actual Fsc reference", StringComparison.Ordinal)) then
        fail $"The production validator did not reject the identity-preserving minimum DLL substitution at the package-integrity check: {minimumPoisonOutput}"
    printfn "PASS production validator rejected FSharp.Core 10.0.100 metadata carrying 10.0.115 DLL payloads."

    let substitutionEvidence = Path.Combine(root, "fsc-reference-substitution-evidence")
    Directory.CreateDirectory substitutionEvidence |> ignore
    let substitutionReplacementRoot = Path.Combine(root, "fsc-reference-substitution")
    let substitutionReplacementPath = Path.Combine(substitutionReplacementRoot, sdkDefaultAsset.Replace('/', Path.DirectorySeparatorChar))
    extractPackageEntry sdkDefaultPackage sdkDefaultAsset substitutionReplacementPath
    let referencePathEvidence = Path.Combine(substitutionEvidence, "reference-path-before.txt")
    let referenceWithRefEvidence = Path.Combine(substitutionEvidence, "reference-with-ref-before.txt")
    let compilerArgumentsEvidence = Path.Combine(substitutionEvidence, "fsc-command-line-args.txt")
    let substitutionReachedEvidence = Path.Combine(substitutionEvidence, "substitution-reached.txt")
    let substitutionTarget =
        XElement("Target", XAttribute("Name", "SubstituteMinimumFscFSharpCoreReference"),
            XAttribute("AfterTargets", "FindReferenceAssembliesForReferences"), XAttribute("BeforeTargets", "CoreCompile"),
            XAttribute("Condition", "'$(FSharpCoreImplicitPackageVersion)' == '10.0.100'"),
            XElement("ItemGroup",
                XElement("_OriginalFSharpCoreReferencePath", XAttribute("Include", "@(ReferencePath)"),
                    XAttribute("Condition", "'%(ReferencePath.Filename)' == 'FSharp.Core'")),
                XElement("_OriginalFSharpCoreReferenceWithRefs", XAttribute("Include", "@(ReferencePathWithRefAssemblies)"),
                    XAttribute("Condition", "'%(ReferencePathWithRefAssemblies.Filename)' == 'FSharp.Core'"))),
            XElement("WriteLinesToFile", XAttribute("File", referencePathEvidence),
                XAttribute("Lines", "@(_OriginalFSharpCoreReferencePath->'%(FullPath)')"), XAttribute("Overwrite", "true")),
            XElement("WriteLinesToFile", XAttribute("File", referenceWithRefEvidence),
                XAttribute("Lines", "@(_OriginalFSharpCoreReferenceWithRefs->'%(FullPath)')"), XAttribute("Overwrite", "true")),
            XElement("ItemGroup",
                XElement("ReferencePathWithRefAssemblies", XAttribute("Remove", "@(ReferencePathWithRefAssemblies)"),
                    XAttribute("Condition", "'%(ReferencePathWithRefAssemblies.Filename)' == 'FSharp.Core'")),
                XElement("ReferencePathWithRefAssemblies", XAttribute("Include", substitutionReplacementPath))),
            XElement("WriteLinesToFile", XAttribute("File", substitutionReachedEvidence),
                XAttribute("Lines", "substitution ran before Fsc"), XAttribute("Overwrite", "true")))
    let captureFscArgumentsTarget =
        XElement("Target", XAttribute("Name", "CaptureSubstitutedFscCommandLine"), XAttribute("AfterTargets", "CoreCompile"),
            XAttribute("Condition", "'$(FSharpCoreImplicitPackageVersion)' == '10.0.100'"),
            XElement("WriteLinesToFile", XAttribute("File", compilerArgumentsEvidence),
                XAttribute("Lines", "@(FscCommandLineArgs)"), XAttribute("Overwrite", "true")))
    let _, substitutionEnvironment = policyFiles "fsc-reference-substitution" [| substitutionTarget; captureFscArgumentsTarget |]
    let substitutionCode, substitutionOutput = runValidator packagePath substitutionEnvironment
    let evidenceText path = if File.Exists path then File.ReadAllText path else "<missing>"
    if substitutionCode = 0
       || not (File.Exists substitutionReachedEvidence)
       || not (File.Exists referencePathEvidence)
       || not (File.Exists referenceWithRefEvidence)
       || not (File.Exists compilerArgumentsEvidence)
       || not (substitutionOutput.Contains("differs from the staged package asset", StringComparison.Ordinal))
       || not (substitutionOutput.Contains($"Captured Fsc command-line FSharp.Core reference '{substitutionReplacementPath}'.", StringComparison.Ordinal)) then
        fail $"The production validator did not reject a substituted Fsc reference: {substitutionOutput}{Environment.NewLine}ReferencePath before substitution: {evidenceText referencePathEvidence}{Environment.NewLine}ReferencePathWithRefAssemblies before substitution: {evidenceText referenceWithRefEvidence}{Environment.NewLine}Captured Fsc arguments: {evidenceText compilerArgumentsEvidence}"
    let unchangedReferencePath = File.ReadAllLines referencePathEvidence |> Array.filter (String.IsNullOrWhiteSpace >> not)
    let originalCompilerReference = File.ReadAllLines referenceWithRefEvidence |> Array.filter (String.IsNullOrWhiteSpace >> not)
    let actualCompilerArguments = File.ReadAllLines compilerArgumentsEvidence
    if unchangedReferencePath.Length <> 1
       || originalCompilerReference.Length <> 1
       || not (unchangedReferencePath[0].Contains("fsharp.core" + string Path.DirectorySeparatorChar + "10.0.100", StringComparison.OrdinalIgnoreCase))
       || not (originalCompilerReference[0].Contains("fsharp.core" + string Path.DirectorySeparatorChar + "10.0.100", StringComparison.OrdinalIgnoreCase))
       || not (actualCompilerArguments |> Array.exists (fun argument -> argument.Contains(substitutionReplacementPath, StringComparison.Ordinal)))
       || String.Equals(unchangedReferencePath[0], substitutionReplacementPath, StringComparison.OrdinalIgnoreCase) then
        fail "The Fsc substitution control did not preserve ReferencePath while changing the actual Fsc command-line reference."
    let substitutedHash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes substitutionReplacementPath))
    let sdkPayloadHash = packageEntryHash sdkDefaultPackage sdkDefaultAsset
    if not (String.Equals(substitutedHash, sdkPayloadHash, StringComparison.Ordinal)) then
        fail "The independently captured substituted Fsc reference did not contain the expected 10.0.115 DLL."
    printfn "PASS inherited target kept ReferencePath unchanged and changed Fsc's captured command-line input; validator rejected it."

    let relativeReferenceWorkingDirectory = Path.Combine(root, "relative-reference-validator-cwd")
    Directory.CreateDirectory relativeReferenceWorkingDirectory |> ignore
    File.WriteAllText(
        Path.Combine(relativeReferenceWorkingDirectory, "global.json"),
        $"{{\"sdk\":{{\"version\":\"{selectedSdk}\",\"rollForward\":\"disable\"}}}}")
    let relativeReferencePath = Path.Combine("fsc-relative-reference", "FSharp.Core.dll")
    let relativeReferenceAsset = fsharpCoreCompileAsset minimumAssets "10.0.100"
    let validatorWorkingDirectoryDll = Path.Combine(relativeReferenceWorkingDirectory, relativeReferencePath)
    extractPackageEntry minimumPackage relativeReferenceAsset validatorWorkingDirectoryDll
    let relativeValidatorHash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes validatorWorkingDirectoryDll))
    let trustedMinimumHash = expectedFSharpCore100AssetHash relativeReferenceAsset
    if not (String.Equals(relativeValidatorHash, trustedMinimumHash, StringComparison.Ordinal)) then
        fail "The relative-reference validator-working-directory fixture does not contain canonical FSharp.Core 10.0.100."
    let sdkWorkingDirectoryDll = Path.Combine(root, "relative-reference-sdk-default.dll")
    extractPackageEntry sdkDefaultPackage sdkDefaultAsset sdkWorkingDirectoryDll
    let sdkWorkingDirectoryHash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes sdkWorkingDirectoryDll))
    if String.Equals(sdkWorkingDirectoryHash, trustedMinimumHash, StringComparison.Ordinal) then
        fail "The relative-reference consumer-working-directory fixture must have different FSharp.Core DLL bytes."

    let runRelativeReferenceControl label (replacementDll: string) (shouldPass: bool) =
        let evidenceDirectory = Path.Combine(root, label + "-evidence")
        Directory.CreateDirectory evidenceDirectory |> ignore
        let compilerWorkingDirectoryEvidence = Path.Combine(evidenceDirectory, "compiler-working-directory.txt")
        let compilerReferenceHashEvidence = Path.Combine(evidenceDirectory, "compiler-reference-sha256.txt")
        let compilerArgumentsEvidence = Path.Combine(evidenceDirectory, "fsc-command-line-args.txt")
        let relativeSubstitutionTarget =
            XElement("Target", XAttribute("Name", "SubstituteRelativeMinimumFscReference"),
                XAttribute("AfterTargets", "FindReferenceAssembliesForReferences"), XAttribute("BeforeTargets", "CoreCompile"),
                XAttribute("Condition", "'$(FSharpCoreImplicitPackageVersion)' == '10.0.100'"),
                XElement("MakeDir", XAttribute("Directories", "$(MSBuildProjectDirectory)/" + Path.GetDirectoryName relativeReferencePath)),
                XElement("Copy", XAttribute("SourceFiles", replacementDll),
                    XAttribute("DestinationFiles", "$(MSBuildProjectDirectory)/" + relativeReferencePath),
                    XAttribute("OverwriteReadOnlyFiles", "true")),
                XElement("GetFileHash", XAttribute("Files", "$(MSBuildProjectDirectory)/" + relativeReferencePath),
                    XAttribute("Algorithm", "SHA256"),
                    XElement("Output", XAttribute("TaskParameter", "Items"), XAttribute("ItemName", "_RelativeFSharpCoreReferenceHash"))),
                XElement("WriteLinesToFile", XAttribute("File", compilerReferenceHashEvidence),
                    XAttribute("Lines", "@(_RelativeFSharpCoreReferenceHash->'%(FileHash)')"), XAttribute("Overwrite", "true")),
                XElement("ItemGroup",
                    XElement("ReferencePathWithRefAssemblies", XAttribute("Remove", "@(ReferencePathWithRefAssemblies)"),
                        XAttribute("Condition", "'%(ReferencePathWithRefAssemblies.Filename)' == 'FSharp.Core'")),
                    XElement("ReferencePathWithRefAssemblies", XAttribute("Include", relativeReferencePath))))
        let captureRelativeFscArgumentsTarget =
            XElement("Target", XAttribute("Name", "CaptureRelativeFscInputs"), XAttribute("AfterTargets", "CoreCompile"),
                XAttribute("Condition", "'$(FSharpCoreImplicitPackageVersion)' == '10.0.100'"),
                XElement("WriteLinesToFile", XAttribute("File", compilerWorkingDirectoryEvidence),
                    XAttribute("Lines", "$(MSBuildProjectDirectory)"), XAttribute("Overwrite", "true")),
                XElement("WriteLinesToFile", XAttribute("File", compilerArgumentsEvidence),
                    XAttribute("Lines", "@(FscCommandLineArgs)"), XAttribute("Overwrite", "true")))
        let _, relativeEnvironment = policyFiles label [| relativeSubstitutionTarget; captureRelativeFscArgumentsTarget |]
        let relativeCode, relativeOutput =
            runValidatorFrom packagePath relativeEnvironment relativeReferenceWorkingDirectory (Some selectedSdkFsiDll)
        let compilerWorkingDirectory =
            if File.Exists compilerWorkingDirectoryEvidence then File.ReadAllText(compilerWorkingDirectoryEvidence).Trim()
            else fail $"{label} did not capture the Fsc working directory."
        let compilerReferencePath = Path.GetFullPath(relativeReferencePath, compilerWorkingDirectory)
        if not (File.Exists compilerReferenceHashEvidence) then
            let capturedArguments = if File.Exists compilerArgumentsEvidence then File.ReadAllText compilerArgumentsEvidence else "<missing>"
            fail $"{label} did not capture the DLL hash at compiler-resolved path '{compilerReferencePath}'. Validator exit {relativeCode}.{Environment.NewLine}{relativeOutput}{Environment.NewLine}Captured Fsc arguments: {capturedArguments}"
        let compilerReferenceHash = File.ReadAllText(compilerReferenceHashEvidence).Trim()
        let compilerArguments = File.ReadAllLines compilerArgumentsEvidence
        if not (compilerArguments |> Array.exists (fun argument -> argument.Contains("-r:" + relativeReferencePath, StringComparison.Ordinal))) then
            fail $"{label} did not independently capture Fsc's relative -r: reference: {String.Join(Environment.NewLine, compilerArguments)}"
        if shouldPass then
            if relativeCode <> 0
               || not (relativeOutput.Contains($"Captured Fsc command-line FSharp.Core reference '{compilerReferencePath}'.", StringComparison.Ordinal))
               || not (String.Equals(compilerReferenceHash, trustedMinimumHash, StringComparison.Ordinal)) then
                fail $"A valid relative Fsc reference was not accepted against the consumer working directory: {relativeOutput}"
            printfn "PASS valid relative Fsc reference resolved from compiler working directory '%s'." compilerWorkingDirectory
        else
            let validatorReferenceHash =
                Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(Path.Combine(relativeReferenceWorkingDirectory, relativeReferencePath))))
            if relativeCode = 0
               || not (relativeOutput.Contains("differs from the staged package asset", StringComparison.Ordinal))
               || not (relativeOutput.Contains($"Captured Fsc command-line FSharp.Core reference '{compilerReferencePath}'.", StringComparison.Ordinal))
               || not (String.Equals(compilerReferenceHash, sdkWorkingDirectoryHash, StringComparison.Ordinal))
               || not (String.Equals(validatorReferenceHash, trustedMinimumHash, StringComparison.Ordinal))
               || String.Equals(compilerReferenceHash, validatorReferenceHash, StringComparison.Ordinal) then
                fail $"The relative Fsc substitution did not reach trusted compiler-reference hash verification: {relativeOutput}"
            printfn "PASS relative Fsc substitution was rejected using DLL bytes from compiler working directory '%s'." compilerWorkingDirectory

    runRelativeReferenceControl "relative-reference-valid" validatorWorkingDirectoryDll true
    runRelativeReferenceControl "relative-reference-substitution" sdkWorkingDirectoryDll false

    let runReferenceCardinalityControl label (removeReference: bool) (addDuplicate: bool) expectedCount =
        let evidenceDirectory = Path.Combine(root, label + "-evidence")
        Directory.CreateDirectory evidenceDirectory |> ignore
        let compilerArgumentsEvidence = Path.Combine(evidenceDirectory, "fsc-command-line-args.txt")
        let duplicateReferencePath = Path.Combine(root, label, "FSharp.Core.dll")
        if addDuplicate then extractPackageEntry sdkDefaultPackage sdkDefaultAsset duplicateReferencePath
        let referenceItems =
            [ if removeReference then
                  yield XElement("ReferencePathWithRefAssemblies", XAttribute("Remove", "@(ReferencePathWithRefAssemblies)"),
                      XAttribute("Condition", "'%(ReferencePathWithRefAssemblies.Filename)' == 'FSharp.Core'"))
              if addDuplicate then
                  yield XElement("ReferencePathWithRefAssemblies", XAttribute("Include", duplicateReferencePath)) ]
        let cardinalityTarget =
            XElement("Target", XAttribute("Name", "CreateFscReferenceCardinalityControl"),
                XAttribute("AfterTargets", "FindReferenceAssembliesForReferences"), XAttribute("BeforeTargets", "CoreCompile"),
                XAttribute("Condition", "'$(FSharpCoreImplicitPackageVersion)' == '10.0.100'"),
                XElement("PropertyGroup", XElement("SkipCompilerExecution", "true"), XElement("SkipCopyBuildProduct", "true")),
                XElement("ItemGroup", referenceItems))
        let captureCardinalityArgumentsTarget =
            XElement("Target", XAttribute("Name", "CaptureCardinalityControlFscArguments"), XAttribute("AfterTargets", "CoreCompile"),
                XAttribute("Condition", "'$(FSharpCoreImplicitPackageVersion)' == '10.0.100'"),
                XElement("WriteLinesToFile", XAttribute("File", compilerArgumentsEvidence),
                    XAttribute("Lines", "@(FscCommandLineArgs)"), XAttribute("Overwrite", "true")))
        let _, cardinalityEnvironment = policyFiles label [| cardinalityTarget; captureCardinalityArgumentsTarget |]
        let cardinalityCode, cardinalityOutput = runValidator packagePath cardinalityEnvironment
        let actualCompilerReferences =
            if File.Exists compilerArgumentsEvidence then
                File.ReadAllLines compilerArgumentsEvidence
                |> Array.filter (fun argument ->
                    argument.Contains("FSharp.Core.dll", StringComparison.OrdinalIgnoreCase)
                    && (argument.StartsWith("-r:", StringComparison.OrdinalIgnoreCase)
                        || argument.StartsWith("--reference:", StringComparison.OrdinalIgnoreCase)))
            else [||]
        if cardinalityCode = 0
           || not (cardinalityOutput.Contains($"Expected one actual FSharp.Core compiler reference in Fsc command-line arguments, found {expectedCount}.", StringComparison.Ordinal))
           || actualCompilerReferences.Length <> expectedCount then
            fail $"{label} did not reach the actual Fsc reference cardinality check: {cardinalityOutput}{Environment.NewLine}Captured Fsc arguments: {String.Join(Environment.NewLine, actualCompilerReferences)}"
        printfn "PASS %s actual Fsc reference cardinality was rejected at %d." label expectedCount

    runReferenceCardinalityControl "missing-fsc-fsharp-core-reference" true false 0
    runReferenceCardinalityControl "duplicate-fsc-fsharp-core-reference" false true 2

    let availableSdks = installedSdkFsiDlls ()
    let mismatchSdk =
        availableSdks
        |> List.tryFind (fun (version, _) -> version = "9.0.121")
        |> Option.orElseWith (fun () -> availableSdks |> List.tryFind (fun (version, _) -> version <> selectedSdk))
    match mismatchSdk with
    | Some(mismatchVersion, fsiDll) ->
        let mismatchCode, mismatchOutput = runValidatorFrom packagePath [] Environment.CurrentDirectory (Some fsiDll)
        if mismatchCode = 0
           || not (mismatchOutput.Contains($"Running FSI SDK {mismatchVersion} at", StringComparison.Ordinal))
           || not (mismatchOutput.Contains($"does not match workspace-selected SDK {selectedSdk}", StringComparison.Ordinal))
           || mismatchOutput.Contains("SDK-default and minimum-version F# consumers compiled.", StringComparison.Ordinal) then
            fail $"Validator did not reject running FSI SDK {mismatchVersion} against selected SDK {selectedSdk}: {mismatchOutput}"
        printfn "PASS FSI SDK mismatch negative control: running %s was rejected while workspace selected %s." mismatchVersion selectedSdk
    | None -> fail "No second installed SDK is available for the FSI-host mismatch control."

    let ancestorDirectory = Path.Combine(root, "ancestor-policy")
    let nestedWorkingDirectory = Path.Combine(ancestorDirectory, "nested", "workspace")
    Directory.CreateDirectory nestedWorkingDirectory |> ignore
    let inheritedGlobalJson =
        "{\n  // inherited SDK policy\n  \"sdk\": {\n    \"version\": \"" + selectedSdk + "\",\n    \"rollForward\": \"disable\",\n    \"allowPrerelease\": false,\n  },\n}\n"
    File.WriteAllText(Path.Combine(ancestorDirectory, "global.json"), inheritedGlobalJson)
    let ancestorEvidence, ancestorEnvironment = policyFiles "ancestor-policy-evidence" [||]
    let ancestorResult = runValidatorFrom packagePath ancestorEnvironment nestedWorkingDirectory (Some selectedSdkFsiDll)
    check "ancestor global.json with comments, trailing commas, and allowPrerelease false" accepted ancestorResult
    if not ((snd ancestorResult).Contains(Path.Combine(ancestorDirectory, "global.json"), StringComparison.Ordinal)) then
        fail "Validator did not report the applicable ancestor global.json."
    let ancestorSdkConfigurations = Directory.GetFiles(ancestorEvidence, "sdk-*.json")
    if ancestorSdkConfigurations.Length <> 2 then fail "Ancestor SDK policy was not captured for both consumers."
    for sdkConfiguration in ancestorSdkConfigurations do
        use configuration = JsonDocument.Parse(File.ReadAllText sdkConfiguration)
        let sdk = configuration.RootElement.GetProperty("sdk")
        if sdk.GetProperty("version").GetString() <> selectedSdk
           || sdk.GetProperty("rollForward").GetString() <> "disable"
           || sdk.GetProperty("allowPrerelease").ValueKind <> JsonValueKind.False then
            fail $"Temporary consumer did not preserve ancestor SDK policy: {sdkConfiguration}"
    verifyPolicy ancestorEvidence
    verifySdkReference (snd ancestorResult) sdkPath sdkDefaultAsset sdkDefaultVersion
    verifyMinimumReference (snd ancestorResult) (Path.Combine(ancestorEvidence, "assets-10.0.100.json"))
    printfn "PASS ancestor SDK policy was discovered and preserved in both exact-pinned consumers."

    let runSdkPathsControl label (configuredPaths: string list) =
        let policyDirectory = Path.Combine(root, label)
        let nestedDirectory = Path.Combine(policyDirectory, "nested", "workspace")
        Directory.CreateDirectory nestedDirectory |> ignore
        let globalJsonPath = Path.Combine(policyDirectory, "global.json")
        let configuration =
            JsonSerializer.Serialize(
                {| sdk =
                    {| version = selectedSdk
                       rollForward = "disable"
                       paths = configuredPaths |} |})
        File.WriteAllText(globalJsonPath, configuration + Environment.NewLine)
        let evidenceDirectory, environment = policyFiles (label + "-evidence") [||]
        let result = runValidatorFrom packagePath environment nestedDirectory (Some selectedSdkFsiDll)
        check $"global.json SDK paths: {label}" accepted result
        if not ((snd result).Contains($"Effective global.json: {globalJsonPath}.", StringComparison.Ordinal)) then
            fail $"Validator did not use {globalJsonPath}."
        let consumerConfigurations = Directory.GetFiles(evidenceDirectory, "sdk-*.json")
        if consumerConfigurations.Length <> 2 then fail $"Expected both {label} consumer SDK configurations."
        let expectedPaths =
            configuredPaths
            |> List.map (fun path ->
                if path = "$host$" then path
                elif Path.IsPathRooted path then Path.GetFullPath path
                else Path.GetFullPath(Path.Combine(policyDirectory, path)))
            |> List.toArray
        for consumerConfiguration in consumerConfigurations do
            use consumerGlobal = JsonDocument.Parse(File.ReadAllText consumerConfiguration)
            let sdk = consumerGlobal.RootElement.GetProperty("sdk")
            let actualPaths = sdk.GetProperty("paths").EnumerateArray() |> Seq.map (fun path -> path.GetString()) |> Seq.toArray
            if sdk.GetProperty("version").GetString() <> selectedSdk
               || sdk.GetProperty("rollForward").GetString() <> "disable"
               || actualPaths <> expectedPaths then
                fail $"Temporary consumer changed effective SDK paths from {globalJsonPath}: {consumerConfiguration}"
        verifyPolicy evidenceDirectory
        verifySdkReference (snd result) sdkPath sdkDefaultAsset sdkDefaultVersion
        verifyMinimumReference (snd result) (Path.Combine(evidenceDirectory, "assets-10.0.100.json"))
        printfn "PASS %s SDK paths and exact SDK selection were preserved." label

    runSdkPathsControl "sdk-path-host-token" [ "$host$" ]
    let absoluteMissingSdkPath = Path.Combine(root, "absolute-sdk-search-path")
    runSdkPathsControl "sdk-path-mixed" [ "./relative-sdk-search-path"; absoluteMissingSdkPath; "$host$" ]

    let unavailableSdkDirectory = Path.Combine(root, "unavailable-sdk")
    Directory.CreateDirectory unavailableSdkDirectory |> ignore
    let unavailableSdk = "10.0.999"
    File.WriteAllText(Path.Combine(unavailableSdkDirectory, "global.json"), $"{{\"sdk\":{{\"version\":\"{unavailableSdk}\",\"rollForward\":\"disable\"}}}}")
    let unavailableCode, unavailableOutput = runValidatorFrom packagePath [] unavailableSdkDirectory (Some selectedSdkFsiDll)
    if unavailableCode = 0 || not (unavailableOutput.Contains(unavailableSdk, StringComparison.Ordinal)) then
        fail $"The production validator did not fail on an unavailable SDK pin: {unavailableOutput}"
    printfn "PASS production validator failed explicitly for unavailable pinned SDK %s." unavailableSdk

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
