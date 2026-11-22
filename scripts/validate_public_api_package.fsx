open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Reflection
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Text.Json.Nodes
open System.Text.RegularExpressions
open System.Xml.Linq

type PackageSummary =
    { PackagePath: string
      PackageId: string
      Version: string
      TargetFramework: string
      AssemblyName: string
      AssemblyVersion: string
      Dependencies: string list }

exception private ValidationException of string

let private fail message = raise (ValidationException message)
let private localName (element: XElement) = element.Name.LocalName

let private requiredText (parent: XElement) namespaceName name =
    match parent.Element(XName.Get(name, namespaceName)) |> Option.ofObj with
    | Some element when not (String.IsNullOrWhiteSpace element.Value) -> element.Value.Trim()
    | _ -> fail $"Required nuspec metadata '{name}' is missing or empty."

let private runProcess reportOutput (workingDirectory: string) (fileName: string) (arguments: string list) =
    let startInfo = ProcessStartInfo(fileName)
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.Environment.Remove "FSharpCoreImplicitPackageVersion" |> ignore
    for argument in arguments do
        startInfo.ArgumentList.Add argument

    use child = new Process(StartInfo = startInfo)
    if not (child.Start()) then
        fail $"Could not start '{fileName}'."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()
    let commandArguments = String.Join(" ", arguments |> List.toArray)
    if not (child.WaitForExit(180000)) then
        child.Kill(true)
        fail $"Command '{fileName} {commandArguments}' timed out."

    let output = outputTask.GetAwaiter().GetResult()
    let error = errorTask.GetAwaiter().GetResult()
    if child.ExitCode <> 0 then
        fail
            $"Command '{fileName} {commandArguments}' exited {child.ExitCode}.\n{output}{error}"
    if reportOutput then
        if not (String.IsNullOrEmpty output) then
            Console.Out.Write output
        if not (String.IsNullOrEmpty error) then
            Console.Error.Write error
    output, error

let private packageFrameworkGroup (namespaceName: string) (metadata: XElement) =
    let dependencyContainers =
        metadata.Elements(XName.Get("dependencies", namespaceName))
        |> Seq.toArray

    if dependencyContainers.Length > 1 then
        fail "Package must contain at most one dependencies element."

    match dependencyContainers with
    | [||] -> [||]
    | [| dependencies |] ->
        let directDependencies =
            dependencies.Elements(XName.Get("dependency", namespaceName))
            |> Seq.toArray
        let groups = dependencies.Elements(XName.Get("group", namespaceName)) |> Seq.toArray
        if directDependencies.Length > 0 && groups.Length > 0 then
            fail "Package dependencies cannot mix direct dependency declarations and framework groups."

        if directDependencies.Length > 0 then
            directDependencies
        else
            for group in groups do
                let framework =
                    group.Attribute(XName.Get "targetFramework")
                    |> Option.ofObj
                    |> Option.map (fun attribute -> attribute.Value)
                    |> Option.defaultValue ""
                if framework <> ".NETCoreApp10.0" && framework <> "net10.0" then
                    fail $"Package dependency group targets unexpected framework '{framework}'."

            groups
            |> Array.collect (fun group -> group.Elements(XName.Get("dependency", namespaceName)) |> Seq.toArray)
    | _ -> fail "Package dependencies metadata is ambiguous."

let private versioning =
    Assembly.LoadFrom(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "NuGet.Versioning.dll")))
let private rangeType = versioning.GetType("NuGet.Versioning.VersionRange", true)
let private versionType = versioning.GetType("NuGet.Versioning.NuGetVersion", true)

let private versionRangeIncludes (range: obj) (version: string) =
    let parsedVersion = versionType.GetMethod("Parse", [| typeof<string> |]).Invoke(null, [| box version |])
    rangeType.GetMethod("Satisfies", [| versionType |]).Invoke(range, [| parsedVersion |]) :?> bool

let private parsePackageVersionRange (value: string) =
    // Use the running FSI SDK's NuGet parser without adding a package restore or dependency graph.
    let parsed = [| box value; null |]
    let tryParse = rangeType.GetMethod("TryParse", [| typeof<string>; rangeType.MakeByRefType() |])
    if not (tryParse.Invoke(null, parsed) :?> bool) then
        fail $"FSharp.Core dependency has an invalid NuGet version range: '{value}'."
    let range = parsed[1]
    let minimum = versionType.GetMethod("Parse", [| typeof<string> |]).Invoke(null, [| box "10.0.100" |])
    let lowerBound = rangeType.GetProperty("MinVersion").GetValue range
    let inclusive = rangeType.GetProperty("IsMinInclusive").GetValue range :?> bool
    let satisfies = versionRangeIncludes range "10.0.100"
    if isNull lowerBound || not (lowerBound.Equals minimum) || not inclusive || not satisfies then
        fail $"FSharp.Core dependency must have inclusive minimum 10.0.100 and include that version; found '{value}'."
    range

let private requireVersionInRange (range: obj) version =
    if not (versionRangeIncludes range version) then
        fail $"FSharp.Core dependency range does not include selected version {version}."

let private hasDowngradeDiagnostic (output: string) =
    Regex.IsMatch(output, @"(?im)^.*\b(?:error|warning)\s+NU1605\s*:")

let private warningListWithoutDowngrade (value: string) =
    value.Split([| ';'; ',' |], StringSplitOptions.RemoveEmptyEntries ||| StringSplitOptions.TrimEntries)
    |> Array.filter (fun code -> not (code.Equals("NU1605", StringComparison.OrdinalIgnoreCase)))
    |> String.concat ";"

let private warningListWithDowngrade (value: string) =
    let entries = value.Split([| ';'; ',' |], StringSplitOptions.RemoveEmptyEntries ||| StringSplitOptions.TrimEntries)
    if entries |> Array.exists (fun code -> code.Equals("NU1605", StringComparison.OrdinalIgnoreCase)) then value
    else value + ";NU1605"

let private msbuildProperty name (value: string) =
    // MSBuild treats unescaped semicolons as property separators on the command line.
    "-p:" + name + "=" + value.Replace("%", "%25").Replace(";", "%3B")

let private locateCachedPackage (packageId: string) (version: string) =
    let globalPackages =
        match Environment.GetEnvironmentVariable("NUGET_PACKAGES") |> Option.ofObj |> Option.filter (String.IsNullOrWhiteSpace >> not) with
        | Some path -> Path.GetFullPath path
        | None ->
            let globalPackagesOutput =
                try
                    runProcess false Environment.CurrentDirectory "dotnet" [ "nuget"; "locals"; "global-packages"; "--list" ] |> fst
                with _ -> ""
            let fromDotnet =
                globalPackagesOutput.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
                |> Array.tryPick (fun line ->
                    let separator = line.IndexOf(':')
                    if separator >= 0 then Some(line.Substring(separator + 1).Trim()) else None)
            match fromDotnet with
            | Some path when not (String.IsNullOrWhiteSpace path) -> Path.GetFullPath path
            | _ ->
                let profile = Environment.GetFolderPath Environment.SpecialFolder.UserProfile
                if String.IsNullOrWhiteSpace profile then fail "Could not locate NuGet's global package cache."
                Path.Combine(profile, ".nuget", "packages")

    let normalizedId = packageId.ToLowerInvariant()
    let expectedFile = Path.Combine(globalPackages, normalizedId, version.ToLowerInvariant(), $"{normalizedId}.{version.ToLowerInvariant()}.nupkg")
    if File.Exists expectedFile then expectedFile else ""

let private verifyPackageIdentity (packagePath: string) (packageId: string) (version: string) =
    try
        use archive = ZipFile.OpenRead packagePath
        let nuspecs = archive.Entries |> Seq.filter (fun entry -> entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase)) |> Seq.toArray
        if nuspecs.Length <> 1 then false
        else
            use stream = nuspecs[0].Open()
            let document = XDocument.Load stream
            let metadata = document.Descendants() |> Seq.tryFind (fun element -> localName element = "metadata")
            match metadata with
            | None -> false
            | Some value ->
                let id = value.Elements() |> Seq.tryFind (fun element -> localName element = "id") |> Option.map (fun element -> element.Value.Trim())
                let packageVersion = value.Elements() |> Seq.tryFind (fun element -> localName element = "version") |> Option.map (fun element -> element.Value.Trim())
                id = Some packageId && packageVersion = Some version
    with _ -> false

type private SdkPolicy =
    { GlobalJsonPath: string option
      HasSdkSettings: bool
      Version: string option
      RollForward: string option
      ErrorMessage: string option
      AllowPrerelease: bool option
      Paths: string list option }

let private fsiSdkDetails () =
    let compilerService =
        AppDomain.CurrentDomain.GetAssemblies()
        |> Array.tryFind (fun assembly -> assembly.GetName().Name = "FSharp.Compiler.Service")
        |> Option.defaultWith (fun () -> fail "Could not identify the running FSI SDK: FSharp.Compiler.Service is not loaded.")
    let compilerPath = compilerService.Location
    if String.IsNullOrWhiteSpace compilerPath then
        fail "Could not identify the running FSI SDK: FSharp.Compiler.Service has no physical assembly path."
    let fsharpDirectory = Path.GetDirectoryName(Path.GetFullPath compilerPath)
    if Path.GetFileName fsharpDirectory <> "FSharp" then
        fail $"Could not identify the running FSI SDK from compiler assembly '{compilerPath}'."
    let sdkPath = Directory.GetParent(fsharpDirectory).FullName |> Path.GetFullPath
    let sdkVersion = Path.GetFileName(Path.TrimEndingDirectorySeparator sdkPath)
    if not (File.Exists(Path.Combine(fsharpDirectory, "fsi.dll"))) then
        fail $"The running FSI compiler assembly is not beside fsi.dll: '{compilerPath}'."
    sdkVersion, sdkPath

let private sdkPolicy () =
    let jsonOptions = JsonDocumentOptions(CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true)
    let mutable current = DirectoryInfo(Path.GetFullPath Environment.CurrentDirectory)
    let globalJson =
        let mutable found = None
        while found.IsNone && not (isNull current) do
            let candidate = Path.Combine(current.FullName, "global.json")
            if File.Exists candidate then found <- Some candidate
            else current <- current.Parent
        found
    match globalJson with
    | None -> { GlobalJsonPath = None; HasSdkSettings = false; Version = None; RollForward = None; ErrorMessage = None; AllowPrerelease = None; Paths = None }
    | Some path ->
        let document =
            try JsonDocument.Parse(File.ReadAllText path, jsonOptions)
            with error -> fail $"Could not parse effective global.json '{path}': {error.Message}"
        use document = document
        let root = document.RootElement
        if root.ValueKind <> JsonValueKind.Object then
            fail $"Effective global.json '{path}' must have a JSON object root."
        let mutable sdk = Unchecked.defaultof<JsonElement>
        if not (root.TryGetProperty("sdk", &sdk)) then
            { GlobalJsonPath = Some path; HasSdkSettings = false; Version = None; RollForward = None; ErrorMessage = None; AllowPrerelease = None; Paths = None }
        elif sdk.ValueKind <> JsonValueKind.Object then
            fail $"The sdk setting in effective global.json '{path}' must be an object."
        else
            let supported = set [ "version"; "rollForward"; "allowPrerelease"; "paths"; "errorMessage" ]
            for setting in sdk.EnumerateObject() do
                if not (supported.Contains setting.Name) then
                    fail $"Cannot safely preserve unrecognized SDK setting '{setting.Name}' in '{path}'."
            let readString (settingName: string) =
                let mutable value = Unchecked.defaultof<JsonElement>
                if sdk.TryGetProperty(settingName, &value) then
                    if value.ValueKind <> JsonValueKind.String || String.IsNullOrWhiteSpace(value.GetString()) then
                        fail $"The {settingName} setting in '{path}' must be a non-empty string."
                    Some(value.GetString())
                else None
            let version = readString "version"
            let rollForward = readString "rollForward"
            let errorMessage = readString "errorMessage"
            let allowPrerelease =
                let mutable value = Unchecked.defaultof<JsonElement>
                if sdk.TryGetProperty("allowPrerelease", &value) then
                    match value.ValueKind with
                    | JsonValueKind.True -> Some true
                    | JsonValueKind.False -> Some false
                    | _ -> fail $"The allowPrerelease setting in '{path}' must be true or false."
                else None
            let paths =
                let mutable value = Unchecked.defaultof<JsonElement>
                if sdk.TryGetProperty("paths", &value) then
                    if value.ValueKind <> JsonValueKind.Array then
                        fail $"The paths setting in '{path}' must be an array of strings."
                    value.EnumerateArray()
                    |> Seq.map (fun item ->
                        if item.ValueKind <> JsonValueKind.String || String.IsNullOrWhiteSpace(item.GetString()) then
                            fail $"The paths setting in '{path}' must contain only non-empty strings."
                        let itemPath = item.GetString()
                        if itemPath = "$host$" then itemPath
                        elif Path.IsPathRooted itemPath then Path.GetFullPath itemPath
                        else Path.GetFullPath(Path.Combine(Path.GetDirectoryName path, itemPath)))
                    |> Seq.toList
                    |> Some
                else None
            { GlobalJsonPath = Some path
              HasSdkSettings = true
              Version = version
              RollForward = rollForward
              ErrorMessage = errorMessage
              AllowPrerelease = allowPrerelease
              Paths = paths }

let private sdkDetails () =
    let fsiVersion, fsiPath = fsiSdkDetails ()
    let policy = sdkPolicy ()
    let selectionDirectory =
        if policy.GlobalJsonPath.IsNone then Environment.CurrentDirectory, None
        else
            let directory = Path.Combine(Path.GetTempPath(), $"PipeL4 SDK selection {Guid.NewGuid():N}")
            Directory.CreateDirectory directory |> ignore
            let sdkSettings = JsonObject()
            match policy.Version with
            | Some value -> sdkSettings["version"] <- JsonValue.Create value
            | None -> ()
            match policy.RollForward with
            | Some value -> sdkSettings["rollForward"] <- JsonValue.Create value
            | None -> ()
            match policy.ErrorMessage with
            | Some value -> sdkSettings["errorMessage"] <- JsonValue.Create value
            | None -> ()
            match policy.AllowPrerelease with
            | Some value -> sdkSettings["allowPrerelease"] <- JsonValue.Create value
            | None -> ()
            match policy.Paths with
            | Some paths ->
                let pathValues = JsonArray()
                for path in paths do pathValues.Add(JsonValue.Create path)
                sdkSettings["paths"] <- pathValues
            | None -> ()
            let root = JsonObject()
            if policy.HasSdkSettings then root["sdk"] <- sdkSettings
            File.WriteAllText(Path.Combine(directory, "global.json"), root.ToJsonString())
            directory, Some directory
    let sdkVersion, sdkPath =
        try
            let versionOutput, _ = runProcess false (fst selectionDirectory) "dotnet" [ "--version" ]
            let selectedVersion = versionOutput.Trim()
            if String.IsNullOrWhiteSpace selectedVersion then fail "Could not determine the SDK selected for the workspace."
            let sdkInfo, _ = runProcess false (fst selectionDirectory) "dotnet" [ "--info" ]
            let selectedPath =
                Regex.Match(sdkInfo, @"(?m)^\s*Base Path:\s*(.+?)\s*$")
                |> fun matchResult ->
                    if matchResult.Success then Path.GetFullPath(matchResult.Groups[1].Value.Trim())
                    else fail $"Could not locate the workspace-selected SDK {selectedVersion} from 'dotnet --info'."
            if Path.GetFileName(Path.TrimEndingDirectorySeparator selectedPath) <> selectedVersion then
                fail $"'dotnet --info' reports SDK path '{selectedPath}', which does not match selected version {selectedVersion}."
            selectedVersion, selectedPath
        finally
            match snd selectionDirectory with
            | Some directory when Directory.Exists directory -> Directory.Delete(directory, true)
            | _ -> ()
    let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal
    let normalizedFsiPath = Path.TrimEndingDirectorySeparator fsiPath
    let normalizedSdkPath = Path.TrimEndingDirectorySeparator sdkPath
    if fsiVersion <> sdkVersion || not (String.Equals(normalizedFsiPath, normalizedSdkPath, comparison)) then
        fail $"Running FSI SDK {fsiVersion} at '{fsiPath}' does not match workspace-selected SDK {sdkVersion} at '{sdkPath}'."
    printfn "Running FSI SDK %s matches workspace-selected SDK %s." fsiVersion sdkVersion
    match policy.GlobalJsonPath with
    | Some path -> printfn "Effective global.json: %s." path
    | None -> printfn "Effective global.json: none."
    sdkVersion, sdkPath, policy

let private packageEntryHash (packagePath: string) (assetPath: string) =
    use archive = ZipFile.OpenRead packagePath
    let entry =
        archive.GetEntry(assetPath)
        |> Option.ofObj
        |> Option.defaultWith (fun () -> fail $"Package '{packagePath}' is missing FSharp.Core asset '{assetPath}'.")
    use stream = entry.Open()
    Convert.ToHexString(SHA256.HashData stream)

// NuGet.org's signed FSharp.Core 10.0.100 package. The package was verified with
// `dotnet nuget verify --all`; pinning its SHA-512 keeps ordinary validation offline.
let private fsharpCore100OfficialPackageSha512 =
    "AeCswCd28W5ewTOgsg+y9Ri4l6cjwiOKBh7Qb+73Iu8fGRFdmIVU3R/U0Kb3e8yW/Up0Le/4CItCh1GqtXuLRA=="

let private fsharpCore100OfficialAssetSha256 =
    Map.ofList
        [ "lib/netstandard2.0/FSharp.Core.dll", "B907C420D77CA09731659EFBB43B7F55477EBC2068CAA80ED7E581B66E508F88"
          "lib/netstandard2.1/FSharp.Core.dll", "09730E5BA269E2A255588DA8839FB5CFF43899DFAFBED0D64E905A6F4171EC5B" ]

let private expectedFSharpCore100AssetHash assetPath =
    fsharpCore100OfficialAssetSha256
    |> Map.tryFind assetPath
    |> Option.defaultWith (fun () -> fail $"No trusted FSharp.Core 10.0.100 fingerprint is available for asset '{assetPath}'.")

let private verifyFSharpCore100Package packagePath =
    let actualHash = Convert.ToBase64String(SHA512.HashData(File.ReadAllBytes packagePath))
    if not (String.Equals(actualHash, fsharpCore100OfficialPackageSha512, StringComparison.Ordinal)) then
        fail $"FSharp.Core 10.0.100 package SHA-512 does not match the trusted NuGet.org package fingerprint: {actualHash}."
    for asset in fsharpCore100OfficialAssetSha256 do
        let assetPath = asset.Key
        let expectedHash = asset.Value
        let actualAssetHash = packageEntryHash packagePath assetPath
        if not (String.Equals(actualAssetHash, expectedHash, StringComparison.Ordinal)) then
            fail $"FSharp.Core 10.0.100 asset '{assetPath}' does not match the trusted NuGet.org DLL fingerprint."

let private fscReferenceArguments (compilerWorkingDirectory: string) (arguments: string array) =
    let compilerWorkingDirectory =
        try
            if not (Path.IsPathRooted compilerWorkingDirectory) then
                fail $"Fsc compiler working directory must be absolute: '{compilerWorkingDirectory}'."
            Path.GetFullPath compilerWorkingDirectory
        with
        | ValidationException message -> fail message
        | error -> fail $"Could not resolve Fsc compiler working directory '{compilerWorkingDirectory}': {error.Message}"
    if not (Directory.Exists compilerWorkingDirectory) then
        fail $"Fsc compiler working directory does not exist: '{compilerWorkingDirectory}'."
    let references = ResizeArray<string>()
    let mutable index = 0
    while index < arguments.Length do
        let argument = arguments[index].Trim().Trim('"')
        let referenceValue =
            if argument.StartsWith("--reference:", StringComparison.OrdinalIgnoreCase) then Some(argument.Substring("--reference:".Length))
            elif argument.StartsWith("-r:", StringComparison.OrdinalIgnoreCase) then Some(argument.Substring(3))
            elif argument.StartsWith("--reference=", StringComparison.OrdinalIgnoreCase) then Some(argument.Substring("--reference=".Length))
            elif argument.StartsWith("-r=", StringComparison.OrdinalIgnoreCase) then Some(argument.Substring(3))
            elif (argument.Equals("--reference", StringComparison.OrdinalIgnoreCase) || argument.Equals("-r", StringComparison.OrdinalIgnoreCase))
                 && index + 1 < arguments.Length then
                index <- index + 1
                Some(arguments[index].Trim().Trim('"'))
            else None
        match referenceValue with
        | Some value when not (String.IsNullOrWhiteSpace value) ->
            let reference = value.Trim().Trim('"')
            try references.Add(Path.GetFullPath(reference, compilerWorkingDirectory))
            with error -> fail $"Could not resolve Fsc reference '{reference}' against compiler working directory '{compilerWorkingDirectory}': {error.Message}"
        | _ -> ()
        index <- index + 1
    references.ToArray()

let private fsharpCoreAssets (assetsPath: string) (version: string) =
    use assets = JsonDocument.Parse(File.ReadAllText assetsPath)
    let targets = assets.RootElement.GetProperty("targets")
    let targetGroups =
        targets.EnumerateObject()
        |> Seq.filter (fun target ->
            target.Name = "net10.0" || target.Name.StartsWith("net10.0/", StringComparison.OrdinalIgnoreCase))
        |> Seq.toArray
    if targetGroups.Length <> 1 then
        fail $"Expected one restored net10.0 target for FSharp.Core {version}, found {targetGroups.Length}."
    let libraryName = "FSharp.Core/" + version
    let library =
        targetGroups[0].Value.EnumerateObject()
        |> Seq.tryFind (fun item -> item.Name.Equals(libraryName, StringComparison.OrdinalIgnoreCase))
        |> Option.defaultWith (fun () -> fail $"Restored target does not contain {libraryName}.")
    let readAssets (sectionName: string) =
        let mutable section = Unchecked.defaultof<JsonElement>
        if library.Value.TryGetProperty(sectionName, &section) && section.ValueKind = JsonValueKind.Object then
            section.EnumerateObject()
            |> Seq.map (fun asset -> asset.Name.Replace('\\', '/'))
            |> Seq.filter (fun path -> Path.GetFileName(path).Equals("FSharp.Core.dll", StringComparison.OrdinalIgnoreCase))
            |> Seq.distinct
            |> Seq.toList
        else []
    let compileAssets = readAssets "compile"
    let runtimeAssets = readAssets "runtime"
    if compileAssets.Length <> 1 then
        fail $"Expected one net10.0 FSharp.Core compile DLL asset for {version}, found {compileAssets.Length}."
    compileAssets, runtimeAssets

let private stagePackage (packageId: string) (version: string) (consumerCache: string) (sdkPath: string) (feed: string) (preferSdkPack: bool) =
    let normalizedId = packageId.ToLowerInvariant()
    let fileName = $"{normalizedId}.{version.ToLowerInvariant()}.nupkg"
    let cacheCandidates =
        [ Path.Combine(consumerCache, normalizedId, version.ToLowerInvariant(), fileName)
          locateCachedPackage packageId version ]
        |> List.filter (String.IsNullOrWhiteSpace >> not)
    let sdkPack = Path.Combine(sdkPath, "FSharp", "library-packs", $"FSharp.Core.{version}.nupkg")
    let validSdkPack = if File.Exists sdkPack && verifyPackageIdentity sdkPack packageId version then Some sdkPack else None
    let source =
        if preferSdkPack then validSdkPack
        else
            cacheCandidates
            |> List.tryFind (fun path -> File.Exists path && verifyPackageIdentity path packageId version)
            |> Option.orElse validSdkPack
        |> Option.defaultWith (fun () ->
            fail $"Could not obtain verified {packageId} {version}. Checked exact package cache entries and selected SDK library-pack '{sdkPack}'.")
    let destination = Path.Combine(feed, fileName)
    File.Copy(source, destination, true)
    if not (verifyPackageIdentity destination packageId version) then
        fail $"Staged package '{destination}' does not contain {packageId} {version}."
    if packageId.Equals("FSharp.Core", StringComparison.OrdinalIgnoreCase)
       && version = "10.0.100"
       && not preferSdkPack then
        verifyFSharpCore100Package destination
    let sourceKind = if Path.GetFullPath(source) = Path.GetFullPath(sdkPack) then "selected SDK library-pack" else "exact package cache entry"
    printfn "Staged %s %s from %s." packageId version sourceKind
    destination

let private validatePackageFile packagePath =
    let fullPath = Path.GetFullPath packagePath
    if not (File.Exists fullPath) then
        fail $"Package file does not exist: {fullPath}"

    use package = ZipFile.OpenRead fullPath
    let names = package.Entries |> Seq.map (fun entry -> entry.FullName) |> Seq.toArray
    let entries = Set.ofArray names

    if entries.Count <> names.Length then
        fail "Package archive contains duplicate entry paths."

    for name in names do
        if name.StartsWith("/", StringComparison.Ordinal)
           || name.Contains('\\')
           || (name.Split('/') |> Array.contains "..") then
            fail $"Package archive contains an unsafe path: '{name}'."

    let nuspecNames = names |> Array.filter (fun name -> name.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
    if nuspecNames.Length <> 1 then
        fail $"Expected one embedded nuspec, found {nuspecNames.Length}."

    let document =
        use stream = package.GetEntry(nuspecNames[0]).Open()
        XDocument.Load stream

    let root = document.Root |> Option.ofObj |> Option.defaultWith (fun () -> fail "Embedded nuspec has no root element.")
    let namespaceName = root.Name.NamespaceName
    if localName root <> "package" || String.IsNullOrWhiteSpace namespaceName then
        fail "Embedded nuspec root is not a namespaced NuGet package element."

    let metadataElements = root.Elements(XName.Get("metadata", namespaceName)) |> Seq.toArray
    let metadata =
        match metadataElements with
        | [| value |] -> value
        | _ -> fail "Embedded nuspec must contain exactly one metadata element."

    let packageId = requiredText metadata namespaceName "id"
    let version = requiredText metadata namespaceName "version"
    if packageId <> "PipeL4.Plugins" then
        fail $"Expected PackageId 'PipeL4.Plugins', found '{packageId}'."
    if Path.GetFileName fullPath <> $"{packageId}.{version}.nupkg" then
        fail "Package filename does not match the identity and version in its nuspec."

    let description = requiredText metadata namespaceName "description"
    if description.Equals("package description", StringComparison.OrdinalIgnoreCase) then
        fail "Package description is still the SDK placeholder."
    requiredText metadata namespaceName "authors" |> ignore
    requiredText metadata namespaceName "tags" |> ignore
    requiredText metadata namespaceName "copyright" |> ignore

    let projectUrl = requiredText metadata namespaceName "projectUrl"
    if projectUrl <> "https://github.com/jbbfett/PipeL4" then
        fail $"Unexpected project URL '{projectUrl}'."

    let repositories = metadata.Elements(XName.Get("repository", namespaceName)) |> Seq.toArray
    match repositories with
    | [| repository |] ->
        let url = repository.Attribute(XName.Get "url") |> Option.ofObj |> Option.map (fun value -> value.Value) |> Option.defaultValue ""
        let kind = repository.Attribute(XName.Get "type") |> Option.ofObj |> Option.map (fun value -> value.Value) |> Option.defaultValue ""
        if url <> "https://github.com/jbbfett/PipeL4.git" || kind <> "git" then
            fail "Package repository metadata does not match the PipeL4 git repository."
    | _ -> fail "Package must contain exactly one repository metadata element."

    let license =
        metadata.Elements(XName.Get("license", namespaceName))
        |> Seq.toArray
    let licensePath =
        match license with
        | [| value |] ->
            let kind = value.Attribute(XName.Get "type") |> Option.ofObj |> Option.map (fun attribute -> attribute.Value) |> Option.defaultValue ""
            if kind <> "file" then
                fail "Package license metadata must refer to a bundled license file."
            value.Value.Trim()
        | _ -> fail "Package must contain exactly one license metadata element."

    if licensePath <> "LICENSE" || not (entries.Contains licensePath) then
        fail $"Package license path '{licensePath}' does not resolve to a bundled file."

    let readmePath = requiredText metadata namespaceName "readme"
    if readmePath <> "README.md" || not (entries.Contains readmePath) then
        fail $"Package README path '{readmePath}' does not resolve to a bundled file."
    use readmeStream = package.GetEntry(readmePath).Open()
    use readmeReader = new StreamReader(readmeStream, UTF8Encoding(false, true), true)
    let readme = readmeReader.ReadToEnd()
    if String.IsNullOrWhiteSpace readme then
        fail "Package README is empty."

    let assemblyPath = "lib/net10.0/PipeL4.Plugins.dll"
    if not (entries.Contains assemblyPath) then
        fail $"Package is missing the public contract assembly at '{assemblyPath}'."

    let assemblies =
        names
        |> Array.filter (fun name -> name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
    if assemblies <> [| assemblyPath |] then
        let displayedAssemblies = String.Join(", ", assemblies)
        fail $"Package must contain only the public contract assembly; found: {displayedAssemblies}."

    if names |> Array.exists (fun name ->
        name.Contains("PipeL4.Core", StringComparison.OrdinalIgnoreCase)
        || name.Contains("PipeL4.PluginHost", StringComparison.OrdinalIgnoreCase)
        || name.Contains("FSharp.Compiler.Service", StringComparison.OrdinalIgnoreCase)) then
        fail "Package contains a Core, PluginHost, or compiler-service implementation binary."

    let payload =
        names
        |> Array.filter (fun name ->
            name <> nuspecNames[0]
            && name <> "[Content_Types].xml"
            && name <> "_rels/.rels"
            && name <> ".signature.p7s"
            && not (name.StartsWith("package/services/metadata/core-properties/", StringComparison.Ordinal)))
        |> Set.ofArray
    let expectedPayload = Set.ofList [ assemblyPath; "README.md"; "LICENSE" ]
    if payload <> expectedPayload then
        let actual = String.Join(", ", payload |> Seq.sort)
        fail $"Package contains unexpected or missing payload files: {actual}."

    let dependencyElements = packageFrameworkGroup namespaceName metadata
    let dependencies =
        dependencyElements
        |> Array.map (fun dependency ->
            dependency.Attribute(XName.Get "id")
            |> Option.ofObj
            |> Option.map (fun attribute -> attribute.Value)
            |> Option.defaultValue "")

    if dependencies.Length = 0 || dependencies |> Array.exists (fun id -> id <> "FSharp.Core") then
        let displayedDependencies = String.Join(", ", dependencies)
        fail $"Package must declare only its required FSharp.Core dependency; found: {displayedDependencies}."

    let fsharpDependency =
        dependencyElements
        |> Array.tryFind (fun dependency ->
            dependency.Attribute(XName.Get "id")
            |> Option.ofObj
            |> Option.exists (fun attribute -> attribute.Value = "FSharp.Core"))
        |> Option.defaultWith (fun () -> fail "Package is missing its FSharp.Core dependency.")
    let fsharpRange =
        fsharpDependency.Attribute(XName.Get "version")
        |> Option.ofObj
        |> Option.map (fun attribute -> parsePackageVersionRange attribute.Value)
        |> Option.defaultWith (fun () -> fail "FSharp.Core dependency has no version range.")

    let assemblyFile = Path.Combine(Path.GetTempPath(), $"PipeL4.Plugins.{Guid.NewGuid():N}.dll")
    let assemblyIdentity =
        try
            let copyAssembly () =
                use assemblyStream = package.GetEntry(assemblyPath).Open()
                use output = new FileStream(assemblyFile, FileMode.CreateNew, FileAccess.Write, FileShare.None)
                assemblyStream.CopyTo output
                output.Flush()
            copyAssembly ()
            AssemblyName.GetAssemblyName assemblyFile
        finally
            if File.Exists assemblyFile then
                File.Delete assemblyFile

    if assemblyIdentity.Name <> "PipeL4.Plugins" then
        fail $"Contract assembly name must remain 'PipeL4.Plugins'; found '{assemblyIdentity.Name}'."
    if assemblyIdentity.Version <> Version(1, 0, 0, 0) then
        fail $"Contract assembly CLR version must remain 1.0.0.0; found '{assemblyIdentity.Version}'."

    { PackagePath = fullPath
      PackageId = packageId
      Version = version
      TargetFramework = "net10.0"
      AssemblyName = assemblyIdentity.Name
      AssemblyVersion = assemblyIdentity.Version.ToString()
      Dependencies = dependencies |> Array.distinct |> Array.toList }
    , fsharpRange

let private validateIndependentConsumer (packagePath: string) (version: string) (fsharpRange: obj) (fsharpVersionOverride: string option) useInvalidConsumerSource (sdkVersion: string) (sdkPath: string) (sdkPolicy: SdkPolicy) =
    let root = Path.Combine(Path.GetTempPath(), $"PipeL4 public API consumer {Guid.NewGuid():N}")
    let feed = Path.Combine(root, "feed")
    let project = Path.Combine(root, "consumer")
    let packageCache = Path.Combine(root, "packages")
    Directory.CreateDirectory feed |> ignore
    Directory.CreateDirectory project |> ignore

    try
        let sdkSettings = JsonObject()
        sdkSettings["version"] <- JsonValue.Create sdkVersion
        sdkSettings["rollForward"] <- JsonValue.Create "disable"
        match sdkPolicy.ErrorMessage with
        | Some value -> sdkSettings["errorMessage"] <- JsonValue.Create value
        | None -> ()
        match sdkPolicy.AllowPrerelease with
        | Some value -> sdkSettings["allowPrerelease"] <- JsonValue.Create value
        | None -> ()
        match sdkPolicy.Paths with
        | Some paths ->
            let pathValues = JsonArray()
            for path in paths do pathValues.Add(JsonValue.Create path)
            sdkSettings["paths"] <- pathValues
        | None -> ()
        let sdkConfiguration = JsonObject()
        sdkConfiguration["sdk"] <- sdkSettings
        File.WriteAllText(Path.Combine(project, "global.json"), sdkConfiguration.ToJsonString(JsonSerializerOptions(WriteIndented = true)) + Environment.NewLine)
        let childSdkVersion =
            try runProcess false project "dotnet" [ "--version" ] |> fst
            with ValidationException message -> fail $"Temporary consumer could not select validator SDK {sdkVersion}: {message}"
        if childSdkVersion.Trim() <> sdkVersion then
            fail $"Temporary consumer selected SDK {childSdkVersion.Trim()}; expected validator SDK {sdkVersion}."
        printfn "Temporary consumer selected .NET SDK %s." sdkVersion

        File.Copy(packagePath, Path.Combine(feed, Path.GetFileName packagePath))
        let config =
            XDocument(
                XElement(
                    "configuration",
                    XElement(
                        "packageSources",
                        XElement("clear"),
                        XElement("add", XAttribute("key", "PipeL4 public API validation feed"), XAttribute("value", feed)))
                )
            )
        config.Save(Path.Combine(project, "NuGet.Config"))

        let properties =
            XElement(
                "PropertyGroup",
                XElement("TargetFramework", "net10.0"),
                XElement("RestorePackagesWithLockFile", "false"),
                XElement("DisableImplicitLibraryPacksFolder", "true"),
                XElement("WarningsAsErrors", "$(WarningsAsErrors);NU1605"),
                XElement("ProvideCommandLineArgs", "true"))
        match fsharpVersionOverride with
        | Some fsharpVersion -> properties.Add(XElement("FSharpCoreImplicitPackageVersion", fsharpVersion))
        | None -> ()

        let projectFile =
            XElement(
                "Project",
                XAttribute("Sdk", "Microsoft.NET.Sdk"),
                properties,
                XElement(
                    "ItemGroup",
                    XElement(
                        "PackageReference",
                        XAttribute("Include", "PipeL4.Plugins"),
                        XAttribute("Version", version))),
                XElement(
                    "ItemGroup",
                    XElement("Compile", XAttribute("Include", "Consumer.fs")))
            )
        projectFile.Save(Path.Combine(project, "Consumer.fsproj"))

        // Evaluate after all project imports, including Directory.Build.targets. Global
        // properties then hold the effective warning policy through restore and build.
        let evaluated, _ =
            runProcess false project "dotnet"
                [ "msbuild"
                  Path.Combine(project, "Consumer.fsproj")
                  "-getProperty:FSCorePackageVersion"
                  "-getProperty:FSharpCoreImplicitPackageVersion"
                  "-getProperty:NoWarn"
                  "-getProperty:RestoreNoWarn"
                  "-getProperty:WarningsAsErrors" ]
        use evaluatedProperties = JsonDocument.Parse evaluated
        let property (name: string) = evaluatedProperties.RootElement.GetProperty("Properties").GetProperty(name).GetString()
        let selectedFsharpVersion =
            if fsharpVersionOverride.IsSome then property "FSharpCoreImplicitPackageVersion"
            else property "FSCorePackageVersion"
        if String.IsNullOrWhiteSpace selectedFsharpVersion then
            fail "Could not determine the consumer's selected FSharp.Core version."
        let expectedFsharpVersion = fsharpVersionOverride |> Option.defaultValue selectedFsharpVersion
        let isSdkDefaultConsumer = fsharpVersionOverride.IsNone
        if selectedFsharpVersion <> expectedFsharpVersion then
            fail $"Consumer selected FSharp.Core {selectedFsharpVersion}; expected {expectedFsharpVersion}."
        requireVersionInRange fsharpRange selectedFsharpVersion
        printfn "Consumer scenario requires FSharp.Core %s." selectedFsharpVersion
        let warningPolicy =
            [ msbuildProperty "NoWarn" (warningListWithoutDowngrade (property "NoWarn"))
              msbuildProperty "RestoreNoWarn" (warningListWithoutDowngrade (property "RestoreNoWarn"))
              msbuildProperty "WarningsAsErrors" (warningListWithDowngrade (property "WarningsAsErrors")) ]
        stagePackage "FSharp.Core" expectedFsharpVersion packageCache sdkPath feed isSdkDefaultConsumer |> ignore

        let source =
            if useInvalidConsumerSource then
                """namespace IndependentConsumer

type InvalidConsumer() =
    let invalidValue: int = "not an integer"
"""
            else
                """namespace IndependentConsumer

open System.Threading
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(_connectionState: IConnectionState) =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input: ProcessorInput, _output: IProcessorOutput, cancellationToken: CancellationToken) =
            cancellationToken.ThrowIfCancellationRequested()
            let result: ProcessorResult = { RetainedLength = input.Bytes.Length }
            ValueTask<ProcessorResult>(result)
"""
        File.WriteAllText(Path.Combine(project, "Consumer.fs"), source)

        let restoreArguments =
            [ "restore"
              Path.Combine(project, "Consumer.fsproj")
              "--configfile"
              Path.Combine(project, "NuGet.Config")
              "--source"
              feed
              "--packages"
              packageCache
              "--verbosity"
              "minimal"
              "-p:NuGetAudit=false" ] @ warningPolicy
        let restoreOutput, restoreError = runProcess true project "dotnet" restoreArguments
        if hasDowngradeDiagnostic (restoreOutput + restoreError) then
            fail "Independent consumer restore reported NuGet downgrade warning NU1605."

        let assetsPath = Path.Combine(project, "obj", "project.assets.json")
        use assets = JsonDocument.Parse(File.ReadAllText assetsPath)
        let resolvedFSharpVersions =
            assets.RootElement.GetProperty("libraries").EnumerateObject()
            |> Seq.choose (fun library ->
                let separator = library.Name.IndexOf('/')
                if separator > 0
                   && library.Name.Substring(0, separator).Equals("FSharp.Core", StringComparison.OrdinalIgnoreCase) then
                    Some(library.Name.Substring(separator + 1))
                else
                    None)
            |> Seq.toArray
        if resolvedFSharpVersions <> [| expectedFsharpVersion |] then
            let displayedVersions = String.Join(", ", resolvedFSharpVersions)
            fail
                $"Independent consumer resolved FSharp.Core {displayedVersions}; expected {expectedFsharpVersion}."
        requireVersionInRange fsharpRange resolvedFSharpVersions[0]

        let fsharpAssetsPath = Path.Combine(project, "obj", "project.assets.json")
        let compileAssets, runtimeAssets = fsharpCoreAssets fsharpAssetsPath expectedFsharpVersion
        let stagedFSharpPackage = Path.Combine(feed, $"fsharp.core.{expectedFsharpVersion}.nupkg")
        let sdkFSharpPackage = Path.Combine(sdkPath, "FSharp", "library-packs", $"FSharp.Core.{expectedFsharpVersion}.nupkg")
        let isCanonicalMinimum = not isSdkDefaultConsumer && expectedFsharpVersion = "10.0.100"
        if isCanonicalMinimum then verifyFSharpCore100Package stagedFSharpPackage
        for assetPath in List.distinct (compileAssets @ runtimeAssets) do
            let stagedHash = packageEntryHash stagedFSharpPackage assetPath
            if isSdkDefaultConsumer then
                let sdkHash = packageEntryHash sdkFSharpPackage assetPath
                if not (String.Equals(stagedHash, sdkHash, StringComparison.Ordinal)) then
                    fail $"Staged FSharp.Core asset '{assetPath}' differs from the selected SDK library-pack."
            elif isCanonicalMinimum then
                let expectedHash = expectedFSharpCore100AssetHash assetPath
                if not (String.Equals(stagedHash, expectedHash, StringComparison.Ordinal)) then
                    fail $"Staged FSharp.Core 10.0.100 asset '{assetPath}' differs from the trusted NuGet.org package."
            let extractedPath = Path.Combine(packageCache, "fsharp.core", expectedFsharpVersion.ToLowerInvariant(), assetPath.Replace('/', Path.DirectorySeparatorChar))
            if not (File.Exists extractedPath) then
                fail $"Restored FSharp.Core asset '{assetPath}' is missing from the isolated consumer cache."
            let extractedHash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes extractedPath))
            if not (String.Equals(stagedHash, extractedHash, StringComparison.Ordinal)) then
                fail $"Restored FSharp.Core asset '{assetPath}' differs from the staged package."
        if isSdkDefaultConsumer then
            printfn "Verified staged FSharp.Core compile/runtime assets for net10.0 against selected SDK library-pack."
        elif isCanonicalMinimum then
            printfn "Verified staged FSharp.Core compile/runtime assets for net10.0 against the signed NuGet.org FSharp.Core 10.0.100 package."
        else
            fail $"No independent FSharp.Core payload reference exists for minimum consumer version {expectedFsharpVersion}."

        let restoredPackage =
            Path.Combine(packageCache, "pipel4.plugins", version.ToLowerInvariant(), $"pipel4.plugins.{version.ToLowerInvariant()}.nupkg")
        if not (File.Exists restoredPackage)
           || not (SHA256.HashData(File.ReadAllBytes restoredPackage).AsSpan().SequenceEqual(SHA256.HashData(File.ReadAllBytes packagePath))) then
            fail "Independent consumer did not restore the supplied local PipeL4.Plugins candidate."
        printfn "Independent consumer resolved FSharp.Core %s and the supplied local PipeL4.Plugins candidate." expectedFsharpVersion

        let referenceCapturePath = Path.Combine(project, "capture-fsharp-core.targets")
        let referencePathFile = Path.Combine(project, "reference-path-fsharp-core.txt")
        let fscArgumentsFile = Path.Combine(project, "fsc-command-line-args.txt")
        let captureProject = XElement("Project")
        let inheritedTargets = Environment.GetEnvironmentVariable("CustomAfterMicrosoftCommonTargets")
        if not (String.IsNullOrWhiteSpace inheritedTargets) then
            for inheritedTarget in inheritedTargets.Split([| ';' |], StringSplitOptions.RemoveEmptyEntries) do
                captureProject.Add(XElement("Import", XAttribute("Project", inheritedTarget)))
        captureProject.Add(
            XElement("Target", XAttribute("Name", "CaptureActualFscInputs"), XAttribute("AfterTargets", "CoreCompile"),
                XElement("ItemGroup",
                    XElement("_PipeL4ReferencePathFSharpCore", XAttribute("Include", "@(ReferencePath)"),
                        XAttribute("Condition", "'%(ReferencePath.Filename)' == 'FSharp.Core'"))),
                XElement("WriteLinesToFile", XAttribute("File", referencePathFile),
                    XAttribute("Lines", "@(_PipeL4ReferencePathFSharpCore->'%(FullPath)')"),
                    XAttribute("Overwrite", "true")),
                XElement("WriteLinesToFile", XAttribute("File", fscArgumentsFile),
                    XAttribute("Lines", "@(FscCommandLineArgs)"),
                    XAttribute("Overwrite", "true"))))
        captureProject.Save referenceCapturePath

        let buildArguments =
            [ "build"
              Path.Combine(project, "Consumer.fsproj")
              "--configuration"
              "Release"
              "--no-restore"
              "--verbosity"
              "minimal"
              msbuildProperty "CustomAfterMicrosoftCommonTargets" referenceCapturePath ] @ warningPolicy
        let buildOutput, buildError = runProcess true project "dotnet" buildArguments
        if hasDowngradeDiagnostic (buildOutput + buildError) then
            fail "Independent consumer build reported NuGet downgrade warning NU1605."
        if not (File.Exists referencePathFile) || not (File.Exists fscArgumentsFile) then
            fail "The consumer build did not expose its FSharp.Core Fsc command-line reference."
        let referencePaths = File.ReadAllLines referencePathFile |> Array.filter (String.IsNullOrWhiteSpace >> not)
        if referencePaths.Length <> 1 then
            fail $"Expected one FSharp.Core item in ReferencePath, found {referencePaths.Length}."
        let referencePath = Path.GetFullPath referencePaths[0]
        let commandLineArguments = File.ReadAllLines fscArgumentsFile
        // dotnet build is launched with the consumer project as its working directory; Fsc inherits it.
        let compilerWorkingDirectory = Path.GetFullPath project
        printfn "Using Fsc compiler working directory '%s'." compilerWorkingDirectory
        let fsharpCoreReferences =
            fscReferenceArguments compilerWorkingDirectory commandLineArguments
            |> Array.filter (fun path -> Path.GetFileName(path).Equals("FSharp.Core.dll", StringComparison.OrdinalIgnoreCase))
        if fsharpCoreReferences.Length <> 1 then
            fail $"Expected one actual FSharp.Core compiler reference in Fsc command-line arguments, found {fsharpCoreReferences.Length}."
        let actualCompilerReference = fsharpCoreReferences[0]
        printfn "Captured ReferencePath FSharp.Core '%s'." referencePath
        printfn "Captured Fsc command-line FSharp.Core reference '%s'." actualCompilerReference
        if not (File.Exists actualCompilerReference) then
            fail $"Fsc's actual FSharp.Core compiler reference does not exist: '{actualCompilerReference}'."
        let relativeAsset =
            match compileAssets with
            | [ assetPath ] -> assetPath
            | _ -> fail $"Could not identify exactly one resolved net10.0 FSharp.Core compile asset for reference '{actualCompilerReference}'."
        let referenceHash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes actualCompilerReference))
        let stagedHash = packageEntryHash stagedFSharpPackage relativeAsset
        if not (String.Equals(referenceHash, stagedHash, StringComparison.Ordinal)) then
            fail $"Fsc's actual FSharp.Core compiler reference '{actualCompilerReference}' differs from the staged package asset '{relativeAsset}'."
        if isSdkDefaultConsumer then
            let expectedHash = packageEntryHash sdkFSharpPackage relativeAsset
            if not (String.Equals(referenceHash, expectedHash, StringComparison.Ordinal)) then
                fail $"Actual FSharp.Core compiler reference '{relativeAsset}' differs from the selected SDK library-pack."
            printfn "Verified actual Fsc reference '%s' asset '%s' SHA-256 %s against selected SDK library-pack." actualCompilerReference relativeAsset referenceHash
        elif isCanonicalMinimum then
            let expectedHash = expectedFSharpCore100AssetHash relativeAsset
            if not (String.Equals(referenceHash, expectedHash, StringComparison.Ordinal)) then
                fail $"Fsc's actual FSharp.Core compiler reference '{actualCompilerReference}' differs from the trusted NuGet.org 10.0.100 asset."
            printfn "Verified actual Fsc reference '%s' asset '%s' SHA-256 %s against the signed NuGet.org FSharp.Core 10.0.100 package." actualCompilerReference relativeAsset referenceHash
        else
            fail $"No independent FSharp.Core reference can verify Fsc's actual compiler input '{actualCompilerReference}'."
    finally
        if Directory.Exists root then
            Directory.Delete(root, true)

let private validatePackage packagePath useInvalidConsumerSource =
    let summary, fsharpRange = validatePackageFile packagePath
    let sdkVersion, sdkPath, sdkPolicy = sdkDetails ()
    printfn "Validator selected .NET SDK %s." sdkVersion
    validateIndependentConsumer summary.PackagePath summary.Version fsharpRange None useInvalidConsumerSource sdkVersion sdkPath sdkPolicy
    if not useInvalidConsumerSource then
        validateIndependentConsumer summary.PackagePath summary.Version fsharpRange (Some "10.0.100") false sdkVersion sdkPath sdkPolicy
    summary

let arguments = fsi.CommandLineArgs |> Array.skip 1

let exitStatus =
    try
        let packagePath, useInvalidConsumerSource =
            match arguments with
            | [| packagePath |] -> Some packagePath, false
            | [| packagePath; "--test-invalid-consumer" |] -> Some packagePath, true
            | _ -> None, false

        match packagePath with
        | None ->
            eprintfn "Usage: dotnet fsi scripts/validate_public_api_package.fsx <PipeL4.Plugins-package.nupkg> [--test-invalid-consumer]"
            2
        | Some packagePath ->
            match validatePackage packagePath useInvalidConsumerSource with
            | summary ->
                printfn
                    "Validated public API package: PackageId=%s, version=%s, target=%s, assembly=%s (%s), dependencies=%s; SDK-default and minimum-version F# consumers compiled."
                    summary.PackageId
                    summary.Version
                    summary.TargetFramework
                    summary.AssemblyName
                    summary.AssemblyVersion
                    (String.Join(", ", summary.Dependencies))
                0
    with
    | ValidationException message ->
        eprintfn "Public API package validation failed: %s" message
        1
    | error ->
        eprintfn "Unexpected public API package validation error: %s" error.Message
        2

Environment.Exit exitStatus
