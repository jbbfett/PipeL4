open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Text.Json
open System.Text.Json.Nodes
open System.Xml.Linq

exception private ValidationException of string

let private fail message = raise (ValidationException message)

type private SdkSelection =
    { AllowPrerelease: bool option
      Paths: string list option
      Version: string
      BasePath: string
      FsiPath: string }

type private SdkPolicy =
    { GlobalJsonPath: string option
      RequestedVersion: string option
      AllowPrerelease: bool option
      Paths: string list option }

let private runDotnetExecutable executable workingDirectory (environment: (string * string) list) (arguments: string list) =
    let startInfo = ProcessStartInfo(executable)
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true

    for name, value in environment do
        startInfo.Environment[name] <- value

    for argument in arguments do
        startInfo.ArgumentList.Add argument

    use child = new Process(StartInfo = startInfo)

    let command = String.concat " " arguments
    let started =
        try child.Start()
        with error ->
            fail $"Could not inspect effective NuGet helper host '{executable}' from '{workingDirectory}': {error.Message}"

    if not started then
        fail $"Could not start effective NuGet helper host '{executable}' with arguments '{command}' from '{workingDirectory}'."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()

    if not (child.WaitForExit(30000)) then
        child.Kill(true)
        fail $"Host '{executable}' with arguments '{command}' timed out in '{workingDirectory}'."

    let output = outputTask.GetAwaiter().GetResult()
    let error = errorTask.GetAwaiter().GetResult()

    if child.ExitCode <> 0 then
        fail $"Host '{executable}' with arguments '{command}' failed in '{workingDirectory}' (exit {child.ExitCode}).\n{output}{error}"

    output, error

let private runDotnet workingDirectory environment arguments =
    runDotnetExecutable "dotnet" workingDirectory environment arguments

let private runningFsiSdk () =
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
    let fsiPath = Path.Combine(fsharpDirectory, "fsi.dll")

    if not (File.Exists fsiPath) then
        fail $"The running FSI compiler assembly is not beside fsi.dll: '{compilerPath}'."

    sdkVersion, sdkPath, fsiPath

let private readSdkPolicy () =
    let jsonOptions = JsonDocumentOptions(CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true)
    let mutable current = DirectoryInfo(Path.GetFullPath Environment.CurrentDirectory)
    let mutable globalJson = None

    while globalJson.IsNone && not (isNull current) do
        let candidate = Path.Combine(current.FullName, "global.json")

        if File.Exists candidate then
            globalJson <- Some candidate
        else
            current <- current.Parent

    match globalJson with
    | None ->
        { GlobalJsonPath = None
          RequestedVersion = None
          AllowPrerelease = None
          Paths = None }
    | Some path ->
        use document =
            try JsonDocument.Parse(File.ReadAllText path, jsonOptions)
            with error -> fail $"Could not parse effective global.json '{path}': {error.Message}"

        let mutable sdk = Unchecked.defaultof<JsonElement>

        if not (document.RootElement.TryGetProperty("sdk", &sdk)) then
            { GlobalJsonPath = Some path
              RequestedVersion = None
              AllowPrerelease = None
              Paths = None }
        elif sdk.ValueKind <> JsonValueKind.Object then
            fail $"The sdk setting in effective global.json '{path}' must be an object."
        else
            let readString (name: string) =
                let mutable value = Unchecked.defaultof<JsonElement>

                if sdk.TryGetProperty(name, &value) then
                    if value.ValueKind <> JsonValueKind.String || String.IsNullOrWhiteSpace(value.GetString()) then
                        fail $"The {name} setting in '{path}' must be a non-empty string."

                    Some(value.GetString())
                else
                    None

            let allowPrerelease =
                let mutable value = Unchecked.defaultof<JsonElement>

                if sdk.TryGetProperty("allowPrerelease", &value) then
                    match value.ValueKind with
                    | JsonValueKind.True -> Some true
                    | JsonValueKind.False -> Some false
                    | _ -> fail $"The allowPrerelease setting in '{path}' must be true or false."
                else
                    None

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

                        if itemPath = "$host$" then
                            itemPath
                        elif Path.IsPathRooted itemPath then
                            Path.GetFullPath itemPath
                        else
                            Path.GetFullPath(Path.Combine(Path.GetDirectoryName path, itemPath)))
                    |> Seq.toList
                    |> Some
                else
                    None

            { GlobalJsonPath = Some path
              RequestedVersion = readString "version"
              AllowPrerelease = allowPrerelease
              Paths = paths }

let private resolveSdkSelection () =
    let policy = readSdkPolicy ()
    let selectedOutput, _ = runDotnet Environment.CurrentDirectory [] [ "--version" ]
    let selectedVersion = selectedOutput.Trim()

    if String.IsNullOrWhiteSpace selectedVersion then
        fail "Could not determine the SDK selected for the validation workspace."

    let info, _ = runDotnet Environment.CurrentDirectory [] [ "--info" ]
    let basePathMatch = System.Text.RegularExpressions.Regex.Match(info, "(?m)^\\s*Base Path:\\s*(.+?)\\s*$")

    if not basePathMatch.Success then
        fail $"Could not locate the selected .NET SDK {selectedVersion} from 'dotnet --info'."

    let basePath = Path.GetFullPath(basePathMatch.Groups[1].Value.Trim())

    if Path.GetFileName(Path.TrimEndingDirectorySeparator basePath) <> selectedVersion then
        fail $"'dotnet --info' reports SDK path '{basePath}', which does not match selected version {selectedVersion}."

    let fsiVersion, fsiBasePath, fsiPath = runningFsiSdk ()
    let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal

    if fsiVersion <> selectedVersion || not (String.Equals(Path.TrimEndingDirectorySeparator fsiBasePath, Path.TrimEndingDirectorySeparator basePath, comparison)) then
        fail $"Running FSI SDK {fsiVersion} at '{fsiBasePath}' does not match the SDK selected from '{Environment.CurrentDirectory}' ({selectedVersion} at '{basePath}')."

    let requestedVersion = policy.RequestedVersion |> Option.defaultValue "none"
    printfn
        "Parent SDK selection: requested %s from %s; dotnet selected %s at %s; running FSI is %s."
        requestedVersion
        (policy.GlobalJsonPath |> Option.defaultValue "no global.json")
        selectedVersion
        basePath
        fsiVersion

    { AllowPrerelease = policy.AllowPrerelease
      Paths = policy.Paths
      Version = selectedVersion
      BasePath = basePath
      FsiPath = fsiPath }

let private writeSdkPin directory (selection: SdkSelection) =
    Directory.CreateDirectory directory |> ignore
    let sdk = JsonObject()
    sdk["version"] <- JsonValue.Create selection.Version
    sdk["rollForward"] <- JsonValue.Create "disable"

    match selection.AllowPrerelease with
    | Some value -> sdk["allowPrerelease"] <- JsonValue.Create value
    | None -> ()

    match selection.Paths with
    | Some paths ->
        let pathValues = JsonArray()
        for path in paths do pathValues.Add(JsonValue.Create path)
        sdk["paths"] <- pathValues
    | None -> ()

    let root = JsonObject()
    root["sdk"] <- sdk
    File.WriteAllText(Path.Combine(directory, "global.json"), root.ToJsonString() + Environment.NewLine)

let private verifySdkSelection directory (environment: (string * string) list) (selection: SdkSelection) =
    let output, _ = runDotnet directory environment [ "--version" ]
    let selectedVersion = output.Trim()

    if selectedVersion <> selection.Version then
        fail $"Temporary workspace '{directory}' selected .NET SDK {selectedVersion}; expected exact SDK {selection.Version}."

    printfn "Temporary workspace selected exact .NET SDK %s: %s." selectedVersion directory

let private verifySdkPin label directory (environment: (string * string) list) (selection: SdkSelection) =
    let globalJson = Path.Combine(directory, "global.json")

    if not (File.Exists globalJson) then
        fail $"Production SDK pin is missing for {label}: '{globalJson}'."

    use document =
        try JsonDocument.Parse(File.ReadAllText globalJson, JsonDocumentOptions(CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true))
        with error -> fail $"Could not read production SDK pin '{globalJson}': {error.Message}"

    let sdk = document.RootElement.GetProperty("sdk")

    if sdk.GetProperty("version").GetString() <> selection.Version
       || sdk.GetProperty("rollForward").GetString() <> "disable" then
        fail $"Production SDK pin for {label} at '{globalJson}' must select exact SDK {selection.Version} with rollForward=disable."

    let mutable allowPrerelease = Unchecked.defaultof<JsonElement>

    match selection.AllowPrerelease with
    | Some expected when sdk.TryGetProperty("allowPrerelease", &allowPrerelease) ->
        let expectedKind = if expected then JsonValueKind.True else JsonValueKind.False
        if allowPrerelease.ValueKind <> expectedKind then
            fail $"Production SDK pin for {label} did not preserve allowPrerelease={expected}."
    | Some expected -> fail $"Production SDK pin for {label} omitted allowPrerelease={expected}."
    | None when sdk.TryGetProperty("allowPrerelease", &allowPrerelease) ->
        fail $"Production SDK pin for {label} added an unspecified allowPrerelease value."
    | None -> ()

    let mutable paths = Unchecked.defaultof<JsonElement>

    match selection.Paths with
    | Some expected when sdk.TryGetProperty("paths", &paths) && paths.ValueKind = JsonValueKind.Array ->
        let actual = paths.EnumerateArray() |> Seq.map (fun item -> item.GetString()) |> Seq.toList
        if actual <> expected then
            fail $"Production SDK pin for {label} changed inherited SDK paths."
    | Some _ -> fail $"Production SDK pin for {label} omitted inherited SDK paths."
    | None when sdk.TryGetProperty("paths", &paths) ->
        fail $"Production SDK pin for {label} added unspecified SDK paths."
    | None -> ()

    verifySdkSelection directory environment selection
    printfn "Verified production SDK pin (%s): %s, rollForward=disable." label selection.Version

let private effectiveGlobalJsonPath directory =
    let mutable current = DirectoryInfo(Path.GetFullPath directory)
    let mutable found = None

    while found.IsNone && not (isNull current) do
        let candidate = Path.Combine(current.FullName, "global.json")

        if File.Exists candidate then
            found <- Some candidate
        else
            current <- current.Parent

    found

let private sdkDetailsAt executable directory (environment: (string * string) list) =
    let versionOutput, _ = runDotnetExecutable executable directory environment [ "--version" ]
    let version = versionOutput.Trim()
    let info, _ = runDotnetExecutable executable directory environment [ "--info" ]
    let basePathMatch = System.Text.RegularExpressions.Regex.Match(info, "(?m)^\\s*Base Path:\\s*(.+?)\\s*$")

    if not basePathMatch.Success then
        fail $"Could not identify SDK Base Path using effective NuGet helper host '{executable}' from '{directory}'."

    let basePath = Path.GetFullPath(basePathMatch.Groups[1].Value.Trim())

    if Path.GetFileName(Path.TrimEndingDirectorySeparator basePath) <> version then
        fail $"Effective NuGet helper host '{executable}' reported SDK {version} from '{directory}' but Base Path '{basePath}'."

    version, basePath

let private effectiveEnvironmentValue (environment: (string * string) list) name =
    environment
    |> List.tryPick (fun (key, value) ->
        if String.Equals(key, name, StringComparison.OrdinalIgnoreCase) then Some value else None)
    |> Option.orElseWith (fun () -> Environment.GetEnvironmentVariable name |> Option.ofObj)

let private fallbackNugetHelperHostPath (environment: (string * string) list) =
    let dotnetName = if OperatingSystem.IsWindows() then "dotnet.exe" else "dotnet"
    let fileExists path =
        try File.Exists path
        with _ -> false

    let runtimeAssemblyPath = typeof<int>.Assembly.Location

    let runtimeHost =
        if String.IsNullOrWhiteSpace runtimeAssemblyPath then
            None
        else
            try
                let runtimeDirectory = Path.GetDirectoryName runtimeAssemblyPath
                let candidate = Path.GetFullPath(Path.Combine(runtimeDirectory, "..", "..", "..", dotnetName))
                if fileExists candidate then Some candidate else None
            with _ -> None

    let pathHost =
        effectiveEnvironmentValue environment "PATH"
        |> Option.map (fun path ->
            path.Split(Path.PathSeparator)
            |> Array.tryPick (fun directory ->
                let candidate = Path.Combine(directory, dotnetName)
                if fileExists candidate then Some candidate else None))
        |> Option.defaultValue None

    let globalHost =
        let programFiles =
            match effectiveEnvironmentValue environment "ProgramW6432" with
            | Some path when not (String.IsNullOrEmpty path) -> path
            | _ -> Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles)

        let candidate = Path.Combine(programFiles, "dotnet", dotnetName)
        if fileExists candidate then Some candidate else None

    runtimeHost
    |> Option.orElse pathHost
    |> Option.orElse globalHost
    |> Option.defaultWith (fun () ->
        fail
            $"Could not determine the effective FSI NuGet helper host: no '{dotnetName}' executable was found beside the runtime assembly '{runtimeAssemblyPath}', in PATH, or at the platform global-install location.")

let private nugetHelperHostPath environment =
    match effectiveEnvironmentValue environment "DOTNET_HOST_PATH" with
    | Some hostPath when not (String.IsNullOrWhiteSpace hostPath) -> hostPath
    | _ -> fallbackNugetHelperHostPath environment

let private verifySourceDirectorySdk sourcePath (environment: (string * string) list) (selection: SdkSelection) =
    let sourceDirectory = Path.GetDirectoryName(Path.GetFullPath sourcePath)
    let helperHost = nugetHelperHostPath environment
    let helperVersion, helperBasePath = sdkDetailsAt helperHost sourceDirectory environment
    let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal
    let expectedBasePath = Path.TrimEndingDirectorySeparator selection.BasePath
    let actualBasePath = Path.TrimEndingDirectorySeparator helperBasePath

    if helperVersion <> selection.Version || not (String.Equals(actualBasePath, expectedBasePath, comparison)) then
        let globalJson = effectiveGlobalJsonPath sourceDirectory |> Option.defaultValue "none"

        fail
            $"FSI dependency-manager NuGet source discovery would select SDK {helperVersion} at '{helperBasePath}' using host '{helperHost}' from source directory '{sourceDirectory}' (effective global.json: {globalJson}); validated FSI uses SDK {selection.Version} at '{selection.BasePath}'. Make the effective host and source-directory SDK policy select the validated SDK before retrying."

    printfn "FSI dependency-manager NuGet source discovery uses host '%s' from '%s' and selects the validated SDK %s at %s." helperHost sourceDirectory helperVersion helperBasePath

let private packagePathFromArgument argument =
    let fullPath = Path.GetFullPath argument

    if Directory.Exists fullPath then
        let packages =
            Directory.EnumerateFiles(fullPath, "PipeL4.Plugins.*.nupkg", SearchOption.TopDirectoryOnly)
            |> Seq.toArray

        match packages with
        | [| packagePath |] -> packagePath
        | _ -> fail $"Expected exactly one PipeL4.Plugins package in '{fullPath}', found {packages.Length}."
    elif File.Exists fullPath then
        fullPath
    else
        fail $"Public API package does not exist: {fullPath}"

let private packageMetadata packagePath =
    use package = ZipFile.OpenRead packagePath
    let nuspecs =
        package.Entries
        |> Seq.filter (fun entry -> entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
        |> Seq.toArray

    if nuspecs.Length <> 1 then
        fail $"Expected one nuspec in '{packagePath}', found {nuspecs.Length}."

    use stream = nuspecs[0].Open()
    let document = XDocument.Load stream

    let metadata =
        document.Descendants()
        |> Seq.tryFind (fun element -> element.Name.LocalName = "metadata")
        |> Option.defaultWith (fun () -> fail "The public API package nuspec has no metadata element.")

    let requiredText name =
        metadata.Elements()
        |> Seq.tryFind (fun element -> element.Name.LocalName = name)
        |> Option.map (fun element -> element.Value.Trim())
        |> Option.filter (String.IsNullOrWhiteSpace >> not)
        |> Option.defaultWith (fun () -> fail $"The public API package nuspec has no '{name}' value.")

    let packageId = requiredText "id"
    let version = requiredText "version"

    if not (packageId.Equals("PipeL4.Plugins", StringComparison.OrdinalIgnoreCase)) then
        fail $"Expected PackageId 'PipeL4.Plugins', found '{packageId}'."

    if package.GetEntry("lib/net10.0/PipeL4.Plugins.dll") |> Option.ofObj |> Option.isNone then
        fail "The package does not contain lib/net10.0/PipeL4.Plugins.dll."

    let fsharpCoreDependency =
        metadata.Descendants()
        |> Seq.tryFind (fun element ->
            element.Name.LocalName = "dependency"
            && (element.Attribute(XName.Get "id")
                |> Option.ofObj
                |> Option.exists (fun attribute ->
                    attribute.Value.Equals("FSharp.Core", StringComparison.OrdinalIgnoreCase))))
        |> Option.defaultWith (fun () -> fail "The public API package does not declare FSharp.Core.")

    let fsharpCoreRange =
        fsharpCoreDependency.Attribute(XName.Get "version")
        |> Option.ofObj
        |> Option.map (fun attribute -> attribute.Value.Trim().TrimStart('[', '(').TrimEnd(']', ')'))
        |> Option.defaultWith (fun () -> fail "The public API package has no FSharp.Core version range.")

    let fsharpCoreVersion =
        let bounds = fsharpCoreRange.Split([| ',' |], StringSplitOptions.None)
        bounds[0].Trim()

    if String.IsNullOrWhiteSpace fsharpCoreVersion then
        fail "The public API package has no usable FSharp.Core minimum version."

    packageId, version, fsharpCoreVersion

let private locateCachedPackage (packageId: string) (version: string) =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.ArgumentList.Add "nuget"
    startInfo.ArgumentList.Add "locals"
    startInfo.ArgumentList.Add "global-packages"
    startInfo.ArgumentList.Add "--list"

    use child = new Process(StartInfo = startInfo)

    if not (child.Start()) then
        fail "Could not locate NuGet's global package cache."

    let output = child.StandardOutput.ReadToEnd()
    let error = child.StandardError.ReadToEnd()
    child.WaitForExit()

    if child.ExitCode <> 0 then
        fail $"Could not locate NuGet's global package cache. {error}"

    let globalPackages =
        output.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.tryPick (fun line ->
            let separator = line.IndexOf ':'

            if separator >= 0 then
                Some(line.Substring(separator + 1).Trim())
            else
                None)
        |> Option.defaultWith (fun () -> fail "Could not parse NuGet's global package cache path.")

    let normalizedId = packageId.ToLowerInvariant()
    let packageDirectory = Path.Combine(globalPackages, normalizedId, version.ToLowerInvariant())
    let packagePath = Path.Combine(packageDirectory, $"{normalizedId}.{version.ToLowerInvariant()}.nupkg")

    if File.Exists packagePath then
        packagePath
    else
        fail $"Could not stage {packageId} {version} from NuGet's package cache at '{packagePath}'. Restore the repository with Paket first."

let private validateExampleVersion sourcePath version =
    let expectedReference = $"#r \"nuget: PipeL4.Plugins, {version}\""

    let references =
        File.ReadAllLines sourcePath
        |> Array.map (fun line -> line.Trim())
        |> Array.filter (fun line -> line.StartsWith("#r", StringComparison.Ordinal))

    if references <> [| expectedReference |] then
        fail
            $"The authoring example must contain exactly '{expectedReference}', matching the supplied local package version."

    let source = File.ReadAllText sourcePath

    if not (source.Contains("#if INTERACTIVE", StringComparison.Ordinal))
       || not (source.Contains("#endif", StringComparison.Ordinal)) then
        fail "The authoring example must guard its package reference with #if INTERACTIVE and #endif."

let private writeNuGetConfig (path: string) (localFeed: string) =
    let config =
        XDocument(
            XElement(
                "configuration",
                XElement(
                    "packageSources",
                    XElement("clear"),
                    XElement("add", XAttribute("key", "PipeL4 local candidate"), XAttribute("value", localFeed)))
            )
        )

    config.Save path

let private runFsi sourcePath workingDirectory localFeed packageCache httpCache (sdkSelection: SdkSelection) =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.ArgumentList.Add "fsi"
    startInfo.ArgumentList.Add "--exec"
    startInfo.ArgumentList.Add sourcePath
    let fsiHome = Path.Combine(workingDirectory, "fsi-home")
    let fsiTemp = Path.Combine(fsiHome, "temp")
    Directory.CreateDirectory fsiHome |> ignore
    Directory.CreateDirectory fsiTemp |> ignore

    if not (OperatingSystem.IsWindows()) then
        for path in [ fsiHome; fsiTemp ] do
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead ||| UnixFileMode.UserWrite ||| UnixFileMode.UserExecute)

    writeSdkPin fsiHome sdkSelection
    writeSdkPin fsiTemp sdkSelection

    let allowPrerelease =
        sdkSelection.AllowPrerelease
        |> Option.map (fun value -> if value then "true" else "false")
        |> Option.defaultValue "inherited default"
    printfn "Temporary SDK pin: version %s, rollForward=disable, allowPrerelease=%s." sdkSelection.Version allowPrerelease

    let sdkPaths = sdkSelection.Paths |> Option.map (String.concat ";") |> Option.defaultValue "inherited default"
    printfn "Temporary SDK paths: %s." sdkPaths

    startInfo.Environment["HOME"] <- fsiHome
    startInfo.Environment["USERPROFILE"] <- fsiHome
    startInfo.Environment["DOTNET_CLI_HOME"] <- fsiHome
    startInfo.Environment["XDG_RUNTIME_DIR"] <- fsiHome
    startInfo.Environment["NUGET_PACKAGES"] <- packageCache
    startInfo.Environment["NUGET_HTTP_CACHE_PATH"] <- httpCache
    startInfo.Environment["RestoreSources"] <- localFeed
    startInfo.Environment["NuGetAudit"] <- "false"
    startInfo.Environment["TMPDIR"] <- fsiTemp
    startInfo.Environment["TEMP"] <- fsiTemp
    startInfo.Environment["TMP"] <- fsiTemp
    startInfo.Environment.Remove "FSharpCoreImplicitPackageVersion" |> ignore
    startInfo.Environment.Remove "NoWarn" |> ignore

    let childEnvironment =
        [ "HOME", fsiHome
          "USERPROFILE", fsiHome
          "DOTNET_CLI_HOME", fsiHome
          "XDG_RUNTIME_DIR", fsiHome
          "NUGET_PACKAGES", packageCache
          "NUGET_HTTP_CACHE_PATH", httpCache
          "RestoreSources", localFeed
          "NuGetAudit", "false"
          "TMPDIR", fsiTemp
          "TEMP", fsiTemp
          "TMP", fsiTemp ]

    verifySdkPin "FSI working directory" workingDirectory childEnvironment sdkSelection
    verifySdkPin "FSI HOME / dependency-manager cache" fsiHome childEnvironment sdkSelection
    verifySdkPin "FSI temp / dependency-manager restore projects" fsiTemp childEnvironment sdkSelection
    verifySourceDirectorySdk sourcePath childEnvironment sdkSelection

    printfn "FSI child identity: SDK %s; %s." sdkSelection.Version sdkSelection.FsiPath

    use child = new Process(StartInfo = startInfo)

    if not (child.Start()) then
        fail "Could not start dotnet fsi."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()

    if not (child.WaitForExit(180000)) then
        child.Kill(true)
        fail "The local FSI authoring validation timed out."

    let output = outputTask.GetAwaiter().GetResult()
    let error = errorTask.GetAwaiter().GetResult()

    if child.ExitCode <> 0 then
        fail
            $"dotnet fsi exited {child.ExitCode} from '{workingDirectory}' using SDK {sdkSelection.Version} ({sdkSelection.FsiPath}).\nstdout:\n{output}\nstderr:\n{error}"

    if (output + error).Contains("NU1605", StringComparison.Ordinal) then
        fail "FSI reported an FSharp.Core downgrade while restoring the local API candidate."

    if not (String.IsNullOrEmpty output) then
        Console.Out.Write output

    if not (String.IsNullOrEmpty error) then
        Console.Error.Write error

    let managerCache = Path.Combine(fsiHome, ".packagemanagement", "nuget", "Cache")

    if Directory.Exists managerCache then
        let generatedScripts = Directory.GetFiles(managerCache, "*.fsx", SearchOption.AllDirectories)
        let resolvedReferences = Directory.GetFiles(managerCache, "*.resolvedReferences.paths", SearchOption.AllDirectories)
        let projectAssets = Directory.GetFiles(fsiHome, "project.assets.json", SearchOption.AllDirectories)

        let artifactDirectories =
            [ yield managerCache
              yield! generatedScripts |> Array.map Path.GetDirectoryName
              yield! resolvedReferences |> Array.map Path.GetDirectoryName
              yield! projectAssets |> Array.map Path.GetDirectoryName ]
            |> List.distinct

        for directory in artifactDirectories do
            verifySdkSelection directory childEnvironment sdkSelection

        printfn
            "FSI dependency-manager evidence: %d generated script(s), %d resolved-reference cache(s), %d retained project.assets.json file(s); HOME and temp SDK roots are pinned to %s."
            generatedScripts.Length
            resolvedReferences.Length
            projectAssets.Length
            sdkSelection.Version
    else
        printfn
            "FSI dependency-manager evidence: no persistent cache directory; HOME and temp SDK roots were pinned to %s."
            sdkSelection.Version

let private verifyRestoredPackage
    (packageCache: string)
    (localFeed: string)
    (packageId: string)
    (version: string)
    =
    let packageDirectory =
        Path.Combine(packageCache, packageId.ToLowerInvariant(), version.ToLowerInvariant())

    let metadataPath = Path.Combine(packageDirectory, ".nupkg.metadata")

    if not (File.Exists metadataPath) then
        fail $"FSI did not restore {packageId} {version} into the isolated package cache."

    use metadata = JsonDocument.Parse(File.ReadAllText metadataPath)

    let source =
        metadata.RootElement.GetProperty("source").GetString()
        |> Option.ofObj
        |> Option.defaultWith (fun () -> fail "NuGet package cache metadata did not record its source.")

    let mutable sourceUri = Unchecked.defaultof<Uri>

    let normalizedSource =
        if Uri.TryCreate(source, UriKind.Absolute, &sourceUri) && sourceUri.IsFile then
            sourceUri.LocalPath
        else
            source

    let pathComparison =
        if OperatingSystem.IsWindows() then
            StringComparison.OrdinalIgnoreCase
        else
            StringComparison.Ordinal

    if not (String.Equals(Path.GetFullPath normalizedSource, Path.GetFullPath localFeed, pathComparison)) then
        fail $"FSI restored {packageId} {version} from unexpected source '{source}'."

let private validate packageArgument sourceArgument =
    let sdkSelection = resolveSdkSelection ()
    let packagePath = packagePathFromArgument packageArgument
    let sourcePath = Path.GetFullPath sourceArgument

    if not (File.Exists sourcePath) then
        fail $"Authoring example does not exist: {sourcePath}"

    let packageId, version, fsharpCoreVersion = packageMetadata packagePath
    validateExampleVersion sourcePath version

    let root = Path.Combine(Path.GetTempPath(), $"PipeL4 FSI authoring validation {Guid.NewGuid():N}")
    let feed = Path.Combine(root, "feed")
    let packageCache = Path.Combine(root, "packages")
    let httpCache = Path.Combine(root, "http-cache")

    try
        Directory.CreateDirectory feed |> ignore
        File.Copy(packagePath, Path.Combine(feed, Path.GetFileName packagePath))
        let fsharpCorePackage = locateCachedPackage "FSharp.Core" fsharpCoreVersion
        File.Copy(fsharpCorePackage, Path.Combine(feed, Path.GetFileName fsharpCorePackage))
        writeNuGetConfig (Path.Combine(root, "NuGet.Config")) feed

        writeSdkPin root sdkSelection
        runFsi sourcePath root feed packageCache httpCache sdkSelection
        verifyRestoredPackage packageCache feed packageId version
        printfn "Validated FSI authoring: %s %s restored only from the local candidate feed." packageId version
    finally
        if Directory.Exists root then
            Directory.Delete(root, true)

let arguments = fsi.CommandLineArgs |> Array.skip 1

let exitStatus =
    try
        match arguments with
        | [| packagePath; sourcePath |] ->
            validate packagePath sourcePath
            0
        | _ ->
            eprintfn "Usage: dotnet fsi scripts/validate_fsi_authoring.fsx <PipeL4.Plugins-package-or-directory> <processor.fsx>"
            2
    with
    | ValidationException message ->
        eprintfn "FSI authoring validation failed: %s" message
        1
    | error ->
        eprintfn "Unexpected FSI authoring validation error: %s" error.Message
        2

Environment.Exit exitStatus
