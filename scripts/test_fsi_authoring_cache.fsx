open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Text.Json.Nodes
open System.Xml.Linq

type private ProcessResult =
    { ExitCode: int
      TimedOut: bool
      StandardOutput: string
      StandardError: string }

exception private RegressionException of string

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

let private fail message = raise (RegressionException message)

let private createPrivateDirectory path =
    Directory.CreateDirectory path |> ignore

    if not (OperatingSystem.IsWindows()) then
        File.SetUnixFileMode(
            path,
            UnixFileMode.UserRead ||| UnixFileMode.UserWrite ||| UnixFileMode.UserExecute)

let private currentDotnetHostPath () =
    match Option.ofObj Environment.ProcessPath with
    | Some path when File.Exists path -> Path.GetFullPath path
    | _ ->
        use current = Process.GetCurrentProcess()

        match Option.ofObj current.MainModule with
        | Some moduleInfo when File.Exists moduleInfo.FileName -> Path.GetFullPath moduleInfo.FileName
        | _ -> fail "Could not identify the current dotnet host executable."

let private runProcessRemoving executable workingDirectory (environment: (string * string) list) (removedEnvironment: string list) (arguments: string list) =
    let startInfo = ProcessStartInfo(executable)
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true

    for name in removedEnvironment do
        startInfo.Environment.Remove name |> ignore

    for name, value in environment do
        startInfo.Environment[name] <- value

    for argument in arguments do
        startInfo.ArgumentList.Add argument

    use child = new Process(StartInfo = startInfo)

    if not (child.Start()) then
        fail $"Could not start '{executable}' for the FSI authoring cache regression."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()

    let timedOut = not (child.WaitForExit(180000))

    if timedOut then
        child.Kill(true)
        child.WaitForExit()

    { ExitCode = child.ExitCode
      TimedOut = timedOut
      StandardOutput = outputTask.GetAwaiter().GetResult()
      StandardError = errorTask.GetAwaiter().GetResult() }

let private runProcess executable workingDirectory environment arguments =
    runProcessRemoving executable workingDirectory environment [] arguments

let private runProcessWithoutDotnetHost executable workingDirectory environment arguments =
    runProcessRemoving executable workingDirectory environment [ "DOTNET_HOST_PATH" ] arguments

let private runDotnet workingDirectory environment arguments =
    runProcessWithoutDotnetHost "dotnet" workingDirectory environment arguments

let private runDotnetWithoutDotnetHost workingDirectory environment arguments =
    runDotnet workingDirectory environment arguments

let private processChildren processId =
    let childrenPath = Path.Combine("/proc", string processId, "task", string processId, "children")

    if File.Exists childrenPath then
        (File.ReadAllText childrenPath).Split([| ' '; '\t'; '\r'; '\n' |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.choose (fun value -> match Int32.TryParse value with true, childId -> Some childId | _ -> None)
    else
        [||]

let private findNugetHelperProcess parentProcessId =
    let pending = Stack<int>()
    let visited = HashSet<int>()
    pending.Push parentProcessId
    let mutable result = None

    while pending.Count > 0 && result.IsNone do
        let processId = pending.Pop()

        if visited.Add processId then
            for childId in processChildren processId do
                pending.Push childId

                let processDirectory = Path.Combine("/proc", string childId)
                let commandPath = Path.Combine(processDirectory, "cmdline")

                if File.Exists commandPath then
                    try
                        let arguments =
                            File.ReadAllBytes commandPath
                            |> Text.Encoding.UTF8.GetString
                            |> fun value -> value.Split([| '\000' |], StringSplitOptions.RemoveEmptyEntries)

                        let command = String.Join(" ", arguments)
                        let lowerCommand = command.ToLowerInvariant()

                        if lowerCommand.Contains("nuget", StringComparison.Ordinal)
                           && lowerCommand.Contains("list", StringComparison.Ordinal)
                           && lowerCommand.Contains("source", StringComparison.Ordinal) then
                            let executablePath = Path.Combine(processDirectory, "exe")
                            let workingDirectoryPath = Path.Combine(processDirectory, "cwd")
                            let executable = File.ResolveLinkTarget(executablePath, true)
                            let workingDirectory = DirectoryInfo(workingDirectoryPath).ResolveLinkTarget true

                            if not (isNull executable) && not (isNull workingDirectory) then
                                result <- Some(executable.FullName, workingDirectory.FullName, command)
                    with _ -> ()

    result

let private runProcessTracingNugetHelper executable workingDirectory environment arguments =
    if not (OperatingSystem.IsLinux()) then
        fail "The independent NuGet helper process trace currently requires Linux /proc support."

    let startInfo = ProcessStartInfo(executable)
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.Environment.Remove "DOTNET_HOST_PATH" |> ignore

    for name, value in environment do
        startInfo.Environment[name] <- value

    for argument in arguments do
        startInfo.ArgumentList.Add argument

    use child = new Process(StartInfo = startInfo)

    if not (child.Start()) then
        fail $"Could not start '{executable}' for the fallback NuGet helper process trace."

    let outputTask = child.StandardOutput.ReadToEndAsync()
    let errorTask = child.StandardError.ReadToEndAsync()
    let timer = Stopwatch.StartNew()
    let mutable helperTrace = None

    while not child.HasExited && timer.Elapsed < TimeSpan.FromMinutes 3.0 do
        if helperTrace.IsNone then
            helperTrace <- findNugetHelperProcess child.Id

        if helperTrace.IsNone then
            Threading.Thread.Sleep 1

    let timedOut = not child.HasExited

    if timedOut then
        child.Kill(true)
        child.WaitForExit()

    if helperTrace.IsNone then
        helperTrace <- findNugetHelperProcess child.Id

    let result =
        { ExitCode = child.ExitCode
          TimedOut = timedOut
          StandardOutput = outputTask.GetAwaiter().GetResult()
          StandardError = errorTask.GetAwaiter().GetResult() }

    result, helperTrace

let private selectedSdkDetails executable workingDirectory =
    let versionResult = runProcessWithoutDotnetHost executable workingDirectory [] [ "--version" ]
    let infoResult = runProcessWithoutDotnetHost executable workingDirectory [] [ "--info" ]

    if versionResult.TimedOut
       || versionResult.ExitCode <> 0
       || infoResult.TimedOut
       || infoResult.ExitCode <> 0 then
        fail $"Could not inspect SDK selection for host '{executable}' from '{workingDirectory}'. {versionResult.StandardOutput}{versionResult.StandardError}{infoResult.StandardOutput}{infoResult.StandardError}"

    let version = versionResult.StandardOutput.Trim()
    let info = infoResult.StandardOutput + infoResult.StandardError
    let basePathMatch = System.Text.RegularExpressions.Regex.Match(info, "(?m)^\\s*Base Path:\\s*(.+?)\\s*$")

    if not basePathMatch.Success then
        fail $"Could not read SDK Base Path for host '{executable}' from '{workingDirectory}'."

    version, Path.GetFullPath(basePathMatch.Groups[1].Value.Trim())

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

let private resolveSdkSelection () =
    let policy = readSdkPolicy ()
    let selected = runDotnet Environment.CurrentDirectory [] [ "--version" ]

    if selected.TimedOut || selected.ExitCode <> 0 then
        let requested = policy.RequestedVersion |> Option.defaultValue "none"
        fail $"Could not determine the SDK selected for the parent validator. Requested version: {requested}. {selected.StandardOutput}{selected.StandardError}"

    let version = selected.StandardOutput.Trim()
    let info = runDotnet Environment.CurrentDirectory [] [ "--info" ]

    if info.TimedOut || info.ExitCode <> 0 then
        fail $"Could not determine the selected SDK base path. {info.StandardOutput}{info.StandardError}"

    let basePathMatch = System.Text.RegularExpressions.Regex.Match(info.StandardOutput + info.StandardError, "(?m)^\\s*Base Path:\\s*(.+?)\\s*$")

    if not basePathMatch.Success then
        fail "Could not parse the selected SDK base path from 'dotnet --info'."

    let basePath = Path.GetFullPath(basePathMatch.Groups[1].Value.Trim())

    if Path.GetFileName(Path.TrimEndingDirectorySeparator basePath) <> version then
        fail $"The selected SDK path '{basePath}' does not match 'dotnet --version' ({version})."

    let fsiVersion, fsiBasePath, fsiPath = runningFsiSdk ()
    let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal

    if fsiVersion <> version || not (String.Equals(Path.TrimEndingDirectorySeparator fsiBasePath, Path.TrimEndingDirectorySeparator basePath, comparison)) then
        fail $"Running FSI SDK {fsiVersion} at '{fsiBasePath}' does not match dotnet's selected SDK {version} at '{basePath}'."

    let requested = policy.RequestedVersion |> Option.defaultValue "none"
    printfn
        "Parent SDK selection: global.json requested %s; dotnet selected %s at %s; running FSI is %s."
        requested
        version
        basePath
        fsiVersion

    match policy.GlobalJsonPath with
    | Some path -> printfn "Effective global.json: %s." path
    | None -> printfn "Effective global.json: none."

    { AllowPrerelease = policy.AllowPrerelease
      Paths = policy.Paths
      Version = version
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

let private verifySdkSelection directory environment (selection: SdkSelection) =
    let result = runDotnet directory environment [ "--version" ]

    if result.TimedOut || result.ExitCode <> 0 then
        fail $"Could not select the intended .NET SDK in temporary workspace '{directory}'. Requested exact SDK {selection.Version}. {result.StandardOutput}{result.StandardError}"

    let selectedVersion = result.StandardOutput.Trim()

    if selectedVersion <> selection.Version then
        fail $"Temporary workspace '{directory}' selected .NET SDK {selectedVersion}; expected exact SDK {selection.Version}."

    printfn "Temporary workspace selected exact .NET SDK %s: %s." selectedVersion directory

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

let private packageIdentity packagePath =
    use package = ZipFile.OpenRead packagePath

    let nuspecs =
        package.Entries
        |> Seq.filter (fun entry -> entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
        |> Seq.toArray

    if nuspecs.Length <> 1 then
        fail $"Expected one nuspec in '{packagePath}', found {nuspecs.Length}."

    use stream = nuspecs[0].Open()
    let document = XDocument.Load stream
    let metadata = document.Descendants() |> Seq.tryFind (fun element -> element.Name.LocalName = "metadata")
    let metadata = metadata |> Option.defaultWith (fun () -> fail $"Package '{packagePath}' has no metadata element.")

    let read name =
        metadata.Elements()
        |> Seq.tryFind (fun element -> element.Name.LocalName = name)
        |> Option.map (fun element -> element.Value.Trim())
        |> Option.defaultWith (fun () -> fail $"Package '{packagePath}' has no '{name}' value.")

    read "id", read "version"

let private nearestGlobalJson directory =
    let mutable current = DirectoryInfo(Path.GetFullPath directory)
    let mutable found = None

    while found.IsNone && not (isNull current) do
        let candidate = Path.Combine(current.FullName, "global.json")

        if File.Exists candidate then
            found <- Some candidate
        else
            current <- current.Parent

    found

let private verifySdkPin directory (selection: SdkSelection) =
    let path = Path.Combine(directory, "global.json")

    if not (File.Exists path) then
        fail $"Temporary SDK pin is missing: {path}"

    use document = JsonDocument.Parse(File.ReadAllText path)
    let sdk = document.RootElement.GetProperty("sdk")

    if sdk.GetProperty("version").GetString() <> selection.Version
       || sdk.GetProperty("rollForward").GetString() <> "disable" then
        fail $"Temporary global.json at '{path}' does not pin exact SDK {selection.Version}."

    let mutable allowPrerelease = Unchecked.defaultof<JsonElement>

    match selection.AllowPrerelease with
    | Some expected when sdk.TryGetProperty("allowPrerelease", &allowPrerelease) ->
        if allowPrerelease.ValueKind <> (if expected then JsonValueKind.True else JsonValueKind.False) then
            fail $"Temporary global.json at '{path}' did not preserve allowPrerelease={expected}."
    | Some expected -> fail $"Temporary global.json at '{path}' omitted allowPrerelease={expected}."
    | None when sdk.TryGetProperty("allowPrerelease", &allowPrerelease) ->
        fail $"Temporary global.json at '{path}' added an unspecified allowPrerelease setting."
    | None -> ()

    let mutable sdkPaths = Unchecked.defaultof<JsonElement>

    match selection.Paths with
    | Some expected when sdk.TryGetProperty("paths", &sdkPaths) && sdkPaths.ValueKind = JsonValueKind.Array ->
        let actual = sdkPaths.EnumerateArray() |> Seq.map (fun item -> item.GetString()) |> Seq.toList

        if actual <> expected then
            fail $"Temporary global.json at '{path}' changed inherited SDK paths."
    | Some _ -> fail $"Temporary global.json at '{path}' omitted inherited SDK paths."
    | None when sdk.TryGetProperty("paths", &sdkPaths) ->
        fail $"Temporary global.json at '{path}' added unspecified SDK paths."
    | None -> ()

    verifySdkSelection directory [] selection

let private enumerateInstalledSdks () =
    let result =
        runProcessWithoutDotnetHost
            (currentDotnetHostPath ())
            Environment.CurrentDirectory
            []
            [ "--list-sdks" ]

    if result.TimedOut || result.ExitCode <> 0 then
        fail $"Could not enumerate installed SDKs. {result.StandardOutput}{result.StandardError}"

    let sdks =
        result.StandardOutput.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.choose (fun line ->
            let matched = System.Text.RegularExpressions.Regex.Match(line.Trim(), "^(\\S+)\\s+\\[(.+)\\]$")
            if matched.Success then Some(matched.Groups[1].Value, matched.Groups[2].Value) else None)
        |> Array.toList

    if List.isEmpty sdks then
        fail $"Could not parse installed SDKs from 'dotnet --list-sdks'. Output: {result.StandardOutput}"

    sdks

let private selectAlternateSdk (selection: SdkSelection) installed =
    installed
    |> List.tryFind (fun (version, _) -> not (String.Equals(version, selection.Version, StringComparison.Ordinal)))

let private findGlobalPackagesPath () =
    let result = runDotnet Environment.CurrentDirectory [] [ "nuget"; "locals"; "global-packages"; "--list" ]

    if result.TimedOut || result.ExitCode <> 0 then
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

    if result.TimedOut || result.ExitCode <> 0 || not (diagnostics.Contains(expectedText, StringComparison.Ordinal)) then
        fail $"{label} failed (exit {result.ExitCode}, timed out: {result.TimedOut}).{Environment.NewLine}{diagnostics}"

    printfn "PASS %s" label

let private expectFailure (label: string) (expectedText: string) (result: ProcessResult) =
    let diagnostics = result.StandardOutput + Environment.NewLine + result.StandardError

    if result.TimedOut || result.ExitCode = 0 || not (diagnostics.Contains(expectedText, StringComparison.Ordinal)) then
        fail $"{label} did not fail as expected (exit {result.ExitCode}, timed out: {result.TimedOut}).{Environment.NewLine}{diagnostics}"

    printfn "PASS %s (exit %d)" label result.ExitCode

let private expectProductionSdkPins label (result: ProcessResult) =
    let output = result.StandardOutput + result.StandardError

    for pinLabel in
        [ "FSI working directory"
          "FSI HOME / dependency-manager cache"
          "FSI temp / dependency-manager restore projects" ] do
        let evidence = $"Verified production SDK pin ({pinLabel}):"

        if not (output.Contains(evidence, StringComparison.Ordinal)) then
            fail $"{label} did not report production SDK pin verification for {pinLabel}.\n{output}"

let private directFailureDiagnostics
    seedWorkingDirectory
    seededHome
    seedFeed
    seedPackages
    seedHttpCache
    candidatePath
    sourcePath
    (sdkSelection: SdkSelection)
    (environment: (string * string) list)
    (result: ProcessResult)
    =
    let summary = StringBuilder()
    let add (name: string) (value: string) = summary.AppendLine($"{name}={value}") |> ignore
    let managerCache = Path.Combine(seededHome, ".packagemanagement", "nuget", "Cache")
    let packageId, packageVersion = packageIdentity candidatePath

    add "diagnostic" "direct-fsi-cache-seed-failure"
    add "child.sdk.version" sdkSelection.Version
    add "child.sdk.base-path" sdkSelection.BasePath
    add "child.fsi.path" sdkSelection.FsiPath
    add "child.working-directory" seedWorkingDirectory
    add "child.exit-code" (string result.ExitCode)
    add "child.timed-out" (string result.TimedOut)
    add "source-script" sourcePath
    add "candidate.package" $"{packageId} {packageVersion}"
    add "candidate.sha256" (Convert.ToHexString(SHA256.HashData(File.ReadAllBytes candidatePath)))
    add "isolated.local-feed" seedFeed
    add "isolated.package-cache" seedPackages
    add "isolated.http-cache" seedHttpCache

    for name, value in environment do
        add $"environment.{name}" value

    let nugetConfig = Path.Combine(seedWorkingDirectory, "NuGet.Config")

    if File.Exists nugetConfig then
        let sources =
            XDocument.Load(nugetConfig).Descendants()
            |> Seq.filter (fun element -> element.Name.LocalName = "add")
            |> Seq.choose (fun element ->
                let key = element.Attribute(XName.Get "key") |> Option.ofObj |> Option.map (fun value -> value.Value)
                let value = element.Attribute(XName.Get "value") |> Option.ofObj |> Option.map (fun value -> value.Value)
                match key, value with
                | Some key, Some value -> Some $"{key}={value}"
                | _ -> None)
            |> String.concat ";"

        add "nuget.config" nugetConfig
        add "nuget.sources" sources

    let cachedPackageMetadata = Path.Combine(seedPackages, packageId.ToLowerInvariant(), packageVersion.ToLowerInvariant(), ".nupkg.metadata")

    if File.Exists cachedPackageMetadata then
        try
            use metadata = JsonDocument.Parse(File.ReadAllText cachedPackageMetadata)
            let source = metadata.RootElement.GetProperty("source").GetString()
            add "restored.package.identity" $"{packageId} {packageVersion}"
            add "restored.package.source" (Option.ofObj source |> Option.defaultValue "<empty>")
        with error ->
            add "restored.package.metadata-error" error.Message
    else
        add "restored.package.metadata" "not present in isolated package cache"

    let assetsFiles =
        if Directory.Exists seedWorkingDirectory then
            Directory.GetFiles(Path.GetDirectoryName seedWorkingDirectory, "project.assets.json", SearchOption.AllDirectories)
        else
            [||]

    if assetsFiles.Length = 0 then
        add "project.assets.json" "not retained by the FSI dependency manager"
    else
        for assetsPath in assetsFiles do
            try
                use assets = JsonDocument.Parse(File.ReadAllText assetsPath)
                let project = assets.RootElement.GetProperty("project")
                let restore = project.GetProperty("restore")
                let sources =
                    let mutable configuredSources = Unchecked.defaultof<JsonElement>
                    if restore.TryGetProperty("sources", &configuredSources) && configuredSources.ValueKind = JsonValueKind.Object then
                        configuredSources.EnumerateObject() |> Seq.map (fun source -> source.Name) |> String.concat ","
                    else
                        "<not recorded>"
                let packagesPath =
                    let mutable configuredPackages = Unchecked.defaultof<JsonElement>
                    if restore.TryGetProperty("packagesPath", &configuredPackages) then configuredPackages.GetString() else "<not recorded>"
                let pipeLibrary =
                    assets.RootElement.GetProperty("libraries").EnumerateObject()
                    |> Seq.tryFind (fun library -> library.Name.StartsWith("PipeL4.Plugins/", StringComparison.OrdinalIgnoreCase))
                    |> Option.map (fun library -> library.Name)
                    |> Option.defaultValue "<not resolved>"
                add "project.assets.json.path" assetsPath
                add "project.assets.json.sources" sources
                add "project.assets.json.packages-path" packagesPath
                add "project.assets.json.PipeL4.Plugins" pipeLibrary
            with error ->
                add "project.assets.json.read-error" $"{assetsPath}: {error.Message}"

    let resolvedFiles =
        if Directory.Exists managerCache then
            Directory.GetFiles(managerCache, "*.resolvedReferences.paths", SearchOption.AllDirectories)
        else
            [||]

    if resolvedFiles.Length = 0 then
        add "dependency-manager.restore-evidence" "no .resolvedReferences.paths file was retained"
        add "dependency-manager.restore-result" "not independently exposed by FSI; no resolved-reference cache was retained"
    else
        for resolvedPath in resolvedFiles do
            let lines = File.ReadAllLines resolvedPath
            let packageLines = lines |> Array.filter (fun line -> line.Contains("PipeL4.Plugins", StringComparison.OrdinalIgnoreCase))
            add "dependency-manager.resolvedReferences.paths" resolvedPath
            add "dependency-manager.restore-evidence" (if packageLines.Length > 0 then "PipeL4.Plugins reference was resolved" else "cache exists without a PipeL4.Plugins entry")
            add "dependency-manager.restore-result" (if packageLines.Length > 0 then "success evidenced by the cached PipeL4.Plugins assembly path" else "no PipeL4.Plugins resolution was recorded")
            for line in packageLines do
                add "observed.resolved-reference" line

    let generatedScripts =
        if Directory.Exists managerCache then
            Directory.GetFiles(managerCache, "*.fsx", SearchOption.AllDirectories)
        else
            [||]

    if generatedScripts.Length = 0 then
        add "dependency-manager.generated-fsharp-script" "not retained"
    else
        for generatedScript in generatedScripts do
            add "dependency-manager.generated-fsharp-script" generatedScript

            for line in File.ReadAllLines generatedScript do
                let trimmed = line.Trim()

                if trimmed.StartsWith("#r", StringComparison.Ordinal)
                   || trimmed.Contains("MSBuildSDKsPath:", StringComparison.Ordinal)
                   || trimmed.Contains("MSBuildExtensionsPath:", StringComparison.Ordinal) then
                    add "dependency-manager.generated-reference" trimmed

    add "assembly-load-evidence" "not collected; diagnostics do not execute an FSI assembly probe"

    add "stdout" result.StandardOutput
    add "stderr" result.StandardError
    summary.ToString()

let private printDirectFailureDiagnostics (summary: string) =
    Console.Error.WriteLine("--- direct FSI failure diagnostics ---")
    Console.Error.Write summary
    Console.Error.WriteLine("--- end direct FSI failure diagnostics ---")

let private collectDirectFailureDiagnostics
    ((seedWorkingDirectory, seededHome, seedFeed, seedPackages, seedHttpCache, candidatePath, sourcePath, sdkSelection, environment, result))
    =
    try
        directFailureDiagnostics
            seedWorkingDirectory
            seededHome
            seedFeed
            seedPackages
            seedHttpCache
            candidatePath
            sourcePath
            sdkSelection
            environment
            result
    with error ->
        String.Join(
            Environment.NewLine,
            [| "diagnostic=direct-fsi-cache-seed-failure"
               $"child.sdk.version={sdkSelection.Version}"
               $"child.fsi.path={sdkSelection.FsiPath}"
               $"child.working-directory={seedWorkingDirectory}"
               $"source-script={sourcePath}"
               $"child.exit-code={result.ExitCode}"
               $"child.timed-out={result.TimedOut}"
               $"diagnostic-collection.error={error.Message}"
               "stdout=" + result.StandardOutput
               "stderr=" + result.StandardError |])

let private cacheFileInventory roots =
    roots
    |> List.collect (fun root ->
        let fullRoot = Path.GetFullPath root

        if not (Directory.Exists fullRoot) then
            [ $"{fullRoot}\t<absent>" ]
        else
            Directory.GetFiles(fullRoot, "*", SearchOption.AllDirectories)
            |> Array.map (fun path ->
                let info = FileInfo path
                let relativePath = Path.GetRelativePath(fullRoot, path)
                let hash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes path))
                $"{fullRoot}\t{relativePath}\t{info.Length}\t{info.LastWriteTimeUtc.Ticks}\t{hash}")
            |> Array.toList)
    |> List.sort

let private writeSdkSwitchingTraceWrapper wrapperPath =
    let wrapper =
        """#!/bin/sh
if [ "$1" = "--version" ] || [ "$1" = "--info" ]; then
  exec "$PIPEL4_TRACE_ALT_DOTNET" "$@"
fi
if [ "$1" = "nuget" ] && [ "$2" = "list" ] && [ "$3" = "source" ]; then
  selected_sdk=$("$PIPEL4_TRACE_ALT_DOTNET" --version 2>&1)
  selected_base=$("$PIPEL4_TRACE_ALT_DOTNET" --info 2>&1 | awk '/^[[:space:]]*Base Path:/ { sub(/^[[:space:]]*Base Path:[[:space:]]*/, ""); print; exit }')
  printf '%s\t%s\t%s\t%s\n' "$PWD" "$selected_sdk" "$selected_base" "$*" >> "$PIPEL4_TRACE_FILE"
  exec "$PIPEL4_TRACE_ALT_DOTNET" "$@"
fi
exec "$PIPEL4_TRACE_VALIDATED_DOTNET" "$@"
"""

    File.WriteAllText(wrapperPath, wrapper)
    File.SetUnixFileMode(
        wrapperPath,
        UnixFileMode.UserRead ||| UnixFileMode.UserWrite ||| UnixFileMode.UserExecute)

let private writeSameVersionDifferentBasePathWrapper wrapperPath =
    let wrapper =
        """#!/bin/sh
if [ "$1" = "--info" ]; then
  "$PIPEL4_SAME_VERSION_HOST" "$@" | sed "s#^[[:space:]]*Base Path:.*# Base Path: $PIPEL4_DIFFERENT_BASE_PATH/#"
  exit 0
fi
exec "$PIPEL4_SAME_VERSION_HOST" "$@"
"""

    File.WriteAllText(wrapperPath, wrapper)
    File.SetUnixFileMode(
        wrapperPath,
        UnixFileMode.UserRead ||| UnixFileMode.UserWrite ||| UnixFileMode.UserExecute)

let private createIsolatedSdkHost root (installedSdk: string * string) =
    let version, sdkRoot = installedSdk
    let installRoot = Directory.GetParent(Path.GetFullPath sdkRoot).FullName
    let hostName = if OperatingSystem.IsWindows() then "dotnet.exe" else "dotnet"
    let sourceHost = Path.Combine(installRoot, hostName)
    let isolatedHostRoot = Path.Combine(root, $"sdk-host-{version}")
    let sdkDirectory = Path.Combine(isolatedHostRoot, "sdk")
    let hostDirectory = Path.Combine(isolatedHostRoot, "host")

    if not (File.Exists sourceHost) then
        fail $"Could not locate the installed dotnet host for SDK {version} at '{sourceHost}'."

    Directory.CreateDirectory sdkDirectory |> ignore
    Directory.CreateDirectory hostDirectory |> ignore
    let isolatedHost = Path.Combine(isolatedHostRoot, hostName)
    File.Copy(sourceHost, isolatedHost, true)

    if not (OperatingSystem.IsWindows()) then
        File.SetUnixFileMode(isolatedHost, File.GetUnixFileMode sourceHost)

    Directory.CreateSymbolicLink(Path.Combine(sdkDirectory, version), Path.Combine(Path.GetFullPath sdkRoot, version)) |> ignore
    Directory.CreateSymbolicLink(Path.Combine(hostDirectory, "fxr"), Path.Combine(installRoot, "host", "fxr")) |> ignore
    Directory.CreateSymbolicLink(Path.Combine(isolatedHostRoot, "shared"), Path.Combine(installRoot, "shared")) |> ignore

    for optionalDirectory in [ "packs"; "templates" ] do
        let source = Path.Combine(installRoot, optionalDirectory)
        if Directory.Exists source then
            Directory.CreateSymbolicLink(Path.Combine(isolatedHostRoot, optionalDirectory), source) |> ignore

    isolatedHost

let rec private copyDirectory sourcePath destinationPath =
    Directory.CreateDirectory destinationPath |> ignore

    for sourceFile in Directory.EnumerateFiles sourcePath do
        File.Copy(sourceFile, Path.Combine(destinationPath, Path.GetFileName sourceFile))

    for sourceDirectory in Directory.EnumerateDirectories sourcePath do
        copyDirectory sourceDirectory (Path.Combine(destinationPath, Path.GetFileName sourceDirectory))

let private deleteDirectoryLink path =
    let link = DirectoryInfo path

    if isNull link.LinkTarget then
        fail $"Expected '{path}' to be a directory link in the isolated SDK fixture."

    link.Delete()

let private createFallbackSdkHostPair root validatedSdk alternativeSdk =
    if OperatingSystem.IsWindows() then
        fail "The isolated runtime-root fallback-host regression currently requires Unix directory links."

    let validatedHost = createIsolatedSdkHost root validatedSdk
    let fallbackHost = createIsolatedSdkHost root alternativeSdk
    let validatedRoot = Path.GetDirectoryName validatedHost
    let fallbackRoot = Path.GetDirectoryName fallbackHost
    let installRoot =
        typeof<int>.Assembly.Location
        |> Path.GetDirectoryName
        |> fun runtimeDirectory -> Path.GetFullPath(Path.Combine(runtimeDirectory, "..", "..", ".."))
    let sourceShared = Path.Combine(installRoot, "shared")
    let fallbackShared = Path.Combine(fallbackRoot, "shared")

    deleteDirectoryLink fallbackShared
    Directory.CreateDirectory fallbackShared |> ignore

    let runtimeAssemblyDirectory = Path.GetDirectoryName typeof<int>.Assembly.Location
    let currentRuntimeVersion = Path.GetFileName(Path.TrimEndingDirectorySeparator runtimeAssemblyDirectory)

    for frameworkDirectory in Directory.EnumerateDirectories sourceShared do
        let frameworkName = Path.GetFileName frameworkDirectory
        let targetFrameworkDirectory = Path.Combine(fallbackShared, frameworkName)
        Directory.CreateDirectory targetFrameworkDirectory |> ignore

        for versionDirectory in Directory.EnumerateDirectories frameworkDirectory do
            let version = Path.GetFileName versionDirectory
            let target = Path.Combine(targetFrameworkDirectory, version)

            if frameworkName = "Microsoft.NETCore.App" && version = currentRuntimeVersion then
                copyDirectory versionDirectory target
            else
                Directory.CreateSymbolicLink(target, Path.GetFullPath versionDirectory) |> ignore

    let validatedShared = Path.Combine(validatedRoot, "shared")
    deleteDirectoryLink validatedShared
    Directory.CreateSymbolicLink(validatedShared, Path.GetFullPath fallbackShared) |> ignore

    validatedHost, fallbackHost

let private traceExternalNugetHelper
    root
    sourceDirectory
    sourcePath
    localFeed
    candidatePath
    fsharpCorePackage
    fsharpCoreVersion
    (sdkSelection: SdkSelection)
    (validatedDotnet: string)
    (alternateDotnet: string)
    =
    match nearestGlobalJson sourceDirectory with
    | Some path -> fail $"The host-selection trace fixture unexpectedly inherits a global.json: {path}"
    | None -> ()

    if OperatingSystem.IsWindows() then
        fail "The DOTNET_HOST_PATH subprocess trace wrapper currently requires the Unix shell available on the supported Ubuntu runner."
    else
        let traceWorkingDirectory = Path.Combine(root, "external-source-trace-workspace")
        let traceHome = Path.Combine(traceWorkingDirectory, "home")
        let traceTemp = Path.Combine(traceHome, "temp")
        let tracePackages = Path.Combine(traceWorkingDirectory, "packages")
        let traceHttp = Path.Combine(traceWorkingDirectory, "http-cache")
        let traceFile = Path.Combine(root, "dotnet-host-helper-trace.tsv")
        let traceWrapperDirectory = Path.Combine(root, "dotnet-host-trace-wrapper")
        let traceWrapper = Path.Combine(traceWrapperDirectory, "dotnet-host-wrapper")
        Directory.CreateDirectory traceWorkingDirectory |> ignore
        createPrivateDirectory traceHome
        createPrivateDirectory traceTemp
        createPrivateDirectory traceWrapperDirectory
        File.Copy(candidatePath, Path.Combine(localFeed, Path.GetFileName candidatePath), true)
        File.Copy(fsharpCorePackage, Path.Combine(localFeed, Path.GetFileName fsharpCorePackage), true)
        writeNuGetConfig (Path.Combine(sourceDirectory, "NuGet.Config")) localFeed
        writeSdkPin traceWorkingDirectory sdkSelection
        writeSdkPin traceHome sdkSelection
        writeSdkPin traceTemp sdkSelection
        verifySdkPin traceWorkingDirectory sdkSelection
        verifySdkPin traceHome sdkSelection
        verifySdkPin traceTemp sdkSelection

        writeSdkSwitchingTraceWrapper traceWrapper
        let originalPath = Environment.GetEnvironmentVariable "PATH" |> Option.ofObj |> Option.defaultValue ""
        let validatedDirectory = Path.GetDirectoryName validatedDotnet

        let environment =
            [ "HOME", traceHome
              "USERPROFILE", traceHome
              "DOTNET_CLI_HOME", traceHome
              "XDG_RUNTIME_DIR", traceHome
              "NUGET_PACKAGES", tracePackages
              "NUGET_HTTP_CACHE_PATH", traceHttp
              "TMPDIR", traceTemp
              "TEMP", traceTemp
              "TMP", traceTemp
              "RestoreSources", localFeed
              "NuGetAudit", "false"
              "FSharpCoreImplicitPackageVersion", fsharpCoreVersion
              "PATH", validatedDirectory + string Path.PathSeparator + originalPath
              "DOTNET_HOST_PATH", traceWrapper
              "PIPEL4_TRACE_VALIDATED_DOTNET", validatedDotnet
              "PIPEL4_TRACE_ALT_DOTNET", alternateDotnet
              "PIPEL4_TRACE_FILE", traceFile ]

        let pathSdkVersion, pathSdkBasePath = selectedSdkDetails validatedDotnet sourceDirectory

        if pathSdkVersion <> sdkSelection.Version
           || not (String.Equals(
                       Path.TrimEndingDirectorySeparator pathSdkBasePath,
                       Path.TrimEndingDirectorySeparator sdkSelection.BasePath,
                       if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal)) then
            fail $"PATH host selected SDK {pathSdkVersion} at '{pathSdkBasePath}' from the external source directory; expected {sdkSelection.Version} at '{sdkSelection.BasePath}'."

        let alternateSdk = runProcessWithoutDotnetHost alternateDotnet sourceDirectory [] [ "--version" ]
        let alternateInfo = runProcessWithoutDotnetHost alternateDotnet sourceDirectory [] [ "--info" ]

        if alternateSdk.TimedOut
           || alternateSdk.ExitCode <> 0
           || alternateInfo.TimedOut
           || alternateInfo.ExitCode <> 0 then
            fail $"Could not inspect the alternative DOTNET_HOST_PATH SDK. {alternateSdk.StandardOutput}{alternateSdk.StandardError}{alternateInfo.StandardOutput}{alternateInfo.StandardError}"

        let alternateVersion = alternateSdk.StandardOutput.Trim()

        let alternateBasePathMatch =
            System.Text.RegularExpressions.Regex.Match(
                alternateInfo.StandardOutput + alternateInfo.StandardError,
                "(?m)^\\s*Base Path:\\s*(.+?)\\s*$")

        if not alternateBasePathMatch.Success then
            fail $"Could not parse Base Path from the alternative DOTNET_HOST_PATH host '{alternateDotnet}'."

        let alternateBasePath = Path.GetFullPath(alternateBasePathMatch.Groups[1].Value.Trim())

        if alternateVersion = sdkSelection.Version then
            fail $"The alternative DOTNET_HOST_PATH fixture unexpectedly selected the validated SDK {sdkSelection.Version}."

        if String.Equals(
               Path.TrimEndingDirectorySeparator alternateBasePath,
               Path.TrimEndingDirectorySeparator sdkSelection.BasePath,
               if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal) then
            fail $"Alternative SDK {alternateVersion} unexpectedly shares the validated SDK Base Path '{alternateBasePath}'."

        let tracedFsi = runProcess validatedDotnet traceWorkingDirectory environment [ "fsi"; "--exec"; sourcePath ]

        if tracedFsi.TimedOut || tracedFsi.ExitCode <> 0 then
            fail $"Direct FSI run for the DOTNET_HOST_PATH helper trace failed (exit {tracedFsi.ExitCode}). {tracedFsi.StandardOutput}{tracedFsi.StandardError}"

        if not (File.Exists traceFile) then
            fail "The FSI dependency manager did not invoke the traced dotnet wrapper for NuGet source discovery."

        let expectedDirectory = Path.GetFullPath sourceDirectory
        let helperTrace =
            File.ReadAllLines traceFile
            |> Array.choose (fun line ->
                let fields = line.Split([| '\t' |], StringSplitOptions.None)
                if fields.Length >= 4 then Some(fields[0], fields[1], fields[2], fields[3]) else None)
            |> Array.tryFind (fun (directory, _, _, arguments) ->
                let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal
                String.Equals(Path.GetFullPath directory, expectedDirectory, comparison)
                && arguments.StartsWith("nuget list source", StringComparison.Ordinal))

        match helperTrace with
        | Some(directory, helperSdk, helperBasePath, arguments) when helperSdk = alternateVersion ->
            let helperBasePath = Path.GetFullPath helperBasePath

            if not (String.Equals(
                        Path.TrimEndingDirectorySeparator helperBasePath,
                        Path.TrimEndingDirectorySeparator alternateBasePath,
                        if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal)) then
                fail $"Actual NuGet helper Base Path '{helperBasePath}' did not match DOTNET_HOST_PATH preflight Base Path '{alternateBasePath}'."

            printfn "Observed actual DOTNET_HOST_PATH NuGet helper: cwd=%s; effective global.json=none; SDK=%s at %s; PATH SDK=%s; argv=dotnet %s." directory helperSdk helperBasePath sdkSelection.Version arguments
            Some(helperSdk, helperBasePath, directory, arguments, traceWrapper)
        | Some(directory, helperSdk, helperBasePath, arguments) ->
            fail $"NuGet helper trace found cwd '{directory}', SDK {helperSdk} at '{helperBasePath}', args '{arguments}'; expected DOTNET_HOST_PATH SDK {alternateVersion}."
        | None ->
            fail $"The traced NuGet helper did not run from external source directory '{expectedDirectory}'. Trace: {File.ReadAllText traceFile}"

let private runRegression packageArgument sourceArgument =
    let sdkSelection = resolveSdkSelection ()
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
    let seedTemp = Path.Combine(seededHome, "temp")
    let validatorTempRoot = Path.Combine(Path.GetTempPath(), $"PipeL4 FSI validator temp {Guid.NewGuid():N}")
    let externalControlRoot = Path.Combine(Path.GetTempPath(), $"PipeL4 FSI external SDK control {Guid.NewGuid():N}")

    let profileEnvironment profile =
        [ "HOME", profile
          "USERPROFILE", profile
          "DOTNET_CLI_HOME", profile
          "XDG_RUNTIME_DIR", profile
          "NUGET_PACKAGES", globalPackages
          "TMPDIR", validatorTempRoot
          "TEMP", validatorTempRoot
          "TMP", validatorTempRoot ]

    let validate profile package source =
        let environment = profileEnvironment profile
        verifySdkSelection root environment sdkSelection

        runDotnet
            root
            environment
            [ "fsi"; validatorPath; package; source ]

    try
        Directory.CreateDirectory(Path.GetDirectoryName candidate) |> ignore
        Directory.CreateDirectory seedFeed |> ignore
        createPrivateDirectory coldHome
        createPrivateDirectory seededHome
        createPrivateDirectory seedTemp
        createPrivateDirectory validatorTempRoot
        writeSdkPin root sdkSelection
        writeSdkPin seededHome sdkSelection
        writeSdkPin seedTemp sdkSelection
        writeSdkPin seedWorkingDirectory sdkSelection
        verifySdkPin root sdkSelection
        verifySdkPin seededHome sdkSelection
        verifySdkPin seedTemp sdkSelection
        verifySdkPin seedWorkingDirectory sdkSelection

        if Option.isSome (nearestGlobalJson validatorTempRoot) then
            fail $"The production-validator temp root unexpectedly inherits a test global.json: {validatorTempRoot}"

        File.Copy(packagePath, candidate)
        File.Copy(sourcePath, source)
        File.Copy(candidate, Path.Combine(seedFeed, Path.GetFileName candidate))
        File.Copy(fsharpCorePackage, Path.Combine(seedFeed, Path.GetFileName fsharpCorePackage))
        writeNuGetConfig (Path.Combine(seedWorkingDirectory, "NuGet.Config")) seedFeed

        let matchingHostWorkspace = Path.Combine(root, "dotnet-host-match-validation")
        let matchingHostTemp = Path.Combine(matchingHostWorkspace, "temp")
        Directory.CreateDirectory matchingHostWorkspace |> ignore
        createPrivateDirectory matchingHostTemp
        writeSdkPin matchingHostWorkspace sdkSelection
        let matchingHost = currentDotnetHostPath ()
        let matchingSourceDirectory = Path.GetDirectoryName sourcePath
        let pathHostVersion, pathHostBasePath = selectedSdkDetails "dotnet" matchingSourceDirectory
        let explicitHostVersion, explicitHostBasePath = selectedSdkDetails matchingHost matchingSourceDirectory

        let samePath =
            String.Equals(
                Path.TrimEndingDirectorySeparator pathHostBasePath,
                Path.TrimEndingDirectorySeparator explicitHostBasePath,
                if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal)

        if pathHostVersion <> sdkSelection.Version
           || explicitHostVersion <> sdkSelection.Version
           || not samePath
           || not (String.Equals(
                       Path.TrimEndingDirectorySeparator pathHostBasePath,
                       Path.TrimEndingDirectorySeparator sdkSelection.BasePath,
                       if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal)) then
            fail $"The PATH host selected SDK {pathHostVersion} at '{pathHostBasePath}' and DOTNET_HOST_PATH selected {explicitHostVersion} at '{explicitHostBasePath}' from '{matchingSourceDirectory}'; both must match validated SDK {sdkSelection.Version} at '{sdkSelection.BasePath}'."

        let matchingHostEnvironment =
            [ "TMPDIR", matchingHostTemp
              "TEMP", matchingHostTemp
              "TMP", matchingHostTemp
              "DOTNET_HOST_PATH", matchingHost ]
        let matchingHostValidation =
            runProcess
                matchingHost
                matchingHostWorkspace
                matchingHostEnvironment
                [ sdkSelection.FsiPath; validatorPath; candidate; sourcePath ]
        let matchingHostOutput = matchingHostValidation.StandardOutput + matchingHostValidation.StandardError

        if matchingHostValidation.TimedOut
           || matchingHostValidation.ExitCode <> 0
           || not (matchingHostOutput.Contains("FSI dependency-manager NuGet source discovery uses host", StringComparison.Ordinal)) then
            fail $"Production validation did not succeed when PATH dotnet and DOTNET_HOST_PATH selected the same SDK (exit {matchingHostValidation.ExitCode}). {matchingHostOutput}"

        printfn "PASS production validation succeeded when PATH dotnet and DOTNET_HOST_PATH selected SDK %s." sdkSelection.Version

        let invalidHostPath = Path.Combine(root, "missing-dotnet-host")
        let invalidHostValidation =
            runProcess
                matchingHost
                matchingHostWorkspace
                [ "TMPDIR", matchingHostTemp
                  "TEMP", matchingHostTemp
                  "TMP", matchingHostTemp
                  "DOTNET_HOST_PATH", invalidHostPath ]
                [ sdkSelection.FsiPath; validatorPath; candidate; sourcePath ]
        let invalidHostOutput = invalidHostValidation.StandardOutput + invalidHostValidation.StandardError

        if invalidHostValidation.TimedOut
           || invalidHostValidation.ExitCode = 0
           || not (invalidHostOutput.Contains("Could not inspect effective NuGet helper host", StringComparison.Ordinal))
           || not (invalidHostOutput.Contains(invalidHostPath, StringComparison.Ordinal)) then
            fail $"Production validation did not fail clearly for unavailable DOTNET_HOST_PATH '{invalidHostPath}' (exit {invalidHostValidation.ExitCode}). {invalidHostOutput}"

        printfn "PASS production validation failed explicitly for an unavailable DOTNET_HOST_PATH."

        if OperatingSystem.IsWindows() then
            printfn "SKIP same-version, different-Base-Path wrapper control: this fixture uses a Unix shell wrapper."
        else
            let sameVersionWrapperDirectory = Path.Combine(root, "same-version-host-wrapper")
            let sameVersionWrapper = Path.Combine(sameVersionWrapperDirectory, "dotnet-host-wrapper")
            let differentBasePath = Path.Combine(root, "same-version-other-install", "sdk", sdkSelection.Version)
            createPrivateDirectory sameVersionWrapperDirectory
            writeSameVersionDifferentBasePathWrapper sameVersionWrapper

            let sameVersionMismatch =
                runProcess
                    matchingHost
                    matchingHostWorkspace
                    [ "TMPDIR", matchingHostTemp
                      "TEMP", matchingHostTemp
                      "TMP", matchingHostTemp
                      "DOTNET_HOST_PATH", sameVersionWrapper
                      "PIPEL4_SAME_VERSION_HOST", matchingHost
                      "PIPEL4_DIFFERENT_BASE_PATH", differentBasePath ]
                    [ sdkSelection.FsiPath; validatorPath; candidate; sourcePath ]
            let sameVersionMismatchOutput = sameVersionMismatch.StandardOutput + sameVersionMismatch.StandardError

            if sameVersionMismatch.TimedOut
               || sameVersionMismatch.ExitCode = 0
               || not (sameVersionMismatchOutput.Contains($"would select SDK {sdkSelection.Version} at '{differentBasePath}", StringComparison.Ordinal))
               || not (sameVersionMismatchOutput.Contains($"validated FSI uses SDK {sdkSelection.Version} at '{sdkSelection.BasePath}'", StringComparison.Ordinal)) then
                fail $"Production validation accepted the same SDK version from a different Base Path (exit {sameVersionMismatch.ExitCode}). {sameVersionMismatchOutput}"

            printfn "PASS production validation rejected SDK %s from a different Base Path." sdkSelection.Version

        let sdkControlWorkspace = Path.Combine(root, "sdk-pin-control")
        writeSdkPin sdkControlWorkspace sdkSelection
        verifySdkPin sdkControlWorkspace sdkSelection
        printfn "PASS temporary child workspace selected the same exact SDK as its parent."

        let policyWorkspace = Path.Combine(root, "allow-prerelease-policy")
        Directory.CreateDirectory policyWorkspace |> ignore
        File.WriteAllText(
            Path.Combine(policyWorkspace, "global.json"),
            $"{{\"sdk\":{{\"version\":\"{sdkSelection.Version}\",\"rollForward\":\"disable\",\"allowPrerelease\":false,\"paths\":[\"$host$\"]}}}}")
        let policyValidation =
            runDotnet
                policyWorkspace
                []
                [ sdkSelection.FsiPath; validatorPath; packagePath; sourcePath ]
        let policyOutput = policyValidation.StandardOutput + policyValidation.StandardError

        if policyValidation.TimedOut
           || policyValidation.ExitCode <> 0
           || not (policyOutput.Contains("Temporary SDK pin: version " + sdkSelection.Version + ", rollForward=disable, allowPrerelease=false.", StringComparison.Ordinal))
           || not (policyOutput.Contains("Temporary SDK paths: $host$.", StringComparison.Ordinal)) then
            fail $"Production validation did not preserve allowPrerelease=false and SDK paths in its child pins (exit {policyValidation.ExitCode}). {policyOutput}"

        printfn "PASS production validator preserved inherited allowPrerelease=false and SDK paths in temporary SDK pins."

        let installedSdks = runDotnet Environment.CurrentDirectory [] [ "--list-sdks" ]

        if installedSdks.TimedOut || installedSdks.ExitCode <> 0 then
            fail $"Could not list installed SDKs for the newer-SDK selection control. {installedSdks.StandardError}"

        let selectedVersion = Version.Parse sdkSelection.Version
        let newerSdks =
            installedSdks.StandardOutput.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
            |> Array.choose (fun line ->
                let matched = System.Text.RegularExpressions.Regex.Match(line, "^(\\S+)\\s+\\[")

                if matched.Success then
                    match Version.TryParse matched.Groups[1].Value with
                    | true, version when version > selectedVersion -> Some matched.Groups[1].Value
                    | _ -> None
                else
                    None)

        if newerSdks.Length > 0 then
            verifySdkPin sdkControlWorkspace sdkSelection
            printfn "PASS installed newer SDKs (%s) did not take over exact-pinned child execution." (String.Join(", ", newerSdks))
        else
            printfn "LIMITATION: no SDK newer than %s is installed locally; exact pinning was verified, but takeover by a newer installed SDK could not be exercised." sdkSelection.Version

        let installedVersionStrings =
            installedSdks.StandardOutput.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
            |> Array.choose (fun line ->
                let matched = System.Text.RegularExpressions.Regex.Match(line, "^(\\S+)\\s+\\[")
                if matched.Success then Some matched.Groups[1].Value else None)
            |> Set.ofArray
        let maxInstalledMajor =
            installedVersionStrings
            |> Seq.choose (fun version ->
                match Version.TryParse version with
                | true, parsed -> Some parsed.Major
                | _ -> None)
            |> Seq.append [ Version.Parse(sdkSelection.Version).Major ]
            |> Seq.max
        let mutable unavailableMajor = maxInstalledMajor + 1
        let mutable unavailableSdk = $"{unavailableMajor}.0.0"

        while installedVersionStrings.Contains unavailableSdk do
            unavailableMajor <- unavailableMajor + 1
            unavailableSdk <- $"{unavailableMajor}.0.0"

        let unavailableWorkspace = Path.Combine(root, "unavailable-sdk-control")
        Directory.CreateDirectory unavailableWorkspace |> ignore
        File.WriteAllText(
            Path.Combine(unavailableWorkspace, "global.json"),
            $"{{\"sdk\":{{\"version\":\"{unavailableSdk}\",\"rollForward\":\"disable\"}}}}")
        let unavailable =
            runDotnet
                unavailableWorkspace
                []
                [ sdkSelection.FsiPath; validatorPath; candidate; source ]

        let unavailableOutput = unavailable.StandardOutput + unavailable.StandardError

        if unavailable.TimedOut || unavailable.ExitCode = 0 || not (unavailableOutput.Contains(unavailableSdk, StringComparison.Ordinal)) then
            fail $"Production FSI authoring validation did not fail explicitly for unavailable SDK {unavailableSdk}: {unavailableOutput}"

        printfn "PASS production validator failed explicitly when exact SDK %s was unavailable." unavailableSdk

        let mutationWork = Path.Combine(Path.GetTempPath(), $"PipeL4 FSI production pin mutation {Guid.NewGuid():N}")
        let mutationTemp = Path.Combine(mutationWork, "temp")
        let mutatedValidator = Path.Combine(mutationWork, "validate_fsi_authoring_without_pins.fsx")
        createPrivateDirectory mutationWork
        createPrivateDirectory mutationTemp

        try
            if Option.isSome (nearestGlobalJson mutationWork) then
                fail $"The production pin-removal control unexpectedly inherits a global.json: {mutationWork}"

            let productionValidatorSource = File.ReadAllLines validatorPath
            let productionPinWrites =
                set [ "writeSdkPin root sdkSelection"
                      "writeSdkPin fsiHome sdkSelection"
                      "writeSdkPin fsiTemp sdkSelection" ]
            let removedPinWrites =
                productionValidatorSource
                |> Array.filter (fun line -> productionPinWrites.Contains(line.Trim()))

            if removedPinWrites.Length <> 3 then
                fail $"Expected to disable all 3 production SDK pin writes, removed {removedPinWrites.Length}."

            let mutationSource =
                productionValidatorSource
                |> Array.filter (fun line -> not (productionPinWrites.Contains(line.Trim())))

            File.WriteAllLines(mutatedValidator, mutationSource)

            let mutationEnvironment =
                [ "TMPDIR", mutationTemp
                  "TEMP", mutationTemp
                  "TMP", mutationTemp ]

            let mutationResult =
                runDotnet
                    mutationWork
                    mutationEnvironment
                    [ sdkSelection.FsiPath; mutatedValidator; candidate; source ]

            let mutationOutput = mutationResult.StandardOutput + mutationResult.StandardError

            if mutationResult.TimedOut
               || mutationResult.ExitCode = 0
               || not (mutationOutput.Contains("Production SDK pin is missing", StringComparison.Ordinal)) then
                fail $"Regression failed to detect disabled production SDK pins (exit {mutationResult.ExitCode}). {mutationOutput}"

            printfn "PASS production-pin-removal mutation was rejected for a missing validator-owned SDK pin."
        finally
            if Directory.Exists mutationWork then
                Directory.Delete(mutationWork, true)

        let availableSdks = enumerateInstalledSdks ()
        let singleSdkFixture = [ sdkSelection.Version, "<controlled-single-sdk-fixture>" ]

        if selectAlternateSdk sdkSelection singleSdkFixture |> Option.isSome then
            fail "The single-SDK decision fixture incorrectly selected an alternative SDK."

        printfn "PASS single-SDK fixture selects the explicit cross-SDK SKIP path; the negative test is not counted as executed."

        match selectAlternateSdk sdkSelection availableSdks with
        | None ->
            printfn "SKIP DOTNET_HOST_PATH cross-SDK mismatch control: no installed SDK differs from validated SDK %s." sdkSelection.Version
            printfn "SKIP external source global.json mismatch control: no alternative installed SDK is available."
        | Some(alternativeSdk) ->
            let alternativeVersion, alternativeSdkRoot = alternativeSdk
            let validatedSdk =
                availableSdks
                |> List.tryFind (fun (version, _) -> String.Equals(version, sdkSelection.Version, StringComparison.Ordinal))
                |> Option.defaultWith (fun () -> fail $"The running SDK {sdkSelection.Version} was not present in 'dotnet --list-sdks'.")

            let validatedDotnet = createIsolatedSdkHost root validatedSdk
            let alternateDotnet = createIsolatedSdkHost root (alternativeVersion, alternativeSdkRoot)
            let hostMismatchSourceDirectory = Path.Combine(externalControlRoot, "dotnet-host-mismatch-source")
            let hostMismatchSource = Path.Combine(hostMismatchSourceDirectory, Path.GetFileName source)
            let hostMismatchFeed = Path.Combine(hostMismatchSourceDirectory, "feed")
            Directory.CreateDirectory hostMismatchSourceDirectory |> ignore
            Directory.CreateDirectory hostMismatchFeed |> ignore
            File.Copy(source, hostMismatchSource)
            File.Copy(candidate, Path.Combine(hostMismatchFeed, Path.GetFileName candidate))
            File.Copy(fsharpCorePackage, Path.Combine(hostMismatchFeed, Path.GetFileName fsharpCorePackage))
            writeNuGetConfig (Path.Combine(hostMismatchSourceDirectory, "NuGet.Config")) hostMismatchFeed

            let helperTrace =
                traceExternalNugetHelper
                    root
                    hostMismatchSourceDirectory
                    hostMismatchSource
                    hostMismatchFeed
                    candidate
                    fsharpCorePackage
                    fsharpCoreVersion
                    sdkSelection
                    validatedDotnet
                    alternateDotnet

            let helperSdk, helperBasePath, helperDirectory, helperArguments, traceWrapper =
                match helperTrace with
                | Some(helperSdk, helperBasePath, helperDirectory, helperArguments, traceWrapper) ->
                    if helperSdk <> alternativeVersion then
                        fail $"Traced NuGet helper selected SDK {helperSdk}; expected installed alternative SDK {alternativeVersion}."

                    helperSdk, helperBasePath, helperDirectory, helperArguments, traceWrapper
                | None -> fail "The DOTNET_HOST_PATH helper trace did not execute on this multi-SDK machine."

            if not (String.Equals(Path.GetFullPath helperDirectory, Path.GetFullPath hostMismatchSourceDirectory, if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal))
               || not (helperArguments.StartsWith("nuget list source", StringComparison.Ordinal)) then
                fail $"The traced NuGet helper used unexpected cwd '{helperDirectory}' or arguments '{helperArguments}'."

            let mismatchWorkspace = Path.Combine(root, "dotnet-host-mismatch-validation")
            let mismatchTemp = Path.Combine(mismatchWorkspace, "temp")
            Directory.CreateDirectory mismatchWorkspace |> ignore
            createPrivateDirectory mismatchTemp
            writeSdkPin mismatchWorkspace sdkSelection
            verifySdkPin mismatchWorkspace sdkSelection
            let pathDirectory = Path.GetDirectoryName validatedDotnet
            let originalPath = Environment.GetEnvironmentVariable "PATH" |> Option.ofObj |> Option.defaultValue ""
            let mismatchEnvironment =
                [ "TMPDIR", mismatchTemp
                  "TEMP", mismatchTemp
                  "TMP", mismatchTemp
                  "PATH", pathDirectory + string Path.PathSeparator + originalPath
                  "DOTNET_HOST_PATH", traceWrapper
                  "PIPEL4_TRACE_VALIDATED_DOTNET", validatedDotnet
                  "PIPEL4_TRACE_ALT_DOTNET", alternateDotnet
                  "PIPEL4_TRACE_FILE", Path.Combine(root, "dotnet-host-helper-trace.tsv") ]
            let mismatchValidation =
                runProcess
                    validatedDotnet
                    mismatchWorkspace
                    mismatchEnvironment
                    [ "fsi"; validatorPath; candidate; hostMismatchSource ]
            let mismatchOutput = mismatchValidation.StandardOutput + mismatchValidation.StandardError

            if mismatchValidation.TimedOut
               || mismatchValidation.ExitCode = 0
               || not (mismatchOutput.Contains($"would select SDK {alternativeVersion}", StringComparison.Ordinal))
               || not (mismatchOutput.Contains(helperBasePath, StringComparison.Ordinal))
               || not (mismatchOutput.Contains($"using host '{traceWrapper}'", StringComparison.Ordinal))
               || not (mismatchOutput.Contains($"validated FSI uses SDK {sdkSelection.Version}", StringComparison.Ordinal)) then
                fail $"Production validation did not reject the DOTNET_HOST_PATH SDK mismatch (exit {mismatchValidation.ExitCode}). {mismatchOutput}"

            printfn "PASS PATH selected SDK %s while traced DOTNET_HOST_PATH NuGet helper selected SDK %s; production preflight rejected the mismatch." sdkSelection.Version helperSdk

            if OperatingSystem.IsWindows() then
                printfn "SKIP unset-host cross-SDK fallback control: this isolated runtime-root fixture uses Unix directory links."
            else
                let fallbackPairRoot = Path.Combine(root, "runtime-derived-fallback-hosts")
                let fallbackPathHost, fallbackHost =
                    createFallbackSdkHostPair
                        fallbackPairRoot
                        validatedSdk
                        (alternativeVersion, alternativeSdkRoot)
                let fallbackPathVersion, fallbackPathBasePath = selectedSdkDetails fallbackPathHost hostMismatchSourceDirectory
                let fallbackSdkVersion, fallbackBasePath = selectedSdkDetails fallbackHost hostMismatchSourceDirectory
                let pathHostDirectory = Path.GetDirectoryName fallbackPathHost
                let originalPath = Environment.GetEnvironmentVariable "PATH" |> Option.ofObj |> Option.defaultValue ""
                let fallbackPath = pathHostDirectory + string Path.PathSeparator + originalPath

                if fallbackPathVersion <> sdkSelection.Version
                   || fallbackSdkVersion <> alternativeVersion then
                    fail
                        $"The fallback-host fixture selected SDK {fallbackPathVersion} from PATH and SDK {fallbackSdkVersion} from the runtime-derived host; expected {sdkSelection.Version} and {alternativeVersion}."

                printfn
                    "Original fallback-host reproduction: DOTNET_HOST_PATH=unset; FSI SDK=%s; PATH host=%s at %s; runtime-derived fallback host=%s selects SDK %s at %s from external source directory '%s'."
                    sdkSelection.Version
                    fallbackPathVersion
                    fallbackPathBasePath
                    fallbackHost
                    fallbackSdkVersion
                    fallbackBasePath
                    hostMismatchSourceDirectory

                let fallbackMismatchWorkspace = Path.Combine(root, "unset-host-fallback-mismatch-validation")
                Directory.CreateDirectory fallbackMismatchWorkspace |> ignore
                writeSdkPin fallbackMismatchWorkspace sdkSelection
                let fallbackEnvironment =
                    [ "PATH", fallbackPath
                      "TMPDIR", mismatchTemp
                      "TEMP", mismatchTemp
                      "TMP", mismatchTemp ]
                let fallbackMismatchValidation =
                    runProcessWithoutDotnetHost
                        fallbackPathHost
                        fallbackMismatchWorkspace
                        fallbackEnvironment
                        [ sdkSelection.FsiPath; validatorPath; candidate; hostMismatchSource ]
                let fallbackMismatchOutput =
                    fallbackMismatchValidation.StandardOutput + fallbackMismatchValidation.StandardError

                if fallbackMismatchValidation.TimedOut
                   || fallbackMismatchValidation.ExitCode = 0
                   || not (fallbackMismatchOutput.Contains($"would select SDK {alternativeVersion} at '{fallbackBasePath}'", StringComparison.Ordinal))
                   || not (fallbackMismatchOutput.Contains($"using host '{fallbackHost}'", StringComparison.Ordinal))
                   || not (fallbackMismatchOutput.Contains($"validated FSI uses SDK {sdkSelection.Version} at '{sdkSelection.BasePath}'", StringComparison.Ordinal)) then
                    fail
                        $"Production validation did not reject the unset-DOTNET_HOST_PATH fallback mismatch (exit {fallbackMismatchValidation.ExitCode}). {fallbackMismatchOutput}"

                let fallbackTraceWorkspace = Path.Combine(root, "unset-host-helper-trace-workspace")
                let fallbackTraceHome = Path.Combine(fallbackTraceWorkspace, "home")
                let fallbackTraceTemp = Path.Combine(fallbackTraceHome, "temp")
                let fallbackTracePackages = Path.Combine(fallbackTraceWorkspace, "packages")
                let fallbackTraceHttp = Path.Combine(fallbackTraceWorkspace, "http-cache")
                Directory.CreateDirectory fallbackTraceWorkspace |> ignore
                createPrivateDirectory fallbackTraceHome
                createPrivateDirectory fallbackTraceTemp
                writeSdkPin fallbackTraceWorkspace sdkSelection
                writeSdkPin fallbackTraceHome sdkSelection
                writeSdkPin fallbackTraceTemp sdkSelection
                verifySdkPin fallbackTraceWorkspace sdkSelection
                verifySdkPin fallbackTraceHome sdkSelection
                verifySdkPin fallbackTraceTemp sdkSelection

                let fallbackTraceEnvironment =
                    [ "HOME", fallbackTraceHome
                      "USERPROFILE", fallbackTraceHome
                      "DOTNET_CLI_HOME", fallbackTraceHome
                      "XDG_RUNTIME_DIR", fallbackTraceHome
                      "NUGET_PACKAGES", fallbackTracePackages
                      "NUGET_HTTP_CACHE_PATH", fallbackTraceHttp
                      "TMPDIR", fallbackTraceTemp
                      "TEMP", fallbackTraceTemp
                      "TMP", fallbackTraceTemp
                      "RestoreSources", hostMismatchFeed
                      "NuGetAudit", "false"
                      "FSharpCoreImplicitPackageVersion", fsharpCoreVersion
                      "PATH", fallbackPath ]

                let fallbackTraceResult, actualFallbackTrace =
                    runProcessTracingNugetHelper
                        fallbackPathHost
                        fallbackTraceWorkspace
                        fallbackTraceEnvironment
                        [ sdkSelection.FsiPath; "--exec"; hostMismatchSource ]

                let actualFallbackHost, actualFallbackDirectory, actualFallbackArguments =
                    actualFallbackTrace
                    |> Option.defaultWith (fun () ->
                        fail
                            $"Could not independently observe the unset-DOTNET_HOST_PATH NuGet helper process under /proc (FSI exit {fallbackTraceResult.ExitCode}). {fallbackTraceResult.StandardOutput}{fallbackTraceResult.StandardError}")

                if fallbackTraceResult.TimedOut then
                    fail "The direct FSI run for the unset-host NuGet helper trace timed out."

                let helperSdk, helperBasePath = selectedSdkDetails actualFallbackHost actualFallbackDirectory
                let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal

                if not (String.Equals(Path.GetFullPath actualFallbackHost, Path.GetFullPath fallbackHost, comparison))
                   || not (String.Equals(Path.GetFullPath actualFallbackDirectory, Path.GetFullPath hostMismatchSourceDirectory, comparison))
                   || helperSdk <> alternativeVersion
                   || not (String.Equals(
                               Path.TrimEndingDirectorySeparator helperBasePath,
                               Path.TrimEndingDirectorySeparator fallbackBasePath,
                               comparison))
                   || not (actualFallbackArguments.Contains("nuget list source", StringComparison.Ordinal)) then
                    fail
                        $"Actual fallback NuGet helper used host '{actualFallbackHost}' from '{actualFallbackDirectory}', selecting SDK {helperSdk} at '{helperBasePath}' with args '{actualFallbackArguments}'; expected host '{fallbackHost}' from '{hostMismatchSourceDirectory}' selecting SDK {alternativeVersion} at '{fallbackBasePath}'."

                printfn
                    "Observed actual fallback NuGet helper: DOTNET_HOST_PATH=unset; host=%s; cwd=%s; SDK=%s at %s; PATH host SDK=%s; argv=%s."
                    actualFallbackHost
                    actualFallbackDirectory
                    helperSdk
                    helperBasePath
                    fallbackPathVersion
                    actualFallbackArguments

                if fallbackTraceResult.ExitCode <> 0 then
                    printfn
                        "LIMITATION: the direct FSI trace fixture exited %d after invoking the traced fallback helper: %s"
                        fallbackTraceResult.ExitCode
                        fallbackTraceResult.StandardError

                let fallbackMutantPath = Path.Combine(root, "validate_fsi_authoring_path-fallback-mutant.fsx")
                let validatorSource = File.ReadAllText validatorPath
                let fallbackResolverLine = "    | _ -> fallbackNugetHelperHostPath environment"
                let pathFallbackMutantLine = "    | _ -> \"dotnet\""
                let fallbackMutantSource = validatorSource.Replace(fallbackResolverLine, pathFallbackMutantLine, StringComparison.Ordinal)

                if String.Equals(fallbackMutantSource, validatorSource, StringComparison.Ordinal) then
                    fail "Could not create the temporary PATH-fallback mutant for mutation-sensitivity verification."

                File.WriteAllText(fallbackMutantPath, fallbackMutantSource)
                let fallbackMutantValidation =
                    runProcessWithoutDotnetHost
                        fallbackPathHost
                        fallbackMismatchWorkspace
                        fallbackEnvironment
                        [ sdkSelection.FsiPath; fallbackMutantPath; candidate; hostMismatchSource ]
                let fallbackMutantOutput =
                    fallbackMutantValidation.StandardOutput + fallbackMutantValidation.StandardError

                if fallbackMutantValidation.TimedOut
                   || fallbackMutantOutput.Contains($"would select SDK {alternativeVersion}", StringComparison.Ordinal)
                   || not (fallbackMutantOutput.Contains("FSI dependency-manager NuGet source discovery uses host 'dotnet'", StringComparison.Ordinal)) then
                    fail
                        $"The temporary PATH-fallback mutant did not recreate the preflight false positive (exit {fallbackMutantValidation.ExitCode}). {fallbackMutantOutput}"

                if fallbackMutantValidation.ExitCode = 0 then
                    printfn
                        "PASS PATH-fallback mutation reproduced the original false positive: preflight accepted SDK %s while the independently traced helper selected SDK %s."
                        sdkSelection.Version
                        helperSdk
                else
                    printfn
                        "PASS PATH-fallback mutation bypassed mismatch rejection and accepted PATH SDK %s; full validation later failed with exit %d. The independent trace still proves the helper selected SDK %s."
                        sdkSelection.Version
                        fallbackMutantValidation.ExitCode
                        helperSdk

            let policyDirectory = Path.Combine(externalControlRoot, "external-source-sdk-policy")
            let policySource = Path.Combine(policyDirectory, Path.GetFileName source)
            let policyFeed = Path.Combine(policyDirectory, "feed")
            Directory.CreateDirectory policyDirectory |> ignore
            Directory.CreateDirectory policyFeed |> ignore
            File.Copy(source, policySource)
            File.Copy(candidate, Path.Combine(policyFeed, Path.GetFileName candidate))
            File.Copy(fsharpCorePackage, Path.Combine(policyFeed, Path.GetFileName fsharpCorePackage))
            writeNuGetConfig (Path.Combine(policyDirectory, "NuGet.Config")) policyFeed
            File.WriteAllText(
                Path.Combine(policyDirectory, "global.json"),
                $"{{\"sdk\":{{\"version\":\"{alternativeVersion}\",\"rollForward\":\"disable\"}}}}")
            let policyWorkspace = Path.Combine(root, "external-source-global-json-mismatch")
            let policyTemp = Path.Combine(policyWorkspace, "temp")
            Directory.CreateDirectory policyWorkspace |> ignore
            createPrivateDirectory policyTemp
            writeSdkPin policyWorkspace sdkSelection
            let policyResult =
                runProcessWithoutDotnetHost
                    (currentDotnetHostPath ())
                    policyWorkspace
                    [ "TMPDIR", policyTemp
                      "TEMP", policyTemp
                      "TMP", policyTemp ]
                    [ sdkSelection.FsiPath; validatorPath; candidate; policySource ]
            let policyOutput = policyResult.StandardOutput + policyResult.StandardError

            if policyResult.TimedOut
               || policyResult.ExitCode = 0
               || not (policyOutput.Contains($"would select SDK {alternativeVersion}", StringComparison.Ordinal))
               || not (policyOutput.Contains($"validated FSI uses SDK {sdkSelection.Version}", StringComparison.Ordinal)) then
                fail $"Production validation did not reject external source global.json selecting SDK {alternativeVersion}. {policyOutput}"

            printfn "PASS validator rejected external source global.json SDK %s against validated FSI SDK %s." alternativeVersion sdkSelection.Version

        let diagnosticWork = Path.Combine(root, "pre-restore-diagnostic")
        let diagnosticHome = Path.Combine(diagnosticWork, "home")
        let diagnosticTemp = Path.Combine(diagnosticHome, "temp")
        let diagnosticPackages = Path.Combine(diagnosticWork, "packages")
        let diagnosticHttp = Path.Combine(diagnosticWork, "http-cache")
        let diagnosticFeed = Path.Combine(diagnosticWork, "feed")
        let missingSource = Path.Combine(diagnosticWork, "does-not-exist.fsx")
        Directory.CreateDirectory diagnosticFeed |> ignore
        createPrivateDirectory diagnosticHome
        createPrivateDirectory diagnosticTemp
        writeSdkPin diagnosticWork sdkSelection
        writeSdkPin diagnosticHome sdkSelection
        writeSdkPin diagnosticTemp sdkSelection
        File.Copy(candidate, Path.Combine(diagnosticFeed, Path.GetFileName candidate))
        File.Copy(fsharpCorePackage, Path.Combine(diagnosticFeed, Path.GetFileName fsharpCorePackage))
        writeNuGetConfig (Path.Combine(diagnosticWork, "NuGet.Config")) diagnosticFeed

        let diagnosticEnvironment =
            [ "HOME", diagnosticHome
              "USERPROFILE", diagnosticHome
              "DOTNET_CLI_HOME", diagnosticHome
              "XDG_RUNTIME_DIR", diagnosticHome
              "NUGET_PACKAGES", diagnosticPackages
              "NUGET_HTTP_CACHE_PATH", diagnosticHttp
              "TMPDIR", diagnosticTemp
              "TEMP", diagnosticTemp
              "TMP", diagnosticTemp
              "RestoreSources", diagnosticFeed
              "NuGetAudit", "false"
              "FSharpCoreImplicitPackageVersion", fsharpCoreVersion ]

        verifySdkPin diagnosticWork sdkSelection
        verifySdkPin diagnosticHome sdkSelection
        verifySdkPin diagnosticTemp sdkSelection

        let beforeRestoreFailure = runDotnet diagnosticWork diagnosticEnvironment [ "fsi"; "--exec"; missingSource ]

        if beforeRestoreFailure.TimedOut
           || beforeRestoreFailure.ExitCode = 0
           || not ((beforeRestoreFailure.StandardOutput + beforeRestoreFailure.StandardError).Contains(missingSource, StringComparison.Ordinal)) then
            fail $"The pre-restore diagnostic control did not fail on the missing script as expected. {beforeRestoreFailure.StandardOutput}{beforeRestoreFailure.StandardError}"

        let preRestoreManagerCache = Path.Combine(diagnosticHome, ".packagemanagement", "nuget", "Cache")
        let preRestoreReferences =
            if Directory.Exists preRestoreManagerCache then
                Directory.GetFiles(preRestoreManagerCache, "*.resolvedReferences.paths", SearchOption.AllDirectories)
            else
                [||]

        let diagnosticPackageId, diagnosticPackageVersion = packageIdentity candidate
        let preRestorePackage = Path.Combine(diagnosticPackages, diagnosticPackageId.ToLowerInvariant(), diagnosticPackageVersion.ToLowerInvariant())

        if preRestoreReferences.Length > 0 || Directory.Exists preRestorePackage then
            fail "The missing-script control unexpectedly reached dependency restore before diagnostics."

        let diagnosticCacheRoots = [ diagnosticHome; diagnosticPackages; diagnosticHttp ]
        let inventoryBeforeDiagnostics = cacheFileInventory diagnosticCacheRoots
        let preRestoreSummary =
            collectDirectFailureDiagnostics
                (diagnosticWork,
                 diagnosticHome,
                 diagnosticFeed,
                 diagnosticPackages,
                 diagnosticHttp,
                 candidate,
                 missingSource,
                 sdkSelection,
                 diagnosticEnvironment,
                 beforeRestoreFailure)
        let inventoryAfterDiagnostics = cacheFileInventory diagnosticCacheRoots

        printfn "Pre-restore diagnostic cache inventory before (%d entries):" inventoryBeforeDiagnostics.Length
        for item in inventoryBeforeDiagnostics do printfn "CACHE %s" item
        printfn "Pre-restore diagnostic cache inventory after (%d entries):" inventoryAfterDiagnostics.Length
        for item in inventoryAfterDiagnostics do printfn "CACHE %s" item

        if inventoryBeforeDiagnostics <> inventoryAfterDiagnostics then
            fail
                $"Failure diagnostics modified the investigated cache after a pre-restore failure.\nBefore:\n{String.concat Environment.NewLine inventoryBeforeDiagnostics}\nAfter:\n{String.concat Environment.NewLine inventoryAfterDiagnostics}"

        for absentEvidence in
            [ "project.assets.json=not retained by the FSI dependency manager"
              "dependency-manager.restore-evidence=no .resolvedReferences.paths file was retained"
              "dependency-manager.generated-fsharp-script=not retained"
              "assembly-load-evidence=not collected; diagnostics do not execute an FSI assembly probe" ] do
            if not (preRestoreSummary.Contains(absentEvidence, StringComparison.Ordinal)) then
                fail $"Pre-restore diagnostics did not report absent evidence '{absentEvidence}'.\n{preRestoreSummary}"

        if not (preRestoreSummary.Contains($"child.exit-code={beforeRestoreFailure.ExitCode}", StringComparison.Ordinal))
           || not (preRestoreSummary.Contains($"child.timed-out={beforeRestoreFailure.TimedOut}", StringComparison.Ordinal))
           || not (preRestoreSummary.Contains("stdout=", StringComparison.Ordinal))
           || not (preRestoreSummary.Contains("stderr=", StringComparison.Ordinal)) then
            fail $"Pre-restore diagnostics omitted the original child result.\n{preRestoreSummary}"

        printfn "PASS pre-restore failure diagnostics were read-only across HOME, package, HTTP, and dependency-manager caches."

        let cold = validate coldHome candidate source
        expectSuccess "cold candidate validation restored from the isolated local feed" "restored only from the local candidate feed" cold
        expectProductionSdkPins "cold validation" cold
        let runtimeAssemblyDirectory = Path.GetDirectoryName typeof<int>.Assembly.Location
        let runtimeFallbackHost =
            Path.GetFullPath(Path.Combine(runtimeAssemblyDirectory, "..", "..", "..", if OperatingSystem.IsWindows() then "dotnet.exe" else "dotnet"))
        let coldOutput = cold.StandardOutput + cold.StandardError

        if not (coldOutput.Contains($"FSI dependency-manager NuGet source discovery uses host '{runtimeFallbackHost}'", StringComparison.Ordinal)) then
            fail $"Cold validation did not report the runtime-derived compatible fallback host '{runtimeFallbackHost}'. {coldOutput}"

        printfn "PASS compatible runtime-derived fallback host succeeded with DOTNET_HOST_PATH absent and selected SDK %s." sdkSelection.Version

        let directEnvironment =
            [ "HOME", seededHome
              "USERPROFILE", seededHome
              "DOTNET_CLI_HOME", seededHome
              "XDG_RUNTIME_DIR", seededHome
              "NUGET_PACKAGES", seedPackages
              "NUGET_HTTP_CACHE_PATH", seedHttpCache
              "TMPDIR", seedTemp
              "TEMP", seedTemp
              "TMP", seedTemp
              "RestoreSources", seedFeed
              "NuGetAudit", "false"
              "FSharpCoreImplicitPackageVersion", fsharpCoreVersion ]

        verifySdkSelection seedWorkingDirectory directEnvironment sdkSelection
        verifySdkSelection seededHome directEnvironment sdkSelection
        verifySdkSelection seedTemp directEnvironment sdkSelection
        verifySdkPin seedWorkingDirectory sdkSelection
        verifySdkPin seededHome sdkSelection
        verifySdkPin seedTemp sdkSelection

        let direct = runDotnet seedWorkingDirectory directEnvironment [ "fsi"; "--exec"; source ]

        if direct.TimedOut || direct.ExitCode <> 0 then
            let diagnostics =
                collectDirectFailureDiagnostics
                    (seedWorkingDirectory,
                     seededHome,
                     seedFeed,
                     seedPackages,
                     seedHttpCache,
                     candidate,
                     source,
                     sdkSelection,
                     directEnvironment,
                     direct)

            printDirectFailureDiagnostics diagnostics
            fail $"Direct FSI cache seeding failed (exit {direct.ExitCode}, timed out: {direct.TimedOut}); see structured diagnostics above."

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

        let generatedScripts = Directory.GetFiles(managerCache, "*.fsx", SearchOption.AllDirectories)
        let resolvedReferenceFiles = Directory.GetFiles(managerCache, "*.resolvedReferences.paths", SearchOption.AllDirectories)
        let generatedAssets = Directory.GetFiles(seededHome, "project.assets.json", SearchOption.AllDirectories)
        let sdkArtifactDirectories =
            [ yield seedWorkingDirectory
              yield seededHome
              yield seedTemp
              yield managerCache
              yield! generatedScripts |> Array.map Path.GetDirectoryName
              yield! resolvedReferenceFiles |> Array.map Path.GetDirectoryName
              yield! generatedAssets |> Array.map Path.GetDirectoryName ]
            |> List.distinct

        for directory in sdkArtifactDirectories do
            verifySdkSelection directory directEnvironment sdkSelection

            let fullDirectory = Path.GetFullPath directory
            let fullDirectRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath seedWorkingDirectory)
            let directRootPrefix = fullDirectRoot + string Path.DirectorySeparatorChar
            let fullTempRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath seedTemp)
            let tempRootPrefix = fullTempRoot + string Path.DirectorySeparatorChar
            let pathComparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal
            let expectedGlobalJson =
                if String.Equals(fullDirectory, fullTempRoot, pathComparison)
                   || fullDirectory.StartsWith(tempRootPrefix, pathComparison) then
                    Path.Combine(seedTemp, "global.json")
                elif String.Equals(fullDirectory, fullDirectRoot, pathComparison)
                     || fullDirectory.StartsWith(directRootPrefix, pathComparison) then
                    Path.Combine(seedWorkingDirectory, "global.json")
                else
                    Path.Combine(seededHome, "global.json")

            match nearestGlobalJson directory with
            | Some path when Path.GetFullPath path = Path.GetFullPath expectedGlobalJson -> ()
            | Some path -> fail $"Dependency-manager artifact root '{directory}' inherits unexpected SDK policy '{path}'."
            | None -> fail $"Dependency-manager artifact root '{directory}' has no pinned global.json ancestor."

        if generatedScripts.Length = 0 then
            fail "Direct FSI did not retain a generated F# dependency-manager script."

        if resolvedReferenceFiles.Length = 0 then
            fail "Direct FSI did not retain a .resolvedReferences.paths dependency-manager cache."

        printfn
            "PASS FSI dependency-manager working roots used SDK %s; retained %d generated script(s), %d resolved-reference cache(s), and %d project.assets.json file(s)."
            sdkSelection.Version
            generatedScripts.Length
            resolvedReferenceFiles.Length
            generatedAssets.Length

        printfn "PASS direct FSI seeded a conflicting PipeL4.Plugins resolution under HOME/.packagemanagement/nuget/Cache"

        let diagnosticSource = Path.Combine(root, "DirectFailure.fsx")
        File.WriteAllText(diagnosticSource, File.ReadAllText(source) + Environment.NewLine + "let invalidValue: int = \"not an integer\"")
        let directFailure = runDotnet seedWorkingDirectory directEnvironment [ "fsi"; "--exec"; diagnosticSource ]

        if directFailure.TimedOut || directFailure.ExitCode = 0 then
            fail $"The controlled direct-FSI diagnostic failure did not fail (exit {directFailure.ExitCode}, timed out: {directFailure.TimedOut})."

        let failureDiagnostics =
            collectDirectFailureDiagnostics
                (seedWorkingDirectory,
                 seededHome,
                 seedFeed,
                 seedPackages,
                 seedHttpCache,
                 candidate,
                 diagnosticSource,
                 sdkSelection,
                 directEnvironment,
                 directFailure)

        for requiredEvidence in
            [ "child.sdk.version="
              "child.fsi.path="
              "child.working-directory="
              "environment.FSharpCoreImplicitPackageVersion="
              "nuget.sources="
              "restored.package.source="
              "dependency-manager.resolvedReferences.paths="
              "dependency-manager.generated-fsharp-script="
              "project.assets.json="
              "assembly-load-evidence=not collected; diagnostics do not execute an FSI assembly probe"
              "observed.resolved-reference="
              "stdout="
              "stderr=" ] do
            if not (failureDiagnostics.Contains(requiredEvidence, StringComparison.Ordinal)) then
                fail $"Direct FSI failure diagnostics omitted expected evidence '{requiredEvidence}'.\n{failureDiagnostics}"

        printDirectFailureDiagnostics failureDiagnostics
        printfn "PASS direct FSI failure diagnostics captured existing SDK/restore artifacts and stdout/stderr without an assembly probe."

        let warm = validate seededHome candidate source
        expectSuccess "warm candidate validation ignored the seeded FSI resolution and restored from its local feed" "restored only from the local candidate feed" warm
        expectProductionSdkPins "warm validation" warm

        let repeated = validate seededHome candidate source
        expectSuccess "repeated candidate validation remained successful" "restored only from the local candidate feed" repeated
        expectProductionSdkPins "repeated validation" repeated

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

        if Directory.Exists validatorTempRoot then
            Directory.Delete(validatorTempRoot, true)

        if Directory.Exists externalControlRoot then
            Directory.Delete(externalControlRoot, true)

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
