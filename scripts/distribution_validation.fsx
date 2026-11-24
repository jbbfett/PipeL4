open System
open System.Collections.Generic
open System.Globalization
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

exception private ValidationException of string
exception private UsageException of string

type private ArtifactExpectations =
    { ArtifactRoot: string
      ConsumerSdk: string
      ProducerSdk: string
      TestedSha: string
      RunId: string
      ProducerAttempt: string }

type private PackageRecord =
    { Id: string
      Version: string
      Path: string
      WorkspacePath: string
      Sha256: string
      SourceRevision: string
      SourceRevisionFile: string }

type private ValidatedArtifact =
    { ToolPackagePath: string
      ApiPackagePath: string
      PackageVersion: string }

type private CliResult =
    { ExitCode: int
      StandardOutput: string
      StandardError: string }

let private sdkRoles =
    Map.ofList [ "10.0.115", "canonical"; "10.0.404", "compatibility" ]

let private aggregateJobs =
    [ "produce_canonical"
      "produce_compatibility"
      "consume_115_from_115"
      "consume_404_from_115"
      "consume_115_from_404"
      "consume_404_from_404" ]

let private expectedPackageDirectories =
    [ "PipeL4", "packages/PipeL4"
      "PipeL4.Plugins", "packages/PipeL4.Plugins" ]

let private expectedChecksumPaths packageVersion =
    Set.ofList
        [ $"packages/PipeL4/PipeL4.{packageVersion}.nupkg"
          "packages/PipeL4/source-revision.txt"
          $"packages/PipeL4.Plugins/PipeL4.Plugins.{packageVersion}.nupkg"
          "packages/PipeL4.Plugins/source-revision.txt" ]

let private usageText =
    """Usage:
  dotnet fsi --exec scripts/distribution_validation.fsx select-artifact --artifact-name <name> --producer-attempt <attempt> --producer-sdk <version> --tested-sha <sha> --run-id <id> --github-output <path>
  dotnet fsi --exec scripts/distribution_validation.fsx artifact --artifact-root <path> --consumer-sdk <version> --producer-sdk <version> --tested-sha <sha> --run-id <id> --producer-attempt <attempt> [--github-output <path>]
  dotnet fsi --exec scripts/distribution_validation.fsx aggregate --produce-canonical <result> --produce-compatibility <result> --consume-115-from-115 <result> --consume-404-from-115 <result> --consume-115-from-404 <result> --consume-404-from-404 <result>
  dotnet fsi --exec scripts/distribution_validation.fsx self-test
"""

let private fail message = raise (ValidationException message)

let private require condition message =
    if not condition then fail message

let private localName (element: XElement) = element.Name.LocalName

let private jsonProperty (parent: JsonElement) (name: string) =
    if parent.ValueKind <> JsonValueKind.Object then
        fail $"Expected a JSON object while reading '{name}'."

    let mutable value = Unchecked.defaultof<JsonElement>

    if parent.TryGetProperty(name, &value) then
        value
    else
        fail $"Required manifest field '{name}' is missing."

let private jsonString (parent: JsonElement) (name: string) =
    let value = jsonProperty parent name

    if value.ValueKind <> JsonValueKind.String then
        fail $"Manifest field '{name}' must be a string."

    let result = value.GetString()

    if String.IsNullOrWhiteSpace result then
        fail $"Manifest field '{name}' must not be empty."

    result

let private jsonScalarText (parent: JsonElement) (name: string) =
    let value = jsonProperty parent name

    match value.ValueKind with
    | JsonValueKind.String -> value.GetString()
    | JsonValueKind.Number -> value.GetRawText()
    | _ -> value.GetRawText()

let private jsonObjectProperty (parent: JsonElement) (name: string) =
    let value = jsonProperty parent name

    if value.ValueKind <> JsonValueKind.Object then
        fail $"Manifest field '{name}' must be a JSON object."

    value

let private validateSafeRelativePath label (relativePath: string) =
    require (not (String.IsNullOrWhiteSpace relativePath)) $"{label} must not be empty."
    require (not (Path.IsPathRooted relativePath)) $"{label} must be relative: '{relativePath}'."
    require (not (relativePath.Contains('\\'))) $"{label} contains an unsupported path separator: '{relativePath}'."
    require (not (relativePath.Contains('\000'))) $"{label} contains a null character."

    let parts = relativePath.Split('/')

    require
        (parts.Length > 0
         && parts |> Array.forall (fun part -> not (String.IsNullOrWhiteSpace part) && part <> "." && part <> ".."))
        $"{label} contains an unsafe path segment: '{relativePath}'."

let private isWithinRoot (root: string) (fullPath: string) =
    let rootWithSeparator = Path.TrimEndingDirectorySeparator root + string Path.DirectorySeparatorChar
    let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal
    fullPath.StartsWith(rootWithSeparator, comparison)

let private ensureNoReparsePoints (root: string) (fullPath: string) =
    let comparison = if OperatingSystem.IsWindows() then StringComparison.OrdinalIgnoreCase else StringComparison.Ordinal
    let relative = Path.GetRelativePath(root, fullPath)
    let parts =
        if relative = "." then [||]
        else relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)

    let paths =
        parts
        |> Array.scan (fun parent part -> if String.IsNullOrEmpty parent then part else Path.Combine(parent, part)) root

    for path in paths do
        if Directory.Exists path || File.Exists path then
            let attributes = File.GetAttributes path

            require
                (attributes &&& FileAttributes.ReparsePoint = enum<FileAttributes> 0)
                $"Artifact path traverses a symbolic link or reparse point: '{Path.GetRelativePath(root, path)}'."

    require
        (fullPath = root || isWithinRoot root fullPath)
        $"Artifact path escapes its root: '{Path.GetRelativePath(root, fullPath)}'."

let private artifactPath root relativePath =
    validateSafeRelativePath "Artifact path" relativePath
    let fullPath = Path.GetFullPath(Path.Combine(root, relativePath))
    ensureNoReparsePoints root fullPath
    fullPath

let private sha256File (path: string) =
    use stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
    SHA256.HashData stream |> Convert.ToHexString |> fun hash -> hash.ToLowerInvariant()

let private parsePackageRecord (element: JsonElement) =
    if element.ValueKind <> JsonValueKind.Object then
        fail "Manifest package records must be JSON objects."

    { Id = jsonString element "id"
      Version = jsonString element "version"
      Path = jsonString element "path"
      WorkspacePath = jsonString element "workspace_path"
      Sha256 = jsonString element "sha256"
      SourceRevision = jsonString element "source_revision"
      SourceRevisionFile = jsonString element "source_revision_file" }

let private readPackageRecords (manifest: JsonElement) =
    let packages = jsonProperty manifest "packages"

    if packages.ValueKind <> JsonValueKind.Array then
        fail "Manifest field 'packages' must be an array."

    let records = packages.EnumerateArray() |> Seq.map parsePackageRecord |> Seq.toList

    require
        (records.Length = 2)
        $"Manifest must contain exactly the tool and public API packages; found {records.Length}."

    let ids = records |> List.map (fun record -> record.Id)

    require
        (ids |> List.distinct |> List.length = ids.Length)
        "Manifest contains duplicate package IDs."

    require
        (Set.ofList ids = Set.ofList [ "PipeL4"; "PipeL4.Plugins" ])
        "Manifest package IDs are incomplete or unexpected."

    records |> List.map (fun record -> record.Id, record) |> Map.ofList

let private parseChecksumFile (path: string) =
    let entries = Dictionary<string, string>(StringComparer.Ordinal)
    let linePattern = Regex("^([0-9a-fA-F]{64})  (.+)$", RegexOptions.CultureInvariant)

    let lines =
        try File.ReadAllLines path
        with error -> fail $"Could not read SHA256SUMS: {error.Message}"

    for line in lines do
        let matchResult = linePattern.Match line

        require matchResult.Success $"Malformed SHA256SUMS entry: '{line}'."
        let hash = matchResult.Groups[1].Value.ToLowerInvariant()
        let relativePath = matchResult.Groups[2].Value
        validateSafeRelativePath "SHA256SUMS path" relativePath

        require
            (not (entries.ContainsKey relativePath))
            $"Duplicate SHA256SUMS entry: '{relativePath}'."

        entries.Add(relativePath, hash)

    entries

let private nugetVersioning =
    Assembly.LoadFrom(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "NuGet.Versioning.dll")))

let private nugetRangeType = nugetVersioning.GetType("NuGet.Versioning.VersionRange", true)
let private nugetVersionType = nugetVersioning.GetType("NuGet.Versioning.NuGetVersion", true)
let private nugetRangeTryParse = nugetRangeType.GetMethod("TryParse", [| typeof<string>; nugetRangeType.MakeByRefType() |])
let private nugetVersionParse = nugetVersionType.GetMethod("Parse", [| typeof<string> |])
let private nugetRangeSatisfies = nugetRangeType.GetMethod("Satisfies", [| nugetVersionType |])
let private expectedMinimumFSharpCore = nugetVersionParse.Invoke(null, [| box "10.0.100" |])

let private parseFSharpCoreRange (value: string) =
    let parsed = [| box value; null |]

    if not (nugetRangeTryParse.Invoke(null, parsed) :?> bool) || isNull parsed[1] then
        fail $"Invalid FSharp.Core dependency NuGet version range '{value}'."

    parsed[1]

let private nugetRangeIncludes (range: obj) (version: obj) =
    nugetRangeSatisfies.Invoke(range, [| version |]) :?> bool

let private validateFSharpCorePolicy (metadata: XElement) packageName =
    let dependencies =
        metadata.Descendants()
        |> Seq.filter (fun element -> localName element = "dependency")
        |> Seq.toArray

    require (dependencies.Length > 0) $"{packageName} declares no dependencies."

    require
        (dependencies
         |> Array.forall (fun dependency ->
             let id = dependency.Attribute(XName.Get "id") |> Option.ofObj |> Option.map (fun value -> value.Value)
             id |> Option.exists (fun value -> value.Equals("FSharp.Core", StringComparison.OrdinalIgnoreCase))))
        $"{packageName} declares a dependency other than FSharp.Core."

    for dependency in dependencies do
        let value =
            dependency.Attribute(XName.Get "version")
            |> Option.ofObj
            |> Option.map (fun attribute -> attribute.Value)
            |> Option.defaultValue ""

        let range = parseFSharpCoreRange value
        let lowerBound = nugetRangeType.GetProperty("MinVersion").GetValue range
        let lowerInclusive = nugetRangeType.GetProperty("IsMinInclusive").GetValue range :?> bool

        require
            (not (isNull lowerBound)
             && lowerBound.Equals expectedMinimumFSharpCore
             && lowerInclusive
             && nugetRangeIncludes range expectedMinimumFSharpCore)
            $"FSharp.Core dependency must have inclusive minimum 10.0.100 and include that version; found '{value}'."

let private readNuspec (packagePath: string) =
    try
        use archive = ZipFile.OpenRead packagePath
        let names = archive.Entries |> Seq.map (fun entry -> entry.FullName) |> Seq.toArray
        let nuspecNames = names |> Array.filter (fun name -> name.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))

        require
            (nuspecNames.Length = 1)
            $"Expected one nuspec in {Path.GetFileName packagePath}, found {nuspecNames.Length}."

        for entry in archive.Entries do
            use input = entry.Open()
            input.CopyTo Stream.Null

        let nuspecEntry = archive.GetEntry(nuspecNames[0])
        use nuspecStream = nuspecEntry.Open()
        let document = XDocument.Load nuspecStream

        let metadata =
            document.Descendants()
            |> Seq.tryFind (fun element -> localName element = "metadata")
            |> Option.defaultWith (fun () -> fail $"Package {Path.GetFileName packagePath} has no nuspec metadata.")

        let field name =
            metadata.Elements()
            |> Seq.tryFind (fun element -> localName element = name)
            |> Option.map (fun element -> element.Value.Trim())
            |> Option.filter (String.IsNullOrWhiteSpace >> not)
            |> Option.defaultWith (fun () -> fail $"Package {Path.GetFileName packagePath} has no nuspec '{name}'.")

        let packageId = field "id"
        let version = field "version"
        let repository =
            metadata.Elements()
            |> Seq.tryFind (fun element -> localName element = "repository")
            |> Option.defaultWith (fun () -> fail $"Package {packageId} has no nuspec repository metadata.")

        let commit =
            repository.Attribute(XName.Get "commit")
            |> Option.ofObj
            |> Option.map (fun attribute -> attribute.Value)
            |> Option.defaultValue ""

        packageId, version, commit, metadata
    with
    | ValidationException _ -> reraise ()
    | :? InvalidDataException as error -> fail $"Could not read package {Path.GetFileName packagePath}: {error.Message}"
    | :? IOException as error -> fail $"Could not read package {Path.GetFileName packagePath}: {error.Message}"
    | :? System.Xml.XmlException as error -> fail $"Could not parse package {Path.GetFileName packagePath} nuspec: {error.Message}"

let private parsePositiveInt64 (label: string) (value: string) =
    match Int64.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture) with
    | true, number when number > 0L -> number
    | _ -> fail $"Invalid expected workflow {label} '{value}'."

let private checkWorkflowIdentity (expectations: ArtifactExpectations) =
    require
        (sdkRoles.ContainsKey expectations.ConsumerSdk)
        $"Unsupported consumer SDK {expectations.ConsumerSdk}."

    let role =
        sdkRoles
        |> Map.tryFind expectations.ProducerSdk
        |> Option.defaultWith (fun () -> fail $"Unsupported producer SDK {expectations.ProducerSdk}.")

    require
        (expectations.TestedSha.Length = 40
         && expectations.TestedSha |> Seq.forall (fun character -> Uri.IsHexDigit character))
        "Invalid tested checkout SHA."

    let runId = parsePositiveInt64 "run ID" expectations.RunId
    let producerAttempt = parsePositiveInt64 "producer attempt" expectations.ProducerAttempt
    role, runId, producerAttempt

let private producerArtifactName role producerSdk testedSha producerAttempt =
    $"PipeL4-{role}-sdk-{producerSdk}-source-{testedSha}-attempt-{producerAttempt}"

let private selectProducerArtifactName (artifactName: string) (producerAttempt: string) (producerSdk: string) (testedSha: string) runId =
    require (sdkRoles.ContainsKey producerSdk) $"Unsupported producer SDK {producerSdk}."
    require (not (String.IsNullOrWhiteSpace artifactName)) "Producer artifact name output is missing or empty."
    require
        (testedSha.Length = 40 && testedSha |> Seq.forall Uri.IsHexDigit)
        "Invalid tested checkout SHA."
    parsePositiveInt64 "run ID" runId |> ignore
    let parsedAttempt = parsePositiveInt64 "producer attempt" producerAttempt

    let role = sdkRoles[producerSdk]
    let expectedName = producerArtifactName role producerSdk testedSha producerAttempt

    require
        (producerAttempt = parsedAttempt.ToString(CultureInfo.InvariantCulture)
         && artifactName = expectedName)
        $"Producer artifact output '{artifactName}' does not match expected identity '{expectedName}'."

    artifactName

let private validateArtifact (expectations: ArtifactExpectations) =
    let expectedRole, expectedRunId, expectedProducerAttempt = checkWorkflowIdentity expectations
    let artifactRoot = Path.GetFullPath expectations.ArtifactRoot

    require (Directory.Exists artifactRoot) $"Downloaded producer artifact is missing: {artifactRoot}."
    ensureNoReparsePoints artifactRoot artifactRoot

    let manifestPath = Path.Combine(artifactRoot, "manifest.json")
    let sumsPath = Path.Combine(artifactRoot, "SHA256SUMS")

    require (File.Exists manifestPath) "Artifact is missing manifest.json."
    require (File.Exists sumsPath) "Artifact is missing SHA256SUMS."
    ensureNoReparsePoints artifactRoot manifestPath
    ensureNoReparsePoints artifactRoot sumsPath

    let manifestDocument =
        try JsonDocument.Parse(File.ReadAllText manifestPath)
        with error -> fail $"Could not parse artifact manifest: {error.Message}"

    use manifestDocument = manifestDocument
    let manifest = manifestDocument.RootElement

    require (manifest.ValueKind = JsonValueKind.Object) "Artifact manifest must be a JSON object."

    let formatVersion = jsonProperty manifest "format_version"

    require
        (formatVersion.ValueKind = JsonValueKind.Number && formatVersion.GetInt32() = 1)
        "Unsupported artifact manifest format."

    let producer = jsonObjectProperty manifest "producer"
    let actualProducerSdk = jsonString producer "sdk"
    let actualProducerRole = jsonString producer "role"

    require
        (actualProducerSdk = expectations.ProducerSdk)
        $"Wrong producer SDK: expected {expectations.ProducerSdk}, found {actualProducerSdk}."

    require
        (actualProducerRole = expectedRole)
        $"Wrong producer role for SDK {expectations.ProducerSdk}: expected {expectedRole}, found {actualProducerRole}."

    let actualRunId = jsonScalarText manifest "run_id"
    let actualRunAttempt = jsonScalarText manifest "run_attempt"

    require
        (actualRunId = expectations.RunId)
        $"Wrong workflow run ID: expected {expectations.RunId}, found {actualRunId}."

    require
        (actualRunAttempt = expectedProducerAttempt.ToString(CultureInfo.InvariantCulture))
        $"Wrong producer artifact attempt: expected {expectedProducerAttempt}, found {actualRunAttempt}."

    require
        (jsonString manifest "tested_checkout_sha" = expectations.TestedSha)
        "Artifact checkout SHA does not match the tested checkout."

    let packageRecords = readPackageRecords manifest
    let checksums = parseChecksumFile sumsPath

    let allPackages =
        try Directory.EnumerateFiles(artifactRoot, "*.nupkg", SearchOption.AllDirectories) |> Seq.toArray
        with error -> fail $"Could not enumerate artifact packages: {error.Message}"

    require
        (allPackages.Length = 2)
        $"Expected exactly two nupkg files in the artifact, found {allPackages.Length}."

    allPackages |> Array.iter (ensureNoReparsePoints artifactRoot)

    let mutable selectedVersion: string option = None
    let selectedPackages = Dictionary<string, string>(StringComparer.Ordinal)

    for packageId, relativeDirectory in expectedPackageDirectories do
        let folder = artifactPath artifactRoot relativeDirectory

        require (Directory.Exists folder) $"Expected package directory is missing: {relativeDirectory}."

        let packageFiles = Directory.EnumerateFiles(folder, "*.nupkg", SearchOption.TopDirectoryOnly) |> Seq.toArray

        require
            (packageFiles.Length = 1)
            $"Expected exactly one {packageId} package, found {packageFiles.Length}."

        let packagePath = packageFiles[0]
        ensureNoReparsePoints artifactRoot packagePath
        let record = packageRecords[packageId]

        require
            (selectedVersion |> Option.forall ((=) record.Version))
            "Tool and public API package versions do not match."

        selectedVersion <- Some record.Version
        let expectedName = $"{packageId}.{record.Version}.nupkg"

        require
            (Path.GetFileName packagePath = expectedName)
            $"Unexpected package filename {Path.GetFileName packagePath}; expected {expectedName}."

        let expectedRelativePath = $"{relativeDirectory}/{expectedName}"
        validateSafeRelativePath "Manifest package path" record.Path

        require
            (record.Path = expectedRelativePath)
            $"Manifest path does not select {expectedRelativePath}."

        validateSafeRelativePath "Manifest workspace path" record.WorkspacePath
        let expectedWorkspacePath = $"artifacts/producer-sdk-{expectations.ProducerSdk}-{expectedRole}/{expectedRelativePath}"

        require
            (record.WorkspacePath = expectedWorkspacePath)
            $"Manifest workspace path does not identify the selected producer artifact: {record.WorkspacePath}."

        let expectedRevisionPath = $"{relativeDirectory}/source-revision.txt"
        validateSafeRelativePath "Manifest source-revision path" record.SourceRevisionFile

        require
            (record.SourceRevisionFile = expectedRevisionPath)
            $"Unexpected source-revision metadata path for {packageId}."

        require
            (record.SourceRevision = expectations.TestedSha)
            $"Manifest source revision for {packageId} is incorrect."

        let revisionPath = artifactPath artifactRoot expectedRevisionPath

        require (File.Exists revisionPath) $"Source revision file for {packageId} is missing."
        ensureNoReparsePoints artifactRoot revisionPath
        let revision = File.ReadAllText(revisionPath).Trim()

        require
            (revision = expectations.TestedSha)
            $"Source revision file for {packageId} is incorrect."

        let actualPackageHash = sha256File packagePath

        require
            (record.Sha256 = actualPackageHash)
            $"Manifest package SHA-256 does not match {Path.GetFileName packagePath}."

        let actualId, actualVersion, commit, metadata = readNuspec packagePath

        require
            (actualId = packageId && actualVersion = record.Version)
            $"Nuspec identity for {Path.GetFileName packagePath} does not match its manifest record."

        require
            (commit = expectations.TestedSha)
            $"Nuspec repository commit for {packageId} does not match the tested checkout."

        if packageId = "PipeL4.Plugins" then
            validateFSharpCorePolicy metadata packageId

        selectedPackages.Add(packageId, packagePath)

        for relativePath in [ expectedRelativePath; expectedRevisionPath ] do
            let expectedHash = if relativePath = expectedRelativePath then record.Sha256 else sha256File (artifactPath artifactRoot relativePath)
            let mutable checksum = ""

            require
                (checksums.TryGetValue(relativePath, &checksum) && checksum = expectedHash)
                $"SHA256SUMS is missing or mismatches {relativePath}."

    let version = selectedVersion |> Option.defaultWith (fun () -> fail "Manifest package versions are missing.")
    let expectedChecksums = expectedChecksumPaths version

    require
        (Set.ofSeq checksums.Keys = expectedChecksums)
        "SHA256SUMS contains missing or unexpected files."

    { ToolPackagePath = Path.GetFullPath selectedPackages["PipeL4"]
      ApiPackagePath = Path.GetFullPath selectedPackages["PipeL4.Plugins"]
      PackageVersion = version }

let private validateJobResults (results: Map<string, string>) =
    let failures =
        aggregateJobs
        |> List.choose (fun job ->
            match Map.tryFind job results with
            | Some "success" -> None
            | Some result -> Some $"{job}={result}"
            | None -> Some $"{job}=<missing>")

    if not failures.IsEmpty then
        let joinedFailures = String.Join(", ", failures)
        fail $"All distribution jobs must succeed; {joinedFailures}."

let private appendGitHubOutput path (artifact: ValidatedArtifact) =
    let values =
        [ "tool_package_path", artifact.ToolPackagePath
          "api_package_path", artifact.ApiPackagePath
          "package_version", artifact.PackageVersion ]

    for _, value in values do
        require
            (not (value.Contains('\n') || value.Contains('\r')))
            "Cannot write a multiline value to GITHUB_OUTPUT."

    let contents = values |> List.map (fun (key, value) -> $"{key}={value}{Environment.NewLine}") |> String.concat ""

    try File.AppendAllText(path, contents)
    with error -> fail $"Could not append validated package outputs to '{path}': {error.Message}"

let private appendProducerIdentityOutputs path artifactName producerAttempt =
    let values = [ "artifact_name", artifactName; "producer_attempt", producerAttempt ]

    for _, value in values do
        require
            (not (String.IsNullOrWhiteSpace value) && not (value.Contains('\n') || value.Contains('\r')))
            "Cannot write an empty or multiline producer identity output."

    let contents = values |> List.map (fun (key, value) -> $"{key}={value}{Environment.NewLine}") |> String.concat ""

    try File.AppendAllText(path, contents)
    with error -> fail $"Could not append producer identity outputs to '{path}': {error.Message}"

let private setJsonString (target: JsonObject) (name: string) (value: string) =
    target[name] <- JsonValue.Create value

let private writeFixturePackage (packagePath: string) (packageId: string) (version: string) (commit: string) (isApi: bool) =
    let packageDirectory = Path.GetDirectoryName packagePath
    Directory.CreateDirectory packageDirectory |> ignore
    let dependencyText =
        if isApi then "<dependencies><group targetFramework=\"net10.0\"><dependency id=\"FSharp.Core\" version=\"[10.0.100, )\" /></group></dependencies>"
        else ""

    let nuspec =
        $"<package><metadata><id>{packageId}</id><version>{version}</version>{dependencyText}<repository type=\"git\" url=\"https://github.com/jbbfett/PipeL4.git\" commit=\"{commit}\" /></metadata></package>"

    use archive = ZipFile.Open(packagePath, ZipArchiveMode.Create)
    do
        let entry = archive.CreateEntry($"{packageId}.nuspec", CompressionLevel.Optimal)
        use stream = entry.Open()
        use writer = new StreamWriter(stream)
        writer.Write nuspec

    do
        let entry = archive.CreateEntry($"lib/net10.0/{packageId}.dll", CompressionLevel.Optimal)
        use stream = entry.Open()
        let payload = Encoding.UTF8.GetBytes($"readable fixture payload for {packageId}")
        stream.Write(payload, 0, payload.Length)

let private writeFixtureManifest (root: string) (testedSha: string) (producerAttempt: string) =
    let manifest = JsonObject()
    manifest["format_version"] <- JsonValue.Create 1

    let producer = JsonObject()
    setJsonString producer "sdk" "10.0.115"
    setJsonString producer "role" "canonical"
    manifest["producer"] <- producer
    manifest["run_id"] <- JsonValue.Create 42
    manifest["run_attempt"] <- JsonValue.Create (Int32.Parse(producerAttempt, CultureInfo.InvariantCulture))
    setJsonString manifest "tested_checkout_sha" testedSha

    let packages = JsonArray()

    for packageId, directory in expectedPackageDirectories do
        let relativeDirectory = directory
        let packageFile = $"{packageId}.1.2.3.nupkg"
        let packagePath = Path.Combine(root, relativeDirectory, packageFile)
        let record = JsonObject()
        setJsonString record "id" packageId
        setJsonString record "version" "1.2.3"
        setJsonString record "path" $"{relativeDirectory}/{packageFile}"
        setJsonString record "workspace_path" $"artifacts/producer-sdk-10.0.115-canonical/{relativeDirectory}/{packageFile}"
        setJsonString record "sha256" (sha256File packagePath)
        setJsonString record "source_revision" testedSha
        setJsonString record "source_revision_file" $"{relativeDirectory}/source-revision.txt"
        packages.Add record

    manifest["packages"] <- packages
    File.WriteAllText(Path.Combine(root, "manifest.json"), manifest.ToJsonString())

let private writeFixtureChecksums (root: string) =
    let packageVersion = "1.2.3"
    let paths = expectedChecksumPaths packageVersion |> Set.toList |> List.sort
    let lines = paths |> List.map (fun relative -> $"{sha256File (artifactPath root relative)}  {relative}{Environment.NewLine}")
    File.WriteAllText(Path.Combine(root, "SHA256SUMS"), String.concat "" lines)

let private makeTestArtifact (root: string) (producerAttempt: string) =
    let testedSha = String.replicate 40 "a"

    for packageId, relativeDirectory in expectedPackageDirectories do
        let packagePath = Path.Combine(root, relativeDirectory, $"{packageId}.1.2.3.nupkg")
        writeFixturePackage packagePath packageId "1.2.3" testedSha (packageId = "PipeL4.Plugins")
        let revisionPath = Path.Combine(root, relativeDirectory, "source-revision.txt")
        Directory.CreateDirectory(Path.GetDirectoryName revisionPath) |> ignore
        File.WriteAllText(revisionPath, testedSha + Environment.NewLine)

    writeFixtureManifest root testedSha producerAttempt
    writeFixtureChecksums root

    { ArtifactRoot = root
      ConsumerSdk = "10.0.115"
      ProducerSdk = "10.0.115"
      TestedSha = testedSha
      RunId = "42"
      ProducerAttempt = producerAttempt }

let rec private copyDirectory (source: string) (destination: string) =
    Directory.CreateDirectory destination |> ignore

    for file in Directory.EnumerateFiles(source, "*", SearchOption.TopDirectoryOnly) do
        File.Copy(file, Path.Combine(destination, Path.GetFileName file))

    for directory in Directory.EnumerateDirectories(source, "*", SearchOption.TopDirectoryOnly) do
        copyDirectory directory (Path.Combine(destination, Path.GetFileName directory))

let private updateManifest (root: string) (edit: JsonObject -> unit) =
    let manifestPath = Path.Combine(root, "manifest.json")
    let manifest = JsonNode.Parse(File.ReadAllText manifestPath)

    if isNull manifest then fail "Test fixture manifest unexpectedly parsed as null."
    edit (manifest.AsObject())
    File.WriteAllText(manifestPath, manifest.ToJsonString())

let private refreshFixtureIntegrity (root: string) =
    let manifestPath = Path.Combine(root, "manifest.json")
    let manifest = JsonNode.Parse(File.ReadAllText manifestPath)

    if isNull manifest then fail "Test fixture manifest unexpectedly parsed as null."

    let packages = manifest["packages"].AsArray()

    for record in packages do
        if isNull record then fail "Test fixture package record unexpectedly parsed as null."
        let packageId = record["id"].GetValue<string>()
        let relativeDirectory = expectedPackageDirectories |> List.find (fun (id, _) -> id = packageId) |> snd
        let packageVersion = record["version"].GetValue<string>()
        let packagePath = artifactPath root $"{relativeDirectory}/{packageId}.{packageVersion}.nupkg"
        setJsonString (record.AsObject()) "sha256" (sha256File packagePath)

    File.WriteAllText(manifestPath, manifest.ToJsonString())
    writeFixtureChecksums root

let private rewritePackage (packagePath: string) (mutateNuspec: XDocument -> unit) (mutateEntry: string -> byte[] -> byte[]) =
    let temporary = packagePath + ".rewritten"

    let rewrite () =
        use source = ZipFile.OpenRead packagePath
        use output = ZipFile.Open(temporary, ZipArchiveMode.Create)

        for entry in source.Entries do
            let newEntry = output.CreateEntry(entry.FullName, CompressionLevel.Optimal)
            use outputStream = newEntry.Open()
            use input = entry.Open()
            use buffer = new MemoryStream()
            input.CopyTo buffer
            let mutable contents = buffer.ToArray()

            if entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase) then
                use nuspecStream = new MemoryStream(contents)
                let document = XDocument.Load nuspecStream
                mutateNuspec document
                document.Save(outputStream, SaveOptions.DisableFormatting)
            else
                contents <- mutateEntry entry.FullName contents
                outputStream.Write(contents, 0, contents.Length)

    rewrite ()
    File.Move(temporary, packagePath, true)

let private editPackageNuspec (packagePath: string) (mutation: XDocument -> unit) =
    rewritePackage packagePath mutation (fun _ contents -> contents)

let private editPackagePayload (packagePath: string) (payloadPath: string) (mutation: byte[] -> byte[]) =
    rewritePackage packagePath ignore (fun entryName contents ->
        if entryName = payloadPath then mutation contents else contents)

let private expectArtifactRejection (label: string) (sourceRoot: string) (mutate: string -> unit) (expectations: ArtifactExpectations) =
    let target = Path.Combine(Path.GetDirectoryName sourceRoot, label)
    copyDirectory sourceRoot target
    mutate target
    let mutable wasRejected = false

    try
        validateArtifact { expectations with ArtifactRoot = target } |> ignore
    with ValidationException _ ->
        wasRejected <- true

    if not wasRejected then
        fail $"Negative artifact control was accepted: {label}."

    printfn "PASS artifact rejection control: %s." label

let private expectArtifactDiagnostic (label: string) (sourceRoot: string) (mutate: string -> unit) (expectations: ArtifactExpectations) (expectedDiagnostic: string) =
    let target = Path.Combine(Path.GetDirectoryName sourceRoot, label)
    copyDirectory sourceRoot target
    mutate target

    let diagnostic =
        try
            validateArtifact { expectations with ArtifactRoot = target } |> ignore
            fail $"Negative artifact control was accepted: {label}."
        with ValidationException message -> message

    require
        (diagnostic.Contains(expectedDiagnostic, StringComparison.Ordinal))
        $"{label}: expected validation diagnostic '{expectedDiagnostic}', got '{diagnostic}'."

    printfn "PASS artifact validation diagnostic: %s." label

let private runCli (arguments: string list) =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- Environment.CurrentDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    [ "fsi"; "--exec"; Path.Combine(__SOURCE_DIRECTORY__, "distribution_validation.fsx") ] @ arguments
    |> List.iter startInfo.ArgumentList.Add

    use child = new Process(StartInfo = startInfo)

    if not (child.Start()) then
        fail "Could not start dotnet fsi for the distribution CLI self-test."

    let stdout = child.StandardOutput.ReadToEndAsync()
    let stderr = child.StandardError.ReadToEndAsync()

    if not (child.WaitForExit(120000)) then
        child.Kill(true)
        fail "Distribution CLI self-test process timed out."

    { ExitCode = child.ExitCode
      StandardOutput = stdout.GetAwaiter().GetResult()
      StandardError = stderr.GetAwaiter().GetResult() }

let private requireCliResult (label: string) (expectedExit: int) (expectedText: string) (result: CliResult) =
    require
        (result.ExitCode = expectedExit)
        $"{label}: expected exit {expectedExit}, got {result.ExitCode}.\n{result.StandardOutput}{result.StandardError}"

    require
        ((result.StandardOutput + result.StandardError).Contains(expectedText, StringComparison.Ordinal))
        $"{label}: output did not include '{expectedText}'.\n{result.StandardOutput}{result.StandardError}"

let private validateFSharpCoreRangeFixture (range: string) =
    let metadata =
        XElement(
            XName.Get "metadata",
            XElement(
                XName.Get "dependencies",
                XElement(
                    XName.Get "dependency",
                    XAttribute(XName.Get "id", "FSharp.Core"),
                    XAttribute(XName.Get "version", range)
                )
            )
        )

    validateFSharpCorePolicy metadata "PipeL4.Plugins"

let private runNuGetRangeRegressionTests () =
    let accepted =
        [ "[10.0.100, )"
          "[10.0.100]"
          "10.0.100"
          "[10.0.100, 10.1.0]" ]

    let rejected =
        [ "[10.0.100-beta, )"
          "(10.0.100, )"
          "[10.0.100,10.0.100-beta]"
          "[10.0.99, )"
          "[10.0.100,10.0.99]"
          "[10.0.100, )junk" ]

    for range in accepted do
        try validateFSharpCoreRangeFixture range
        with ValidationException message -> fail $"NuGet range regression rejected legal '{range}': {message}"

        printfn "PASS accepted FSharp.Core NuGet range: %s." range

    for range in rejected do
        let mutable wasRejected = false

        try validateFSharpCoreRangeFixture range
        with ValidationException _ -> wasRejected <- true

        require wasRejected $"NuGet range regression accepted invalid or incompatible '{range}'."
        printfn "PASS rejected FSharp.Core NuGet range: %s." range

let private runSelfTests () =
    let testRoot = Path.Combine(Path.GetTempPath(), $"PipeL4 distribution validation {Guid.NewGuid():N}")
    Directory.CreateDirectory testRoot |> ignore

    try
        let validRoot = Path.Combine(testRoot, "valid")
        runNuGetRangeRegressionTests ()

        let expectations = makeTestArtifact validRoot "2"
        let valid = validateArtifact expectations

        require
            (File.Exists valid.ToolPackagePath
             && File.Exists valid.ApiPackagePath
             && valid.PackageVersion = "1.2.3")
            "Positive artifact fixture did not return the expected selected packages."

        printfn "PASS positive producer artifact fixture."

        let rerunRoot = Path.Combine(testRoot, "producer-attempt-1")
        let rerunExpectations = makeTestArtifact rerunRoot "1"
        let consumerAttempt = "2"
        validateArtifact rerunExpectations |> ignore
        let producerName = producerArtifactName "canonical" "10.0.115" rerunExpectations.TestedSha "1"
        require
            (consumerAttempt = "2"
             && selectProducerArtifactName producerName "1" "10.0.115" rerunExpectations.TestedSha rerunExpectations.RunId = producerName)
            "Consumer attempt 2 did not select the matching producer attempt 1 artifact."

        printfn "PASS consumer attempt 2 selects the successful producer attempt 1 artifact."

        let outputFile = Path.Combine(testRoot, "github-output.txt")
        let artifactCli =
            runCli
                [ "artifact"
                  "--artifact-root"
                  validRoot
                  "--consumer-sdk"
                  expectations.ConsumerSdk
                  "--producer-sdk"
                  expectations.ProducerSdk
                  "--tested-sha"
                  expectations.TestedSha
                  "--run-id"
                  expectations.RunId
                  "--producer-attempt"
                  expectations.ProducerAttempt
                  "--github-output"
                  outputFile ]

        requireCliResult "valid artifact CLI invocation" 0 "Verified producer SDK" artifactCli
        let output = File.ReadAllText outputFile

        require
            (output.Contains($"tool_package_path={valid.ToolPackagePath}", StringComparison.Ordinal)
             && output.Contains($"api_package_path={valid.ApiPackagePath}", StringComparison.Ordinal)
             && output.Contains("package_version=1.2.3", StringComparison.Ordinal))
            "Artifact CLI did not emit all three expected GitHub outputs."

        printfn "PASS artifact CLI accepts a valid fixture and writes expected outputs."
        requireCliResult "artifact CLI usage" 2 "Usage error" (runCli [ "artifact" ])
        requireCliResult "artifact CLI validation failure" 1 "Downloaded producer artifact is missing" (runCli [ "artifact"; "--artifact-root"; Path.Combine(testRoot, "missing"); "--consumer-sdk"; expectations.ConsumerSdk; "--producer-sdk"; expectations.ProducerSdk; "--tested-sha"; expectations.TestedSha; "--run-id"; expectations.RunId; "--producer-attempt"; expectations.ProducerAttempt ])

        let selectionOutput = Path.Combine(testRoot, "producer-output.txt")
        let selectionArguments =
            [ "select-artifact"
              "--artifact-name"
              producerName
              "--producer-attempt"
              "1"
              "--producer-sdk"
              "10.0.115"
              "--tested-sha"
              rerunExpectations.TestedSha
              "--run-id"
              rerunExpectations.RunId
              "--github-output"
              selectionOutput ]

        requireCliResult "producer output selection" 0 "producer attempt 1" (runCli selectionArguments)
        let selectionValues = File.ReadAllText selectionOutput
        require
            (selectionValues.Contains($"artifact_name={producerName}", StringComparison.Ordinal)
             && selectionValues.Contains("producer_attempt=1", StringComparison.Ordinal))
            "Producer identity selection did not emit both validated outputs."

        requireCliResult
            "missing producer artifact name"
            2
            "Required select-artifact option '--artifact-name' is missing"
            (runCli
                [ "select-artifact"
                  "--producer-attempt"
                  "1"
                  "--producer-sdk"
                  "10.0.115"
                  "--tested-sha"
                  rerunExpectations.TestedSha
                  "--run-id"
                  rerunExpectations.RunId
                  "--github-output"
                  selectionOutput ])

        let wrongNameArguments = selectionArguments |> List.map (fun value -> if value = producerName then producerName + "-wrong" else value)
        requireCliResult "wrong producer artifact name" 1 "does not match expected identity" (runCli wrongNameArguments)

        let wrongProducerName = producerArtifactName "compatibility" "10.0.404" rerunExpectations.TestedSha "1"
        let wrongProducerArguments = selectionArguments |> List.map (fun value -> if value = producerName then wrongProducerName else value)
        requireCliResult "wrong producer role artifact name" 1 "does not match expected identity" (runCli wrongProducerArguments)

        let wrongAttemptArguments = selectionArguments |> List.map (fun value -> if value = "1" then "2" else value)
        requireCliResult "inconsistent producer attempt output" 1 "does not match expected identity" (runCli wrongAttemptArguments)

        let emptyOutputArguments = selectionArguments |> List.map (fun value -> if value = producerName then "" else value)
        requireCliResult "empty producer artifact name" 1 "missing or empty" (runCli emptyOutputArguments)

        let emptyAttemptArguments = selectionArguments |> List.map (fun value -> if value = "1" then "" else value)
        requireCliResult "empty producer attempt output" 1 "Invalid expected workflow producer attempt" (runCli emptyAttemptArguments)

        printfn "PASS producer output selection rejects missing, empty, wrong, and inconsistent outputs."

        let aggregateArguments =
            aggregateJobs
            |> List.collect (fun job -> [ "--" + job.Replace('_', '-'); "success" ])

        requireCliResult "aggregate CLI success" 0 "consume_404_from_404: success" (runCli ([ "aggregate" ] @ aggregateArguments))

        let failingAggregateArguments =
            aggregateArguments
            |> List.chunkBySize 2
            |> List.map (fun pair -> if pair[0] = "--consume-404-from-404" then [ pair[0]; "failure" ] else pair)
            |> List.concat

        requireCliResult
            "aggregate CLI failure"
            1
            "consume_404_from_404=failure"
            (runCli ([ "aggregate" ] @ failingAggregateArguments))

        printfn "PASS artifact and aggregate CLI success and failure exit codes."

        expectArtifactRejection "wrong-producer-sdk" validRoot (fun root ->
            updateManifest root (fun manifest -> manifest["producer"].AsObject()["sdk"] <- JsonValue.Create "10.0.404")) expectations

        expectArtifactRejection "wrong-producer-role" validRoot (fun root ->
            updateManifest root (fun manifest -> manifest["producer"].AsObject()["role"] <- JsonValue.Create "compatibility")) expectations

        expectArtifactRejection "wrong-checkout-sha" validRoot ignore { expectations with TestedSha = String.replicate 40 "b" }

        expectArtifactRejection "wrong-source-revision" validRoot (fun root ->
            let revision = Path.Combine(root, "packages/PipeL4/source-revision.txt")
            File.WriteAllText(revision, String.replicate 40 "b")
            writeFixtureChecksums root) expectations

        expectArtifactRejection "wrong-run-id" validRoot (fun root ->
            updateManifest root (fun manifest -> manifest["run_id"] <- JsonValue.Create 43)) expectations

        expectArtifactRejection "wrong-attempt" validRoot (fun root ->
            updateManifest root (fun manifest -> manifest["run_attempt"] <- JsonValue.Create 3)) expectations

        expectArtifactRejection
            "compatibility-job-cannot-select-canonical"
            validRoot
            ignore
            { expectations with ProducerSdk = "10.0.404" }

        expectArtifactDiagnostic
            "package-payload-hash-mismatch"
            validRoot
            (fun root ->
                editPackagePayload
                    (Path.Combine(root, "packages/PipeL4/PipeL4.1.2.3.nupkg"))
                    "lib/net10.0/PipeL4.dll"
                    (fun _ -> Encoding.UTF8.GetBytes("changed readable payload")))
            expectations
            "Manifest package SHA-256 does not match PipeL4.1.2.3.nupkg"

        expectArtifactDiagnostic
            "wrong-nuspec-repository-commit"
            validRoot
            (fun root ->
                let packagePath = Path.Combine(root, "packages/PipeL4/PipeL4.1.2.3.nupkg")
                editPackageNuspec packagePath (fun document ->
                    let repository = document.Descendants() |> Seq.find (fun element -> localName element = "repository")
                    repository.SetAttributeValue(XName.Get "commit", String.replicate 40 "b"))
                refreshFixtureIntegrity root)
            expectations
            "Nuspec repository commit for PipeL4 does not match the tested checkout."

        expectArtifactRejection "missing-package" validRoot (fun root ->
            File.Delete(Path.Combine(root, "packages/PipeL4/PipeL4.1.2.3.nupkg"))) expectations

        expectArtifactRejection "extra-package" validRoot (fun root ->
            File.WriteAllText(Path.Combine(root, "unexpected.nupkg"), "unexpected")) expectations

        expectArtifactRejection "wrong-package-id" validRoot (fun root ->
            let packagePath = Path.Combine(root, "packages/PipeL4/PipeL4.1.2.3.nupkg")
            editPackageNuspec packagePath (fun document ->
                let id = document.Descendants() |> Seq.find (fun element -> localName element = "id")
                id.Value <- "PipeL4.Wrong")
            refreshFixtureIntegrity root) expectations

        expectArtifactRejection "wrong-package-version" validRoot (fun root ->
            let packagePath = Path.Combine(root, "packages/PipeL4/PipeL4.1.2.3.nupkg")
            editPackageNuspec packagePath (fun document ->
                let version = document.Descendants() |> Seq.find (fun element -> localName element = "version")
                version.Value <- "9.9.9")
            refreshFixtureIntegrity root) expectations

        expectArtifactRejection "wrong-manifest-version" validRoot (fun root ->
            updateManifest root (fun manifest ->
                let package = manifest["packages"].AsArray().Item(0).AsObject()
                package["version"] <- JsonValue.Create "9.9.9") ) expectations

        expectArtifactRejection "unsafe-manifest-path" validRoot (fun root ->
            updateManifest root (fun manifest ->
                let package = manifest["packages"].AsArray().Item(0).AsObject()
                package["path"] <- JsonValue.Create "../PipeL4.1.2.3.nupkg") ) expectations

        expectArtifactRejection "malformed-manifest" validRoot (fun root ->
            File.WriteAllText(Path.Combine(root, "manifest.json"), "{")) expectations

        expectArtifactRejection "incomplete-manifest" validRoot (fun root ->
            updateManifest root (fun manifest -> manifest.AsObject().Remove("run_attempt") |> ignore)) expectations

        expectArtifactRejection "wrong-fsharp-core-policy" validRoot (fun root ->
            let packagePath = Path.Combine(root, "packages/PipeL4.Plugins/PipeL4.Plugins.1.2.3.nupkg")
            editPackageNuspec packagePath (fun document ->
                let dependency = document.Descendants() |> Seq.find (fun element -> localName element = "dependency")
                dependency.SetAttributeValue(XName.Get "version", "[10.0.99, )"))
            refreshFixtureIntegrity root) expectations

        let allSuccess = aggregateJobs |> List.map (fun job -> job, "success") |> Map.ofList
        validateJobResults allSuccess
        printfn "PASS aggregate accepts all six successful jobs."

        for job in aggregateJobs do
            for result in [ "failure"; "cancelled"; "skipped"; "unexpected" ] do
                let mutated = allSuccess |> Map.add job result
                let mutable rejected = false

                try validateJobResults mutated with ValidationException _ -> rejected <- true

                require rejected $"Aggregate accepted {job}={result}."

            printfn "PASS aggregate rejects every result for %s independently." job

        printfn "PASS distribution artifact and aggregate self-tests."
    finally
        if Directory.Exists testRoot then
            Directory.Delete(testRoot, true)

let private parseNamedOptions (command: string) (required: string list) (optional: string list) (args: string list) =
    let allowed = Set.ofList (required @ optional)

    let rec loop (remaining: string list) (values: Map<string, string>) =
        match remaining with
        | [] -> values
        | optionName :: value :: tail when optionName.StartsWith("--", StringComparison.Ordinal) ->
            if not (allowed.Contains optionName) then
                raise (UsageException $"Unknown {command} option '{optionName}'.")

            if Map.containsKey optionName values then
                raise (UsageException $"Duplicate {command} option '{optionName}'.")

            if value.StartsWith("--", StringComparison.Ordinal) then
                raise (UsageException $"Missing value for {command} option '{optionName}'.")

            loop tail (Map.add optionName value values)
        | [ optionName ] when optionName.StartsWith("--", StringComparison.Ordinal) ->
            raise (UsageException $"Missing value for {command} option '{optionName}'.")
        | unexpected :: _ -> raise (UsageException $"Unexpected {command} argument '{unexpected}'.")

    let values = loop args Map.empty

    for optionName in required do
        if not (Map.containsKey optionName values) then
            raise (UsageException $"Required {command} option '{optionName}' is missing.")

    values

let private optionValue name values = Map.find name values

let private runArtifactCommand args =
    let options =
        parseNamedOptions
            "artifact"
            [ "--artifact-root"
              "--consumer-sdk"
              "--producer-sdk"
              "--tested-sha"
              "--run-id"
              "--producer-attempt" ]
            [ "--github-output" ]
            args

    let expectations =
        { ArtifactRoot = optionValue "--artifact-root" options
          ConsumerSdk = optionValue "--consumer-sdk" options
          ProducerSdk = optionValue "--producer-sdk" options
          TestedSha = optionValue "--tested-sha" options
          RunId = optionValue "--run-id" options
          ProducerAttempt = optionValue "--producer-attempt" options }

    let artifact =
        try
            validateArtifact expectations
        with ValidationException message ->
            fail
                $"Artifact '{Path.GetFullPath expectations.ArtifactRoot}' for producer SDK {expectations.ProducerSdk} and consumer SDK {expectations.ConsumerSdk} was rejected: {message}"

    match Map.tryFind "--github-output" options with
    | Some path -> appendGitHubOutput path artifact
    | None -> ()

    let role = sdkRoles[expectations.ProducerSdk]
    printfn "Verified producer SDK %s (%s) for consumer SDK %s." expectations.ProducerSdk role expectations.ConsumerSdk

let private runSelectArtifactCommand args =
    let options =
        parseNamedOptions
            "select-artifact"
            [ "--artifact-name"
              "--producer-attempt"
              "--producer-sdk"
              "--tested-sha"
              "--run-id"
              "--github-output" ]
            []
            args

    let artifactName = optionValue "--artifact-name" options
    let producerAttempt = optionValue "--producer-attempt" options
    let producerSdk = optionValue "--producer-sdk" options
    let testedSha = optionValue "--tested-sha" options
    let runId = optionValue "--run-id" options
    let selectedName = selectProducerArtifactName artifactName producerAttempt producerSdk testedSha runId

    appendProducerIdentityOutputs (optionValue "--github-output" options) selectedName producerAttempt
    printfn "Selected producer artifact %s (producer attempt %s)." selectedName producerAttempt

let private runAggregateCommand args =
    let required = aggregateJobs |> List.map (fun job -> "--" + job.Replace('_', '-'))
    let options = parseNamedOptions "aggregate" required [] args

    let results =
        aggregateJobs
        |> List.map (fun job -> job, optionValue ("--" + job.Replace('_', '-')) options)
        |> Map.ofList

    for job in aggregateJobs do
        printfn "%s: %s" job results[job]

    validateJobResults results

let private printUsage () = Console.WriteLine usageText

let private main (arguments: string list) =
    match arguments with
    | [] ->
        eprintfn "%s" usageText
        2
    | [ "--help" ]
    | [ "-h" ] ->
        printUsage ()
        0
    | command :: rest when rest |> List.exists (fun argument -> argument = "--help" || argument = "-h") ->
        printUsage ()
        0
    | "artifact" :: rest ->
        try
            runArtifactCommand rest
            0
        with UsageException message ->
            eprintfn "Usage error: %s\n%s" message usageText
            2
    | "select-artifact" :: rest ->
        try
            runSelectArtifactCommand rest
            0
        with UsageException message ->
            eprintfn "Usage error: %s\n%s" message usageText
            2
    | "aggregate" :: rest ->
        try
            runAggregateCommand rest
            0
        with UsageException message ->
            eprintfn "Usage error: %s\n%s" message usageText
            2
    | [ "self-test" ] ->
        try
            runSelfTests ()
            0
        with error ->
            eprintfn "Distribution self-test failed: %s" error.Message
            1
    | command :: _ ->
        eprintfn "Unknown command '%s'.\n%s" command usageText
        2

let private exitStatus =
    try
        main (fsi.CommandLineArgs |> Array.skip 1 |> Array.toList)
    with
    | ValidationException message ->
        eprintfn "Distribution validation failed: %s" message
        1
    | error ->
        eprintfn "Unexpected distribution validation error: %s\n%s" error.Message (error.ToString())
        1

Environment.Exit exitStatus
