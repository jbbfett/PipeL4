module PackageValidation

open System
open System.Buffers.Binary
open System.IO
open System.IO.Compression
open System.Text
open System.Text.Json
open System.Text.RegularExpressions
open System.Xml
open System.Xml.Linq

type PackageSummary =
    { PackagePath: string
      PackageId: string
      Version: string
      TargetFramework: string
      ToolCommand: string
      ReadmePath: string }

exception private ValidationException of string

let private requiredToolFiles =
    [ "PipeL4.dll"
      "PipeL4.deps.json"
      "PipeL4.runtimeconfig.json"
      "PipeL4.Core.dll"
      "PipeL4.Plugins.dll"
      "FSharp.Compiler.Service.dll"
      "FSharp.Core.dll"
      "FSharp.DependencyManager.Nuget.dll"
      "Microsoft.Extensions.Logging.Console.dll" ]

let private requiredNoticeFiles =
    [ "THIRD_PARTY_NOTICES.md"
      "notices/FSharp.LICENSE.txt"
      "notices/DotNet.LICENSE.txt"
      "notices/DotNet.THIRD-PARTY-NOTICES.txt" ]

let private expectedLicenseUrl = "https://licenses.nuget.org/MIT"
let private expectedProjectUrl = "https://github.com/jbbfett/PipeL4"
let private expectedRepositoryUrl = "https://github.com/jbbfett/PipeL4.git"
let private expectedRepositoryType = "git"

let private supportedNuspecNamespaceUris =
    [ "http://schemas.microsoft.com/packaging/2010/07/nuspec.xsd"
      "http://schemas.microsoft.com/packaging/2011/08/nuspec.xsd"
      "http://schemas.microsoft.com/packaging/2011/10/nuspec.xsd"
      "http://schemas.microsoft.com/packaging/2012/06/nuspec.xsd"
      "http://schemas.microsoft.com/packaging/2013/01/nuspec.xsd"
      "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd" ]

let private isSupportedNuspecNamespace namespaceUri =
    supportedNuspecNamespaceUris
    |> List.exists (fun supportedUri -> String.Equals(namespaceUri, supportedUri, StringComparison.Ordinal))

let private fail message = raise (ValidationException message)

let private localName (element: XElement) = element.Name.LocalName

let private children (parent: XElement) name =
    parent.Elements(XName.Get(name, parent.Name.NamespaceName))

let private child (parent: XElement) name =
    children parent name |> Seq.tryHead

let private rejectForeignChild (parent: XElement) name scope =
    if parent.Elements() |> Seq.exists (fun element -> localName element = name && element.Name.Namespace <> parent.Name.Namespace) then
        fail $"Nuspec {scope} contains a foreign-namespace '{name}' element."

let private requiredText (parent: XElement) name =
    match child parent name with
    | Some element when not (String.IsNullOrWhiteSpace element.Value) -> element.Value.Trim()
    | _ -> fail $"Required nuspec metadata '{name}' is missing or empty."

let private validateUrl value label =
    match Uri.TryCreate(value, UriKind.Absolute) with
    | true, uri when uri.Scheme = Uri.UriSchemeHttps && not (String.IsNullOrWhiteSpace uri.Host) -> ()
    | _ -> fail $"{label} must be an absolute HTTPS URL; found '{value}'."

let private hasUnexpectedAttributes (element: XElement) allowedNames =
    element.Attributes()
    |> Seq.exists (fun attribute ->
        not attribute.IsNamespaceDeclaration
        && (attribute.Name.NamespaceName <> "" || not (List.contains attribute.Name.LocalName allowedNames)))

let private hasNestedElementsOrNonWhitespaceText (element: XElement) =
    element.Nodes()
    |> Seq.exists (function
        | :? XElement -> true
        | :? XText as text -> not (String.IsNullOrWhiteSpace text.Value)
        | _ -> false)

let private hasOnlyScalarText (element: XElement) =
    element.Nodes()
    |> Seq.forall (function
        | :? XText -> true
        | _ -> false)

let private normalizePackagePath (path: string) =
    let parts =
        path.Split('/', StringSplitOptions.RemoveEmptyEntries)
        |> Array.filter ((<>) ".")

    if parts.Length = 0 then "." else String.Join("/", parts)

let private validateMarkdownLinks (readme: string) (entries: Set<string>) =
    let pattern = Regex("!?\\[[^\\]]*\\]\\(([^)]+)\\)", RegexOptions.CultureInvariant)

    for matched in pattern.Matches(readme) do
        let rawTarget = matched.Groups[1].Value.Trim()
        let target =
            rawTarget.Split((null: char array), StringSplitOptions.RemoveEmptyEntries)
            |> Array.tryHead
            |> Option.defaultValue ""
            |> fun value -> value.Trim('<', '>')

        if target.StartsWith("#", StringComparison.Ordinal) then
            ()
        else
            let hasScheme = Regex.IsMatch(target, "^[A-Za-z][A-Za-z0-9+.-]*:", RegexOptions.CultureInvariant)

            if hasScheme then
                match Uri.TryCreate(target, UriKind.Absolute) with
                | true, uri when uri.Scheme = Uri.UriSchemeHttps && not (String.IsNullOrWhiteSpace uri.Host) -> ()
                | _ -> fail $"Package README has an unsupported link '{target}'."
            elif target.StartsWith("/", StringComparison.Ordinal) || target.Contains('\\') then
                fail $"Package README has a repository-local link '{target}'."
            else
                let packagePath = target.Split('#', '?')[0] |> normalizePackagePath
                if packagePath.Split('/') |> Array.contains ".." || not (entries.Contains packagePath) then
                    fail $"Package README link '{target}' is not a package-relative file or web URL."

let private jsonProperty (label: string) (element: JsonElement) (propertyName: string) =
    try
        match element.TryGetProperty propertyName with
        | true, value -> value
        | false, _ -> fail $"Package {label} is missing JSON property '{propertyName}'."
    with
    | :? InvalidOperationException -> fail $"Package {label} has invalid JSON structure at '{propertyName}'."

let private jsonString (label: string) (element: JsonElement) =
    try
        match element.GetString() with
        | value when not (isNull value) -> value
        | _ -> fail $"Package {label} contains a null JSON string value."
    with
    | :? InvalidOperationException -> fail $"Package {label} contains a value with the wrong JSON type."

let private optionalJsonProperty (label: string) (element: JsonElement) (propertyName: string) =
    try
        match element.TryGetProperty propertyName with
        | true, value -> Some value
        | false, _ -> None
    with
    | :? InvalidOperationException -> fail $"Package {label} has invalid JSON structure at '{propertyName}'."

let private readUInt16 (bytes: byte array) offset =
    BinaryPrimitives.ReadUInt16LittleEndian(ReadOnlySpan<byte>(bytes, offset, 2))

let private readUInt32 (bytes: byte array) offset =
    BinaryPrimitives.ReadUInt32LittleEndian(ReadOnlySpan<byte>(bytes, offset, 4))

let private readUInt64 (bytes: byte array) offset =
    BinaryPrimitives.ReadUInt64LittleEndian(ReadOnlySpan<byte>(bytes, offset, 8))

let private seekAndRead (stream: FileStream) offset count =
    if offset < 0L || offset > stream.Length - int64 count then
        raise (InvalidDataException("ZIP directory points outside the archive."))

    stream.Position <- offset
    let bytes = Array.zeroCreate<byte> count
    stream.ReadExactly(bytes)
    bytes

let private centralDirectoryInfo (stream: FileStream) =
    let tailLength = int (min stream.Length 65557L)
    let tailOffset = stream.Length - int64 tailLength
    let tail = seekAndRead stream tailOffset tailLength
    let mutable eocdIndex = -1

    for index in tail.Length - 22 .. -1 .. 0 do
        if eocdIndex < 0
           && readUInt32 tail index = 0x06054b50u
           && index + 22 + int (readUInt16 tail (index + 20)) <= tail.Length then
            eocdIndex <- index

    if eocdIndex < 0 then
        raise (InvalidDataException("ZIP end-of-central-directory record was not found."))

    let eocdOffset = tailOffset + int64 eocdIndex
    let disk = readUInt16 tail (eocdIndex + 4)
    let directoryDisk = readUInt16 tail (eocdIndex + 6)
    let entriesOnDisk = readUInt16 tail (eocdIndex + 8)
    let totalEntries = readUInt16 tail (eocdIndex + 10)
    let directorySize = readUInt32 tail (eocdIndex + 12)
    let directoryOffset = readUInt32 tail (eocdIndex + 16)

    let needsZip64 =
        entriesOnDisk = UInt16.MaxValue
        || totalEntries = UInt16.MaxValue
        || directorySize = UInt32.MaxValue
        || directoryOffset = UInt32.MaxValue

    let count, size, offset =
        if needsZip64 then
            let locator = seekAndRead stream (eocdOffset - 20L) 20
            if readUInt32 locator 0 <> 0x07064b50u then
                raise (InvalidDataException("ZIP64 locator was not found."))
            if readUInt32 locator 4 <> 0u || readUInt32 locator 16 <> 1u then
                raise (InvalidDataException("Multi-disk ZIP archives are not supported."))

            let zip64Offset = readUInt64 locator 8
            if zip64Offset > uint64 Int64.MaxValue then
                raise (InvalidDataException("ZIP64 directory offset is too large."))
            let record = seekAndRead stream (int64 zip64Offset) 56
            if readUInt32 record 0 <> 0x06064b50u then
                raise (InvalidDataException("ZIP64 end-of-central-directory record was not found."))
            if readUInt32 record 16 <> 0u || readUInt32 record 20 <> 0u then
                raise (InvalidDataException("Multi-disk ZIP archives are not supported."))

            let onDisk = readUInt64 record 24
            let total = readUInt64 record 32
            if onDisk <> total then
                raise (InvalidDataException("ZIP archive has inconsistent central-directory entry counts."))
            total, readUInt64 record 40, readUInt64 record 48
        else
            if disk <> 0us || directoryDisk <> 0us || entriesOnDisk <> totalEntries then
                raise (InvalidDataException("Multi-disk ZIP archives are not supported."))
            uint64 totalEntries, uint64 directorySize, uint64 directoryOffset

    if count > uint64 Int32.MaxValue || offset > uint64 Int64.MaxValue || size > uint64 Int64.MaxValue then
        raise (InvalidDataException("ZIP central directory is too large to validate."))
    let archiveLength = uint64 stream.Length
    if offset > archiveLength || size > archiveLength - offset then
        raise (InvalidDataException("ZIP central directory points outside the archive."))
    int count, int64 offset

let private centralDirectoryCrcs (stream: FileStream) =
    let count, offset = centralDirectoryInfo stream
    stream.Position <- offset
    let crcs = ResizeArray<uint32>(count)

    for _ in 1 .. count do
        let header = Array.zeroCreate<byte> 46
        stream.ReadExactly(header)
        if readUInt32 header 0 <> 0x02014b50u then
            raise (InvalidDataException("Invalid ZIP central-directory entry."))
        let nameLength = int (readUInt16 header 28)
        let extraLength = int (readUInt16 header 30)
        let commentLength = int (readUInt16 header 32)
        crcs.Add(readUInt32 header 16)
        stream.Position <- stream.Position + int64 nameLength + int64 extraLength + int64 commentLength
        if stream.Position > stream.Length then
            raise (InvalidDataException("ZIP central directory points outside the archive."))

    crcs.ToArray()

let private crc32Table =
    Array.init 256 (fun index ->
        let mutable value = uint32 index
        for _ in 0 .. 7 do
            value <- if value &&& 1u = 1u then 0xEDB88320u ^^^ (value >>> 1) else value >>> 1
        value)

let private validateArchiveIntegrity (packagePath: string) (archive: ZipArchive) =
    use file = new FileStream(packagePath, FileMode.Open, FileAccess.Read, FileShare.Read)
    let expectedCrcs = centralDirectoryCrcs file
    if expectedCrcs.Length <> archive.Entries.Count then
        raise (InvalidDataException("ZIP central-directory entry count does not match the archive."))

    let buffer = Array.zeroCreate<byte> 65536

    for index in 0 .. archive.Entries.Count - 1 do
        let entry = archive.Entries[index]
        let mutable crc = 0xFFFFFFFFu
        use input = entry.Open()
        let mutable read = input.Read(buffer, 0, buffer.Length)
        while read > 0 do
            for position in 0 .. read - 1 do
                let tableIndex = int ((crc ^^^ uint32 buffer[position]) &&& 0xFFu)
                crc <- crc32Table[tableIndex] ^^^ (crc >>> 8)
            read <- input.Read(buffer, 0, buffer.Length)
        let actualCrc = crc ^^^ 0xFFFFFFFFu
        if actualCrc <> expectedCrcs[index] then
            fail $"Package archive has a corrupt entry: {entry.FullName}."

let private validatePackageCore (packagePath: string) =
    if not (File.Exists packagePath) then
        fail $"Package file does not exist: {packagePath}"

    use package = ZipFile.OpenRead packagePath
    validateArchiveIntegrity packagePath package

    let names = package.Entries |> Seq.map (fun entry -> entry.FullName) |> Seq.toArray
    let entries = Set.ofArray names

    if entries.Count <> names.Length then
        fail "Package archive contains duplicate entry paths."

    for name in names do
        if name.StartsWith("/", StringComparison.Ordinal)
           || name.Contains('\\')
           || (name.Split('/') |> Array.contains "..") then
            fail $"Package archive contains an unsafe path: '{name}'."

    let nuspecs = names |> Array.filter (fun name -> name.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))
    if nuspecs.Length <> 1 then
        fail $"Expected one embedded nuspec, found {nuspecs.Length} in {Path.GetFileName packagePath}."

    let nuspec =
        use input = package.GetEntry(nuspecs[0]).Open()
        XDocument.Load input

    let nuspecRoot =
        match nuspec.Root |> Option.ofObj with
        | None -> fail "Embedded nuspec has no root element; expected 'package'."
        | Some root when localName root <> "package" ->
            fail $"Embedded nuspec root must be the NuGet 'package' element; found '{root.Name}'."
        | Some root when not (isSupportedNuspecNamespace root.Name.NamespaceName) ->
            fail $"Embedded nuspec package element uses an unsupported namespace '{root.Name.NamespaceName}'."
        | Some root -> root

    rejectForeignChild nuspecRoot "metadata" "package"

    let metadataElements = children nuspecRoot "metadata" |> Seq.toArray
    let metadata =
        match metadataElements with
        | [| value |] -> value
        | _ -> fail $"Embedded nuspec must contain exactly one metadata element in namespace '{nuspecRoot.Name.NamespaceName}'."

    let packageId = requiredText metadata "id"
    let version = requiredText metadata "version"
    if packageId <> "PipeL4" then
        fail $"Expected PackageId 'PipeL4', found '{packageId}'."

    let expectedFilename = $"{packageId}.{version}.nupkg"
    if Path.GetFileName packagePath <> expectedFilename then
        fail $"Package filename '{Path.GetFileName packagePath}' does not match nuspec version '{version}'."

    let description = requiredText metadata "description"
    if description.Equals("package description", StringComparison.OrdinalIgnoreCase) then
        fail "Package description is still the SDK placeholder."
    requiredText metadata "authors" |> ignore
    requiredText metadata "tags" |> ignore

    rejectForeignChild metadata "license" "metadata"
    rejectForeignChild metadata "licenseUrl" "metadata"

    let licenseUrls = children metadata "licenseUrl" |> Seq.toArray

    match licenseUrls with
    | [||] -> ()
    | [| licenseUrl |] when licenseUrl.Value.Trim() = expectedLicenseUrl -> ()
    | [| _ |] -> fail "Unexpected nuspec licenseUrl metadata."
    | _ -> fail "Package must contain at most one licenseUrl metadata element."

    rejectForeignChild metadata "projectUrl" "metadata"
    match children metadata "projectUrl" |> Seq.toArray with
    | [| projectUrl |] ->
        if hasUnexpectedAttributes projectUrl [] then
            fail "Project URL metadata must not contain attributes."
        if not (hasOnlyScalarText projectUrl) then
            fail "Project URL metadata must contain scalar text only."
        if projectUrl.Value <> expectedProjectUrl then
            fail $"Project URL must be exactly '{expectedProjectUrl}', found '{projectUrl.Value}'."
        validateUrl projectUrl.Value "Project URL"
    | _ -> fail $"Package must contain exactly one project URL '{expectedProjectUrl}'."

    rejectForeignChild metadata "repository" "metadata"
    match children metadata "repository" |> Seq.toArray with
    | [| repository |] ->
        if hasUnexpectedAttributes repository [ "url"; "type"; "branch"; "commit" ] then
            fail "Repository metadata contains an unexpected or namespace-qualified attribute."
        let repositoryUrl = repository.Attribute(XName.Get "url") |> Option.ofObj |> Option.map (fun attribute -> attribute.Value) |> Option.defaultValue ""
        let repositoryType = repository.Attribute(XName.Get "type") |> Option.ofObj |> Option.map (fun attribute -> attribute.Value) |> Option.defaultValue ""
        if repositoryUrl <> expectedRepositoryUrl then
            fail $"Repository URL must be '{expectedRepositoryUrl}', found '{repositoryUrl}'."
        if repositoryType <> expectedRepositoryType then
            fail $"Repository type must be '{expectedRepositoryType}', found '{repositoryType}'."
        if hasNestedElementsOrNonWhitespaceText repository then
            fail "Repository metadata must not contain nested elements or non-whitespace text."
        validateUrl repositoryUrl "Repository URL"
    | _ -> fail "Package must contain exactly one repository metadata element."

    let licenses = children metadata "license" |> Seq.toArray

    match licenses with
    | [| license |] ->
        let licenseType = license.Attribute(XName.Get "type") |> Option.ofObj |> Option.map (fun attribute -> attribute.Value.Trim()) |> Option.defaultValue ""
        let licenseValue = license.Value.Trim()
        if licenseType <> "expression" || licenseValue <> "MIT" then
            fail "Package license must be the MIT expression."
    | _ -> fail "Package must contain exactly one MIT expression license element."

    let missingNotices = requiredNoticeFiles |> List.filter (entries.Contains >> not)
    if not missingNotices.IsEmpty then
        let displayedNotices = String.Join(", ", missingNotices)
        fail $"Package is missing bundled dependency notices: {displayedNotices}"

    let readmeName = requiredText metadata "readme"
    if readmeName.StartsWith("/", StringComparison.Ordinal)
       || (readmeName.Split('/') |> Array.contains "..") then
        fail $"Package README path '{readmeName}' is not package-relative."
    if not (entries.Contains readmeName) then
        fail $"Package README '{readmeName}' is declared but missing from {Path.GetFileName packagePath}."

    let readme =
        use input = package.GetEntry(readmeName).Open()
        use reader = new StreamReader(input, UTF8Encoding(false, true), true)
        reader.ReadToEnd()
    validateMarkdownLinks readme entries

    let hasToolPackageType =
        child metadata "packageTypes"
        |> Option.exists (fun packageTypes ->
            packageTypes.Elements()
            |> Seq.exists (fun item ->
                item.Name = XName.Get("packageType", packageTypes.Name.NamespaceName)
                && (item.Attribute(XName.Get "name") |> Option.ofObj |> Option.exists (fun attribute -> attribute.Value = "DotnetTool"))))
    if not hasToolPackageType then
        fail "NuGet package type is not DotnetTool."

    let toolRoot = "tools/net10.0/any/"
    let toolSettingsPath = $"{toolRoot}DotnetToolSettings.xml"
    if not (entries.Contains toolSettingsPath) then
        fail $"Expected .NET 10 tool settings at '{toolSettingsPath}'."

    let settings =
        use input = package.GetEntry(toolSettingsPath).Open()
        XDocument.Load input

    let settingsRoot =
        match settings.Root |> Option.ofObj with
        | Some root -> root
        | None -> fail "Tool settings XML has no root element; expected 'DotNetCliTool'."

    if settingsRoot.Name <> XName.Get "DotNetCliTool" then
        fail $"Tool settings XML root must be the unqualified element 'DotNetCliTool'; found '{settingsRoot.Name}'."

    let settingsVersion =
        settingsRoot.Attribute(XName.Get "Version")
        |> Option.ofObj
        |> Option.map (fun attribute -> attribute.Value)
        |> Option.defaultValue ""
    if settingsVersion <> "1" then
        fail $"Tool settings root must declare Version='1'; found '{settingsVersion}'."

    let commandContainers = settingsRoot.Elements(XName.Get "Commands") |> Seq.toArray
    if commandContainers.Length <> 1 then
        fail $"Tool settings XML must contain one direct 'Commands' element; found {commandContainers.Length}."

    let validCommand =
        commandContainers[0].Elements(XName.Get "Command")
        |> Seq.exists (fun element ->
            (element.Attribute(XName.Get "Name") |> Option.ofObj |> Option.exists (fun attribute -> attribute.Value = "pipel4"))
            && (element.Attribute(XName.Get "EntryPoint") |> Option.ofObj |> Option.exists (fun attribute -> attribute.Value = "PipeL4.dll"))
            && (element.Attribute(XName.Get "Runner") |> Option.ofObj |> Option.exists (fun attribute -> attribute.Value = "dotnet")))
    if not validCommand then
        fail "ToolCommandName 'pipel4' is not mapped to PipeL4.dll with Runner='dotnet' inside the root Commands element."

    let missingFiles = requiredToolFiles |> List.map (fun filename -> toolRoot + filename) |> List.filter (entries.Contains >> not)
    if not missingFiles.IsEmpty then
        let displayedFiles = String.Join(", ", missingFiles)
        fail $"Tool package is missing runtime files: {displayedFiles}"

    let runtimeConfig =
        use input = package.GetEntry(toolRoot + "PipeL4.runtimeconfig.json").Open()
        JsonDocument.Parse input
    let runtimeOptions = jsonProperty "runtime configuration" runtimeConfig.RootElement "runtimeOptions"
    let tfm = jsonProperty "runtime configuration" runtimeOptions "tfm" |> jsonString "runtime configuration"
    let framework = jsonProperty "runtime configuration" runtimeOptions "framework"
    let frameworkName = jsonProperty "runtime configuration" framework "name" |> jsonString "runtime configuration"
    let frameworkVersion = jsonProperty "runtime configuration" framework "version" |> jsonString "runtime configuration"
    if tfm <> "net10.0" || frameworkName <> "Microsoft.NETCore.App" || isNull frameworkVersion || not (frameworkVersion.StartsWith("10.", StringComparison.Ordinal)) then
        fail "Package runtime configuration does not target .NET 10."

    use dependencies =
        let input = package.GetEntry(toolRoot + "PipeL4.deps.json").Open()
        JsonDocument.Parse input
    let dependenciesRoot = dependencies.RootElement
    let runtimeTargetInfo = jsonProperty "dependency manifest" dependenciesRoot "runtimeTarget"
    let runtimeTarget = jsonProperty "dependency manifest" runtimeTargetInfo "name" |> jsonString "dependency manifest"
    let targets = jsonProperty "dependency manifest" dependenciesRoot "targets"
    let targetAssets = jsonProperty "dependency manifest" targets runtimeTarget
    let missingAssets = ResizeArray<string>()
    if targetAssets.ValueKind <> JsonValueKind.Object then
        fail "Package dependency manifest has an invalid runtime target."
    else
        for library in targetAssets.EnumerateObject() do
            match optionalJsonProperty "dependency manifest" library.Value "runtime" with
            | Some runtimeAssets when runtimeAssets.ValueKind = JsonValueKind.Object ->
                for asset in runtimeAssets.EnumerateObject() do
                    if asset.Name <> "_._" then
                        let assetName = asset.Name.Replace('\\', '/') |> fun path -> path.Split('/') |> Array.last
                        if not (entries.Contains(toolRoot + assetName)) then
                            missingAssets.Add($"{library.Name}: {asset.Name}")
            | Some _ -> fail "Package dependency manifest has an invalid runtime asset list."
            | None -> ()
    if missingAssets.Count > 0 then
        let displayedAssets = String.Join(", ", missingAssets)
        fail $"Dependencies listed in PipeL4.deps.json are missing from the tool: {displayedAssets}"

    let metadataPrefix = "package/services/metadata/core-properties/"
    let unexpected = ResizeArray<string>()
    for name in names do
        let allowed =
            name = "_rels/.rels"
            || name = nuspecs[0]
            || name = readmeName
            || List.contains name requiredNoticeFiles
            || name = "[Content_Types].xml"
            || name = ".signature.p7s"
            || name.StartsWith(toolRoot, StringComparison.Ordinal)
            || (name.StartsWith(metadataPrefix, StringComparison.Ordinal)
                && name.EndsWith(".psmdcp", StringComparison.Ordinal)
                && not (name.Substring(metadataPrefix.Length).Contains('/')))
        let lowered = name.ToLowerInvariant()
        let leaf = name.Split('/') |> Array.last
        let isTestOrTemporary =
            lowered.Contains(".tests/", StringComparison.Ordinal)
            || lowered.EndsWith("tests.dll", StringComparison.Ordinal)
            || lowered.EndsWith(".trx", StringComparison.Ordinal)
            || lowered.EndsWith(".tmp", StringComparison.Ordinal)
            || leaf.Contains("benchmark", StringComparison.OrdinalIgnoreCase)
        if not allowed || isTestOrTemporary then unexpected.Add name
    if unexpected.Count > 0 then
        let displayedUnexpected = String.Join(", ", unexpected)
        fail $"Package contains unexpected, test, benchmark, or temporary entries: {displayedUnexpected}"

    { PackagePath = packagePath
      PackageId = packageId
      Version = version
      TargetFramework = "net10.0"
      ToolCommand = "pipel4"
      ReadmePath = readmeName }

let validatePackageFile (packagePath: string) : Result<PackageSummary, string> =
    try
        let fullPath = Path.GetFullPath packagePath
        Ok(validatePackageCore fullPath)
    with
    | ValidationException message -> Error message
    | :? InvalidDataException as error -> Error $"Not a valid NuGet ZIP package or corrupt package archive: {error.Message}"
    | :? XmlException as error -> Error $"Package metadata is not valid XML: {error.Message}"
    | :? JsonException as error -> Error $"Package runtime metadata is invalid JSON: {error.Message}"
    | :? DecoderFallbackException as error -> Error $"Package README or runtime metadata is invalid UTF-8: {error.Message}"
    | :? IOException as error -> Error $"Unable to read package '{packagePath}': {error.Message}"
    | :? UnauthorizedAccessException as error -> Error $"Unable to access package '{packagePath}': {error.Message}"
    | :? ArgumentException as error -> Error $"Invalid package path '{packagePath}': {error.Message}"
    | :? NotSupportedException as error -> Error $"Invalid package path '{packagePath}': {error.Message}"

let validateInput (inputPath: string) : Result<PackageSummary, string> =
    try
        let fullPath = Path.GetFullPath inputPath
        if Directory.Exists fullPath then
            let packages = Directory.GetFiles(fullPath, "*.nupkg", SearchOption.TopDirectoryOnly) |> Array.sort
            if packages.Length <> 1 then
                Error $"Expected exactly one .nupkg in '{fullPath}', found {packages.Length}."
            else
                validatePackageFile packages[0]
        elif File.Exists fullPath then
            validatePackageFile fullPath
        else
            Error $"Package file or output directory does not exist: {fullPath}"
    with
    | :? ArgumentException as error -> Error $"Invalid package path '{inputPath}': {error.Message}"
    | :? NotSupportedException as error -> Error $"Invalid package path '{inputPath}': {error.Message}"
    | :? PathTooLongException as error -> Error $"Invalid package path '{inputPath}': {error.Message}"
    | :? UnauthorizedAccessException as error -> Error $"Unable to access package path '{inputPath}': {error.Message}"
    | :? IOException as error -> Error $"Unable to inspect package path '{inputPath}': {error.Message}"
