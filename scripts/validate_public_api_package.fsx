open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Reflection
open System.Text
open System.Text.Json
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

let private parsePackageVersionRange (value: string) =
    let normalized = value.Trim().TrimStart('[', '(').TrimEnd(']', ')')
    let bounds = normalized.Split([| ',' |], StringSplitOptions.None)
    let lowerBound = bounds[0].Trim()
    if String.IsNullOrWhiteSpace lowerBound then
        fail $"FSharp.Core dependency must have a usable version range; found '{value}'."
    lowerBound

let private locateCachedPackage (packageId: string) (version: string) =
    let globalPackagesOutput, _ =
        runProcess false Environment.CurrentDirectory "dotnet" [ "nuget"; "locals"; "global-packages"; "--list" ]
    let globalPackages =
        globalPackagesOutput.Split([| Environment.NewLine; "\n" |], StringSplitOptions.RemoveEmptyEntries)
        |> Array.tryPick (fun line ->
            let separator = line.IndexOf(':')
            if separator >= 0 then
                Some(line.Substring(separator + 1).Trim())
            else
                None)
        |> Option.defaultWith (fun () -> fail "Could not locate NuGet's global package cache.")

    let normalizedId = packageId.ToLowerInvariant()
    let packageDirectory = Path.Combine(globalPackages, normalizedId, version.ToLowerInvariant())
    let expectedFile = Path.Combine(packageDirectory, $"{normalizedId}.{version.ToLowerInvariant()}.nupkg")
    if File.Exists expectedFile then
        expectedFile
    else
        fail
            $"Could not stage dependency {packageId} {version} from the NuGet package cache at '{expectedFile}'. Restore the repository with Paket before validating the consumer."

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
    let fsharpVersion =
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
    , fsharpVersion

let private validateIndependentConsumer packagePath version fsharpVersion useInvalidConsumerSource =
    let root = Path.Combine(Path.GetTempPath(), $"PipeL4 public API consumer {Guid.NewGuid():N}")
    let feed = Path.Combine(root, "feed")
    let project = Path.Combine(root, "consumer")
    let packageCache = Path.Combine(root, "packages")
    Directory.CreateDirectory feed |> ignore
    Directory.CreateDirectory project |> ignore

    try
        File.Copy(packagePath, Path.Combine(feed, Path.GetFileName packagePath))
        let fsharpPackage = locateCachedPackage "FSharp.Core" fsharpVersion
        File.Copy(fsharpPackage, Path.Combine(feed, Path.GetFileName fsharpPackage))

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

        let projectFile =
            XElement(
                "Project",
                XAttribute("Sdk", "Microsoft.NET.Sdk"),
                XElement(
                    "PropertyGroup",
                    XElement("TargetFramework", "net10.0"),
                    XElement("FSharpCoreImplicitPackageVersion", fsharpVersion),
                    XElement("RestorePackagesWithLockFile", "false")),
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

        let restoreOutput, restoreError =
            runProcess true project "dotnet"
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
                  "-p:NuGetAudit=false" ]
        if (restoreOutput + restoreError).Contains("NU1605", StringComparison.Ordinal) then
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
        if resolvedFSharpVersions <> [| fsharpVersion |] then
            let displayedVersions = String.Join(", ", resolvedFSharpVersions)
            fail
                $"Independent consumer resolved FSharp.Core {displayedVersions}; expected the package minimum {fsharpVersion}."
        printfn "Independent consumer resolved FSharp.Core %s from the isolated local feed." fsharpVersion

        let buildOutput, buildError =
            runProcess true project "dotnet"
                [ "build"
                  Path.Combine(project, "Consumer.fsproj")
                  "--configuration"
                  "Release"
                  "--no-restore"
                  "--verbosity"
                  "minimal" ]
        if (buildOutput + buildError).Contains("NU1605", StringComparison.Ordinal) then
            fail "Independent consumer build reported NuGet downgrade warning NU1605."
    finally
        if Directory.Exists root then
            Directory.Delete(root, true)

let private validatePackage packagePath useInvalidConsumerSource =
    let summary, fsharpVersion = validatePackageFile packagePath
    validateIndependentConsumer summary.PackagePath summary.Version fsharpVersion useInvalidConsumerSource
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
                    "Validated public API package: PackageId=%s, version=%s, target=%s, assembly=%s (%s), dependencies=%s; independent F# consumer compiled."
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
