open System
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Text.Json
open System.Xml.Linq

exception private ValidationException of string

let private fail message = raise (ValidationException message)

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

let private runFsi sourcePath workingDirectory localFeed packageCache httpCache =
    let startInfo = ProcessStartInfo("dotnet")
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true
    startInfo.ArgumentList.Add "fsi"
    startInfo.ArgumentList.Add "--exec"
    startInfo.ArgumentList.Add sourcePath
    let fsiHome = Path.Combine(workingDirectory, "fsi-home")
    Directory.CreateDirectory fsiHome |> ignore

    if not (OperatingSystem.IsWindows()) then
        File.SetUnixFileMode(
            fsiHome,
            UnixFileMode.UserRead ||| UnixFileMode.UserWrite ||| UnixFileMode.UserExecute)

    startInfo.Environment["HOME"] <- fsiHome
    startInfo.Environment["USERPROFILE"] <- fsiHome
    startInfo.Environment["DOTNET_CLI_HOME"] <- fsiHome
    startInfo.Environment["XDG_RUNTIME_DIR"] <- fsiHome
    startInfo.Environment["NUGET_PACKAGES"] <- packageCache
    startInfo.Environment["NUGET_HTTP_CACHE_PATH"] <- httpCache
    startInfo.Environment["RestoreSources"] <- localFeed
    startInfo.Environment["NuGetAudit"] <- "false"
    startInfo.Environment.Remove "FSharpCoreImplicitPackageVersion" |> ignore
    startInfo.Environment.Remove "NoWarn" |> ignore

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
        fail $"dotnet fsi exited {child.ExitCode}.\n{output}{error}"

    if (output + error).Contains("NU1605", StringComparison.Ordinal) then
        fail "FSI reported an FSharp.Core downgrade while restoring the local API candidate."

    if not (String.IsNullOrEmpty output) then
        Console.Out.Write output

    if not (String.IsNullOrEmpty error) then
        Console.Error.Write error

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

        runFsi sourcePath root feed packageCache httpCache
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
