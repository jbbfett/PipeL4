#load "PackageValidation.fsx"

open System
open System.Buffers.Binary
open System.Diagnostics
open System.IO
open System.IO.Compression
open System.Text
open System.Xml.Linq
open PackageValidation

let usage () =
    eprintfn "Usage: dotnet fsi scripts/test_validate_package.fsx <package-or-output-directory>"

let readEntry (archive: ZipArchive) (entryName: string) =
    use input = archive.GetEntry(entryName).Open()
    use output = new MemoryStream()
    input.CopyTo output
    output.ToArray()

let nuspecMetadata packagePath entryName =
    use archive = ZipFile.OpenRead packagePath
    use input = archive.GetEntry(entryName).Open()
    let document = XDocument.Load input
    match document.Root |> Option.ofObj |> Option.bind (fun root -> root.Elements() |> Seq.tryFind (fun element -> element.Name.LocalName = "metadata")) with
    | Some metadata -> metadata
    | None -> failwith "Test package nuspec has no metadata element."

let directChildren (parent: XElement) name =
    parent.Elements()
    |> Seq.filter (fun element -> element.Name.LocalName = name)
    |> Seq.toArray

let rewritePackage sourcePath targetPath omitEntry replacement extraEntry =
    use original = ZipFile.OpenRead sourcePath
    use output = new ZipArchive(new FileStream(targetPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None), ZipArchiveMode.Create)

    for entry in original.Entries do
        if omitEntry <> Some entry.FullName then
            let mutable content = readEntry original entry.FullName
            match replacement with
            | Some(entryName, oldText, newText) when entry.FullName = entryName ->
                let updated = Encoding.UTF8.GetString(content).Replace(oldText, newText, StringComparison.Ordinal)
                content <- Encoding.UTF8.GetBytes updated
            | _ -> ()

            let target = output.CreateEntry(entry.FullName, CompressionLevel.Optimal)
            use targetStream = target.Open()
            targetStream.Write(content, 0, content.Length)

    match extraEntry with
    | Some(entryName, content) ->
        let target = output.CreateEntry(entryName, CompressionLevel.Optimal)
        use targetStream = target.Open()
        targetStream.Write(content, 0, content.Length)
    | None -> ()

let rewritePackageNuspec sourcePath targetPath nuspecEntry mutate =
    use original = ZipFile.OpenRead sourcePath
    use output = new ZipArchive(new FileStream(targetPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None), ZipArchiveMode.Create)

    for entry in original.Entries do
        let content =
            if entry.FullName = nuspecEntry then
                use input = entry.Open()
                let document = XDocument.Load input
                let metadata =
                    document.Root.Elements()
                    |> Seq.filter (fun element -> element.Name.LocalName = "metadata")
                    |> Seq.exactlyOne
                mutate metadata
                use serialized = new MemoryStream()
                document.Save(serialized, SaveOptions.DisableFormatting)
                serialized.ToArray()
            else
                readEntry original entry.FullName

        let target = output.CreateEntry(entry.FullName, CompressionLevel.Optimal)
        use targetStream = target.Open()
        targetStream.Write(content, 0, content.Length)

let runDotnetFromOutsideRepository (workingDirectory: string) arguments =
    let startInfo = new ProcessStartInfo("dotnet")
    for argument in arguments do
        startInfo.ArgumentList.Add argument
    startInfo.WorkingDirectory <- workingDirectory
    startInfo.UseShellExecute <- false
    startInfo.RedirectStandardOutput <- true
    startInfo.RedirectStandardError <- true

    use childProcess = new Process(StartInfo = startInfo)
    if not (childProcess.Start()) then
        failwith "Could not start dotnet fsi for the CLI validation check."
    let standardOutput = childProcess.StandardOutput.ReadToEnd()
    let standardError = childProcess.StandardError.ReadToEnd()
    childProcess.WaitForExit()
    childProcess.ExitCode, standardOutput, standardError

let runValidatorFromOutsideRepository workingDirectory packagePath =
    let validatorPath = Path.Combine(__SOURCE_DIRECTORY__, "validate_package.fsx")
    runDotnetFromOutsideRepository workingDirectory [ "fsi"; validatorPath; packagePath ]

let runToolInstallFromOutsideRepository workingDirectory sourcePath toolPath version =
    runDotnetFromOutsideRepository
        workingDirectory
        [ "tool"
          "install"
          "PipeL4"
          "--tool-path"
          toolPath
          "--source"
          sourcePath
          "--version"
          version
          "--no-http-cache"
          "--verbosity"
          "quiet" ]

let writePackageWithIncorrectEntryCrc sourcePath targetPath =
    let bytes = File.ReadAllBytes sourcePath
    let signature = [| 0x50uy; 0x4Buy; 0x05uy; 0x06uy |]
    let minimumOffset = max 0 (bytes.Length - 65557)
    let mutable endRecord = -1

    for index in bytes.Length - 22 .. -1 .. minimumOffset do
        if endRecord < 0
           && bytes[index] = signature[0]
           && bytes[index + 1] = signature[1]
           && bytes[index + 2] = signature[2]
           && bytes[index + 3] = signature[3] then
            endRecord <- index

    if endRecord < 0 then
        failwith "Could not find the ZIP end-of-central-directory record for the CRC test."

    let directoryOffset = int (BinaryPrimitives.ReadUInt32LittleEndian(ReadOnlySpan<byte>(bytes, endRecord + 16, 4)))
    let centralSignature = BinaryPrimitives.ReadUInt32LittleEndian(ReadOnlySpan<byte>(bytes, directoryOffset, 4))
    if centralSignature <> 0x02014b50u then
        failwith "Could not find the ZIP central-directory record for the CRC test."

    bytes[directoryOffset + 16] <- bytes[directoryOffset + 16] ^^^ 0x01uy
    File.WriteAllBytes(targetPath, bytes)

let runTests sourceSummary injectFailure =
    let sourcePath = sourceSummary.PackagePath
    let nuspecEntry =
        use archive = ZipFile.OpenRead sourcePath
        archive.Entries
        |> Seq.map (fun entry -> entry.FullName)
        |> Seq.find (fun name -> name.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase))

    let temporaryRoot = Path.Combine(Path.GetTempPath(), $"PipeL4 package validation tests {Guid.NewGuid():N}")
    let outsideRepositoryDirectory = Path.Combine(temporaryRoot, "outside repository")
    let mitLicense = "<license type=\"expression\">MIT</license>"
    let sdkLicenseUrl = "<licenseUrl>https://licenses.nuget.org/MIT</licenseUrl>"
    let projectUrl = "https://github.com/jbbfett/PipeL4"
    let repositoryUrl = "https://github.com/jbbfett/PipeL4.git"
    let foreignLicense = "<x:license xmlns:x=\"urn:foreign\" type=\"expression\">MIT</x:license>"
    let sdkNuspecNamespace = "http://schemas.microsoft.com/packaging/2012/06/nuspec.xsd"
    let mutable passed = 0
    let mutable failed = 0

    let run name action =
        try
            action ()
            passed <- passed + 1
            printfn "PASS %s" name
        with error ->
            failed <- failed + 1
            eprintfn "FAIL %s: %s" name error.Message

    let scenarioDirectory name =
        let directory = Path.Combine(temporaryRoot, name)
        Directory.CreateDirectory directory |> ignore
        directory

    let canonicalChild (metadata: XElement) name =
        metadata.Elements(XName.Get(name, metadata.Name.NamespaceName)) |> Seq.exactlyOne

    let rewriteMetadata targetDirectory mutate =
        let target = Path.Combine(targetDirectory, Path.GetFileName sourcePath)
        rewritePackageNuspec sourcePath target nuspecEntry mutate
        target

    let expectError (expectedText: string) (path: string) =
        match validateInput path with
        | Error message when message.Contains(expectedText, StringComparison.Ordinal) -> ()
        | Error message ->
            failwith $"Expected diagnostic containing '{expectedText}', but got: {message}"
        | Ok summary ->
            failwith $"Expected rejection containing '{expectedText}', but {summary.PackagePath} passed."

    try
        Directory.CreateDirectory temporaryRoot |> ignore
        Directory.CreateDirectory outsideRepositoryDirectory |> ignore
        let globalJsonPath = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "..", "global.json"))
        File.Copy(globalJsonPath, Path.Combine(outsideRepositoryDirectory, "global.json"))

        run "accepts MIT expression and SDK-generated license URL" (fun () ->
            let metadata = nuspecMetadata sourcePath nuspecEntry
            match directChildren metadata "licenseUrl" with
            | [| licenseUrl |] when licenseUrl.Value.Trim() = "https://licenses.nuget.org/MIT" -> ()
            | _ -> failwith "Test package does not contain exactly one SDK-generated MIT license URL."
            match validatePackageFile sourcePath with
            | Ok summary when summary.Version = sourceSummary.Version && summary.PackageId = "PipeL4" -> ()
            | Ok summary -> failwith $"Candidate summary changed unexpectedly: {summary.PackageId} {summary.Version}."
            | Error message -> failwith $"Candidate package was rejected: {message}")

        run "accepts the canonical project and repository metadata" (fun () ->
            let metadata = nuspecMetadata sourcePath nuspecEntry
            match directChildren metadata "projectUrl" with
            | [| value |] when value.Value.Trim() = projectUrl -> ()
            | _ -> failwith "Test package does not contain the canonical project URL."
            match directChildren metadata "repository" with
            | [| repository |]
                when repository.Attribute(XName.Get "url").Value = repositoryUrl
                     && repository.Attribute(XName.Get "type").Value = "git" -> ()
            | _ -> failwith "Test package does not contain the canonical Git repository URL and type.")

        run "accepts standard optional repository branch and commit attributes" (fun () ->
            let directory = scenarioDirectory "optional repository attributes"
            let commit =
                System.Security.Cryptography.SHA256.HashData(Encoding.UTF8.GetBytes "optional repository metadata regression")
                |> Convert.ToHexString
                |> fun value -> value.ToLowerInvariant()
            let target =
                rewriteMetadata directory (fun metadata ->
                    let repository = canonicalChild metadata "repository"
                    repository.SetAttributeValue(XName.Get "branch", "feature/metadata-regression")
                    repository.SetAttributeValue(XName.Get "commit", commit))
            match validateInput target with
            | Ok _ -> ()
            | Error message -> failwith $"Standard optional repository attributes were rejected: {message}")

        run "accepts repository metadata without optional branch or commit attributes" (fun () ->
            let directory = scenarioDirectory "repository without optional attributes"
            let target =
                rewriteMetadata directory (fun metadata ->
                    let repository = canonicalChild metadata "repository"
                    for name in [ "branch"; "commit" ] do
                        repository.Attribute(XName.Get name)
                        |> Option.ofObj
                        |> Option.iter (fun attribute -> attribute.Remove()))
            match validateInput target with
            | Ok _ -> ()
            | Error message -> failwith $"Repository metadata without optional attributes was rejected: {message}")

        run "rejects a missing projectUrl node" (fun () ->
            let directory = scenarioDirectory "missing project URL"
            let target = rewriteMetadata directory (fun metadata -> canonicalChild metadata "projectUrl" |> fun element -> element.Remove())
            expectError "exactly one project URL" target)

        run "rejects a missing repository node" (fun () ->
            let directory = scenarioDirectory "missing repository node"
            let target = rewriteMetadata directory (fun metadata -> canonicalChild metadata "repository" |> fun element -> element.Remove())
            expectError "exactly one repository metadata element" target)

        run "rejects duplicate projectUrl nodes" (fun () ->
            let directory = scenarioDirectory "duplicate project URLs"
            let target =
                rewriteMetadata directory (fun metadata ->
                    metadata.Add(XElement(canonicalChild metadata "projectUrl")))
            expectError "exactly one project URL" target)

        run "rejects duplicate repository nodes" (fun () ->
            let directory = scenarioDirectory "duplicate repository nodes"
            let target =
                rewriteMetadata directory (fun metadata ->
                    metadata.Add(XElement(canonicalChild metadata "repository")))
            expectError "exactly one repository metadata element" target)

        run "rejects a foreign-namespace projectUrl sibling" (fun () ->
            let directory = scenarioDirectory "foreign project URL sibling"
            let target =
                rewriteMetadata directory (fun metadata ->
                    metadata.Add(XElement(XName.Get("projectUrl", "urn:foreign"), projectUrl)))
            expectError "foreign-namespace 'projectUrl' element" target)

        run "rejects a foreign-namespace repository sibling" (fun () ->
            let directory = scenarioDirectory "foreign repository sibling"
            let target =
                rewriteMetadata directory (fun metadata ->
                    let repository = XElement(XName.Get("repository", "urn:foreign"))
                    repository.Add(XAttribute(XName.Get "type", "git"))
                    repository.Add(XAttribute(XName.Get "url", repositoryUrl))
                    metadata.Add repository)
            expectError "foreign-namespace 'repository' element" target)

        run "rejects nested foreign XML inside projectUrl" (fun () ->
            let directory = scenarioDirectory "nested foreign project URL XML"
            let target =
                rewriteMetadata directory (fun metadata ->
                    let project = canonicalChild metadata "projectUrl"
                    project.RemoveNodes()
                    project.Add(XElement(XName.Get("nested", "urn:foreign"), projectUrl)))
            expectError "scalar text only" target)

        run "rejects nested foreign XML inside repository" (fun () ->
            let directory = scenarioDirectory "nested foreign repository XML"
            let target =
                rewriteMetadata directory (fun metadata ->
                    (canonicalChild metadata "repository").Add(XElement(XName.Get("nested", "urn:foreign"), "unexpected")))
            expectError "nested elements or non-whitespace text" target)

        run "rejects unexpected projectUrl attributes" (fun () ->
            let directory = scenarioDirectory "project URL attribute"
            let target =
                rewriteMetadata directory (fun metadata ->
                    (canonicalChild metadata "projectUrl").Add(XAttribute(XName.Get "source", "ambiguous")))
            expectError "must not contain attributes" target)

        run "rejects a namespace-qualified repository url attribute" (fun () ->
            let directory = scenarioDirectory "qualified repository URL attribute"
            let target =
                rewriteMetadata directory (fun metadata ->
                    (canonicalChild metadata "repository").Add(XAttribute(XName.Get("url", "urn:foreign"), "https://example.invalid/PipeL4.git")))
            expectError "unexpected or namespace-qualified attribute" target)

        run "rejects a namespace-qualified repository type attribute" (fun () ->
            let directory = scenarioDirectory "qualified repository type attribute"
            let target =
                rewriteMetadata directory (fun metadata ->
                    (canonicalChild metadata "repository").Add(XAttribute(XName.Get("type", "urn:foreign"), "svn")))
            expectError "unexpected or namespace-qualified attribute" target)

        run "rejects an incorrect project URL" (fun () ->
            let directory = scenarioDirectory "incorrect project URL"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, projectUrl, "https://example.invalid/PipeL4")) None
            expectError "Project URL must be" target)

        run "rejects an incorrect repository URL" (fun () ->
            let directory = scenarioDirectory "incorrect repository URL"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, repositoryUrl, "https://example.invalid/PipeL4.git")) None
            expectError "Repository URL must be" target)

        run "rejects an incorrect repository type" (fun () ->
            let directory = scenarioDirectory "incorrect repository type"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, "type=\"git\"", "type=\"svn\"")) None
            expectError "Repository type must be 'git'" target)

        run "rejects a missing repository URL" (fun () ->
            let directory = scenarioDirectory "missing repository URL"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, repositoryUrl, "")) None
            expectError "Repository URL must be" target)

        run "accepts the MIT expression without a license URL" (fun () ->
            let directory = scenarioDirectory "no license URL"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, sdkLicenseUrl, "")) None
            let licenseUrls = nuspecMetadata target nuspecEntry |> fun metadata -> directChildren metadata "licenseUrl"
            if licenseUrls.Length <> 0 then
                failwith "Test mutation did not remove the license URL."
            match validateInput target with
            | Ok _ -> ()
            | Error message -> failwith $"MIT expression without a license URL was rejected: {message}")

        run "rejects multiple package candidates in a directory" (fun () ->
            let directory = scenarioDirectory "multiple packages"
            File.Copy(sourcePath, Path.Combine(directory, Path.GetFileName sourcePath))
            File.Copy(sourcePath, Path.Combine(directory, "PipeL4.second-candidate.nupkg"))
            expectError "Expected exactly one .nupkg" directory)

        run "rejects a filename that disagrees with the nuspec version" (fun () ->
            let directory = scenarioDirectory "filename mismatch"
            let target = Path.Combine(directory, "PipeL4.different-version.nupkg")
            File.Copy(sourcePath, target)
            expectError "does not match nuspec version" target)

        run "accepts a changed prerelease version when filename and nuspec agree" (fun () ->
            let directory = scenarioDirectory "version independence"
            let changedVersion = "42.7.3-preview.19"
            let target = Path.Combine(directory, $"PipeL4.{changedVersion}.nupkg")
            let oldVersion = $"<version>{sourceSummary.Version}</version>"
            let newVersion = $"<version>{changedVersion}</version>"
            rewritePackage sourcePath target None (Some(nuspecEntry, oldVersion, newVersion)) None
            match validateInput target with
            | Ok summary when summary.Version = changedVersion -> ()
            | Ok summary -> failwith $"Expected version {changedVersion}, found {summary.Version}."
            | Error message -> failwith $"Consistent prerelease version was rejected: {message}")

        run "rejects a declared README missing from the package" (fun () ->
            let directory = scenarioDirectory "missing readme"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target (Some sourceSummary.ReadmePath) None None
            expectError "declared but missing" target)

        run "rejects a missing bundled dependency notice" (fun () ->
            let directory = scenarioDirectory "missing dependency notice"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target (Some "notices/DotNet.THIRD-PARTY-NOTICES.txt") None None
            expectError "missing bundled dependency notices" target)

        run "rejects a missing F# scripting runtime dependency" (fun () ->
            let directory = scenarioDirectory "missing scripting runtime"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target (Some "tools/net10.0/any/FSharp.Compiler.Service.dll") None None
            expectError "missing runtime files" target)

        run "rejects unexpected packaged files" (fun () ->
            let directory = scenarioDirectory "unexpected file"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None None (Some("temporary-output.txt", Encoding.UTF8.GetBytes "unexpected"))
            expectError "unexpected, test, benchmark, or temporary entries" target)

        run "rejects an unexpected package identity" (fun () ->
            let directory = scenarioDirectory "incorrect identity"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, "<id>PipeL4</id>", "<id>Other</id>")) None
            expectError "Expected PackageId 'PipeL4'" target)

        run "rejects a package without the MIT license expression" (fun () ->
            let directory = scenarioDirectory "incorrect license"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, mitLicense, "<license type=\"expression\">Apache-2.0</license>")) None
            expectError "Package license must be the MIT expression" target)

        run "rejects a foreign-namespace license replacing the MIT license" (fun () ->
            let directory = scenarioDirectory "foreign replacement license"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, mitLicense, foreignLicense)) None
            expectError "foreign-namespace 'license' element" target)

        run "rejects a foreign-namespace license alongside the MIT license" (fun () ->
            let directory = scenarioDirectory "foreign additional license"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, mitLicense, mitLicense + foreignLicense)) None
            expectError "foreign-namespace 'license' element" target)

        run "rejects a foreign-namespace licenseUrl with an unexpected value" (fun () ->
            let directory = scenarioDirectory "foreign license URL"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let foreignLicenseUrl = "<x:licenseUrl xmlns:x=\"urn:foreign\">https://example.invalid/license</x:licenseUrl>"
            rewritePackage sourcePath target None (Some(nuspecEntry, sdkLicenseUrl, foreignLicenseUrl)) None
            expectError "foreign-namespace 'licenseUrl' element" target)

        run "rejects foreign-namespace metadata alongside package metadata" (fun () ->
            let directory = scenarioDirectory "foreign metadata impersonation"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let foreignMetadata = "<x:metadata xmlns:x=\"urn:foreign\"><x:id>PipeL4</x:id></x:metadata></package>"
            rewritePackage sourcePath target None (Some(nuspecEntry, "</package>", foreignMetadata)) None
            expectError "foreign-namespace 'metadata' element" target)

        run "rejects a foreign-namespace package and metadata structure" (fun () ->
            let directory = scenarioDirectory "foreign package namespace"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage
                sourcePath
                target
                None
                (Some(nuspecEntry, $"xmlns=\"{sdkNuspecNamespace}\"", "xmlns=\"urn:foreign\""))
                None
            expectError "unsupported namespace 'urn:foreign'" target)

        run "rejects a case-changed NuGet namespace URI" (fun () ->
            let directory = scenarioDirectory "case-changed NuGet namespace"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let caseChangedNuspecNamespace = "http://schemas.microsoft.com/packaging/2012/06/Nuspec.xsd"
            rewritePackage
                sourcePath
                target
                None
                (Some(
                    nuspecEntry,
                    $"xmlns=\"{sdkNuspecNamespace}\"",
                    $"xmlns=\"{caseChangedNuspecNamespace}\""))
                None
            expectError $"unsupported namespace '{caseChangedNuspecNamespace}'" target)

        run "rejects duplicate identical license elements" (fun () ->
            let directory = scenarioDirectory "duplicate identical licenses"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, mitLicense, mitLicense + mitLicense)) None
            expectError "exactly one MIT expression license element" target)

        run "rejects MIT followed by a conflicting license element" (fun () ->
            let directory = scenarioDirectory "MIT then conflicting license"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let apacheLicense = "<license type=\"expression\">Apache-2.0</license>"
            rewritePackage sourcePath target None (Some(nuspecEntry, mitLicense, mitLicense + apacheLicense)) None
            expectError "exactly one MIT expression license element" target)

        run "rejects a conflicting license element before MIT" (fun () ->
            let directory = scenarioDirectory "duplicate conflicting licenses"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let apacheLicense = "<license type=\"expression\">Apache-2.0</license>"
            rewritePackage sourcePath target None (Some(nuspecEntry, mitLicense, apacheLicense + mitLicense)) None
            expectError "exactly one MIT expression license element" target)

        run "rejects duplicate identical licenseUrl elements" (fun () ->
            let directory = scenarioDirectory "duplicate identical license URLs"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, sdkLicenseUrl, sdkLicenseUrl + sdkLicenseUrl)) None
            expectError "at most one licenseUrl metadata element" target)

        run "rejects the expected license URL followed by a conflicting URL" (fun () ->
            let directory = scenarioDirectory "expected then conflicting license URLs"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let unexpectedUrl = "<licenseUrl>https://example.invalid/license</licenseUrl>"
            rewritePackage sourcePath target None (Some(nuspecEntry, sdkLicenseUrl, sdkLicenseUrl + unexpectedUrl)) None
            expectError "at most one licenseUrl metadata element" target)

        run "rejects a conflicting license URL before the expected URL" (fun () ->
            let directory = scenarioDirectory "duplicate conflicting license URLs"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let unexpectedUrl = "<licenseUrl>https://example.invalid/license</licenseUrl>"
            rewritePackage sourcePath target None (Some(nuspecEntry, sdkLicenseUrl, unexpectedUrl + sdkLicenseUrl)) None
            expectError "at most one licenseUrl metadata element" target)

        run "rejects an unexpected license URL" (fun () ->
            let directory = scenarioDirectory "incorrect license URL"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage sourcePath target None (Some(nuspecEntry, "https://licenses.nuget.org/MIT", "https://example.invalid/license")) None
            expectError "Unexpected nuspec licenseUrl metadata" target)

        run "rejects a tool settings document with an invalid root" (fun () ->
            let directory = scenarioDirectory "incorrect tool settings root"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage
                sourcePath
                target
                None
                (Some("tools/net10.0/any/DotnetToolSettings.xml", "DotNetCliTool", "InvalidRoot"))
                None
            expectError "root must be the unqualified element 'DotNetCliTool'" target)

        run "the .NET tool installer rejects an invalid tool settings root" (fun () ->
            let directory = scenarioDirectory "installer rejects invalid tool settings root"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            let toolPath = Path.Combine(directory, "installed tool")
            rewritePackage
                sourcePath
                target
                None
                (Some("tools/net10.0/any/DotnetToolSettings.xml", "DotNetCliTool", "InvalidRoot"))
                None
            expectError "root must be the unqualified element 'DotNetCliTool'" target
            let exitCode, standardOutput, standardError =
                runToolInstallFromOutsideRepository
                    outsideRepositoryDirectory
                    directory
                    toolPath
                    sourceSummary.Version
            let diagnostic = standardOutput + standardError
            if exitCode = 0
               || not (diagnostic.Contains("error in XML document", StringComparison.OrdinalIgnoreCase)) then
                failwith $"Expected installer rejection for malformed tool settings, got exit {exitCode}: {diagnostic}")

        run "rejects a command outside the direct Commands container" (fun () ->
            let directory = scenarioDirectory "incorrect command container"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            rewritePackage
                sourcePath
                target
                None
                (Some("tools/net10.0/any/DotnetToolSettings.xml", "Commands", "WrongContainer"))
                None
            expectError "direct 'Commands' element" target)

        run "rejects a corrupt package archive" (fun () ->
            let directory = scenarioDirectory "corrupt archive"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            File.WriteAllBytes(target, Encoding.UTF8.GetBytes "not a NuGet ZIP archive")
            expectError "NuGet ZIP package" target)

        run "rejects an entry whose ZIP CRC no longer matches" (fun () ->
            let directory = scenarioDirectory "incorrect ZIP CRC"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            writePackageWithIncorrectEntryCrc sourcePath target
            match validateInput target with
            | Error message when message.Contains("corrupt entry", StringComparison.Ordinal)
                                 || message.Contains("CRC32 checksum of the extracted data does not match the expected value from the archive", StringComparison.Ordinal) -> ()
            | Error message -> failwith $"Expected an entry corruption diagnostic, but got: {message}"
            | Ok summary -> failwith $"Expected rejection of a corrupt entry, but {summary.PackagePath} passed.")

        run "CLI validates a package path with spaces from outside the repository" (fun () ->
            let directory = scenarioDirectory "CLI package path with spaces"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            File.Copy(sourcePath, target)
            let exitCode, standardOutput, standardError =
                runValidatorFromOutsideRepository outsideRepositoryDirectory target
            if exitCode <> 0 || not (standardOutput.Contains("PackageId=PipeL4", StringComparison.Ordinal)) then
                failwith $"Expected CLI success (exit 0), got {exitCode}. stdout: {standardOutput} stderr: {standardError}")

        run "CLI returns a nonzero exit with an actionable invalid-archive diagnostic" (fun () ->
            let directory = scenarioDirectory "CLI corrupt path with spaces"
            let target = Path.Combine(directory, Path.GetFileName sourcePath)
            File.WriteAllBytes(target, Encoding.UTF8.GetBytes "not a NuGet ZIP archive")
            let exitCode, _, standardError = runValidatorFromOutsideRepository outsideRepositoryDirectory target
            if exitCode = 0 || not (standardError.Contains("NuGet ZIP package", StringComparison.Ordinal)) then
                failwith $"Expected nonzero CLI exit and archive diagnostic, got {exitCode}. stderr: {standardError}")

        if injectFailure then
            run "controlled exit-code regression failure" (fun () ->
                failwith "controlled failure injected to verify dotnet fsi exit status")
    finally
        if Directory.Exists temporaryRoot then
            Directory.Delete(temporaryRoot, true)

    if Directory.Exists temporaryRoot then
        failed <- failed + 1
        eprintfn "FAIL temporary test artifacts were not cleaned up: %s" temporaryRoot
    else
        printfn "PASS temporary test artifacts cleaned up"

    printfn "Package validation tests: %d passed, %d failed." passed failed
    if failed = 0 then 0 else 1

let main () =
    let arguments = fsi.CommandLineArgs |> Array.skip 1

    if arguments.Length = 1 && arguments[0] = "--probe-unexpected-exception" then
        failwith "controlled unexpected exception to verify dotnet fsi exit status"
    elif arguments.Length <> 1 then
        usage ()
        2
    else
        match validateInput arguments[0] with
        | Error message ->
            eprintfn "Package validation test setup failed: %s" message
            1
        | Ok summary ->
            let injectFailure =
                Environment.GetEnvironmentVariable("PIPEL4_TEST_VALIDATE_PACKAGE_INJECT_FAILURE") = "1"
            runTests summary injectFailure

let exitStatus =
    try
        main ()
    with error ->
        eprintfn "Unexpected package validation test error: %s" error.Message
        2

Environment.Exit exitStatus
