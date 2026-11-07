module internal PipeL4.ExampleDistribution

open System
open System.IO
open System.Reflection

[<NoEquality; NoComparison>]
type private Example =
    { Name: string
      FileName: string
      ResourceName: string }

[<NoEquality; NoComparison>]
type private CopyOptions =
    { OutputDirectory: string option
      Force: bool }

let private examples =
    [| { Name = "AsciiUppercase"
         FileName = "AsciiUppercase.fsx"
         ResourceName = "PipeL4.Examples.AsciiUppercase.fsx" }
       { Name = "BytePatternReplace"
         FileName = "BytePatternReplace.fsx"
         ResourceName = "PipeL4.Examples.BytePatternReplace.fsx" }
       { Name = "PrefixMode"
         FileName = "PrefixMode.fsx"
         ResourceName = "PipeL4.Examples.PrefixMode.fsx" } |]

let private help =
    String.concat
        Environment.NewLine
        [ "Usage: pipel4 examples <command>"
          ""
          "Commands:"
          "  list                                      List bundled examples."
          "  copy <name> [--output-dir <directory>]    Copy an example into a directory."
          "        [--force]"
          "  --help, -h                                Show this help."
          ""
          "Example names are case-sensitive. The output directory must already exist."
          "Existing files are preserved unless --force is supplied." ]

let private usage = "Usage: pipel4 examples copy <name> [--output-dir <directory>] [--force]"

let private error exitCode message =
    eprintfn "Error: %s" message
    exitCode

let private readResource (example: Example) =
    let assembly = Assembly.GetExecutingAssembly()

    match assembly.GetManifestResourceStream(example.ResourceName) |> Option.ofObj with
    | None -> Error $"Bundled example resource '{example.ResourceName}' is missing."
    | Some resource ->
        use resource = resource
        use content = new MemoryStream()
        resource.CopyTo content
        Ok(content.ToArray())

let private parseCopyOptions (arguments: string list) =
    let rec parse remaining outputDirectory force =
        match remaining with
        | [] ->
            Ok
                { OutputDirectory = outputDirectory
                  Force = force }
        | "--force" :: tail when not force -> parse tail outputDirectory true
        | "--force" :: _ -> Error "Option '--force' may only be supplied once."
        | "--output-dir" :: directory :: tail
            when not (String.IsNullOrWhiteSpace directory)
                 && not (directory.StartsWith("--", StringComparison.Ordinal)) ->
            match outputDirectory with
            | Some _ -> Error "Option '--output-dir' may only be supplied once."
            | None -> parse tail (Some directory) force
        | "--output-dir" :: _ -> Error "Option '--output-dir' requires a directory argument."
        | option :: _ -> Error $"Unknown examples copy option '{option}'."

    parse arguments None false

let private inspectDestination (path: string) =
    let info = FileInfo(path)

    if not (isNull info.LinkTarget) then
        Error "The destination is a symbolic link; refusing to replace it."
    else
        try
            let attributes = File.GetAttributes path

            if attributes.HasFlag(FileAttributes.ReparsePoint) then
                Error "The destination is a reparse point; refusing to replace it."
            elif attributes.HasFlag(FileAttributes.Directory) then
                Error "The destination path is a directory."
            else
                Ok true
        with
        | :? FileNotFoundException -> Ok false
        | :? DirectoryNotFoundException -> Ok false

let private extract (example: Example) (options: CopyOptions) =
    let requestedDirectory = options.OutputDirectory |> Option.defaultValue Environment.CurrentDirectory
    let outputDirectory = Path.GetFullPath requestedDirectory

    if not (Directory.Exists outputDirectory) then
        Error $"Output directory '{outputDirectory}' does not exist or is not a directory."
    else
        let destination = Path.Combine(outputDirectory, example.FileName)

        match inspectDestination destination with
        | Error message -> Error message
        | Ok true when not options.Force ->
            Error $"Destination '{destination}' already exists. Use --force to overwrite it."
        | Ok _ ->
            match readResource example with
            | Error message -> Error message
            | Ok bytes ->
                let temporaryPath =
                    Path.Combine(outputDirectory, $".{example.FileName}.{Guid.NewGuid():N}.tmp")

                let mutable temporaryFileExists = false

                let writeTemporaryFile () =
                    use output =
                        new FileStream(
                            temporaryPath,
                            FileMode.CreateNew,
                            FileAccess.Write,
                            FileShare.None,
                            4096,
                            FileOptions.WriteThrough
                        )

                    temporaryFileExists <- true
                    output.Write(bytes, 0, bytes.Length)
                    output.Flush(true)

                try
                    // The nested scope disposes the exclusive stream before the rename.
                    writeTemporaryFile ()
                    File.Move(temporaryPath, destination, options.Force)
                    temporaryFileExists <- false
                    Ok destination
                finally
                    if temporaryFileExists then
                        try
                            File.Delete temporaryPath
                        with _ -> ()

let private copyExample name optionArguments =
    match parseCopyOptions optionArguments with
    | Error message -> error 2 $"{message} {usage}"
    | Ok options ->
        match examples |> Array.tryFind (fun example -> String.Equals(example.Name, name, StringComparison.Ordinal)) with
        | None ->
            error 2 $"Unknown example '{name}'. Run 'pipel4 examples list' to see available names."
        | Some example ->
            try
                match extract example options with
                | Ok destination ->
                    printfn "Copied %s to %s" example.FileName destination
                    0
                | Error message -> error 1 message
            with
            | :? IOException as failure -> error 1 $"Could not copy example: {failure.Message}"
            | :? UnauthorizedAccessException as failure -> error 1 $"Could not copy example: {failure.Message}"
            | :? ArgumentException as failure -> error 1 $"Could not copy example: {failure.Message}"
            | :? NotSupportedException as failure -> error 1 $"Could not copy example: {failure.Message}"
            | :? System.Security.SecurityException as failure -> error 1 $"Could not copy example: {failure.Message}"

let internal run (arguments: string array) =
    match Array.toList arguments with
    | [ "--help" ]
    | [ "-h" ] ->
        printfn "%s" help
        0
    | [ "list" ] ->
        examples |> Array.iter (fun example -> printfn "%s" example.Name)
        0
    | "copy" :: name :: optionArguments -> copyExample name optionArguments
    | _ -> error 2 $"{usage}. Use 'pipel4 examples --help' for details."
