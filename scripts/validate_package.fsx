#load "PackageValidation.fsx"

open System
open PackageValidation

let arguments = fsi.CommandLineArgs |> Array.skip 1

let main () =
    if arguments.Length <> 1 then
        eprintfn "Usage: dotnet fsi scripts/validate_package.fsx <package-or-output-directory>"
        2
    else
        match validateInput arguments[0] with
        | Ok summary ->
            printfn
                "Validated %s: PackageId=%s, version=%s, target=%s, command=%s"
                (IO.Path.GetFileName summary.PackagePath)
                summary.PackageId
                summary.Version
                summary.TargetFramework
                summary.ToolCommand
            0
        | Error message ->
            eprintfn "Package validation failed: %s" message
            1

let exitStatus =
    try
        main ()
    with error ->
        eprintfn "Unexpected package validator error: %s" error.Message
        2

Environment.Exit exitStatus
