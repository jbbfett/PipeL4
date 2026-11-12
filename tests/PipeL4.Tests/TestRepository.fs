namespace PipeL4.Tests

open System
open System.IO

module TestRepository =
    let repositoryRoot =
        let rec find (directory: DirectoryInfo option) =
            match directory with
            | None ->
                failwithf
                    "Could not find the PipeL4 repository root above test output '%s'."
                    AppContext.BaseDirectory
            | Some current ->
                let solutionPath = Path.Combine(current.FullName, "PipeL4.sln")
                let applicationProjectPath =
                    Path.Combine(current.FullName, "src", "PipeL4", "PipeL4.fsproj")

                if File.Exists solutionPath && File.Exists applicationProjectPath then
                    current.FullName
                else
                    find (current.Parent |> Option.ofObj)

        find (Some(DirectoryInfo(AppContext.BaseDirectory)))
