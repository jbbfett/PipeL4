# PipeL4

PipeL4 is an experimental TCP byte-stream proxy with programmable F# processing pipelines.
It targets .NET 10 and requires the .NET 10 runtime.

Install the tool with `dotnet tool install --global PipeL4`. To install a prerelease, add
`--prerelease`. The command is `pipel4`.

Run `pipel4 --help` for listener, backend, and processor options. Processor scripts are F# `.fsx`
files and run as trusted code in the PipeL4 process; they are not sandboxed.
