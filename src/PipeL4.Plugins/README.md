# PipeL4.Plugins

`PipeL4.Plugins` is the .NET 10 library containing PipeL4's public F# processor contracts. Version
`0.1.0-alpha.2` matches the `PipeL4` .NET Tool release. See the exact-version
[PipeL4.Plugins NuGet page](https://www.nuget.org/packages/PipeL4.Plugins/0.1.0-alpha.2).

F# projects targeting .NET 10 can compile against `IByteStreamProcessor`, `ProcessorInput`,
`ProcessorResult`, `IProcessorOutput`, and `IConnectionState` without referencing the PipeL4 runtime
or script host. The .NET 10 SDK is required to build a consumer; applications need a compatible
.NET 10 runtime.

The package includes the contract assembly and declares its FSharp.Core dependency. It does not
include or install the PipeL4 command-line tool. See the [PipeL4 repository](https://github.com/jbbfett/PipeL4)
for the runtime's current processor and ownership rules.

`PipeL4` is distributed as a .NET tool, while `PipeL4.Plugins` is its standalone public API NuGet
library. The tool already carries the public contract assembly for its script host, so users do not
need to install this library separately to run `.fsx` plugins. The library is for other .NET/F#
consumers and for F# Interactive or editor type checking.

Both packages use the single `PipeL4PackageVersion` property and are released together. The matching
[`PipeL4` 0.1.0-alpha.2 tool package](https://www.nuget.org/packages/PipeL4/0.1.0-alpha.2) bundles
the same contract assembly for scripts run by `pipel4`.

For F# script authoring, guard the package reference so F# Interactive can restore the public
contract assembly while PipeL4 compiles the same script against its own assembly:

```fsharp
#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif
```

The guarded package reference is for F# Interactive and editor type checking. PipeL4 compiles out
that reference and supplies the matching bundled assembly when it runs the script. Host-run scripts
may use standard .NET/F# libraries and PipeL4 APIs. The host accepts only this constrained reference
form; arbitrary package dependencies, assembly paths, `#load`, and `#I` are unsupported. See the
repository's [F# script authoring guide](https://github.com/jbbfett/PipeL4#f-script-authoring) for
VS Code/Ionide setup, the accepted directive restrictions, and how to run the same script through
PipeL4.
