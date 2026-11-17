# PipeL4.Plugins

`PipeL4.Plugins` is the .NET 10 library containing PipeL4's public F# processor contracts. It lets
F# projects compile against `IByteStreamProcessor`, `ProcessorInput`, `ProcessorResult`,
`IProcessorOutput`, and `IConnectionState` without referencing the PipeL4 runtime or script host.

The package includes the contract assembly and declares its FSharp.Core dependency. It does not
include or install the PipeL4 command-line tool. See the [PipeL4 repository](https://github.com/jbbfett/PipeL4)
for the runtime's current processor and ownership rules.

`PipeL4` is distributed as a .NET tool, while `PipeL4.Plugins` is its standalone public API NuGet
library. The tool already carries the public contract assembly for its script host, so users do not
need to install this library separately to run `.fsx` plugins. The library is for other .NET/F#
consumers and for F# Interactive or editor type checking.

Both packages use the single `PipeL4PackageVersion` property and are released together. The
previously published `0.1.0-alpha.1` package is the `PipeL4` tool release. At this checkout,
`0.1.0-alpha.2` is the local, unpublished candidate version for both packages; it is not available
from NuGet.org.

For F# script authoring, guard the package reference so F# Interactive can restore the public
contract assembly while PipeL4 compiles the same script against its own assembly:

```fsharp
#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif
```

The candidate version above is local and unpublished. See the repository's
[F# script authoring guide](https://github.com/jbbfett/PipeL4#f-script-authoring) for local-candidate
validation, VS Code/Ionide setup, the accepted directive restrictions, and how to run the same
script through PipeL4.
