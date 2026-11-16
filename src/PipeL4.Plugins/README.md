# PipeL4.Plugins

`PipeL4.Plugins` is the .NET 10 library containing PipeL4's public F# processor contracts. It lets
F# projects compile against `IByteStreamProcessor`, `ProcessorInput`, `ProcessorResult`,
`IProcessorOutput`, and `IConnectionState` without referencing the PipeL4 runtime or script host.

The package includes the contract assembly and declares its FSharp.Core dependency. It does not
include or install the PipeL4 command-line tool. See the [PipeL4 repository](https://github.com/jbbfett/PipeL4)
for the runtime's current processor and ownership rules.
