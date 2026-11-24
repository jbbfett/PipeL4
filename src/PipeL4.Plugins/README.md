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

## Functional processor contracts

The public library also provides `CreateContext`, `ProcessContext`, and the automatically opened
`FunctionalProcessor` module. These are contracts and helpers only: the script host still requires
an `IByteStreamProcessor` class. It does not discover or invoke functional `create`, `handle`, or
`dispose` functions yet. The snippets below illustrate intended authoring syntax and can be
typechecked against the contract assembly; they are not standalone runnable PipeL4 plugins.

Both contexts are immutable struct records with no equality or comparison:

```fsharp
type CreateContext =
    { ConnectionState: IConnectionState }

type ProcessContext =
    { Input: System.Buffers.ReadOnlySequence<byte>
      IsCompleted: bool
      Output: IProcessorOutput
      CancellationToken: System.Threading.CancellationToken
      ConnectionState: IConnectionState }
```

Opening `PipeL4.Plugins` makes these helpers available:

```fsharp
val emitInput: ctx: ProcessContext -> offset: int64 -> length: int64 -> unit
val emitBytes: ctx: ProcessContext -> bytes: System.ReadOnlySpan<byte> -> unit
val releaseAll: ProcessorResult
val retainSuffix: length: int64 -> ProcessorResult
```

For example, a stateless input-backed processor can be expressed as:

```fsharp
open PipeL4.Plugins

let handle ctx =
    emitInput ctx 0L ctx.Input.Length
    releaseAll
```

An intended stateful form passes local state explicitly:

```fsharp
open PipeL4.Plugins

let create () = ref 0L

let handle ctx (state: int64 ref) =
    state.Value <- state.Value + ctx.Input.Length
    emitInput ctx 0L ctx.Input.Length
    releaseAll

let dispose (state: int64 ref) =
    printfn "Total: %d" state.Value
```

F# needs the local-state type annotation before resolving `.Value`. `handle` is the single
supported functional entry point for future ScriptCompiler discovery. It requires no backticks
and compiles without a reserved-word warning.

The future lifecycle supports either `create () : 'State` or
`create (ctx: CreateContext) : 'State`, a mandatory `handle` with or without an explicit state
argument, and optional `dispose (state: 'State) : unit`. A future adapter will keep a fresh strongly
typed state value per processor instance and pass it to `handle` and explicit `dispose`. Local
state will not be automatically disposed, even if it implements disposal interfaces. There is no
local-state registry. `IConnectionState` remains a separate runtime-managed, connection-wide
registry; mutable values shared across concurrent directions require their own synchronization.
Only the contracts are present here; async handler/disposal return forms, adapter storage,
discovery, invocation, and lifecycle tests remain deferred. The contexts are ordinary structs,
not byref-like types, so they do not prevent future asynchronous invocation forms.

`Input` is the original segmented, non-owning sequence. `emitInput` forwards its offset and length
directly to `Output.AppendInput`, preserving input-backed zero-copy emission, repetition, and
ordering with generated output. `emitBytes` makes one copy directly from its span into
`Output.GetSpan(bytes.Length)`, then calls `Advance(bytes.Length)`; it creates no intermediate
array. Empty spans request no storage and emit nothing. For arrays or slices, explicit
`System.ReadOnlySpan<byte>(bytes)` or `System.ReadOnlySpan<byte>(bytes, offset, count)` avoids
implicit-conversion warnings; read-only memory can be passed as `memory.Span`. Pass both arguments
to `emitBytes` together: a span cannot be captured in an ordinary F# first-class function value.
Direct `ctx.Output.GetSpan`, `GetMemory`, and `Advance` remain available. Advance previously
requested writable memory before another output operation, and never use that memory afterward.

Emission does not select retention. `releaseAll` is `{ RetainedLength = 0L }`, and
`retainSuffix length` is `{ RetainedLength = length }`. Neither helper emits, schedules work, or
validates the result. The runtime still validates the suffix against the current input and requires
zero retention at normal upstream completion. `IsCompleted` describes normal completion only;
cancellation and failure do not synthesize EOF. Helpers do not add cancellation checks or wrap
output failures; processor code can observe `ctx.CancellationToken` cooperatively.

The context and all invocation input/output memory must not escape the invocation, including into
local state or connection state. A future async invocation must preserve the existing lifetime
through completion, including across suspension. Runtime memory accounting, synchronous output
capacity failures, and downstream backpressure continue to belong to the existing runtime.

There is no runtime execution path for functional processors to benchmark yet. Throughput,
latency, allocations, copied-byte ratio, logical buffering, and physical capacity benchmarks for
that path are deferred until the adapter exists. These helpers only forward existing output
operations or construct the existing result struct; the contexts require no heap allocation when
used directly. No SDK, package alignment, runtime dependency, or class-based processor behavior
changes are required.
