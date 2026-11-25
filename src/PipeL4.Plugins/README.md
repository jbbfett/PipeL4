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

## Functional processors

The script host discovers functional processors and binds their functions when loading the script.
Only `handle` is mandatory. Open `PipeL4.Plugins`; its automatically opened `FunctionalProcessor`
module supplies the helpers. A complete stateless script is:

```fsharp
#if INTERACTIVE
#r "nuget: PipeL4.Plugins, 0.1.0-alpha.2"
#endif

open PipeL4.Plugins

let handle ctx =
    emitInput ctx 0L ctx.Input.Length
    releaseAll
```

Use it with the existing `--client-to-server-plugin ./Passthrough.fsx` or
`--server-to-client-plugin ./Passthrough.fsx` options. No internal Core or PluginHost reference is
needed. The functional API described here is in the current source checkout. The version property
still reads `0.1.0-alpha.2`; that does **not** establish that previously published packages contain
these additions. Build and validate the matching tool and API candidates locally using the
[repository authoring instructions](https://github.com/jbbfett/PipeL4#functional-processors).
Do not mix an older tool or API binary with these scripts.

### Entry points and typed state

| Definition | Supported return types | Requirement |
| --- | --- | --- |
| `handle ctx` | `ProcessorResult`, `Task<ProcessorResult>`, `ValueTask<ProcessorResult>` | Stateless form |
| `create ()` or `create (ctx: CreateContext)` | `'State` | Required for stateful handling; synchronous only |
| `handle ctx (state: 'State)` | `ProcessorResult`, `Task<ProcessorResult>`, `ValueTask<ProcessorResult>` | Curried stateful form |
| `dispose (state: 'State)` | `unit`, `Task<unit>`, `ValueTask`, `ValueTask<unit>` | Optional with stateful handling |

Task and ValueTask types are in `System.Threading.Tasks`. State types must agree exactly across
creation, handling, and disposal. Closed generic and value types are supported; open generic
entry points, byref-like state, tupled `handle (ctx, state)`, and async creation are unsupported.
A stateless handler cannot declare `create` or `dispose`.

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

`create ()` is a function because it runs once for **each stage instance**, eagerly and
synchronously during connection pipeline construction, even if that stage never receives input.
A top-level mutable value is initialized with the script and can be shared by connections.
Allocate fresh mutable state inside `create`; returning a shared object does not clone it. Repeated
stages, opposite directions, and separate connections receive independent creation calls. There
is no public `StageState` type or stage-state registry. The adapter keeps the returned value typed
through invocation and cleanup. For a retaining processor, `ctx.Input.Length` includes previously
retained bytes: the counter above is appropriate for its non-retaining passthrough behavior.

`CreateContext.ConnectionState` and `ProcessContext.ConnectionState` expose the same non-owning
`IConnectionState` registry to every stage in both directions of a connection. A new connection
gets a new registry. Use `create (ctx: CreateContext)` to obtain a shared value with `GetOrAdd`,
or access it from `handle`. Shared mutable values require synchronization because directions may
run concurrently. Keys use ordinal strings and exact .NET types; independently declared script
types do not gain common identity by having the same shape. The registry owns its published
values and cleans them up after the directional pipelines finish; plugins must not dispose the
registry or a value it owns. See the [state contract](https://github.com/jbbfett/PipeL4/blob/main/docs/architecture.md#state).

Explicit `dispose state` is optional. Returned local state is **never automatically disposed**,
even if it implements `IDisposable` or `IAsyncDisposable`; its fields are not recursively disposed.
Write explicit cleanup for local resources you own. Cleanup runs through the existing processor
lifetime, in reverse stage order within each direction, and asynchronous cleanup is awaited.
It runs after normal completion, failure, or cancellation. A creation failure has no returned
state to clean up; earlier successfully constructed stages still receive cleanup attempts.
Cleanup failures do not prevent attempts on remaining stages. Avoid publishing the same owned
resource in both local and shared state and disposing it twice.

### Context, emission, and retention

`ProcessContext` is an immutable struct record containing `Input: ReadOnlySequence<byte>`,
`IsCompleted: bool`, `Output: IProcessorOutput`, `CancellationToken`, and `ConnectionState`.
`CreateContext` is an immutable struct record containing `ConnectionState`. Neither record
supports equality or comparison. Input is a potentially segmented TCP byte stream, not a complete
message, line, or frame. Do not flatten it just to inspect bytes: iterate segments or use a
`SequenceReader<byte>` in synchronous code.

| Helper | Meaning |
| --- | --- |
| `emitInput ctx offset length` | Append a borrowed range of the current input, with `int64` offset/length and no payload copy |
| `emitBytes ctx bytes` | Copy a `ReadOnlySpan<byte>` directly into runtime-owned output and commit it |
| `releaseAll` | Return `{ RetainedLength = 0L }`; emit nothing by itself |
| `retainSuffix length` | Return `{ RetainedLength = length }`; emit nothing by itself |

Calls emit in order, including repeated or overlapping borrowed ranges mixed with generated bytes.
Emission and retention are independent: emitting a range does not release it, and returning
`releaseAll` does not forward any bytes. `retainSuffix n` asks the runtime to keep the final `n`
input bytes and join them with future input. Everything before that suffix is released after
successful validation. Emitting and retaining the same bytes is allowed, but emitting them again
on a later invocation duplicates output unless your algorithm accounts for it.

```fsharp
open PipeL4.Plugins

// Bounded four-byte delay. Earlier bytes flow while a suffix remains retained.
let handle (ctx: ProcessContext) =
    ctx.CancellationToken.ThrowIfCancellationRequested()
    let retained = if ctx.IsCompleted then 0L else min 4L ctx.Input.Length
    emitInput ctx 0L (ctx.Input.Length - retained)
    retainSuffix retained
```

At normal EOF, flush or deliberately discard unresolved bytes and return zero retention.
`IsCompleted` means normal upstream completion only: cancellation or failure does not synthesize
an EOF call. Helpers do not add cancellation checks; observe the token cooperatively. Retention
must be within the current input length and zero at EOF, or the runtime rejects the result.
Output is staged until the invocation succeeds and the result is valid. The runtime controls
when downstream stages execute; helpers do not invoke downstream code or schedule future work.
There are no timers or spontaneous/background emissions.

For `emitBytes`, use `System.ReadOnlySpan<byte>(bytes)` or
`System.ReadOnlySpan<byte>(bytes, offset, count)` for arrays/slices to avoid implicit-conversion
warnings, or pass `memory.Span`. Empty spans request no writable storage. Pass both arguments
together; spans cannot be captured in ordinary first-class F# function values.

Advanced processors can use `ctx.Output.GetSpan`, `GetMemory`, and `Advance` directly, as in the
uppercase and bounded replacement examples. They write directly into runtime storage without an
intermediate application array. Advance any outstanding writable memory before another output
operation. After `Advance`, never read or write that memory again. A size hint is a minimum;
physical accounting charges the full rented capacity.

Input memory, the output object, the context, and any writable memory must not escape the
invocation, including into local/connection state or background tasks. They remain valid until
an asynchronous handler completes, including across suspension. Uncommitted `GetMemory` memory
can survive an `await` inside that invocation; reacquire its span afterward. Spans and
`SequenceReader` are byref-like and cannot be kept across suspension. The runtime awaits the
handler before validation, downstream execution, or releasing input. Cancellation does not make
it safe to use memory after completion.

Generated writes are synchronous and can fail immediately on capacity exhaustion; an async
handler does not make `GetMemory` or `emitBytes` awaitable. Downstream sink backpressure remains
awaited between input chunks. Runtime logical accounting, physical ownership, and cleanup are
the same as for class processors.

### F# inference and editor authoring

Basic `let handle ctx = emitInput ctx 0L ctx.Input.Length; releaseAll` infers `ProcessContext`
without annotations or reserved-word warnings. Annotate the state argument when member lookup
needs it, such as `(state: int64 ref)` above. There is one reproducible record-field inference
limitation: an unannotated **first** use of `ctx.IsCompleted` may select the older `ProcessorInput`
type before F# sees the functional helper. Annotate that handler:

```fsharp
open PipeL4.Plugins

let handle (ctx: ProcessContext) =
    if ctx.IsCompleted then
        emitInput ctx 0L ctx.Input.Length
        releaseAll
    else
        retainSuffix ctx.Input.Length
```

This snippet demonstrates inference only; retaining an entire stream until EOF is unbounded and
is unsuitable for long-lived streams. Use a bounded suffix as in `RetainedTail.fsx` for streaming.
The intentionally failing
[reproduction](https://github.com/jbbfett/PipeL4/blob/main/tests/fixtures/functional/ContextInferenceRepro.fsx)
is checked by both FSI and production compiler tests; the annotated form succeeds. Ordinary
handlers do not all require an annotation. The entry point name remains `handle`.

### Examples and class-based compatibility

[Functional examples](https://github.com/jbbfett/PipeL4/tree/main/examples/functional) use only
public contracts and the guarded package reference above:

| Script | Behavior and corresponding class example |
| --- | --- |
| `AsciiUppercase.fsx` | Stateless, segmented ASCII transformation; preserves other binary bytes, like `examples/AsciiUppercase.fsx` |
| `PrefixMode.fsx` | Fresh typed mode per stage; retains fewer than four initial bytes, borrows the prefix, then passes through or masks digits, like `examples/PrefixMode.fsx` |
| `BytePatternReplace.fsx` | Replaces `HELLO` with `WORLD`, with bounded local prefix state, generated output in batches of at most 256 requested bytes, and incomplete-prefix EOF flush, like `examples/BytePatternReplace.fsx` |
| `RetainedTail.fsx` | Stateless input-backed emission while retaining a bounded suffix; flushes it at normal EOF |
| `AsyncCount.fsx` | Typed local counter, awaited EOF diagnostic I/O, input-backed output, and explicit asynchronous disposal |

`HELLO` has no non-empty prefix that is also a suffix. The replacement example retries a
mismatching byte as a possible new start (`HEHELLO` becomes `HEWORLD`); it is not a general matcher
for arbitrary self-overlapping patterns. It keeps only a matched-prefix length, emits all output
as generated bytes, and requests no runtime retention. PrefixMode preserves the first four bytes
and makes its mode decision only once; shorter input flushes unchanged at EOF. Neither example
assumes socket reads align with patterns or prefixes. See also the existing
[Passthrough](https://github.com/jbbfett/PipeL4/blob/main/tests/fixtures/functional/Passthrough.fsx) and
[Counter](https://github.com/jbbfett/PipeL4/blob/main/tests/fixtures/functional/Counter.fsx) fixtures;
these simple examples are reused rather than copied into another directory.

Existing class scripts remain supported. Exactly one visible concrete processor class is selected
by the interface and takes precedence over functions; an invalid or ambiguous class is an error.
Without a processor class, define one top-level `handle`, with optional `create` and `dispose` in
the same top-level module. Nested modules may contain helpers. Do not put both example forms in
one file if you intend to run the functional one.

For example, class-based passthrough:

```fsharp
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
```

Its functional equivalent is the minimal script at the start of this section. Stateful counting
with explicit disposal can be written as:

```fsharp
open System
open System.Threading.Tasks
open PipeL4.Plugins

type Processor() =
    let mutable total = 0L
    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            total <- total + input.Bytes.Length
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
    interface IDisposable with
        member _.Dispose() = printfn "Total: %d" total
```

The `create`/`handle`/`dispose` counter above provides the same behavior. The full class and
functional `PrefixMode.fsx` examples provide the third comparison: retention, borrowed prefix,
and generated masked output. Functional code removes processor class/interface members,
constructor-held mutable fields, and synchronous `ValueTask` result wrapping. It preserves
ordering, stage isolation, cancellation, memory lifetimes, retention validation, cleanup, and
backpressure. Shorter syntax alone implies no performance improvement. The
[benchmark record](https://github.com/jbbfett/PipeL4/blob/main/benchmarks/PipeL4.Benchmarks/README.md#compiled-functional-script-binding)
reports measured allocations, copies, logical buffering, physical capacity, and the limits of
its ShortRun throughput/latency evidence. Loading binds typed delegates once; handling adds no
reflection, dynamic state boxing, mandatory input flattening, or Task allocation on the synchronous
fast path. Source-authored asynchronous work and closures can still allocate.
