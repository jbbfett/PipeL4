# PipeL4

PipeL4 is an experimental high-performance TCP proxy with programmable F# processing pipelines.

It explores protocol-agnostic byte-stream processing for integration testing, protocol experiments, and runtime research on .NET.

> **Status:** experimental. Architecture, runtime semantics, and plugin APIs may change as they are validated through implementation and benchmarks.

## Core ideas

- TCP only.
- Implemented in F#; project dependencies are managed with Paket.
- Backend is defined by configuration.
- Client → server and server → client use separate ordered pipelines and may run concurrently.
- Plugins are F# `.fsx` scripts compiled when loaded.
- Plugins may inspect, transform, drop, delay, buffer, and frame data.
- The runtime owns pipeline memory, buffering, backpressure, and execution.
- Plugins are trusted code; PipeL4 does not provide a sandbox.
- Plugin code may be analyzed for expensive patterns and reported through diagnostics/warnings. Automatic optimization is out of scope.

## Scope

The current scope is the smallest useful end-to-end system: accept a TCP connection, proxy it to a
configured backend through independent directional processor pipelines, load F# `.fsx` plugins, and
verify that runtime-managed buffering, framing, memory accounting, and diagnostics work together
without expanding the project scope.

Validate:

- bidirectional TCP forwarding;
- directional processor pipelines;
- F# plugin compilation/loading;
- runtime-managed buffering and framing;
- per-connection and per-plugin memory accounting;
- performance diagnostics;
- throughput, latency, allocation, and copy-overhead benchmarks.

## Non-goals

Current non-goals:

- HTTP-specific features;
- plugin-controlled routing;
- GUI;
- service-mesh or distributed control-plane features;
- plugin sandboxing;
- automatic source optimization;
- unrestricted third-party plugin dependencies.

## Documentation

- [Architecture](docs/architecture.md), including the [processor model](docs/architecture.md#processor-model), [plugin ownership](docs/architecture.md#plugins), and [memory ownership](docs/architecture.md#memory)
- [EOF and half-close behavior](docs/backpressure.md#eof-and-half-close)
- [Runtime backpressure contract](docs/backpressure.md)
- [Agent instructions](AGENTS.md)

## Install PipeL4

PipeL4 targets `net10.0`. Install the .NET 10 SDK to use the .NET tool commands below; it includes
the .NET 10 runtime required to run PipeL4.

The package can be created and installed from this checkout:

```sh
dotnet tool restore
dotnet paket restore
dotnet build PipeL4.sln
dotnet pack src/PipeL4/PipeL4.fsproj --configuration Release --no-restore -p:PaketDisableCliTest=true -p:BuildInParallel=false --output ./artifacts
dotnet tool install --global PipeL4 --add-source ./artifacts --prerelease
```

`--add-source` adds the local feed while leaving configured NuGet sources available for the
package's dependencies. `--prerelease` includes the prerelease package produced by this checkout.

The global command is `pipel4`. If the shell cannot find it, add the .NET global tools directory to
`PATH` (normally `$HOME/.dotnet/tools` on Linux and macOS or `%USERPROFILE%\.dotnet\tools` on
Windows), then open a new shell.

For a repository-scoped installation, run these commands from the repository root, where
`./artifacts` was created. Add PipeL4 to the existing local tool manifest:

```sh
dotnet tool install --local PipeL4 --add-source ./artifacts --prerelease
dotnet pipel4 --help
```

The manifest is discovered from the current directory or a parent directory. Run the local tool
with `dotnet pipel4` followed by its arguments (or use `dotnet tool run pipel4 -- <arguments>`).

To install a newer prerelease package from the local feed:

```sh
dotnet tool update --global PipeL4 --add-source ./artifacts --prerelease
dotnet tool update --local PipeL4 --add-source ./artifacts --prerelease
```

Remove a global installation with `dotnet tool uninstall --global PipeL4`, or remove a local one
with `dotnet tool uninstall --local PipeL4`.

Maintainers should follow the [release procedure](docs/releasing.md) to build and validate a release
candidate. CI validates packages but does not publish them.

After PipeL4 is published to NuGet.org, the corresponding global install and update commands are
`dotnet tool install --global PipeL4 --prerelease` and
`dotnet tool update --global PipeL4 --prerelease`. Those commands depend on publication and are not
needed for the local-package workflow above.

## Installed examples

The three user-facing processor scripts are embedded in the PipeL4 executable so they can be
inspected and copied after installing the .NET Tool, without cloning the repository or downloading
example files separately. Names are case-sensitive and listed in a stable order:

```sh
pipel4 examples list
```

Copy an example into the current directory, or into an existing directory with `--output-dir`:

```sh
pipel4 examples copy AsciiUppercase
pipel4 examples copy PrefixMode --output-dir ./plugins
```

The copied file keeps its original filename, such as `AsciiUppercase.fsx`. An existing destination
is preserved and the command fails unless `--force` is supplied. With `--force`, an existing
regular file is replaced; symbolic-link destinations are refused. The output directory must
already exist. See `pipel4 examples --help` for the command syntax.

Edit the extracted script and load it with the existing directional plugin option, for example:

```sh
pipel4 \
  --listen-address 127.0.0.1 \
  --listen-port 5000 \
  --backend-host 127.0.0.1 \
  --backend-port 6000 \
  --client-to-server-plugin ./AsciiUppercase.fsx
```

Scripts compiled by the PipeL4 host do not need script-level `#r` directives for PipeL4 assemblies;
the host supplies its supported references. See the individual example descriptions below for each
processor's behavior and limits.

## Practical quickstart

This walkthrough runs the installed `pipel4` command with the repository's example processor. It
uses the local-package installation above and does not require a PipeL4 package from NuGet.org.

### Prerequisites

Install the .NET 10 SDK and PipeL4 as described in [Install PipeL4](#install-pipel4). The relative
example script path below assumes commands are run from the repository root.

### Inspect the CLI and version

Print the complete option list and check the installed version:

```sh
pipel4 --help
pipel4 --version
```

`-h` is an equivalent standalone help invocation. Help and version requests must each be used by
themselves. Listener and backend addresses must be IPv4 or IPv6 literals; PipeL4 does not resolve
hostnames.

### Run the example end to end

The example at [examples/AsciiUppercase.fsx](examples/AsciiUppercase.fsx) uses only the public
`PipeL4.Plugins` processor API. It iterates the incoming `ReadOnlySequence<byte>` segment by segment,
writes generated bytes, preserves every byte outside ASCII `a`–`z`, and returns
`RetainedLength = 0`. It therefore has no pending state to flush at EOF. The runtime owns the input
and output memory; the script does not retain or dispose either one. Scripts run as trusted code in
the PipeL4 process and are not sandboxed. See the [processor contract](docs/architecture.md#processor-model),
[plugin ownership](docs/architecture.md#plugins), and [EOF contract](docs/backpressure.md#eof-and-half-close)
for the complete rules.

User-facing processor examples live in `examples/` and use one descriptive PascalCase `.fsx` file
per processor. `AsciiUppercase.fsx` is the canonical stateless example and
`BytePatternReplace.fsx` demonstrates bounded stateful matching, while
`PrefixMode.fsx` demonstrates bounded prefix-based mode selection; the `.fsx` files under
`benchmarks/` are benchmark fixtures, not user-facing examples. The example intentionally has no
explicit F# `module` declaration: PipeL4 discovers its one public `IByteStreamProcessor`
implementation by interface, not by module name. It also has no `#r` directive because the host
supplies the supported assembly references.

### Stateful byte pattern replacement

The example at [examples/BytePatternReplace.fsx](examples/BytePatternReplace.fsx) replaces each
non-overlapping, case-sensitive ASCII byte sequence `HELLO` with `WORLD`. It processes bytes
directly, so arbitrary binary data and the boundaries between TCP reads, processor invocations,
and input segments do not affect the result. It is stateful because a
trailing `H`, `HE`, `HEL`, or `HELL` may be the beginning of a match; each processor instance
copies at most those four pending bytes into its own bounded state. The runtime does not retain
input memory for this example, and output for an unresolved suffix is delayed until a mismatch,
more input, or normal EOF. At EOF the incomplete suffix is emitted unchanged exactly once.

Run it with the same echo backend and CLI shape as the stateless example, for example:

```sh
pipel4 \
  --listen-address 127.0.0.1 \
  --listen-port 5000 \
  --backend-host 127.0.0.1 \
  --backend-port 6000 \
  --client-to-server-plugin ./examples/BytePatternReplace.fsx
```

Sending `Say HELLO\n` through the proxy produces `Say WORLD\n` from the echo backend. The example
does not decode text, expose protocol framing, support overlapping-match semantics, or provide
unbounded buffering; it is only a byte-stream replacement demonstration. Use
`--server-to-client-plugin` instead when the transformation should apply to backend-to-client
traffic.

### Select a processing mode from the stream prefix

The example at [examples/PrefixMode.fsx](examples/PrefixMode.fsx) inspects the first four bytes of
the configured direction's stream. It preserves those bytes in the output. If they are ASCII
`MASK`, it replaces subsequent ASCII digits `0` through `9` with `X`; otherwise, it passes all
subsequent bytes through unchanged. This is content-based processing-mode selection, not access
control or connection rejection.

The choice is made once per processor instance and does not depend on TCP read or input-segment
boundaries. Until the fourth byte arrives, the runtime retains at most the first three bytes. If
EOF arrives first, those pending bytes are forwarded unchanged. After a decision, later bytes are
processed in the selected mode without inspecting for another prefix. In masking mode the prefix is
borrowed from the original input, and generated output is written in bounded batches; pass-through
data uses borrowed input ranges. The plugin operates on bytes and does not parse a higher-level
protocol.

Load it on client-to-backend traffic with the existing CLI option:

```sh
pipel4 \
  --listen-address 127.0.0.1 \
  --listen-port 5000 \
  --backend-host 127.0.0.1 \
  --backend-port 6000 \
  --client-to-server-plugin ./examples/PrefixMode.fsx
```

Use `--server-to-client-plugin` to apply the same content rule independently to backend-to-client
traffic. Each configured direction has its own processor instance and prefix decision.

Start any TCP echo backend on `127.0.0.1:6000`. For a Unix-like shell, this Python standard-library
server is a one-connection test backend:

```sh
python3 - <<'PY'
import socket

with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 6000))
    listener.listen(1)
    connection, _ = listener.accept()
    with connection:
        while True:
            data = connection.recv(65536)
            if not data:
                break
            connection.sendall(data)
PY
```

In another terminal, from the repository root, start the installed tool with explicit
listener/backend endpoints and the example on client-to-backend traffic:

```sh
pipel4 \
  --listen-address 127.0.0.1 \
  --listen-port 5000 \
  --backend-host 127.0.0.1 \
  --backend-port 6000 \
  --client-to-server-plugin ./examples/AsciiUppercase.fsx
```

In a third terminal, send data through the proxy. The command below uses OpenBSD netcat's explicit
`-N` option to shut down the network socket after standard input reaches EOF; other netcat variants
may use a different flag:

```sh
printf 'Hello, PipeL4!\n' | nc -N 127.0.0.1 5000
```

The echo response is `HELLO, PIPEL4!`. Press Ctrl+C in the PipeL4 terminal after the exchange; the
process shuts down cooperatively and exits successfully.

Script paths are resolved to absolute paths relative to the caller's current working directory.
The `./examples/...` path above therefore assumes the command is run from the repository root. An
installed tool does not resolve relative script paths from its installation directory; use an
absolute path or a path relative to the directory from which `pipel4` is launched.

### Copy and modify the example

The existing CLI accepts a script at any readable `.fsx` path. Copy the canonical example, edit the
copy, and pass its path to the same plugin option:

```sh
cp examples/AsciiUppercase.fsx /tmp/MyAsciiProcessor.fsx
$EDITOR /tmp/MyAsciiProcessor.fsx
pipel4 \
  --listen-address 127.0.0.1 \
  --listen-port 5000 \
  --backend-host 127.0.0.1 \
  --backend-port 6000 \
  --client-to-server-plugin /tmp/MyAsciiProcessor.fsx
```

These are PipeL4-hosted scripts: `pipel4` compiles, references, discovers, and runs the processor
inside the proxy. Running `dotnet fsi examples/AsciiUppercase.fsx` is a different execution mode;
the script deliberately has no assembly reference directive and is not a standalone FSI program.

### Configure ordered processors in both directions

Each plugin option adds one stage, and repeated options run in the order given. This configures two
stages toward the backend and two stages toward the client:

```sh
pipel4 \
  --listen-address 127.0.0.1 --listen-port 5000 \
  --backend-host 127.0.0.1 --backend-port 6000 \
  --client-to-server-plugin ./examples/AsciiUppercase.fsx \
  --client-to-server-plugin ./examples/AsciiUppercase.fsx \
  --server-to-client-plugin ./examples/AsciiUppercase.fsx \
  --server-to-client-plugin ./examples/AsciiUppercase.fsx
```

`--client-to-server-plugin` applies to bytes sent to the configured backend. The
`--server-to-client-plugin` stages apply to bytes returned by the backend. The two directions are
independent and may run concurrently. Configured scripts are compiled and validated before the
listener starts.

### Capacity limits and what they measure

The CLI has three optional byte-capacity switches:

| Switch | Controls | Important boundary |
| --- | --- | --- |
| `--client-to-server-logical-capacity-bytes` | Live logical buffering in the client-to-backend processor pipeline | Directional processor accounting; it is not a physical-memory limit |
| `--server-to-client-logical-capacity-bytes` | Live logical buffering in the backend-to-client processor pipeline | Directional processor accounting; it is not a physical-memory limit |
| `--connection-physical-capacity-bytes` | Actual capacity of active runtime-owned buffer leases for one connection | Shared by both directions of that connection; it is not total process memory |

Logical limits are directional and apply when that direction uses a processor pipeline: retained
suffixes, staged logical ranges, and other live pipeline references consume logical capacity.
Raw forwarding uses no processor stages or processor-pipeline logical accounting, but its
runtime-owned receive buffer still participates in the connection-wide physical limit. The physical
limit is shared only within one connection; separate connections have separate budgets.

If omitted, all three limits are unbounded for compatibility. Logical limits must be at least 1
byte; the physical limit must be at least 2 bytes. Physical accounting charges the capacity
returned by the runtime's pool owners, which can be larger than the requested receive size. It is
runtime-owned physical capacity, not RSS, the managed heap, inactive pool storage, or arbitrary
arrays/dictionaries allocated by trusted plugin code. For the detailed admission, ownership,
backpressure, and progress rules, see the [architecture capacity section](docs/architecture.md#capacity-ownership-and-progress-boundary)
and the [backpressure contract](docs/backpressure.md).

### Configuration errors, logs, diagnostics, stages, and exit codes

The executable uses the .NET JSON console logger. JSON lifecycle records (for example listener,
connection, and close events), runtime diagnostics, compiler warnings, and runtime warnings/errors
are written to standard output. Compiler warnings include `ScriptPath` and `Diagnostic` fields.
Command-line errors, script read/compilation/discovery failures reported before listener startup,
and the top-level fatal runtime error message are written to standard error. The CLI identifies
missing required options, unknown options, invalid value formats, and invalid endpoint values by
the relevant argument. A missing or unreadable `.fsx` path is reported before the listener starts;
compiler diagnostics retain their script path and source location where available.

Runtime diagnostics are enabled by default. Add `--disable-runtime-diagnostics` to omit optional
counters, timings, progress categories, and terminal runtime-diagnostics summaries; required
capacity accounting, lifecycle behavior, cleanup, and failure logging remain active.

Runtime stage records use `Direction` values `client-to-backend` and `backend-to-client`, and
`StageIndex` is zero-based (`0` is the first configured stage). Map an index to a processor by
reading the repeated plugin options in command-line order. Compiler-warning records and
processor-failure messages include the canonical `ScriptPath`; the terminal stage-metrics records
identify the stage by direction and index. Processor-failure text uses a one-based stage number,
so `stage 1` in an exception refers to runtime `StageIndex 0`.

Exit status is `0` for help, normal completion, or cooperative Ctrl+C shutdown; `2` for invalid
CLI arguments or plugin configuration/compilation errors; and `1` for an unexpected runtime error.
Ctrl+C requests cancellation, so cancellation is cooperative and does not synthesize a final EOF
flush; the directional EOF and cleanup rules are documented in the [backpressure contract](docs/backpressure.md#eof-and-half-close).

### Connection-scoped plugin state

A stateful processor can receive `IConnectionState` in its constructor. Existing parameterless
processors remain valid.

```fsharp
open System.Collections.Concurrent
open System.Threading.Tasks
open PipeL4.Plugins

type Processor(connectionState: IConnectionState) =
    let requests =
        connectionState.GetOrAdd(
            "requests",
            fun () -> ConcurrentDictionary<string, int64>()
        )

    interface IByteStreamProcessor with
        member _.ProcessAsync(input, output, _) =
            requests.AddOrUpdate("invocations", 1L, fun _ count -> count + 1L)
            |> ignore
            output.AppendInput(0L, input.Bytes.Length)
            ValueTask<ProcessorResult>({ RetainedLength = 0L })
```

Processor fields are local to one stage instance. Connection state is shared by every stage and
both directions of one TCP connection, while different connections are isolated. Registry access
and publication are thread-safe, but mutable stored values require their own synchronization.
Shared values from independently compiled scripts must use the same CLR type identity; standard
.NET types are suitable, while separately declared look-alike script types are not compatible.
State changes are not rolled back when processing fails, and connection state neither retains nor
owns processor input/output memory. The runtime owns the registry and its successfully published
disposable values for the lifetime of the connection.

## Benchmarks

The IPv4 loopback suite compares direct and raw proxy traffic with the empty processor path,
internal identity processors (one, four, and eight stages), compiled input-backed `.fsx`
processors (one and four stages), repeated and cached connection-state access, and a
generated-output `.fsx` processor. Each measured operation is one request/response exchange over
a persistent connection with a 64 B, 4 KiB, or 1 MiB payload. Compilation, discovery, listener
startup, connection establishment, processor construction, and a validated preflight exchange
occur in global setup.

Run the full benchmark locally in Release mode:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*RawTcpForwardingBenchmarks*'
```

Run the focused connection-state access and lifecycle benchmarks:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*ConnectionState*'
```

Run the focused stage, direction, and connection snapshot benchmarks:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*SnapshotBenchmarks*'
```

Use BenchmarkDotNet's dry job for a smoke run of every benchmark case:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*' --job Dry
```

See the [benchmark README](benchmarks/PipeL4.Benchmarks/README.md) for route definitions,
methodology, limitations, and the latest dated benchmark results. Reports are written beneath
`BenchmarkDotNet.Artifacts/`, which is intentionally excluded from source control.

## License

TBD
