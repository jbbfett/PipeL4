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
- Plugin code may be analyzed for expensive patterns and reported through diagnostics/warnings. Automatic optimization is out of scope for now.

## First implementation

The first experiment should validate the architecture end to end with the smallest useful system: accept a TCP connection, proxy it to a configured backend through independent directional processor pipelines, load F# `.fsx` plugins, and verify that runtime-managed buffering, framing, memory accounting, and diagnostics work together without expanding the project scope.

Validate:

- bidirectional TCP forwarding;
- directional processor pipelines;
- F# plugin compilation/loading;
- runtime-managed buffering and framing;
- per-connection and per-plugin memory accounting;
- performance diagnostics;
- throughput, latency, allocation, and copy-overhead benchmarks.

## Non-goals

For now:

- HTTP-specific features;
- plugin-controlled routing;
- GUI;
- service-mesh or distributed control-plane features;
- plugin sandboxing;
- automatic source optimization;
- unrestricted third-party plugin dependencies.

## Documentation

- [Architecture](docs/architecture.md)
- [Agent instructions](AGENTS.md)

## Running with script plugins

Configure F# script processors independently for each direction with repeatable options. Options
are executed in argument order within their direction:

```sh
dotnet run --project src/PipeL4 -- \
  --listen-address 127.0.0.1 \
  --listen-port 5000 \
  --backend-host 127.0.0.1 \
  --backend-port 6000 \
  --client-to-server-plugin ./plugins/example.fsx
```

Use `--client-to-server-plugin <path>` for traffic sent to the configured backend and
`--server-to-client-plugin <path>` for traffic returned to the client. Repeating an option adds
another stage, so `--client-to-server-plugin a.fsx --client-to-server-plugin b.fsx` executes
`a.fsx` before `b.fsx`. Configured scripts compile and validate before the listener starts.

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

Use BenchmarkDotNet's dry job for a smoke run of every benchmark case:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*' --job Dry
```

See the [benchmark README](benchmarks/PipeL4.Benchmarks/README.md) for route definitions,
methodology, limitations, and the latest dated branch-exit results. Reports are written beneath
`BenchmarkDotNet.Artifacts/`, which is intentionally excluded from source control.

## License

TBD
