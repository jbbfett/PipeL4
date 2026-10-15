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

## Benchmarks

The TCP forwarding benchmark compares six IPv4 loopback routes:

- `Direct` connects directly to the echo backend;
- `RawPipeL4` uses the raw forwarding implementation;
- `ViaEmptyProcessorPipeline` uses the processor-enabled path with no processors;
- `ViaProcessorPipeline` uses one identity processor in each direction;
- `ViaFourProcessorPipeline` uses four identity processors in each direction;
- `ViaEightProcessorPipeline` uses eight identity processors in each direction.

Each identity processor emits its complete input as an input-backed range and retains nothing, so the depth comparison measures pipeline scheduling, ownership, and range-management overhead rather than application logic. Tests verify through eight stages that this path preserves the runtime-owned input backing instead of making an intermediate payload copy.

Each operation uses a persistent connection for one request/response echo exchange with a 64 B, 4 KiB, or 1 MiB payload. Reads accumulate the expected byte count and do not assume that TCP preserves write boundaries.

BenchmarkDotNet reports round-trip duration, the ratio to the direct baseline, and managed allocations. `Logical MiB/s` counts the application payload once in each direction; it does not count bytes separately for both TCP legs of the proxied route. Listener creation, connection establishment, buffer allocation, and a validated preflight exchange occur during global setup rather than measurement.

Run the full benchmark locally in Release mode:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*RawTcpForwardingBenchmarks*'
```

Use BenchmarkDotNet's dry job for a smoke run:

```sh
dotnet run --project benchmarks/PipeL4.Benchmarks -c Release -- --filter '*RawTcpForwardingBenchmarks*' --job Dry
```

The generated reports include the runtime, operating system, CPU, and SDK context. They are written beneath `BenchmarkDotNet.Artifacts/`, which is intentionally excluded from source control.

These scenarios cover one persistent connection with one exchange in flight. They do not measure connection startup, concurrent-connection scaling, or pipelined streaming throughput. Allocation results cover the in-process client, echo backend, and proxy workload rather than PipeL4 alone. Exact copied-byte ratios and logical/physical runtime buffer-memory counters are not yet exposed and are not approximated by this benchmark.

## License

TBD
