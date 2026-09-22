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

## License

TBD
