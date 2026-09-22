# Project intent

PipeL4 is an experimental F#/.NET TCP byte-stream runtime/proxy. Preserve the documented architecture unless a task explicitly asks to reconsider it.

Read `README.md` and `docs/architecture.md` before architectural changes.

# Project direction

`README.md` defines the current implementation milestone and its scope.

Do not implement features outside the current milestone unless explicitly requested.

Architecture decisions and unresolved design questions are documented in `docs/architecture.md`.

Do not resolve open architectural questions implicitly while implementing unrelated tasks.

# Current constraints

- TCP only.
- Backend comes from configuration.
- C -> S and S -> C pipelines are separate and may run concurrently.
- Ordering within one direction is deterministic.
- Plugins are F# `.fsx`, compiled on load.
- No custom DSL.
- Plugin dependencies are limited to standard .NET/F# libraries plus PipeL4-provided APIs.
- Plugins are trusted; do not build a sandbox unless explicitly requested.
- Runtime owns pipeline/network memory.
- Plugins do not own network buffers.
- Processor model: runtime controls downstream execution.
- Plugin state may be per-direction or per-connection.
- Shared per-connection state may be concurrent.
- Prefer diagnostics/warnings over automatic optimization.
- Use Paket for project dependency management; keep Paket configuration authoritative.
- Keep a Dockerfile.

# Engineering rules

- Keep the core protocol-agnostic.
- Do not add HTTP, plugin routing, GUI, Kubernetes, service-mesh features, unrestricted plugin package loading, or other out-of-scope functionality without an explicit task.
- Do not freeze speculative APIs prematurely.
- Prefer the simplest implementation that preserves documented invariants.
- Use memory primitives appropriate to the specific path; do not force one type everywhere.
- Avoid unnecessary allocations and copies on hot paths.
- Make ownership and lifetime explicit in runtime code.
- Preserve cancellation, cleanup, and backpressure.
- Do not serialize both traffic directions merely to simplify shared state.
- Add or change dependencies through Paket rather than introducing an independent dependency-management path.

# Performance

Performance-sensitive changes require benchmarks or a clear reason why benchmarking is not yet possible.

Track at least:

- throughput;
- latency;
- allocations;
- copied bytes / copy ratio;
- logical buffered bytes;
- physical reserved memory.

Do not trade correctness or clear ownership for unmeasured micro-optimizations.

# Plugin diagnostics

- Prefer warnings for expensive but valid code.
- Use errors only for contract violations or code that cannot execute correctly.
- Diagnostics are not a security boundary.
- Do not automatically rewrite plugin source.

# Architectural changes

If implementation conflicts with a current decision:

1. document the conflict;
2. propose the smallest viable alternative;
3. compare behavioral and performance consequences;
4. update `docs/architecture.md` with the accepted change.

Do not silently introduce a new architectural model.
