# Architecture

PipeL4 is experimental. This document records current decisions, not a stable specification.

## Data flow

```text
Client
  |
  v
C -> S processor pipeline
  |
  v
Configured backend
  |
  v
S -> C processor pipeline
  |
  v
Client
```

The two directions may run concurrently. Ordering inside each direction is preserved.

## Core

The runtime owns:

- TCP listeners and backend connections;
- connection lifecycle and cancellation;
- processor scheduling and ordering;
- pipeline memory;
- buffering and backpressure;
- memory accounting;
- plugin loading and diagnostics.

The core is protocol-agnostic and operates on byte streams.

## Processor model

Plugins form ordered stream processors. The runtime controls pipeline execution.

A processor conceptually:

1. observes available input;
2. produces zero or more downstream byte ranges;
3. reports input progress/consumption;
4. may require more input or suspend asynchronously.

The exact F# API and memory types are intentionally not fixed yet.

Consumption is monotonic: consumed input cannot later be requested again.

## Plugins

Plugins are F# `.fsx` scripts compiled on load.

Initial plugin environment:

- standard .NET/F# libraries;
- PipeL4-provided APIs;
- no unrestricted package loading.

Plugins are trusted code. Security and side effects are the plugin author's responsibility.

Plugins may observe, transform, drop, delay, buffer, frame, and maintain protocol state.

## State

Current scopes:

- **per-direction** — isolated C -> S / S -> C state;
- **per-connection** — state shared by both directions.

Shared connection state may be accessed concurrently. Synchronization is the plugin author's responsibility.

## Memory

Pipeline memory is runtime-owned. Plugins do not retain or release network buffers directly.

The runtime chooses suitable .NET memory primitives per execution path and may use spans, memory, sequences, or pools internally.

This enables:

- centralized lifetime management;
- zero-copy paths where practical;
- controlled copying when required;
- cancellation/failure cleanup;
- backpressure;
- accounting by connection, direction, and plugin.

Diagnostics should distinguish:

- **logical buffered bytes** — data still required by processing;
- **physical reserved bytes** — memory the runtime cannot yet reuse or return.

Arbitrary allocations made by plugin code are outside exact pipeline-memory accounting.

## Framing

TCP is a byte stream. Framing is plugin behavior, not a core protocol feature.

A processor may leave input unconsumed until enough bytes arrive to identify a frame.

## Dependencies

PipeL4 is implemented in F# and uses Paket for project dependency management.

Paket manages dependencies of the application and development toolchain. It does not imply unrestricted runtime package loading for `.fsx` plugins; the plugin dependency boundary remains intentionally narrow for the initial implementation.

## Diagnostics

PipeL4 does not rewrite or automatically optimize plugin code at this stage.

Diagnostics may warn about blocking work, avoidable allocations, expensive sequence operations, excessive buffering, long processing times, and similar performance hazards.

## Open questions

- exact processor API;
- concrete input/output memory representations;
- whether separate `consumed` and `examined` positions are required;
- writable transformation / copy-on-write semantics;
- scheduling and fast paths between processors;
- buffering limits and fairness;
- detailed exception and EOF semantics;
- plugin reload/unload behavior.
