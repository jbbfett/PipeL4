# Runtime backpressure contract

This document defines the backpressure contract for PipeL4's
protocol-agnostic, bidirectional TCP runtime. The runtime implements ordered
execution, awaited downstream propagation, ownership, cancellation, EOF
handling, and asynchronous physical capacity admission described here.

The bounded accounting model keeps logical budgets directional and physical
reservations in one connection-wide domain. Production forwarding uses the
configured limits, while omitted capacity options retain the unbounded
compatibility default. Internal bounded entry points use the same admission and
cleanup rules for integration coverage.

The contract applies independently to the client-to-backend and
backend-to-client directions. It does not add protocol framing, routing,
rate-limiting, pass-through policy, or a new scheduling model.

## Terms

### Direction

A direction is one ordered chain:

```text
source socket -> ordered processor stages -> destination socket
```

The two directions run as separate operations and may be active concurrently.
There is no ordering relationship between directions. Ordering is deterministic
only within one direction.

### Logical byte occurrence

A logical byte occurrence is a byte range that the runtime must still preserve
for processing or delivery. Logical occurrences are counted at the stream
boundaries where they are live, not by comparing their byte values.

The same physical bytes can have more than one logical occurrence. For example,
an invocation may emit an input range while retaining that range, or append the
same input range more than once. Those occurrences are independent logical work
and must be counted independently for logical occupancy.

An input sequence presented to a processor is a view of that stage's pending
ranges. The view itself is not an additional logical occurrence.

### Logical buffered bytes

Directional logical occupancy is the sum of live logical byte occurrences in:

- each stage's pending input, including a retained suffix and newly admitted
  upstream bytes;
- an invocation's output while it is being staged transactionally;
- output pending at a downstream stage; and
- output whose sink write is still in progress.

When a range is transferred from one runtime-owned queue to the next, its
logical occurrence transfers rather than being counted twice. If the processor
created a second occurrence, such as overlapping retention and emission, both
remain charged. A retained suffix is therefore not a new copy, but it is a
live logical occurrence until the stage releases it.

Logical occupancy is a capacity-policy quantity. It is distinct from the
existing observability fields:

- `CurrentRetainedBytes` is only the last committed retained suffix for each
  stage, not total logical occupancy;
- `InputBytes`, `InputBackedOutputBytes`, and `GeneratedOutputBytes` are
  cumulative activity/provenance counters, not current occupancy; and
- tentative output from a failed invocation is not published in successful
  activity metrics, even though it must be charged while the invocation is
  active for capacity safety.

Unadvanced writable memory has zero logical byte length. Its backing lease is
  still physically reserved until the invocation advances or releases it.

### Physical reserved memory

Physical reservation is the sum of the actual capacities of all distinct,
active runtime-owned memory leases for the connection. A lease is counted once
even when multiple ranges, stages, or an in-flight sink write reference it.

Physical accounting therefore has these properties:

- the full capacity of a rented owner is charged, not its requested size,
  received length, emitted length, or retained suffix length;
- ordinary runtime-owned and generated-output acquisition reserves the requested
  capacity before calling `Rent`, then reconciles a larger returned owner once,
  synchronously, against the remaining budget; an owner that cannot fit is
  disposed and the operation faults without retrying with smaller requests;
- TCP receive acquisition is the bounded transport exception: an unexposed
  oversized owner is disposed and the receive request is retried with a smaller
  size; if the pool still cannot fit a one-byte request, the receive faults
  deterministically;
- a pooled array or `IMemoryOwner<byte>` remains charged until its final runtime
  reference is released and its owner is disposed;
- borrowed input output reuses the existing lease and adds no physical charge;
- generated output reserves a separate lease, charged at its actual capacity;
- a pool returning a larger capacity than requested charges the larger value;
- a failed owner disposal remains conservatively charged because the runtime
  cannot know whether the storage was released; and
- if both capacity inspection and owner disposal fail, the connection enters a
  terminal unknown-capacity saturation state. `Int64.MaxValue` current and peak
  values in that state are sentinels for unknown retained capacity, not measured
  byte totals, and releases or later cleanup cannot clear the saturation; and
- arbitrary plugin allocations are outside exact pipeline-memory accounting.

Physical reservation is the memory-safety quantity. Logical occupancy explains
how much ordered work is buffered and detects logical amplification; the two
quantities must not be treated as interchangeable.

## Ownership and admission invariants

1. The runtime owns all pipeline and network memory. Plugins receive temporary
   views and runtime-owned output writers; they never rent, retain, release, or
   dispose network buffers directly.
2. The runtime reserves physical capacity before acquiring and exposing a
   runtime-owned receive buffer or writable generated-output memory. A larger
   pool result is reconciled immediately before exposure; a reservation that
   cannot fit is not silently overcommitted. Owners supplied to `Track` or
   `TrackAsync` already exist before admission can run, so those APIs cannot
   provide a strict pre-acquisition bound; runtime forwarding uses the
   reservation-aware rental paths instead.
3. Logical capacity is charged when a logical occurrence is admitted to a
   pending range or staged output. Output charges are tentative until result,
   output, and retention validation succeeds, and are rolled back on failure.
4. A successful invocation commits its retained suffix and ordered output as one
   runtime transaction. A failed, cancelled, or invalid invocation commits
   neither its tentative output nor a speculative retention value.
5. A range is released only after no stage, downstream range, active sink write,
   or retained suffix still needs it. Releasing a logical occurrence does not
   imply that its physical owner is immediately returned if another occurrence
   still references the same lease.
6. The runtime does not evict, drop, reorder, or reinterpret bytes to recover
   capacity. Byte-processing decisions remain entirely within the plugin.
7. The runtime may use a future generic compaction or copy operation only if an
   approved design preserves byte order, ownership, processor semantics, and
   accounting. Such an operation is not part of this contract's current
   implementation.

## Downstream propagation

Backpressure propagates through awaited operations, from the destination toward
the source:

1. A sink must keep the sequence valid until its `WriteAsync` value task
   completes. The last stage therefore cannot release its output or finish the
   current push while the destination write is pending.
2. A downstream processor that is asynchronously suspended keeps its input
   ranges live. The upstream stage does not receive another invocation for the
   same push, and the runtime does not queue an additional push concurrently.
3. A push completes only after all stages that received its newly available
   input and the sink boundary have completed. The source forwarding loop does
   not perform the next socket receive until that push completes.
4. When a sink, processor, or capacity wait releases memory or returns credit,
   the suspended directional operation may resume. Capacity becoming available
   is not by itself a processor invocation trigger; processors run again only
   after new upstream bytes or upstream completion.
5. Opposite-direction work continues independently. A shared connection memory
   budget may make a direction wait for credit, but it must not introduce a
   connection-wide processor lock or serialize the two byte streams.

This gives TCP's own receive window an opportunity to close naturally when the
runtime cannot advance. It does not promise a particular socket-buffer size or
starvation freedom for capacity waiters.

## Suspension, resumption, and progress

### Valid suspension

The following are valid directional suspension points:

- an asynchronous processor invocation awaiting an external or plugin-owned
  operation;
- a sink write awaiting destination socket capacity;
- a runtime capacity wait before admitting another input lease; and
- a receive or send operation awaiting the transport.

While suspended, all input and output memory supplied to the active invocation
remains valid. The runtime must not invoke that stage again, release its input,
or dispose its output object until the value task completes or the operation is
terminated by cancellation or fault handling.

### Progress

Progress is made when a successful operation consumes, drops, transfers, or
delivers logical occurrences; releases a physical lease; completes a sink write;
observes upstream completion; or otherwise makes a pending operation eligible to
continue. A processor may validly make no byte progress by retaining all input
and emitting nothing while upstream is open: this means that it is awaiting
additional input, not that the runtime should spin.

The runtime must not reinvoke a stage merely because the previous invocation
returned without output or because a capacity waiter was signalled. A new input
event or EOF is required.

### Capacity exhaustion and impossible progress

When a requested input lease, logical occurrence, or generated-output lease
would exceed a configured limit, the bounded accounting paths follow this
order:

1. If another in-flight operation in the same capacity domain can release credit,
   suspend the requesting operation on an asynchronous waiter and wait for that
   release or cancellation. Waiters are scanned in arrival order and every
   currently fitting request is granted; temporarily ineligible requests are
   skipped, so starvation is not ruled out.
2. A request larger than the domain limit fails immediately with an argument
   error. Cancellation removes a queued waiter without changing accounting.
   Controller disposal terminates outstanding waits while already-issued
   reservations remain releasable.
3. If no releasable work can create enough credit, report a deterministic
   capacity/progress fault. Do not busy-wait, silently drop bytes, or wait
   indefinitely for an event that cannot be observed with the available credit.
4. Discard the current invocation's tentative output, release all ranges and
   leases owned by the failed direction, and apply the existing connection
   failure lifecycle. The sibling direction is cancelled, and the listener and
   other connections remain unaffected.

The authoritative admission state, waiter count, and capacity commit are
serialized by a short shared capacity gate. A successful reservation
linearizes at its capacity commit under that gate, and waiter registration
linearizes when it is counted there. The gate arbitrates capacity only; it does
not serialize processor execution or the two directional pipelines.

Under the current synchronous `IBufferWriter<byte>` contract, a processor cannot
await inside `GetMemory` or `Advance`. If a generated-output reservation cannot
fit while an invocation is executing, the minimal compatible behavior is an
immediate invocation/connection fault with atomic cleanup. A resumable output
API would require separate architectural approval.

A processor that retains input until more bytes arrive can therefore reach a
valid but unsatisfiable state: its retained bytes consume the available budget
and no pending sink or downstream operation can release credit. The runtime must
fault deterministically rather than evicting the retained suffix. A future
compaction or emergency-reserve policy is an open design choice.

## Retention, pending output, and expansion

### Retained input

`RetainedLength` describes one suffix of the current logical input. The runtime
keeps that suffix, appends later upstream ranges without copying, and presents
the combined sequence on the next input or EOF invocation. A one-byte suffix may
therefore keep the full physical capacity of its backing owner reserved.

On successful non-EOF processing, the prefix before the suffix is released. On
EOF, every stage must return zero retention after resolving or discarding its
final input. An EOF result that retains bytes is a contract violation and is
terminal for that pipeline.

### Pending and staged output

Output is ordered and staged until the invocation completes successfully and
its result is validated. Borrowed ranges retain their existing physical owner;
generated ranges own runtime storage. Downstream processing cannot observe
staged output from a failed or cancelled invocation.

The peak logical charge during an invocation includes both still-live input
occurrences and newly staged output occurrences. This deliberately accounts for
the temporary overlap required to preserve atomicity. Physical charge includes
all active leases, including an unadvanced writable lease.

Every runtime range carries a reservation exactly equal to its logical length.
Transfers between stages and into the sink move that reservation without
reacquisition. Consuming a prefix releases only that prefix; a retained or
segmented suffix keeps the proportional reservation. Sink output remains
charged until its awaited write completes. Failed or cancelled invocations
release abandoned logical ranges before physical-owner cleanup can block, and
primary and cleanup failures are reported together.

### Expansion

Generated output may be shorter than, equal to, or longer than input. Borrowed
output may repeat or overlap input without a physical copy, but each repeated
logical occurrence still consumes logical capacity and runtime range metadata.
Expansion is valid only while both logical and physical reservations remain
within their configured limits. The runtime does not infer whether expansion is
protocol meaningful.

## Connection and direction lifecycle

The runtime uses the existing connection task and two directional tasks rather
than a separate lifecycle framework. The following transitions define ownership
and terminal behavior:

| Phase | Active owner | Transition and cleanup |
| --- | --- | --- |
| Connection setup | The connection task owns the accepted client, backend client, connection state, capacity accounting, and linked cancellation source. | Backend connection and processor construction must finish before directional forwarding starts. A setup failure disposes both sockets and then connection state. |
| Direction startup | Each direction owns one forwarding task, its physical share, and its processor pipeline when configured. | Both tasks start independently and may enter receive, processing, or write suspension concurrently. |
| Capacity admission and receive | A pending admission owns no lease; a successful admission transfers its reservation to the rented receive owner. | Cancellation removes a waiter or disposes an admitted but untransferred owner. Received bytes are not exposed to the pipeline until the receive completes successfully. |
| Processor invocation | The stage owns its pending input view; the invocation owns tentative output and any writable lease. | Success commits retention and output transactionally. Failure or cancellation discards tentative output and releases abandoned logical occurrences before physical owners. |
| Sink write | The final ranges and their logical reservations remain owned by the direction. | Ownership is released only after the awaited write succeeds, fails, or observes cancellation. A pending write prevents another receive in that direction. |
| Directional EOF | EOF enters pipeline completion; it is not cancellation. | Final processor output is flushed, zero retention is required, and destination send shutdown occurs only after successful completion. The sibling remains active. |
| Directional failure | The direction publishes its primary failure before potentially blocking terminal cleanup. | One idempotent connection abort requests cancellation and disposes both sockets so runtime-controlled sibling waits terminate. Independently occurring directional failures remain observable. |
| Directional cancellation | The active receive, send, admission, or invocation observes the linked token. | Cancellation is non-flushing. No synthetic EOF invocation is made; pending logical and physical ownership is torn down after cooperative work returns. |
| Runtime-memory cleanup | The pipeline clears retained gauges, releases all abandoned logical reservations, then attempts every physical-owner release. | After those attempts, the direction closes its physical share and expands the sibling share. Conservative connection-wide charge remains when owner release is uncertain. |
| External cleanup | Direction-owned processors are disposed in reverse order after the runtime-memory/share boundary. | One processor failure does not skip later disposals. Slow processor disposal does not withhold an otherwise free physical share, but the direction task remains incomplete until disposal returns. |
| Connection termination | The connection task observes both directional outcomes, then disposes connection state and capacity controllers. | Normal half-close waits for the sibling. Failure aborts the sibling. Primary, secondary-direction, abort, pipeline, processor, and state-cleanup failures are logged or aggregated without replacing the primary fault. |

Physical-share completion is idempotent. Its state changes from `Open` to
`Committed` before the completed share closes, then to `Completed` only after
the sibling's expanded limit is usable; any exception publishes `Failed` and
the state cannot reopen. Cancellation racing EOF or failure cannot repeat the
runtime-memory callback or transfer a share twice.

Runtime-controlled capacity, socket, and cooperative processor waits receive
the connection token. PipeL4 cannot preempt plugin code, cancellation callbacks,
or owner disposal that blocks or ignores cancellation; those external waits can
still prevent bounded connection termination.

## EOF, half-close, cancellation, and faults

### EOF and half-close

EOF is upstream completion, not a cancellation signal. The runtime invokes each
stage as needed with `IsCompleted = true`, flushes final ordered output through
all downstream stages and the sink, verifies zero final retention, and only then
performs the destination send-side shutdown. EOF in one direction leaves the
opposite direction active; its stages, retained input, and memory remain live.

An EOF control event carries no input bytes and should not itself consume logical
input credit. Final generated or borrowed output remains subject to capacity
limits, and inability to produce that output is a fault rather than silent
truncation.

### Cancellation

Cancellation is non-flushing. It cancels receives, sends, capacity waits,
processor invocations, and sink writes through the existing cancellation token.
The runtime does not invoke a final EOF pass after cancellation. Once active
cooperative operations have returned, tentative output, retained ranges, leases,
and processors are cleaned up; cleanup failures are preserved according to the
existing aggregation rules.

Trusted plugins and sinks must cooperate with cancellation. PipeL4 cannot
preempt code that ignores the token or blocks a thread indefinitely, so bounded
shutdown is not guaranteed for such code.

### Faults

Processor exceptions, invalid results, output-contract violations, sink or
transport failures, capacity faults, and cleanup failures follow the existing
connection-scoped fault lifecycle:

- the failing directional pipeline stops accepting new input;
- uncommitted output is discarded;
- pending ranges and runtime-owned leases are released as far as ownership
  permits;
- a transport or processing failure cancels the sibling direction so it cannot
  remain blocked; and
- the listener and unrelated connections continue operating.

The primary fault must remain observable even when cleanup produces additional
errors. A fault does not become a successful half-close and does not flush
plugin state as if EOF had arrived.

## Implemented capacity boundary and remaining limitations

### TCP receive admission lifecycle

Each forwarding direction performs admission in this order:

1. The direction samples logical input headroom and its directional physical
   headroom. If retained logical data has consumed the entire logical budget,
   the direction faults immediately. If its physical share is exhausted at a
   quiescent receive boundary, the direction faults unless a committed sibling
   handoff is already able to release credit; it does not wait for a merely
   possible future half-close.
2. The runtime reserves physical capacity before asking `ArrayPool<byte>` or
   `MemoryPool<byte>` for a receive owner. The requested receive size is never
   larger than the logical headroom, the 64 KiB transport bound, or the current
   physical share headroom.
3. The owner is inspected before exposure. Its actual capacity is charged to
   both the connection-wide physical budget and the direction's share. If a
   pool over-rents beyond that headroom, the owner is disposed while unexposed
   and the request is retried with a smaller size. Retries halve the request
   until one byte; a pool that still cannot fit produces a deterministic fault.
4. `ReceiveAsync` uses only the admitted owner capacity and the sampled logical
   headroom. Received bytes are then admitted to the directional logical
   pipeline, and the next receive cannot start until the push, downstream
   processing, and sink write complete.
5. EOF disposes the unused receive owner, flushes the pipeline, and shuts down
   only that direction's destination send side. Cancellation disposes the
   owner without an EOF flush. After all direction-owned leases are cleaned up,
   the completed direction closes its share and the sibling may expand to the
   full connection limit.

An inactive direction may therefore hold one physical receive owner while its
socket is waiting, but it can hold only its own share. It cannot consume the
other direction's progress reserve, and no connection-wide scheduler or polling
loop is introduced.

The current code already provides runtime ownership, ordered non-reentrant stage
execution, awaited sink writes, retention validation, EOF flushing, independent
direction tasks, connection-level physical lease observation, and the internal
capacity controller. The controller provides allocation-light immediate
reservation, asynchronous waiting, cancellation, disposal, and release-driven
wakeup. Physical admission is integrated at runtime-owned receive/input lease
boundaries: requested capacity is reserved before a pool call, and the actual
returned capacity is charged before the owner is exposed. If cleanup is
uncertain because owner disposal throws, the known capacity remains charged and
further admission is closed for that physical domain. Supplied owners passed to
`TrackAsync` are necessarily already acquired and therefore remain a boundedness
limitation of that compatibility API.
The normal forwarding CLI configures the client-to-backend and backend-to-client
logical limits independently, plus the connection-wide physical limit. Each
option accepts a finite decimal byte capacity at its documented minimum; an
omitted option retains the `Int64.MaxValue` unbounded compatibility default.
The remaining limitations are:

- omitted CLI capacity options retain the unbounded compatibility default;
- bounded connections require each half-share to fit the pool's actual minimum
  receive owner while both directions are open;
- `IBufferWriter<byte>` has no asynchronous reservation operation or output-credit
  signal, so synchronous output exhaustion faults immediately;
- a retained input that fills its share faults at the quiescent receive boundary
  unless a sibling share handoff is already committed; a completed handoff is
  published only after the expanded capacity is usable;
- `ProcessorResult` has no explicit minimum-more-input, maximum-expansion, or
  progress declaration.

These gaps are intentional. They must not be filled by silently changing the
public FSX processor contract in an unrelated task.

## Capacity admission behavior

The implemented boundary is:

1. Keep the internal capacity controller directional for logical occupancy and
   connection-wide for physical lease reservations. Bounded connections add a
   directional physical share for every open direction; existing physical lease
   ownership and shared-buffer deduplication remain at the lease boundary.
2. Gate source owner acquisition and receive admission on available credit. A
   reservation must be established before the pool is asked for a runtime-owned
   receive buffer or output memory is exposed. Actual pool over-rents are
   reconciled before exposure; receive rentals retry with smaller requests when
   the unexposed owner does not fit. Release notifications wake capacity
   waiters without invoking processors spontaneously.
3. Tracked owners and array leases own their reservations. Normal disposal
   releases capacity and wakes eligible waiters; failed owner disposal keeps the
   reservation conservatively charged.
4. Generated output remains synchronous because `IBufferWriter<byte>` cannot
   await. If its physical reservation cannot fit, the invocation faults
   immediately with the existing atomic cleanup behavior.
5. Retain the depth-first `ProcessorPipeline` loop, one outstanding operation per
   direction, awaited sink writes, and independent TCP forwarding tasks. Do not
   introduce a queueing scheduler, protocol-specific buffer, framing policy, or
   rate limiter.

Admission adds no byte copies: it wraps the existing runtime-owned lease and
reserves its actual capacity. Direction snapshots expose current and peak
logical buffered bytes alongside stage metrics; connection snapshots also
expose current and peak physical usage. These readings are sampled
independently and do not imply one globally synchronized instant.

No public `IByteStreamProcessor`, `ProcessorInput`, `ProcessorResult`, or
`IProcessorOutput` member is added by this boundary.

## Runtime diagnostics

Runtime diagnostics are enabled by default. The valueless
`--disable-runtime-diagnostics` option disables optional counters, invocation timing, progress
categories, and terminal summary logging. It does not disable capacity enforcement, current or peak
logical/physical accounting, lifecycle logging, or failure logging. Repeating the switch is a CLI
validation error.

Capacity snapshots include the current and peak number of queued waiters, the cumulative number of
waits that actually entered a queue, and total and maximum elapsed wait duration. Immediate
admissions are not waits. A queued wait is completed exactly once when it is granted, cancelled,
terminated by disposal, or fails during waiter registration/coordination. Durations use monotonic
`Stopwatch` timestamps and are reported as elapsed milliseconds in terminal JSON logs. Logical,
directional-physical, and connection-physical budgets retain separate wait counters; a composite
physical admission exposes the connection-wide and directional snapshots independently.

Stage snapshots retain cumulative input/provenance/outcome counters and add maximum completed
invocation duration. Both cumulative and maximum durations are elapsed wall-clock time measured
with monotonic timestamps. They include asynchronous suspension and are not CPU time, downstream
latency, or socket-write time. Successful, failed, and cancellation-classified invocations all
contribute a completed duration. Disabled diagnostics leave structurally valid stage snapshots but
omit optional activity values and do not timestamp invocations.

Directional diagnostics count receive rental attempts, requested bytes, returned owner capacity,
successfully exposed owners, and over-rent retries. They also classify deterministic progress
failures at their decision points: logical receive exhaustion, synchronous logical output
exhaustion, synchronous physical output exhaustion, physical receive exhaustion, and pool-owner
fit failure. A retry contributes an attempt and returned capacity for that owner, but only the
owner that is finally exposed contributes to the exposed-owner count.

When enabled, terminal JSON logging emits one connection summary, one summary per direction, and
one summary per configured stage. Summaries are emitted after runtime-owned logical and physical
cleanup, directional share handoff, processor disposal, and connection-state disposal, but before
capacity controllers are disposed. Direction summaries include logical and directional physical
share occupancy/limits, handoff state, terminal outcome, cleanup-failure presence, wait metrics,
receive metrics, progress categories, and stage activity. Connection summaries include connection
physical occupancy/peak, the connection physical limit and wait metrics, both handoff states, and
cleanup-failure presence. Existing failure events remain emitted independently of these summaries.

These values describe runtime-owned leases and logical references, not process RSS, inactive
pool-cached storage, all plugin allocations, or process-wide copy accounting. Physical occupancy
is the capacity of active runtime-owned leases; returned pooled arrays are no longer counted. If
owner disposal throws, the lease remains conservatively charged because the runtime cannot know
whether storage was released, and the summary reports cleanup failure rather than a successful
release. Generated-output provenance counts bytes advanced through runtime output and is not a
complete process-wide copy metric. Snapshots are detached values sampled independently under
concurrency, so they are internally meaningful but do not promise one globally synchronized
instant across both directions. Internal snapshots and externally logged terminal summaries are
therefore related observations at different lifecycle boundaries, not two views that must match
at every live instant. Optional snapshot or logger failures are isolated from forwarding outcomes;
disabling optional diagnostics does not disable capacity enforcement or required lifecycle/failure
logging.

## Test matrix

The existing tests cover ownership, ordering, retention, EOF, cancellation,
failure isolation, awaited sinks, bounded physical and logical admission,
waiter races, directional isolation, and lease cleanup.

| Area | Scenario | Required result |
| --- | --- | --- |
| Propagation | Slow sink; second push attempted while first write is pending | Second push is not queued or run concurrently; source admission waits; ordering is preserved. |
| Propagation | Async processor suspension and later release | Input remains valid; the same stage is not re-entered; downstream and source resume after completion. |
| Retention | Fragmented input completes a retained suffix | Prefix releases, suffix combines with later input, and logical/physical accounting remains correct. |
| Retention | Processor retains all input and emits nothing | No busy loop; runtime waits for new bytes or EOF. |
| Retention | Retained suffix fills available budget | If another operation can release credit, wait and resume; otherwise produce a deterministic capacity fault. |
| Progress | Both directions retain their physical shares at a quiescent receive boundary | Both directions reach the exhausted state, the connection fails deterministically, and logical/physical cleanup reaches zero. |
| Progress | EOF is pending behind exhausted physical admission | The direction fails deterministically without waiting for the unread EOF. |
| Handoff | Receive admission races a committed sibling share transfer | Admission waits or resamples coherently and can use the expanded share; it never observes completion with the old limit. |
| Accounting | Borrowed overlap/repetition and emitted-plus-retained input | Logical occurrences are charged separately; shared physical lease is charged once. |
| Accounting | Generated output expansion and pool over-rent | Actual physical capacity and logical output are checked; no over-budget owner is exposed. |
| Atomicity | `AppendInput`, writable output, invocation validation, or sink failure after partial output | Tentative output and logical charges roll back; all owners are released; primary error is preserved. |
| EOF | EOF with final borrowed/generated output at the limit | Final processing occurs in order; output is delivered or the direction faults; send shutdown follows only successful flush. |
| EOF | EOF with unresolved retention | Contract violation faults the pipeline and releases retained storage; sink completion is not reported. |
| Cancellation | Cancellation during source admission, capacity wait, processor, or sink write | No EOF flush; active work observes cancellation; all owned memory is cleaned up. |
| Half-close | One direction reaches EOF while the other retains input or writes slowly | Completed direction shuts down only its destination send side; opposite direction continues. |
| Transport | Slow downstream TCP receiver | Pending output remains accounted, source receives stop while the socket send is pending, and resume preserves bytes and ordering. |
| Transport | Slow processor with normal release or cancellation | No concurrent receive/push is issued; release resumes the direction, cancellation cleans all owned memory, and the opposite direction remains independent. |
| Fault isolation | Capacity/processor/sink fault in one direction or connection | Sibling direction is cancelled for that connection; listener and unrelated connections continue. |
| Independence | Both directions contend for a shared physical budget | No cross-direction execution lock; the shared physical controller uses its work-conserving arrival-order scan. |
| Fragmentation | Same logical byte stream delivered in different socket chunk boundaries | Processor-visible and sink-visible ordered bytes match, subject only to plugin-defined buffering. |
| Raw path | No configured processors | Existing raw forwarding and half-close behavior remain unchanged. |
| Performance | Bounded and unbounded benchmark routes | Track throughput, latency, allocations, copied bytes/copy ratio, peak logical occupancy, and peak physical reservation. |

## Decisions requiring architectural approval

The following choices remain deliberately open:

- whether synchronous output exhaustion remains an immediate fault or warrants a
  future resumable/public processor API;
- capacity-fault types and diagnostic exposure beyond the internal runtime
  snapshots; and
- any coalescing, compaction, or copy-on-write policy, which requires benchmark
  evidence before adoption.

The current arbitration policy is deliberately small: scan waiters in arrival
order, grant every request that currently fits, and skip requests that do not.
It is work-conserving but makes no starvation guarantee.
