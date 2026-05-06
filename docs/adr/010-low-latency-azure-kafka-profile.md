# ADR-010: Low-Latency Azure Kafka Client Profile

**Status:** Accepted
**Date:** 2026-05-06
**Author:** Jeremy Hogan

## Context

The standard fsi-dsp reference clients (`reference/{java,python}-{producer,consumer}/`)
ship a throughput-oriented configuration: `linger.ms=20`, `batch.size=32768`,
`compression.type=zstd`, default consumer fetch sizing. This profile is appropriate
for the bulk of FSI streaming workloads — bank transactions, end-of-day reconciliation,
schema-validated change-data feeds where dozens of MB/s sustained throughput dominates
and per-message latency in the hundreds of milliseconds is acceptable.

Two FSI workload classes do not fit this profile:

1. **Sub-100ms-tier workloads.** Real-time fraud detection, intraday risk signals,
   market-data fan-out, and access-transparency event streams all carry SLAs
   incompatible with linger-based batching. A `linger.ms=20` producer adds 20ms to
   the floor of every send; a consumer with `fetch.min.bytes=1024` and
   `fetch.max.wait.ms=500` may wait half a second per poll under low-volume
   conditions. These are unacceptable contributions to a sub-100ms end-to-end budget.

2. **Azure-hosted clients behind Internal Load Balancers.** Azure ILB enforces
   a 4-minute idle TCP timeout with no RST/FIN sent to the client. The default
   Kafka client configuration (`socket.keepalive.enable=false`,
   `connections.max.idle.ms=540000`, `reconnect.backoff.max.ms=10000`) does not
   detect or survive this kill cleanly — the next produce or fetch fails with
   `TimeoutException`, and the 10-second reconnect floor introduces multi-second
   blind spots. The behavior is documented by Microsoft and is not configurable
   at the ILB layer below 4 minutes (and 4 minutes is the maximum). The fix lives
   on the client.

A third concern compounds with both: rebalance latency. A workload that takes
tens of seconds to recover from a single pod restart cannot meet a sub-100ms p99,
no matter how well-tuned the steady-state path is.

These three concerns admit independent solutions, and combining them produces a
distinct, named profile that is reusable across engagements with similar workload
shapes.

## Decision

We adopt a named reference profile, `low-latency-azure`, shipping as four new
fsi-dsp artifacts alongside the existing standard variants:

- `reference/java-consumer-low-latency-azure/`
- `reference/java-producer-low-latency-azure/`
- `reference/python-consumer-low-latency-azure/`
- `reference/python-producer-low-latency-azure/`

Each artifact has a stable ID in `MANIFEST.yaml` with the following capabilities:

```yaml
capabilities:
  latency_tier: sub_100ms
  cloud: azure
  connection_resilience: ilb_aware
  backpressure: queue_decoupled  # consumer artifacts only
```

The profile stacks three orthogonal layers on top of the C4E pattern (manual
commit, idempotency, Avro, handler injection, graceful shutdown):

### Layer 1: Latency-favored client tuning

**Producer:** `compression.type=none`, `batch.size=16384`, `linger.ms=0`.

**Consumer:** `max.poll.records=10` (Java; Python uses default), `fetch.min.bytes=1`,
`fetch.max.wait.ms=0`, `max.poll.interval.ms=600000` (10-min headroom for
backpressure stalls).

Trades batching efficiency for per-message latency. Acceptable when sustained
throughput is below ~50 MB/s and per-message latency dominates the SLA.

### Layer 2: ILB-aware connection management

`socket.keepalive.enable=true` (default `false` — enables OS-level TCP keepalive
probes), `connections.max.idle.ms=180000` (3-min recycle, before Azure ILB's 4-min
kill), `reconnect.backoff.max.ms=1000` (1-second reconnect cap, vs. default 10).

These three settings together survive the Azure ILB idle kill without operator
intervention. The 3-minute connection recycle proactively rotates connections
before ILB fires; the keepalive probes detect any kill that does occur within
seconds rather than at the next send; the tightened reconnect cap collapses the
recovery window.

### Layer 3: Cooperative-sticky and static group membership (consumers)

`partition.assignment.strategy=CooperativeStickyAssignor` enables incremental
rebalances (KIP-429) — in-flight partitions don't pause during reassignment.
Optional `group.instance.id` (KIP-345) opts the consumer into static membership;
a pod restart within `session.timeout.ms` does not trigger rebalance at all.

A 30-second rebalance is a 30-second p99 latency outlier. Avoiding it is the
largest single contributor to predictable sub-100ms behavior across pod-restart
events.

### Layer 4: Decoupled poll/process with backpressure (Java consumer only)

The standard consumer pattern processes each record inline in the poll loop and
commits synchronously after the batch. This couples consumer-group health
(heartbeat liveness) to downstream processing latency: a slow downstream stalls
the heartbeat and triggers a rebalance.

The Java low-latency variant runs the poll loop on a dedicated thread whose
sole job is `poll()` plus heartbeat. Records are handed to a worker thread via
a bounded `LinkedBlockingQueue`. When the queue fills, the poll thread calls
`consumer.pause()` to stop fetching while heartbeats continue; `consumer.resume()`
fires when the queue drains. Offsets are tracked in a `ConcurrentHashMap` and
committed asynchronously from the poll thread.

The Python variant does not include this pattern. The Python `confluent-kafka`
client's poll loop and handler invocation remain inline; users requiring
decoupled poll/process in Python should fork the variant and add a `queue.Queue`
between `consumer.poll()` and the handler.

## Sources

- Microsoft Azure: Load Balancer idle timeout documentation (4-minute fixed
  default; the workaround lives client-side)
- KIP-429: Kafka Consumer Incremental Rebalance Protocol (cooperative sticky)
- KIP-345: Reduce Multiple Consumer Rebalances by Specifying Member Identity
  (static membership)
- Apache Kafka: client configuration reference for `socket.keepalive.enable`,
  `connections.max.idle.ms`, `reconnect.backoff.max.ms`
- Confluent: low-latency client tuning guidance in the developer documentation

## Consequences

**Easier:**

- Workloads with sub-100ms SLAs can adopt a vetted profile by citation rather
  than re-deriving the tuning per engagement.
- Azure ILB idle kills no longer require operator intervention or
  `connections.max.idle.ms` tuning during incident response.
- Pod restarts no longer create rebalance-induced latency cliffs (with static
  membership configured).
- Customer overlays can cite `reference/java-consumer-low-latency-azure@v1`
  as a stable artifact ID; no per-customer code fork.

**Harder:**

- Throughput is meaningfully lower than the standard profile (`linger.ms=0`,
  `compression.type=none`, small batches). Workloads above ~50 MB/s sustained
  should not adopt this profile without revisiting the tuning.
- The Java consumer variant has a larger memory footprint (bounded work queue
  plus offset map; ~1-2 MB per consumer instance).
- Async commits increase commit traffic to the broker by ~10× compared to
  per-batch sync commit. Modern Confluent Cloud and CP clusters absorb this
  comfortably; very small clusters may not.
- Two variants per language doubles the maintenance surface for client tuning
  patches. Both variants share the same business logic (handler injection,
  Avro, graceful shutdown); only the configuration and (for Java consumer)
  the threading model differ.

**Mitigations:**

- The variant READMEs explicitly call out workload fit ("appropriate when all
  apply") and tradeoffs. Misapplication should be self-correcting on read.
- Future tuning patches that affect both variants should be applied as a single
  PR touching both directories; CI surfaces drift if only one is updated.
- The `capabilities` block in `MANIFEST.yaml` makes workload-fit machine-readable;
  the cflt-ai act-rail's gate 2 (`fsi_dsp_coverage`) can match request shape to
  capability set rather than relying on operator selection.
