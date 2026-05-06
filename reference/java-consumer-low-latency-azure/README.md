# FSI Kafka Consumer — Low-Latency Azure Variant (Java)

Variant of `reference/java-consumer/` tuned for sub-100ms end-to-end latency on Azure-hosted Kafka clients. See **ADR-010** for the full rationale and tradeoffs.

This variant is appropriate when **all** apply:
- Latency tier ≤ 100ms (e.g., real-time risk, fraud detection, market data fan-out)
- Client runs on Azure compute behind an Internal Load Balancer (ILB) or otherwise subject to NAT/firewall idle TCP kill
- Throughput is **modest** (the latency tuning sacrifices batching efficiency)

If your workload is throughput-oriented (>50 MB/s sustained) or runs outside Azure, use `reference/java-consumer/` (standard variant).

## What's different from the standard variant

Three orthogonal layers stacked on top of the same C4E pattern (manual commit, Avro, handler injection, graceful shutdown):

### 1. Latency-favored client tuning

| Setting | Standard | Low-latency variant | Effect |
|---|---|---|---|
| `max.poll.records` | 500 | 10 | smaller batches → faster per-poll completion |
| `max.poll.interval.ms` | 300_000 | 600_000 | 10-min headroom for backpressure stalls |
| `fetch.min.bytes` | 1024 | 1 | broker returns immediately with any data |
| `fetch.max.wait.ms` | 500 | 0 | no broker-side batching delay |

### 2. ILB-aware connection management

Azure Internal Load Balancer kills idle TCP at 4 minutes with no RST/FIN. Default Kafka client doesn't see the kill until the next poll (visible as "broker unavailable" with no warning).

| Setting | Default | Variant | Effect |
|---|---|---|---|
| `socket.keepalive.enable` | `false` | `true` | OS-level TCP keepalive probes |
| `connections.max.idle.ms` | 540_000 | 180_000 | 3-min recycle, before ILB fires |
| `reconnect.backoff.max.ms` | 10_000 | 1_000 | 1s reconnect cap (default 10s creates blind spots) |

### 3. Decoupled poll/process with backpressure

The standard variant processes each record inline in the poll loop and commits synchronously after the batch. This couples consumer-group health (heartbeat liveness) to downstream processing latency — a slow downstream stalls the heartbeat and triggers rebalance.

The low-latency variant runs the poll loop on a dedicated thread whose only job is `poll()` + heartbeat. Records are handed to a worker thread via a bounded `LinkedBlockingQueue` (capacity 1000). When the queue fills (downstream backpressure), the poll thread calls `consumer.pause()` to stop fetching while heartbeats continue; `consumer.resume()` fires when the queue drains.

Offsets are tracked in a `ConcurrentHashMap<TopicPartition, OffsetAndMetadata>` and committed asynchronously from the poll thread.

### 4. Cooperative-sticky + static group membership

| Setting | Effect |
|---|---|
| `partition.assignment.strategy: CooperativeStickyAssignor` | incremental rebalances; in-flight partitions don't pause |
| `group.instance.id` (optional, via `config`) | static membership — pod restart within `session.timeout.ms` doesn't trigger rebalance |

Rebalance avoidance compounds with the latency tuning: a 30-second rebalance is a 30-second latency outlier.

## Usage

Same as standard variant. Provide a `Map<String, String>` config with at minimum `group.id`, `topics`, `bootstrap.servers`, `schema.registry.url`. To enable static membership, add `group.instance.id` (e.g., from your pod ordinal or hostname).

```java
Map<String, String> config = Map.of(
    "group.id", "fraud-signal-processor",
    "topics", "fraud.alerts.v1.alert-signal",
    "bootstrap.servers", "<azure-event-hubs-or-cc-cluster>:9092",
    "schema.registry.url", "https://psrc-...azure.confluent.cloud",
    "group.instance.id", System.getenv("HOSTNAME"),  // pod ordinal
    "client.id", "fraud-processor-1"
);

try (FsiConsumer consumer = new FsiConsumer(config, this::handleSignal)) {
    consumer.start();  // blocks until SIGTERM/SIGINT
}
```

## Tradeoffs

**You give up:**
- Throughput. With `linger.ms=0` and small batches, sustained throughput is ~5-10× lower than the standard variant. Don't pick this profile for streaming workloads >50 MB/s.
- Memory. The bounded work queue + offset map adds ~1-2 MB of steady-state heap per consumer instance.
- Commit chattiness. Async commits per cycle increase commit traffic to the broker by ~10×. Acceptable on modern Confluent Cloud / CP clusters.

**You get:**
- p99 latency under 100ms across pod restarts and downstream stalls.
- Survival across Azure ILB idle kills with no operator intervention.
- No rebalance-induced latency cliffs on pod restart (with static membership).

## See also

- ADR-010: Low-Latency Azure Kafka Profile
- KIP-429: Kafka Consumer Incremental Rebalance Protocol (cooperative sticky)
- KIP-345: Reduce Multiple Consumer Rebalances by Specifying Member Identity
- Microsoft Azure Load Balancer idle timeout documentation
- `reference/java-consumer/` for the throughput-default variant
