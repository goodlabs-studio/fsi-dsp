# FSI Kafka Consumer — Low-Latency Azure Variant (Python)

Variant of `reference/python-consumer/` tuned for sub-100ms end-to-end latency on Azure-hosted Kafka clients. See **ADR-010** for the full rationale and tradeoffs.

This variant is appropriate when **all** apply:
- Latency tier ≤ 100ms (real-time risk, fraud detection, market-data fan-out)
- Client runs on Azure compute behind an Internal Load Balancer (ILB)
- Throughput is **modest** (latency tuning sacrifices batching efficiency)

If your workload is throughput-oriented or runs outside Azure, use `reference/python-consumer/` (standard variant).

## What's different from the standard variant

Three layers on top of the same C4E pattern (manual commit, Avro deserialization, handler injection, graceful shutdown):

### 1. Latency-favored client tuning

| Setting | Standard | Low-latency variant | Effect |
|---|---|---|---|
| `max.poll.interval.ms` | 300_000 | 600_000 | 10-min headroom for backpressure |
| `fetch.min.bytes` | 1024 | 1 | broker returns immediately with any data |
| `fetch.wait.max.ms` | 500 | 0 | no broker-side batching delay |

### 2. ILB-aware connection management

Azure Internal Load Balancer kills idle TCP at 4 minutes with no RST/FIN. The variant defeats this with three settings:

| Setting | Default | Variant | Effect |
|---|---|---|---|
| `socket.keepalive.enable` | `False` | `True` | OS-level TCP keepalive probes |
| `connections.max.idle.ms` | 540_000 | 180_000 | 3-min recycle, before ILB fires |
| `reconnect.backoff.max.ms` | 10_000 | 1_000 | 1s reconnect cap |

### 3. Cooperative-sticky + static group membership

| Setting | Effect |
|---|---|
| `partition.assignment.strategy: cooperative-sticky` | incremental rebalances; in-flight partitions don't pause |
| `group.instance.id` (optional config key) | static membership — pod restart within `session.timeout.ms` doesn't trigger rebalance |

## Note on decoupled poll/process

Unlike the Java variant, the Python variant does **not** include the decoupled poll/process pattern with backpressure pause/resume. The Python `confluent-kafka` client's poll loop and handler invocation are still inline. If you need decoupled poll/process in Python, fork this variant and add a `queue.Queue` between `consumer.poll()` and the handler — the same architecture as the Java variant, but you own the implementation.

For most Python-side use cases, the latency tuning + ILB resilience + static membership combination is sufficient.

## Usage

```python
from fsi_consumer import FsiConsumer

def handle_signal(key: str, value: dict):
    # your business logic; raise to trigger error handling
    process_signal(key, value)

config = {
    "group.id": "fraud-signal-processor",
    "topics": "fraud.alerts.v1.alert-signal",
    "bootstrap.servers": "<cc-cluster>:9092",
    "schema.registry.url": "https://psrc-...azure.confluent.cloud",
    "schema.registry.basic.auth.user.info": "<sr-key>:<sr-secret>",
    "sasl.username": "<api-key>",
    "sasl.password": "<api-secret>",
    "group.instance.id": os.getenv("HOSTNAME"),  # static membership via pod ordinal
    "client.id": "fraud-processor-1",
}

with FsiConsumer(config, handle_signal) as consumer:
    consumer.start()  # blocks until SIGTERM/SIGINT
```

## Tradeoffs

**You give up:**
- Throughput (latency tuning sacrifices batching efficiency)
- Slightly more commit traffic to the broker

**You get:**
- p99 latency under 100ms in steady state
- Survival across Azure ILB idle kills
- No rebalance latency cliffs on pod restart (with static membership)

## See also

- ADR-010: Low-Latency Azure Kafka Profile
- KIP-429 (cooperative sticky), KIP-345 (static membership)
- Microsoft Azure Load Balancer idle timeout documentation
- `reference/python-consumer/` for the throughput-default variant
- `reference/java-consumer-low-latency-azure/` for the decoupled poll/process pattern
