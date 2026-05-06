# FSI Kafka Producer — Low-Latency Azure Variant (Python)

Variant of `reference/python-producer/` tuned for sub-100ms produce latency on Azure-hosted Kafka clients. See **ADR-010** for the full rationale and tradeoffs.

This variant is appropriate when **all** apply:
- Latency tier ≤ 100ms (real-time risk, fraud signals, market-data fan-out)
- Client runs on Azure compute behind an Internal Load Balancer (ILB)
- Per-message commit pressure matters more than per-byte throughput

If your workload is throughput-oriented (>50 MB/s sustained), use `reference/python-producer/` (standard variant).

## What's different from the standard variant

Two orthogonal layers on top of the same C4E pattern (idempotency, Avro, DLQ, graceful shutdown):

### 1. Latency-favored client tuning

| Setting | Standard | Low-latency variant | Effect |
|---|---|---|---|
| `compression.type` | `zstd` | `none` | no compression CPU at low TPS |
| `batch.size` | 32_768 | 16_384 | absorbs micro-bursts without holding messages |
| `linger.ms` | 20 | 0 | immediate dispatch |

Idempotency, `acks=all`, and infinite retries are **unchanged** — durability properties hold.

### 2. ILB-aware connection management

Azure Internal Load Balancer kills idle TCP at 4 minutes with no RST/FIN. The variant defeats this with three settings:

| Setting | Default | Variant | Effect |
|---|---|---|---|
| `socket.keepalive.enable` | `False` | `True` | OS-level TCP keepalive probes |
| `connections.max.idle.ms` | 540_000 | 180_000 | 3-min recycle, before ILB fires |
| `reconnect.backoff.max.ms` | 10_000 | 1_000 | 1s reconnect cap |

## Usage

Same as standard variant; drop-in replacement when the latency profile is appropriate.

```python
from fsi_producer import FsiProducer

config = {
    "topic": "fraud.signals.v1.signal-event",
    "bootstrap.servers": "<cc-cluster>:9092",
    "schema.registry.url": "https://psrc-...azure.confluent.cloud",
    "schema.registry.basic.auth.user.info": "<sr-key>:<sr-secret>",
    "sasl.username": "<api-key>",
    "sasl.password": "<api-secret>",
    "client.id": "fraud-publisher-1",
}

with FsiProducer(config) as producer:
    producer.send(key, avro_value)  # idempotent, fire-and-forget
```

## Tradeoffs

**You give up:**
- Network bandwidth efficiency (~3-4× more bytes per message without compression)
- Slight increase in producer-side request rate (smaller batches → more requests)

**You get:**
- p99 produce latency under 50ms under steady load
- Survival across Azure ILB idle kills
- Predictable commit timing (no linger jitter)

## See also

- ADR-010: Low-Latency Azure Kafka Profile
- Microsoft Azure Load Balancer idle timeout documentation
- `reference/python-producer/` for the throughput-default variant
