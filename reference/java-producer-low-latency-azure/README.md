# FSI Kafka Producer — Low-Latency Azure Variant (Java)

Variant of `reference/java-producer/` tuned for sub-100ms produce latency on Azure-hosted Kafka clients. See **ADR-010** for the full rationale and tradeoffs.

This variant is appropriate when **all** apply:
- Latency tier ≤ 100ms (real-time risk, fraud signals, market-data fan-out)
- Client runs on Azure compute behind an Internal Load Balancer (ILB) or otherwise subject to NAT/firewall idle TCP kill
- Per-message commit pressure matters more than per-byte throughput

If your workload is throughput-oriented (>50 MB/s sustained), use `reference/java-producer/` (standard variant).

## What's different from the standard variant

Two orthogonal layers stacked on the same C4E producer pattern (idempotency, Avro, DLQ, graceful shutdown):

### 1. Latency-favored client tuning

| Setting | Standard | Low-latency variant | Effect |
|---|---|---|---|
| `compression.type` | `zstd` | `none` | no compression CPU at low TPS |
| `batch.size` | 32_768 | 16_384 | absorbs micro-bursts without holding messages |
| `linger.ms` | 20 | 0 | immediate dispatch (Kafka 4.0+ default is 5 — explicitly 0 here) |

Idempotency, `acks=all`, and `retries=Integer.MAX_VALUE` are **unchanged** — durability properties hold.

### 2. ILB-aware connection management

Azure Internal Load Balancer kills idle TCP at 4 minutes with no RST/FIN. Default Kafka client doesn't see the kill until the next produce, manifesting as a `TimeoutException` with no warning.

| Setting | Default | Variant | Effect |
|---|---|---|---|
| `socket.keepalive.enable` | `false` | `true` | OS-level TCP keepalive probes |
| `connections.max.idle.ms` | 540_000 | 180_000 | 3-min recycle, before ILB fires |
| `reconnect.backoff.max.ms` | 10_000 | 1_000 | 1s reconnect cap (default 10s creates detection blind spots) |

## Usage

Same as standard variant. The variant is a drop-in replacement when the latency profile is appropriate.

```java
Map<String, String> config = Map.of(
    "topic.name", "fraud.signals.v1.signal-event",   // FsiProducer requires topic.name
    "bootstrap.servers", "<cc-cluster>:9092",
    "schema.registry.url", "https://psrc-...azure.confluent.cloud",
    "client.id", "fraud-publisher-1"
);

try (FsiProducer producer = new FsiProducer(config)) {
    producer.send(key, avroValue);  // fire-and-forget with idempotent guarantee
}
```

## Build & test

Requires JDK 17 and Maven 3.9+. The tests are offline: they use Kafka's `MockProducer`/`MockConsumer`, so no broker, Schema Registry or network access is needed (Maven only downloads dependencies).

```bash
cd reference/java-producer-low-latency-azure
mvn -B verify          # compile, generate Avro classes from schemas/, run tests
mvn -B test -Dtest=FsiProducerTest#failedSendIsRoutedToDlqWithErrorHeaders   # run one test
```

`mvn verify` writes build output to `target/` (git-ignored). CI runs the same command for all four Java reference clients (`.github/workflows/java-reference.yml`).

The tests double as usage examples. They cover:

- **FSI config invariants:** `acks=all`, idempotence, `max.in.flight=5`, unbounded retries, Avro + Schema Registry, SASL_SSL, client-id derivation, `topic.name` required.
- **Variant tuning,** and that every property is one the Java client or Avro serializer actually recognises.
- **Send path:** keyed records land on the configured topic, `sendSync()` returns metadata, and a failed send is dead-lettered to `<topic>.dlq` with `dlq.*` headers. KNOWN GAP: the DLQ record has no payload, and retryable errors are not retried first.
- **DLQ handler:** retryable vs non-retryable classification, 1s/2s/4s backoff, error categories, and 1024-char message truncation.
- **Azure ILB connection recycling** (`connections.max.idle.ms`, `reconnect.backoff.max.ms`). KNOWN ISSUE: `socket.keepalive.enable` is a librdkafka-only property that the Java client ignores (see Tradeoffs).

Tests marked **KNOWN GAP** / **KNOWN ISSUE** pin down current behaviour that a production adopter must change. Fix the code, then update the assertion.

## Tradeoffs

**You give up:**
- Network bandwidth efficiency. No compression + small batches = ~3-4× higher byte-on-wire per message. At >50 MB/s sustained, this matters.
- Slight increase in producer-side request rate (smaller batches = more requests per second).

**You get:**
- p99 produce latency under 50ms under steady load.
- Survival across Azure ILB idle kills with no operator intervention.
- Predictable commit timing for upstream observability (no linger jitter).

## See also

- ADR-010: Low-Latency Azure Kafka Profile
- Microsoft Azure Load Balancer idle timeout documentation
- `reference/java-producer/` for the throughput-default variant
