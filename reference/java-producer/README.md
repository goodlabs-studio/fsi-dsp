# FSI Kafka Producer (Java, standard variant)

C4E reference producer: idempotent `acks=all` production, Avro via Schema Registry, DLQ routing (`FsiDlqHandler`), Dynatrace JMX metrics and graceful shutdown. Throughput-tuned (`zstd`, 32 KB batches, 20 ms linger). For latency-critical paths, see `../java-producer-low-latency-azure`.

This is a reference implementation. Copy and adapt it.

## Usage

```java
Map<String, String> config = Map.of(
    "topic.name", "payments.transaction.completed",          // required
    "bootstrap.servers", "<cluster-bootstrap>:9092",
    "schema.registry.url", "https://<schema-registry>",
    "schema.registry.basic.auth.user.info", srKey + ":" + srSecret,   // from Vault
    "sasl.jaas.config", jaasConfig,                           // from Vault
    "client.id", "ledger-writer-1"
);

try (FsiProducer producer = new FsiProducer(config)) {
    producer.send(accountNumber, avroRecord);   // async; failures go to <topic>.dlq
    producer.flush();
}
```

See `ExampleApp.java` for a complete program. Note: it loads `../../schemas/cncb-account-transaction.avsc`, which does not exist. The example schemas live in `schemas/examples/`.

## Build & test

Requires JDK 17 and Maven 3.9+. The tests are offline: they use Kafka's `MockProducer`/`MockConsumer`, so no broker, Schema Registry or network access is needed (Maven only downloads dependencies).

```bash
cd reference/java-producer
mvn -B verify          # compile, generate Avro classes from schemas/, run tests
mvn -B test -Dtest=FsiProducerTest#failedSendIsRoutedToDlqWithErrorHeaders   # run one test
```

`mvn verify` writes build output to `target/` (git-ignored). CI runs the same command for all four Java reference clients (`.github/workflows/java-reference.yml`).

The tests double as usage examples. They cover:

- **FSI config invariants:** `acks=all`, idempotence, `max.in.flight=5`, unbounded retries, Avro + Schema Registry, SASL_SSL, client-id derivation, `topic.name` required.
- **Variant tuning,** and that every property is one the Java client or Avro serializer actually recognises.
- **Send path:** keyed records land on the configured topic, `sendSync()` returns metadata, and a failed send is dead-lettered to `<topic>.dlq` with `dlq.*` headers. KNOWN GAP: the DLQ record has no payload, and retryable errors are not retried first.
- **DLQ handler:** retryable vs non-retryable classification, 1s/2s/4s backoff, error categories, and 1024-char message truncation.

Tests marked **KNOWN GAP** / **KNOWN ISSUE** pin down current behaviour that a production adopter must change. Fix the code, then update the assertion.
