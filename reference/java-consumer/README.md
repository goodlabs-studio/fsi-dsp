# FSI Kafka Consumer (Java, standard variant)

C4E reference consumer: Avro `GenericRecord` deserialization, manual commit after each poll batch, graceful shutdown via `wakeup()` and a shutdown hook, and Dynatrace JMX metrics. Throughput-tuned (500-record polls). For latency-critical paths, see `../java-consumer-low-latency-azure`.

This is a reference implementation. Copy and adapt it.

## Usage

```java
Map<String, String> config = Map.of(
    "group.id", "payments-ledger",                            // required
    "topics", "payments.transaction.completed",              // required, comma-separated
    "bootstrap.servers", "<cluster-bootstrap>:9092",
    "schema.registry.url", "https://<schema-registry>",
    "schema.registry.basic.auth.user.info", srKey + ":" + srSecret,
    "sasl.jaas.config", jaasConfig
);

try (FsiConsumer consumer = new FsiConsumer(config, (key, record) -> process(record))) {
    consumer.start();   // blocks; returns after shutdown() or SIGTERM
}
```

## Build & test

Requires JDK 17 and Maven 3.9+. The tests are offline: they use Kafka's `MockProducer`/`MockConsumer`, so no broker, Schema Registry or network access is needed (Maven only downloads dependencies).

```bash
cd reference/java-consumer
mvn -B verify          # compile, generate Avro classes from schemas/, run tests
mvn -B test -Dtest=FsiConsumerTest#handlerFailureIsLoggedAndItsOffsetStillCommitted   # run one test
```

`mvn verify` writes build output to `target/` (git-ignored). CI runs the same command for all four Java reference clients (`.github/workflows/java-reference.yml`).

The tests double as usage examples. They cover:

- **FSI config invariants:** manual commit, `auto.offset.reset=earliest`, Avro `GenericRecord`, SASL_SSL, `group.id` and `topics` required.
- **Variant tuning,** and that every property is one the Java client or Avro deserializer actually recognises.
- **Consume loop:** records reach the handler in order, and offsets are committed after the batch. KNOWN GAP: a record whose handler throws is still committed, with no retry and no DLQ.

Tests marked **KNOWN GAP** / **KNOWN ISSUE** pin down current behaviour that a production adopter must change. Fix the code, then update the assertion.
