package org.fsi.kafka.consumer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Offline tests for the low-latency Azure reference consumer (ADR-010).
 *
 * No broker, Schema Registry or network is needed: configuration is checked via
 * {@link FsiConsumer#buildProperties}, and the decoupled poll/worker/commit loop
 * runs against Kafka's {@link MockConsumer} on a background thread. Read these as executable usage examples.
 */
class FsiConsumerTest {

    static final String TOPIC = "payments.transaction.completed";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Txn\",\"namespace\":\"test\","
                    + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

    /** The minimum config an application passes in (values come from Vault/env in real use). */
    static Map<String, String> minimalConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("schema.registry.basic.auth.user.info", "sr-key:sr-secret");
        config.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='k' password='s';");
        config.put("group.id", "payments-ledger");
        config.put("topics", TOPIC);
        return config;
    }

    static ConsumerRecord<String, GenericRecord> record(long offset, String key) {
        GenericRecord value = new GenericData.Record(SCHEMA);
        value.put("id", "t-" + offset);
        return new ConsumerRecord<>(TOPIC, 0, offset, key, value);
    }

    /** Keys that neither the Java consumer nor the Confluent Avro deserializer recognise. */
    static Set<String> unknownKeys(Properties props) {
        Set<String> known = new HashSet<>(ConsumerConfig.configNames());
        known.addAll(AbstractKafkaSchemaSerDeConfig.baseConfigDef().names());
        known.add(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
        return props.stringPropertyNames().stream()
                .filter(k -> !known.contains(k))
                .collect(Collectors.toSet());
    }

    /** A MockConsumer with partition 0 starting at offset 0. */
    static MockConsumer<String, GenericRecord> mockConsumer() {
        MockConsumer<String, GenericRecord> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mock.updateBeginningOffsets(Map.of(TP, 0L));
        return mock;
    }

    // ── Configuration: FSI invariants (shared by both consumer variants) ──

    @Test
    void commitsManuallyAndReplaysFromEarliest() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals("false", p.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("earliest", p.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("payments-ledger", p.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    void deserializesAvroAsGenericRecords() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals(StringDeserializer.class.getName(), p.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(KafkaAvroDeserializer.class.getName(), p.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        // GenericRecord (not generated classes) lets compatible schema changes land without a redeploy
        assertEquals("false", p.get(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG));
        assertEquals("http://localhost:8081", p.get("schema.registry.url"));
    }

    @Test
    void authenticatesWithSaslSsl() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("PLAIN", p.get("sasl.mechanism"));
    }

    @Test
    void derivesClientIdFromGroupAndDefaultsRack() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals("fsi-payments-ledger-consumer", p.get(ConsumerConfig.CLIENT_ID_CONFIG));
        assertEquals("us-east-1", p.get(ConsumerConfig.CLIENT_RACK_CONFIG));
    }

    @Test
    void rejectsMissingGroupIdOrTopics() {
        Map<String, String> noGroup = minimalConfig();
        noGroup.remove("group.id");
        assertThrows(IllegalArgumentException.class, () -> FsiConsumer.buildProperties(noGroup));

        Map<String, String> noTopics = minimalConfig();
        noTopics.remove("topics");
        assertThrows(IllegalArgumentException.class, () -> new FsiConsumer(noTopics, (k, v) -> { }));
    }

    // ── Configuration: low-latency Azure tuning ──

    @Test
    void lowLatencyVariantFetchesImmediatelyInSmallBatches() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals(10, p.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(600000, p.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(1, p.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertEquals(0, p.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
    }

    @Test
    void usesCooperativeRebalancingAndOptionalStaticMembership() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals("org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
                p.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG));
        assertNull(p.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));

        // Static membership only when the caller supplies a stable id (e.g. a StatefulSet pod name)
        Map<String, String> config = minimalConfig();
        config.put("group.instance.id", "fraud-scorer-0");
        assertEquals("fraud-scorer-0",
                FsiConsumer.buildProperties(config).get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    void recyclesConnectionsBeforeAzureIlbIdleTimeout() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals("180000", p.get(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG));
        assertEquals("1000", p.get(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG));
    }

    @Test
    void onlyKnownUnsupportedPropertyIsSocketKeepalive() {
        // KNOWN ISSUE: socket.keepalive.enable is a librdkafka property; the Java client
        // ignores it (logged as an unknown config), so it gives no keepalive protection.
        // Use OS-level net.ipv4.tcp_keepalive_* for JVM clients. Remove the property
        // from FsiConsumer and change this expectation to Set.of() when fixed.
        assertEquals(Set.of("socket.keepalive.enable"),
                unknownKeys(FsiConsumer.buildProperties(minimalConfig())));
    }

    // ── Consume loop (MockConsumer, poll thread + worker thread) ──

    /** Run start() on a background thread, as an application would on its main thread. */
    static Thread startInBackground(FsiConsumer consumer) {
        Thread t = new Thread(consumer::start, "test-poll-thread");
        t.start();
        return t;
    }

    /** Wait until the poll thread has committed the given offset (commits are async). */
    static long awaitCommittedOffset(MockConsumer<String, GenericRecord> mock, long expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        long current = -1;
        while (System.currentTimeMillis() < deadline) {
            OffsetAndMetadata m = mock.committed(Set.of(TP)).get(TP);
            current = m == null ? -1 : m.offset();
            if (current >= expected) {
                return current;
            }
            Thread.sleep(10);
        }
        return current;
    }

    @Test
    void workerProcessesRecordsAndPollThreadCommitsThem() throws Exception {
        MockConsumer<String, GenericRecord> mock = mockConsumer();
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch processed = new CountDownLatch(2);
        FsiConsumer consumer = new FsiConsumer(List.of(TOPIC), mock, (key, value) -> {
            seen.add(key);
            processed.countDown();
        });
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(TP));
            mock.addRecord(record(0, "ACC-1"));
            mock.addRecord(record(1, "ACC-2"));
        });

        Thread pollThread = startInBackground(consumer);
        assertTrue(processed.await(5, TimeUnit.SECONDS), "worker did not process both records");
        assertEquals(2L, awaitCommittedOffset(mock, 2));

        consumer.shutdown();
        pollThread.join(5000);
        assertFalse(pollThread.isAlive(), "poll thread did not stop after shutdown()");
        assertEquals(List.of("ACC-1", "ACC-2"), seen);
        assertTrue(mock.closed());
    }

    @Test
    void handlerFailureIsLoggedAndItsOffsetStillCommitted() throws Exception {
        MockConsumer<String, GenericRecord> mock = mockConsumer();
        CountDownLatch attempted = new CountDownLatch(2);
        FsiConsumer consumer = new FsiConsumer(List.of(TOPIC), mock, (key, value) -> {
            attempted.countDown();
            if (key.equals("ACC-1")) {
                throw new IllegalStateException("fraud model unavailable");
            }
        });
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(TP));
            mock.addRecord(record(0, "ACC-1"));
            mock.addRecord(record(1, "ACC-2"));
        });

        Thread pollThread = startInBackground(consumer);
        assertTrue(attempted.await(5, TimeUnit.SECONDS));
        // KNOWN GAP: the worker records offset+1 even when the handler throws, so the
        // failed record is committed and never retried or dead-lettered.
        assertEquals(2L, awaitCommittedOffset(mock, 2));

        consumer.shutdown();
        pollThread.join(5000);
        assertFalse(pollThread.isAlive());
    }
}
