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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Offline tests for the standard reference consumer.
 *
 * No broker, Schema Registry or network is needed: configuration is checked via
 * {@link FsiConsumer#buildProperties}, and the poll/process/commit loop runs
 * against Kafka's {@link MockConsumer}. Read these as executable usage examples.
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

    // ── Configuration: standard-variant tuning ──

    @Test
    void standardVariantFavoursThroughput() {
        Properties p = FsiConsumer.buildProperties(minimalConfig());
        assertEquals(500, p.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
        assertEquals(300000, p.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG));
        assertEquals(1024, p.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));   // Java default is 1
        assertEquals(500, p.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
    }

    @Test
    void everyPropertyIsRecognisedByTheClientOrDeserializer() {
        assertEquals(Set.of(), unknownKeys(FsiConsumer.buildProperties(minimalConfig())));
    }

    // ── Consume loop (MockConsumer) ──

    @Test
    void processesRecordsInOrderThenCommitsTheBatch() {
        MockConsumer<String, GenericRecord> mock = mockConsumer();
        List<String> seen = new ArrayList<>();
        AtomicReference<OffsetAndMetadata> committed = new AtomicReference<>();
        FsiConsumer consumer = new FsiConsumer(List.of(TOPIC), mock, (key, value) -> seen.add(key));

        // poll #1: partition assigned, two records arrive
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(TP));
            mock.addRecord(record(0, "ACC-1"));
            mock.addRecord(record(1, "ACC-2"));
        });
        // poll #2: capture what was committed after batch #1, then stop the loop
        mock.schedulePollTask(() -> {
            committed.set(mock.committed(Set.of(TP)).get(TP));
            consumer.shutdown();
        });

        consumer.start();   // blocks until shutdown()

        assertEquals(List.of("ACC-1", "ACC-2"), seen);
        assertEquals(2L, committed.get().offset());   // next offset to read
        assertTrue(mock.closed());
    }

    @Test
    void handlerFailureIsLoggedAndItsOffsetStillCommitted() {
        MockConsumer<String, GenericRecord> mock = mockConsumer();
        List<String> processed = new ArrayList<>();
        AtomicReference<OffsetAndMetadata> committed = new AtomicReference<>();
        FsiConsumer consumer = new FsiConsumer(List.of(TOPIC), mock, (key, value) -> {
            if (key.equals("ACC-1")) {
                throw new IllegalStateException("downstream ledger unavailable");
            }
            processed.add(key);
        });

        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(TP));
            mock.addRecord(record(0, "ACC-1"));
            mock.addRecord(record(1, "ACC-2"));
        });
        mock.schedulePollTask(() -> {
            committed.set(mock.committed(Set.of(TP)).get(TP));
            consumer.shutdown();
        });

        consumer.start();

        assertEquals(List.of("ACC-2"), processed);
        // KNOWN GAP: the failed record (offset 0) is committed with the batch. It is
        // not retried or dead-lettered, so "at-least-once" only holds for records whose
        // handler succeeds. Add DLQ/retry (or seek back) before using this for
        // regulated flows.
        assertEquals(2L, committed.get().offset());
    }
}
