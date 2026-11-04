package org.fsi.kafka.producer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Offline tests for the low-latency Azure reference producer (ADR-010).
 *
 * No broker, Schema Registry or network is needed: configuration is checked via
 * {@link FsiProducer#buildProperties}, and the send/DLQ paths run against Kafka's
 * {@link MockProducer}. Read these as executable usage examples.
 */
class FsiProducerTest {

    static final String TOPIC = "payments.transaction.completed";

    static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Txn\",\"namespace\":\"test\","
                    + "\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

    /** MockProducer needs a value serializer; Avro + Schema Registry are not under test here. */
    static final Serializer<GenericRecord> NO_OP_SERIALIZER = (topic, data) -> new byte[0];

    /** The minimum config an application passes in (values come from Vault/env in real use). */
    static Map<String, String> minimalConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("schema.registry.basic.auth.user.info", "sr-key:sr-secret");
        config.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='k' password='s';");
        config.put("topic.name", TOPIC);
        return config;
    }

    static GenericRecord txn(String id) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", id);
        return record;
    }

    static String header(ProducerRecord<?, ?> record, String key) {
        Header h = record.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** Keys that neither the Java producer nor the Confluent Avro serializer recognise (they are ignored at runtime). */
    static Set<String> unknownKeys(Properties props) {
        Set<String> known = new HashSet<>(ProducerConfig.configNames());
        known.addAll(AbstractKafkaSchemaSerDeConfig.baseConfigDef().names());
        return props.stringPropertyNames().stream()
                .filter(k -> !known.contains(k))
                .collect(Collectors.toSet());
    }

    // ── Configuration: FSI invariants (shared by both producer variants) ──

    @Test
    void enforcesDurabilityDefaults() {
        Properties p = FsiProducer.buildProperties(minimalConfig(), TOPIC);
        assertEquals("all", p.get(ProducerConfig.ACKS_CONFIG));
        assertEquals("true", p.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals(5, p.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        assertEquals(Integer.MAX_VALUE, p.get(ProducerConfig.RETRIES_CONFIG));
        assertEquals(120000, p.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG));
    }

    @Test
    void serializesValuesAsAvroViaSchemaRegistry() {
        Properties p = FsiProducer.buildProperties(minimalConfig(), TOPIC);
        assertEquals(StringSerializer.class.getName(), p.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(KafkaAvroSerializer.class.getName(), p.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("http://localhost:8081", p.get("schema.registry.url"));
        assertEquals("USER_INFO", p.get("basic.auth.credentials.source"));
        assertEquals("sr-key:sr-secret", p.get("basic.auth.user.info"));
    }

    @Test
    void authenticatesWithSaslSsl() {
        Properties p = FsiProducer.buildProperties(minimalConfig(), TOPIC);
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("PLAIN", p.get("sasl.mechanism"));
        assertEquals(minimalConfig().get("sasl.jaas.config"), p.get("sasl.jaas.config"));
    }

    @Test
    void derivesClientIdFromTopicDomainUnlessOverridden() {
        assertEquals("fsi-payments-producer",
                FsiProducer.buildProperties(minimalConfig(), TOPIC).get(ProducerConfig.CLIENT_ID_CONFIG));

        Map<String, String> config = minimalConfig();
        config.put("client.id", "ledger-writer-1");
        assertEquals("ledger-writer-1",
                FsiProducer.buildProperties(config, TOPIC).get(ProducerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    void rejectsMissingTopicName() {
        Map<String, String> config = minimalConfig();
        config.remove("topic.name");
        assertThrows(IllegalArgumentException.class, () -> new FsiProducer(config));
    }

    // ── Configuration: low-latency Azure tuning ──

    @Test
    void lowLatencyVariantDispatchesImmediately() {
        Properties p = FsiProducer.buildProperties(minimalConfig(), TOPIC);
        assertEquals("none", p.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        assertEquals(16384, p.get(ProducerConfig.BATCH_SIZE_CONFIG));
        assertEquals(0, p.get(ProducerConfig.LINGER_MS_CONFIG));
    }

    @Test
    void recyclesConnectionsBeforeAzureIlbIdleTimeout() {
        Properties p = FsiProducer.buildProperties(minimalConfig(), TOPIC);
        // Azure ILB silently drops idle TCP flows at 4 min; recycle at 3 min.
        assertEquals("180000", p.get(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG));
        assertEquals("1000", p.get(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG));
    }

    @Test
    void onlyKnownUnsupportedPropertyIsSocketKeepalive() {
        // KNOWN ISSUE: socket.keepalive.enable is a librdkafka (C/Python/.NET) property.
        // The Java client does not define it, logs "supplied but isn't a known config"
        // and ignores it, so it gives NO keepalive protection here. JVM clients need
        // OS-level net.ipv4.tcp_keepalive_* tuning instead. Remove the property from
        // FsiProducer and change this expectation to Set.of() when fixed.
        assertEquals(Set.of("socket.keepalive.enable"),
                unknownKeys(FsiProducer.buildProperties(minimalConfig(), TOPIC)));
    }

    // ── Send path (MockProducer) ──

    @Test
    void sendWritesKeyedRecordToConfiguredTopic() {
        MockProducer<String, GenericRecord> mock =
                new MockProducer<>(true, new StringSerializer(), NO_OP_SERIALIZER);
        MockProducer<byte[], byte[]> dlqMock =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        try (FsiProducer producer = new FsiProducer(TOPIC, mock, new FsiDlqHandler(TOPIC, "test-dlq", dlqMock))) {
            producer.send("ACC-1", txn("t-1"));

            assertEquals(1, mock.history().size());
            ProducerRecord<String, GenericRecord> sent = mock.history().get(0);
            assertEquals(TOPIC, sent.topic());
            assertEquals("ACC-1", sent.key());   // business key => partition affinity
            assertEquals("t-1", sent.value().get("id").toString());
            assertTrue(dlqMock.history().isEmpty());
        }
    }

    @Test
    void sendSyncReturnsBrokerMetadata() throws Exception {
        MockProducer<String, GenericRecord> mock =
                new MockProducer<>(true, new StringSerializer(), NO_OP_SERIALIZER);
        MockProducer<byte[], byte[]> dlqMock =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        try (FsiProducer producer = new FsiProducer(TOPIC, mock, new FsiDlqHandler(TOPIC, "test-dlq", dlqMock))) {
            RecordMetadata metadata = producer.sendSync("ACC-1", txn("t-1"));
            assertEquals(TOPIC, metadata.topic());
        }
    }

    @Test
    void failedSendIsRoutedToDlqWithErrorHeaders() {
        // autoComplete=false lets the test decide how each send completes
        MockProducer<String, GenericRecord> mock =
                new MockProducer<>(false, new StringSerializer(), NO_OP_SERIALIZER);
        MockProducer<byte[], byte[]> dlqMock =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        try (FsiProducer producer = new FsiProducer(TOPIC, mock, new FsiDlqHandler(TOPIC, "test-dlq", dlqMock))) {
            producer.send("ACC-2", txn("t-2"));
            assertTrue(mock.errorNext(new TimeoutException("broker timed out")));

            assertEquals(1, dlqMock.history().size());
            ProducerRecord<byte[], byte[]> dlq = dlqMock.history().get(0);
            assertEquals(TOPIC + ".dlq", dlq.topic());
            assertArrayEquals("ACC-2".getBytes(StandardCharsets.UTF_8), dlq.key());
            assertEquals(TOPIC, header(dlq, "dlq.original.topic"));
            assertEquals("BROKER_TIMEOUT", header(dlq, "dlq.error.type"));
            assertEquals("broker timed out", header(dlq, "dlq.error.message"));
            assertEquals("0", header(dlq, "dlq.retry.count"));
            assertEquals("test-dlq", header(dlq, "dlq.producer.client.id"));

            // KNOWN GAP: the send callback has no access to the serialized value, so the
            // DLQ record carries headers + key only, not the original payload. A
            // retryable error (like this timeout) is also dead-lettered immediately; the
            // isRetryable()/getBackoffMs() retry policy is not wired into send().
            assertNull(dlq.value());
        }
    }
}
