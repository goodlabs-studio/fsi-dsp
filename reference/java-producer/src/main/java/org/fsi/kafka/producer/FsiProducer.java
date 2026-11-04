package org.fsi.kafka.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FSI C4E Reference Producer
 *
 * Key characteristics:
 * - Idempotent (enable.idempotence=true) for exactly-once per partition
 * - Avro serialization with Schema Registry auto-registration
 * - JMX metrics exposed for Dynatrace OneAgent
 * - Graceful shutdown with flush + close
 * - Callback-based error handling with logging
 *
 * This is a reference implementation. Copy and adapt for your application.
 */
public class FsiProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FsiProducer.class);

    private final Producer<String, GenericRecord> producer;
    private final String topicName;
    private final FsiDlqHandler dlqHandler;

    // Metrics for Dynatrace JMX exposure
    private final AtomicLong totalSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong lastLatencyMs = new AtomicLong(0);

    /**
     * Create a producer from the standard FSI configuration pattern.
     *
     * @param config Map containing at minimum:
     *   - bootstrap.servers (kafka.fsi.internal:9092 via Consul DNS)
     *   - schema.registry.url (schema.fsi.internal via Consul DNS)
     *   - schema.registry.basic.auth.user.info (API key:secret from Vault)
     *   - sasl.jaas.config (for CC authentication)
     *   - topic.name (the fully qualified topic name)
     */
    public FsiProducer(Map<String, String> config) {
        this.topicName = config.get("topic.name");
        if (this.topicName == null || this.topicName.isEmpty()) {
            throw new IllegalArgumentException("topic.name is required");
        }

        Properties props = buildProperties(config, topicName);
        this.producer = new KafkaProducer<>(props);

        registerJmxMetrics();

        // DLQ handler for routing failed messages
        this.dlqHandler = new FsiDlqHandler(config, topicName);

        log.info("FSI Producer initialized for topic: {}", topicName);
    }

    /**
     * Test seam: inject a {@link Producer} (e.g. Kafka's MockProducer) and a DLQ
     * handler so send/error paths can be tested without a broker or Schema
     * Registry. Not part of the public API.
     */
    FsiProducer(String topicName, Producer<String, GenericRecord> producer, FsiDlqHandler dlqHandler) {
        this.topicName = topicName;
        this.producer = producer;
        this.dlqHandler = dlqHandler;
    }

    /**
     * Build Kafka producer properties with C4E defaults.
     * These defaults are non-negotiable for FSI production topics.
     */
    static Properties buildProperties(Map<String, String> config, String topicName) {
        Properties props = new Properties();

        // ── Connection (from Consul DNS + Vault) ──
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getOrDefault("bootstrap.servers", "kafka.fsi.internal:9092"));

        // ── Confluent Cloud authentication ──
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", config.get("sasl.jaas.config"));

        // ── Serialization ──
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // ── Schema Registry ──
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.getOrDefault("schema.registry.url", "https://schema.fsi.internal"));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", config.get("schema.registry.basic.auth.user.info"));

        // Auto-register schemas (C4E manages via Terraform, but producer can register compatible evolutions)
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true");

        // ── Idempotence (C4E MANDATORY) ──
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // safe with idempotence

        // ── Reliability ──
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 minutes
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        // ── Performance ──
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);        // 32KB batches
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);            // 20ms linger for batching

        // ── Client identification (for monitoring) ──
        String clientId = config.getOrDefault("client.id",
                "fsi-" + topicName.split("\\.")[0] + "-producer");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // ── JMX metrics (Dynatrace picks these up via OneAgent) ──
        props.put(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");

        return props;
    }

    /**
     * Send a record asynchronously with error handling.
     * The key should be the natural business key (e.g., account_number, transaction_id).
     */
    public void send(String key, GenericRecord value) {
        long startMs = System.currentTimeMillis();

        ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>(topicName, key, value);

        producer.send(record, (metadata, exception) -> {
            long latency = System.currentTimeMillis() - startMs;
            lastLatencyMs.set(latency);

            if (exception != null) {
                totalErrors.incrementAndGet();
                log.error("Failed to produce to {} [key={}]: {}",
                        topicName, key, exception.getMessage(), exception);
                // DLQ routing with retry classification
                if (dlqHandler != null) {
                    byte[] keyBytes = key != null ? key.getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
                    // Note: value bytes not available in callback -- serialize for DLQ
                    dlqHandler.sendToDlq(keyBytes, null, exception, 0);
                }
            } else {
                totalSent.incrementAndGet();
                log.debug("Produced to {}-{} offset={} latency={}ms",
                        metadata.topic(), metadata.partition(),
                        metadata.offset(), latency);
            }
        });
    }

    /**
     * Send synchronously (blocking). Use sparingly — only when you need the offset
     * before proceeding (e.g., transaction reconciliation).
     */
    public RecordMetadata sendSync(String key, GenericRecord value) throws Exception {
        ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>(topicName, key, value);

        long startMs = System.currentTimeMillis();
        try {
            RecordMetadata metadata = producer.send(record).get();
            lastLatencyMs.set(System.currentTimeMillis() - startMs);
            totalSent.incrementAndGet();
            return metadata;
        } catch (Exception e) {
            totalErrors.incrementAndGet();
            throw e;
        }
    }

    /**
     * Flush pending records. Call before shutdown or when you need durability confirmation.
     */
    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        log.info("Shutting down producer for topic: {}", topicName);
        producer.flush();
        dlqHandler.close();
        producer.close(Duration.ofSeconds(30));
        log.info("Producer closed. Total sent: {}, errors: {}, DLQ: {}",
                totalSent.get(), totalErrors.get(), dlqHandler.getDlqSent());
    }

    // ── JMX Metrics for Dynatrace ──

    /**
     * Register custom MBeans that Dynatrace OneAgent can scrape.
     * These supplement the built-in kafka.producer metrics.
     *
     * Dynatrace JMX extension config:
     *   - org.fsi.kafka.producer:type=FsiProducer,topic=*
     */
    private void registerJmxMetrics() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(
                    "org.fsi.kafka.producer:type=FsiProducer,topic=" + topicName);
            mbs.registerMBean(new FsiProducerMetrics(), name);
            log.info("Registered JMX metrics: {}", name);
        } catch (Exception e) {
            log.warn("Failed to register JMX metrics: {}", e.getMessage());
        }
    }

    // MBean interface
    public interface FsiProducerMetricsMBean {
        long getTotalSent();
        long getTotalErrors();
        long getLastLatencyMs();
        long getDlqSent();
        double getErrorRate();
    }

    // MBean implementation
    private class FsiProducerMetrics implements FsiProducerMetricsMBean {
        public long getTotalSent() { return totalSent.get(); }
        public long getTotalErrors() { return totalErrors.get(); }
        public long getLastLatencyMs() { return lastLatencyMs.get(); }
        public long getDlqSent() { return dlqHandler != null ? dlqHandler.getDlqSent() : 0; }
        public double getErrorRate() {
            long total = totalSent.get() + totalErrors.get();
            return total > 0 ? (double) totalErrors.get() / total : 0.0;
        }
    }
}
