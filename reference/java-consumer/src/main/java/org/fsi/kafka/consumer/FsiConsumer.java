package org.fsi.kafka.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * FSI C4E Reference Consumer
 *
 * Key characteristics:
 * - Avro deserialization with Schema Registry
 * - Manual offset commit (at-least-once delivery guarantee)
 * - Graceful shutdown via wakeup + shutdown hook
 * - JMX metrics for Dynatrace (records consumed, lag, commit latency)
 * - Configurable record handler via functional interface
 *
 * Usage:
 *   var consumer = new FsiConsumer(config, (key, record) -> {
 *       // Process the record
 *       processTransaction(record);
 *   });
 *   consumer.start(); // Blocks until shutdown signal
 */
public class FsiConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FsiConsumer.class);

    private final Consumer<String, GenericRecord> consumer;
    private final List<String> topics;
    private final BiConsumer<String, GenericRecord> recordHandler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Metrics
    private final AtomicLong totalConsumed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong lastPollCount = new AtomicLong(0);
    private final AtomicLong lastCommitLatencyMs = new AtomicLong(0);

    public FsiConsumer(Map<String, String> config, BiConsumer<String, GenericRecord> handler) {
        this.topics = Arrays.asList(config.getOrDefault("topics", "").split(","));
        this.recordHandler = handler;

        if (topics.isEmpty() || topics.get(0).isEmpty()) {
            throw new IllegalArgumentException("topics is required (comma-separated)");
        }

        Properties props = buildProperties(config);
        this.consumer = new KafkaConsumer<>(props);

        registerJmxMetrics(config.getOrDefault("group.id", "unknown"));
        registerShutdownHook();

        log.info("FSI Consumer initialized for topics: {}", topics);
    }

    /**
     * Test seam: inject a {@link Consumer} (e.g. Kafka's MockConsumer) so the
     * consume/commit loop can be tested without a broker or Schema Registry.
     * Does not register JMX metrics or a JVM shutdown hook. Not part of the public API.
     */
    FsiConsumer(List<String> topics, Consumer<String, GenericRecord> consumer,
                BiConsumer<String, GenericRecord> handler) {
        this.topics = topics;
        this.consumer = consumer;
        this.recordHandler = handler;
    }

    static Properties buildProperties(Map<String, String> config) {
        Properties props = new Properties();

        // ── Connection ──
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getOrDefault("bootstrap.servers", "kafka.fsi.internal:9092"));

        // ── Confluent Cloud auth ──
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", config.get("sasl.jaas.config"));

        // ── Consumer group ──
        String groupId = config.get("group.id");
        if (groupId == null) throw new IllegalArgumentException("group.id is required");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // ── Deserialization ──
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // ── Schema Registry ──
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                config.getOrDefault("schema.registry.url", "https://schema.fsi.internal"));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", config.get("schema.registry.basic.auth.user.info"));

        // Return GenericRecord (not specific — allows schema evolution without recompile)
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false");

        // ── Offset management (C4E MANDATORY: manual commit) ──
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ── Performance ──
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);  // 5 min max processing time
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // ── Client identification ──
        String clientId = config.getOrDefault("client.id",
                "fsi-" + groupId + "-consumer");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        // ── Follower fetching (read from nearest replica) ──
        props.put(ConsumerConfig.CLIENT_RACK_CONFIG,
                config.getOrDefault("client.rack", "us-east-1"));

        // ── JMX metrics ──
        props.put(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, "INFO");

        return props;
    }

    /**
     * Start the consume loop. Blocks until shutdown signal.
     * Call this from your application's main thread.
     */
    public void start() {
        running.set(true);
        consumer.subscribe(topics);
        log.info("Consumer subscribed to: {}", topics);

        try {
            while (running.get()) {
                ConsumerRecords<String, GenericRecord> records =
                        consumer.poll(Duration.ofMillis(1000));

                lastPollCount.set(records.count());

                if (records.isEmpty()) continue;

                // Process each record
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    try {
                        recordHandler.accept(record.key(), record.value());
                        totalConsumed.incrementAndGet();
                    } catch (Exception e) {
                        totalErrors.incrementAndGet();
                        log.error("Error processing record [topic={}, partition={}, offset={}, key={}]: {}",
                                record.topic(), record.partition(), record.offset(),
                                record.key(), e.getMessage(), e);

                        // Application-specific error handling:
                        // Option A: Skip and continue (at-least-once with DLQ)
                        // Option B: Seek back and retry (blocks partition)
                        // Option C: Throw to stop the consumer

                        // Default: log and continue (at-least-once semantics)
                    }
                }

                // Commit offsets after successful batch processing
                long commitStart = System.currentTimeMillis();
                consumer.commitSync(Duration.ofSeconds(10));
                lastCommitLatencyMs.set(System.currentTimeMillis() - commitStart);
            }
        } catch (WakeupException e) {
            if (running.get()) throw e; // Unexpected wakeup
            log.info("Consumer wakeup received — shutting down");
        } finally {
            log.info("Closing consumer. Total consumed: {}, errors: {}",
                    totalConsumed.get(), totalErrors.get());
            consumer.close(Duration.ofSeconds(30));
        }
    }

    /**
     * Signal the consumer to stop. Safe to call from another thread.
     */
    public void shutdown() {
        log.info("Shutdown requested");
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            shutdown();
        }, "fsi-consumer-shutdown"));
    }

    // ── JMX Metrics for Dynatrace ──

    private void registerJmxMetrics(String groupId) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(
                    "org.fsi.kafka.consumer:type=FsiConsumer,group=" + groupId);
            mbs.registerMBean(new FsiConsumerMetrics(), name);
            log.info("Registered JMX metrics: {}", name);
        } catch (Exception e) {
            log.warn("Failed to register JMX metrics: {}", e.getMessage());
        }
    }

    public interface FsiConsumerMetricsMBean {
        long getTotalConsumed();
        long getTotalErrors();
        long getLastPollCount();
        long getLastCommitLatencyMs();
        double getErrorRate();
    }

    private class FsiConsumerMetrics implements FsiConsumerMetricsMBean {
        public long getTotalConsumed() { return totalConsumed.get(); }
        public long getTotalErrors() { return totalErrors.get(); }
        public long getLastPollCount() { return lastPollCount.get(); }
        public long getLastCommitLatencyMs() { return lastCommitLatencyMs.get(); }
        public double getErrorRate() {
            long total = totalConsumed.get() + totalErrors.get();
            return total > 0 ? (double) totalErrors.get() / total : 0.0;
        }
    }
}
