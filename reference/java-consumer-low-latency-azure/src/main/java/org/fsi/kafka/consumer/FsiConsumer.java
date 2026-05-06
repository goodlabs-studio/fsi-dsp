package org.fsi.kafka.consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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

    private final KafkaConsumer<String, GenericRecord> consumer;
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

    private Properties buildProperties(Map<String, String> config) {
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

        // ── Performance (latency-optimized for fraud detection) ──
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);          // small batches = fast per-poll completion
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600000);  // 10 min — headroom for backpressure
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);            // return immediately with any data
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 0);          // no broker-side batching delay

        // ── Rebalance strategy ──
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // ── Static group membership (eliminates rebalance on pod restart) ──
        String instanceId = config.get("group.instance.id");
        if (instanceId != null) {
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        }

        // ── Azure connection management ──
        props.put("socket.keepalive.enable", "true");               // OS-level TCP keepalive probes
        props.put("connections.max.idle.ms", "180000");             // 3min — before Azure ILB 4min kill
        props.put("reconnect.backoff.max.ms", "1000");              // cap at 1s — default 10s too slow

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

    // Decoupled poll/process: internal queue between poll thread and worker threads.
    // The poll thread's only job is to call poll() and maintain heartbeat.
    // Worker threads handle processing and offset tracking independently.
    private final BlockingQueue<ConsumerRecord<String, GenericRecord>> workQueue =
            new LinkedBlockingQueue<>(1000);

    // Pending offsets committed asynchronously by the poll thread
    private final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> pendingOffsets =
            new ConcurrentHashMap<>();

    // Backpressure flag: when true, consumer.pause() is active
    private final AtomicBoolean backpressured = new AtomicBoolean(false);

    /**
     * Start the consume loop with decoupled poll/process pattern.
     * Blocks until shutdown signal.
     *
     * The poll thread hands records off to a worker thread pool via a BlockingQueue.
     * If the queue fills (downstream backpressure), consumer.pause() keeps heartbeats
     * alive without fetching new records. consumer.resume() fires when the queue drains.
     */
    public void start() {
        running.set(true);
        consumer.subscribe(topics);
        log.info("Consumer subscribed to: {}", topics);

        // Start worker thread for processing
        Thread workerThread = new Thread(this::processLoop, "fsi-consumer-worker");
        workerThread.setDaemon(true);
        workerThread.start();

        try {
            while (running.get()) {
                ConsumerRecords<String, GenericRecord> records =
                        consumer.poll(Duration.ofMillis(100));

                lastPollCount.set(records.count());

                // Backpressure: pause/resume based on queue capacity
                if (!backpressured.get() && workQueue.remainingCapacity() < 100) {
                    consumer.pause(consumer.assignment());
                    backpressured.set(true);
                    log.warn("Backpressure detected — pausing consumer (queue remaining: {})",
                            workQueue.remainingCapacity());
                } else if (backpressured.get() && workQueue.remainingCapacity() > 500) {
                    consumer.resume(consumer.assignment());
                    backpressured.set(false);
                    log.info("Backpressure cleared — resuming consumer");
                }

                // Enqueue records for worker threads
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    try {
                        workQueue.put(record); // blocks if queue full (shouldn't happen with pause)
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                // Commit pending offsets asynchronously
                if (!pendingOffsets.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>(pendingOffsets);
                    pendingOffsets.keySet().removeAll(toCommit.keySet());

                    long commitStart = System.currentTimeMillis();
                    consumer.commitAsync(toCommit, (offsets, ex) -> {
                        lastCommitLatencyMs.set(System.currentTimeMillis() - commitStart);
                        if (ex != null) {
                            log.warn("Async commit failed, will retry next cycle: {}", ex.getMessage());
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            if (running.get()) throw e; // Unexpected wakeup
            log.info("Consumer wakeup received — shutting down");
        } finally {
            // Final synchronous commit of any remaining offsets
            if (!pendingOffsets.isEmpty()) {
                consumer.commitSync(new HashMap<>(pendingOffsets), Duration.ofSeconds(10));
            }
            log.info("Closing consumer. Total consumed: {}, errors: {}",
                    totalConsumed.get(), totalErrors.get());
            consumer.close(Duration.ofSeconds(30));
        }
    }

    /**
     * Worker thread: takes records from the queue, processes them, tracks offsets.
     */
    private void processLoop() {
        while (running.get() || !workQueue.isEmpty()) {
            try {
                ConsumerRecord<String, GenericRecord> record =
                        workQueue.poll(500, TimeUnit.MILLISECONDS);
                if (record == null) continue;

                try {
                    recordHandler.accept(record.key(), record.value());
                    totalConsumed.incrementAndGet();
                } catch (Exception e) {
                    totalErrors.incrementAndGet();
                    log.error("Error processing record [topic={}, partition={}, offset={}, key={}]: {}",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), e.getMessage(), e);
                    // Default: log and continue (at-least-once semantics)
                }

                // Track offset for async commit by poll thread
                pendingOffsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("Worker thread exiting");
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
