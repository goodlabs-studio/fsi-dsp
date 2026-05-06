package org.fsi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FSI C4E DLQ Handler -- Java.
 *
 * Routes failed messages to {source-topic}.dlq with error categorization,
 * exponential backoff retry, and Kafka header metadata.
 *
 * Error classification:
 *   - Non-retryable (serialization, auth): sent directly to DLQ
 *   - Retryable (broker timeout, leader election): retried up to 3 times
 *     with exponential backoff (1s, 2s, 4s) before DLQ routing
 *
 * This is a reference implementation. Copy and adapt for your application.
 */
public class FsiDlqHandler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FsiDlqHandler.class);

    /** Maximum retry attempts before routing to DLQ. */
    public static final int MAX_RETRIES = 3;

    /** Base backoff in milliseconds (doubles each retry: 1s, 2s, 4s). */
    public static final long BASE_BACKOFF_MS = 1000;

    /** Exception types that should NOT be retried. */
    private static final Set<Class<? extends Exception>> NON_RETRYABLE = new HashSet<>(Arrays.asList(
            org.apache.kafka.common.errors.SerializationException.class,
            org.apache.kafka.common.errors.InvalidRecordException.class,
            org.apache.kafka.common.errors.RecordTooLargeException.class,
            org.apache.kafka.common.errors.TopicAuthorizationException.class,
            org.apache.kafka.common.errors.GroupAuthorizationException.class
    ));

    private final KafkaProducer<byte[], byte[]> dlqProducer;
    private final String dlqTopic;
    private final String sourceTopic;
    private final String clientId;
    private final AtomicLong dlqSent = new AtomicLong(0);

    /**
     * Create a DLQ handler with a dedicated raw-bytes producer.
     *
     * @param config Map with bootstrap.servers and optional SASL config.
     * @param sourceTopic Original topic name -- DLQ topic will be {sourceTopic}.dlq.
     */
    public FsiDlqHandler(Map<String, String> config, String sourceTopic) {
        this.sourceTopic = sourceTopic;
        this.dlqTopic = sourceTopic + ".dlq";
        this.clientId = config.getOrDefault("client.id", "fsi-dlq") + "-dlq";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getOrDefault("bootstrap.servers", "kafka.fsi.internal:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // Copy SASL config if present
        if (config.containsKey("security.protocol")) {
            props.put("security.protocol", config.get("security.protocol"));
        } else if (config.containsKey("sasl.jaas.config")) {
            props.put("security.protocol", "SASL_SSL");
        }
        if (config.containsKey("sasl.mechanism")) {
            props.put("sasl.mechanism", config.get("sasl.mechanism"));
        } else if (config.containsKey("sasl.jaas.config")) {
            props.put("sasl.mechanism", "PLAIN");
        }
        if (config.containsKey("sasl.jaas.config")) {
            props.put("sasl.jaas.config", config.get("sasl.jaas.config"));
        }

        this.dlqProducer = new KafkaProducer<>(props);
        log.info("DLQ handler initialized. DLQ topic: {}", dlqTopic);
    }

    /**
     * Check if an exception is retryable.
     *
     * @param e The exception to classify.
     * @return true if retryable, false if it should go directly to DLQ.
     */
    public boolean isRetryable(Exception e) {
        return !NON_RETRYABLE.stream().anyMatch(cls -> cls.isInstance(e));
    }

    /**
     * Calculate exponential backoff delay for a given retry count.
     *
     * @param retryCount Current retry attempt (0-based).
     * @return Backoff duration in milliseconds (1s, 2s, 4s).
     */
    public long getBackoffMs(int retryCount) {
        return BASE_BACKOFF_MS * (1L << retryCount);
    }

    /**
     * Send a failed message to the DLQ topic with error metadata headers.
     *
     * @param key Original message key as bytes (may be null).
     * @param value Original message value as bytes (may be null).
     * @param error The exception that caused the failure.
     * @param retryCount Number of retry attempts before this DLQ send.
     */
    public void sendToDlq(byte[] key, byte[] value, Exception error, int retryCount) {
        String errorType = classifyError(error);
        String errorMessage = error.getMessage() != null
                ? error.getMessage().substring(0, Math.min(error.getMessage().length(), 1024))
                : "null";
        String timestamp = Instant.now().toString();

        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("dlq.original.topic", sourceTopic.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq.error.type", errorType.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq.error.message", errorMessage.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq.timestamp", timestamp.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq.retry.count", String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("dlq.producer.client.id", clientId.getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(dlqTopic, null, key, value, headers);

        dlqProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to deliver to DLQ {}: {}", dlqTopic, exception.getMessage());
            } else {
                log.warn("Sent to DLQ {}-{} offset={} [error_type={}, retries={}]",
                        metadata.topic(), metadata.partition(), metadata.offset(),
                        errorType, retryCount);
            }
        });

        dlqSent.incrementAndGet();
    }

    /**
     * Classify an exception into a human-readable error category.
     *
     * @param e The exception to classify.
     * @return One of: SERIALIZATION, AUTH_DENIED, BROKER_TIMEOUT, UNKNOWN.
     */
    public String classifyError(Exception e) {
        if (e instanceof org.apache.kafka.common.errors.SerializationException
                || e instanceof org.apache.kafka.common.errors.InvalidRecordException
                || e instanceof org.apache.kafka.common.errors.RecordTooLargeException) {
            return "SERIALIZATION";
        }
        if (e instanceof org.apache.kafka.common.errors.TopicAuthorizationException
                || e instanceof org.apache.kafka.common.errors.GroupAuthorizationException
                || e instanceof org.apache.kafka.common.errors.ClusterAuthorizationException) {
            return "AUTH_DENIED";
        }
        if (e instanceof org.apache.kafka.common.errors.TimeoutException
                || e instanceof org.apache.kafka.common.errors.NotLeaderOrFollowerException
                || e instanceof org.apache.kafka.common.errors.LeaderNotAvailableException) {
            return "BROKER_TIMEOUT";
        }
        return "UNKNOWN";
    }

    /**
     * Get the total number of messages sent to DLQ.
     *
     * @return DLQ message count.
     */
    public long getDlqSent() {
        return dlqSent.get();
    }

    @Override
    public void close() {
        log.info("Closing DLQ handler. Total DLQ messages: {}", dlqSent.get());
        dlqProducer.flush();
        dlqProducer.close();
    }
}
