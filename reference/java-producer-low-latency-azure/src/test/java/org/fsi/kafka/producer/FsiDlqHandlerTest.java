package org.fsi.kafka.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/** Offline tests for DLQ error classification, backoff and record format. */
class FsiDlqHandlerTest {

    static final String TOPIC = "payments.transaction.completed";

    MockProducer<byte[], byte[]> dlqMock;
    FsiDlqHandler handler;

    @BeforeEach
    void setUp() {
        dlqMock = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        handler = new FsiDlqHandler(TOPIC, "ledger-writer-dlq", dlqMock);
    }

    @AfterEach
    void tearDown() {
        handler.close();
    }

    @Test
    void poisonAndAuthErrorsAreNotRetryable() {
        assertFalse(handler.isRetryable(new SerializationException("bad avro")));
        assertFalse(handler.isRetryable(new InvalidRecordException("corrupt")));
        assertFalse(handler.isRetryable(new RecordTooLargeException("too big")));
        assertFalse(handler.isRetryable(new TopicAuthorizationException("denied")));
        assertFalse(handler.isRetryable(new GroupAuthorizationException("denied")));
    }

    @Test
    void transientBrokerErrorsAreRetryable() {
        assertTrue(handler.isRetryable(new TimeoutException("slow broker")));
        assertTrue(handler.isRetryable(new NotLeaderOrFollowerException("leader moved")));
        assertTrue(handler.isRetryable(new LeaderNotAvailableException("election")));
    }

    @Test
    void backoffDoublesFromOneSecond() {
        assertEquals(1000, handler.getBackoffMs(0));
        assertEquals(2000, handler.getBackoffMs(1));
        assertEquals(4000, handler.getBackoffMs(2));
        assertEquals(3, FsiDlqHandler.MAX_RETRIES);
    }

    @Test
    void classifiesErrorsIntoDlqCategories() {
        assertEquals("SERIALIZATION", handler.classifyError(new SerializationException("x")));
        assertEquals("SERIALIZATION", handler.classifyError(new InvalidRecordException("x")));
        assertEquals("AUTH_DENIED", handler.classifyError(new TopicAuthorizationException("x")));
        assertEquals("AUTH_DENIED", handler.classifyError(new ClusterAuthorizationException("x")));
        assertEquals("BROKER_TIMEOUT", handler.classifyError(new TimeoutException("x")));
        assertEquals("BROKER_TIMEOUT", handler.classifyError(new LeaderNotAvailableException("x")));
        assertEquals("UNKNOWN", handler.classifyError(new IllegalStateException("x")));
    }

    @Test
    void dlqRecordCarriesPayloadAndTruncatedErrorMessage() {
        byte[] key = "ACC-9".getBytes(StandardCharsets.UTF_8);
        byte[] value = "{\"id\":\"t-9\"}".getBytes(StandardCharsets.UTF_8);
        String longMessage = "x".repeat(5000);

        handler.sendToDlq(key, value, new SerializationException(longMessage), 2);

        ProducerRecord<byte[], byte[]> dlq = dlqMock.history().get(0);
        assertEquals(TOPIC + ".dlq", dlq.topic());
        assertArrayEquals(key, dlq.key());
        assertArrayEquals(value, dlq.value());
        assertEquals("SERIALIZATION", header(dlq, "dlq.error.type"));
        assertEquals("2", header(dlq, "dlq.retry.count"));
        assertEquals(1024, header(dlq, "dlq.error.message").length());
        assertNotNull(header(dlq, "dlq.timestamp"));
        assertEquals(1, handler.getDlqSent());
    }

    static String header(ProducerRecord<?, ?> record, String key) {
        return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
    }
}
