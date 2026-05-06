"""FSI C4E DLQ Handler -- Python.

Routes failed messages to {source-topic}.dlq with error categorization,
exponential backoff retry, and Kafka header metadata.

This is a reference implementation. Copy and adapt for your application.
"""

import logging
import random
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaError, Producer

log = logging.getLogger(__name__)

# Error codes that should NOT be retried -- send directly to DLQ
NON_RETRYABLE_ERRORS = {
    KafkaError._VALUE_SERIALIZATION,
    KafkaError._KEY_SERIALIZATION,
    KafkaError._VALUE_DESERIALIZATION,
    KafkaError._KEY_DESERIALIZATION,
    KafkaError.TOPIC_AUTHORIZATION_FAILED,
    KafkaError.GROUP_AUTHORIZATION_FAILED,
    KafkaError.CLUSTER_AUTHORIZATION_FAILED,
}

# Maximum retry attempts before routing to DLQ
MAX_RETRIES = 3

# Base backoff in seconds (doubles each retry: 1s, 2s, 4s)
BASE_BACKOFF_S = 1


class FsiDlqHandler:
    """Routes failed messages to a Dead Letter Queue topic.

    Error categorization:
      - Non-retryable (serialization, auth): sent directly to DLQ
      - Retryable (broker timeout, leader election): retried up to 3 times
        with exponential backoff before DLQ routing

    DLQ topic naming follows {source-topic}.dlq convention.
    """

    def __init__(self, producer_config: dict, source_topic: str):
        """Create a DLQ handler with a dedicated producer.

        Args:
            producer_config: Dict with bootstrap.servers and optional SASL config.
            source_topic: Original topic name -- DLQ topic will be {source_topic}.dlq.
        """
        self._source_topic = source_topic
        self._dlq_topic = f"{source_topic}.dlq"
        self._client_id = producer_config.get(
            "client.id", "fsi-dlq"
        ) + "-dlq"

        # Build DLQ producer config -- minimal, just needs connectivity
        dlq_config = {
            "bootstrap.servers": producer_config.get(
                "bootstrap.servers", "localhost:9092"
            ),
            "client.id": self._client_id,
        }

        # Copy SASL config if present
        if producer_config.get("sasl.username"):
            dlq_config["security.protocol"] = "SASL_SSL"
            dlq_config["sasl.mechanism"] = "PLAIN"
            dlq_config["sasl.username"] = producer_config["sasl.username"]
            dlq_config["sasl.password"] = producer_config.get(
                "sasl.password", ""
            )

        self._producer = Producer(dlq_config)

        # Counter for observable metrics
        self._dlq_sent = 0

        log.info("DLQ handler initialized. DLQ topic: %s", self._dlq_topic)

    def handle_error(
        self,
        key: bytes,
        value_bytes: bytes,
        error,
        retry_count: int = 0,
    ):
        """Handle a failed message by classifying and routing.

        Args:
            key: Original message key as bytes (or None).
            value_bytes: Original message value as bytes (or None).
            error: The KafkaError or Exception that caused the failure.
            retry_count: Current retry attempt (0-based).

        Returns:
            None if message was sent to DLQ.
            Tuple of ("retry", backoff_seconds) if caller should retry.
        """
        error_code = getattr(error, "code", lambda: None)()

        # Non-retryable errors go directly to DLQ
        if error_code in NON_RETRYABLE_ERRORS:
            log.warning(
                "Non-retryable error on %s: %s -- routing to DLQ",
                self._source_topic,
                error,
            )
            self._send_to_dlq(key, value_bytes, error, retry_count)
            return None

        # Retryable errors: retry with backoff up to MAX_RETRIES
        if retry_count < MAX_RETRIES:
            backoff = BASE_BACKOFF_S * (2 ** retry_count) + random.uniform(
                0, 0.1 * (2 ** retry_count)
            )
            log.info(
                "Retryable error on %s (attempt %d/%d), backoff %.2fs: %s",
                self._source_topic,
                retry_count + 1,
                MAX_RETRIES,
                backoff,
                error,
            )
            return ("retry", backoff)

        # Exhausted retries -- route to DLQ
        log.warning(
            "Exhausted %d retries on %s: %s -- routing to DLQ",
            MAX_RETRIES,
            self._source_topic,
            error,
        )
        self._send_to_dlq(key, value_bytes, error, retry_count)
        return None

    def _send_to_dlq(self, key, value_bytes, error, retry_count):
        """Produce message to DLQ topic with error metadata headers."""
        error_type = self._classify_error(error)
        now = datetime.now(timezone.utc).isoformat()

        headers = {
            "dlq.original.topic": self._source_topic.encode("utf-8"),
            "dlq.error.type": error_type.encode("utf-8"),
            "dlq.error.message": str(error)[:1024].encode("utf-8"),
            "dlq.timestamp": now.encode("utf-8"),
            "dlq.retry.count": str(retry_count).encode("utf-8"),
            "dlq.producer.client.id": self._client_id.encode("utf-8"),
        }

        self._producer.produce(
            topic=self._dlq_topic,
            key=key,
            value=value_bytes,
            headers=headers,
            on_delivery=self._dlq_delivery_callback,
        )
        self._producer.poll(0)
        self._dlq_sent += 1

        log.warning(
            "Sent to DLQ %s [error_type=%s, retries=%d]",
            self._dlq_topic,
            error_type,
            retry_count,
        )

    def _dlq_delivery_callback(self, err, msg):
        """Callback for DLQ produce -- log failures but don't recurse."""
        if err is not None:
            log.error(
                "Failed to deliver to DLQ %s: %s",
                self._dlq_topic,
                err,
            )

    def _classify_error(self, error) -> str:
        """Map error to a human-readable category.

        Categories:
          SERIALIZATION -- key/value serialization or deserialization failure
          SCHEMA_INCOMPATIBLE -- schema registry compatibility rejection
          BROKER_TIMEOUT -- broker unreachable or request timed out
          AUTH_DENIED -- topic, group, or cluster authorization failure
          UNKNOWN -- unrecognized error
        """
        error_code = getattr(error, "code", lambda: None)()

        if error_code in {
            KafkaError._VALUE_SERIALIZATION,
            KafkaError._KEY_SERIALIZATION,
            KafkaError._VALUE_DESERIALIZATION,
            KafkaError._KEY_DESERIALIZATION,
        }:
            return "SERIALIZATION"

        if error_code in {
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.GROUP_AUTHORIZATION_FAILED,
            KafkaError.CLUSTER_AUTHORIZATION_FAILED,
        }:
            return "AUTH_DENIED"

        if error_code in {
            KafkaError._TIMED_OUT,
            KafkaError._MSG_TIMED_OUT,
            KafkaError.REQUEST_TIMED_OUT,
            KafkaError.NOT_LEADER_FOR_PARTITION,
            KafkaError.LEADER_NOT_AVAILABLE,
        }:
            return "BROKER_TIMEOUT"

        # Check for schema incompatibility by message text
        error_str = str(error).lower()
        if "schema" in error_str and (
            "incompatible" in error_str or "compatibility" in error_str
        ):
            return "SCHEMA_INCOMPATIBLE"

        return "UNKNOWN"

    @property
    def dlq_sent(self) -> int:
        """Total messages routed to DLQ."""
        return self._dlq_sent

    def close(self):
        """Flush and close the DLQ producer."""
        log.info(
            "Closing DLQ handler. Total DLQ messages: %d", self._dlq_sent
        )
        self._producer.flush(timeout=10)
