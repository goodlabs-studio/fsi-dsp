"""FSI C4E Reference Producer -- Python.

Mirrors Java FsiProducer: idempotent, Avro, metrics, graceful shutdown, DLQ routing.
This is a reference implementation. Copy and adapt for your application.
"""

import logging
import threading
import time

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from prometheus_client import Counter, Gauge, start_http_server

from fsi_dlq_handler import FsiDlqHandler

log = logging.getLogger(__name__)


class FsiProducer:
    """FSI C4E Reference Producer for Python applications.

    Key characteristics:
      - Idempotent (enable.idempotence=True) for exactly-once per partition
      - Avro serialization with Schema Registry (modern AvroSerializer API)
      - Prometheus metrics exposed via HTTP endpoint
      - Graceful shutdown with flush + close
      - DLQ routing for failed messages with error categorization

    Usage:
        config = {
            "topic.name": "corebanking.transactions.v1.account-transaction",
            "bootstrap.servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
            "schema.registry.url": "https://psrc-xxxxx.us-east-1.aws.confluent.cloud",
            "sasl.username": "<api-key>",
            "sasl.password": "<api-secret>",
        }
        with FsiProducer(config) as producer:
            producer.send("key-123", {"transaction_id": "tx-001", ...})
    """

    # Prometheus metrics (shared across instances, labeled by topic)
    _total_sent = Counter(
        "fsi_producer_total_sent",
        "Total messages sent successfully",
        ["topic"],
    )
    _total_errors = Counter(
        "fsi_producer_total_errors",
        "Total send errors",
        ["topic"],
    )
    _last_latency_ms = Gauge(
        "fsi_producer_last_latency_ms",
        "Last send latency in milliseconds",
        ["topic"],
    )
    _dlq_sent = Counter(
        "fsi_producer_dlq_sent",
        "Total messages routed to DLQ",
        ["topic"],
    )

    def __init__(self, config: dict):
        """Create a producer from the standard FSI configuration pattern.

        Args:
            config: Dict containing at minimum:
              - topic.name (required): Fully qualified topic name
              - bootstrap.servers: Kafka broker address (default: localhost:9092)
              - schema.registry.url: SR endpoint (default: http://localhost:8081)
              - schema.registry.basic.auth.user.info: SR credentials (api_key:secret)
              - sasl.username: Confluent Cloud API key (enables SASL_SSL)
              - sasl.password: Confluent Cloud API secret
              - value.schema: Avro schema JSON string (optional)
              - client.id: Custom client identifier
              - dlq.enabled: Enable DLQ routing (default: "true")
              - metrics.port: Prometheus HTTP port (default: "9090")

        Raises:
            ValueError: If topic.name is missing.
        """
        self._topic_name = config.get("topic.name")
        if not self._topic_name:
            raise ValueError("topic.name is required")

        # Schema Registry client
        sr_config = {
            "url": config.get(
                "schema.registry.url", "http://localhost:8081"
            ),
        }
        if config.get("schema.registry.basic.auth.user.info"):
            sr_config["basic.auth.user.info"] = config[
                "schema.registry.basic.auth.user.info"
            ]

        self._sr_client = SchemaRegistryClient(sr_config)

        # Serializers -- modern API (NOT deprecated AvroProducer)
        self._key_serializer = StringSerializer("utf_8")

        avro_config = {}
        if config.get("value.schema"):
            from confluent_kafka.schema_registry import Schema

            schema = Schema(config["value.schema"], "AVRO")
            self._value_serializer = AvroSerializer(
                self._sr_client,
                schema,
                to_dict=lambda obj, ctx: obj,
            )
        else:
            self._value_serializer = AvroSerializer(
                self._sr_client,
                to_dict=lambda obj, ctx: obj,
            )

        # Build producer config with C4E mandatory settings
        client_id = config.get(
            "client.id",
            f"fsi-{self._topic_name.split('.')[0]}-producer-python",
        )

        producer_config = {
            "bootstrap.servers": config.get(
                "bootstrap.servers", "localhost:9092"
            ),
            # C4E MANDATORY: Idempotent producer
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            # Reliability
            "retries": 2147483647,
            "delivery.timeout.ms": 120000,
            "request.timeout.ms": 30000,
            # Performance (latency-optimized for fraud detection)
            "compression.type": "none",       # no compression overhead at low TPS
            "batch.size": 16384,              # 16KB — absorbs micro-bursts
            "linger.ms": 0,                   # immediate dispatch
            # Azure connection management
            "socket.keepalive.enable": True,  # default FALSE — Azure ILB kills idle TCP at 4min
            "connections.max.idle.ms": 180000, # 3min — recycle before Azure ILB fires
            "reconnect.backoff.max.ms": 1000, # cap at 1s — default 10s too slow for fraud
            # Client identification
            "client.id": client_id,
        }

        # Add SASL config only if credentials provided
        if config.get("sasl.username"):
            producer_config["security.protocol"] = "SASL_SSL"
            producer_config["sasl.mechanism"] = "PLAIN"
            producer_config["sasl.username"] = config["sasl.username"]
            producer_config["sasl.password"] = config.get(
                "sasl.password", ""
            )

        self._producer = Producer(producer_config)
        self._client_id = client_id

        # Start Prometheus metrics HTTP server in daemon thread
        metrics_port = int(config.get("metrics.port", "9090"))
        try:
            start_http_server(metrics_port)
            log.info("Prometheus metrics server started on port %d", metrics_port)
        except OSError:
            log.warning(
                "Metrics port %d already in use -- metrics server not started",
                metrics_port,
            )

        # DLQ handler
        self._dlq_handler = None
        if config.get("dlq.enabled", "true").lower() != "false":
            self._dlq_handler = FsiDlqHandler(config, self._topic_name)

        log.info(
            "FSI Producer initialized for topic: %s [client.id=%s, dlq=%s]",
            self._topic_name,
            client_id,
            "enabled" if self._dlq_handler else "disabled",
        )

    def send(self, key: str, value: dict):
        """Send a record asynchronously with callback-based error handling.

        The key should be the natural business key (e.g., account_number,
        transaction_id). The value should be a dict matching the Avro schema.

        Args:
            key: Message key (business identifier).
            value: Message value as dict (serialized via AvroSerializer).
        """
        start_ms = time.time() * 1000

        # Serialize key and value
        serialized_key = self._key_serializer(
            key,
            SerializationContext(self._topic_name, MessageField.KEY),
        )
        serialized_value = self._value_serializer(
            value,
            SerializationContext(self._topic_name, MessageField.VALUE),
        )

        def delivery_callback(err, msg):
            latency = time.time() * 1000 - start_ms
            self._last_latency_ms.labels(topic=self._topic_name).set(latency)

            if err is not None:
                self._total_errors.labels(topic=self._topic_name).inc()
                log.error(
                    "Failed to produce to %s [key=%s]: %s",
                    self._topic_name,
                    key,
                    err,
                )

                # DLQ routing with retry classification
                if self._dlq_handler is not None:
                    key_bytes = (
                        key.encode("utf-8") if key is not None else None
                    )
                    self._dlq_handler.handle_error(
                        key_bytes, serialized_value, err, 0
                    )
                    self._dlq_sent.labels(topic=self._topic_name).inc()
            else:
                self._total_sent.labels(topic=self._topic_name).inc()
                log.debug(
                    "Produced to %s-%d offset=%d latency=%.0fms",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    latency,
                )

        self._producer.produce(
            topic=self._topic_name,
            key=serialized_key,
            value=serialized_value,
            on_delivery=delivery_callback,
        )
        self._producer.poll(0)

    def send_sync(self, key: str, value: dict) -> dict:
        """Send a record synchronously (blocking).

        Use sparingly -- only when you need the offset before proceeding
        (e.g., transaction reconciliation).

        Args:
            key: Message key (business identifier).
            value: Message value as dict (serialized via AvroSerializer).

        Returns:
            Dict with 'topic', 'partition', 'offset' of the produced message.

        Raises:
            KafkaException: If the produce fails.
        """
        result = {}
        error_holder = [None]

        def delivery_callback(err, msg):
            if err is not None:
                error_holder[0] = err
                self._total_errors.labels(topic=self._topic_name).inc()
            else:
                result["topic"] = msg.topic()
                result["partition"] = msg.partition()
                result["offset"] = msg.offset()
                self._total_sent.labels(topic=self._topic_name).inc()

        # Serialize
        serialized_key = self._key_serializer(
            key,
            SerializationContext(self._topic_name, MessageField.KEY),
        )
        serialized_value = self._value_serializer(
            value,
            SerializationContext(self._topic_name, MessageField.VALUE),
        )

        self._producer.produce(
            topic=self._topic_name,
            key=serialized_key,
            value=serialized_value,
            on_delivery=delivery_callback,
        )
        self._producer.flush()

        if error_holder[0] is not None:
            raise KafkaException(error_holder[0])

        return result

    def flush(self):
        """Flush pending records. Call before shutdown or when you need
        durability confirmation."""
        self._producer.flush()

    def close(self):
        """Graceful shutdown: flush with 30s timeout and log final metrics."""
        log.info("Shutting down producer for topic: %s", self._topic_name)
        self._producer.flush(timeout=30)

        sent = self._total_sent.labels(topic=self._topic_name)._value.get()
        errors = self._total_errors.labels(topic=self._topic_name)._value.get()
        dlq = self._dlq_sent.labels(topic=self._topic_name)._value.get()

        log.info(
            "Producer closed. Total sent: %d, errors: %d, DLQ: %d",
            sent,
            errors,
            dlq,
        )

        if self._dlq_handler:
            self._dlq_handler.close()

    def __enter__(self):
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close producer on context manager exit."""
        self.close()
        return False
