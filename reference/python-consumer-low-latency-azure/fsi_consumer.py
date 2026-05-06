"""FSI C4E Reference Consumer -- Python.

Mirrors Java FsiConsumer: manual commit, Avro deserialization, handler function
pattern, Prometheus metrics, graceful shutdown.
This is a reference implementation. Copy and adapt for your application.
"""

import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger(__name__)


class FsiConsumer:
    """FSI C4E Reference Consumer for Python applications.

    Key characteristics:
      - Avro deserialization with Schema Registry (modern AvroDeserializer API)
      - Manual offset commit (at-least-once delivery guarantee)
      - Graceful shutdown via SIGTERM/SIGINT signal handlers
      - Prometheus metrics exposed via HTTP endpoint
      - Configurable record handler via callable

    Usage:
        def handle_record(key: str, value: dict):
            print(f"Received: {key} -> {value}")

        config = {
            "group.id": "corebanking-txn-processor",
            "topics": "corebanking.transactions.v1.account-transaction",
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081",
        }
        with FsiConsumer(config, handle_record) as consumer:
            consumer.start()  # Blocks until shutdown signal
    """

    # Prometheus metrics (shared across instances, labeled by group)
    _total_consumed = Counter(
        "fsi_consumer_total_consumed",
        "Total messages consumed successfully",
        ["group"],
    )
    _total_errors = Counter(
        "fsi_consumer_total_errors",
        "Total processing errors",
        ["group"],
    )
    _last_poll_count = Gauge(
        "fsi_consumer_last_poll_count",
        "Number of records in last poll batch",
        ["group"],
    )
    _last_commit_latency_ms = Gauge(
        "fsi_consumer_last_commit_latency_ms",
        "Last commit latency in milliseconds",
        ["group"],
    )

    def __init__(self, config: dict, handler: callable):
        """Create a consumer from the standard FSI configuration pattern.

        Args:
            config: Dict containing at minimum:
              - group.id (required): Consumer group identifier
              - topics (required): Comma-separated topic names
              - bootstrap.servers: Kafka broker address (default: localhost:9092)
              - schema.registry.url: SR endpoint (default: http://localhost:8081)
              - schema.registry.basic.auth.user.info: SR credentials
              - sasl.username: Confluent Cloud API key (enables SASL_SSL)
              - sasl.password: Confluent Cloud API secret
              - client.id: Custom client identifier
              - metrics.port: Prometheus HTTP port (default: "9091")
            handler: Callable(key: str, value: dict) invoked for each record.

        Raises:
            ValueError: If group.id or topics is missing.
        """
        self._group_id = config.get("group.id")
        if not self._group_id:
            raise ValueError("group.id is required")

        topics_str = config.get("topics", "")
        if not topics_str:
            raise ValueError("topics is required (comma-separated)")
        self._topics = [t.strip() for t in topics_str.split(",") if t.strip()]

        self._handler = handler
        self._running = False

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

        sr_client = SchemaRegistryClient(sr_config)

        # Deserializers -- modern API
        self._key_deserializer = StringDeserializer("utf_8")
        self._value_deserializer = AvroDeserializer(sr_client)

        # Build consumer config with C4E mandatory settings
        client_id = config.get(
            "client.id",
            f"fsi-{self._group_id}-consumer-python",
        )

        consumer_config = {
            "bootstrap.servers": config.get(
                "bootstrap.servers", "localhost:9092"
            ),
            "group.id": self._group_id,
            # C4E MANDATORY: Manual commit
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            # Performance (latency-optimized for fraud detection)
            "max.poll.interval.ms": 600000,       # 10 min — headroom for backpressure
            "fetch.min.bytes": 1,                 # return immediately with any data
            "fetch.wait.max.ms": 0,               # no broker-side batching delay
            # Rebalance strategy
            "partition.assignment.strategy": "cooperative-sticky",
            # Azure connection management
            "socket.keepalive.enable": True,      # default FALSE — Azure ILB kills idle TCP at 4min
            "connections.max.idle.ms": 180000,    # 3min — recycle before Azure ILB fires
            "reconnect.backoff.max.ms": 1000,     # cap at 1s — default 10s too slow for fraud
            # Client identification
            "client.id": client_id,
        }

        # Static group membership (eliminates rebalance on pod restart)
        instance_id = config.get("group.instance.id")
        if instance_id:
            consumer_config["group.instance.id"] = instance_id

        # Add SASL config only if credentials provided
        if config.get("sasl.username"):
            consumer_config["security.protocol"] = "SASL_SSL"
            consumer_config["sasl.mechanism"] = "PLAIN"
            consumer_config["sasl.username"] = config["sasl.username"]
            consumer_config["sasl.password"] = config.get(
                "sasl.password", ""
            )

        self._consumer = Consumer(consumer_config)

        # Start Prometheus metrics HTTP server in daemon thread
        metrics_port = int(config.get("metrics.port", "9091"))
        try:
            start_http_server(metrics_port)
            log.info("Prometheus metrics server started on port %d", metrics_port)
        except OSError:
            log.warning(
                "Metrics port %d already in use -- metrics server not started",
                metrics_port,
            )

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        log.info(
            "FSI Consumer initialized [group=%s, topics=%s, client.id=%s]",
            self._group_id,
            self._topics,
            client_id,
        )

    def _signal_handler(self, signum, frame):
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        log.info("Signal %d received -- initiating shutdown", signum)
        self.shutdown()

    def start(self):
        """Start the consume loop. Blocks until shutdown signal.

        Subscribes to configured topics and polls continuously. For each
        message: deserialize key/value, invoke handler, commit offset.
        Errors in the handler are logged and skipped (at-least-once semantics).
        """
        self._running = True
        self._consumer.subscribe(self._topics)
        log.info("Consumer subscribed to: %s", self._topics)

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        log.debug(
                            "Reached end of partition %s-%d at offset %d",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                        continue
                    log.error("Consumer error: %s", msg.error())
                    self._total_errors.labels(group=self._group_id).inc()
                    continue

                # Deserialize key and value
                try:
                    key = self._key_deserializer(
                        msg.key(),
                        SerializationContext(msg.topic(), MessageField.KEY),
                    )
                    value = self._value_deserializer(
                        msg.value(),
                        SerializationContext(msg.topic(), MessageField.VALUE),
                    )
                except Exception as e:
                    self._total_errors.labels(group=self._group_id).inc()
                    log.error(
                        "Deserialization error [topic=%s, partition=%d, offset=%d]: %s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        e,
                    )
                    continue

                # Invoke handler
                try:
                    self._handler(key, value)
                    self._total_consumed.labels(group=self._group_id).inc()
                except Exception as e:
                    self._total_errors.labels(group=self._group_id).inc()
                    log.error(
                        "Error processing record [topic=%s, partition=%d, offset=%d, key=%s]: %s",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        key,
                        e,
                    )
                    # Default: log and continue (at-least-once semantics)

                # Commit offset after successful processing
                try:
                    commit_start = time.time() * 1000
                    self._consumer.commit(asynchronous=False)
                    commit_latency = time.time() * 1000 - commit_start
                    self._last_commit_latency_ms.labels(
                        group=self._group_id
                    ).set(commit_latency)
                except KafkaException as e:
                    log.error("Commit failed: %s", e)

                self._last_poll_count.labels(group=self._group_id).set(1)

        finally:
            consumed = self._total_consumed.labels(
                group=self._group_id
            )._value.get()
            errors = self._total_errors.labels(
                group=self._group_id
            )._value.get()
            log.info(
                "Closing consumer. Total consumed: %d, errors: %d",
                consumed,
                errors,
            )
            self._consumer.close()

    def shutdown(self):
        """Signal the consumer to stop. Safe to call from signal handler or
        another thread."""
        log.info("Shutdown requested for group: %s", self._group_id)
        self._running = False

    def __enter__(self):
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Shutdown consumer on context manager exit."""
        self.shutdown()
        return False
