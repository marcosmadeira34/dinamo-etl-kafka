# dinamo_web/src/dinamo_web/kafka/producer.py
"""
Resilient Kafka producer with idempotence, retry, and DLQ routing.

Design decisions:
- Idempotent by default (exactly-once at producer level)
- Asynchronous send with delivery callbacks
- Automatic DLQ routing on permanent failure
- CNPJ-based partitioning for data locality
- LZ4 compression for SPED text data
- Flush on close for graceful shutdown
"""
import json
import time
import signal
import logging
import hashlib
from typing import Optional, Callable, Any, Dict
from confluent_kafka import Producer as ConfluentProducer, KafkaError, KafkaException

from .config import KafkaConfig
from .topics import TopicRegistry

logger = logging.getLogger("dinamo.kafka.producer")


class DinamoProducer:
    """
    Enterprise-grade Kafka producer for SPED data events.
    
    Features:
    - Idempotent writes (deduplication at broker)
    - Delivery confirmation callbacks
    - Automatic DLQ routing on delivery failure
    - Graceful shutdown with pending message flush
    - CNPJ-based key partitioning
    
    Usage:
        config = KafkaConfig()
        producer = DinamoProducer(config)
        producer.send(
            topic=TopicRegistry.RAW_INGEST.name,
            key="12345678",  # CNPJ base
            value={"file_path": "...", "sped_type": "EFD_FISCAL"},
        )
        producer.close()
    """

    def __init__(self, kafka_config: KafkaConfig, on_error: Optional[Callable] = None):
        self._config = kafka_config
        self._on_error = on_error
        self._producer = ConfluentProducer(kafka_config.producer_config())
        self._running = True
        self._pending = 0

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info(
            f"DinamoProducer initialized | "
            f"brokers={kafka_config.bootstrap_servers} | "
            f"compression={kafka_config.producer_compression_type} | "
            f"idempotence={kafka_config.producer_enable_idempotence}"
        )

    def _handle_shutdown(self, signum, frame):
        """Graceful shutdown: flush pending messages before exit."""
        logger.info(f"Shutdown signal received (sig={signum}). Flushing {self._pending} pending messages...")
        self._running = False
        self.close()

    def _delivery_callback(self, err, msg):
        """
        Delivery report callback.
        Called once per message to indicate delivery success or failure.
        """
        self._pending -= 1

        if err is not None:
            logger.error(
                f"Message delivery FAILED | "
                f"topic={msg.topic()} | partition={msg.partition()} | "
                f"error={err.str()}"
            )
            # Route to DLQ
            self._route_to_dlq(msg, str(err))
        else:
            logger.debug(
                f"Message delivered | "
                f"topic={msg.topic()} | partition={msg.partition()} | "
                f"offset={msg.offset()} | key={msg.key()}"
            )

    def _route_to_dlq(self, original_msg, error_message: str):
        """
        Routes failed messages to the Dead Letter Queue.
        Enriches with error metadata for debugging.
        """
        try:
            dlq_payload = {
                "original_topic": original_msg.topic(),
                "original_partition": original_msg.partition(),
                "original_key": original_msg.key().decode("utf-8") if original_msg.key() else None,
                "original_value": original_msg.value().decode("utf-8") if original_msg.value() else None,
                "error_message": error_message,
                "timestamp_ms": int(time.time() * 1000),
                "retry_count": 0,
            }

            self._producer.produce(
                topic=TopicRegistry.ERRORS_DLQ.name,
                key=original_msg.key(),
                value=json.dumps(dlq_payload).encode("utf-8"),
            )
            logger.warning(
                f"Message routed to DLQ | "
                f"original_topic={original_msg.topic()} | error={error_message}"
            )
        except Exception as e:
            logger.critical(f"CRITICAL: Failed to route message to DLQ: {e}")

    def send(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: int = -1,
    ):
        """
        Send a message to a Kafka topic.
        
        Args:
            topic: Target topic name
            value: Message payload (dict, will be JSON serialized)
            key: Partition key (typically CNPJ base for data locality)
            headers: Optional message headers
            partition: Specific partition (-1 for auto)
        """
        if not self._running:
            raise RuntimeError("Producer is shutting down, cannot accept new messages")

        try:
            serialized_value = json.dumps(value, default=str).encode("utf-8")
            serialized_key = key.encode("utf-8") if key else None

            kafka_headers = (
                [(k, v.encode("utf-8")) for k, v in headers.items()]
                if headers else None
            )

            kwargs = {
                "topic": topic,
                "value": serialized_value,
                "callback": self._delivery_callback,
            }
            if serialized_key:
                kwargs["key"] = serialized_key
            if kafka_headers:
                kwargs["headers"] = kafka_headers
            if partition >= 0:
                kwargs["partition"] = partition

            self._producer.produce(**kwargs)
            self._pending += 1

            # Poll for delivery callbacks (non-blocking)
            self._producer.poll(0)

        except BufferError:
            logger.warning("Producer buffer full. Flushing and retrying...")
            self._producer.flush(timeout=10)
            self.send(topic, value, key, headers, partition)
        except KafkaException as e:
            logger.error(f"Kafka produce error: {e}")
            raise

    def send_batch(self, topic: str, messages: list, key_field: Optional[str] = None):
        """
        Send a batch of messages efficiently.
        
        Args:
            topic: Target topic
            messages: List of dicts to send
            key_field: Field name in each dict to use as partition key
        """
        for msg in messages:
            key = str(msg.get(key_field, "")) if key_field else None
            self.send(topic=topic, value=msg, key=key)

        # Flush after batch
        self._producer.flush(timeout=30)
        logger.info(f"Batch sent | topic={topic} | count={len(messages)}")

    @staticmethod
    def cnpj_partition_key(cnpj: str) -> str:
        """
        Generates a consistent partition key from CNPJ.
        Uses first 8 digits (CNPJ base) for entity-level partitioning.
        """
        import re
        clean = re.sub(r"\D", "", str(cnpj))
        return clean[:8] if len(clean) >= 8 else clean

    def flush(self, timeout: float = 30.0):
        """Flush all pending messages."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still pending after flush timeout")
        return remaining

    def close(self):
        """Gracefully close the producer, flushing all pending messages."""
        logger.info(f"Closing producer. Flushing {self._pending} pending messages...")
        self._producer.flush(timeout=60)
        logger.info("Producer closed successfully")
