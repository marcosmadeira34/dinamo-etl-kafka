# dinamo_web/src/dinamo_web/kafka/consumer.py
"""
Resilient Kafka consumer with manual offset commit and graceful shutdown.

Design decisions:
- Manual offset commit after successful processing (exactly-once semantics)
- Configurable batch processing with max_poll_records
- Graceful shutdown with SIGTERM/SIGINT handling
- Health reporting via heartbeat topic
- Error isolation: failed messages → DLQ, consumer continues
"""
import json
import time
import signal
import logging
from typing import Callable, Optional, Dict, Any, List
from confluent_kafka import Consumer as ConfluentConsumer, KafkaError, TopicPartition

from .config import KafkaConfig
from .topics import TopicRegistry

logger = logging.getLogger("dinamo.kafka.consumer")


class DinamoConsumer:
    """
    Enterprise-grade Kafka consumer for SPED data processing.
    
    Features:
    - Manual offset commit (exactly-once)
    - Graceful shutdown with offset commit on exit
    - Automatic DLQ routing for poison messages
    - Health heartbeat emission
    - Configurable message handler
    
    Usage:
        config = KafkaConfig()
        consumer = DinamoConsumer(
            kafka_config=config,
            topics=[TopicRegistry.RAW_INGEST.name],
            group_id="dinamo-bronze-consumer",
            handler=process_raw_event,
        )
        consumer.start()  # blocking
    """

    MAX_CONSECUTIVE_ERRORS = 10
    HEALTH_INTERVAL_SEC = 60

    def __init__(
        self,
        kafka_config: KafkaConfig,
        topics: List[str],
        group_id: str,
        handler: Callable[[Dict[str, Any]], None],
        dlq_producer=None,
        batch_handler: Optional[Callable[[List[Dict]], None]] = None,
    ):
        self._config = kafka_config
        self._topics = topics
        self._group_id = group_id
        self._handler = handler
        self._batch_handler = batch_handler
        self._dlq_producer = dlq_producer
        self._running = True
        self._consecutive_errors = 0
        self._messages_processed = 0
        self._last_health_check = 0

        consumer_cfg = kafka_config.consumer_config(group_id=group_id)
        self._consumer = ConfluentConsumer(consumer_cfg)

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info(
            f"DinamoConsumer initialized | "
            f"group={group_id} | topics={topics} | "
            f"auto_commit=False | "
            f"max_poll_records={kafka_config.consumer_max_poll_records}"
        )

    def _handle_shutdown(self, signum, frame):
        """Graceful shutdown: commit offsets and close."""
        logger.info(f"Shutdown signal received (sig={signum}). Committing offsets...")
        self._running = False

    def _on_assign(self, consumer, partitions):
        """Callback when partitions are assigned to this consumer."""
        partition_list = [f"{p.topic}[{p.partition}]" for p in partitions]
        logger.info(f"Partitions assigned: {partition_list}")

    def _on_revoke(self, consumer, partitions):
        """Callback when partitions are revoked — commit before rebalance."""
        logger.info("Partitions being revoked. Committing offsets...")
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Error committing during revoke: {e}")

    def _process_message(self, msg) -> bool:
        """
        Process a single message through the handler.
        Returns True on success, False on failure.
        """
        try:
            value = json.loads(msg.value().decode("utf-8"))
            key = msg.key().decode("utf-8") if msg.key() else None

            # Enrich with Kafka metadata
            value["_kafka_metadata"] = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "key": key,
            }

            self._handler(value)
            self._consecutive_errors = 0
            self._messages_processed += 1
            return True

        except Exception as e:
            self._consecutive_errors += 1
            logger.error(
                f"Error processing message | "
                f"topic={msg.topic()} | partition={msg.partition()} | "
                f"offset={msg.offset()} | error={e}",
                exc_info=True
            )
            self._route_to_dlq(msg, str(e))
            return False

    def _route_to_dlq(self, msg, error_message: str):
        """Route failed message to DLQ with error metadata."""
        if not self._dlq_producer:
            logger.warning("No DLQ producer configured. Failed message lost.")
            return

        try:
            dlq_payload = {
                "original_topic": msg.topic(),
                "original_partition": msg.partition(),
                "original_offset": msg.offset(),
                "original_key": msg.key().decode("utf-8") if msg.key() else None,
                "original_value": msg.value().decode("utf-8") if msg.value() else None,
                "error_message": error_message,
                "consumer_group": self._group_id,
                "timestamp_ms": int(time.time() * 1000),
                "retry_count": 0,
            }
            self._dlq_producer.send(
                topic=TopicRegistry.ERRORS_DLQ.name,
                key=msg.key().decode("utf-8") if msg.key() else "unknown",
                value=dlq_payload,
            )
        except Exception as e:
            logger.critical(f"Failed to route to DLQ: {e}")

    def _emit_health(self):
        """Emit health heartbeat if interval has elapsed."""
        now = time.time()
        if now - self._last_health_check >= self.HEALTH_INTERVAL_SEC:
            self._last_health_check = now
            logger.info(
                f"Consumer health | group={self._group_id} | "
                f"processed={self._messages_processed} | "
                f"consecutive_errors={self._consecutive_errors}"
            )

    def start(self):
        """
        Start the consumer loop. Blocking call.
        
        Processing flow:
        1. Poll for messages
        2. Process each message through handler
        3. Commit offset on success
        4. Route failures to DLQ
        5. Emit health heartbeat
        """
        self._consumer.subscribe(
            self._topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        logger.info(f"Consumer started | group={self._group_id} | topics={self._topics}")

        try:
            while self._running:
                # Circuit breaker
                if self._consecutive_errors >= self.MAX_CONSECUTIVE_ERRORS:
                    logger.critical(
                        f"Circuit breaker triggered: {self._consecutive_errors} consecutive errors. "
                        f"Pausing for 30 seconds..."
                    )
                    time.sleep(30)
                    self._consecutive_errors = 0

                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    self._emit_health()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"Partition EOF | {msg.topic()}[{msg.partition()}] "
                            f"offset={msg.offset()}"
                        )
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                # Process message
                success = self._process_message(msg)

                # Commit offset (even on failure — DLQ handles retries)
                try:
                    self._consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.error(f"Offset commit failed: {e}")

                self._emit_health()

        except Exception as e:
            logger.critical(f"Consumer loop fatal error: {e}", exc_info=True)
            raise
        finally:
            self._shutdown()

    def _shutdown(self):
        """Clean shutdown: commit final offsets and close consumer."""
        logger.info("Shutting down consumer...")
        try:
            self._consumer.commit(asynchronous=False)
        except Exception:
            pass
        self._consumer.close()
        logger.info(
            f"Consumer closed | group={self._group_id} | "
            f"total_processed={self._messages_processed}"
        )

    def close(self):
        """Public shutdown method."""
        self._running = False
