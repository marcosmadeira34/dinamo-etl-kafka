# dinamo_web/src/dinamo_web/kafka/dlq_handler.py
"""
Dead Letter Queue handler with configurable retry logic.
Exponential backoff, max retry count, permanent DLQ routing.
"""
import json
import time
import logging
from typing import Optional
from .config import KafkaConfig
from .producer import DinamoProducer
from .topics import TopicRegistry

logger = logging.getLogger("dinamo.kafka.dlq")


class DLQHandler:
    MAX_RETRIES = 5
    BASE_BACKOFF_SEC = 1.0
    MAX_BACKOFF_SEC = 60.0

    def __init__(self, kafka_config: KafkaConfig):
        self._config = kafka_config
        self._producer = DinamoProducer(kafka_config)

    def route_to_retry(self, original_topic, key, value, error_message, retry_count=0):
        if retry_count >= self.MAX_RETRIES:
            self._route_to_permanent_dlq(original_topic, key, value, error_message, retry_count)
            return
        backoff = min(self.BASE_BACKOFF_SEC * (2 ** retry_count), self.MAX_BACKOFF_SEC)
        retry_payload = {
            "original_topic": original_topic,
            "original_key": key,
            "original_value": value,
            "error_message": error_message,
            "retry_count": retry_count + 1,
            "next_retry_at_ms": int((time.time() + backoff) * 1000),
            "created_at_ms": int(time.time() * 1000),
        }
        self._producer.send(topic=TopicRegistry.RETRY.name, key=key or "unknown", value=retry_payload)
        logger.info(f"Message routed to retry | topic={original_topic} | attempt={retry_count + 1} | backoff={backoff:.1f}s")

    def _route_to_permanent_dlq(self, original_topic, key, value, error_message, retry_count):
        dlq_payload = {
            "original_topic": original_topic,
            "original_key": key,
            "original_value": value,
            "error_message": error_message,
            "total_retries": retry_count,
            "final_failure_at_ms": int(time.time() * 1000),
            "status": "PERMANENTLY_FAILED",
        }
        self._producer.send(topic=TopicRegistry.ERRORS_DLQ.name, key=key or "unknown", value=dlq_payload)
        logger.warning(f"Message routed to permanent DLQ | topic={original_topic} | retries={retry_count}")

    def _handle_retry_message(self, message: dict):
        retry_count = message.get("retry_count", 0)
        original_topic = message.get("original_topic")
        original_key = message.get("original_key")
        original_value = message.get("original_value")
        next_retry_at_ms = message.get("next_retry_at_ms", 0)
        now_ms = int(time.time() * 1000)
        if now_ms < next_retry_at_ms:
            time.sleep(min((next_retry_at_ms - now_ms) / 1000.0, self.MAX_BACKOFF_SEC))
        try:
            self._producer.send(topic=original_topic, key=original_key, value=original_value,
                                headers={"X-Retry-Count": str(retry_count)})
        except Exception as e:
            self._route_to_permanent_dlq(original_topic, original_key, original_value, str(e), retry_count)

    def close(self):
        self._producer.close()
