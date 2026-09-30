# dinamo_web/src/dinamo_web/kafka/__init__.py
"""
Kafka integration layer for Dinamo ETL Platform.

Provides:
- Resilient producers with idempotence and retry
- Consumer groups with exactly-once semantics
- Avro serialization via Schema Registry
- Dead Letter Queue handling
- Topic registry with domain-driven naming
"""
from .config import KafkaConfig
from .topics import TopicRegistry

__all__ = ["KafkaConfig", "TopicRegistry"]
