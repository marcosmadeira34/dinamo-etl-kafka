# dinamo_web/src/dinamo_web/kafka/config.py
"""
Centralized Kafka configuration for producers, consumers, and admin clients.

Design decisions:
- Idempotence enabled by default (exactly-once producer semantics)
- LZ4 compression for optimal throughput/CPU ratio on SPED data
- Configurable via YAML or environment variables
- Separate configs for producer/consumer/admin to enforce least-privilege
"""
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger("dinamo.kafka.config")


@dataclass(frozen=True)
class KafkaConfig:
    """
    Immutable Kafka configuration.
    
    Bootstrap servers default to Docker Compose internal network.
    Override via KAFKA_BOOTSTRAP_SERVERS env var for external access.
    """
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            # "kafka-1:9092,kafka-2:9092,kafka-3:9092"
            "kafka:9092"
        )
    )
    schema_registry_url: str = field(
        default_factory=lambda: os.getenv(
            "SCHEMA_REGISTRY_URL",
            "http://schema-registry:8081"
        )
    )
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    # --- Producer defaults ---
    producer_acks: str = "all"
    producer_retries: int = 10
    producer_max_in_flight: int = 5  # safe with idempotence
    producer_enable_idempotence: bool = True
    producer_compression_type: str = "lz4"
    producer_linger_ms: int = 20  # batch window for throughput
    producer_batch_size: int = 65536  # 64KB batches
    producer_buffer_memory: int = 33554432  # 32MB buffer
    producer_max_request_size: int = 2097152  # 2MB max message
    producer_request_timeout_ms: int = 30000  # 30s request timeout
    producer_delivery_timeout_ms: int = 60000  # 60s delivery timeout
    producer_retry_backoff_ms: int = 500

    # --- Consumer defaults ---
    consumer_group_id: str = "dinamo-etl-consumer"
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = False  # manual commit for exactly-once
    consumer_max_poll_records: int = 500
    consumer_max_poll_interval_ms: int = 300000  # 5 min
    consumer_session_timeout_ms: int = 45000
    consumer_heartbeat_interval_ms: int = 15000
    consumer_fetch_min_bytes: int = 1
    consumer_fetch_max_wait_ms: int = 500

    @classmethod
    def from_yaml(cls, config: dict) -> "KafkaConfig":
        """Create KafkaConfig from YAML config dict."""
        kafka_cfg = config.get("kafka", {})
        producer_cfg = kafka_cfg.get("producer", {})
        return cls(
            bootstrap_servers=kafka_cfg.get(
                "bootstrap_servers",
                # os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            ),
            schema_registry_url=kafka_cfg.get(
                "schema_registry_url",
                os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
            ),
            security_protocol=kafka_cfg.get("security_protocol", "PLAINTEXT"),
            sasl_mechanism=kafka_cfg.get("sasl_mechanism"),
            sasl_username=kafka_cfg.get("sasl_username"),
            sasl_password=kafka_cfg.get("sasl_password"),
            producer_compression_type=kafka_cfg.get("compression_type", "lz4"),
            producer_linger_ms=producer_cfg.get("linger_ms", 20),
            producer_batch_size=producer_cfg.get("batch_size", 65536),
            producer_max_request_size=producer_cfg.get("max_request_size", 2097152),
            producer_request_timeout_ms=producer_cfg.get("request_timeout_ms", 30000),
            producer_delivery_timeout_ms=producer_cfg.get("delivery_timeout_ms", 60000),
            producer_retry_backoff_ms=producer_cfg.get("retry_backoff_ms", 500),
            consumer_group_id=kafka_cfg.get("consumer", {}).get("group_id", "dinamo-etl-consumer"),
            consumer_max_poll_records=kafka_cfg.get("consumer", {}).get("max_poll_records", 500),
        )

    def producer_config(self) -> Dict:
        """Returns confluent-kafka producer configuration dict."""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.producer_acks,
            "message.send.max.retries": self.producer_retries,
            "max.in.flight.requests.per.connection": self.producer_max_in_flight,
            "enable.idempotence": self.producer_enable_idempotence,
            "compression.type": self.producer_compression_type,
            "linger.ms": self.producer_linger_ms,
            "batch.size": self.producer_batch_size,
            "queue.buffering.max.kbytes": int(self.producer_buffer_memory / 1024),
            "message.max.bytes": self.producer_max_request_size,
            "request.timeout.ms": self.producer_request_timeout_ms,
            "delivery.timeout.ms": self.producer_delivery_timeout_ms,
            "retry.backoff.ms": self.producer_retry_backoff_ms,
            "security.protocol": self.security_protocol,
        }

        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username
            config["sasl.password"] = self.sasl_password

        return config

    def consumer_config(self, group_id: Optional[str] = None) -> Dict:
        """
        Returns confluent-kafka consumer configuration dict.
        
        Key tuning:
        - enable.auto.commit=false: manual commit after processing
        - max.poll.records=500: bounded batch size for backpressure
        - session.timeout.ms=45s: generous timeout for SPED processing
        """
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id or self.consumer_group_id,
            "auto.offset.reset": self.consumer_auto_offset_reset,
            "enable.auto.commit": self.consumer_enable_auto_commit,
            "max.poll.records": self.consumer_max_poll_records,
            "max.poll.interval.ms": self.consumer_max_poll_interval_ms,
            "session.timeout.ms": self.consumer_session_timeout_ms,
            "heartbeat.interval.ms": self.consumer_heartbeat_interval_ms,
            "fetch.min.bytes": self.consumer_fetch_min_bytes,
            "fetch.max.wait.ms": self.consumer_fetch_max_wait_ms,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username
            config["sasl.password"] = self.sasl_password
        return config

    def admin_config(self) -> Dict:
        """Returns confluent-kafka admin client configuration dict."""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.sasl_username
            config["sasl.password"] = self.sasl_password
        return config
