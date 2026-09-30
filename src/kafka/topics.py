# dinamo_web/src/dinamo_web/kafka/topics.py
"""
Topic registry — single source of truth for all Kafka topic names and metadata.

Design decisions:
- Domain-driven naming: sped.<layer>.<purpose>
- Partition counts sized for target throughput
- Centralized to prevent topic name drift across producers/consumers
"""
from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class TopicSpec:
    """Immutable topic specification."""
    name: str
    partitions: int
    replication_factor: int = 3
    retention_ms: int = 604800000  # 7 days
    cleanup_policy: str = "delete"
    compression_type: str = "lz4"
    description: str = ""


class TopicRegistry:
    """
    Central registry of all Kafka topics used by the Dinamo ETL platform.
    
    Topic naming convention: sped.<domain>.<purpose>
    
    Partition strategy:
    - Ingest/Gold: 12 partitions (moderate throughput, CNPJ-keyed)
    - Bronze/Silver: 24 partitions (high throughput, parallel processing)
    - DLQ/Retry: 6 partitions (low throughput, operational)
    """

    # --- Ingestion ---
    RAW_INGEST = TopicSpec(
        name="sped.raw.ingest",
        partitions=24,
        description="Object Storage Events"
    )

    # --- Bronze ---
    PARSED_BRONZE = TopicSpec(
        name="sped.parsed.bronze",
        partitions=24,
        description="Parsed SPED records (TXT → structured)"
    )

    # --- Silver ---
    ENRICHED_SILVER = TopicSpec(
        name="sped.enriched.silver",
        partitions=24,
        description="Enriched records (parent-child, schema, tax regime)"
    )

    # --- Gold ---
    ANALYTICS_GOLD = TopicSpec(
        name="sped.analytics.gold",
        partitions=12,
        description="Analytical aggregates ready for BI consumption"
    )

    SUBVENCAO_GOLD = TopicSpec(
        name="sped.analytics.subvencao",
        partitions=6,
        description="Subvencao Fiscal (isencao/reducao) — diagnostico concluido" 
    )

    # --- Error Handling ---
    ERRORS_DLQ = TopicSpec(
        name="sped.errors.dlq",
        partitions=12,
        retention_ms=-1,  # infinite retention
        cleanup_policy="delete",
        description="Dead letter queue for permanently failed records"
    )

    RETRY = TopicSpec(
        name="sped.retry",
        partitions=12,
        retention_ms=3600000,  # 1 hour
        description="Records pending re-processing"
    )

    # --- Control ---
    SCHEMA_CHANGES = TopicSpec(
        name="sped.schema.changes",
        partitions=3,
        retention_ms=-1,
        cleanup_policy="compact",
        description="Schema evolution notifications"
    )

    PIPELINE_HEARTBEAT = TopicSpec(
        name="sped.pipeline.heartbeat",
        partitions=3,
        retention_ms=86400000,  # 24h
        description="Pipeline health/liveness signals"
    )

    @classmethod
    def all_topics(cls) -> Dict[str, TopicSpec]:
        """Returns all registered topics as a dict keyed by topic name."""
        return {
            spec.name: spec
            for attr in dir(cls)
            if not attr.startswith("_")
            and isinstance(getattr(cls, attr), TopicSpec)
            for spec in [getattr(cls, attr)]
        }

    @classmethod
    def topic_names(cls) -> list:
        """Returns list of all topic names."""
        return list(cls.all_topics().keys())
