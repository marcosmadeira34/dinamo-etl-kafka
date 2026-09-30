# dinamo_web/src/dinamo_web/observability/metrics.py
"""
Prometheus metrics for the Dinamo ETL Platform.
Exposes counters, histograms, and gauges for pipeline observability.
"""
import logging
from prometheus_client import Counter, Histogram, Gauge, start_http_server

logger = logging.getLogger("dinamo.observability.metrics")

# --- Counters ---
FILES_INGESTED = Counter(
    "sped_files_ingested_total",
    "Total SPED files ingested from bucket",
    ["sped_type"]
)

RECORDS_PROCESSED = Counter(
    "sped_records_processed_total",
    "Total records processed across all layers",
    ["layer", "sped_type", "register"]
)

ERRORS_TOTAL = Counter(
    "sped_errors_total",
    "Total processing errors",
    ["layer", "error_type"]
)

DLQ_MESSAGES = Counter(
    "sped_dlq_messages_total",
    "Messages routed to Dead Letter Queue",
    ["original_topic"]
)

# --- Histograms ---
PROCESSING_DURATION = Histogram(
    "sped_processing_duration_seconds",
    "Processing duration per batch",
    ["layer", "sped_type"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300]
)

BATCH_SIZE = Histogram(
    "sped_batch_size_records",
    "Number of records per micro-batch",
    ["layer"],
    buckets=[10, 50, 100, 500, 1000, 5000, 10000, 50000]
)

# --- Gauges ---
CONSUMER_LAG = Gauge(
    "sped_kafka_consumer_lag",
    "Current consumer lag",
    ["consumer_group", "topic", "partition"]
)

ACTIVE_STREAMS = Gauge(
    "sped_active_streams",
    "Number of active streaming queries",
)

LAST_BATCH_TIMESTAMP = Gauge(
    "sped_last_batch_timestamp_seconds",
    "Timestamp of last processed batch",
    ["layer"]
)


def start_metrics_server(port: int = 8000):
    """Start Prometheus HTTP metrics endpoint."""
    start_http_server(port)
    logger.info(f"Prometheus metrics server started on port {port}")
