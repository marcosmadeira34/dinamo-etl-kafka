#!/bin/bash
# =============================================================================
# Kafka Topic Initialization Script
# Creates all domain topics with proper partitioning and replication
# =============================================================================
set -euo pipefail

BOOTSTRAP="kafka-1:9092"
REPLICATION=3
RETENTION_MS=604800000  # 7 days

echo "========================================"
echo " DINAMO ETL — Kafka Topic Initialization"
echo "========================================"

create_topic() {
    local TOPIC=$1
    local PARTITIONS=$2
    local CLEANUP=${3:-delete}
    local RETENTION=${4:-$RETENTION_MS}

    echo ""
    echo "▸ Creating topic: ${TOPIC}"
    echo "  Partitions: ${PARTITIONS} | Replication: ${REPLICATION} | Cleanup: ${CLEANUP}"

    kafka-topics --create \
        --if-not-exists \
        --bootstrap-server "${BOOTSTRAP}" \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        --config cleanup.policy="${CLEANUP}" \
        --config retention.ms="${RETENTION}" \
        --config min.insync.replicas=2 \
        --config compression.type=lz4 \
        --config max.message.bytes=10485760

    echo "  ✔ Topic ${TOPIC} created successfully"
}

# --- INGEST TOPICS ---
# File arrival events — partitioned by CNPJ hash
create_topic "sped.raw.ingest"          24   "delete"   "${RETENTION_MS}"

# --- BRONZE TOPICS ---
# Parsed SPED records — high partition count for parallelism
create_topic "sped.parsed.bronze"       24   "delete"   "${RETENTION_MS}"

# --- SILVER TOPICS ---
# Enriched records
create_topic "sped.enriched.silver"     24   "delete"   "${RETENTION_MS}"

# --- GOLD TOPICS ---
# Analytical aggregates
create_topic "sped.analytics.gold"      12   "delete"   "${RETENTION_MS}"

# --- ERROR HANDLING ---
# Dead letter queue — delete retention (preserve ALL error evidence)
create_topic "sped.errors.dlq"          12   "delete"   "-1"

# Retry topic — records pending re-processing
create_topic "sped.retry"               12   "delete"   "3600000"  # 1h retention

# --- METADATA / CONTROL ---
# Schema change notifications
create_topic "sped.schema.changes"       3   "compact"  "-1"

# Pipeline health/heartbeat
create_topic "sped.pipeline.heartbeat"   3   "delete"   "86400000"  # 24h

echo ""
echo "========================================"
echo " All topics created successfully!"
echo "========================================"
echo ""

# List all topics
echo "▸ Current topics:"
kafka-topics --list --bootstrap-server "${BOOTSTRAP}" | grep "sped\."
echo ""

# Describe topics
for t in sped.raw.ingest sped.parsed.bronze sped.enriched.silver sped.analytics.gold sped.errors.dlq sped.retry; do
    echo "▸ Topic details: ${t}"
    kafka-topics --describe --bootstrap-server "${BOOTSTRAP}" --topic "${t}" 2>/dev/null | head -3
    echo ""
done
