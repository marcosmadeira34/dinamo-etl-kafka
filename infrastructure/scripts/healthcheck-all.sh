#!/bin/bash
# =============================================================================
# Dinamo ETL Platform — Health Check for All Services
# =============================================================================
set -uo pipefail

PASS=0
FAIL=0
WARN=0

check_service() {
    local NAME=$1
    local CONTAINER=$2
    
    STATUS=$(docker inspect --format='{{.State.Health.Status}}' "${CONTAINER}" 2>/dev/null || echo "not_found")
    
    case "$STATUS" in
        healthy)
            echo "  ✔ ${NAME}: HEALTHY"
            PASS=$((PASS + 1))
            ;;
        unhealthy)
            echo "  ✖ ${NAME}: UNHEALTHY"
            FAIL=$((FAIL + 1))
            ;;
        starting)
            echo "  ⏳ ${NAME}: STARTING"
            WARN=$((WARN + 1))
            ;;
        *)
            echo "  ⚠ ${NAME}: ${STATUS}"
            WARN=$((WARN + 1))
            ;;
    esac
}

echo "============================================"
echo " DINAMO ETL — Service Health Check"
echo "============================================"
echo ""

echo "▸ Core Infrastructure:"
check_service "ZooKeeper"       "dinamo-zookeeper"
check_service "Kafka Broker 1"  "dinamo-kafka-1"
check_service "Kafka Broker 2"  "dinamo-kafka-2"
check_service "Kafka Broker 3"  "dinamo-kafka-3"
check_service "Schema Registry" "dinamo-schema-registry"

echo ""
echo "▸ Processing:"
check_service "Spark Master"    "dinamo-spark-master"

echo ""
echo "▸ Observability:"
check_service "Prometheus"      "dinamo-prometheus"
check_service "Grafana"         "dinamo-grafana"

echo ""
echo "============================================"
echo " Results: ${PASS} healthy | ${FAIL} failed | ${WARN} warnings"
echo "============================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
