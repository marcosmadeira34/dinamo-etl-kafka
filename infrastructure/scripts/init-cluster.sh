#!/bin/bash
# =============================================================================
# Dinamo ETL Platform — Full Cluster Initialization
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " DINAMO ETL — Cluster Initialization"
echo "============================================"
echo ""

# --- 1. Validate .env ---
if [ ! -f "${INFRA_DIR}/.env" ]; then
    echo "⚠  .env file not found. Copying from template..."
    cp "${INFRA_DIR}/.env.template" "${INFRA_DIR}/.env"
    echo "   Please edit ${INFRA_DIR}/.env with your credentials."
    echo "   Then re-run this script."
    exit 1
fi

# --- 2. Start infrastructure ---
echo "▸ Starting Docker Compose stack..."
cd "${INFRA_DIR}"
docker compose up -d --build

# --- 3. Wait for Kafka brokers ---
echo ""
echo "▸ Waiting for Kafka brokers to be healthy..."
for broker in dinamo-kafka-1 dinamo-kafka-2 dinamo-kafka-3; do
    echo -n "  Waiting for ${broker}..."
    until docker inspect --format='{{.State.Health.Status}}' "${broker}" 2>/dev/null | grep -q "healthy"; do
        echo -n "."
        sleep 5
    done
    echo " ✔"
done

# --- 4. Wait for Schema Registry ---
echo -n "▸ Waiting for Schema Registry..."
until docker inspect --format='{{.State.Health.Status}}' dinamo-schema-registry 2>/dev/null | grep -q "healthy"; do
    echo -n "."
    sleep 3
done
echo " ✔"

# --- 5. Create Kafka topics ---
echo ""
echo "▸ Creating Kafka topics..."
docker exec dinamo-kafka-1 bash /opt/kafka/create-topics.sh 2>/dev/null || \
    docker exec dinamo-kafka-1 bash -c "$(cat ${INFRA_DIR}/kafka/create-topics.sh)"

# --- 6. Register Avro schemas ---
echo ""
echo "▸ Registering Avro schemas with Schema Registry..."
SCHEMA_DIR="${INFRA_DIR}/schema-registry/schemas"
SR_URL="http://localhost:8081"

if [ -d "${SCHEMA_DIR}" ]; then
    for schema_file in "${SCHEMA_DIR}"/*.avsc; do
        [ -f "$schema_file" ] || continue
        subject=$(basename "$schema_file" .avsc)
        echo "  Registering schema: ${subject}"
        schema_content=$(cat "$schema_file" | jq -c '.')
        curl -s -X POST "${SR_URL}/subjects/${subject}-value/versions" \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            -d "{\"schema\": $(echo "$schema_content" | jq -Rs .)}" \
            > /dev/null 2>&1 && echo "    ✔ ${subject}" || echo "    ⚠ ${subject} (may already exist)"
    done
fi

# --- 7. Verify ---
echo ""
echo "▸ Running health checks..."
bash "${SCRIPT_DIR}/healthcheck-all.sh"

echo ""
echo "============================================"
echo " ✔ Cluster initialization complete!"
echo "============================================"
echo ""
echo " Services:"
echo "   Kafka:           localhost:29092,29093,29094"
echo "   Schema Registry: http://localhost:8081"
echo "   Spark Master UI: http://localhost:8080"
echo "   Prometheus:       http://localhost:9090"
echo "   Grafana:          http://localhost:3000"
echo ""
