#!/bin/bash
# Setup do cluster kind para o projeto Dinamo
# Executar da raiz do projeto: bash setup-dinamo-dev.sh
# NAO altera nenhum manifest do projeto
set -e

echo "=== Carregando .env ==="
sed -i 's/\r//' infrastructure/.env
set -a && source infrastructure/.env && set +a
echo "BUCKET: $BUCKET_NAME | ENDPOINT: $AWS_ENDPOINT_URL"

echo "=== 1. Cluster kind ==="
kind delete cluster --name dinamo-dev 2>/dev/null || true
kind create cluster --name dinamo-dev

echo "=== 2. Imagens no kind ==="
kind load docker-image dinamo-spark:dev --name dinamo-dev
kind load docker-image dinamo-ingestion:dev --name dinamo-dev
docker pull --platform linux/amd64 confluentinc/cp-kafka:7.6.1
docker pull --platform linux/amd64 confluentinc/cp-zookeeper:7.6.1
kind load docker-image confluentinc/cp-kafka:7.6.1 --name dinamo-dev
kind load docker-image confluentinc/cp-zookeeper:7.6.1 --name dinamo-dev

echo "=== 3. Spark Operator ==="
helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set webhook.enable=true
sleep 20
kubectl patch deployment spark-operator-controller -n spark-operator --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/4","value":"--namespaces=default,spark-dinamo"}]'
kubectl create clusterrolebinding spark-operator-controller-global \
  --clusterrole=cluster-admin \
  --serviceaccount=spark-operator:spark-operator-controller 2>/dev/null || true

echo "=== 4. Namespace e credenciais ==="
kubectl create namespace spark-dinamo
kubectl create namespace dinamo-kafka 2>/dev/null || true
kubectl create serviceaccount spark-operator-spark -n spark-dinamo
kubectl create clusterrolebinding spark-dinamo-role \
  --clusterrole=edit \
  --serviceaccount=spark-dinamo:spark-operator-spark 2>/dev/null || true
kubectl create configmap dinamo-streaming-config -n spark-dinamo \
  --from-literal=AWS_ENDPOINT_URL="$AWS_ENDPOINT_URL" \
  --from-literal=BUCKET_NAME="$BUCKET_NAME" \
  --from-literal=BRONZE_PATH="s3a://$BUCKET_NAME/data-lake/bronze" \
  --from-literal=SILVER_PATH="s3a://$BUCKET_NAME/data-lake/silver" \
  --from-literal=GOLD_PATH="s3a://$BUCKET_NAME/data-lake/gold" \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS="kafka.dinamo-kafka.svc.cluster.local:9092" \
  --from-literal=WATCH_PREFIX="TO_CONVERT/" \
  --from-literal=LOG_LEVEL="INFO"
kubectl create secret generic dinamo-streaming-secrets -n spark-dinamo \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"

echo "=== 5. Kafka ==="
kubectl apply -f dinamo-streaming-deploy/manifests/kafka/kafka-kind.yaml
echo "Aguardando Zookeeper..."
kubectl wait pod zookeeper-0 -n dinamo-kafka --for=condition=Ready --timeout=180s
echo "Aguardando Kafka..."
kubectl wait pod kafka-0 -n dinamo-kafka --for=condition=Ready --timeout=180s

echo "=== 5b. Topicos ==="
for topic in sped.raw.ingest sped.parsed.bronze sped.enriched.silver sped.analytics.gold; do
  kubectl exec -n dinamo-kafka kafka-0 -- \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --topic $topic --partitions 24 --replication-factor 1 --if-not-exists
done

echo "=== 6. Deploy ==="
kubectl apply -f dinamo-streaming-deploy/manifests/dinamo-ingestion.yaml
kubectl apply -f dinamo-streaming-deploy/manifests/dinamo-bronze.yaml
kubectl apply -f dinamo-streaming-deploy/manifests/dinamo-silver.yaml
kubectl apply -f dinamo-streaming-deploy/manifests/dinamo-gold.yaml

echo "=== STATUS FINAL ==="
sleep 30
kubectl get sparkapplications -n spark-dinamo
kubectl get pods -n spark-dinamo
kubectl get pods -n dinamo-kafka
echo ""
echo "Teste ponta a ponta:"
echo "kubectl port-forward -n spark-dinamo svc/dinamo-ingestion 8080:80 &"
echo "curl -X POST http://localhost:8080/events -H 'Content-Type: application/json' -d '{\"eventType\":\"com.oraclecloud.objectstorage.createobject\",\"data\":{\"resourceName\":\"TO_CONVERT/01990619/ECF/2024/SPEDECF-01990619000153-20240101-20241231-20250714105030.txt\"}}'"
