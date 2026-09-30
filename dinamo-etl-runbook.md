# Runbook — dinamo-etl (teste local em `kind`)

> Rodar sempre a partir da raiz do repo (`~/dinamo-etl-official` ou `~/dinamo-etl-kafka`,
> conforme a branch que estiver testando).

## 0. Carregar variáveis de ambiente

Sempre no início de qualquer sessão de terminal nova:

```bash
set -a && source infrastructure/.env && set +a
echo "endpoint=$AWS_ENDPOINT_URL bucket=$BUCKET_NAME"
```

---

## 1. Reset completo do ambiente

Use quando quiser recomeçar do zero (apaga checkpoints, tópicos Kafka e dados da Gold).

```bash
set -a && source infrastructure/.env && set +a

# limpa os checkpoints de streaming
aws s3 rm s3://$BUCKET_NAME/spark-checkpoints/ \
  --endpoint-url $AWS_ENDPOINT_URL --recursive

# (opcional) limpa a saida da Gold de um cliente especifico
aws s3 rm s3://$BUCKET_NAME/data-lake/gold/<CNPJ8>/SUBVENCAO/ --recursive

# recria os topicos Kafka do zero (só necessário na branch feature/kafka-legacy)
for topic in sped.raw.ingest sped.parsed.bronze sped.enriched.silver sped.analytics.gold sped.errors.dlq sped.retry sped.schema.changes sped.pipeline.heartbeat sped.analytics.prediagnostico sped.analytics.subvencao; do
  kubectl exec -n dinamo-kafka kafka-0 -- \
    kafka-topics --bootstrap-server localhost:9092 --delete --topic $topic 2>/dev/null
  sleep 2
  kubectl exec -n dinamo-kafka kafka-0 -- \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --topic $topic --partitions 24 --replication-factor 1 --if-not-exists
done
```

### Limpar checkpoint de UM stream específico (sem resetar tudo)

```bash
aws s3 rm s3://$BUCKET_NAME/spark-checkpoints/silver-stream/ \
  --endpoint-url $AWS_ENDPOINT_URL --recursive
```

---

## 2. Build da imagem e deploy dos SparkApplications

```bash
docker build -t dinamo-spark:dev .
kind load docker-image dinamo-spark:dev --name dinamo-dev

kubectl delete sparkapplication dinamo-bronze dinamo-silver dinamo-gold dinamo-subvencao-local -n spark-dinamo

# IMPORTANTE: o sparkConf tem "${AWS_ACCESS_KEY_ID}"/"${AWS_SECRET_ACCESS_KEY}" -
# sempre aplicar via envsubst, nunca "kubectl apply -f" direto (senão o Spark
# recebe a string literal "${AWS_ACCESS_KEY_ID}" e quebra com SignatureDoesNotMatch).
envsubst < dinamo-launcher/manifests/dinamo-bronze.yaml         | kubectl apply -f -
envsubst < dinamo-launcher/manifests/dinamo-silver.yaml         | kubectl apply -f -
envsubst < dinamo-launcher/manifests/dinamo-gold.yaml           | kubectl apply -f -
envsubst < dinamo-launcher/manifests/dinamo-subvencao-local.yaml | kubectl apply -f -

sleep 20
kubectl get sparkapplications -n spark-dinamo
kubectl get pods -n spark-dinamo
```

### Redeploy rápido de UMA camada só (quando só mudou código dela)

Exemplo pra Silver — mesma lógica pra Bronze/Gold/Subvencao:

```bash
docker build -t dinamo-spark:dev .
kind load docker-image dinamo-spark:dev --name dinamo-dev
kubectl delete sparkapplication dinamo-silver -n spark-dinamo
envsubst < dinamo-launcher/manifests/dinamo-silver.yaml | kubectl apply -f -
```

---

## 3. Disparar o processamento

### 3a. Via ingestion `/scan` (varre um prefixo no bucket)

```bash
kubectl port-forward -n spark-dinamo svc/dinamo-ingestion 8080:80 &
sleep 10

curl -X POST http://localhost:8080/scan \
  -H "Content-Type: application/json" \
  -d '{"prefix": "TO_CONVERT/38122724/ECD/"}'
```

Outros prefixos úteis:
```bash
curl -X POST http://localhost:8080/scan -H "Content-Type: application/json" \
  -d '{"prefix": "Receitanetbx/"}'                     # tudo

curl -X POST http://localhost:8080/scan -H "Content-Type: application/json" \
  -d '{"prefix": "Receitanetbx/01990619/"}'            # só um cliente

curl -X POST http://localhost:8080/scan -H "Content-Type: application/json" \
  -d '{"prefix": "Receitanetbx/01990619/ECF/"}'        # só ECF de um cliente
```

### 3b. Via `/events` (simula um evento único de criação de objeto)

```bash
kubectl port-forward -n spark-dinamo svc/dinamo-ingestion 8080:80 &
sleep 2

curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "com.oraclecloud.objectstorage.createobject",
    "data": {
      "resourceName": "TO_CONVERT/01990619/EFD Fiscal/082025/01990619000153-456059538113-20250801-20250831-0-560FD534566A7B5F8B748F67E0E6D159F42A2F6A-SPED-EFD.txt"
    }
  }'
```

### 3c. Ação sob demanda direto no Kafka (bypassa a API — só branch `feature/kafka-legacy`)

```bash
kubectl exec -n dinamo-kafka kafka-0 -- \
  kafka-console-producer --bootstrap-server localhost:9092 \
  --topic dinamo.ondemand.actions << 'EOF'
{"arguments": ["--action", "REPROCESS_ECF", "--file_path", "TO_CONVERT/01990619/ECF/012024/SPEDECF-12537972000107-20240101-20241231-20250722084002.txt", "--company_size", "light"]}
EOF
```

### 3d. Mover um arquivo já ingerido de volta pra fila de conversão

```bash
aws s3 cp \
  "s3://$BUCKET_NAME/processed/ingested/01990619000153-456059538113-20250801-20250831-0-560FD534566A7B5F8B748F67E0E6D159F42A2F6A-SPED-EFD.txt" \
  "s3://$BUCKET_NAME/TO_CONVERT/01990619/EFD Fiscal/082025/01990619000153-456059538113-20250801-20250831-0-560FD534566A7B5F8B748F67E0E6D159F42A2F6A-SPED-EFD.txt" \
  --endpoint-url $AWS_ENDPOINT_URL
```

---

## 4. Verificação e logs

```bash
sleep 20
kubectl get sparkapplications -n spark-dinamo
kubectl get pods -n spark-dinamo

kubectl logs -n spark-dinamo dinamo-ingestion-driver -f | grep -v "^26/"
kubectl logs -n spark-dinamo dinamo-bronze-driver -f   | grep -v "^26/"
kubectl logs -n spark-dinamo dinamo-silver-driver -f   | grep -v "^26/"
kubectl logs -n spark-dinamo dinamo-gold-driver -f     | grep -v "^26/"
```

---

## 5. Teste isolado de Subvenção

```bash
docker build -t dinamo-spark:dev .
kind load docker-image dinamo-spark:dev --name dinamo-dev
kubectl delete sparkapplication dinamo-bronze dinamo-silver dinamo-gold dinamo-subvencao-local -n spark-dinamo
envsubst < dinamo-launcher/manifests/dinamo-subvencao-local.yaml | kubectl apply -f -

sleep 5
kubectl get sparkapplications -n spark-dinamo dinamo-subvencao-local
kubectl logs -n spark-dinamo -l app=dinamo-subvencao-local -f
```

**Confirma que publicou o evento de conclusão** (só branch `feature/kafka-legacy` —
na branch OCI Queue isso vira `read_from_queue`/poll, não dá pra checar via `kafka-console-consumer`):
```bash
kubectl exec -n dinamo-kafka kafka-0 -- \
  kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sped.analytics.subvencao --from-beginning --timeout-ms 10000
```

**Confirma que gravou na Gold no path certo** (`data-lake/gold`, minúsculo):
```bash
aws s3 ls s3://$BUCKET_NAME/data-lake/gold/01063615/SUBVENCAO/ \
  --endpoint-url $AWS_ENDPOINT_URL --recursive
```

---

## 6. Pod avulso pra rodar o conversor de XLSX

```bash
set -a && source infrastructure/.env && set +a

kubectl run converter --restart=Never -n spark-dinamo --image=dinamo-spark:dev --image-pull-policy=Never \
  --env="AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL" \
  --env="AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  --env="AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  --env="BUCKET_NAME=$BUCKET_NAME" \
  -- sleep 3600

sleep 3
kubectl get pods -n spark-dinamo | grep converter

kubectl cp converter_subvencao_para_xlsx.py spark-dinamo/converter:/tmp/converter_subvencao.py
kubectl exec -n spark-dinamo converter -- python3 /tmp/converter_subvencao.py 30698208 202312 202302 202211 202210 202208 202110

# traz o(s) xlsx gerado(s) de volta pro seu PC
kubectl cp spark-dinamo/converter:/tmp/30698208_202110_isencao.xlsx ./30698208_202110_isencao.xlsx
kubectl cp spark-dinamo/converter:/tmp/30698208_202110_reducao.xlsx ./30698208_202110_reducao.xlsx

# quando terminar de validar, apaga o pod avulso
kubectl delete pod converter -n spark-dinamo
```


# converter todos os deltas em xlsx

set -a && source infrastructure/.env && set +a

kubectl run converter --restart=Never -n spark-dinamo --image=dinamo-spark:dev --image-pull-policy=Never \
  --env="AWS_ENDPOINT_URL=$AWS_ENDPOINT_URL" \
  --env="AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
  --env="AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
  --env="BUCKET_NAME=$BUCKET_NAME" \
  -- sleep 3600

sleep 3
kubectl get pods -n spark-dinamo | grep converter

kubectl cp converter_delta_para_xlsx.py spark-dinamo/converter:/tmp/converter_delta_para_xlsx.py
kubectl exec -n spark-dinamo converter -- python3 /tmp/converter_delta_para_xlsx.py

kubectl delete pod converter -n spark-dinamo


# Ordem correta (scan ANTES do deploy das camadas):

set -a && source infrastructure/.env && set +a

aws s3 rm s3://$BUCKET_NAME/spark-checkpoints/ --endpoint-url $AWS_ENDPOINT_URL --recursive

for topic in sped.raw.ingest sped.parsed.bronze sped.enriched.silver sped.analytics.gold sped.analytics.subvencao; do
  kubectl exec -n dinamo-kafka kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --delete --topic $topic 2>/dev/null
  sleep 2
  kubectl exec -n dinamo-kafka kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --create --topic $topic --partitions 24 --replication-factor 1 --if-not-exists
done

docker build -t dinamo-spark:dev .
kind load docker-image dinamo-spark:dev --name dinamo-dev
kubectl delete sparkapplication dinamo-bronze dinamo-silver dinamo-gold dinamo-subvencao-local -n spark-dinamo

# 1) PRIMEIRO sobe o port-forward e dispara o /scan
kubectl port-forward -n spark-dinamo svc/dinamo-ingestion 8080:80 &
sleep 10
curl -X POST http://localhost:8080/scan -H "Content-Type: application/json" \
  -d '{"prefix": "TO_CONVERT/52848868/EFD Fiscal/"}'

# 2) espera o scan terminar de publicar tudo no Kafka antes de seguir
sleep 5

# 3) SÓ AGORA sobe bronze/silver/gold/subvencao
envsubst < dinamo-launcher/manifests/dinamo-bronze.yaml         | kubectl apply -f -
envsubst < dinamo-launcher/manifests/dinamo-silver.yaml         | kubectl apply -f -
envsubst < dinamo-launcher/manifests/dinamo-gold.yaml           | kubectl apply -f -
envsubst < dinamo-launcher/manifests/dinamo-subvencao-local.yaml | kubectl apply -f -

sleep 20
kubectl get sparkapplications -n spark-dinamo
kubectl get pods -n spark-dinamo


# rebuildar uma camada específica (exemplo SILVER)

# 1) limpa SO o checkpoint do silver (o bronze fica intocado)
aws s3 rm s3://$BUCKET_NAME/spark-checkpoints/silver-stream/ --endpoint-url $AWS_ENDPOINT_URL --recursive

# 2) rebuild + redeploy SO do silver
docker build -t dinamo-spark:dev .
kind load docker-image dinamo-spark:dev --name dinamo-dev
kubectl delete sparkapplication dinamo-silver -n spark-dinamo
envsubst < dinamo-launcher/manifests/dinamo-silver.yaml | kubectl apply -f -

sleep 15
kubectl logs -n spark-dinamo dinamo-silver-driver -f | grep -v "^26/"

=========================

# retornar um processo que parou , exemplo Silver

<!-- sempre setar as variáveis de ambiente com o source -->

set -a && source infrastructure/.env && set +a
echo "confere que carregou: AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"

kubectl delete sparkapplication dinamo-silver -n spark-dinamo
envsubst < dinamo-launcher/manifests/dinamo-silver.yaml | kubectl apply -f -

sleep 15
kubectl logs -n spark-dinamo dinamo-silver-driver -f | grep -v "^26/"


========================

# rodar os scripts de conversao:

# 1) Criar o pod temporariamente:

kubectl run converter \
  -n spark-dinamo \
  --image=docker.io/library/dinamo-spark:dev \
  --restart=Never \
  -- sleep infinity

# 2) Esperar o pod ficar em running
kubectl get pod converter -n spark-dinamo

# 3) Copiar o script para o pod 

kubectl cp converter_delta_para_xlsx.py spark-dinamo/converter:/tmp/converter_delta_para_xlsx.py

# 4) Executar os comandos para iniciar conversao

# para um sped especifico

kubectl exec -it -n spark-dinamo converter -- python3 /tmp/converter_delta_para_xlsx.py 12537972/EFD_CONTRIB/

# para um cnpj em especifico

kubectl exec -it -n spark-dinamo converter -- python3 /tmp/converter_delta_para_xlsx.py 12345678901234

# para um cnpj e meses especificos

kubectl exec -it -n spark-dinamo converter -- python3 /tmp/converter_delta_para_xlsx.py 52848868 202508 202507 202506 202505 202504 202503 202502 202501 202412




========================================

# Confirma que realmente gravou (Delta table da Silver):

set -a && source infrastructure/.env && set +a
aws s3 ls s3://$BUCKET_NAME/data-lake/silver/ --endpoint-url $AWS_ENDPOINT_URL --recursive | tail -20

# Confirma com os logs (procura a linha de "Processing batch"):

kubectl logs -n spark-dinamo dinamo-silver-driver --previous=false | grep -i "batch\|records\|finished"



<!-- Pra ver um job rodando agora -->

O Spark Operator (spark-operator.k8s.io) cria automaticamente um Service expondo a porta 4040 do driver, no padrão <nome-do-job>-ui-svc. Você acha o pod/serviço assim:

bash
kubectl get sparkapplications -n spark-dinamo
kubectl get pods -n spark-dinamo -l spark-role=driver
kubectl get svc -n spark-dinamo | grep ui-svc

E acessa via port-forward direto no serviço (mais estável que apontar pro pod, já que o operator já resolve isso):

bash
kubectl port-forward -n spark-dinamo svc/<nome-do-job>-ui-svc 4040:4040

no exemplo:

kubectl port-forward -n spark-dinamo svc/dinamo-silver-ui-svc 4040:4040

agora só abrir em localhost:4040