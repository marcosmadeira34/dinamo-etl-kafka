# #!/usr/bin/env bash
# set -e
# KUBECTL=/usr/local/bin/kubectl
# MANIFESTS=/home/marcosmadeira/dinamo-etl-kafka/dinamo-launcher/manifests

# echo "=== Lendo credenciais do Kubernetes Secret ==="
# AWS_ACCESS_KEY_ID=$($KUBECTL get secret dinamo-streaming-secrets -n spark-dinamo -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
# AWS_SECRET_ACCESS_KEY=$($KUBECTL get secret dinamo-streaming-secrets -n spark-dinamo -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
# export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

# echo "AWS_ACCESS_KEY_ID  : ${#AWS_ACCESS_KEY_ID} chars"
# echo "AWS_SECRET_ACCESS_KEY: ${#AWS_SECRET_ACCESS_KEY} chars"

# if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
#   echo "ERRO: Credenciais vazias! Abortando."
#   exit 1
# fi

# echo "=== Deletando SparkApplications antigas ==="
# $KUBECTL delete sparkapplication dinamo-bronze dinamo-silver dinamo-gold dinamo-subvencao-local dinamo-recover-missing-silver-events -n spark-dinamo --ignore-not-found

# echo "=== Aguardando pods terminarem (12s) ==="
# sleep 12

# echo "=== Aplicando manifestos ==="
# envsubst < "$MANIFESTS/dinamo-bronze.yaml"          | $KUBECTL apply -f -
# # envsubst < "$MANIFESTS/dinamo-silver.yaml"          | $KUBECTL apply -f -
# # envsubst < "$MANIFESTS/dinamo-gold.yaml"            | $KUBECTL apply -f -
# # envsubst < "$MANIFESTS/dinamo-subvencao-local.yaml" | $KUBECTL apply -f -
# # envsubst < "$MANIFESTS/dinamo-recover-missing-silver-events.yaml"             | $KUBECTL apply -f -

# echo "=== Aguardando inicializacao (20s) ==="
# sleep 20

# echo "=== STATUS FINAL ==="
# $KUBECTL get sparkapplications -n spark-dinamo
# echo ""
# $KUBECTL get pods -n spark-dinamo


#!/usr/bin/env bash
set -e
KUBECTL=/usr/local/bin/kubectl
MANIFESTS=/home/marcosmadeira/dinamo-etl-kafka/dinamo-launcher/manifests
NAMESPACE=spark-dinamo

# Timeout maximo de espera por camada (segundos). Ajuste se algum lote
# grande legitimamente precisar de mais tempo do que isso.
WAIT_TIMEOUT=1800   # 30 min
POLL_INTERVAL=5    # confere status a cada 10s

echo "=== Lendo credenciais do Kubernetes Secret ==="
AWS_ACCESS_KEY_ID=$($KUBECTL get secret dinamo-streaming-secrets -n $NAMESPACE -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
AWS_SECRET_ACCESS_KEY=$($KUBECTL get secret dinamo-streaming-secrets -n $NAMESPACE -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

echo "AWS_ACCESS_KEY_ID  : ${#AWS_ACCESS_KEY_ID} chars"
echo "AWS_SECRET_ACCESS_KEY: ${#AWS_SECRET_ACCESS_KEY} chars"

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "ERRO: Credenciais vazias! Abortando."
  exit 1
fi

echo "=== Deletando SparkApplications antigas ==="
$KUBECTL delete sparkapplication dinamo-bronze dinamo-silver dinamo-gold dinamo-subvencao-local dinamo-recover-missing-silver-events -n $NAMESPACE --ignore-not-found

echo "=== Aguardando pods terminarem (12s) ==="
sleep 12

# Aplica um manifesto e espera o SparkApplication terminar (COMPLETED) antes
# de devolver o controle — e o que garante o encadeamento Bronze->Silver->
# Gold sem rodar dois ao mesmo tempo. available_now sozinho NAO faz isso:
# se disparados juntos, o de baixo checa o Kafka vazio e desiste na hora,
# antes do de cima publicar (silver "Stream finished" em poucos segundos
# com records=0, ja vimos isso acontecer).
wait_for_completion() {
  local app_name="$1"
  local elapsed=0

  echo "--- Aguardando '$app_name' terminar (timeout ${WAIT_TIMEOUT}s) ---"
  while true; do
    state=$($KUBECTL get sparkapplication "$app_name" -n $NAMESPACE \
              -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "")

    case "$state" in
      COMPLETED)
        echo "--- '$app_name' concluido com sucesso (${elapsed}s) ---"
        return 0
        ;;
      FAILED)
        echo "!!! '$app_name' terminou com FAILED apos ${elapsed}s — abortando o encadeamento."
        echo "!!! Log do driver:"
        $KUBECTL logs -n $NAMESPACE "${app_name}-driver" --tail=50 || true
        exit 1
        ;;
      "")
        echo "    [$elapsed s] SparkApplication '$app_name' ainda nao apareceu..."
        ;;
      *)
        echo "    [$elapsed s] estado atual: $state"
        ;;
    esac

    if [ "$elapsed" -ge "$WAIT_TIMEOUT" ]; then
      echo "!!! Timeout de ${WAIT_TIMEOUT}s esperando '$app_name' terminar — abortando."
      exit 1
    fi

    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))
  done
}

deploy_and_wait() {
  local name="$1"
  local manifest="$2"

  echo ""
  echo "=== Aplicando $name ==="
  envsubst < "$manifest" | $KUBECTL apply -f -
  wait_for_completion "$name"
}

# === Encadeamento sequencial: Bronze -> Silver -> Gold ===
# Comenta a linha correspondente pra pular uma camada especifica (ex: so
# rodar Silver+Gold porque a Bronze ja foi feita antes).
deploy_and_wait dinamo-bronze "$MANIFESTS/dinamo-bronze.yaml"
deploy_and_wait dinamo-silver "$MANIFESTS/dinamo-silver.yaml"
deploy_and_wait dinamo-gold   "$MANIFESTS/dinamo-gold.yaml"

# Jobs avulsos (batch, sob demanda) — NAO entram no encadeamento automatico,
# descomente e rode manualmente quando precisar:
# envsubst < "$MANIFESTS/dinamo-subvencao-local.yaml" | $KUBECTL apply -f -
# envsubst < "$MANIFESTS/dinamo-recover-missing-silver-events.yaml" | $KUBECTL apply -f -

echo ""
echo "=== STATUS FINAL ==="
$KUBECTL get sparkapplications -n $NAMESPACE
echo ""
$KUBECTL get pods -n $NAMESPACE