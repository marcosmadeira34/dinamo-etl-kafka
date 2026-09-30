# infrastructure/oci/event-rule.md

## Camada 1 — filtro na origem (OCI Events Rule)

No console OCI (ou via Terraform), a regra que dispara a Notification para o
endpoint do `dinamo-ingestion` deve filtrar por evento E por prefixo, para
nao gerar trafego/custo de invocacao para arquivos fora do diretorio
monitorado:

- Event Type: `Object Storage - Object Create` (com.oraclecloud.objectstorage.createobject)
- Condition (JMESPath, na Rule): filtra `data.resourceName` comecando com
  o prefixo monitorado, ex.: `TO_CONVERT/`
- Action: Notifications Topic -> Subscription HTTPS apontando para o
  Service `dinamo-ingestion` (via LB/Ingress publico ou OCI Function-ponte,
  dependendo da topologia do OKE)

## Camada 2 — filtro no código (defesa em profundidade)

`ObjectStorageEventIngestion.process_event()` reaplica esse mesmo filtro de
prefixo (`watch_prefix`, default `TO_CONVERT/`, configuravel via env var
`WATCH_PREFIX`) antes de publicar em `sped.raw.ingest`. Isso garante que,
mesmo que a Rule no console OCI seja alargada/alterada por engano, eventos
de fora do diretorio certo nao viram mensagens Kafka.

## Por que não precisa mais de polling

O antigo `SpedFileWatcher` fazia polling do bucket porque ele MESMO
descobria arquivos novos. Com OCI Events, a descoberta é push: cada objeto
criado dentro do prefixo monitorado dispara exatamente UM evento
`createobject`. Isso elimina a necessidade do `STATE_FILE`
(`file_watcher_state.json`) e do PVC que guardava esse estado entre
restarts — `dinamo-ingestion` é stateless, por isso pode ser um Deployment
com HPA em vez de um pod único com volume persistente.