# # dinamo_web/src/dinamo_web/streaming/oci_queue_writer.py
# """
# OCI Queue sink para os jobs de Structured Streaming (Bronze/Silver/Gold/
# Subvencao) — substitui o BaseStream.write_to_kafka().

# Diferenca de modelo em relacao ao Kafka: nao ha um DataFrameWriter nativo do
# Spark pra OCI Queue, entao a escrita e feita via foreachPartition — cada
# partition (executor) abre seu proprio OCI QueueClient e publica em lotes de
# ate 20 mensagens por chamada (limite do PutMessages da OCI Queue).

# Mesmo padrao de auth (config-based com fallback pra instance principal) do
# lion-api e do dinamo-launcher, para os 3 lados falarem a mesma "lingua" de
# credenciais OCI.
# """
# import logging
# import os
# import tempfile
# from typing import Iterator

# from pyspark.sql import DataFrame, Row

# logger = logging.getLogger("dinamo.streaming")

# # Limite de mensagens por chamada de PutMessages da OCI Queue.
# _MAX_MESSAGES_PER_PUT = 20


# def _resolve_key_file(key_file: str | None) -> str | None:
#     """Aceita tanto um path (~/.oci/key.pem) quanto o conteudo PEM direto
#     (quando a chave vem via Secret/env var no deploy), igual ao lion-api e
#     ao dinamo-launcher.
#     """
#     if key_file and not os.path.isfile(key_file):
#         tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
#         tmp.write(key_file.replace("\\n", "\n"))
#         tmp.flush()
#         return tmp.name
#     return key_file


# def _build_oci_client(endpoint: str):
#     """Cria um QueueClient novo. Chamado uma vez por partition/executor —
#     nao pode ser compartilhado entre tasks (nao e serializavel pelo Spark).
#     """
#     import oci

#     tenancy_id = os.getenv("OCI_TENANCY_ID")
#     user_id = os.getenv("OCI_USER_ID")
#     fingerprint = os.getenv("OCI_FINGERPRINT")
#     key_file = os.getenv("OCI_KEY_FILE")
#     region = os.getenv("OCI_REGION")

#     if tenancy_id and user_id:
#         config = {
#             "tenancy": tenancy_id,
#             "user": user_id,
#             "fingerprint": fingerprint,
#             "key_file": _resolve_key_file(key_file),
#             "region": region,
#         }
#         return oci.queue.QueueClient(config=config, service_endpoint=endpoint)

#     # Instance principal (OKE) - sem credenciais explicitas no deploy
#     signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
#     return oci.queue.QueueClient(config={}, signer=signer, service_endpoint=endpoint)


# def _write_partition_to_queue(
#     rows: Iterator[Row], queue_id: str, endpoint: str, channel: str
# ) -> None:
#     import oci

#     client = _build_oci_client(endpoint)

#     batch = []
#     for row in rows:
#         batch.append(
#             oci.queue.models.PutMessagesDetailsEntry(
#                 content=row["value"],
#                 channel_id=channel,
#             )
#         )
#         if len(batch) == _MAX_MESSAGES_PER_PUT:
#             client.put_messages(
#                 queue_id=queue_id,
#                 put_messages_details=oci.queue.models.PutMessagesDetails(messages=batch),
#             )
#             batch = []

#     if batch:
#         client.put_messages(
#             queue_id=queue_id,
#             put_messages_details=oci.queue.models.PutMessagesDetails(messages=batch),
#         )


# def write_dataframe_to_queue(
#     df: DataFrame,
#     channel: str,
#     queue_id: str | None = None,
#     service_endpoint: str | None = None,
#     max_parallelism: int = 8,
# ) -> None:
#     """
#     Publica um DataFrame (colunas `kafka_key`/`kafka_value`) num canal
#     (channel_id) do OCI Queue. Drop-in replacement de
#     BaseStream.write_to_kafka(df, topic): `channel` ocupa o mesmo papel que
#     o `topic` do Kafka tinha (identifica/roteia as mensagens dentro da
#     mesma fila — normalmente um dos nomes do TopicRegistry).

#     Args:
#         df: DataFrame com as colunas kafka_key/kafka_value (mesmo shape que
#             os streams ja produzem para o write_to_kafka).
#         channel: channel_id da OCI Queue (reaproveita TopicRegistry.*.name).
#         queue_id: OCID da fila. Default: OCI_STREAMING_QUEUE_ID (ou
#             OCI_QUEUE_ID, a mesma fila do BFF/launcher, se a dedicada pro
#             streaming nao estiver configurada).
#         service_endpoint: messages endpoint da fila. Mesmo fallback acima.
#         max_parallelism: numero maximo de partitions/QueueClients abertos
#             em paralelo (equivalente ao _MAX_KAFKA_WRITE_PARALLELISM antigo).
#     """
#     queue_id = queue_id or os.getenv("OCI_STREAMING_QUEUE_ID") or os.getenv("OCI_QUEUE_ID")
#     service_endpoint = (
#         service_endpoint
#         or os.getenv("OCI_STREAMING_QUEUE_SERVICE_ENDPOINT")
#         or os.getenv("OCI_QUEUE_SERVICE_ENDPOINT")
#     )
#     if not queue_id or not service_endpoint:
#         raise ValueError(
#             "OCI_STREAMING_QUEUE_ID/OCI_QUEUE_ID e "
#             "OCI_STREAMING_QUEUE_SERVICE_ENDPOINT/OCI_QUEUE_SERVICE_ENDPOINT "
#             "precisam estar configurados para escrever no OCI Queue."
#         )

#     # Mesma logica de paralelismo dinamico que o write_to_kafka tinha: evita
#     # tanto poucos QueueClients (gargalo) quanto muitos (um por task).
#     num_partitions = min(df.rdd.getNumPartitions(), max_parallelism)
#     num_partitions = max(num_partitions, 1)

#     queue_df = (
#         df
#         .selectExpr(
#             "CAST(kafka_key AS STRING) as key",
#             "CAST(kafka_value AS STRING) as value",
#         )
#         .coalesce(num_partitions)
#     )

#     queue_df.foreachPartition(
#         lambda rows: _write_partition_to_queue(rows, queue_id, service_endpoint, channel)
#     )
