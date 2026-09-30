# # dinamo_web/src/dinamo_web/streaming/oci_queue_reader.py
# """
# OCI Queue "source" para os jobs de Structured Streaming — substitui o
# BaseStream.read_from_kafka().

# O Spark Structured Streaming nao tem um source nativo pra OCI Queue, entao
# aqui a leitura vira um polling manual chamado pelo driver a cada
# "micro-batch": GetMessages em loop ate juntar ate max_messages_per_batch
# mensagens (ou a fila esvaziar / o long-polling estourar), convertidas num
# DataFrame com o MESMO shape que o read_from_kafka produzia —
# kafka_key/kafka_value/topic/partition/offset/kafka_timestamp — pra nao
# quebrar nenhum codigo downstream (bronze/silver/gold/subvencao e o
# _handle_batch_error da DLQ, que ja leem essas colunas).

# Ack (delete_message) so acontece explicitamente, depois que o batch foi
# processado — sucesso OU roteado pra DLQ, igual ao consumer.commit() do
# Kafka. Se o processo morrer no meio, as mensagens voltam a ficar visiveis
# apos o visibility timeout e sao reentregues (retry automatico), e depois de
# N tentativas o proprio OCIQ manda pra dead letter queue da fila.
# """
# import datetime
# import json
# import logging
# import os
# import tempfile
# from dataclasses import dataclass
# from typing import Any, Optional

# from pyspark.sql import DataFrame, SparkSession
# from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

# logger = logging.getLogger("dinamo.streaming")

# _DATAFRAME_SCHEMA = StructType([
#     StructField("kafka_key", StringType(), True),
#     StructField("kafka_value", StringType(), True),
#     StructField("topic", StringType(), True),        # canal (channel_id) lido
#     StructField("partition", IntegerType(), True),    # sem equivalente na OCI Queue - sempre 0
#     StructField("offset", StringType(), True),        # message id da OCI Queue (nao numerico)
#     StructField("kafka_timestamp", TimestampType(), True),
# ])


# @dataclass
# class PolledMessage:
#     """Envelope minimo de uma mensagem consumida - so o que o ack precisa."""
#     id: str
#     receipt: str
#     key: Optional[str]
#     value: Optional[str]


# def _resolve_key_file(key_file: Optional[str]) -> Optional[str]:
#     if key_file and not os.path.isfile(key_file):
#         tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
#         tmp.write(key_file.replace("\\n", "\n"))
#         tmp.flush()
#         return tmp.name
#     return key_file


# # Client cacheado por endpoint - o polling roda sequencial no driver (thread
# # unica por stream), entao um client por endpoint e suficiente (diferente do
# # writer, que precisa de um client por partition/executor).
# _client_cache: dict[str, Any] = {}


# def _get_client(service_endpoint: str):
#     if service_endpoint in _client_cache:
#         return _client_cache[service_endpoint]

#     import oci

#     tenancy_id = os.getenv("OCI_TENANCY_ID")
#     user_id = os.getenv("OCI_USER_ID")

#     if tenancy_id and user_id:
#         config = {
#             "tenancy": tenancy_id,
#             "user": user_id,
#             "fingerprint": os.getenv("OCI_FINGERPRINT"),
#             "key_file": _resolve_key_file(os.getenv("OCI_KEY_FILE")),
#             "region": os.getenv("OCI_REGION"),
#         }
#         client = oci.queue.QueueClient(config=config, service_endpoint=service_endpoint)
#     else:
#         signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
#         client = oci.queue.QueueClient(config={}, signer=signer, service_endpoint=service_endpoint)

#     _client_cache[service_endpoint] = client
#     return client


# def poll_messages(
#     channel: str,
#     queue_id: str,
#     service_endpoint: str,
#     max_messages_per_batch: int = 500,
#     polling_timeout_in_seconds: int = 20,
# ) -> list[PolledMessage]:
#     """
#     Faz GetMessages em loop ate juntar ate `max_messages_per_batch`
#     mensagens (equivalente ao maxOffsetsPerTrigger do Kafka) ou ate uma
#     chamada voltar vazia (fila drenada / long-poll estourou sem novidade).
#     """
#     client = _get_client(service_endpoint)
#     collected: list[PolledMessage] = []

#     while len(collected) < max_messages_per_batch:
#         response = client.get_messages(
#             queue_id=queue_id,
#             channel_filter=channel,
#             timeout_in_seconds=polling_timeout_in_seconds,
#             limit=min(20, max_messages_per_batch - len(collected)),  # 20 = limite da OCI Queue
#         )
#         raw_messages = response.data.messages if response.data else []
#         if not raw_messages:
#             break

#         for msg in raw_messages:
#             try:
#                 envelope = json.loads(msg.content)
#                 key = envelope.get("key")
#                 value = envelope.get("value")
#             except (json.JSONDecodeError, AttributeError, TypeError):
#                 # Mensagem nao veio do oci_queue_writer.py (sem envelope
#                 # key/value) - trata o content inteiro como o value.
#                 key = None
#                 value = msg.content
#             collected.append(PolledMessage(id=msg.id, receipt=msg.receipt, key=key, value=value))

#     return collected


# def ack_messages(messages: list[PolledMessage], queue_id: str, service_endpoint: str) -> None:
#     """Confirma (deleta) as mensagens depois que o batch foi processado —
#     com sucesso OU roteado pra DLQ. Mesma semantica do consumer.commit()."""
#     if not messages:
#         return

#     client = _get_client(service_endpoint)
#     for msg in messages:
#         try:
#             client.delete_message(queue_id=queue_id, message_receipt=msg.receipt)
#         except Exception:
#             logger.exception(f"Failed to delete (ack) OCI Queue message {msg.id}")


# def messages_to_dataframe(
#     spark: SparkSession, messages: list[PolledMessage], channel: str
# ) -> DataFrame:
#     """Converte as mensagens numa DataFrame com o MESMO shape que o antigo
#     read_from_kafka produzia, pra nao quebrar nenhum codigo downstream."""
#     now = datetime.datetime.utcnow()
#     rows = [(msg.key, msg.value, channel, 0, msg.id, now) for msg in messages]
#     return spark.createDataFrame(rows, schema=_DATAFRAME_SCHEMA)
