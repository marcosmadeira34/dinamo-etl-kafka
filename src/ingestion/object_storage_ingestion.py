# dinamo_web/src/dinamo_web/ingestion/object_storage_ingestion.py
"""
S3/OCI Bucket File Watcher — polls for new SPED files and publishes
file arrival events to Kafka.

Uses existing BucketConnector.list_paths() for file discovery.
Tracks processed files to ensure idempotent processing.
"""
import os
import time
import json
import logging
import hashlib
import re
from typing import Set, Optional
# from schemas.identify_type_sped import SPEDIdentifier

# from src.oracle_cloud.services.oci_queue_producer import OCIQueueProducer
from kafka.producer import DinamoProducer
from kafka.config import KafkaConfig
from kafka.topics import TopicRegistry

logger = logging.getLogger("dinamo.ingestion.object_storage_ingestion")



class ObjectStorageEventIngestion:
    def __init__(self,
        bucket_name: str,
        kafka_config: KafkaConfig,
        file_extensions: tuple = (".txt",),
        watch_prefix: str = "TO_CONVERT/",
        spark=None,
        ):
        # spark e opcional de proposito: este componente roda como Deployment
        # Kubernetes comum (sem Spark), entao bucket_connector so e criado se
        # alguem realmente passar uma SparkSession (uso futuro/testes locais).
        # process_event() abaixo NUNCA usa bucket_connector, so producer +
        # sped_identifier (ambos puro Python).
        if spark is not None:
            from connectors.bucket_connector import BucketConnector
            self.bucket_connector = BucketConnector(spark, bucket_name)
        else:
            self.bucket_connector = None
        # self.producer = OCIQueueProducer(kafka_config)
        self.producer = DinamoProducer(kafka_config)
        # self.sped_identifier = SPEDIdentifier()
        self.file_extensions = file_extensions
        self.bucket_name = bucket_name
        # Diretorio/prefixo monitorado dentro do bucket (equivalente ao
        # watch_prefix que o SpedFileWatcher antigo usava). A Regra de Evento
        # no OCI ja deveria filtrar por isso (ver infrastructure/oci/event-rule),
        # mas filtramos de novo aqui como defesa em profundidade: se a regra
        # for alterada/alargada no console OCI sem o time saber, eventos de
        # fora do diretorio certo nao viram mensagens Kafka por engano.
        self.watch_prefix = watch_prefix

    def extract_metadata(self, object_name: str):
        metadata = re.search(r"TO_CONVERT/(\d+)/[^/]+/(\d{4})/", object_name)
        if not metadata:
            return None, None
        
        return metadata.group(1), metadata.group(2)

    # def identify_sped_type(self, object_name: str) -> str:
    #     try:
    #         return self.sped_identifier.identify_type_sped_fast(object_name)
    #     except Exception as e:
    #         logger.warning(f"Could not identify SPED type for {object_name}: {e}")
    #         return "unknown"


    def process_event(self, event):
        object_name = event['data']['resourceName']

        # Filtro de diretorio: so processa objetos dentro do prefixo monitorado.
        # Isso e o que substitui o polling do antigo SpedFileWatcher - aqui
        # nao ha state file nem deduplicacao manual, porque cada objeto so
        # dispara UM evento "createobject" no OCI, entao nao ha reprocessamento
        # espontaneo para deduplicar.
        if not object_name.startswith(self.watch_prefix):
            logger.debug(f"Ignorando evento fora do prefixo monitorado: {object_name}")
            return

        if not object_name.endswith(self.file_extensions):
            return

        cnpj, periodo = self.extract_metadata(object_name)
        # sped_type = self.identify_sped_type(object_name)

        payload = {
            'file_path'         : object_name,
            'file_name'         : object_name.rsplit('/', 1)[-1],
            'bucket_name'       : self.bucket_name,
            'cnpj'              : cnpj,
            'periodo'           : periodo,
            'event_timestamp_ms': int(time.time() * 1000),
            'source'            : "oci_object_storage_event",
            # 'sped_type'         : sped_type,
        }

        self.producer.send(
            topic   = TopicRegistry.RAW_INGEST.name,
            key     = cnpj,
            value   = payload,
        )        


        logger.info(
            f"Published event | "
            f"CNPJ={cnpj} | "
            f"PERIODO={periodo}"
        )