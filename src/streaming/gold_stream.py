# dinamo_web/src/dinamo_web/streaming/gold_stream.py
"""
Gold Streaming Job — reads enriched Silver records from Kafka,
applies Gold-layer joins and business logic, writes to Gold Parquet
and sped.analytics.gold topic.

Integration with existing modules:
- GoldBuilder.normalize_keys()
- GoldBuilder.join_documents()
- GoldBuilder.join_taxes()
- GoldBuilder.enrich_business()
- GoldBuilder.write_gold()
"""
import logging
import json
import yaml
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from streaming.base_stream import BaseStream
from streaming.stream_config import StreamConfig
from kafka.topics import TopicRegistry

from gold_layer.gold_builder import GoldStreamingBuilder

logger = logging.getLogger("dinamo.streaming.gold")
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)


class GoldStream(BaseStream):
    """
    Builds Gold analytical tables from Silver records.
    
    Flow per micro-batch:
    1. Read enriched records from sped.enriched.silver
    2. Group by CNPJ and period
    3. Apply Gold joins (C100←C170←C190) using existing GoldBuilder
    4. Apply business enrichment (existing)
    5. Write to Gold Parquet (partitioned by CNPJ)
    6. Publish analytics events to sped.analytics.gold
    """

    def __init__(self, spark: SparkSession, config: StreamConfig, gold_builder_class):
        super().__init__(spark, config, stream_name="gold-stream")
        self.gold_builder_class = gold_builder_class

    def get_source_topic(self) -> str:
        return TopicRegistry.ENRICHED_SILVER.name

    def process_micro_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process Silver records into Gold analytical tables.
        Uses existing GoldBuilder for joins and enrichment.

        VETORIZACAO: diferente de Bronze/Silver, a Gold nao opera por
        arquivo — run_complete_gold_build() ja le/escreve por combinacao
        (cnpj, sped_type, month_year) inteira. O ganho aqui e DEDUPLICAR
        essa combinacao antes de construir: se mais de um evento Silver do
        mesmo lote cair na MESMA combinacao (ex: retificadora, reprocesso),
        o build era refeito do zero para cada evento, sem necessidade —
        agora roda uma unica vez por combinacao, e todos os _file_id que
        mapeiam pra ela sao marcados como sucesso/falha juntos.
        """
        # Parse FILE-LEVEL events from Kafka
        json_schema = "_file_id STRING, _sped_type STRING, status STRING, cnpj_8_digits STRING, month_year STRING"
        events_df = (
            batch_df
            .select(F.from_json(F.col("kafka_value"), json_schema).alias("data"))
            .select("data.*")
            .distinct()
            .collect()
        )

        if not events_df:
            return

        valid_events = [
            m for m in events_df
            if m["_file_id"] and m["_sped_type"] and m["cnpj_8_digits"] and m["month_year"]
        ]
        if not valid_events:
            return

        logger.info(f"[Gold] Processing batch {batch_id} | eventos={len(valid_events)}")

        # Agrupa por combinacao de SAIDA (cnpj, sped_type, month_year) — a
        # granularidade real que o GoldBuilder usa. Varios _file_id podem
        # cair na mesma combinacao; o build so roda 1 vez por combinacao.
        combos: dict = {}
        for meta in valid_events:
            key = (meta["cnpj_8_digits"], meta["_sped_type"], meta["month_year"])
            combos.setdefault(key, []).append(meta["_file_id"])

        logger.info(f"[Gold] {len(valid_events)} evento(s) reduzidos a {len(combos)} combinacao(oes) unica(s) para build")

        processed_events = []

        config_files = {
            "EFD_CONTRIB": "sped_contrib_config.yaml",
            "EFD_FISCAL": "sped_fiscal_config.yaml",
            "ECD": "sped_ecd_config.yaml",
            "ECF": "sped_ecf_config.yaml",
        }
        sped_type_list = list(config_files.keys())

        for (cnpj_8_digits, sped_type, month_year), file_ids in combos.items():
            logger.info(
                f"[Gold] Construindo combinacao {sped_type}/{cnpj_8_digits}/{month_year} "
                f"({len(file_ids)} arquivo(s) associado(s))"
            )

            try:
                if sped_type not in config_files:
                    raise ValueError(f"Tipo SPED nao suportado para GOLD: {sped_type}")

                yaml_file = config_files[sped_type]
                yaml_path = f"/opt/spark/app/src/config/{yaml_file}"
                with open(yaml_path, "r") as f:
                    sped_config = yaml.safe_load(f)

                reports_list = [
                    k for k, v in sped_config.items()
                    if isinstance(v, dict) and "entities" in v
                ]
                if not reports_list:
                    raise ValueError(f"Nenhum relatorio encontrado no config {yaml_file}")

                silver_base_path = self.config.silver_path

                gold_builder = GoldStreamingBuilder(
                    spark=self.spark,
                    base_path=silver_base_path,
                    period=month_year,
                )
                gold_builder.config = sped_config

                if sped_type not in sped_type_list:
                    raise ValueError(f"Tipo SPED nao suportado para GOLD: {sped_type}")

                with self.spark_job_label(
                    f"[Gold] construir — {sped_type}/{cnpj_8_digits}/{month_year} ({len(reports_list)} relatorios)"
                ):
                    gold_builder.run_complete_gold_build(
                        client_cnpj=cnpj_8_digits,
                        month_year=month_year,
                        reports=reports_list,
                        sped_type=sped_type,
                    )

                for file_id in file_ids:
                    processed_events.append({
                        "_file_id": file_id,
                        "_sped_type": sped_type,
                        "status": "PROCESSED_GOLD",
                    })

                logger.info(f"[Gold] Combinacao {sped_type}/{cnpj_8_digits}/{month_year} concluida ({len(file_ids)} arquivo(s))")

            except Exception as e:
                logger.error(
                    f"[Gold] Erro construindo combinacao {sped_type}/{cnpj_8_digits}/{month_year}: {e}",
                    exc_info=True,
                )
                for file_id in file_ids:
                    self._report_processing_failure_to_dlq(
                        file_id=file_id, sped_type=sped_type,
                        cnpj_8_digits=cnpj_8_digits, month_year=month_year,
                        error_message=str(e)[:1024],
                    )
                continue

        # Publica TODOS os eventos de sucesso do batch numa unica escrita
        # Kafka, em vez de 1 write_to_kafka por arquivo.
        if processed_events:
            event_df = self.spark.createDataFrame(processed_events)
            gold_kafka_df = (
                event_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            with self.spark_job_label(f"[Gold] publicar Kafka batch {batch_id} ({len(processed_events)} eventos)"):
                self.write_to_kafka(gold_kafka_df, TopicRegistry.ANALYTICS_GOLD.name)
            logger.info(f"[Gold] {len(processed_events)} arquivo(s) processado(s) e evento(s) publicado(s) no Kafka")

    def _report_processing_failure_to_dlq(self, file_id: str, sped_type: str, cnpj_8_digits: str, month_year: str, error_message: str):
        """Publica em sped.errors.dlq a falha isolada de UM arquivo — mesmo
        padrao/envelope usado por Bronze/Silver (_failed_stream/_failed_at
        compativeis com o filtro do republish_dlq.py), MAIS original_topic/
        kafka_value_truncated com o evento de handoff Silver->Gold
        reconstruido — sem isso o republish_dlq.py nao consegue saber pra
        onde/o-que republicar (mesma lacuna corrigida no Bronze/Silver)."""
        try:
            original_event = {
                "_file_id": file_id,
                "_sped_type": sped_type,
                "cnpj_8_digits": cnpj_8_digits,
                "month_year": month_year,
                "status": "PROCESSED_SILVER",
            }
            error_df = self.spark.createDataFrame([{
                "_file_id": file_id,
                "_sped_type": sped_type,
                "status": "PROCESSING_FAILED",
                "_error_message": error_message[:1024],
                "_failed_stream": self.stream_name,
                "_failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "original_topic": TopicRegistry.ENRICHED_SILVER.name,
                "kafka_value_truncated": json.dumps(original_event)[:4096],
            }])
            error_kafka_df = (
                error_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            self.write_to_kafka(error_kafka_df, TopicRegistry.ERRORS_DLQ.name)
        except Exception as dlq_error:
            logger.critical(f"[Gold] Failed to report processing failure to DLQ: {dlq_error}")