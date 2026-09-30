# dinamo_web/src/dinamo_web/streaming/subvencao_stream.py
"""
Subvencao Streaming Job — escuta o mesmo topico que o GoldStream
(sped.enriched.silver), e quando o evento e de EFD_FISCAL, roda o
diagnostico de Subvencao (SubvencaoGoldBuilder) para o CNPJ/periodo do
evento, gravando o resultado na camada Gold em Delta.

Mesma mecanica de BaseStream que todo o resto do dinamo ja usa:
    - readStream do Kafka via BaseStream.read_from_kafka()
    - foreachBatch chamando process_micro_batch() (este arquivo so
      implementa a logica de negocio; toda a infraestrutura de streaming —
      trigger, checkpoint, DLQ, retry — vem de cima, sem reimplementar nada)
    - write_to_kafka() para publicar o evento de conclusao

Por que um stream SEPARADO do GoldStream (nao um branch dentro dele):
    - "Cada SparkApplication e responsavel por iniciar apenas uma camada"
      (mesmo comentario ja usado em apps/bronze_app.py) — Subvencao e um
      relatorio de negocio distinto do motor generico de relatorios
      auxiliares (entities/document_sources) que o GoldStream roda, com
      registros de entrada proprios (0000/0150, que os outros relatorios
      Gold nao precisam) e regras proprias (CST 51 condicional, alíquota
      interestadual, Modelo 65). Deploy/scaling/rollback independentes.
"""
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.streaming.base_stream import BaseStream
from src.streaming.stream_config import StreamConfig
from kafka.topics import TopicRegistry
from modules.subvencao.gold_layer.subvencao_gold_builder import SubvencaoGoldBuilder

logger = logging.getLogger("dinamo.streaming.subvencao")


class SubvencaoStream(BaseStream):
    """
    Flow por micro-batch:
        1. Le eventos FILE-LEVEL de sped.enriched.silver (mesmo formato que
           o GoldStream ja consome: _file_id, _sped_type, cnpj_8_digits,
           month_year).
        2. Filtra so eventos _sped_type == "EFD_FISCAL" (Subvencao so faz
           sentido nesse tipo de SPED).
        3. Para cada (cnpj_8_digits, month_year) distinto no batch, instancia
           SubvencaoGoldBuilder e roda build_subvencao + write_gold_subvencao.
        4. Publica evento de conclusao em sped.analytics.subvencao.
    """

    def __init__(self, spark: SparkSession, config: StreamConfig,
                 bucket_name: str, subvencao_config: dict):
        super().__init__(spark, config, stream_name="subvencao-stream")
        self.bucket_name = bucket_name
        self.subvencao_config = subvencao_config
        # Paths vem do StreamConfig (SILVER_PATH/GOLD_PATH), MESMA fonte que
        # bronze/silver/gold_app.py ja usam via sparkConf.spark.kubernetes.
        # driverEnv.* nos manifestos reais — confirmado: data-lake/silver e
        # data-lake/gold, minusculo (bug corrigido: usava "GOLD/" maiusculo antes).
        self.silver_base_path = config.silver_path
        self.gold_base_path = config.gold_path
        self.base_path = config.base_path

    def get_source_topic(self) -> str:
        return TopicRegistry.ENRICHED_SILVER.name

    def process_micro_batch(self, batch_df: DataFrame, batch_id: int):
        json_schema = "_file_id STRING, _sped_type STRING, status STRING, cnpj_8_digits STRING, month_year STRING"
        eventos = (
            batch_df
            .select(F.from_json(F.col("kafka_value"), json_schema).alias("data"))
            .select("data.*")
            .filter(F.col("_sped_type") == "EFD_FISCAL")
            .distinct()
            .collect()
        )

        if not eventos:
            return

        logger.info(f"[Subvencao] Processing batch {batch_id} | eventos={len(eventos)}")

        filtros = self.subvencao_config["filters"]
        referencias = self.subvencao_config.get("referencias", {})

        for meta in eventos:
            file_id = meta["_file_id"]
            cnpj = meta["cnpj_8_digits"]
            month_year = meta["month_year"]

            if not file_id or not cnpj or not month_year:
                continue

            try:
                builder = SubvencaoGoldBuilder(
                    spark=self.spark,
                    base_path=self.silver_base_path,
                    period=month_year,
                    csts_isencao=filtros["csts"]["isencao"],
                    csts_reducao=filtros["csts"]["reducao"],
                    csts_condicionais_51=filtros["csts"]["condicionais_51"],
                    cfops_validos=filtros["cfops"]["validos"],
                    bucket_name=self.bucket_name,
                    aliquotas_key=referencias["aliquotas_interestadual_key"],
                    municipios_key=referencias["municipios_key"],
                    cfop_key=referencias["cfop_key"],
                )
                builder.build_subvencao(client_cnpj=cnpj, month_year=month_year)

                # output_base = f"{self.gold_base_path}/{cnpj}/SUBVENCAO/{month_year}"
                output_base = f"s3a://{self.bucket_name}/data-lake/subvencao/{cnpj}/{month_year}"
                gravados = builder.write_gold_subvencao(output_base)

                event_df = self.spark.createDataFrame([{
                    "_file_id": file_id,
                    "_sped_type": "EFD_FISCAL",
                    "cnpj_8_digits": cnpj,
                    "month_year": month_year,
                    "status": "PROCESSED_SUBVENCAO",
                }])
                subvencao_kafka_df = (
                    event_df
                    .withColumn("kafka_value", F.to_json(F.struct(
                        "_file_id", "_sped_type", "cnpj_8_digits", "month_year", "status",
                    )))
                    .withColumn("kafka_key", F.col("_file_id"))
                )
                self.write_to_kafka(subvencao_kafka_df, TopicRegistry.SUBVENCAO_GOLD.name)

                logger.info(f"[Subvencao] CNPJ={cnpj} periodo={month_year} concluido: {gravados}")

            except Exception as e:
                logger.error(
                    f"[Subvencao] Erro CNPJ={cnpj} periodo={month_year} file_id={file_id}: {e}",
                    exc_info=True,
                )
                raise
