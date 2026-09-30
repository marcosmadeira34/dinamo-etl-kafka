# dinamo_web/src/dinamo_web/streaming/base_stream.py
"""
Abstract base class for all Structured Streaming jobs.
Provides Kafka source/sink wiring, checkpoint management, and error handling.
"""
import os
import json
import logging
import time
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from streaming.stream_config import StreamConfig
from kafka.topics import TopicRegistry


logger = logging.getLogger("dinamo.streaming")

# ---------------------------------------------------------------------------
# Constantes de tuning
# ---------------------------------------------------------------------------
# Máx. de records por task ao escrever no Kafka.
# Evita que poucos tasks fiquem sobrecarregados e controla o nº de producers
# internos que o Spark abre (1 por task).
_MAX_KAFKA_WRITE_PARALLELISM = 8

# Limite de tamanho do payload de erro enviado ao DLQ (bytes).
_DLQ_MAX_VALUE_BYTES = 4096

# Limite de tamanho da mensagem de erro no DLQ (caracteres).
_DLQ_MAX_ERROR_LEN = 1024


class BaseStream(ABC):
    """
    Base class for Bronze/Silver/Gold streaming jobs.
    
    Subclasses implement process_micro_batch() which is called via foreachBatch.
    This pattern allows reuse of existing PySpark batch logic (SilverSpedBuilder,
    GoldBuilder, etc.) without rewriting for continuous mode.
    """

    def __init__(self, spark: SparkSession, config: StreamConfig, stream_name: str):
        self.spark = spark
        self.config = config
        self.stream_name = stream_name
        self._batch_count = 0
        self._total_records = 0

        self._configure_spark()
        logger.info(f"Stream [{stream_name}] initialized | trigger={config.trigger_interval}")

    def _configure_spark(self):
        """Apply streaming-specific Spark configurations."""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.streaming.noDataMicroBatches.enabled", "false")
        self.spark.conf.set("spark.streaming.backpressure.enabled", "true")
        self.spark.conf.set("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "true")

    def read_from_kafka(self, topic: str) -> DataFrame:
        """Create a streaming DataFrame from a Kafka topic."""
        options = self.config.kafka_read_options(stream_name=self.stream_name)
        options["subscribe"] = topic

        return (
            self.spark
            .readStream
            .format("kafka")
            .options(**options)
            .load()
            .selectExpr(
                "CAST(key AS STRING) as kafka_key",
                "CAST(value AS STRING) as kafka_value",
                "topic",
                "partition",
                "offset",
                "timestamp as kafka_timestamp",
            )
        )

    def write_to_kafka(self, df: DataFrame, topic: str):
        """
        Write DataFrame to Kafka with controlled partitioning and producer tuning.

        Key design decisions:
        - Dynamic coalesce based on record count (avoids both too-few and
          too-many Spark tasks, each of which opens a Kafka producer).
        - All producer tuning comes from StreamConfig.kafka_write_options()
          so it is consistent across normal writes and DLQ writes.
        - Payload size is capped by kafka.max.request.size in the config.
        """
        write_options = self.config.kafka_write_options()

        # Dynamic parallelism: min(partitions, _MAX_KAFKA_WRITE_PARALLELISM)
        # This prevents both:
        #  - Too many producers (hundreds of Spark tasks each opening a connection)
        #  - Too few producers (coalesce(3) bottleneck on large batches)
        num_partitions = min(
            df.rdd.getNumPartitions(),
            _MAX_KAFKA_WRITE_PARALLELISM,
        )
        num_partitions = max(num_partitions, 1)

        kafka_df = (
            df
            .selectExpr(
                "CAST(kafka_key AS STRING) as key",
                "CAST(kafka_value AS STRING) as value"
            )
            .coalesce(num_partitions)
        )

        (
            kafka_df
            .write
            .format("kafka")
            .option("topic", topic)
            .options(**write_options)
            .save()
        )

    def write_to_delta(self, df: DataFrame, path: str, partition_by: list = None, replace_where: str = None):
        """Write a batch DataFrame to Delta Lake (SaaS optimized data lake)."""
        writer = (
            df.write
            .mode("append")
            .format("delta")
            .option("mergeSchema", "true")
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        # writer.save(path)

        if replace_where:
            (
                writer
                .mode('overwrite')
                .option('replaceWhere', replace_where)
                .save(path)
            )
        else:
           writer.save(path)
    
    def write_to_parquet(self, df: DataFrame, path: str, partition_by: list = None):
        """Write a batch DataFrame to Parquet (SaaS optimized data lake)."""
        writer = (
            df.write
            .mode("append")
            .format("parquet")
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
    
    def _foreach_batch_wrapper(self, batch_df: DataFrame, batch_id: int):
        """
        Wrapper for foreachBatch that adds error handling and metrics.
        Delegates actual processing to subclass's process_micro_batch().
        """
        if batch_df.isEmpty():
            logger.debug(f"[{self.stream_name}] Empty batch {batch_id}, skipping")
            return

        record_count = batch_df.count()
        self._batch_count += 1
        self._total_records += record_count

        logger.info(
            f"[{self.stream_name}] Processing batch {batch_id} | "
            f"records={record_count} | total_batches={self._batch_count} | "
            f"total_records={self._total_records}"
        )

        try:
            self.process_micro_batch(batch_df, batch_id)
            logger.info(f"[{self.stream_name}] Batch {batch_id} completed successfully")
        except Exception as e:
            logger.error(
                f"[{self.stream_name}] Batch {batch_id} FAILED: {e}",
                exc_info=True
            )
            self._handle_batch_error(batch_df, batch_id, e)

    # ------------------------------------------------------------------
    # DLQ handling — resilient, with local-disk fallback
    # ------------------------------------------------------------------

    def _handle_batch_error(self, batch_df: DataFrame, batch_id: int, error: Exception):
        """
        Handle batch processing failure.

        Strategy (in order):
        1. Try writing a *lightweight* error payload to the Kafka DLQ topic.
           - Only metadata columns are kept (kafka_key, topic, partition, offset).
           - The raw ``kafka_value`` is truncated to ``_DLQ_MAX_VALUE_BYTES``.
           - The error message is truncated to ``_DLQ_MAX_ERROR_LEN``.
        2. If Kafka is unreachable, fall back to writing the error as a
           JSON file on local disk (``config.dlq_fallback_path``).
        """
        # --- Build lightweight error payload ---
        truncated_error = str(error)[:_DLQ_MAX_ERROR_LEN]

        try:
            error_df = (
                batch_df
                .select(
                    F.col("kafka_key"),
                    # Truncate heavy payload to avoid oversized DLQ messages
                    F.substring(F.col("kafka_value"), 1, _DLQ_MAX_VALUE_BYTES).alias("kafka_value_truncated"),
                    F.col("topic").alias("original_topic"),
                    F.col("partition").alias("original_partition"),
                    F.col("offset").alias("original_offset"),
                )
                .withColumn("_error_message", F.lit(truncated_error))
                .withColumn("_failed_batch_id", F.lit(batch_id))
                .withColumn("_failed_stream", F.lit(self.stream_name))
                .withColumn("_failed_at", F.current_timestamp())
            )

            dlq_kafka_df = (
                error_df
                .withColumn("key", F.coalesce(F.col("kafka_key"), F.lit("unknown")))
                .withColumn("value", F.to_json(F.struct(
                    "kafka_value_truncated",
                    "original_topic",
                    "original_partition",
                    "original_offset",
                    "_error_message",
                    "_failed_batch_id",
                    "_failed_stream",
                    "_failed_at",
                )))
                .select("key", "value")
            )

            # Use full write_options (acks, timeouts, etc.)
            write_options = self.config.kafka_write_options()

            (
                dlq_kafka_df
                .coalesce(1)
                .write
                .format("kafka")
                .option("topic", TopicRegistry.ERRORS_DLQ.name)
                .options(**write_options)
                .save()
            )
            logger.warning(f"[{self.stream_name}] Failed batch {batch_id} routed to DLQ")

        except Exception as dlq_error:
            logger.error(
                f"[{self.stream_name}] Kafka DLQ unreachable for batch {batch_id}: {dlq_error}. "
                f"Falling back to local disk."
            )
            self._fallback_dlq_to_disk(batch_df, batch_id, error, dlq_error)

    def _fallback_dlq_to_disk(
        self,
        batch_df: DataFrame,
        batch_id: int,
        original_error: Exception,
        dlq_error: Exception,
    ):
        """
        Last-resort fallback: persist failed-batch metadata to local JSON
        so that no error evidence is lost even when Kafka is completely down.
        """
        try:
            fallback_dir = self.config.dlq_fallback_path
            os.makedirs(fallback_dir, exist_ok=True)

            ts = int(time.time() * 1000)
            filename = f"{fallback_dir}/{self.stream_name}_batch-{batch_id}_{ts}.json"

            # Collect only keys + offsets (lightweight)
            rows = (
                batch_df
                .select("kafka_key", "topic", "partition", "offset")
                .limit(100)  # cap at 100 rows to avoid OOM
                .toJSON()
                .collect()
            )

            payload = {
                "stream": self.stream_name,
                "batch_id": batch_id,
                "original_error": str(original_error)[:_DLQ_MAX_ERROR_LEN],
                "dlq_error": str(dlq_error)[:_DLQ_MAX_ERROR_LEN],
                "timestamp_ms": ts,
                "record_count": len(rows),
                "sample_records": [json.loads(r) for r in rows],
            }

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

            logger.warning(
                f"[{self.stream_name}] DLQ fallback written to disk: {filename} "
                f"({len(rows)} records)"
            )
        except Exception as disk_error:
            # Absolute last resort — just log everything we can
            logger.critical(
                f"[{self.stream_name}] CRITICAL: ALL DLQ mechanisms failed for batch {batch_id}. "
                f"original_error={original_error} | dlq_error={dlq_error} | disk_error={disk_error}"
            )

    @abstractmethod
    def process_micro_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process a single micro-batch. Implemented by subclasses.
        This is where existing batch logic (SilverSpedBuilder, GoldBuilder) is called.
        """
        ...

    @abstractmethod
    def get_source_topic(self) -> str:
        """Return the Kafka topic to read from."""
        ...

    def _checkpoint_exists(self, checkpoint_path: str) -> bool:
        """
        Confere via boto3 se ja existe um checkpoint real gravado (a
        subpasta "offsets/", que e o que o Structured Streaming usa pra
        saber ate onde ja leu). Sem isso, o proximo start() vai cair no
        startingOffsets=earliest e reprocessar o topico INTEIRO desde o
        primeiro byte ja publicado — foi exatamente essa a causa de um
        incidente real (checkpoint apagado -> 1199 arquivos de CNPJs
        antigos reprocessados do zero).
        """
        try:
            import boto3
            # checkpoint_path e algo como "s3a://{bucket}/spark-checkpoints/{stream_name}"
            without_scheme = checkpoint_path.replace("s3a://", "")
            bucket_name, _, prefix = without_scheme.partition("/")
            s3 = boto3.client(
                "s3",
                endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            )
            resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}/offsets/", MaxKeys=1)
            return resp.get("KeyCount", 0) > 0
        except Exception as e:
            logger.warning(f"[{self.stream_name}] Nao foi possivel verificar se o checkpoint existe (assumindo que existe, pra nao travar o start): {e}")
            return True

    def start(self):
        """Start the streaming query."""
        source_topic = self.get_source_topic()
        checkpoint = self.config.checkpoint_path(self.stream_name)

        if not self._checkpoint_exists(checkpoint):
            logger.warning(
                f"[{self.stream_name}] " + "!" * 60 + "\n"
                f"[{self.stream_name}] ATENCAO: nenhum checkpoint encontrado em {checkpoint}.\n"
               # f"[{self.stream_name}] Este stream vai processar TODO O HISTORICO do topico "
               # f"'{source_topic}' desde o primeiro byte ja publicado "
               # f"(startingOffsets=earliest), NAO apenas mensagens novas.\n"
               # f"[{self.stream_name}] Se isso NAO for intencional (ex: checkpoint apagado por "
               # f"engano durante troubleshooting), interrompa agora (Ctrl+C / delete o "
               # f"SparkApplication) antes de gastar tempo reprocessando dado antigo.\n"
                f"[{self.stream_name}] " + "!" * 60
            )

        # available_now: processa tudo que ha no Kafka agora e TERMINA
        # sozinho — usado pelos jobs efemeros (KEDA ScaledJob). continuous:
        # comportamento original, fica escutando pra sempre (processingTime).
        if self.config.trigger_mode == "available_now":
            trigger_kwargs = {"availableNow": True}
        else:
            trigger_kwargs = {"processingTime": self.config.trigger_interval}

        logger.info(
            f"[{self.stream_name}] Starting stream | "
            f"source={source_topic} | checkpoint={checkpoint} | "
            f"trigger_mode={self.config.trigger_mode}"
        )

        stream_df = self.read_from_kafka(source_topic)

        query = (
            stream_df
            .writeStream
            .foreachBatch(self._foreach_batch_wrapper)
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(**trigger_kwargs)
            .queryName(self.stream_name)
            .start()
        )

        logger.info(f"[{self.stream_name}] Stream started. Query ID: {query.id}")
        return query

    from contextlib import contextmanager

    @contextmanager
    def spark_job_label(self, description: str, group_id: str = None):
        """
        Rotula, na aba Jobs da Spark UI, os jobs disparados DENTRO deste
        bloco — em vez do generico "count/collect at <unknown>:0".

        IMPORTANTE: como o Spark e lazy, o rotulo so tem efeito sobre jobs
        cuja ACAO (collect/count/save/...) acontece dentro do "with". Definir
        o rotulo antes de so montar um DataFrame preguicoso (ex: read_txt())
        nao rotula nada — o job so nasce quando uma acao real dispara.

        Uso:
            with self.spark_job_label(f"[Bronze] read+parse grupo={sped_type}"):
                df_parsed = ...  # o job disparado pela primeira acao aqui usa esse rotulo
        """
        gid = group_id or description
        self.spark.sparkContext.setJobGroup(gid, description)
        try:
            yield
        finally:
            self.spark.sparkContext.setJobGroup(None, None)