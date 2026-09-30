# dinamo_web/src/dinamo_web/streaming/stream_config.py
"""
Streaming configuration — centralizes all Kafka + Spark streaming tuning parameters.
"""
import os
from dataclasses import dataclass, field


@dataclass
class StreamConfig:
    """Spark Structured Streaming configuration."""
    # Kafka
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            # "kafka-1:9092,kafka-2:9092,kafka-3:9092"
            "kafka:9092"
        )
    )

    # Checkpoints
    checkpoint_base_path: str = field(
        default_factory=lambda: os.getenv("CHECKPOINT_BASE_PATH", "/opt/spark/checkpoints")
    )

    # Data lake paths
    base_path : str = field(default_factory=lambda: os.getenv("BASE_PATH", "/opt/spark/data-lake"))
    bronze_path: str = field(default_factory=lambda: os.getenv("BRONZE_PATH", "/opt/spark/data-lake/bronze"))
    silver_path: str = field(default_factory=lambda: os.getenv("SILVER_PATH", "/opt/spark/data-lake/silver"))
    gold_path: str = field(default_factory=lambda: os.getenv("GOLD_PATH", "/opt/spark/data-lake/gold"))

    # DLQ fallback path (local disk when Kafka is unreachable)
    dlq_fallback_path: str = field(
        default_factory=lambda: os.getenv("DLQ_FALLBACK_PATH", "/opt/spark/data-lake/dlq-fallback")
    )

    # Trigger interval (usado so quando trigger_mode == "continuous")
    trigger_interval: str = "30 seconds"

    # Modo do trigger:
    #   "continuous"    -> trigger(processingTime=...), pod fica rodando pra
    #                       sempre (comportamento atual, SparkApplication
    #                       restartPolicy: Always).
    #   "available_now" -> trigger(availableNow=True), processa tudo que
    #                       estiver disponivel no Kafka AGORA e TERMINA
    #                       sozinho — e o que torna o pod efemero. Usado
    #                       pelos jobs disparados via KEDA ScaledJob.
    trigger_mode: str = field(
        default_factory=lambda: os.getenv("TRIGGER_MODE", "available_now")
    )

    # Consumer group ID estavel, usado em kafka.group.id na leitura. Spark
    # Structured Streaming NAO usa consumer group offsets pra recovery (usa
    # checkpoint), mas COMMITA os offsets nesse group como cortesia pra
    # ferramentas externas de monitoramento — e exatamente o que o KEDA
    # kafka scaler precisa pra calcular lag e decidir quando escalar. Sem
    # isso, o KEDA nao teria como saber se ha mensagens pendentes.
    consumer_group_id: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP_ID", "")
    )

    # Kafka consumer options
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int = 50000 # Tuning para alta volumetria (SaaS)
    fail_on_data_loss: bool = False

    # Watermark
    watermark_delay: str = "10 minutes"

    # Output mode
    output_mode: str = "append"

    def __post_init__(self):
        # Carrega o bucket configurado para armazenar checkpoints distribuídos no S3/OCI
        import yaml
        config_path = "/opt/spark/app/src/config/main_config.yaml"
        try:
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    app_config = yaml.safe_load(f)
                bucket_name = app_config.get("s3", {}).get("bucket_name")
                if bucket_name:
                    self.checkpoint_base_path = f"s3a://{bucket_name}/spark-checkpoints"
        except Exception as e:
            print(f"Warning: could not load S3 bucket for checkpoints. Using local: {e}")

    def checkpoint_path(self, stream_name: str) -> str:
        return f"{self.checkpoint_base_path}/{stream_name}"

    def kafka_read_options(self, stream_name: str = "") -> dict:
        options = {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "startingOffsets": self.starting_offsets,
            "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
            "failOnDataLoss": str(self.fail_on_data_loss).lower(),
        }
        # group.id estavel: usa o configurado explicitamente (KAFKA_CONSUMER_
        # GROUP_ID) ou deriva de stream_name como default, pra sempre ter um
        # group visivel ao KEDA mesmo se ninguem configurar nada.
        group_id = self.consumer_group_id or (f"dinamo-{stream_name}" if stream_name else "")
        if group_id:
            options["kafka.group.id"] = group_id
        return options

    def kafka_write_options(self) -> dict:
        """
        Complete Kafka producer options for Spark DataFrame .write.format("kafka").
        Includes timeout, ACK, retry, and payload tuning to prevent TimeoutException.
        """
        return {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            # --- ACK & Reliability ---
            # acks=all + enable.idempotence: uma troca de lider de particao
            # logo apos o ack (e antes da replicacao terminar) nao pode fazer
            # o evento de handoff entre camadas (Bronze->Silver->Gold) sumir
            # silenciosamente. Mesma robustez do producer de ingestao
            # (kafka/config.py), pois trata-se de dado fiscal.
            "kafka.acks": "all",
            "kafka.enable.idempotence": "true",
            "kafka.max.in.flight.requests.per.connection": "5",
            "kafka.retries": "10",
            "kafka.retry.backoff.ms": "500",
            # --- Timeouts ---
            "kafka.request.timeout.ms": "30000",
            "kafka.delivery.timeout.ms": "60000",
            "kafka.max.block.ms": "30000",
            # --- Batching ---
            "kafka.batch.size": "32768",
            "kafka.linger.ms": "10",
            # --- Payload size ---
            "kafka.max.request.size": "2097152",       # 2MB max
            "kafka.buffer.memory": "33554432",          # 32MB buffer
            # --- Compression ---
            "kafka.compression.type": "lz4",
        }
