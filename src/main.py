"""
main.py — Dinamo Streaming ETL Dispatcher

Ponto de entrada para jobs Spark efemeros submetidos pelo dinamo-launcher.
Recebe argumentos via spec.arguments do SparkApplication e despacha
para o processador correto conforme a action.

Uso (pelo launcher via Kafka → SparkApplication):
  --action REPROCESS_ECF --file_path TO_CONVERT/01990619/ECF/2024/SPED.txt
  --action REPROCESS_EFD_CONTRIB --file_path TO_CONVERT/...
  --action GOLD_BACKFILL --cnpj 01990619000153 --periodo 202401

Diferenca do streaming perpetuo:
  - bronze_app.py / silver_app.py / gold_app.py: streaming continuo (restartPolicy: Always)
  - main.py: job efemero sob demanda (disparado pelo launcher, termina apos concluir)
"""
import argparse
import logging
import sys
import os

ROOT_DIR = '/opt/spark/app'
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from observability.structured_logger import configure_structured_logging
from spark.session import create_spark_session
from config.config_manager import ConfigManager

configure_structured_logging(level="INFO", service_name="dinamo-ondemand")
logger = logging.getLogger("DINAMO_ONDEMAND")


def parse_args():
    parser = argparse.ArgumentParser(description="Dinamo ETL On-demand Dispatcher")
    parser.add_argument("--action",       required=True,  help="Action a executar")
    parser.add_argument("--file_path",    required=False, help="Path do arquivo SPED no bucket")
    parser.add_argument("--cnpj",         required=False, help="CNPJ (para backfill)")
    parser.add_argument("--periodo",      required=False, help="Periodo YYYYMM (para backfill)")
    parser.add_argument("--company_size", required=False, default="light",
                        help="Tamanho dos recursos Spark (light/medium/heavy)")
    return parser.parse_args()


def action_reprocess_sped(spark, config, file_path: str, sped_type: str):
    """
    Reprocessa um arquivo SPED especifico do zero:
    1. Le o arquivo bruto do bucket
    2. Grava na camada Bronze (Delta)
    3. Publica evento no Kafka para o Silver processar
    """
    from streaming.bronze_stream import BronzeStream
    from streaming.stream_config import StreamConfig

    logger.info(f"Reprocessando {sped_type} | file_path={file_path}")

    stream_config = StreamConfig()
    bucket_name   = config.main_config["s3"]["bucket_name"]

    bronze = BronzeStream(spark=spark, config=stream_config, bucket_name=bucket_name)

    # Processa o arquivo diretamente (sem consumir Kafka — job efemero)
    bronze.process_single_file(file_path=file_path, sped_type=sped_type)

    logger.info(f"Reprocessamento concluido: {file_path}")


def action_gold_backfill(spark, config, cnpj: str, periodo: str):
    """
    Reconstroi a camada Gold para um CNPJ/periodo especifico
    lendo os dados ja existentes na Silver.
    """
    from streaming.gold_stream import GoldStream
    from streaming.stream_config import StreamConfig

    logger.info(f"Gold backfill | cnpj={cnpj} | periodo={periodo}")

    stream_config = StreamConfig()
    gold = GoldStream(spark=spark, config=stream_config)
    gold.backfill(cnpj=cnpj, periodo=periodo)

    logger.info(f"Gold backfill concluido | cnpj={cnpj} | periodo={periodo}")


# Mapeamento action -> (funcao, sped_type ou None)
ACTION_MAP = {
    "REPROCESS_EFD_CONTRIB": ("sped", "EFD_CONTRIB"),
    "REPROCESS_EFD_FISCAL":  ("sped", "EFD_FISCAL"),
    "REPROCESS_ECD":         ("sped", "ECD"),
    "REPROCESS_ECF":         ("sped", "ECF"),
    "GOLD_BACKFILL":         ("backfill", None),
}


def main():
    args = parse_args()
    action = args.action.upper()

    logger.info(f"Dinamo On-demand | action={action} | company_size={args.company_size}")

    if action not in ACTION_MAP:
        logger.error(f"Action desconhecida: {action}. Disponiveis: {list(ACTION_MAP.keys())}")
        sys.exit(1)

    config_manager = ConfigManager()
    spark = create_spark_session("DINAMO_ONDEMAND", config_manager.main_config)

    action_type, sped_type = ACTION_MAP[action]

    try:
        if action_type == "sped":
            if not args.file_path:
                logger.error(f"--file_path e obrigatorio para action {action}")
                sys.exit(1)
            action_reprocess_sped(spark, config_manager, args.file_path, sped_type)

        elif action_type == "backfill":
            if not args.cnpj or not args.periodo:
                logger.error("--cnpj e --periodo sao obrigatorios para GOLD_BACKFILL")
                sys.exit(1)
            action_gold_backfill(spark, config_manager, args.cnpj, args.periodo)

        logger.info(f"Action {action} concluida com sucesso")

    except Exception as e:
        logger.exception(f"Erro ao executar action {action}: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
