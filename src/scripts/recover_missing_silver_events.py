"""
Recuperacao pontual — incidente do AssertionError (Fase 3 da Silver,
batch 6, 98 arquivos de 01063615/EFD_CONTRIB).

Por que este script NAO usa republish_dlq.py: o DLQ desse incidente foi
gravado no formato "leve" (sem original_topic/kafka_value_truncated),
entao nao ha payload reconstruivel la dentro (todas as 1044 mensagens
vieram como "[skip] envelope incompleto"). Esse gap ja foi corrigido nos
tres streams para incidentes FUTUROS — mas os 98 deste incidente
especifico precisam ser recuperados de outra fonte.

A fonte da verdade aqui e a propria tabela Bronze: o Bronze ja escreveu
esses 98 arquivos com sucesso (o erro foi na LEITURA feita pela Silver,
no unionByName entre periodos com REGs diferentes — corrigido). A coluna
_source_file (preservada, nunca removida antes da escrita) tem tudo que
precisamos pra reconstruir o evento PROCESSED_BRONZE original e republicar
em sped.parsed.bronze, sem tocar no DLQ quebrado.

Uso (dry-run por padrao):
    spark-submit recover_missing_silver_events.py --cnpj 01063615 --sped-type EFD_CONTRIB

Uso real:
    spark-submit recover_missing_silver_events.py --cnpj 01063615 --sped-type EFD_CONTRIB --execute
"""
import argparse
import json
import os
import sys

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark/app/src")

from spark.session import create_spark_session
from config.config_manager import ConfigManager
from streaming.stream_config import StreamConfig
from kafka.topics import TopicRegistry
from kafka.producer import DinamoProducer
from kafka.config import KafkaConfig


def listar_pastas_de_periodo(bucket_name: str, prefix: str) -> list:
    """
    Lista as pastas de periodo (1 nivel abaixo do prefix) via boto3 direto
    — evita a API Java do FileSystem do Hadoop, que retornou "Wrong FS"
    porque FileSystem.get(hadoop_conf) sem URI resolve pro filesystem
    default (local), nao pro S3A. boto3 e mais simples e e a mesma lib que
    o BucketConnector do projeto ja usa por baixo dos panos.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    paginator = s3.get_paginator("list_objects_v2")
    prefixes = set()
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefixes.add(cp["Prefix"])
    return sorted(prefixes)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cnpj", required=True, help="CNPJ raiz (8 digitos), ex: 01063615")
    parser.add_argument("--sped-type", required=True, help="Ex: EFD_CONTRIB")
    parser.add_argument("--execute", action="store_true", help="Sem essa flag, so mostra o que seria republicado (dry-run)")
    args = parser.parse_args()

    config_manager = ConfigManager()
    main_config = config_manager.main_config
    spark = create_spark_session("DINAMO_RECOVER_MISSING_SILVER_EVENTS", main_config)
    stream_config = StreamConfig()

    bucket_name = os.environ["BUCKET_NAME"]
    prefix = f"data-lake/bronze/{args.cnpj}/{args.sped_type}/"
    print(f"Listando pastas de periodo em: s3a://{bucket_name}/{prefix}")

    periodo_prefixes = listar_pastas_de_periodo(bucket_name, prefix)
    if not periodo_prefixes:
        print(f"ERRO: nenhuma pasta de periodo encontrada em {prefix}")
        sys.exit(1)

    periodo_dirs = [f"s3a://{bucket_name}/{p}" for p in periodo_prefixes]
    print(f"Encontrados {len(periodo_dirs)} periodo(s).")

    # Le cada periodo separado, uniona depois — mesma tecnica da Fase 3
    # corrigida na Silver (evita "Conflicting directory structures" ao
    # ler varios periodos com conjuntos diferentes de REG de uma vez so).
    dfs = []
    for periodo_path in periodo_dirs:
        try:
            dfs.append(spark.read.parquet(periodo_path))
        except Exception as e:
            print(f"  [aviso] falha lendo {periodo_path}: {e}")

    if not dfs:
        print("Nenhum periodo pode ser lido. Abortando.")
        sys.exit(1)

    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)

    # 1 linha por arquivo, com o _source_file original (de onde derivamos
    # o file_path/file_name do evento a republicar).
    arquivos = (
        df_all
        .select("_file_id", "_source_file")
        .distinct()
        .collect()
    )
    print(f"Total de arquivos distintos encontrados na Bronze: {len(arquivos)}")

    eventos = []
    for row in arquivos:
        file_id = row["_file_id"]
        source_file = row["_source_file"] or ""
        # _source_file vem de input_file_name(): "s3a://bucket/TO_CONVERT/..."
        # Extrai o file_path relativo (a partir de "TO_CONVERT/"), do
        # mesmo jeito que o resto do projeto ja assume.
        idx = source_file.find("TO_CONVERT/")
        file_path = source_file[idx:] if idx >= 0 else source_file

        eventos.append({
            "_file_id": file_id,
            "_sped_type": args.sped_type,
            "file_path": file_path,
            "status": "PROCESSED_BRONZE",
        })

    print(f"\nEventos a republicar em '{TopicRegistry.PARSED_BRONZE.name}': {len(eventos)}")
    for ev in eventos[:10]:
        print(f"  -> {ev['_file_id']} | {ev['file_path']}")
    if len(eventos) > 10:
        print(f"  ... e mais {len(eventos) - 10}")

    if not args.execute:
        print("\n[DRY-RUN] Nada foi publicado. Rode de novo com --execute pra publicar de verdade.")
        return

    producer = DinamoProducer(KafkaConfig())
    publicados = 0
    for ev in eventos:
        producer.send(
            topic=TopicRegistry.PARSED_BRONZE.name,
            key=ev["_file_id"],
            value=ev,
        )
        publicados += 1
    producer.close()

    print(f"\nRepublicados com sucesso: {publicados}/{len(eventos)}")


if __name__ == "__main__":
    main()
