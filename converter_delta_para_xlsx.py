# converter_delta_para_xlsx.py
#
# Varre um prefixo no bucket (default: DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/)
# procurando tabelas DELTA (qualquer pasta que tenha um "_delta_log/" dentro),
# le cada uma com Spark, converte pra .xlsx e salva IRMAO da pasta original
# (mesmo diretorio pai, nome = nome da pasta da tabela).
#
# Exemplo:
#   s3://bucket/DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/00445637/EFD_FISCAL/012016/FISCAL_0000_C100_C170_C190/_delta_log/...
#   ->
#   s3://bucket/DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/00445637/EFD_FISCAL/012016/FISCAL_0000_C100_C170_C190.xlsx
#
# Mesmo padrao do converter_subvencao_para_xlsx.py: SparkSession LOCAL dentro
# do proprio pod avulso (imagem dinamo-spark:dev, ja tem PySpark + jars do
# Delta + rede liberada pro bucket) -- nao precisa de spark-submit nem de um
# SparkApplication/cluster completo.
#
# Uso (dentro do pod):
#   python3 /tmp/converter_delta_para_xlsx.py
#       -> varre TUDO dentro de DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/
#
#   python3 /tmp/converter_delta_para_xlsx.py 00445637
#       -> so esse CNPJ (DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/00445637/)
#
#   python3 /tmp/converter_delta_para_xlsx.py 00445637/EFD_FISCAL/012016
#       -> so esse periodo
#
#   python3 /tmp/converter_delta_para_xlsx.py 00445637 --force
#       -> reconverte mesmo se o .xlsx ja existir (default: pula os que ja existem)
import os
import io
import sys

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

BUCKET = os.environ["BUCKET_NAME"]
ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")

BASE_PREFIX = "DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/"


def criar_cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def criar_spark_local() -> SparkSession:
    """SparkSession local (sem cluster/executors) - config s3a/Delta espelha
    o que o SparkApplication real ja usa (AWS_* via env)."""
    builder = (
        SparkSession.builder
        .appName("converter_delta_xlsx")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    if ENDPOINT:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    return builder.getOrCreate()


def encontrar_tabelas_delta(s3, prefix: str) -> list[str]:
    """Lista (paginado) tudo sob `prefix` e devolve os caminhos-raiz unicos
    de cada tabela Delta encontrada (qualquer pasta com "_delta_log/" dentro).
    """
    tabelas = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            marcador = "/_delta_log/"
            if marcador in key:
                raiz_tabela = key.split(marcador)[0]
                tabelas.add(raiz_tabela)
    return sorted(tabelas)


def ja_convertido(s3, output_key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=output_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def converter_tabela(spark: SparkSession, s3, tabela_path: str, force: bool) -> None:
    parent, nome_tabela = tabela_path.rsplit("/", 1)
    output_key = f"{parent}/{nome_tabela}.xlsx"

    print(f"=== {tabela_path} ===")

    if not force and ja_convertido(s3, output_key):
        print(f"  -> ja existe {output_key}, pulando (use --force pra reconverter)\n")
        return

    path_delta = f"s3a://{BUCKET}/{tabela_path}"
    try:
        df = spark.read.format("delta").load(path_delta)
    except Exception as e:
        print(f"  -> nao foi possivel ler como Delta ({e}), pulando.\n")
        return

    total = df.count()
    print(f"  -> {total} linhas")

    if total == 0:
        print(f"  -> 0 linhas, nada pra exportar.\n")
        return

    pdf = df.toPandas()

    # pandas/openpyxl escrevem em arquivo LOCAL -- gera em memoria (BytesIO)
    # e sobe via boto3, sem tocar disco do pod.
    buffer = io.BytesIO()
    pdf.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    s3.put_object(Bucket=BUCKET, Key=output_key, Body=buffer.getvalue())
    print(f"  -> salvo em s3://{BUCKET}/{output_key}\n")


def main():
    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--force"]

    subprefixo = args[0].strip("/") if args else ""
    prefix = BASE_PREFIX + (subprefixo + "/" if subprefixo else "")

    s3 = criar_cliente_s3()

    print(f"Procurando tabelas Delta em s3://{BUCKET}/{prefix} ...")
    tabelas = encontrar_tabelas_delta(s3, prefix)
    print(f"Encontradas {len(tabelas)} tabela(s).\n")

    if not tabelas:
        print("Nenhuma tabela encontrada - confere o prefixo.")
        return

    spark = criar_spark_local()
    try:
        for tabela_path in tabelas:
            converter_tabela(spark, s3, tabela_path, force)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
