# converter_subvencao_para_xlsx.py
#
# Converte os relatorios de Subvencao (isencao/reducao, gravados como
# tabelas DELTA no bucket) para .xlsx, pra validacao manual.
#
# DIFERENTE do converter_referencias_para_parquet.py (aquele le/escreve
# .xlsx/.parquet SOLTOS via boto3+pandas puro, sem Spark): aqui o source e
# uma tabela DELTA (tem _delta_log/ + um ou mais arquivos de dados) --
# pandas.read_parquet direto NAO funciona, precisa de algo que entenda o
# log do Delta. Como o pod ja roda a imagem dinamo-spark:dev (mesma usada
# pelo SparkApplication real, ja tem PySpark + jars do Delta + rede
# liberada pro bucket), criamos uma SparkSession LOCAL aqui dentro --
# nao precisa de spark-submit nem de um SparkApplication/cluster completo,
# so o pod avulso mesmo (mesmo padrao que voce ja usa pro converter de
# referencias).
#
# Uso (dentro do pod, mesmo padrao do converter_referencias_para_parquet.py):
#   python3 /tmp/converter_subvencao.py 30698208 202110
#   python3 /tmp/converter_subvencao.py 30698208 202110 202208 202301   # varios periodos
import io
import os
import sys

import boto3
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.environ["BUCKET_NAME"]
ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")

NOMES = ["isencao", "reducao"]  # mesmos dois relatorios gravados por write_gold_subvencao()


def criar_cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def criar_spark_local() -> SparkSession:
    """SparkSession local (sem cluster/executors) -- suficiente pra esses
    volumes pequenos (centenas/milhares de linhas por periodo). Config
    s3a/Delta espelha o que o SparkApplication real ja usa (AWS_* via env)."""
    builder = (
        SparkSession.builder
        .appName("converter_subvencao_xlsx")
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


def converter_um(spark: SparkSession, s3, cnpj: str, periodo: str = None) -> None:
    for nome in NOMES:
        path_delta = f"s3a://{BUCKET}/DINAMO-WEB-DEVELOPMENT/SPED_GOLD_LAYER/{cnpj}/{nome}"
        print(f"Lendo tabela Delta: {path_delta}")
        try:
            df = spark.read.format("delta").load(path_delta)
        except Exception as e:
            print(f"  -> nao encontrada ({e}), pulando.\n")
            continue

        total = df.count()
        print(f"  -> {total} linhas")

        if total == 0:
            print(f"  -> {nome}: 0 linhas, nada pra exportar (esperado se nao houve oportunidade no periodo).\n")
            continue

        pdf = df.toPandas()

        # pandas/openpyxl escrevem em arquivo LOCAL (nao existe engine de
        # excel que va direto em s3a://) -- gera em memoria (BytesIO) e sobe
        # via boto3 pro MESMO lugar de onde leu (ao lado das pastas
        # isencao/reducao, mesmo nivel de periodo), sem tocar disco do pod.
        buffer = io.BytesIO()
        pdf.to_excel(buffer, index=False, engine="openpyxl")
        buffer.seek(0)

        chave_saida = f"data-lake/prediagnostic/{cnpj}/{periodo}/{nome}.xlsx"
        s3.put_object(Bucket=BUCKET, Key=chave_saida, Body=buffer.getvalue())
        print(f"  -> salvo em s3://{BUCKET}/{chave_saida}\n")


def main():
    if len(sys.argv) < 3:
        print("Uso: python3 converter_subvencao_para_xlsx.py <cnpj> <periodo1> [periodo2] ...")
        sys.exit(1)

    cnpj = sys.argv[1]
    periodos = sys.argv[2:]

    spark = criar_spark_local()
    s3 = criar_cliente_s3()
    try:
        for periodo in periodos:
            print(f"=== CNPJ={cnpj} periodo={periodo} ===")
            converter_um(spark, s3, cnpj, periodo)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
