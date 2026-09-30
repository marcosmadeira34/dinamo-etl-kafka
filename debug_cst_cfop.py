# debug_cst_cfop.py
#
# Script standalone de diagnostico: le o REG=C190 direto da Silver pro
# CNPJ/periodo indicado e mostra os valores REAIS de CST_ICMS e CFOP (com
# contagem), pra comparar contra a lista configurada em subvencao_config.yaml.
#
# Uso (dentro do pod/imagem que ja tem os jars Delta/S3A):
#   spark-submit --master local[*] debug_cst_cfop.py <cnpj_8_digitos> <periodo_MMAAAA>
#
# Exemplo:
#   spark-submit --master local[*] debug_cst_cfop.py 01063615 202106
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

cnpj = sys.argv[1]
periodo = sys.argv[2]

spark = (
    SparkSession.builder
    .appName("debug-cst-cfop")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

import os
bucket = os.getenv("BUCKET_NAME")
path = f"s3a://{bucket}/data-lake/silver/{cnpj}/EFD_FISCAL/{periodo}/REG=C190"
print(f"\n=== Lendo: {path} ===\n")

df = spark.read.format("delta").load(path)

print(f"Total de linhas: {df.count()}\n")

print("=== Valores REAIS de CST_ICMS (com contagem) ===")
df.groupBy("CST_ICMS").count().orderBy(F.desc("count")).show(50, truncate=False)

print("=== Valores REAIS de CFOP (com contagem) ===")
df.groupBy("CFOP").count().orderBy(F.desc("count")).show(50, truncate=False)

print("=== Tipo de dado (dtype) de CST_ICMS e CFOP no schema ===")
df.select("CST_ICMS", "CFOP").printSchema()

spark.stop()
