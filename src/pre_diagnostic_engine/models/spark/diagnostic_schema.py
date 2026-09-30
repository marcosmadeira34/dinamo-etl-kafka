from pyspark.sql.types import *

diagnostic_schema = StructType([
    StructField("cnpj", StringType(), False),
    StructField("razao_social", StringType(), True),
    StructField("ano_calendario", IntegerType(), False),
    StructField("score", DoubleType(), True),
    StructField("classification", StringType(), True),
    StructField("go_no_go", StringType(), True)
])
