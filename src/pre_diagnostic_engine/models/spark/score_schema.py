from pyspark.sql.types import *

score_schema = StructType([
    StructField("cnpj", StringType(), False),
    StructField("ano_calendario", IntegerType(), False),
    StructField("raw_score", DoubleType(), True),
    StructField("final_score", DoubleType(), True),
    StructField("classification", StringType(), True)
])
