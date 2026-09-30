from pyspark.sql.types import *

opportunity_schema = StructType([
    StructField("cnpj", StringType(), False),
    StructField("ano_calendario", IntegerType(), False),
    StructField("opportunity_id", StringType(), False),
    StructField("description", StringType(), True),
    StructField("reference_code", StringType(), True),
    StructField("estimated_credit", DoubleType(), True),
    StructField("score_impact", DoubleType(), True)
])
