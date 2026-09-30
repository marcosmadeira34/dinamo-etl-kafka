# ./src/dinamo_web/utils/spark_session.py
import logging
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
load_dotenv()



logger = logging.getLogger("SPARK_SESSION")


def create_spark_session(app_name: str, config: dict) -> SparkSession:

    """
    Cria uma sessão Spark com as configurações necessárias para interagir com o S3.
    """
    
    s3_config = config.get("s3", {})

    endpoint = s3_config.get("endpoint") or os.getenv("AWS_ENDPOINT_URL")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # ---- CRIAÇÃO DE SESSÃO SIMPLIFICADA ----
    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.session.timeZone", "UTC")\
        .config("spark.sql.files.maxPartitionBytes", "67108864") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/checkpoints")
        
        
        

    # Se endpoint for fornecido (ex: Oracle Cloud), configura S3A explicitamente
    if endpoint and access_key and secret_key:
        builder.config("spark.hadoop.fs.s3a.endpoint", endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", access_key)\
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")\
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")\
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",\
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    else:
        # Se não houver credenciais explícitas, assume-se execução em ambiente com Roles (ex: EMR/EC2)
        # Nesses casos, o Hadoop já costuma vir configurado para usar Instance Profile
        logger.info("Credenciais explícitas não encontradas. Assumindo Instance Profile / Default Provider.")

    # print(f"Configurações do spark em create_spar_session {config["spark"]}")


    spark = builder.getOrCreate()

    print(spark.sparkContext.getConf().getAll())
    # config de logs
    spark.sparkContext.setLogLevel("ERROR")

    # Agora o Spark existe
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism * 2)
    
    print("Spark Session Pré-Diagnóstico criada com sucesso.")
    print("Master: ", spark.sparkContext.master)
    print('Default Parallelism: ', spark.sparkContext.defaultParallelism)
    print("App Name: ", spark.sparkContext.appName)
    print("Spark Version: ", spark.version)
    print("Spark UI: ", spark.sparkContext.uiWebUrl)
    print("Spark UI: ", spark.sparkContext.uiWebUrl)

    
    return spark
