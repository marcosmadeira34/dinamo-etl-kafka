# src/pre_diagnostic_engine/repositories/oci_repository.py
"""
Repositório para leitura de dados do bucket OCI (S3A compatível).
Reutiliza BucketConnector e BucketExtractor já existentes no projeto Dinamo.
Não implementa lógica de parse — apenas acesso ao storage.
"""
import os
import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pre_diagnostic_engine.connectors.bucket_connector import BucketConnector
from pre_diagnostic_engine.extractors.bucket_extractor import BucketExtractor 

from pyspark.errors.exceptions.captured import AnalysisException

logger = logging.getLogger("dinamo.tax_intelligence.oci_repository")


class OciRepository:
    """
    Wrapper sobre BucketConnector/BucketExtractor para acesso aos relatórios
    Gold ECF já gerados e persistidos no bucket OCI.
    """

    def __init__(self, spark: SparkSession, bucket_name: str):
        self.spark = spark
        self.bucket_name = bucket_name

        self.connector = BucketConnector(spark, bucket_name)
        self.extractor = BucketExtractor(spark, bucket_name)

        self._configure_s3a_credentials()

    def _configure_s3a_credentials(self):
        """
        Injeta as credenciais OCI/S3 no contexto Hadoop do Spark.
        Necessário para leituras via s3a://
        """

        hadoop_conf = self.spark._jsc.hadoopConfiguration()

        hadoop_conf.set("fs.s3a.access.key",os.getenv("AWS_ACCESS_KEY_ID").strip())
        hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY").strip())
        hadoop_conf.set("fs.s3a.endpoint",os.getenv("AWS_ENDPOINT_URL").replace("https://", "").strip())
        hadoop_conf.set("fs.s3a.path.style.access","true")
        hadoop_conf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    def read_gold_ecf_report(self, report_key: str, client_cnpj: str, month_year: str):
        path = (
            f"SPED_GOLD_LAYER/"
            f"{client_cnpj}/"
            f"ECF/"
            f"{month_year}/"
            f"{report_key}"
        )

        logger.info(f"Lendo Gold ECF report: s3a://{self.bucket_name}/DINAMO-WEB-DEVELOPMENT/{path}")

        try:
            return self.extractor.read_parquet(path)
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.warning(f"Relatório não encontrado no bucket: {path}")
                return None
            raise  # re-lança se for outro tipo de AnalysisException
        
    
    def read_silver_ecf_register(
        self,
        client_cnpj: str,
        month_year: str,
        register: str,
    ) -> DataFrame:
        """
        Lê um registro específico da camada Silver ECF.
        Path padrão: SPED_SILVER_LAYER/<cnpj>/ECF/<month_year>/<register>
        """
        path = f"SPED_SILVER_LAYER/{client_cnpj}/ECF/{month_year}/{register}"
        logger.info(f"Lendo Silver ECF register {register}: s3a://{self.bucket_name}/{path}")
        return self.extractor.read_parquet(path)

    def list_gold_ecf_cnpjs(self, month_year: str) -> List[str]:
        """
        Lista todos os CNPJs que têm relatórios Gold ECF disponíveis
        para o período informado.
        """
        prefix = f"SPED_GOLD_LAYER/"
        all_paths = self.connector.list_paths(prefix)
        cnpjs = set()
        for p in all_paths:
            parts = p.replace(prefix, "").split("/")
            if len(parts) >= 2 and parts[1] == month_year:
                cnpjs.add(parts[0])
        return list(cnpjs)

    def gold_report_exists(
        self, report_key: str, client_cnpj: str, month_year: str
    ) -> bool:
        path = f"SPED_GOLD_LAYER/{client_cnpj}/{month_year}/{report_key}"
        return self.connector.exists(path)
