# ./srcdinamo_web/connectors/bucket_connector.py
from pyspark.sql import SparkSession, DataFrame
from .base_connector import BaseConnector
import logging

logger = logging.getLogger(__name__)

class BucketConnector(BaseConnector):
    """
    Conector para interação com o bucket usando o PySpark
    """
    def __init__(self, spark: SparkSession, bucket_name: str) -> str:
        self.spark = spark
        self.bucket_name = bucket_name

    def _get_full_path(self, path: str) -> str:
        """Método auxiliar para construir a URI completa do bucket"""
        clean_path = path.lstrip('/')
        return f"s3a://{self.bucket_name}/{clean_path}"

    def read_csv(self, path: str, header: bool = True, infer_schema: bool = True,
                 delimiter: str = ";") -> DataFrame:
        """Lê os arquivos CSV de um caminho no bucket"""
        full_path = self._get_full_path(path)
        logger.info(f"Lendo arquivo CSV do Bucket em: {full_path}")
        return self.spark.read.csv(full_path, header=header, inferSchema=infer_schema, sep=delimiter)
    
    def read_parquet(self, path: str) -> DataFrame:
        """Lê os arquivos Parquet de um bucket"""
        full_path = self._get_full_path(path)
        logger.info(f"Lendo arquivo Parquet do Bucket em {full_path}")
        return self.spark.read.parquet(full_path)
    
    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", partition_by: list = None):
        """
        Escreve um DataFrame em formato parquet no bucket
        """
        full_path = self._get_full_path(path)
        logger.info(f"Escrevendo arquivo Parquet no bucket em {full_path}")
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(full_path)