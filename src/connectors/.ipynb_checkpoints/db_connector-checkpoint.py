from pyspark.sql import DataFrame, SparkSession
from .base_connector import BaseConnector

class DatabaseConnector(BaseConnector):
    """
    Conector para interagir com o PostgreSQL usando PySpark JDBC
    """
    def __init__(self, spark: SparkSession, url: str, properties: dict):
        self.spark = spark
        self.url = url
        self.properties = properties

    def write(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Escreve um DataFrame em uma tabela do PostgreSQL
        O modo 'overwrite' simula o TRUNCATE TABKE do SQL
        Docstring for write
        
        :param self: Description
        :param df: Description
        :type df: DataFrame
        :param table_name: Description
        :type table_name: str
        :param mode: Description
        :type mode: str
        """
        df.write.jdbc(url=self.url, table=table_name, mode=mode, properties=self.properties)
