# src/pre_diagnostic_engine/extractors/retention_extractor.py
"""
Extrator de retenções na fonte (IRPJ/CSLL) a partir do ECF Y570.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pre_diagnostic_engine.repositories.oci_repository import OciRepository

logger = logging.getLogger("dinamo.tax_intelligence.retention_extractor")


class RetentionExtractor:
    """
    Extrai retenções na fonte a partir do Registro Y570 (ECF).
    """

    def __init__(self, spark: SparkSession, oci_repo: OciRepository):
        self.spark = spark
        self.oci = oci_repo

    def extract_retentions(self, client_cnpj: str, month_year: str) -> DataFrame:
        """
        Lê retenções IRPJ/CSLL do relatório Gold Y570.
        Retorna: cnpj, codigo_retencao, valor_retido, natureza
        """
        try:
            df = self.oci.read_gold_ecf_report("ECF_0000_Y570", client_cnpj, month_year)
            logger.info(f"Retenções extraídas para CNPJ {client_cnpj}")
            return df
        except Exception as e:
            logger.warning(f"Y570 não encontrado para {client_cnpj}/{month_year}: {e}")
            return self.spark.createDataFrame(
                [],
                schema="cnpj STRING, codigo_retencao STRING, valor_retido DOUBLE, natureza STRING"
            )
