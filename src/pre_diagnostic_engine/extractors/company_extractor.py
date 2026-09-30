# src/pre_diagnostic_engine/extractors/company_extractor.py
"""
Extrator de dados cadastrais da empresa a partir do ECF 0000/0010.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pre_diagnostic_engine.repositories.oci_repository import OciRepository

logger = logging.getLogger("dinamo.tax_intelligence.company_extractor")


class CompanyExtractor:
    """
    Extrai dados cadastrais (razão social, CNPJ, período) do ECF Registro 0000.
    """

    def __init__(self, spark: SparkSession, oci_repo: OciRepository):
        self.spark = spark
        self.oci = oci_repo

    def extract_company_info(self, client_cnpj: str, month_year: str) -> DataFrame:
        """
        Lê identificação da empresa a partir do relatório ECF_0000_0010.
        Retorna: cnpj, razao_social, ano_calendario, forma_trib_per
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_0010", client_cnpj, month_year)
        logger.info(f"Informações cadastrais extraídas para CNPJ {client_cnpj}")
        return df
