# src/pre_diagnostic_engine/extractors/ecf_extractor.py
"""
Extrator de registros ECF a partir dos relatórios Gold já gerados no bucket.
NUNCA lê SPED bruto — sempre parte da Silver/Gold Layer.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pre_diagnostic_engine.repositories.oci_repository import OciRepository
from pre_diagnostic_engine.utils.ecf_deduplicator import deduplicate_ecf_by_file_id

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(message)s") 
logger = logging.getLogger("ECF_EXTRACTOR")


class EcfExtractor:
    """
    Extrai DataFrames dos relatórios Gold ECF persistidos no bucket.
    Fonte: SPED_GOLD_LAYER/<cnpj>/<month_year>/<report_key>
    """

    def __init__(self, spark: SparkSession, oci_repo: OciRepository):
        self.spark = spark
        self.oci = oci_repo
        self.logger = logger

    def extract_0010(self, client_cnpj: str, month_year: str):
        """
        Extrai Registro 0010 (Forma de tributação — FORMA_TRIB_PER).
        Fonte: relatório ECF_0000_0010.
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_0010", client_cnpj, month_year)

        if df is None:
            logger.warning(f"ECF 0010 não encontrado para CNPJ {client_cnpj}")
            return None
        df = deduplicate_ecf_by_file_id(df)
        logger.info(f"ECF 0010 extraído para CNPJ {client_cnpj}")
        return df

    def extract_l100(self, client_cnpj: str, month_year: str):
        """
        Extrai Registro L100 (Balanço Patrimonial — referencial 2.03 = PL).
        Fonte: relatório ECF_0000_L030_L100.
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_L030_L100", client_cnpj, month_year)

        if df is None:
            logger.warning(f"ECF L100 não encontrado para CNPJ {client_cnpj}")
            return None
        df = deduplicate_ecf_by_file_id(df)
        logger.info(f"ECF L100 extraído para CNPJ {client_cnpj}")
        return df

    def extract_l300(self, client_cnpj: str, month_year: str):
        """
        Extrai Registro L300 (DRE ECF — referencial 3.01).
        Fonte: relatório ECF_0000_L030_L300.
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_L030_L300", client_cnpj, month_year)

        if df is None:
            logger.warning(f"ECF L300 não encontrado para CNPJ {client_cnpj}")
            return None
        df = deduplicate_ecf_by_file_id(df)
        logger.info(f"ECF L300 extraído para CNPJ {client_cnpj}")
        return df

    def extract_m310(self, client_cnpj: str, month_year: str):
        """
        Extrai Registro M310 (LALUR — adições, exclusões e compensações).
        Fonte: relatório ECF_0000_M030_M300_M310.
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_M030_M300_M310", client_cnpj, month_year)

        if df is None:
            logger.warning(f"ECF M310 não encontrado para CNPJ {client_cnpj}")
            return None
        df = deduplicate_ecf_by_file_id(df)
        logger.info(f"ECF M310 extraído para CNPJ {client_cnpj}")
        return df

    def extract_n500(self, client_cnpj: str, month_year: str):
        """
        Extrai Registro N500 (Resultado Fiscal — lucro/prejuízo fiscal).
        Fonte: relatório ECF_0000_N030_N500.
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_N030_N500", client_cnpj, month_year)

        if df is None:
            logger.warning(f"ECF N500 não encontrado para CNPJ {client_cnpj}")
            return None
        df = deduplicate_ecf_by_file_id(df)
        logger.info(f"ECF N500 extraído para CNPJ {client_cnpj}")
        return df

    def extract_y570(self, client_cnpj: str, month_year: str):
        """
        Extrai Registro Y570 (Retenções na Fonte — IRPJ/CSLL).
        Fonte: relatório ECF_0000_Y570.
        """
        df = self.oci.read_gold_ecf_report("ECF_0000_Y570", client_cnpj, month_year)

        if df is None:
            logger.warning(f"ECF Y570 não encontrado para CNPJ {client_cnpj}")
            return None
        df = deduplicate_ecf_by_file_id(df)
        logger.info(f"ECF Y570 extraído para CNPJ {client_cnpj}")
        return df
