# src/pre_diagnostic_engine/extractors/darf_extractor.py
"""
Extrator de DARFs a partir da Silver/Gold Layer.
Consolida pagamentos de IRPJ e CSLL por regime.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pre_diagnostic_engine.repositories.oci_repository import OciRepository

logger = logging.getLogger("dinamo.tax_intelligence.darf_extractor")


class DarfExtractor:
    """
    Extrai e consolida pagamentos DARF a partir dos dados Gold ECF.
    Os DARFs são identificados via códigos de receita (YAML-driven).
    """

    def __init__(self, spark: SparkSession, oci_repo: OciRepository):
        self.spark = spark
        self.oci = oci_repo

    def extract_darf_payments(self, client_cnpj: str, month_year: str) -> DataFrame:
        """
        Lê DARFs pagos de IRPJ e CSLL a partir do relatório Gold ECF Y570
        ou de fonte DARF dedicada no bucket.
        Retorna DataFrame com: cnpj, periodo_apuracao, codigo_receita, valor_principal
        """
        try:
            df = self.oci.read_gold_ecf_report("ECF_0000_Y570", client_cnpj, month_year)
            logger.info(f"DARFs extraídos para CNPJ {client_cnpj} | período {month_year}")
            return df
        except Exception as e:
            logger.warning(f"DARF não encontrado para {client_cnpj}/{month_year}: {e}")
            return self.spark.createDataFrame([], schema="cnpj STRING, periodo_apuracao STRING, codigo_receita STRING, valor_principal DOUBLE")
