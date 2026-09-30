# src/pre_diagnostic_engine/analyzers/retention_analyzer.py
"""
Analyzer de Retenções na Fonte.
Fonte: ECF Y570 + Fontes Pagadoras.
Soma retenções IRPJ/CSLL, valida divergências e calcula potencial de compensação.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pre_diagnostic_engine.utils.ecf_deduplicator import deduplicate_ecf_by_file_id

logger = logging.getLogger("dinamo.tax_intelligence.retention_analyzer")

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "retention_rules.yaml")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def _load_rules() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f).get("retention_rules", {})


class RetentionAnalyzer:
    """
    Analisa retenções na fonte de IRPJ e CSLL.
    Input:  DataFrame Y570 com [cnpj, ano_calendario, codigo_retencao, valor_retido, natureza]
    Output: DataFrame consolidado com totais e flags de divergência.
    """

    def __init__(self):
        self.rules = _load_rules()

    def analyze(self, df: DataFrame, anchor: DataFrame = None) -> DataFrame:
        if df is None:
            logger.warning("Y570 ausente — retornando retenções vazias (nulo)")
            return self._empty_result(anchor)
        df = self._consolidate_retentions(df)
        df = self._flag_divergencia(df)
        df = self._calc_potencial_compensacao(df)
        logger.info("RetentionAnalyzer concluído")
        return df

    def _empty_result(self, anchor: DataFrame) -> DataFrame:
        if anchor is None:
            raise ValueError(
                "RetentionAnalyzer: Y570 ausente e nenhum anchor (df_regime) fornecido "
                "para derivar CNPJ/DT_INI."
            )
        base = anchor.select("CNPJ", "DT_INI").distinct()
        return (
            base
            .withColumn("irpj_retido",            F.lit(None).cast("double"))
            .withColumn("csll_retido",             F.lit(None).cast("double"))
            .withColumn("total_retencoes",         F.lit(None).cast("double"))
            .withColumn("divergencia_retencao",    F.lit(None).cast("boolean"))
            .withColumn("potencial_compensacao",   F.lit(None).cast("double"))
        )

    def _consolidate_retentions(self, df: DataFrame) -> DataFrame:
        """Soma IRPJ e CSLL retidos por empresa/ano via Spark aggregation."""

        def _br_to_double(col_name: str):
            """
            Converte campo string no formato BR (ex: "4467,62") para double.
            O Y570 gold usa vírgula como decimal e NÃO usa ponto como separador de milhar.
            """
            return (
                F.regexp_replace(F.col(col_name), ',', '.')
                .cast("double")
            )

        logger.info(f"Colunas originais do Y570: {df.columns}")
        logger.info(f"Tipo de IR_RET:   {df.schema['IR_RET'].dataType}")
        logger.info(f"Tipo de CSLL_RET: {df.schema['CSLL_RET'].dataType}")

        # Deduplica declarações ECF (Original vs. Retificadora) antes de somar retenções,
        # evitando soma duplicada quando há regravações/múltiplos FILE_IDs no bucket.
        df = deduplicate_ecf_by_file_id(df)

        ir   = F.coalesce(_br_to_double("IR_RET"),   F.lit(0.0))
        csll = F.coalesce(_br_to_double("CSLL_RET"), F.lit(0.0))

        return (
            df.groupBy("CNPJ", "DT_INI")
            .agg(
                F.sum(ir).alias("irpj_retido"),
                F.sum(csll).alias("csll_retido"),
                F.sum(ir + csll).alias("total_retencoes"),
            )
        )

    def _flag_divergencia(self, df: DataFrame) -> DataFrame:
        """
        Marca divergência quando total_retencoes == 0 mas empresa deveria ter retenção.
        Tolerância configurável via YAML.
        """
        tolerance = self.rules.get("divergence_tolerance_percent", 5.0)
        return df.withColumn(
            "divergencia_retencao",
            F.col("total_retencoes") <= 0
        )

    def _calc_potencial_compensacao(self, df: DataFrame) -> DataFrame:
        """Potencial de compensação = total de retenções acumulado."""
        return df.withColumn(
            "potencial_compensacao",
            F.col("total_retencoes")
        )
