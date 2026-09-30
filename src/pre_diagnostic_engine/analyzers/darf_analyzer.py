# src/pre_diagnostic_engine/analyzers/darf_analyzer.py
"""
Analyzer de DARFs.
Consolida pagamentos, identifica perfil tributário e calcula atratividade.
Usa códigos de receita definidos em config/darf_codes_map.yaml.
"""
from pre_diagnostic_engine.extractors import darf_extractor
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DARF ANALYZER")

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "darf_codes_map.yaml")


def _load_darf_codes() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f).get("darf_codes", {})


class DarfAnalyzer:
    """
    Analisa pagamentos DARF (IRPJ/CSLL) por regime tributário.
    Input:  DataFrame com [cnpj, dt_ini periodo_apuracao, codigo_receita, valor_principal]
    Output: DataFrame com totais e perfil tributário identificado.
    """

    def __init__(self):
        self.codes = _load_darf_codes()

    def analyze(self, df: DataFrame) -> DataFrame:
        if df is None:
            logger.warning("Nenhum DARF encontrado")
            return None

        df = self._cast_values(df)
        df = self._identify_regime_darf(df)
        df = self._consolidate_payments(df)
        df = self._flag_ausencia_darf(df)
        df = self._calc_recorrencia(df)
        logger.info("DarfAnalyzer concluído")
        return df

    def _cast_values(self, df: DataFrame) -> DataFrame:
        return df.withColumn("valor_principal", F.col("valor_principal").cast("double"))

    def _identify_regime_darf(self, df: DataFrame) -> DataFrame:
        """
        Classifica cada DARF no regime correspondente via YAML codes.
        Usa broadcast-friendly column expression (sem UDF).
        """
        irpj_lr_mensal   = self.codes.get("lucro_real_mensal", {}).get("irpj", [])
        irpj_lr_trim     = self.codes.get("lucro_real_trimestral", {}).get("irpj", [])
        irpj_lp          = self.codes.get("lucro_presumido", {}).get("irpj", [])
        irpj_ajuste      = self.codes.get("ajuste_anual", {}).get("irpj", [])
        csll_lr_mensal   = self.codes.get("lucro_real_mensal", {}).get("csll", [])
        csll_lr_trim     = self.codes.get("lucro_real_trimestral", {}).get("csll", [])
        csll_lp          = self.codes.get("lucro_presumido", {}).get("csll", [])

        all_irpj_lr = irpj_lr_mensal + irpj_lr_trim + irpj_ajuste
        all_csll_lr = csll_lr_mensal + csll_lr_trim
        all_irpj_lp = irpj_lp
        all_csll_lp = csll_lp

        return (
            df.withColumn("is_irpj_lucro_real",   F.col("codigo_receita").isin(all_irpj_lr))
              .withColumn("is_csll_lucro_real",    F.col("codigo_receita").isin(all_csll_lr))
              .withColumn("is_irpj_lucro_presumido", F.col("codigo_receita").isin(all_irpj_lp))
              .withColumn("is_csll_lucro_presumido", F.col("codigo_receita").isin(all_csll_lp))
        )

    def _consolidate_payments(self, df: DataFrame) -> DataFrame:
        """Consolida valores pagos por empresa/ano e tipo."""
        return (
            df.groupBy("CNPJ", "DT_INI")
              .agg(
                  F.sum(F.when(F.col("is_irpj_lucro_real"),   F.col("valor_principal")).otherwise(0.0)).alias("irpj_lr_pago"),
                  F.sum(F.when(F.col("is_csll_lucro_real"),    F.col("valor_principal")).otherwise(0.0)).alias("csll_lr_pago"),
                  F.sum(F.when(F.col("is_irpj_lucro_presumido"), F.col("valor_principal")).otherwise(0.0)).alias("irpj_lp_pago"),
                  F.sum(F.when(F.col("is_csll_lucro_presumido"), F.col("valor_principal")).otherwise(0.0)).alias("csll_lp_pago"),
                  F.sum(F.col("valor_principal")).alias("total_darf_pago"),
                  F.countDistinct("codigo_receita").alias("qtd_codigos_receita"),
              )
        )

    def _flag_ausencia_darf(self, df: DataFrame) -> DataFrame:
        return df.withColumn("ausencia_darf", F.col("total_darf_pago") <= 0)

    def _calc_recorrencia(self, df: DataFrame) -> DataFrame:
        """Conta anos com pagamento DARF via Window."""
        w = Window.partitionBy("CNPJ").orderBy("DT_INI").rowsBetween(Window.unboundedPreceding, 0)
        return df.withColumn(
            "anos_com_darf",
            F.sum((F.col("total_darf_pago") > 0).cast("int")).over(w)
        )
