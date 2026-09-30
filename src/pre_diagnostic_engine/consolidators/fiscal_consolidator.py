# src/pre_diagnostic_engine/consolidators/fiscal_consolidator.py
"""
Consolida resultados dos analyzers Fiscal, Contábil e Patrimônio
num único DataFrame por (cnpj, ano_calendario).
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.fiscal_consolidator")


class FiscalConsolidator:
    """
    Une os resultados de:
    - ContabilAnalyzer   → resultado contábil, lucro/prejuízo, recorrência
    - FiscalAnalyzer     → resultado fiscal, base tributável, recorrência
    - PatrimonioAnalyzer → PL, evolução, capacidade JCP
    """

    def consolidate(
        self,
        df_contabil: DataFrame,
        df_fiscal: DataFrame,
        df_patrimonio: DataFrame,
    ) -> DataFrame:
        df = df_contabil.join(df_fiscal,   on=["CNPJ", "DT_INI"], how="outer")
        df = df.join(df_patrimonio,         on=["CNPJ", "DT_INI"], how="outer")
        df = self._fill_nulls(df)
        logger.info("FiscalConsolidator concluído")
        return df

    def _fill_nulls(self, df: DataFrame) -> DataFrame:
        numeric_cols = [
            "resultado_contabil", "lucro_contabil", "prejuizo_contabil",
            "resultado_fiscal",   "lucro_fiscal",   "prejuizo_fiscal",
            "base_tributavel",    "patrimonio_liquido", "capacidade_jcp_estimada",
            "evolucao_patrimonial_pct",
            "recorrencia_lucro_anos", "recorrencia_prejuizo_anos",
            "recorrencia_lucro_fiscal_anos", "recorrencia_prejuizo_fiscal_anos",
        ]
        bool_cols = [
            "tem_lucro_contabil", "tem_prejuizo_contabil",
            "tem_lucro_fiscal",   "tem_prejuizo_fiscal", "pl_positivo",
        ]
        fill_numeric = {c: 0.0 for c in numeric_cols if c in df.columns}
        fill_bool    = {c: False for c in bool_cols if c in df.columns}
        return df.fillna({**fill_numeric, **fill_bool})

