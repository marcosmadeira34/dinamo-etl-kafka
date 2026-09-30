# src/pre_diagnostic_engine/consolidators/financial_consolidator.py
"""
Consolida resultados financeiros: DARF + Retenções.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.financial_consolidator")


class FinancialConsolidator:
    """
    Une DarfAnalyzer e RetentionAnalyzer num único DataFrame.
    """

    def consolidate(
        self,
        df_darf: DataFrame = None,
        df_retention: DataFrame = None
    ) -> DataFrame:

        # nenhum dataframe disponível
        if df_darf is None and df_retention is None:
            logger.warning("Nenhum dataframe financeiro disponível")
            return None

        # apenas retenções
        if df_darf is None:
            logger.info("Consolidação apenas com retenções")
            df = df_retention

        # apenas DARFs
        elif df_retention is None:
            logger.info("Consolidação apenas com DARFs")
            df = df_darf

        # ambos disponíveis
        else:
            logger.info("Consolidação DARF + retenções")
            df = df_darf.join(
                df_retention,
                on=["CNPJ", "DT_INI"],
                how="outer"
            )

        df = self._fill_nulls(df)

        logger.info("FinancialConsolidator concluído")

        return df

    def _fill_nulls(self, df: DataFrame) -> DataFrame:

        numeric_cols = [
            "irpj_lr_pago",
            "csll_lr_pago",
            "irpj_lp_pago",
            "csll_lp_pago",
            "total_darf_pago",
            "irpj_retido",
            "csll_retido",
            "total_retencoes",
            "potencial_compensacao",
        ]

        bool_cols = [
            "ausencia_darf",
            "divergencia_retencao"
        ]

        fill_num = {
            c: 0.0
            for c in numeric_cols
            if c in df.columns
        }

        fill_bool = {
            c: False
            for c in bool_cols
            if c in df.columns
        }

        return df.fillna({
            **fill_num,
            **fill_bool
        })



