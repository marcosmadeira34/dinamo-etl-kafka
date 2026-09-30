
# src/pre_diagnostic_engine/consolidators/opportunity_consolidator.py
"""
Consolida oportunidades L300 e M310 num único DataFrame.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.opportunity_consolidator")


class OpportunityConsolidator:
    """
    Junta OpportunityL300Analyzer e OpportunityM310Analyzer.
    """

    def consolidate(
        self,
        df_l300: DataFrame,
        df_m310: DataFrame,
    ) -> DataFrame:
        df = df_l300.join(df_m310, on=["CNPJ", "DT_INI"], how="outer")
        df = self._fill_nulls(df)
        df = self._calc_total_opportunity_score(df)
        logger.info("OpportunityConsolidator concluído")
        return df

    def _fill_nulls(self, df: DataFrame) -> DataFrame:
        score_cols = [c for c in df.columns if c.endswith("_score") or c.endswith("_valor")]
        flag_cols  = [c for c in df.columns if c.endswith("_encontrado")]
        fill_num   = {c: 0.0   for c in score_cols if c in df.columns}
        fill_bool  = {c: False for c in flag_cols   if c in df.columns}
        return df.fillna({**fill_num, **fill_bool})

    def _calc_total_opportunity_score(self, df: DataFrame) -> DataFrame:
        l300 = F.coalesce(F.col("score_l300_total").cast("double"), F.lit(0.0)) if "score_l300_total" in df.columns else F.lit(0.0)
        m310 = F.coalesce(F.col("score_m310_total").cast("double"), F.lit(0.0)) if "score_m310_total" in df.columns else F.lit(0.0)
        return df.withColumn("total_opportunity_score", l300 + m310)