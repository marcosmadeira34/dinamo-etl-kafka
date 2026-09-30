# src/pre_diagnostic_engine/scorers/viability_score.py
"""
Scorer de Viabilidade Comercial.
Combina go_no_go + final_score + estimated_recovery para gerar
o índice final de atratividade comercial.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.viability_score")


class ViabilityScorer:
    """
    Calcula estimated_recovery e commercial_viability_index.
    Input:  DataFrame com final_score, go_no_go, oportunidades scored.
    Output: DataFrame com estimated_recovery e commercial_viability_index.
    """

    def score(self, df: DataFrame, df_opportunities: DataFrame) -> DataFrame:
        """
        Junta totais de oportunidades (estimated_credit) ao DataFrame principal
        e calcula o índice de viabilidade.
        """
        df = self._join_estimated_recovery(df, df_opportunities)
        df = self._calc_viability_index(df)
        logger.info("ViabilityScorer concluído")
        return df

    def _join_estimated_recovery(self, df: DataFrame, df_opp: DataFrame) -> DataFrame:
        """
        Agrega total de crédito estimado por CNPJ/ano e faz join ao principal.
        """
        df_recovery = (
            df_opp
            .groupBy("CNPJ", "DT_INI")
            .agg(F.sum("estimated_credit").alias("estimated_recovery"))
        )
        return df.join(df_recovery, on=["CNPJ", "DT_INI"], how="left").fillna(
            {"estimated_recovery": 0.0}
        )

    def _calc_viability_index(self, df: DataFrame) -> DataFrame:
        """
        Índice composto: 60% score + 40% normalizado pelo recovery.
        Recovery_score = min(estimated_recovery / 100_000, 40.0)
        — cap em 40 pontos para não dominar o score de qualidade.
        """
        recovery_score = F.least(
            F.col("estimated_recovery") / F.lit(100_000.0) * F.lit(40.0),
            F.lit(40.0)
        )
        return df.withColumn(
            "commercial_viability_index",
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.lit(100.0),
                    F.col("final_score") * F.lit(0.6) + recovery_score * F.lit(0.4)
                )
            )
        )