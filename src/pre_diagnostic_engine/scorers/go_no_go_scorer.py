# src/pre_diagnostic_engine/scorers/go_no_go_scorer.py
"""
Scorer de GO/NO-GO.
Aplica pesos do scoring_rules.yaml sobre as colunas de análise
e produz: raw_score, final_score, classification.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.go_no_go_scorer")

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "scoring_rules.yaml")


def _load_scoring_rules() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


class GoNoGoScorer:
    """
    Calcula o score final de GO/NO-GO com base nos pesos YAML.
    Input:  DataFrame consolidado com todas as colunas de análise.
    Output: DataFrame com raw_score, final_score, classification adicionados.
    """

    def __init__(self):
        cfg = _load_scoring_rules()
        self.weights = cfg.get("weights", {})
        self.classification_bands = cfg.get("classification", {})

    def score(self, df: DataFrame) -> DataFrame:
        df = self._calc_raw_score(df)
        df = self._cap_score(df)
        df = self._classify(df)
        logger.info("GoNoGoScorer concluído")
        return df

    def _calc_raw_score(self, df: DataFrame) -> DataFrame:
        """
        Soma ponderada via expressões Spark (sem collect).
        Cada componente só contribui se a coluna existir no DataFrame.
        """
        cols = set(df.columns)

        def pts(key: str) -> int:
            return self.weights.get(key, {}).get("points", 0)

        score_expr = F.lit(0.0)

        if "is_lucro_real" in cols:
            score_expr = score_expr + F.when(F.col("is_lucro_real"), F.lit(float(pts("lucro_real")))).otherwise(F.lit(0.0))

        if "pl_positivo" in cols:
            score_expr = score_expr + F.when(F.col("pl_positivo"), F.lit(float(pts("patrimonio_positivo")))).otherwise(F.lit(0.0))

        if "tem_lucro_fiscal" in cols:
            score_expr = score_expr + F.when(F.col("tem_lucro_fiscal"), F.lit(float(pts("lucro_fiscal")))).otherwise(F.lit(0.0))

        if "total_retencoes" in cols:
            score_expr = score_expr + F.when(F.col("total_retencoes") > 0, F.lit(float(pts("retencoes")))).otherwise(F.lit(0.0))

        if "ausencia_darf" in cols:
            score_expr = score_expr + F.when(~F.col("ausencia_darf"), F.lit(float(pts("darf_pago")))).otherwise(F.lit(0.0))

        if "score_l300_total" in cols:
            score_expr = score_expr + F.coalesce(F.col("score_l300_total").cast("double"), F.lit(0.0))

        if "score_m310_total" in cols:
            score_expr = score_expr + F.coalesce(F.col("score_m310_total").cast("double"), F.lit(0.0))

        # Penalidades
        if "recorrencia_prejuizo_fiscal_anos" in cols:
            score_expr = score_expr + F.when(
                F.col("recorrencia_prejuizo_fiscal_anos") >= 2,
                F.lit(float(pts("prejuizo_recorrente")))
            ).otherwise(F.lit(0.0))

        if "ausencia_darf" in cols:
            score_expr = score_expr + F.when(
                F.col("ausencia_darf"),
                F.lit(float(pts("ausencia_darf")))
            ).otherwise(F.lit(0.0))

        return df.withColumn("raw_score", score_expr)

    def _cap_score(self, df: DataFrame) -> DataFrame:
        """Garante score entre 0 e 100."""
        return df.withColumn(
            "final_score",
            F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.col("raw_score")))
        )

    def _classify(self, df: DataFrame) -> DataFrame:
        """
        Mapeia final_score para classificação via bandas YAML.
        """
        bands = self.classification_bands
        return df.withColumn(
            "classification",
            F.when(F.col("final_score") >= bands.get("oportunidade_altissima", {}).get("min", 80), F.lit("OPORTUNIDADE_ALTISSIMA"))
             .when(F.col("final_score") >= bands.get("oportunidade_forte", {}).get("min", 60), F.lit("OPORTUNIDADE_FORTE"))
             .when(F.col("final_score") >= bands.get("oportunidade_moderada", {}).get("min", 40), F.lit("OPORTUNIDADE_MODERADA"))
             .when(F.col("final_score") >= bands.get("baixa_atratividade", {}).get("min", 20), F.lit("BAIXA_ATRATIVIDADE"))
             .otherwise(F.lit("NO_GO"))
        )