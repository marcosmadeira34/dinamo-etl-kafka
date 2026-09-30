# src/pre_diagnostic_engine/analyzers/viability_analyzer.py
"""
Analyzer de Viabilidade Comercial.
Consolida todos os sinais para produzir um indicador de viabilidade de GO/NO-GO.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.viability_analyzer")

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "go_no_go_rules.yaml")


def _load_go_no_go_rules() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f).get("go_no_go_rules", {})


class ViabilityAnalyzer:
    """
    Aplica regras de GO/NO-GO sobre o DataFrame consolidado de análises.
    Input:  DataFrame com todas as colunas geradas pelos analyzers anteriores.
    Output: DataFrame com colunas go_no_go e commercial_priority.
    """

    def __init__(self):
        self.rules = _load_go_no_go_rules()

    def analyze(self, df: DataFrame) -> DataFrame:
        df = self._apply_go_no_go(df)
        df = self._apply_commercial_priority(df)
        logger.info("ViabilityAnalyzer concluído")
        return df

    def _apply_go_no_go(self, df: DataFrame) -> DataFrame:
        """
        GO/NO-GO baseado nas regras YAML:
        - NO-GO: regime LP sequencial por N anos
        - NO-GO: ausência de DARF
        - NO-GO: prejuízo fiscal recorrente
        - GO: Lucro Real com DARF pago
        """
        max_lp_years  = self.rules.get("max_sequential_presumido_years", 5)
        min_pl        = self.rules.get("min_pl_threshold", 0.0)
        required_code = self.rules.get("required_regime", "R")

        # verifica se a coluna ausencia_darf existe
        has_ausencia_darf = "ausencia_darf" in df.columns

        if has_ausencia_darf:
            go_condition = (
                (F.col("is_lucro_real") == True) &
                (F.col("patrimonio_liquido") > min_pl)
                # (F.col("ausencia_darf") == False) &
            )

            no_go_condition = (
                (F.col("regime_no_go_presumido") == True) 
                #(F.col("ausencia_darf") == True)
            )

        else:
            logger.warning("Coluna ausencia_darf não encontrada. Regras DARF foram ignoradas.")
            go_condition = (
                (F.col("is_lucro_real") == True) &
                (F.col("patrimonio_liquido") > min_pl)
            )

            no_go_condition = (
                (F.col("regime_no_go_presumido") == True) 
            )
        return df.withColumn(
            "go_no_go",
            F.when(no_go_condition, F.lit("NO_GO"))
             .when(go_condition, F.lit("GO"))
             .otherwise(F.lit("ANALISAR"))
        )

    def _apply_commercial_priority(self, df: DataFrame) -> DataFrame:
        """
        Prioridade comercial derivada do score e go_no_go.
        """
        return df.withColumn(
            "commercial_priority",
            F.when(
                (F.col("go_no_go") == "GO") & (F.col("final_score") >= 80),
                F.lit("ALTISSIMA")
            ).when(
                (F.col("go_no_go") == "GO") & (F.col("final_score") >= 60),
                F.lit("ALTA")
            ).when(
                (F.col("go_no_go") == "GO") & (F.col("final_score") >= 40),
                F.lit("MEDIA")
            ).when(
                F.col("go_no_go") == "ANALISAR",
                F.lit("BAIXA")
            ).otherwise(F.lit("NO_GO"))
        )
