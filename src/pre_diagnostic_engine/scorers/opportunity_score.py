# src/pre_diagnostic_engine/scorers/opportunity_score.py
"""
Scorer de Oportunidades.
Explode as oportunidades encontradas (L300 + M310) em linhas individuais
com estimated_credit e score_impact.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.opportunity_score")

_L300_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "l300_reference_map.yaml")
_M310_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "m310_codes_map.yaml")


def _load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


class OpportunityScorer:
    """
    Pontua oportunidades individuais identificadas pelos analyzers L300/M310.
    Input:  DataFrame consolidado com colunas opp_l300_* e opp_m310_*
    Output: DataFrame no formato opportunity_schema (uma linha por oportunidade).
    """

    def __init__(self):
        self.l300_rules = _load_yaml(_L300_PATH).get("l300_opportunities", {})
        self.m310_rules = _load_yaml(_M310_PATH).get("m310_opportunities", {})

    def build_opportunity_rows(self, df: DataFrame) -> DataFrame:
        """
        Gera um DataFrame no schema gold_pre_diagnostic_opportunities,
        uma linha por (CNPJ, DT_INI, opportunity_id).
        Usa stack/unpivot via SQL expression para evitar collect.
        """
        cols = set(df.columns)
        union_dfs = []

        # ── L300 ─────────────────────────────────────────────────────────────
        for key, rule in self.l300_rules.items():
            flag_col  = f"opp_l300_{key}_encontrado"
            valor_col = f"opp_l300_{key}_valor"
            if flag_col not in cols:
                continue
            credit_pct = rule.get("estimated_credit_percent", 0.0)
            score_imp  = float(rule.get("score", 0))
            desc       = rule.get("description", key)
            ref        = rule.get("reference", "")
            opp_id     = f"L300_{key.upper()}"

            valor_expr = F.col(valor_col).cast("double") if valor_col in cols else F.lit(0.0)
            credit_expr = F.abs(valor_expr) * F.lit(credit_pct) if credit_pct > 0 else F.lit(0.0)

            part = (
                df.filter(F.col(flag_col) == True)
                  .select(
                      F.col("CNPJ"),
                      F.col("DT_INI"),
                      F.lit(opp_id).alias("opportunity_id"),
                      F.lit(desc).alias("description"),
                      F.lit(ref).alias("reference_code"),
                      credit_expr.alias("estimated_credit"),
                      F.lit(score_imp).alias("score_impact"),
                  )
            )
            union_dfs.append(part)

        # ── M310 ─────────────────────────────────────────────────────────────
        for key, rule in self.m310_rules.items():
            flag_col  = f"opp_m310_{key}_encontrado"
            valor_col = f"opp_m310_{key}_valor"
            if flag_col not in cols:
                continue
            score_imp = float(rule.get("score", 0))
            code      = rule.get("code", "")
            opp_id    = f"M310_{key.upper()}"

            valor_expr = F.col(valor_col).cast("double") if valor_col in cols else F.lit(0.0)

            part = (
                df.filter(F.col(flag_col) == True)
                  .select(
                      F.col("CNPJ"),
                      F.col("DT_INI"),
                      F.lit(opp_id).alias("opportunity_id"),
                      F.lit(key).alias("description"),
                      F.lit(code).alias("reference_code"),
                      F.abs(valor_expr).alias("estimated_credit"),
                      F.lit(score_imp).alias("score_impact"),
                  )
            )
            union_dfs.append(part)

        if not union_dfs:
            logger.warning("Nenhuma oportunidade encontrada para pontuar")
            return df.sparkSession.createDataFrame(
                [],
                schema="CNPJ STRING, DT_INI INT, opportunity_id STRING, description STRING, reference_code STRING, estimated_credit DOUBLE, score_impact DOUBLE"
            )

        result = union_dfs[0]
        for other in union_dfs[1:]:
            result = result.unionByName(other)

        logger.info(f"OpportunityScorer: {len(union_dfs)} oportunidades geradas")
        return result