# src/pre_diagnostic_engine/analyzers/opportunity_m310_analyzer.py
"""
Analyzer de Oportunidades M310 (LALUR — adições/exclusões).
Identifica Subvenções, Art.30, Crédito IRPJ, JCP Dedutível via YAML.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pre_diagnostic_engine.utils.ecf_deduplicator import deduplicate_ecf_by_file_id

logger = logging.getLogger("dinamo.tax_intelligence.opportunity_m310_analyzer")

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "m310_codes_map.yaml")

# A Gold Layer usa códigos de período acumulados mensais (A00..A12), onde
# A00 = fechamento do exercício. "T04" nunca existiu nesse schema real —
# zerava todas as oportunidades M310 sempre.
PER_APURACAO = "A00"




def _load_m310_rules() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f).get("m310_opportunities", {})


class OpportunityM310Analyzer:
    """
    Identifica oportunidades tributárias mapeadas no M310 (LALUR).
    Input:  DataFrame M310 com [cnpj, ano_calendario, COD_PART, VL_PART]
    Output: DataFrame com flags e valores por oportunidade.
    """

    def __init__(self):
        self.rules = _load_m310_rules()

    def analyze(self, df: DataFrame, anchor: DataFrame = None) -> DataFrame:
        """
        Para cada código YAML, cria colunas de flag/valor via Spark aggregations.
        """
        if df is None:
            logger.warning("M310 ausente — retornando oportunidades M310 vazias (nulo)")
            return self._empty_result(anchor)

        # Deduplica declarações ECF (Original vs. Retificadora) antes de somar valores
        df = deduplicate_ecf_by_file_id(df)

        # Coleta códigos (broadcast-safe)
        opportunity_codes = {
            key: rule.get("code")
            for key, rule in self.rules.items()
            if rule.get("code")
        }

        agg_exprs = []
        for opp_key, code in opportunity_codes.items():
            col_flag  = f"opp_m310_{opp_key}_encontrado"
            col_valor = f"opp_m310_{opp_key}_valor"
            score_pts = self.rules.get(opp_key, {}).get("score", 0)
            col_score = f"opp_m310_{opp_key}_score"

            valor_col = F.regexp_replace(F.regexp_replace(F.col("VL_CTA"), r"\.", ""), ",", ".").cast("double")

            # Código pode ter prefixo — busca startswith via contains/like
            agg_exprs += [
                F.max(F.when((F.col("CODIGO") == code) & (F.col("PER_APUR") == PER_APURACAO), F.lit(True)).otherwise(F.lit(False))).alias(col_flag),
                F.sum(F.when((F.col("CODIGO") == code) & (F.col("PER_APUR") == PER_APURACAO), valor_col).otherwise(F.lit(0.0))).alias(col_valor),
                F.max(F.when((F.col("CODIGO") == code) & (F.col("PER_APUR") == PER_APURACAO), F.lit(score_pts)).otherwise(F.lit(0))).alias(col_score),
            ]

        df_agg = df.groupBy("CNPJ", "DT_INI").agg(*agg_exprs)

        # Score total M310
        df_agg = df_agg.withColumn(
            "score_m310_total",
            sum(
                F.when(F.col(f"opp_m310_{k}_encontrado"), F.col(f"opp_m310_{k}_score")).otherwise(F.lit(0))
                for k in opportunity_codes
            )
        )

        logger.info(f"OpportunityM310Analyzer concluído — {len(opportunity_codes)} oportunidades verificadas")
        return df_agg

    def _empty_result(self, anchor: DataFrame) -> DataFrame:
        if anchor is None:
            raise ValueError(
                "OpportunityM310Analyzer: M310 ausente e nenhum anchor (df_regime) "
                "fornecido para derivar CNPJ/DT_INI."
            )
        opportunity_codes = {
            key: rule.get("code")
            for key, rule in self.rules.items()
            if rule.get("code")
        }
        df_agg = anchor.select("CNPJ", "DT_INI").distinct()
        for opp_key in opportunity_codes:
            df_agg = (
                df_agg
                .withColumn(f"opp_m310_{opp_key}_encontrado", F.lit(None).cast("boolean"))
                .withColumn(f"opp_m310_{opp_key}_valor",       F.lit(None).cast("double"))
                .withColumn(f"opp_m310_{opp_key}_score",       F.lit(None).cast("int"))
            )
        df_agg = df_agg.withColumn("score_m310_total", F.lit(None).cast("double"))
        return df_agg





