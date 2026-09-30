# src/pre_diagnostic_engine/analyzers/opportunity_l300_analyzer.py
"""
Analyzer de Oportunidades L300 (DRE ECF).
Identifica PAT, Receita JCP e Despesa JCP a partir de referencias YAML.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pre_diagnostic_engine.utils.ecf_deduplicator import deduplicate_ecf_by_file_id

logger = logging.getLogger("dinamo.tax_intelligence.opportunity_l300_analyzer")
logger.setLevel(logging.INFO)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "l300_reference_map.yaml")
# A Gold Layer usa códigos de período acumulados mensais (A00..A12), onde A00 =
# fechamento do exercício (mesmo valor total do ano já contido em A12 — somar
# os dois duplica o valor). "T04" nunca existiu nesse schema real. Mesma
# convenção usada em contabil_analyzer.py e patrimonio_analyzer.py.
PER_APURACAO_VALIDO = "A00"


def _load_l300_rules() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f).get("l300_opportunities", {})


class OpportunityL300Analyzer:
    """
    Identifica oportunidades tributárias mapeadas no L300 (DRE).
    Input:  DataFrame L300 com [cnpj, ano_calendario, CODIGO, VALOR]
    Output: DataFrame com colunas indicando cada oportunidade encontrada.
    """

    def __init__(self):
        self.rules = _load_l300_rules()

    def analyze(self, df: DataFrame, anchor: DataFrame = None) -> DataFrame:
        """
        Para cada regra YAML, cria coluna de flag e valor encontrado.
        Retorna um DataFrame agregado por cnpj/ano_calendario.
        """
        if df is None:
            logger.warning("L300 ausente — retornando oportunidades L300 vazias (nulo)")
            return self._empty_result(anchor)

        # Deduplica declarações ECF (Original vs. Retificadora) antes de somar valores
        df = deduplicate_ecf_by_file_id(df)

        # Coleta referencias de cada oportunidade (broadcast-safe: pequeno dict)
        opportunity_refs = {
            key: rule.get("reference")
            for key, rule in self.rules.items()
            if rule.get("reference")
        }

        # Construção de colunas de flag via SQL expressions (sem UDF)
        agg_exprs = []
        flag_cols = []
        for opp_key, reference in opportunity_refs.items():
            col_flag  = f"opp_l300_{opp_key}_encontrado"
            col_valor = f"opp_l300_{opp_key}_valor"
            score_pts = self.rules.get(opp_key, {}).get("score", 0)
            col_score = f"opp_l300_{opp_key}_score"

            valor_col = F.regexp_replace(F.regexp_replace(F.col("VALOR"), r"\.", ""), ",", ".").cast("double")

            condicao_periodo = (F.col("CODIGO") == reference) & (F.col("PER_APUR") == PER_APURACAO_VALIDO)

            agg_exprs += [
                F.max(F.when(F.col("CODIGO") == reference, F.lit(True)).otherwise(F.lit(False))).alias(col_flag),
                F.sum(F.when(condicao_periodo, valor_col).otherwise(F.lit(0.0))).alias(col_valor),
                F.max(F.when(F.col("CODIGO") == reference, F.lit(score_pts)).otherwise(F.lit(0))).alias(col_score),
            ]
            flag_cols.append(col_flag)

        df_agg = df.groupBy("CNPJ", "DT_INI").agg(*agg_exprs)

        # Score total L300 = soma dos scores de oportunidades encontradas
        df_agg = df_agg.withColumn(
            "score_l300_total",
            sum(
                F.when(F.col(f"opp_l300_{k}_encontrado"), F.col(f"opp_l300_{k}_score")).otherwise(F.lit(0))
                for k in opportunity_refs
            )
        )

        logger.info(f"OpportunityL300Analyzer concluído — {len(opportunity_refs)} oportunidades verificadas")
        return df_agg

    def _empty_result(self, anchor: DataFrame) -> DataFrame:
        if anchor is None:
            raise ValueError(
                "OpportunityL300Analyzer: L300 ausente e nenhum anchor (df_regime) "
                "fornecido para derivar CNPJ/DT_INI."
            )
        opportunity_refs = {
            key: rule.get("reference")
            for key, rule in self.rules.items()
            if rule.get("reference")
        }
        df_agg = anchor.select("CNPJ", "DT_INI").distinct()
        for opp_key in opportunity_refs:
            df_agg = (
                df_agg
                .withColumn(f"opp_l300_{opp_key}_encontrado", F.lit(None).cast("boolean"))
                .withColumn(f"opp_l300_{opp_key}_valor",       F.lit(None).cast("double"))
                .withColumn(f"opp_l300_{opp_key}_score",       F.lit(None).cast("int"))
            )
        df_agg = df_agg.withColumn("score_l300_total", F.lit(None).cast("double"))
        return df_agg
