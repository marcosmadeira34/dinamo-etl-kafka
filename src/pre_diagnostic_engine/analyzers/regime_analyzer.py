# src/pre_diagnostic_engine/analyzers/regime_analyzer.py
"""
Analyzer de regime tributário.
Fonte: ECF Registro 0010 — campo FORMA_TRIB_PER
NUNCA usa collect() ou loops Python em volume.
"""
import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("dinamo.tax_intelligence.regime_analyzer")

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "regime_rules.yaml")


def _load_regime_rules() -> dict:
    with open(_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f).get("regime_rules", {})


class RegimeAnalyzer:

    def __init__(self):
        self.rules = _load_regime_rules()

    def analyze(self, df: DataFrame) -> DataFrame:
        df = self._map_regime(df)
        df = self._apply_regime_score(df)
        df = self._detect_alternancia(df)
        df = self._detect_sequential_presumido(df)
        df = self._flag_lucro_real_eligible(df)
        logger.info("RegimeAnalyzer concluído")
        return df

    def _map_regime(self, df: DataFrame) -> DataFrame:
        """
        CORREÇÃO: o regime_rules.yaml define code='RRRR' para Lucro Real,
        mas o _apply_regime_score e _flag_lucro_real_eligible usavam
        self.rules.get("lucro_real", {}).get("code", "R") — o default "R"
        nunca batia com "RRRR", zerando regime_score_pts e is_lucro_real.
        Agora lê o code diretamente do YAML sem fallback incorreto.
        """
        regime_map = {
            v["code"]: v["description"]
            for k, v in self.rules.items()
            if isinstance(v, dict) and "code" in v
        }
        mapping_expr = F.create_map(*[
            item
            for pair in [(F.lit(k), F.lit(v)) for k, v in regime_map.items()]
            for item in pair
        ])
        return df.withColumn("regime_descricao", mapping_expr[F.col("FORMA_TRIB_PER")])

    def _apply_regime_score(self, df: DataFrame) -> DataFrame:
        # Lê o code real do YAML — não usa fallback hardcoded
        lr_code = self.rules.get("lucro_real",     {}).get("code")
        lp_code = self.rules.get("lucro_presumido", {}).get("code")
        lr_pts  = self.rules.get("lucro_real",     {}).get("points", 30)
        lp_pts  = self.rules.get("lucro_presumido", {}).get("points", 10)

        expr = F.lit(0)
        if lr_code:
            expr = F.when(F.col("FORMA_TRIB_PER") == lr_code, F.lit(lr_pts)).otherwise(expr)
        if lp_code:
            expr = F.when(F.col("FORMA_TRIB_PER") == lp_code, F.lit(lp_pts)).otherwise(expr)

        return df.withColumn("regime_score_pts", expr)

    def _detect_alternancia(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("CNPJ").orderBy("DT_INI")
        return df.withColumn(
            "regime_alternado",
            F.col("FORMA_TRIB_PER") != F.lag("FORMA_TRIB_PER", 1).over(w)
        )

    def _detect_sequential_presumido(self, df: DataFrame) -> DataFrame:
        lp_code       = self.rules.get("lucro_presumido", {}).get("code")
        max_seq_years = self.rules.get("no_go_presumido_sequencial", {}).get("years", 5)
        w = Window.partitionBy("CNPJ").orderBy("DT_INI").rowsBetween(Window.unboundedPreceding, 0)

        df = df.withColumn(
            "is_lucro_presumido",
            F.col("FORMA_TRIB_PER") == lp_code if lp_code else F.lit(False)
        )
        df = df.withColumn("anos_consecutivos_lp",
            F.sum(F.col("is_lucro_presumido").cast("int")).over(w))
        df = df.withColumn("regime_no_go_presumido",
            F.col("anos_consecutivos_lp") >= max_seq_years)
        return df

    def _flag_lucro_real_eligible(self, df: DataFrame) -> DataFrame:
        lr_code = self.rules.get("lucro_real", {}).get("code")
        return df.withColumn(
            "is_lucro_real",
            F.col("FORMA_TRIB_PER") == lr_code if lr_code else F.lit(False)
        )

