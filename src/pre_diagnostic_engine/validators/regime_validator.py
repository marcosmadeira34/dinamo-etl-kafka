# src/pre_diagnostic_engine/validators/regime_validator.py
"""Valida colunas obrigatórias do RegimeAnalyzer."""
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("dinamo.tax_intelligence.regime_validator")

REQUIRED_COLS = ["CNPJ", "DT_INI", "FORMA_TRIB_PER",
                 "is_lucro_real", "regime_no_go_presumido", "anos_consecutivos_lp"]


class RegimeValidator:
    def validate(self, df: DataFrame) -> None:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"RegimeValidator: colunas ausentes → {missing}")
        logger.info("RegimeValidator: OK")





