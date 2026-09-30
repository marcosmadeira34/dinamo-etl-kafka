# src/pre_diagnostic_engine/validators/patrimonio_validator.py
"""Valida colunas do PatrimonioAnalyzer."""
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("dinamo.tax_intelligence.patrimonio_validator")

REQUIRED_COLS = ["CNPJ", "DT_INI", "patrimonio_liquido", "pl_positivo"]


class PatrimonioValidator:
    def validate(self, df: DataFrame) -> None:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"PatrimonioValidator: colunas ausentes → {missing}")
        logger.info("PatrimonioValidator: OK")