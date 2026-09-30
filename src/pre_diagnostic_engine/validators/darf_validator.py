
# src/pre_diagnostic_engine/validators/darf_validator.py
"""Valida colunas obrigatórias do DarfAnalyzer."""
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("dinamo.tax_intelligence.darf_validator")

REQUIRED_COLS = ["CNPJ", "DT_INI", "total_darf_pago", "ausencia_darf"]


class DarfValidator:
    def validate(self, df: DataFrame) -> None:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"DarfValidator: colunas ausentes → {missing}")
        logger.info("DarfValidator: OK")