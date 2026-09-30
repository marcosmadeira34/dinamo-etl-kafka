# src/pre_diagnostic_engine/validators/lucro_validator.py
"""Valida colunas de lucro contábil e fiscal."""
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("dinamo.tax_intelligence.lucro_validator")

REQUIRED_COLS = [
    "CNPJ", "DT_INI",
    "resultado_contabil", "tem_lucro_contabil", "tem_prejuizo_contabil",
    "resultado_fiscal",   "tem_lucro_fiscal",   "tem_prejuizo_fiscal",
]


class LucroValidator:
    def validate(self, df: DataFrame) -> None:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"LucroValidator: colunas ausentes → {missing}")
        logger.info("LucroValidator: OK")