# src/pre_diagnostic_engine/validators/fiscal_result_validator.py
"""Valida o DataFrame final antes de gravar na Gold Layer."""
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("dinamo.tax_intelligence.fiscal_result_validator")

REQUIRED_COLS = [
    "CNPJ", "DT_INI", "cnpj_base",
    "final_score", "classification", "go_no_go", "commercial_priority",
]


class FiscalResultValidator:
    def validate(self, df: DataFrame) -> None:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"FiscalResultValidator: colunas ausentes → {missing}")
        logger.info("FiscalResultValidator: DataFrame final validado — OK")