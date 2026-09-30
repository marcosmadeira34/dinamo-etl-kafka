"""Valida colunas do RetentionAnalyzer."""
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("dinamo.tax_intelligence.retention_validator")

REQUIRED_COLS = ["CNPJ", "DT_INI", "total_retencoes",
                 "divergencia_retencao", "potencial_compensacao"]


class RetentionValidator:
    def validate(self, df: DataFrame) -> None:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"RetentionValidator: colunas ausentes → {missing}")
        logger.info("RetentionValidator: OK")