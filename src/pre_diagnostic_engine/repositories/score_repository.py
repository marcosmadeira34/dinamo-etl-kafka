# src/pre_diagnostic_engine/repositories/score_repository.py
"""
Repositório especializado para leitura/escrita de scores e classificações.
"""
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from .delta_repository import DeltaRepository

logger = logging.getLogger("dinamo.tax_intelligence.score_repository")


class ScoreRepository:

    def __init__(self, spark: SparkSession, gold_base_path: str):
        self.spark = spark
        self.delta = DeltaRepository(spark, gold_base_path)

    def read_scores(
        self,
        cnpj_base: Optional[str] = None,
        ano_calendario: Optional[int] = None,
    ) -> DataFrame:
        return self.delta.read_filtered(
            "gold_pre_diagnostic_scores", cnpj_base, ano_calendario
        )

    def upsert_scores(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_scores")
        self.delta.upsert(
            df,
            "gold_pre_diagnostic_scores",
            merge_keys=["CNPJ", "DT_INI"],
            partition_cols=["DT_INI", "CNPJ"],
        )
