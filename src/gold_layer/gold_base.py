# gold_builder/base_gold_builder.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from functools import reduce
from collections import defaultdict
import logging
import re
import unicodedata
from abc import ABC, abstractmethod



logger = logging.getLogger(__name__)

class BaseGoldBuilder(ABC):

    TECH_COLS = {
        "_parent_uid_final",
        "_row_id",
        "_file_id",
        "_sped_type",
        "REG"
    }

    def __init__(self, spark, base_path: str, ano: int):
        self.spark = spark
        self.base_path = base_path
        self.ano = ano
        self.df_gold = None
        self._enable_optimizations()

    # -----------------------------------------  
    # Spark optimizations 
    # -----------------------------------------

    def _enable_optimizations(self):
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.sql.shuffle.partitions", self.spark.sparkContext.defaultParallelism * 2)
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    


    # -----------------------------------------
    # Write GOLD (sem heurística)
    # -----------------------------------------
    def write_gold(self, output_path: str):
        if self.df_gold is None:
            logger.warning("df_gold não gerado – GOLD ignorada")
            return None

        self._validate_no_duplicate_columns(self.df_gold)

        (
            self.df_gold
            .transform(self._normalize_column_names)
            .write
            .mode("overwrite")
            .partitionBy("CNPJ")
            .parquet(output_path)
        )

        logger.info(f"GOLD escrita com sucesso em {output_path}")
        return self

    def _validate_no_duplicate_columns(self, df: DataFrame):
        lower = [c.lower() for c in df.columns]
        dups = {c for c in lower if lower.count(c) > 1}

        if dups:
            raise RuntimeError(
                f"Contrato GOLD violado – colunas duplicadas: {dups}"
            )

            

    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        for c in df.columns:
            new = (
                unicodedata.normalize("NFKD", c)
                .encode("ASCII", "ignore")
                .decode("ASCII")
                .upper()
                .replace(" ", "_")
                .replace("/", "_")
            )
            if new != c:
                df = df.withColumnRenamed(c, new)
        return df

    # -----------------------------------------
    # CONTRACT
    # -----------------------------------------
    @abstractmethod
    def build(self):
        ...

    @abstractmethod
    def sanity_checks(self):
        ...