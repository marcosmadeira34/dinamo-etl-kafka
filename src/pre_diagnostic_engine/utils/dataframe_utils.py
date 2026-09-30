# src/pre_diagnostic_engine/utils/dataframe_utils.py
"""Utilitários genéricos para DataFrames Spark."""
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def safe_join(left: DataFrame, right: DataFrame, on: List[str], how: str = "left") -> DataFrame:
    """Join seguro — evita colunas duplicadas removendo as do right que já existem no left."""
    right_cols = [c for c in right.columns if c not in on]
    right_dedup = right.select(*on, *right_cols)
    return left.join(right_dedup, on=on, how=how)


def add_cnpj_base(df: DataFrame) -> DataFrame:
    """Adiciona coluna cnpj_base (8 dígitos) para particionamento."""
    if "cnpj_base" not in df.columns:
        return df.withColumn("cnpj_base", F.substring(F.col("cnpj"), 1, 8))
    return df


def coalesce_double(df: DataFrame, col_name: str, default: float = 0.0) -> DataFrame:
    """Substitui null em coluna double por default."""
    return df.withColumn(col_name, F.coalesce(F.col(col_name).cast("double"), F.lit(default)))


def columns_exist(df: DataFrame, cols: List[str]) -> bool:
    return all(c in df.columns for c in cols)


