# src/pre_diagnostic_engine/utils/indicators.py
"""Funções auxiliares de cálculo de indicadores fiscais."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calc_effective_tax_rate(df: DataFrame) -> DataFrame:
    """
    ETR = (irpj_lr_pago + csll_lr_pago) / lucro_fiscal  (quando lucro_fiscal > 0).
    """
    required = {"irpj_lr_pago", "csll_lr_pago", "lucro_fiscal"}
    if not required.issubset(set(df.columns)):
        return df
    return df.withColumn(
        "effective_tax_rate",
        F.when(
            F.col("lucro_fiscal") > 0,
            (F.col("irpj_lr_pago") + F.col("csll_lr_pago")) / F.col("lucro_fiscal")
        ).otherwise(F.lit(None).cast("double"))
    )


def calc_darf_vs_contabil_ratio(df: DataFrame) -> DataFrame:
    """Razão entre DARF pago e resultado contábil — proxy de carga tributária efetiva."""
    if "total_darf_pago" not in df.columns or "resultado_contabil" not in df.columns:
        return df
    return df.withColumn(
        "darf_contabil_ratio",
        F.when(
            F.col("resultado_contabil") > 0,
            F.col("total_darf_pago") / F.col("resultado_contabil")
        ).otherwise(F.lit(None).cast("double"))
    )