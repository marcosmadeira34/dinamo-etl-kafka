# src/pre_diagnostic_engine/utils/ecf_deduplicator.py
"""
Utilitário de deduplicação de declarações ECF.
Deduplica registros ECF garantindo que apenas a declaração mais recente (Retificadora)
seja mantida para cada (CNPJ, DT_INI), evitando duplicação de dados e somas indevidas.
"""
import logging
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("dinamo.tax_intelligence.ecf_deduplicator")


def deduplicate_ecf_by_file_id(df: DataFrame, partition_cols: Optional[List[str]] = None) -> DataFrame:
    """
    Deduplica declarações ECF mantendo apenas o arquivo/versão mais recente por (CNPJ, DT_INI).
    
    Estratégia:
    1. Se a coluna FILE_ID / _file_id existir, ordena decrescente para selecionar a última versão transmitida.
    2. Se a coluna IND_REC (0=Original, 1=Retificadora) existir, ordena decrescente para dar prioridade a Retificadoras.
    3. Preserva períodos fracionados distintos (ex: DT_INI diferentes no mesmo ano).
    """
    if df is None:
        return None

    if partition_cols is None:
        partition_cols = [c for c in ["CNPJ", "DT_INI"] if c in df.columns]

    if not partition_cols:
        logger.debug("Nenhuma coluna de partição encontrada no DataFrame. Retornando sem alteração.")
        return df

    order_exprs = []

    if "IND_REC" in df.columns:
        order_exprs.append(F.col("IND_REC").desc())

    if "_file_id" in df.columns:
        order_exprs.append(F.col("_file_id").desc())
    elif "FILE_ID" in df.columns:
        order_exprs.append(F.col("FILE_ID").desc())

    if not order_exprs:
        logger.debug("Nenhuma coluna IND_REC/FILE_ID/_file_id encontrada. Aplicando dropDuplicates().")
        return df.dropDuplicates()

    window_spec = Window.partitionBy(*partition_cols).orderBy(*order_exprs)
    return (
        df.withColumn("_file_rank", F.dense_rank().over(window_spec))
          .filter(F.col("_file_rank") == 1)
          .drop("_file_rank")
    )
