# src/pre_diagnostic_engine/repositories/delta_repository.py
"""
Repositório base para leitura e escrita em Delta Lake.
Utiliza Spark nativamente sem collect() ou toPandas().
"""
import logging
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.delta_repository")

 
class DeltaRepository:
    """
    Abstração de leitura e escrita Delta Lake com suporte a:
    - merge/upsert incremental
    - schema evolution
    - particionamento por DT_INI e CNPJ
    """
 
    def __init__(self, spark: SparkSession, base_path: str):
        self.spark = spark
        self.base_path = base_path.rstrip("/")
 
    def _table_path(self, table_name: str) -> str:
        return f"{self.base_path}/{table_name}"
 
    def read(self, table_name: str) -> DataFrame:
        path = self._table_path(table_name)
        logger.info(f"Lendo Delta table: {path}")
        return self.spark.read.format("delta").load(path)
 
    def read_filtered(
        self,
        table_name: str,
        cnpj: Optional[str] = None,
        dt_ini: Optional[int] = None,
    ) -> DataFrame:
        """Leitura com filtro pushdown de partição para evitar full scans."""
        path = self._table_path(table_name)
        logger.info(f"Lendo Delta table com filtros: {path}")
 
        df = self.spark.read.format("delta").load(path)
 
        if cnpj:
            df = df.filter(df["CNPJ"] == cnpj)
        if dt_ini:
            df = df.filter(df["DT_INI"] == dt_ini)
 
        return df
 
    def write(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_cols: Optional[List[str]] = None,
    ) -> None:
        path = self._table_path(table_name)
        logger.info(f"Escrevendo Delta table: {path} | mode={mode}")
 
        writer = (
            df.write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
        )
 
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
 
        writer.save(path)
        logger.info(f"Delta table gravada: {path}")
 
    def upsert(
        self,
        df: DataFrame,
        table_name: str,
        merge_keys: List[str],
        partition_cols: Optional[List[str]] = None,
    ) -> None:
        """
        Merge incremental (UPSERT) com suporte a schema evolution.
        Se a tabela não existir, faz write inicial.
 
        BUG CORRIGIDO: o original não filtrava linhas com chaves de merge nulas
        antes do MERGE/write. Isso gerava partições __HIVE_DEFAULT_PARTITION__
        no S3 toda vez que o outer join do FinalConsolidator produzia linhas
        onde CNPJ ou DT_INI era null (registros sem correspondência em algum
        dos sub-DataFrames).
 
        A correção filtra qualquer linha onde QUALQUER chave de merge seja nula
        antes de executar o upsert. Essas linhas não têm identidade no modelo
        de dados e não devem ser persistidas.
        """
        path = self._table_path(table_name)
 
        # ── Filtra linhas com chaves nulas ────────────────────────────────────
        null_filter = None
        for key in merge_keys:
            if key in df.columns:
                condition = F.col(key).isNotNull()
                null_filter = condition if null_filter is None else null_filter & condition
 
        if null_filter is not None:
            rows_before = df.count()
            df = df.filter(null_filter)
            rows_after = df.count()
            if rows_before != rows_after:
                logger.warning(
                    f"{table_name}: {rows_before - rows_after} linha(s) com chave(s) "
                    f"nula(s) removida(s) antes do upsert. "
                    f"Verifique joins no FinalConsolidator."
                )
 
        if df.isEmpty():
            logger.warning(f"{table_name}: DataFrame vazio após filtro de nulos. Upsert ignorado.")
            return

        # ── Deduplicação defensiva por chave de merge ─────────────────────────
        # O Delta MERGE falha (DeltaUnsupportedOperationException) se mais de uma
        # linha do source casar com a mesma linha do target. Isso acontece quando
        # a origem (ex.: ECF 0010 com declaração retificadora) tem mais de uma
        # linha física para o mesmo (CNPJ, DT_INI). Como rede de segurança
        # genérica, mantemos apenas 1 linha por chave de merge antes do MERGE —
        # sem lógica de negócio de qual versão é "a correta". Se isso disparar
        # com frequência, vale investigar a causa raiz na extração (retificadoras).
        rows_before_dedup = df.count()
        df = df.dropDuplicates(merge_keys)
        rows_after_dedup = df.count()
        if rows_before_dedup != rows_after_dedup:
            logger.warning(
                f"{table_name}: {rows_before_dedup - rows_after_dedup} linha(s) duplicada(s) "
                f"na chave de merge {merge_keys} removida(s) antes do upsert "
                f"(dropDuplicates arbitrário — investigar origem da duplicidade)."
            )

        if DeltaTable.isDeltaTable(self.spark, path):
            logger.info(f"Delta MERGE em: {path} | keys={merge_keys}")
            delta_table = DeltaTable.forPath(self.spark, path)
 
            merge_condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys]
            )
 
            (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"MERGE concluído: {path}")
        else:
            logger.info(f"Tabela não existe — fazendo write inicial: {path}")
            self.write(
                df,
                table_name,
                mode="overwrite",
                partition_cols=partition_cols or ["DT_INI", "CNPJ"],
            )
 
    def table_exists(self, table_name: str) -> bool:
        path = self._table_path(table_name)
        return DeltaTable.isDeltaTable(self.spark, path)
 








