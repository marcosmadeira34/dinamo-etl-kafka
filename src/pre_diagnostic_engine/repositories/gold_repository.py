# src/pre_diagnostic_engine/repositories/gold_repository.py
"""
Repositório de leitura e escrita das Gold Tables de Tax Intelligence.
Fonte oficial de dados: GOLD LAYER do bucket OCI/S3.
Todas as tabelas possuem particionamento por DT_INI e cnpj_base.
"""
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from .delta_repository import DeltaRepository
from pyspark.sql import functions as F
from delta.tables import DeltaTable

logger = logging.getLogger("dinamo.tax_intelligence.gold_repository")

# ── Nomes canônicos das Gold Tables ──────────────────────────────────────────
TABLE_COMPANY_TAX_PROFILE      = "gold_company_tax_profile"
TABLE_PRE_DIAGNOSTIC_SUMMARY   = "gold_pre_diagnostic_summary"
TABLE_PRE_DIAGNOSTIC_SCORES    = "gold_pre_diagnostic_scores"
TABLE_PRE_DIAGNOSTIC_OPPORTUNITIES = "gold_pre_diagnostic_opportunities"
TABLE_PRE_DIAGNOSTIC_GO_NO_GO  = "gold_pre_diagnostic_go_no_go"
TABLE_PRE_DIAGNOSTIC_FINANCIALS = "gold_pre_diagnostic_financials"
TABLE_PRE_DIAGNOSTIC_MASTER     = "gold_pre_diagnostic_master"
TABLE_PRE_DIAGNOSTIC_MASTER_HISTORICAL = "gold_pre_diagnostic_master_historical"

PARTITION_COLS = ["DT_INI", "CNPJ"]
MERGE_KEYS     = ["CNPJ", "DT_INI"]


class GoldRepository:
    """
    Repositório de acesso às Gold Tables de Tax Intelligence.
    Toda leitura parte da Gold Layer — nunca do SPED bruto.
    """

    def __init__(self, spark: SparkSession, gold_base_path: str):
        self.spark = spark
        self.delta = DeltaRepository(spark, gold_base_path)

    # ── LEITURAS ──────────────────────────────────────────────────────────────

    def read_ecf_gold(self, report_key: str, cnpj_base: Optional[str] = None,
                      dt_ini: Optional[int] = None) -> DataFrame:
        """
        Lê um relatório Gold ECF já gerado pelo pipeline Dinamo.
        report_key exemplos: 'ECF_0000_0010', 'ECF_0000_L030_L300', etc.
        """
        return self.delta.read_filtered(report_key, cnpj_base, dt_ini)

    def read_company_tax_profile(self, cnpj_base: Optional[str] = None,
                                 dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_COMPANY_TAX_PROFILE, cnpj_base, dt_ini)

    def read_pre_diagnostic_summary(self, cnpj_base: Optional[str] = None,
                                    dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_SUMMARY, cnpj_base, dt_ini)

    def read_pre_diagnostic_scores(self, cnpj_base: Optional[str] = None,
                                   dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_SCORES, cnpj_base, dt_ini)

    def read_pre_diagnostic_opportunities(self, cnpj_base: Optional[str] = None,
                                          dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_OPPORTUNITIES, cnpj_base, dt_ini)

    def read_pre_diagnostic_go_no_go(self, cnpj_base: Optional[str] = None,
                                     dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_GO_NO_GO, cnpj_base, dt_ini)

    def read_pre_diagnostic_financials(self, cnpj_base: Optional[str] = None,
                                       dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_FINANCIALS, cnpj_base, dt_ini)

    def read_pre_diagnostic_master(self, cnpj_base: Optional[str] = None,
                                       dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_MASTER, cnpj_base, dt_ini)

    def read_pre_diagnostic_master_historical(self, cnpj_base: Optional[str] = None,
                                           dt_ini: Optional[int] = None) -> DataFrame:
        return self.delta.read_filtered(TABLE_PRE_DIAGNOSTIC_MASTER_HISTORICAL, cnpj_base, dt_ini)

    # ── ESCRITAS (UPSERT) ─────────────────────────────────────────────────────

    def upsert_company_tax_profile(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_company_tax_profile")
        self.delta.upsert(df, TABLE_COMPANY_TAX_PROFILE, MERGE_KEYS, PARTITION_COLS)

    def upsert_pre_diagnostic_summary(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_summary")
        self.delta.upsert(df, TABLE_PRE_DIAGNOSTIC_SUMMARY, MERGE_KEYS, PARTITION_COLS)

    def upsert_pre_diagnostic_scores(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_scores")
        self.delta.upsert(df, TABLE_PRE_DIAGNOSTIC_SCORES, MERGE_KEYS, PARTITION_COLS)

    def upsert_pre_diagnostic_opportunities(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_opportunities")
        # Múltiplas oportunidades por empresa/ano → merge_keys inclui opportunity_id
        self.delta.upsert(
            df, TABLE_PRE_DIAGNOSTIC_OPPORTUNITIES,
            ["CNPJ", "DT_INI", "opportunity_id"],
            PARTITION_COLS,
        )

    def upsert_pre_diagnostic_go_no_go(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_go_no_go")
        self.delta.upsert(df, TABLE_PRE_DIAGNOSTIC_GO_NO_GO, MERGE_KEYS, PARTITION_COLS)

    def upsert_pre_diagnostic_financials(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_financials")
        self.delta.upsert(df, TABLE_PRE_DIAGNOSTIC_FINANCIALS, MERGE_KEYS, PARTITION_COLS)

    def upsert_pre_diagnostic_master(self, df_final) -> None:
        logger.info("Upsert: gold_pre_diagnostic_master")
        self.delta.upsert(df_final, TABLE_PRE_DIAGNOSTIC_MASTER, MERGE_KEYS, PARTITION_COLS)

    def upsert_pre_diagnostic_master_historical(self, df: DataFrame) -> None:
        logger.info("Upsert: gold_pre_diagnostic_master_historical")
        self.delta.upsert(df, TABLE_PRE_DIAGNOSTIC_MASTER_HISTORICAL, MERGE_KEYS, PARTITION_COLS)

    
    def write_pre_diagnostic_master_historical(
        self,
        df: DataFrame,
        cnpj: str
    ) -> None:

        path = (
            f"{self.delta.base_path}/"
            f"gold_pre_diagnostic_master_historical"
        )

        logger.info(
            f"Upsert histórico consolidado: {path}"
        )

        merge_keys = ["CNPJ", "DT_INI"]

        # remove registros inválidos
        df = df.dropna(subset=merge_keys)

        # tabela ainda não existe
        if not DeltaTable.isDeltaTable(self.spark, path):

            logger.info(
                "Histórico não existe. Criando tabela Delta inicial."
            )

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("cnpj_base")
                .save(path)
            )

            return

        # merge incremental
        delta_table = DeltaTable.forPath(
            self.spark,
            path
        )

        source_alias = "source"
        target_alias = "target"

        merge_condition = " AND ".join([
            f"{target_alias}.{k} = {source_alias}.{k}"
            for k in merge_keys
        ])

        (
            delta_table.alias(target_alias)
            .merge(
                df.alias(source_alias),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info(
            f"MERGE histórico concluído: {path}"
        )



    # Relatório master
    def build_historical_master(self, df_atual: DataFrame) -> DataFrame:

        base_path = (f"{self.delta.base_path}/"f"{TABLE_PRE_DIAGNOSTIC_MASTER}")

        try:

            df_hist = (
                self.spark.read
                .format("delta")
                .load(base_path)
            )

        except Exception:

            logger.warning(
                "Histórico inexistente. Criando histórico inicial."
            )

            return df_atual.dropDuplicates(["CNPJ", "DT_INI"])

        cnpjs = [
            r["CNPJ"]
            for r in df_atual.select("CNPJ").distinct().collect()
        ]

        df_hist = df_hist.filter(
            F.col("CNPJ").isin(cnpjs)
        )

        df_hist = df_hist.withColumn(
            "ano",
            F.substring("DT_INI", 5, 4).cast("int")
        )

        ano_atual = 2026

        df_hist = df_hist.filter(
            F.col("ano") >= (ano_atual - 5)
        ).drop("ano")

        return (
            df_hist.unionByName(
                df_atual,
                allowMissingColumns=True
            )
            .dropDuplicates(["CNPJ", "DT_INI"])
        )


