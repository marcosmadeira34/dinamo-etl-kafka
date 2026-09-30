# src/pre_diagnostic_engine/consolidators/final_consolidator.py
"""
Consolida TODOS os sub-resultados num DataFrame final por (CNPJ, DT_INI).
"""
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("dinamo.tax_intelligence.final_consolidator")

JOIN_KEYS = {"CNPJ", "DT_INI"}


def _safe_join(left: DataFrame, right: DataFrame, how: str) -> DataFrame:
    """Join sem AMBIGUOUS_REFERENCE: remove de `right` colunas duplicadas."""
    duplicate = (set(left.columns) & set(right.columns)) - JOIN_KEYS
    if duplicate:
        logger.debug(f"Dropando colunas duplicadas: {duplicate}")
        right = right.drop(*duplicate)
    return left.join(right, on=list(JOIN_KEYS), how=how)


class FinalConsolidator:

    def consolidate(
        self,
        df_regime:        DataFrame,
        df_fiscal:        DataFrame,
        df_financial:     DataFrame,
        df_opportunities: DataFrame,
        df_company:       DataFrame,
    ) -> DataFrame:
        """
        Faz joins sequenciais LEFT por (CNPJ, DT_INI).

        CORREÇÕES:
        1. Joins mudados de OUTER para LEFT — df_regime é a âncora (tem CNPJ real).
           Outer join com DataFrames que podem ter CNPJ=None gerava linhas fantasmas
           com CNPJ=None no resultado, causando __HIVE_DEFAULT_PARTITION__ no Delta.

        2. _consolidar_master() removido daqui — referenciava self.spark inexistente
           no FinalConsolidator (não recebe spark no __init__), causando AttributeError
           capturado silenciosamente pelo try/except, que retornava df sem joins analíticos.
           O histórico agora é construído no pipeline via consolidar_master_historico().

        3. Filtro de nulos no final como camada de segurança adicional.
        """
        for key in JOIN_KEYS:
            if key not in df_regime.columns:
                raise ValueError(f"df_regime não possui coluna obrigatória: {key}")

        df = df_regime
        df = _safe_join(df, df_fiscal,        how="left")
        df = _safe_join(df, df_financial,     how="left")
        df = _safe_join(df, df_opportunities, how="left")

        # ── Dados cadastrais ──────────────────────────────────────────────────
        nome_col = (
            "NOME"        if df_company is not None and "NOME"        in df_company.columns else
            "razao_social" if df_company is not None and "razao_social" in df_company.columns else
            None
        )

        if df_company is None:
            logger.warning("df_company ausente — seguindo sem dados cadastrais (NOME nulo)")
        elif nome_col is not None:
            extra = [nome_col] + (["DT_FIN"] if "DT_FIN" in df_company.columns else [])
            df_cadastro = (
                df_company
                .select(*list(JOIN_KEYS) + extra)
                .dropDuplicates(["CNPJ", "DT_INI"])
            )
            if nome_col == "razao_social":
                df_cadastro = df_cadastro.withColumnRenamed("razao_social", "NOME")
            df = _safe_join(df, df_cadastro, how="left")
        else:
            logger.warning("df_company sem NOME nem razao_social.")

        # ── cnpj_base + filtro de nulos ───────────────────────────────────────
        if "cnpj_base" not in df.columns:
            df = df.withColumn("cnpj_base", F.substring(F.col("CNPJ"), 1, 8))

        df = df.filter(F.col("CNPJ").isNotNull() & F.col("DT_INI").isNotNull())

        logger.info("FinalConsolidator concluído")
        return df

    def consolidar_master_historico(
        self,
        spark: SparkSession,
        df_atual: DataFrame,
        base_path: str,
        cnpj: str,
    ) -> DataFrame:
        """
        Lê gold_pre_diagnostic_master do Delta, filtra CNPJ + últimos 5 anos,
        une com df_atual e deduplica. Chamado pelo pipeline pós-persistência.
        """
        path = f"{base_path}/gold_pre_diagnostic_master_historical"
        logger.info(f"Lendo histórico master em {path}")

        try:
            df_hist = spark.read.format("delta").load(path)
        except Exception as e:
            logger.warning(f"Histórico inexistente: {e}")
            return df_atual.dropDuplicates(["CNPJ", "DT_INI"]).orderBy("DT_INI")

        df_hist = (
            df_hist
            .filter(F.col("cnpj_base") == cnpj)
            .filter(F.col("CNPJ").isNotNull() & F.col("DT_INI").isNotNull())
            .withColumn("_ano", F.substring("DT_INI", 5, 4).cast("int"))
        )

        ano_max_row = df_hist.agg(F.max("_ano")).collect()[0][0]
        if ano_max_row is None:
            logger.warning(f"Nenhum histórico válido para CNPJ {cnpj}")
            return df_atual.dropDuplicates(["CNPJ", "DT_INI"]).orderBy("DT_INI")

        df_hist = df_hist.filter(F.col("_ano") >= (ano_max_row - 5)).drop("_ano")

        # dropDuplicates não garante que df_atual (execução nova) vença df_hist (execução
        # antiga) para o mesmo (CNPJ, DT_INI) — a ordem de retenção não é determinística.
        # Removemos do histórico as chaves já presentes em df_atual antes de unir, garantindo
        # que a declaração recém-processada (já deduplicada Original vs. Retificadora)
        # sempre prevaleça sobre dados antigos acumulados no Delta.
        df_atual_keys = df_atual.select("CNPJ", "DT_INI").distinct()
        df_hist_restante = df_hist.join(df_atual_keys, on=["CNPJ", "DT_INI"], how="left_anti")

        df_final = (
            df_atual
            .unionByName(df_hist_restante, allowMissingColumns=True)
            .orderBy("DT_INI")
        )
        logger.info(f"Histórico consolidado: {df_final.count()} períodos")
        return df_final