# src/pre_diagnostic_engine/analyzers/fiscal_analyzer.py
"""
Analyzer Fiscal.
Fonte: ECF N500 — Resultado Fiscal
Identifica lucro/prejuízo fiscal, base tributável e recorrência.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("dinamo.tax_intelligence.fiscal_analyzer")

# Candidatos de coluna de resultado fiscal no N500 — em ordem de prioridade
_RESULTADO_FISCAL_CANDIDATES = [
    "VL_REC_LIQ_LTIR",   # nome canônico ECF N500
    "VALOR",             # alguns relatórios Gold normalizam para VALOR
    "VL_CTA",            # alternativa comum
]


class FiscalAnalyzer:
    """
    Analisa o resultado fiscal (LALUR/N500) da empresa.
    Input:  DataFrame N500 com [cnpj, ano_calendario, VL_REC_LIQ_LTIR, VL_IRPJ_DEV, ...]
    Output: DataFrame com lucro/prejuízo fiscal e flags de recorrência.
    """

    def analyze(self, df: DataFrame, anchor: DataFrame = None) -> DataFrame:
        if df is None:
            logger.warning("N500 ausente — retornando resultado fiscal vazio (nulo)")
            return self._empty_result(anchor)
        df = self._extract_resultado_fiscal(df)
        df = self._flag_lucro_prejuizo(df)
        df = self._calc_base_tributavel(df)
        df = self._calc_recorrencia_fiscal(df)
        logger.info("FiscalAnalyzer concluído")
        return df

    def _empty_result(self, anchor: DataFrame) -> DataFrame:
        if anchor is None:
            raise ValueError(
                "FiscalAnalyzer: N500 ausente e nenhum anchor (df_regime) fornecido "
                "para derivar CNPJ/DT_INI."
            )
        base = anchor.select("CNPJ", "DT_INI").distinct()
        return (
            base
            .withColumn("resultado_fiscal",   F.lit(None).cast("double"))
            .withColumn("lucro_fiscal",       F.lit(None).cast("double"))
            .withColumn("prejuizo_fiscal",    F.lit(None).cast("double"))
            .withColumn("tem_lucro_fiscal",   F.lit(None).cast("boolean"))
            .withColumn("tem_prejuizo_fiscal",F.lit(None).cast("boolean"))
            .withColumn("base_tributavel",    F.lit(None).cast("double"))
            .withColumn("recorrencia_lucro_fiscal_anos",    F.lit(None).cast("long"))
            .withColumn("recorrencia_prejuizo_fiscal_anos", F.lit(None).cast("long"))
        )

    def _extract_resultado_fiscal(self, df: DataFrame) -> DataFrame:
        """
        Consolida o lucro fiscal (LAIR — Lucro Antes do IRPJ).
        O N500 contém campo VL_REC_LIQ_LTIR ou equivalente.
        """
        # Campo padrão do N500 — ajustar conforme schema real do projeto
        resultado_col = None
        for candidate in _RESULTADO_FISCAL_CANDIDATES:
            if candidate in df.columns:
                resultado_col = F.col(candidate).cast("double")
                logger.info(f"FiscalAnalyzer usando coluna: {candidate}")
                break
 
        if resultado_col is None:
            logger.warning(
                f"Nenhuma coluna de resultado fiscal encontrada. "
                f"Colunas disponíveis: {df.columns}. Usando zero."
            )
            resultado_col = F.lit(0.0)
 
        return (
            df.groupBy("CNPJ", "DT_INI")
              .agg(F.sum(resultado_col).alias("resultado_fiscal"))
        )

    def _flag_lucro_prejuizo(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("lucro_fiscal", F.when(F.col("resultado_fiscal") > 0, F.col("resultado_fiscal")).otherwise(F.lit(0.0)))
              .withColumn("prejuizo_fiscal", F.when(F.col("resultado_fiscal") < 0, F.abs(F.col("resultado_fiscal"))).otherwise(F.lit(0.0)))
              .withColumn("tem_lucro_fiscal", F.col("resultado_fiscal") > 0)
              .withColumn("tem_prejuizo_fiscal", F.col("resultado_fiscal") < 0)
        )

    def _calc_base_tributavel(self, df: DataFrame) -> DataFrame:
        """
        Base tributável = lucro_fiscal quando positivo.
        """
        return df.withColumn(
            "base_tributavel",
            F.when(F.col("resultado_fiscal") > 0, F.col("resultado_fiscal")).otherwise(F.lit(0.0))
        )

    def _calc_recorrencia_fiscal(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("CNPJ").orderBy("DT_INI").rowsBetween(Window.unboundedPreceding, 0)
        return (
            df.withColumn("recorrencia_lucro_fiscal_anos", F.sum(F.col("tem_lucro_fiscal").cast("int")).over(w))
              .withColumn("recorrencia_prejuizo_fiscal_anos", F.sum(F.col("tem_prejuizo_fiscal").cast("int")).over(w))
        )
