# src/pre_diagnostic_engine/analyzers/contabil_analyzer.py
"""
Analyzer Contábil.
Fonte: ECF L300 — referencial 3.01
Identifica lucro/prejuízo contábil e recorrência.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("dinamo.tax_intelligence.contabil_analyzer")

# Referencial DRE na ECF L300
DRE_REFERENCIAL = "3.01"
# A Gold Layer usa códigos de período acumulados mensais (A00..A12), onde
# A00 = fechamento do exercício. "T04" nunca existiu nesse schema real —
# era um valor placeholder que nunca casava com o dado, zerando
# resultado_contabil sempre.
PER_REFERENCIAL = "A00"


class ContabilAnalyzer:
    """
    Analisa resultado contábil (DRE) a partir do L300.
    Input:  DataFrame L300 com [cnpj, ano_calendario, CD_CONTA_COSIF, VL_CTA]
    Output: DataFrame com lucro/prejuízo e flags de recorrência.
    """

    def analyze(self, df: DataFrame, anchor: DataFrame = None) -> DataFrame:
        if df is None:
            logger.warning("L300 ausente — retornando resultado contábil vazio (nulo)")
            return self._empty_result(anchor)
        df = self._extract_resultado_contabil(df)
        df = self._flag_lucro_prejuizo(df)
        df = self._calc_recorrencia(df)
        logger.info("ContabilAnalyzer concluído")
        return df

    def _empty_result(self, anchor: DataFrame) -> DataFrame:
        """
        Constrói um DataFrame de saída com o mesmo schema de analyze(),
        porém com métricas nulas, ancorado nos (CNPJ, DT_INI) reais do 0010.
        """
        if anchor is None:
            raise ValueError(
                "ContabilAnalyzer: L300 ausente e nenhum anchor (df_regime) fornecido "
                "para derivar CNPJ/DT_INI."
            )
        base = anchor.select("CNPJ", "DT_INI").distinct()
        return (
            base
            .withColumn("resultado_contabil",     F.lit(None).cast("double"))
            .withColumn("lucro_contabil",          F.lit(None).cast("double"))
            .withColumn("prejuizo_contabil",       F.lit(None).cast("double"))
            .withColumn("tem_lucro_contabil",      F.lit(None).cast("boolean"))
            .withColumn("tem_prejuizo_contabil",   F.lit(None).cast("boolean"))
            .withColumn("recorrencia_lucro_anos",     F.lit(None).cast("long"))
            .withColumn("recorrencia_prejuizo_anos",  F.lit(None).cast("long"))
        )

    def _extract_resultado_contabil(self, df: DataFrame) -> DataFrame:
        def _br_to_double(col_name):
            return F.regexp_replace(
                F.regexp_replace(F.col(col_name), r'\.', ''), ',', '.'
            ).cast("double")

        valor_col = (
            _br_to_double("VALOR") if "VALOR" in df.columns
            else _br_to_double("VALOR") if "VALOR" in df.columns
            else F.col(df.columns[3]).cast("double")
        )

        # Pega a conta raiz 3.01 (Resultado Líquido antes IRPJ/CSLL)
        # PER_APUR = A00 → fechamento do exercício (código real da Gold Layer)
        return (
            df.filter(
                (F.col("CODIGO") == DRE_REFERENCIAL) &  # ← conta raiz, não filhos
                (F.col("PER_APUR") == PER_REFERENCIAL)             # ← saldo final do exercício
            )
            .groupBy("CNPJ", "DT_INI")
            # .agg(F.first(valor_col).alias("resultado_contabil"))
            .agg(F.max(valor_col).alias("resultado_contabil"))
        )

    def _flag_lucro_prejuizo(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("lucro_contabil", F.when(F.col("resultado_contabil") > 0, F.col("resultado_contabil")).otherwise(F.lit(0.0)))
              .withColumn("prejuizo_contabil", F.when(F.col("resultado_contabil") < 0, F.abs(F.col("resultado_contabil"))).otherwise(F.lit(0.0)))
              .withColumn("tem_lucro_contabil", F.col("resultado_contabil") > 0)
              .withColumn("tem_prejuizo_contabil", F.col("resultado_contabil") < 0)
        )

    def _calc_recorrencia(self, df: DataFrame) -> DataFrame:
        """
        Conta anos acumulados de lucro e prejuízo via Window running sum.
        """
        w = Window.partitionBy("CNPJ").orderBy("DT_INI").rowsBetween(Window.unboundedPreceding, 0)
        return (
            df.withColumn("recorrencia_lucro_anos", F.sum(F.col("tem_lucro_contabil").cast("int")).over(w))
              .withColumn("recorrencia_prejuizo_anos", F.sum(F.col("tem_prejuizo_contabil").cast("int")).over(w))
        )




