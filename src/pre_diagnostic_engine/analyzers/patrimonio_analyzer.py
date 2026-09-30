# src/pre_diagnostic_engine/analyzers/patrimonio_analyzer.py
"""
Analyzer de Patrimônio Líquido.
Fonte: ECF L100 — referencial 2.03
Valida PL positivo, evolução patrimonial e capacidade JCP.
NUNCA usa collect() ou loops Python em volume.
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("dinamo.tax_intelligence.patrimonio_analyzer")

# Referencial de Patrimônio Líquido na ECF L100
PL_REFERENCIAL = "2.03"

# Referencial de período do saldo do PL — a Gold Layer usa códigos acumulados
# mensais (A00..A12), onde A00 = fechamento do exercício. Não existe "T01" nesse
# schema real (era um valor placeholder que nunca casava com o dado, zerando
# patrimonio_liquido sempre). Confirmado com o time: A00 é o valor correto
# também para "PL Inicial" neste relatório.
PERIODO_SALDO_INICIAL = "A00"

# Candidatos de coluna de valor PL no L100 — em ordem de prioridade
_PL_VALUE_CANDIDATES = [
    "VAL_CTA_REF_INI",   # nome canônico ECF L100
    #"VL_CTA",            # alternativa comum nos relatórios Gold
    #"VALOR",             # normalização Gold
]


class PatrimonioAnalyzer:
    """
    Analisa a evolução do Patrimônio Líquido da empresa.
    Input:  DataFrame L100 com colunas [cnpj, ano_calendario, COD_CONTA_SUP, VAL_CTA_REF_INI]
    Output: DataFrame com indicadores de PL e capacidade JCP.
    """

    def analyze(self, df: DataFrame, anchor: DataFrame = None) -> DataFrame:
        if df is None:
            logger.warning("L100 ausente — retornando patrimônio vazio (nulo)")
            return self._empty_result(anchor)
        df = self._extract_pl(df)
        df = self._flag_pl_positivo(df)
        df = self._calc_evolucao_patrimonial(df)
        df = self._calc_capacidade_jcp(df)
        logger.info("PatrimonioAnalyzer concluído")
        return df

    def _empty_result(self, anchor: DataFrame) -> DataFrame:
        if anchor is None:
            raise ValueError(
                "PatrimonioAnalyzer: L100 ausente e nenhum anchor (df_regime) fornecido "
                "para derivar CNPJ/DT_INI."
            )
        base = anchor.select("CNPJ", "DT_INI").distinct()
        return (
            base
            .withColumn("patrimonio_liquido",        F.lit(None).cast("double"))
            .withColumn("pl_positivo",                F.lit(None).cast("boolean"))
            .withColumn("evolucao_patrimonial_pct",   F.lit(None).cast("double"))
            .withColumn("capacidade_jcp_estimada",    F.lit(None).cast("double"))
        )

    def _extract_pl(self, df: DataFrame) -> DataFrame:
        """
        Extrai o Patrimônio Líquido inicial do exercício.
        Fonte: CODIGO = '2.03', período T01 (saldo inicial).
        """
        # Converte string BR → double
        def _br_to_double(col_name: str):
            return (
                F.regexp_replace(
                    F.regexp_replace(F.col(col_name), r'\.', ''),
                    ',', '.'
                ).cast("double")
            )

        pl_col = None
        for candidate in _PL_VALUE_CANDIDATES:
            if candidate in df.columns:
                pl_col = _br_to_double(candidate)
                logger.info(f"PatrimonioAnalyzer usando coluna: {candidate}")
                break

        if pl_col is None:
            logger.warning(f"Nenhuma coluna de valor PL encontrada. Colunas: {df.columns}. Usando zero.")
            pl_col = F.lit(0.0)

        return (
            df.filter(
                (F.col("CODIGO") == PL_REFERENCIAL) &   # ← conta raiz do PL no referencial ECF
                (F.col("PER_APUR") == PERIODO_SALDO_INICIAL)     # ← saldo inicial = primeiro trimestre
            )
            .groupBy("CNPJ", "DT_INI")
            .agg(F.first(pl_col).alias("patrimonio_liquido"))
        )

    def _flag_pl_positivo(self, df: DataFrame) -> DataFrame:
        return df.withColumn("pl_positivo", F.col("patrimonio_liquido") > 0)

    def _calc_evolucao_patrimonial(self, df: DataFrame) -> DataFrame:
        """
        Calcula variação percentual do PL em relação ao ano anterior via Window.
        """
        w = Window.partitionBy("CNPJ").orderBy("DT_INI")
        pl_anterior = F.lag("patrimonio_liquido", 1).over(w)
        return df.withColumn(
            "evolucao_patrimonial_pct",
            F.when(
                pl_anterior.isNotNull() & (pl_anterior != 0),
                (F.col("patrimonio_liquido") - pl_anterior) / F.abs(pl_anterior) * 100
            ).otherwise(F.lit(None).cast("double"))
        )

    def _calc_capacidade_jcp(self, df: DataFrame) -> DataFrame:
        """
        Capacidade JCP: PL positivo habilita distribuição de JCP.
        Estimativa conservadora: 6% TJLP sobre PL médio.
        """
        TJLP = 0.06
        return df.withColumn(
            "capacidade_jcp_estimada",
            F.when(F.col("pl_positivo"), F.col("patrimonio_liquido") * TJLP)
             .otherwise(F.lit(0.0))
        )
