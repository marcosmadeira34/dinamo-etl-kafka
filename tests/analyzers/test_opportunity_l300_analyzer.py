"""
tests/analyzers/test_opportunity_l300_analyzer.py

Regressão: o PAT (e demais oportunidades L300) saíam com valor duplicado
porque o analyzer somava tanto PER_APUR="A12" (dezembro acumulado) quanto
PER_APUR="A00" (fechamento do exercício) — os dois carregam o MESMO total
anual na Gold Layer, então somar ambos dobrava o valor. A convenção real
(confirmada em contabil_analyzer.py e patrimonio_analyzer.py) é usar
exclusivamente PER_APUR="A00" como período de fechamento.

Requer PySpark — ausente no venv local (ver requirements.txt). Pulado
automaticamente quando pyspark não está instalado.
"""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from pre_diagnostic_engine.analyzers.opportunity_l300_analyzer import OpportunityL300Analyzer

PAT_CODIGO = "3.01.01.07.01.13"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_opportunity_l300_analyzer")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestPatNaoDuplicaEntrePerApur:

    def test_a12_e_a00_com_mesmo_total_nao_dobra_valor(self, spark):
        """Cenário real do bug: A12 e A00 carregam o mesmo total do ano.
        Apenas A00 deve ser somado."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012020", PAT_CODIGO, "A12", "74344,52"),
                ("11222333000144", "01012020", PAT_CODIGO, "A00", "74344,52"),
            ],
            ["CNPJ", "DT_INI", "CODIGO", "PER_APUR", "VALOR"],
        )

        result = OpportunityL300Analyzer().analyze(df).collect()

        assert len(result) == 1
        assert result[0]["opp_l300_pat_valor"] == pytest.approx(74344.52)

    def test_apenas_a00_presente_soma_normalmente(self, spark):
        df = spark.createDataFrame(
            [("11222333000144", "01012024", PAT_CODIGO, "A00", "10000,00")],
            ["CNPJ", "DT_INI", "CODIGO", "PER_APUR", "VALOR"],
        )

        result = OpportunityL300Analyzer().analyze(df).collect()

        assert result[0]["opp_l300_pat_valor"] == pytest.approx(10000.00)

    def test_a12_sem_a00_nao_e_contabilizado(self, spark):
        """Sem a linha A00 (fechamento), o analyzer não deve inferir valor a
        partir de A12 isoladamente — mantém a mesma convenção usada nos
        demais analyzers do projeto (contabil/patrimônio)."""
        df = spark.createDataFrame(
            [("11222333000144", "01012024", PAT_CODIGO, "A12", "10000,00")],
            ["CNPJ", "DT_INI", "CODIGO", "PER_APUR", "VALOR"],
        )

        result = OpportunityL300Analyzer().analyze(df).collect()

        assert result[0]["opp_l300_pat_valor"] == 0.0

    def test_outros_periodos_mensais_nao_interferem(self, spark):
        """Linhas A01..A11 (acumulados mensais parciais) não devem ser
        somadas junto com o fechamento A00."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012024", PAT_CODIGO, "A06", "5000,00"),
                ("11222333000144", "01012024", PAT_CODIGO, "A00", "12000,00"),
            ],
            ["CNPJ", "DT_INI", "CODIGO", "PER_APUR", "VALOR"],
        )

        result = OpportunityL300Analyzer().analyze(df).collect()

        assert result[0]["opp_l300_pat_valor"] == pytest.approx(12000.00)
