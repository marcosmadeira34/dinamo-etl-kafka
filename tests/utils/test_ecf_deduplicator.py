"""
tests/utils/test_ecf_deduplicator.py

Testa a deduplicação de declarações ECF (Original vs. Retificadora) e a
preservação de períodos fracionados legítimos no mesmo ano civil.

Requer PySpark — ausente no venv local de desenvolvimento (ver requirements.txt,
pyspark é fornecido pelo ambiente Spark em runtime). O teste é pulado
automaticamente quando pyspark não está instalado e roda normalmente no
ambiente de CI/Spark.
"""
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from pre_diagnostic_engine.utils.ecf_deduplicator import deduplicate_ecf_by_file_id


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_ecf_deduplicator")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestDeduplicateOriginalVsRetificadora:

    def test_keeps_only_retificadora_values(self, spark):
        """Original (IND_REC=0) e Retificadora (IND_REC=1) no mesmo período:
        apenas os valores da Retificadora devem sobrar."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012023", "0", "100.00"),
                ("11222333000144", "01012023", "1", "250.00"),
            ],
            ["CNPJ", "DT_INI", "IND_REC", "VALOR"],
        )

        result = deduplicate_ecf_by_file_id(df).collect()

        assert len(result) == 1
        assert result[0]["IND_REC"] == "1"
        assert result[0]["VALOR"] == "250.00"

    def test_sum_after_dedup_does_not_double_count(self, spark):
        """Simula o cenário real: soma (F.sum) só deve contar a Retificadora."""
        from pyspark.sql import functions as F

        df = spark.createDataFrame(
            [
                ("11222333000144", "01012023", "0", 100.0),
                ("11222333000144", "01012023", "1", 250.0),
            ],
            ["CNPJ", "DT_INI", "IND_REC", "VALOR"],
        )

        total = (
            deduplicate_ecf_by_file_id(df)
            .agg(F.sum("VALOR").alias("total"))
            .collect()[0]["total"]
        )

        assert total == 250.0

    def test_prefers_highest_file_id_when_no_ind_rec(self, spark):
        """Sem IND_REC, usa FILE_ID mais recente como critério de precedência."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012023", 1, "100.00"),
                ("11222333000144", "01012023", 2, "300.00"),
            ],
            ["CNPJ", "DT_INI", "FILE_ID", "VALOR"],
        )

        result = deduplicate_ecf_by_file_id(df).collect()

        assert len(result) == 1
        assert result[0]["FILE_ID"] == 2
        assert result[0]["VALOR"] == "300.00"


class TestPreservesFractionatedPeriods:

    def test_distinct_dt_ini_are_preserved(self, spark):
        """Períodos fracionados legítimos (ex: cisão/fusão) têm DT_INI
        diferentes no mesmo ano civil e não devem ser deduplicados entre si."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012023", "1", "100.00"),
                ("11222333000144", "01072023", "1", "200.00"),
            ],
            ["CNPJ", "DT_INI", "IND_REC", "VALOR"],
        )

        result = deduplicate_ecf_by_file_id(df).collect()

        assert len(result) == 2
        dt_inis = {row["DT_INI"] for row in result}
        assert dt_inis == {"01012023", "01072023"}

    def test_fractionated_periods_with_retificadoras(self, spark):
        """Cada período fracionado deve reter sua própria Retificadora,
        sem interferir na deduplicação do outro período."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012023", "0", "100.00"),
                ("11222333000144", "01012023", "1", "150.00"),
                ("11222333000144", "01072023", "0", "200.00"),
                ("11222333000144", "01072023", "1", "260.00"),
            ],
            ["CNPJ", "DT_INI", "IND_REC", "VALOR"],
        )

        result = {
            row["DT_INI"]: row["VALOR"]
            for row in deduplicate_ecf_by_file_id(df).collect()
        }

        assert result == {"01012023": "150.00", "01072023": "260.00"}


class TestEdgeCases:

    def test_none_dataframe_returns_none(self):
        assert deduplicate_ecf_by_file_id(None) is None

    def test_no_partition_columns_returns_unchanged(self, spark):
        """Sem CNPJ/DT_INI, não há como particionar a janela — retorna sem alteração."""
        df = spark.createDataFrame(
            [("A", 1), ("A", 1), ("B", 2)],
            ["COL_X", "COL_Y"],
        )

        result = deduplicate_ecf_by_file_id(df).collect()

        assert len(result) == 3

    def test_partition_columns_without_ind_rec_or_file_id_drops_exact_duplicates(self, spark):
        """CNPJ/DT_INI presentes mas sem IND_REC/FILE_ID: cai para dropDuplicates()
        (remove apenas linhas idênticas, não decide entre Original/Retificadora)."""
        df = spark.createDataFrame(
            [
                ("11222333000144", "01012023", "100.00"),
                ("11222333000144", "01012023", "100.00"),
                ("11222333000144", "01012023", "999.00"),
            ],
            ["CNPJ", "DT_INI", "VALOR"],
        )

        result = deduplicate_ecf_by_file_id(df).collect()

        assert len(result) == 2
