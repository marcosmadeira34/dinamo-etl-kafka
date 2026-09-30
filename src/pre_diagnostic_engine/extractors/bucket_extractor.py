# ./src/dinamo_web/extractors/bucket_extractor.py
import logging
from functools import reduce
from typing import List, Union

from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, StructType
from pyspark.errors.exceptions.captured import AnalysisException

from pre_diagnostic_engine.connectors.bucket_connector import BucketConnector


logger = logging.getLogger(__name__)


class BucketExtractor:
    """
    Responsável por extrair arquivos (CSV, Parquet, texto) do bucket usando Spark.
    """
    def __init__(self, spark: SparkSession, bucket_name: str):
        self.spark = spark
        self.bucket = bucket_name
        self.bucket_connector = BucketConnector(spark, bucket_name)

    
    
    def read_parquet(self, path: str):
        full_path = f"s3a://{self.bucket}/DINAMO-WEB-DEVELOPMENT/{path}"
        logger.info(f"Read Parquet em {full_path}")

        try:
            df = self.spark.read.parquet(full_path)
            return df
        except AnalysisException as e:
            logger.warning(f"Path não encontrado: {full_path}")
           
            error_msg = str(e)

            if "PATH_NOT_FOUND" in error_msg:
                return None

            logger.error(f"Erro ao ler parquet: {error_msg}")
            raise None

    
    def read_txt(self, paths):
        """
        Lê arquivos TXT do S3 preservando a ORDEM REAL das linhas.
        Gera _row_id determinístico usando zipWithIndex().
        """

        # -------------------------------------------------
        # 1️⃣ NORMALIZA ENTRADA
        # -------------------------------------------------
        if isinstance(paths, str):
            paths = [paths]

        full_s3_paths = [f"s3a://{self.bucket}/{p}" for p in paths]

        # -------------------------------------------------
        # 2️⃣ LEITURA BRUTA
        # -------------------------------------------------
        df_raw = self.spark.read.text(*full_s3_paths)

        # -------------------------------------------------
        # 3️⃣ GARANTE RASTREABILIDADE DO ARQUIVO
        # -------------------------------------------------
        df_raw = df_raw.withColumn("_source_file", F.input_file_name())

        # -------------------------------------------------
        # 4️⃣ GERA ÍNDICE GLOBAL (ORDEM REAL)
        # ⚠️ ESSA É A PARTE MAIS IMPORTANTE
        # -------------------------------------------------
        df_indexed = (
            df_raw
            .rdd
            .zipWithIndex()
            .toDF(["row", "_row_id"])
            .select(
                F.col("row.value").alias("value"),
                "_row_id",
                F.col("row._source_file").alias("_source_file")
            )
        )

        # -------------------------------------------------
        # 5️⃣ (OPCIONAL) ÍNDICE POR ARQUIVO
        # 👉 útil se processar múltiplos arquivos juntos
        # -------------------------------------------------
        w = Window.partitionBy("_source_file").orderBy("_row_id")

        df_indexed = df_indexed.withColumn(
            "_row_id_file",
            F.row_number().over(w)
        )

        # -------------------------------------------------
        # 6️⃣ RETORNO FINAL
        # -------------------------------------------------
        return df_indexed

    # def read_txt(self, paths):
    #     """
    #     Lê um ou mais arquivos .txt de um caminho ou lista de caminhos.
    #     Corrigido para construir caminhos S3 completos para cada item da lista.
    #     """
    #     # Se 'paths' for uma string, coloque em uma lista para padronizar o processamento
    #     if isinstance(paths, str):
    #         paths = [paths]
        
    #     # Use uma compreensão de lista para construir o caminho S3 completo para cada arquivo
    #     # Ex: ['s3a://bucket/caminho1', 's3a://bucket/caminho2']
    #     full_s3_paths = [f"s3a://{self.bucket}/{p}" for p in paths]
        
    #     # Desempacota a lista de caminhos COMPLETOS para o Spark
    #     # Ex: spark.read.text("s3a://bucket/caminho1", "s3a://bucket/caminho2", ...)
    #     return self.spark.read.text(*full_s3_paths)

    def parse_sped_records(self, df_txt: DataFrame, record_types: Union[str, List[str]]) -> DataFrame:
        """
        Filtra múltiplos registros SPED e quebra a linha em colunas.
        """

        if isinstance(record_types, str):
            record_types = [record_types]

        condition = reduce(
            lambda acc, r: acc | F.col("value").contains(f"|{r}|"),
            record_types[1:],
            F.col("value").contains(f"|{record_types[0]}|")
        )

        return (
            df_txt
            .filter(condition)
            .withColumn("cols", F.split(F.col("value"), "\|"))
        )

    def read_csv(self, path: str) -> DataFrame:
        full_path = f"s3a://{self.bucket}/{path}"
        logger.info(f"Lendo CSV direto via Spark: {full_path}")
        
        return (
            self.spark.read \
            .option("header", "true") \
            .option("sep", ";") \
            .option("inferSchema", "true") \
            .csv(full_path)
        )


        
    def convert_parquet_to_csv(self, parquet_path: str, csv_path: str) -> str:
        """
        Converte um arquivo parquet para um único arquivo csv com o nome final especificado.
        - parquet_path deve ser algo como: 'converted/arquivo.parquet'
        - csv_path deve ser algo como: 'converted/arquivo.csv'
        - Spark escreve em um diretório, então renomeamos o arquivo final.
        """

        # aceitar DataFrame ou caminho
        if isinstance(parquet_path, str):
            df_parquet = self.read_parquet(parquet_path)
        elif isinstance(parquet_path, DataFrame):
            df_parquet = parquet_path
        else:
            raise ValueError("parquet_path deve ser caminho do arquivo ou DataFrame")

        # reduzir para 1 arquivo
        df_to_write = df_parquet.coalesce(1)

        # diretório temporário
        temp_dir = csv_path + "_tmp_dir"

        full_temp_path = f"s3a://{self.bucket}/{temp_dir}"
        full_final_path = f"s3a://{self.bucket}/{csv_path}"

        logger.info(f"Convertendo {parquet_path} -> {full_final_path}")

        # grava csv no diretório temporário
        df_to_write.write \
            .mode("overwrite") \
            .option("header", True) \
            .option("delimiter", ";") \
            .csv(full_temp_path)

        # listar arquivos gerados
        temp_files = self.bucket_connector.list_paths(temp_dir)

        part_file = next((f for f in temp_files if f.endswith(".csv")), None)
        if not part_file:
            raise RuntimeError("Nenhum arquivo .csv encontrado no diretório temporário.")

        # mover o arquivo para o destino final
        self.bucket_connector.move_file(
            source_key=part_file,
            dest_key=csv_path
        )

        # deletar diretório temporário
        self.bucket_connector.delete_prefix(temp_dir)

        logger.info(f"Arquivo csv final gravado como: {full_final_path}")

        return full_final_path
        
    def convert_txt_to_parquet(self, txt_input, parquet_path: str, coalesce_one: bool = True) -> str:
        """
        Converte um arquivo txt ou DataFrame para um único arquivo parquet com o nome final especificado.
        - parquet_path deve ser algo como: 'converted/arquivo.parquet'
        - Spark escreve em um diretório, então renomeamos o arquivo final.
        """

        # aceitar DataFrame ou caminho
        if isinstance(txt_input, DataFrame):
            df_txt = txt_input
        elif isinstance(txt_input, str):
            df_txt = self.read_txt(txt_input)
        else:
            raise ValueError("txt_input deve ser caminho do arquivo ou DataFrame")

        # reduzir para 1 arquivo
        df_to_write = df_txt.coalesce(1) if coalesce_one else df_txt

        # diretório temporário (Spark exige)
        temp_dir = parquet_path + "_tmp_dir"

        full_temp_path = f"s3a://{self.bucket}/{temp_dir}"
        full_final_path = f"s3a://{self.bucket}/{parquet_path}"

        logger.info(f"Convertendo {txt_input} -> {full_final_path}")

        # grava parquet no diretório temporário
        df_to_write.write.mode("overwrite").parquet(full_temp_path)

        # --- renomear arquivo part-00000.parquet ---
        # listar os arquivos gerados
        temp_files = self.bucket_connector.list_paths(temp_dir)

        part_file = next((f for f in temp_files if f.endswith(".parquet")), None)
        if not part_file:
            raise RuntimeError("Nenhum arquivo .parquet encontrado no diretório temporário.")

        # mover o arquivo para o destino final com o nome correto
        self.bucket_connector.move_file(
            source_key=part_file,
            dest_key=parquet_path
        )
        # deletar o diretório temporário
        self.bucket_connector.delete_prefix(temp_dir)

        logger.info(f"Arquivo parquet final gravado como: {full_final_path}")
        return full_final_path 
    
    def convert_csv_to_parquet(self, csv_input, parquet_path: str, base_path: str | None = None, output_path: str | None = None, coalesce_one: bool = True) -> str:
        if isinstance(csv_input, DataFrame):
            df_csv = csv_input
        elif isinstance(csv_input, str):
            if base_path:
                csv_input = f"{base_path.rstrip('/')}/{csv_input.lstrip('/')}"
            df_csv = self.read_csv(csv_input)
        else:
            raise ValueError("csv_input deve ser caminho do arquivo ou DataFrame")

        # reduzir para 1 arquivo
        df_to_write = df_csv.coalesce(1) if coalesce_one else df_csv

        # diretório temporário (Spark exige)
        temp_dir = parquet_path + "_tmp_dir"

        full_temp_path = f"s3a://{self.bucket}/{temp_dir}"
        full_final_path = f"s3a://{self.bucket}/{parquet_path}"

        logger.info(f"Convertendo {csv_input} -> {full_final_path}")

        # grava parquet no diretório temporário
        df_to_write.write.mode("overwrite").parquet(full_temp_path)

        # --- renomear arquivo part-00000.parquet ---
        # listar os arquivos gerados
        temp_files = self.bucket_connector.list_paths(temp_dir)

        part_file = next((f for f in temp_files if f.endswith(".parquet")), None)
        if not part_file:
            raise RuntimeError("Nenhum arquivo .parquet encontrado no diretório temporário.")

        # mover o arquivo para o destino final com o nome correto
        self.bucket_connector.move_file(
            source_key=part_file,
            dest_key=parquet_path
        )
        # deletar o diretório temporário
        self.bucket_connector.delete_prefix(temp_dir)

        logger.info(f"Arquivo parquet final gravado como: {full_final_path}")
        return full_final_path 

    def list_paths(self, prefix: str = "") -> List[str]:
        """
        Lista todas as chaves sob um prefix específico (modo simples).
        """
        keys = []
        continuation_token = None

        while True:
            response = (self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
                if continuation_token else self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            )

            for obj in response.get("Contents", []):
                keys.append(obj["Key"])

            if response.get("IsTruncated"):
                continuation_token = response.get("NextContinuationToken")
            else:
                break

        return keys

    def build_regime_tributario_parquet(self, csv_prefix: str) -> str:
        """
        Lê todos os arquivos .csv dentro de `csv_prefix`,
        concatena e grava como REGIME_TRIBUTARIO/regime_tributario.parquet
        no mesmo bucket.
        """

        logger.info(f"Buscando CSVs em: {csv_prefix}")

        # lista arquivos no bucket
        csv_files = [
            f for f in self.bucket_connector.list_paths(csv_prefix)
            if f.endswith(".csv")
        ]

        if not csv_files:
            raise RuntimeError("Nenhum arquivo .csv encontrado para regime tributário.")

        logger.info(f"Encontrados {len(csv_files)} CSVs para união")

        # lê todos os CSVs
        dfs: list[DataFrame] = []
        for csv in csv_files:
            logger.info(f"Arquivo CSV: {csv}")
            df = self.read_csv(csv)

            if "ano" in df.columns:
                df = df.withColumn("ano", F.substring("ano", 1, 4))
            dfs.append(df)

        # une todos (schemas já iguais)
        logger.info("Realizando UNION dos CSVs...")
        df_final: DataFrame = reduce(lambda a, b: a.unionByName(b), dfs)

        # caminho final parquet
        parquet_path = "REGIME_TRIBUTARIO/regime_tributario.parquet"

        logger.info("Convertendo para parquet final...")
        return self.convert_csv_to_parquet(
            csv_input=df_final,
            parquet_path=parquet_path,
            coalesce_one=True
        )

    def apply_sped_schema(self, df: DataFrame, record_type: str, schema: StructType, field_map: dict) -> DataFrame:
        """
        Aplica schema e casting a um DataFrame SPED já filtrado por REG.
        """

        TECH_COLS = [
            "_row_id",
            "_file_id",
            "_sped_type",
            "_is_parent",
            "_parent_uid",
            "_active_parent_uid",
            "_parent_uid_final",
            "REG"
        ]

        tech_cols_present = [
            c for c in TECH_COLS if c in df.columns
        ]

        # cria colunas a partir da posição no array "cols"
        for field_name, position in field_map.items():
            df = df.withColumn(field_name, F.col("cols")[position])

        # aplica cast conforme StructType
        for field in schema.fields:
            col_name = field.name
            data_type = field.dataType

            if col_name not in df.columns:
                continue

            if isinstance(data_type, DateType):
                df = df.withColumn(
                    col_name,
                    F.to_date(F.col(col_name), "yyyyMMdd")
                )
            else:
                df = df.withColumn(
                    col_name,
                    F.col(col_name).cast(data_type)
                )

        # -------------------------------------------------
        # 4️⃣ SELEÇÃO FINAL (TÉCNICAS + CAMPOS DO LAYOUT)
        # -------------------------------------------------
        business_cols = [f.name for f in schema.fields]

        return df.select(*tech_cols_present, *business_cols)

    def list_files(self, prefix: str):
        return self.bucket_connector.list_paths(prefix)

    def load_text(self, key: str) -> DataFrame:
        return self.spark.read.text(f"s3a://{self.bucket}/{key}")
    
    def extract_sped_fiscal(self, path: str, fmt: str):
        return self.read_parquet(path) if fmt == "parquet" else self.read_csv(path)

    def extract_sped_contribuicoes(self, path: str, fmt: str):
        return self.read_parquet(path) if fmt == "parquet" else self.read_csv(path)

    def extract_cfop_table(self, path: str, fmt: str):
        return self.read_parquet(path) if fmt == "parquet" else self.read_csv(path)
    

    
    
