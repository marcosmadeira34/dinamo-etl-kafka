from typing import Dict, List, Optional, Tuple
# dinamo_web/src/dinamo_web/silver_sped_builder/silver_layer.py
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F, Row
from pyspark.sql.types import StructType, DateType
import logging
from typing import Dict, Iterable
from pyspark.sql import Row
import re
from pyspark.sql.types import StructType, StructField, StringType, LongType

from pyspark.sql.functions import spark_partition_id

class SilverSpedBuilder:
    """
    Tratamento e limpezas iniciais do arquivos brutos SPEDs extraídos do bucket para
    preparação à etapa de aplicação das regras de negócios (Business Logic).
    """
    def __init__(self, spark: SparkSession, regime_config: dict, tabela_cfop_config: dict):
        self.spark = spark
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        # Carrega o arquivo de regime tributário como padrão da classe
        bucket = regime_config["bucket_name"]
        path = regime_config["path"]

        parquet_path = f"s3a://{bucket}/{path}"
        
        # print(f"Os parquet_path são: {parquet_path}")

        # self.logger.info(f"Carregando regime tributário de: {parquet_path}")

        self.regime_df = (
            spark.read
            .parquet(parquet_path)
            .select("ano", "cnpj", "regime")
        )
        # logger.info("Arquivo de regime tributário carregado com sucesso.")

        tabela_cfop_bucket = tabela_cfop_config["bucket_name"]
        tabela_path = tabela_cfop_config["path"]

        tabela_cfop_path = f"s3a://{tabela_cfop_bucket}/{tabela_path}"

        # Carrega o arquivo tabela CFOP
        self.tabela_cfop_df = (
            spark.read
            .parquet(tabela_cfop_path)
        )
        self.logger = logging.getLogger("SPEDTransformer")

    # Extrai metadados do REG 0000
    def extract_sped_metadata(self, df_0000: DataFrame) -> dict:
        self.logger.info("Extraindo metadados do REG 0000")

        row = (
            df_0000
            .select(
                F.col("CNPJ").alias("CNPJ"),
                F.col("NOME").alias("NOME"),
                F.col("_file_id").alias("FILE_ID"), 
                F.col("_sped_type").alias("OBRIG_ACESS"),
                F.col("DT_INI").alias("DT_INI"),
                # F.col("DT_FIN").alias("DT_FIN")
            )
            .limit(1)
            .collect()
        )

        if not row:
            raise ValueError("Registro 0000 não encontrado ou vazio")

        return row[0].asDict()

    # Propaga os metadados extraidos para aplicar no enriquecimento da camada Gold posteriormente
    def propagate_sped_metadata(self, metadata: dict, silver_df: DataFrame) -> DataFrame:
        """
        Propaga os metadados extraídos para para aplicar o enriquecimento da camada Gold posteriormente com 
        as colunas técnicas de vínculo.
        """
        self.logger.info("Iniciando propagaao de metadados do arquivo SPED")

        return (
            silver_df
            .withColumn("FILE_ID",      F.lit(metadata["FILE_ID"]))
            .withColumn("CNPJ",         F.lit(metadata["CNPJ"]))
            .withColumn("NOME",         F.lit(metadata["NOME"]))
            .withColumn("OBRIG_ACESS",  F.lit(metadata["OBRIG_ACESS"]))
            .withColumn("DT_INI",       F.lit(metadata["DT_INI"]))
        )


    # Extrai o CNPJ diretamente do REG 0000 a partir do array 'cols'.
    def extract_cnpj_from_0000(self, df: DataFrame, field_map_0000: dict) -> str:
        """
        Extrai o CNPJ diretamente do REG 0000 a partir do array 'cols'.
        Não depende de schema aplicado.
        Zero shuffle.
        """

        cnpj_pos = field_map_0000["CNPJ"]

        row = (
            df
            .where(F.col("REG") == "0000")
            .select(F.col("cols")[cnpj_pos].alias("CNPJ"))
            .filter(F.col("CNPJ").isNotNull())
            .limit(1)
            .collect()
        )

        if not row:
            raise ValueError("CNPJ não encontrado no REG 0000")

        return row[0]["CNPJ"]
        
    def apply_parent_child_relationships(self, df_parsed, parent_child_mapping, file_id=None, sped_type=None):
        """
        Aplica hierarquia pai-filho usando Window functions nativas do Spark.

        Substitui a versao anterior que usava rdd.mapPartitions() (Python UDF implicita
        via RDD — serializa/deserializa TODAS as linhas para Python, alto custo de CPU
        e memoria, proporcional ao volume do arquivo).

        Nova abordagem:
        - Tudo em Spark SQL nativo (JVM), sem serializar dados para Python
        - Window.partitionBy(_source_file).orderBy(_row_id): garante ordem por arquivo
        - last(uid, ignorenulls=True) propaga o _row_id do pai mais recente para o filho
        - override_regs (children_override) sempre recebem o root_uid (REG 0000)
        - Custo: 1 sort por particao (ja necessario) + N window functions (uma por REG pai),
          todas executadas em JVM sem round-trip Python

        SEGURANCA MULTI-ARQUIVO: file_id/sped_type agora sao OPCIONAIS. Se
        df_parsed ja tiver as colunas _file_id/_sped_type (caso normal, pois
        a Bronze ja grava esses valores corretos por linha, derivados de
        _source_file), a funcao NAO sobrescreve — preserva o valor real de
        cada linha. Isso e o que torna seguro chamar esta funcao com um
        DataFrame contendo VARIOS arquivos de uma vez: antes, o antigo
        F.lit(file_id) incondicional carimbava TODAS as linhas com um unico
        valor, corrompendo o dado nesse cenario. O fallback com F.lit(...)
        so acontece se a coluna realmente nao existir (compatibilidade com
        chamadas antigas que ainda passem um DataFrame sem essas colunas).
        Todo o resto da funcao ja opera por linha via _source_file/_row_id
        (Window.partitionBy("_source_file")), entao nao precisou de nenhuma
        outra mudanca para suportar multiplos arquivos simultaneamente.
        """
        self.logger.info("Iniciando aplicacao da hierarquia pai-filho (modo Spark nativo)")

        # =====================================================
        # 1. PREPARACAO
        # =====================================================
        df = df_parsed

        if "_file_id" not in df.columns:
            if file_id is None:
                raise ValueError(
                    "df_parsed nao possui coluna _file_id e nenhum file_id foi "
                    "fornecido como fallback."
                )
            df = df.withColumn("_file_id", F.lit(file_id))

        if "_sped_type" not in df.columns:
            if sped_type is None:
                raise ValueError(
                    "df_parsed nao possui coluna _sped_type e nenhum sped_type foi "
                    "fornecido como fallback."
                )
            df = df.withColumn("_sped_type", F.lit(sped_type))

        df = df.withColumn(
            "REG",
            F.regexp_replace(
                F.trim(F.upper(F.split(F.col("value"), "\\|").getItem(1))),
                r'[^A-Z0-9]',
                ''
            )
        )

        # =====================================================
        # 2. BUILD MAPPING (driver - leve, sem broadcast necessario
        #    porque nao vai para os executores via Python)
        # =====================================================
        parent_map   = {}   # child -> parent_reg
        override_set = set() # regs que sempre vao para o root (0000)

        for item in parent_child_mapping:
            parent = item["parent"]
            for child in item.get("children", []):
                parent_map[child] = parent
            for child in item.get("children_override", []):
                override_set.add(child)

        # =====================================================
        # 3. JANELA DE ORDEM - por arquivo, ordenada por _row_id
        #    sem shuffle global (repartition ja estava no original)
        # =====================================================
        w_file = (
            Window
            .partitionBy("_source_file")
            .orderBy("_row_id")
            .rowsBetween(Window.unboundedPreceding, -1)
        )

        # =====================================================
        # 4. ROOT UID: _row_id do REG 0000 de cada arquivo
        #    last(nulls ignorados) dentro da janela acumulada
        #    retorna o _row_id do 0000 ja visto antes de cada linha
        # =====================================================
        df = df.withColumn(
            "_root_uid",
            F.last(
                F.when(F.col("REG") == "0000", F.col("_row_id")),
                ignorenulls=True
            ).over(w_file)
        )

        # =====================================================
        # 5. PARENT UID POR REG
        #    Para cada REG pai que aparece no mapeamento,
        #    propaga seu _row_id para as linhas seguintes
        #    usando last(ignorenulls=True) na janela acumulada.
        # =====================================================
        # Conjunto de REGs que sao pais (precisam de coluna auxiliar)
        parent_regs = set(parent_map.values())

        # Cria uma coluna _uid_<PAI> para cada REG pai
        for parent_reg in parent_regs:
            df = df.withColumn(
                f"_uid_{parent_reg}",
                F.last(
                    F.when(F.col("REG") == parent_reg, F.col("_row_id")),
                    ignorenulls=True
                ).over(w_file)
            )

        # =====================================================
        # 6. RESOLVE _expected_parent e _parent_uid
        # =====================================================
        # Monta expressao CASE WHEN para _expected_parent
        expected_expr = F.lit(None).cast("string")
        for child_reg, parent_reg in parent_map.items():
            expected_expr = F.when(
                F.col("REG") == child_reg, F.lit(parent_reg)
            ).otherwise(expected_expr)

        # Monta expressao CASE WHEN para _parent_uid:
        # - override_set -> root_uid
        # - demais        -> _uid_<PAI> se existir, senao root_uid
        parent_uid_expr = F.lit(None).cast("long")
        for child_reg in override_set:
            parent_uid_expr = F.when(
                F.col("REG") == child_reg, F.col("_root_uid")
            ).otherwise(parent_uid_expr)

        for child_reg, parent_reg in parent_map.items():
            if child_reg in override_set:
                continue  # ja tratado acima
            uid_col = f"_uid_{parent_reg}"
            parent_uid_expr = F.when(
                F.col("REG") == child_reg,
                F.coalesce(F.col(uid_col), F.col("_root_uid"))
            ).otherwise(parent_uid_expr)

        df = (
            df
            .withColumn("_expected_parent",  expected_expr)
            .withColumn("_parent_uid",        parent_uid_expr)
            .withColumn("_parent_uid_final",  F.col("_parent_uid"))
        )

        # Remove colunas auxiliares temporarias (_uid_<PAI> e _root_uid)
        aux_cols = ["_root_uid"] + [f"_uid_{r}" for r in parent_regs]
        df_result = df.drop(*aux_cols)

        self.logger.info("Hierarquia pai-filho aplicada com sucesso (Spark nativo)")
        return df_result

        
    # Aplica herança de campos entre registros pai → filho
    def apply_field_inheritance(self, dfs_by_reg: Dict[str, DataFrame], inheritance_map: dict) -> Dict[str, DataFrame]:
        """
        Aplica herança de campos entre registros pai → filho.
        Trabalha EXCLUSIVAMENTE com múltiplos DataFrames por REG.
        """
        self.logger.info(f"Iniciando aplicacao da heranca de campos no REG {dfs_by_reg.keys()}")
        result = dfs_by_reg.copy()

        for child_reg, rule in inheritance_map.items():
            parent_reg = rule["from"]
            fields = rule["fields"]

            if child_reg not in result or parent_reg not in result:
                continue

            parent_df = (
                result[parent_reg]
                .select(
                    F.col("_parent_uid").alias("PARENT_UID"),
                    *[F.col(f) for f in fields]
                )
            )
            child_df = result[child_reg]

            child_df = (
                child_df
                .join(
                    parent_df,
                    child_df["_parent_uid_final"] == parent_df["PARENT_UID"],
                    "left"
                )
                .drop("PARENT_UID")
            )
            result[child_reg] = child_df
        self.logger.info(f"Colunas do pai {parent_reg}: {result[parent_reg].columns}")
        self.logger.info(f"Colunas do filho {child_reg}: {result[child_reg].columns}")
        parent_cols = set(result[parent_reg].columns)

        invalid = [f for f in fields if f not in parent_cols]

        if invalid:
            raise ValueError(
                f"Configuração inválida de herança: "
                f"{child_reg} tenta herdar campos inexistentes em {parent_reg}: {invalid}"
            )

        return result

    # Valida se o mapemanto dos campos dos registros SPED então de acordo com os schemas definidos
    def validate_schema_vs_map(self,schema, field_map, reg):
        # self.logger.info("Validando schema vs map")

        # validação de estrutura dos campos mapeados antes de aplicar o schema
        
        if not isinstance(field_map, dict):
            raise TypeError(f"Field map do REG {reg} deve ser um dict mas é {type(field_map)}\n"
            f"Conteúdo: {field_map}"
            )

        schema_fields = [f.name.strip() for f in schema.fields]
        map_fields = [k.strip() for k in field_map.keys()]

        if schema_fields != map_fields:
            diffs = []

            max_len = max(len(schema_fields), len(map_fields))
            for i in range(max_len):

                s = schema_fields[i] if i < len(schema_fields) else None
                m = map_fields[i] if i < len(map_fields) else None

                if s != m:
                    diffs.append(
                        f"pos {i} -> schema='{s}' | map='{m}'"
                    )

            raise ValueError(
                f"Schema e fields_map divergentes no REG {reg}\n"
                f"Diferenças:\n" + "\n".join(diffs) +
                f"\n\nSchema: {schema_fields}\n"
                f"Map: {map_fields}"
            )
        # self.logger.info(f"Schema vs map validado com sucesso para o REG {reg}.")

    # Escreve os registros SPED particionados por REG em formato Parquet.
    def write_by_register(self, df: DataFrame, record_types: List[str], output_path: str, num_partitions: int = None):

        """
        Escrita massivamente paralela da Silver Layer
        utilizando schema canônico único + Delta Lake.

        Estrutura final:

        SPED_SILVER_LAYER/
            _delta_log/
            REG=0000/
            REG=0110/
            REG=C100/
            REG=C170/
        """

        self.logger.info(
            "Iniciando escrita massivamente paralela da Silver Layer"
        )

        # ============================================================
        # FILTRA SOMENTE REGS NECESSÁRIOS
        # ============================================================

        df = df.where(F.col("REG").isin(record_types))

        # ============================================================
        # CRIA SCHEMA CANÔNICO ÚNICO
        # ============================================================

        canonical_columns = sorted(
            set(df.columns)
        )

        # garante existência de todas as colunas
        for col_name in canonical_columns:

            if col_name not in df.columns:

                df = df.withColumn(
                    col_name,
                    F.lit(None).cast(StringType())
                )

        # ============================================================
        # NORMALIZA ORDEM DAS COLUNAS
        # ============================================================

        df = df.select(*canonical_columns)

        # ============================================================
        # REPARTITION REAL PARA PARALELISMO
        # ============================================================

        if num_partitions is None:

            num_partitions = (
                self.spark.sparkContext.defaultParallelism * 2
            )

        df = df.repartition(
            num_partitions,
            "REG"
        )

        # ============================================================
        # ESCRITA ÚNICA MASSIVAMENTE PARALELA
        # ============================================================

        (
            df.write
            .format("delta")
            .mode("overwrite")
            # .option("overwriteSchema", "true")
            .option("compression", "snappy")
            .option("maxRecordsPerFile", 500000)
            .partitionBy("REG") # dynamic partition overwrite ativo, não precisa do overwriteSchema
            .save(output_path)
        )

        self.logger.info(
            f"Silver Layer escrita com sucesso em {output_path}"
        )


    # Cria novas colunas nos dataframes SPED
    def create_new_columns(self, dataframes: Dict[str, 'DataFrame'], columns: Iterable[str], 
                        default_value=None, 
                        records: Iterable[str] | None = None) -> Dict[str, 'DataFrame']:
        """
        Cria múltiplas colunas nos dataframes SPED.

        :param dataframes: dict {registro: DataFrame}
        :param columns: lista de nomes das colunas a criar
        :param default_value: valor default (None, 0, '', etc)
        :param records: registros SPED onde aplicar (None = todos)
        """

        for record, df in dataframes.items():

            if records and record not in records:
                continue

            for column_name in columns:
                df = df.withColumn(column_name, F.lit(default_value))

            dataframes[record] = df  # 🔴 ESSENCIAL
        
        # self.logger.info("Novas colunas criadas com sucesso.")
        return dataframes
    
    # Verifica o CNPJ no registro 0000
    def verify_cnpj(self, dataframes: Dict[str, 'DataFrame']) -> str:
        search_field = dataframes.get("0000")
        if search_field is None:
            raise ValueError("Registro 0000 não encontrado no DataFrame.")

        else:
            found_cnpj = search_field.select("CNPJ").first()[0][:8]
            # self.logger.info(f"CNPJ encontrado: {found_cnpj}")
            return found_cnpj

    # Verifica o regime tributário 
    def verify_tax_regime(self, cnpj: str, ano: int | None = None) -> str:
        # 🔹 Remove qualquer máscara (., /, -)
        cnpj = re.sub(r"\D", "", str(cnpj))
        
        # 🔹 Extrai apenas os 8 primeiros dígitos (CNPJ base)
        cnpj_base = cnpj[:8]

        self.logger.info(f"Busca inicial feita para CNPJ base {cnpj_base}")

        df = self.regime_df.filter(
            F.col("cnpj") == cnpj_base
        )

        if ano is not None:
            self.logger.info(
                f"Buscando regime tributário para CNPJ base {cnpj_base} no ano {ano}"
            )

            df = (
                df.filter(F.col("ano") <= ano)
                .orderBy(F.col("ano").desc())
            )

        row = df.select("regime").first()

        if not row:
            self.logger.warning(
                f"Regime tributário não encontrado para CNPJ base {cnpj_base}"
            )
            return "NAO_LOCALIZADO"

        self.logger.info(
            f"Regime tributário encontrado para {cnpj_base}: {row['regime']}"
        )

        return row["regime"]
    
    # Aplica o regime tributário aos dataframes SPED
    def apply_tax_regime(self, dataframes: Dict[str, 'DataFrame'], regime: str) -> Dict[str, 'DataFrame']:
        """
        Aplica o regime tributário aos dataframes SPED.

        :param dataframes: dicionário de dataframes SPED
        :param regime: regime tributário a aplicar
        :return: dicionário de dataframes SPED com regime tributário aplicado
        """

        for record, df in dataframes.items():
            df = df.withColumn("REGIME_TRIBUTARIO", F.lit(regime))
            dataframes[record] = df

        self.logger.info("Regime tributário aplicado aos dataframes.")
        return dataframes

    # Aplica regras de CFOP
    def apply_cfop_rules(self, dataframes: Dict[str, DataFrame], records: Iterable[str]) -> Dict[str, DataFrame]:
        """
        Aplica enriquecimento de CFOP aos registros informados
        usando a tabela CFOP carregada no construtor.
        """
        self.logger.info("Iniciando regras de CFOP")

        # para registros que não tiverem CFOP
        outs = {}

        # 🔹 Normaliza CFOP da tabela oficial
        cfops_columns = (
            self.tabela_cfop_df
            .select(
                F.lpad(F.col("cfop").cast("string"), 4, "0").alias("CFOP"),
                F.col("desc_cfop").alias("DESC_CFOP"),
                F.col("nat_cfop").alias("NAT_CFOP"),
                F.col("cred_deb_cbs_ibs").alias("CRED_DEB_CBS_IBS"), # Define se vai ter credito ou debito 
                # pode adicionar mais colunas se necessário
                
                
            )
            .dropDuplicates(["CFOP"])
        )

        for record in records:
            df = dataframes.get(record)

            if df is None:
                self.logger.info(f"Registro {record} na regra de CFOP não encontrado")
                continue

            if record not in records:
                return dataframes

            if "CFOP" not in df.columns:
                self.logger.info(f"Registro {record} na regra de CFOP não encontrado")
                outs[record] = df
                continue

            # 🔹 Normaliza CFOP do SPED antes do join
            df = (
                df
                .withColumn(
                    "CFOP",
                    F.lpad(F.col("CFOP").cast("string"), 4, "0")
                )
                .join(
                    F.broadcast(cfops_columns),
                    on="CFOP",
                    how="left"
                )
            )

            dataframes[record] = df
            self.logger.info(f"CFOP aplicado ao registro {record}")

        # self.logger.info("Regras de CFOP finalizadas")
        return dataframes
    
    # Aplica schema e casting a um DataFrame SPED já filtrado por REG.
    def apply_sped_schema(self, df: DataFrame, record_type: str, schema: StructType, field_map: dict) -> DataFrame:
        """
        Aplica schema e casting a um DataFrame SPED já filtrado por REG.
        """

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

        tech_cols = [c for c in df.columns if c.startswith("_")]

        #business_cols = [f.name for f in schema.fields]

        # if record_type == "C170":
        #     df = df.withColumnRenamed('VL_ICMS', 'VL_ICMS_ITEM')
        #     self.logger.info(f"Coluna VL_ICMS renomeada na camada Silver para VL_ICMS_ITEM no registro C170. Evidência: {df.filter(F.col('VL_ICMS_ITEM').isNotNull()).count()}")

        # if record_type == "C190":
        #     df = df.withColumnRenamed('VL_ICMS', 'VL_ICMS_TOTAL')
        #     self.logger.info(f"Coluna VL_ICMS renomeada na camada Silver para VL_ICMS_TOTAL no registro C190. Evidência: {df.filter(F.col('VL_ICMS_TOTAL').isNotNull()).count()}")
    
        # ----------------------------
        # CONTRATO DE SAÍDA = DF REAL
        # ----------------------------
        tech_cols = [c for c in df.columns if c.startswith("_")]
        business_cols = [c for c in df.columns if not c.startswith("_") and c != "cols"]

        # # ajusta business_cols se houve rename
        # rename_map = {
        #     "C170": {"VL_ICMS": "VL_ICMS_ITEM"},
        #     "C190": {"VL_ICMS": "VL_ICMS_TOTAL"},
        # }

        # if record_type in rename_map:
        #     business_cols = [
        #         rename_map[record_type].get(c, c)
        #         for c in business_cols
        #     ]

        return df.select(*(tech_cols + business_cols))

    
    def write_silver_with_metadata(self, df_parsed: DataFrame, file_id: str, sped_type: str,
        record_types: List[str],
        output_path: str,
        num_partitions: int = None,
    ) -> DataFrame:
        """
        Orquestra: extrai metadados do 0000, propaga para todas as linhas,
        e escreve a Silver particionada por REG.
        Retorna o df_enriched para reuso (ex: apply_parent_child_relationships já aplicado antes).
        """
        df_0000 = df_parsed.filter(F.col("REG") == "0000")
        metadata = self.extract_sped_metadata(df_0000)

        df_enriched = self.propagate_sped_metadata(metadata, df_parsed)

        self.write_by_register(
            df_enriched,
            record_types,
            output_path,
            num_partitions=num_partitions,
        )

        return df_enriched