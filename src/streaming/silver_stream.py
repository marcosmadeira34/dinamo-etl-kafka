# silver_stream.py - correcao de path em write_to_delta
import re
import yaml
import json
import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from streaming.base_stream import BaseStream
from streaming.stream_config import StreamConfig
from kafka.topics import TopicRegistry
from schemas.register_type_sped import SPEDS_REGISTRY_TYPES
from functools import reduce

logger = logging.getLogger("dinamo.streaming.silver")
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)


class SilverStream(BaseStream):
    """
    Enriches Bronze records → Silver layer using existing builders.
    
    Flow per micro-batch:
    1. Read parsed records from sped.parsed.bronze
    2. Group by file_id and sped_type
    3. Apply parent-child relationships (existing SilverSpedBuilder)
    4. Apply SPED schema per register (existing)
    5. Apply field inheritance (existing)
    6. Write to Silver Parquet (partitioned by REG + sped_type)
    7. Publish enriched records to sped.enriched.silver
    """

    def __init__(self, spark: SparkSession, config: StreamConfig, silver_builder):
        """
        Args:
            spark: SparkSession
            config: StreamConfig
            silver_builder: Existing SilverSpedBuilder instance
        """
        super().__init__(spark, config, stream_name="silver-stream")
        self.builder = silver_builder

    # def get_source_channel(self) -> str:
    #     return TopicRegistry.PARSED_BRONZE.name

    def get_source_topic(self) -> str:
        return TopicRegistry.PARSED_BRONZE.name

    def extract_sped_metadata(self, df_0000: DataFrame) -> dict:
        """
        Extrai metadados do REG 0000.

        Retorna:
        {
            "CNPJ": "...",
            "NOME": "...",
            "DT_INI": "...",
            "DT_FIN": "...",
            "FILE_ID": "..."
        }
        """

        if df_0000 is None or df_0000.isEmpty():
            raise ValueError("REG 0000 vazio.")

        row = (
            df_0000
            .select(
                "CNPJ",
                "NOME",
                "DT_INI",
                "DT_FIN",
                "FILE_ID"
            )
            .limit(1)
            .collect()[0]
        )

        return {
            "CNPJ": row["CNPJ"],
            "NOME": row["NOME"],
            "DT_INI": row["DT_INI"],
            "DT_FIN": row["DT_FIN"],
            "FILE_ID": row["FILE_ID"]
        }

   
    def process_micro_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Process a batch of parsed Bronze records.

        FASE 1: agrupa a LEITURA da Bronze por pasta fisica
        (cnpj/sped_type/periodo).

        FASE 2: hierarquia pai-filho (apply_parent_child_relationships) e
        aplicacao de schema por REG (apply_sped_schema) rodam UMA VEZ por
        grupo, nao mais uma vez por arquivo. So foi seguro depois de corrigir
        apply_parent_child_relationships para nao sobrescrever _file_id/
        _sped_type com F.lit(...) quando essas colunas ja existem.

        FASE 3: o agrupamento da Fase 1 (por pasta fisica exata, ou seja,
        por periodo) se mostrou fino demais — um lote com muitos periodos do
        mesmo cnpj (ex.: 340 arquivos / 4 filiais por periodo = ~85
        periodos) gerava ~85 grupos pequenos, cada um pagando o custo fixo
        de hierarquia+schema+cache do zero (medido em ~50 minutos no total).
        Agora o agrupamento e por (cnpj, sped_type), abrangendo TODOS os
        periodos do batch numa UNICA leitura multi-pasta
        (spark.read.parquet(*paths) — aceita varios paths nativamente,
        sem a pegadinha do *args que corrigimos no read_txt() da Bronze).

        Extracao de metadados do REG 0000, propagate_sped_metadata e a
        escrita final continuam POR ARQUIVO, de proposito: propagate_sped_
        metadata tambem usa F.lit(...) com um dicionario de metadados unico
        (CNPJ/NOME/DT_INI variam por arquivo), entao nao pode ser vetorizado
        sem o mesmo tipo de correcao — decisao separada, nao tomada aqui.
        Como essas etapas finais operam sobre dado ja cacheado (materializado
        uma vez pro grupo inteiro), o custo por arquivo aqui e baixo.
        """
        # 1. Parse FILE-LEVEL events from Kafka
        json_schema = "_file_id STRING, _sped_type STRING, file_path STRING, status STRING"
        events_df = (
            batch_df
            .select(F.from_json(F.col("kafka_value"), json_schema).alias("data"))
            .select("data.*")
            .distinct()
            .collect()
        )

        if not events_df:
            return

        valid_events = [m for m in events_df if m["_file_id"] and m["_sped_type"]]
        if not valid_events:
            return

        logger.info(f"[Silver] Processing batch {batch_id} | files={len(valid_events)}")

        config_files = {
            "EFD_CONTRIB": ("sped_contrib_config.yaml", "parent_child_mapping_contrib"),
            "EFD_FISCAL":  ("sped_fiscal_config.yaml",  "parent_child_mapping_fiscal"),
            "ECD":         ("sped_ecd_config.yaml",     "parent_child_mapping_ecd"),
            "ECF":         ("sped_ecf_config.yaml",     "parent_child_mapping_ecf"),
        }

        # FASE 3: agrupa por (cnpj, sped_type) — abrange TODOS os periodos
        # do mesmo cnpj+tipo presentes no batch numa UNICA leitura
        # multi-pasta (spark.read.parquet aceita *paths nativamente, sem a
        # pegadinha do *args que corrigimos no read_txt() da Bronze).
        # Antes (Fase 1), o agrupamento era por pasta fisica exata
        # (cnpj/tipo/periodo) — para um lote com muitos periodos do mesmo
        # cnpj (ex.: 340 arquivos / 4 filiais por periodo = ~85 periodos),
        # isso gerava ~85 grupos pequenos, cada um pagando o custo fixo de
        # hierarquia+schema+cache do zero — medido em ~50 min no total.
        # Agrupando por cnpj+tipo, o mesmo lote vira potencialmente 1 unico
        # grupo, pagando esse custo fixo uma vez so — mesmo principio que
        # ja funciona bem no Bronze.
        files_by_cnpj_type: dict = {}
        for meta in valid_events:
            file_id = meta["_file_id"]
            sped_type = meta["_sped_type"]
            file_path = meta["file_path"] or ""
            _meta = re.search(r'TO_CONVERT/(\d+)/[^/]+/(\d{4,6})/', file_path)
            cnpj_path = _meta.group(1) if _meta else "unknown"
            periodo_path = _meta.group(2) if _meta else "unknown"
            bronze_path = f"{self.config.bronze_path}/{cnpj_path}/{sped_type}/{periodo_path}"

            key = (cnpj_path, sped_type)
            entry = files_by_cnpj_type.setdefault(key, {"file_entries": [], "bronze_paths": set()})
            entry["file_entries"].append((file_id, sped_type, file_path))
            entry["bronze_paths"].add(bronze_path)

        processed_events = []

        for (cnpj_path, sped_type), group_info in files_by_cnpj_type.items():
            file_entries = group_info["file_entries"]
            bronze_paths = sorted(group_info["bronze_paths"])
            file_ids_in_path = {fid for fid, _, _ in file_entries}
            group_label = f"{cnpj_path}/{sped_type}"
            logger.info(
                f"[Silver] Lendo {len(bronze_paths)} pasta(s) Bronze de {group_label} "
                f"({len(file_entries)} arquivo(s))..."
            )

            try:
                # NAO usar spark.read.parquet(*bronze_paths): quando os
                # periodos tem conjuntos DIFERENTES de REG presentes (comum —
                # nem todo periodo tem, por exemplo, REG=E100), o Spark tenta
                # inferir um esquema de particao UNICO entre todas as pastas
                # e falha com "Conflicting directory structures detected".
                # Le cada periodo separado (deixa o Spark inferir REG por
                # pasta, isolado) e uniona depois — union e barato
                # comparado ao custo de hierarquia/schema que ainda roda
                # so 1 vez sobre o resultado unido.
                with self.spark_job_label(f"[Silver] ler bronze — {group_label} ({len(bronze_paths)} periodos)"):
                    dfs_por_periodo = []
                    paths_com_falha = []
                    for path in bronze_paths:
                        try:
                            dfs_por_periodo.append(self.spark.read.parquet(path))
                        except Exception as read_err:
                            logger.warning(f"[Silver] Falha lendo periodo {path}: {read_err}")
                            paths_com_falha.append(path)

                    if not dfs_por_periodo:
                        raise RuntimeError(f"Nenhuma pasta Bronze pode ser lida em {group_label}")

                    df_bronze_group = reduce(
                        lambda a, b: a.unionByName(b, allowMissingColumns=True),
                        dfs_por_periodo
                    )
                    sample_rows = df_bronze_group.select("_file_id").limit(50).collect()
                sample_ids = {r["_file_id"] for r in sample_rows}
                if sample_rows and not (sample_ids & file_ids_in_path):
                    raise RuntimeError(
                        f"Amostra de _file_id em {group_label} nao bate com nenhum arquivo "
                        f"esperado. Amostra: {list(sample_ids)[:5]}"
                    )

                registry = SPEDS_REGISTRY_TYPES.get(sped_type)
                if not registry:
                    raise RuntimeError(f"SPED nao registrado: {sped_type}")

                schemas_sped = registry["schemas"]
                fields_map_sped = registry["fields_map"]

                yaml_file, mapping_key = config_files[sped_type]
                yaml_path = f"/opt/spark/app/src/config/{yaml_file}"
                with open(yaml_path, "r") as f:
                    sped_config = yaml.safe_load(f)
                parent_child_mapping = sped_config[mapping_key]

                # FASE 2: hierarquia pai-filho UMA VEZ para o grupo inteiro
                # (agora seguro — apply_parent_child_relationships nao
                # sobrescreve mais _file_id/_sped_type quando ja presentes,
                # e o Window.partitionBy("_source_file") interno ja isola
                # corretamente cada arquivo dentro do grupo).
                with self.spark_job_label(f"[Silver] hierarquia pai-filho — {group_label}"):
                    df_group = self.builder.apply_parent_child_relationships(
                        df_bronze_group, parent_child_mapping
                    )
                    # PERF: localCheckpoint quebra o plano logico acumulado
                    # pelas Window functions — feito 1x pro grupo inteiro
                    # agora, nao mais 1x por arquivo.
                    df_group = df_group.localCheckpoint(eager=True)

                # FASE 2: schema aplicado por REG, UMA VEZ para todos os
                # arquivos do grupo (apply_sped_schema e puramente por
                # linha/posicao em "cols", sem suposicao de arquivo unico).
                regs_presentes = {
                    r["REG"] for r in df_group.select("REG").distinct().collect()
                }
                dfs_by_reg = {}
                for reg, schema in schemas_sped.items():
                    if reg not in fields_map_sped or reg not in regs_presentes:
                        continue
                    field_map = fields_map_sped[reg]
                    self.builder.validate_schema_vs_map(schema, field_map, reg)
                    df_reg = df_group.where(F.col("REG") == reg)
                    df_reg = self.builder.apply_sped_schema(
                        df=df_reg, schema=schema, field_map=field_map, record_type=reg
                    )
                    dfs_by_reg[reg] = df_reg

                df_0000 = dfs_by_reg.get("0000")
                if df_0000 is None:
                    raise RuntimeError(f"REG 0000 nao encontrado/processado no grupo {group_label}")

                # Metadados POR ARQUIVO extraidos do REG 0000 do grupo —
                # 1 linha por arquivo (REG 0000 aparece 1x por SPED), coleta
                # pequena mesmo com muitos arquivos/periodos no grupo.
                meta_rows = (
                    df_0000
                    .select("_file_id", "CNPJ", "DT_INI", "NOME")
                    .distinct()
                    .collect()
                )
                metadata_by_file = {r["_file_id"]: r for r in meta_rows}

                # Union UMA VEZ para o grupo inteiro — o filtro por arquivo
                # roda depois, sobre dado ja materializado (cache), nao
                # recomputa hierarquia/schema por arquivo.
                df_silver_group = reduce(
                    lambda a, b: a.unionByName(b, allowMissingColumns=True),
                    dfs_by_reg.values()
                )
                df_silver_group = df_silver_group.cache()
                with self.spark_job_label(f"[Silver] materializar grupo — {group_label}"):
                    df_silver_group.count()

            except Exception as e:
                logger.error(f"[Silver] Erro processando grupo {group_label} ({sped_type}): {e}", exc_info=True)
                for file_id, sped_type_, file_path in file_entries:
                    self._report_processing_failure_to_dlq(
                        file_id=file_id, sped_type=sped_type_, file_path=file_path,
                        error_message=f"Erro no grupo {group_label}: {e}"[:1024],
                    )
                try:
                    df_silver_group.unpersist()
                except Exception:
                    pass
                continue

            # A partir daqui, POR ARQUIVO: so operacoes leves (metadados ja
            # coletados, filtro sobre dado cacheado, propagate_sped_metadata
            # via F.lit — que so faz sentido por arquivo, ja que CNPJ/NOME/
            # DT_INI diferem entre arquivos — e a escrita final).
            for file_id, sped_type_, file_path in file_entries:
                logger.info(f"[Silver] Enriching file: {file_id} | type: {sped_type_}")
                try:
                    meta_row = metadata_by_file.get(file_id)
                    if meta_row is None:
                        logger.error(f"[Silver] REG 0000 nao encontrado para {file_id} dentro do grupo")
                        self._report_processing_failure_to_dlq(
                            file_id=file_id, sped_type=sped_type_, file_path=file_path,
                            error_message="REG 0000 nao encontrado para este arquivo dentro do grupo.",
                        )
                        continue

                    metadata = {
                        "FILE_ID":     file_id,
                        "OBRIG_ACESS": sped_type_,
                        "DT_INI":      meta_row["DT_INI"],
                        "NOME":        meta_row["NOME"],
                        "CNPJ":        meta_row["CNPJ"],
                    }
                    cnpj_root    = metadata["CNPJ"] or ""
                    cnpj_8_digits = re.sub(r"\D", "", str(cnpj_root))[:8]
                    dt_ini        = str(metadata["DT_INI"] or "")
                    month_year    = f"{dt_ini[4:8]}{dt_ini[2:4]}" if len(dt_ini) >= 8 else ""

                    if len(cnpj_8_digits) != 8 or len(month_year) != 6:
                        logger.error(
                            f"[Silver] Metadados invalidos extraidos do REG 0000 para {file_id} "
                            f"(cnpj='{cnpj_8_digits}', month_year='{month_year}') - "
                            "abortando escrita para nao gravar em path inconsistente."
                        )
                        self._report_processing_failure_to_dlq(
                            file_id=file_id, sped_type=sped_type_, file_path=file_path,
                            error_message=f"Metadados invalidos do REG 0000 (cnpj='{cnpj_8_digits}', month_year='{month_year}').",
                        )
                        continue

                    silver_base = f"{self.config.silver_path}/{cnpj_8_digits}/{sped_type_}/{month_year}"

                    df_file_silver = df_silver_group.filter(F.col("_file_id") == file_id)
                    df_file_silver = self.builder.propagate_sped_metadata(metadata, df_file_silver)

                    logger.info(f"[Silver] Escrevendo para o caminho: {silver_base}")
                    with self.spark_job_label(f"[Silver] escrever silver — {sped_type_}/{cnpj_8_digits}/{month_year}"):
                        self.write_to_delta(
                            df_file_silver, silver_base, partition_by=["REG"],
                            replace_where=f"_file_id = '{file_id}'",
                        )

                    processed_events.append({
                        "_file_id": file_id,
                        "_sped_type": sped_type_,
                        "cnpj_8_digits": cnpj_8_digits,
                        "month_year": month_year,
                        "status": "PROCESSED_SILVER",
                    })

                    logger.info(f"[Silver] File enriched: {file_id} | registers={list(dfs_by_reg.keys())}")

                except Exception as e:
                    logger.error(f"[Silver] Error enriching {file_id}: {e}", exc_info=True)
                    self._report_processing_failure_to_dlq(
                        file_id=file_id, sped_type=sped_type_, file_path=file_path,
                        error_message=str(e)[:1024],
                    )
                    continue

            df_silver_group.unpersist()

        # 7. Publica TODOS os eventos de sucesso do batch numa unica escrita
        # Kafka, em vez de 1 write_to_kafka por arquivo.
        if processed_events:
            event_df = self.spark.createDataFrame(processed_events)
            silver_kafka_df = (
                event_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            with self.spark_job_label(f"[Silver] publicar Kafka batch {batch_id} ({len(processed_events)} eventos)"):
                self.write_to_kafka(silver_kafka_df, TopicRegistry.ENRICHED_SILVER.name)
            logger.info(f"[Silver] {len(processed_events)} arquivo(s) processado(s) e evento(s) publicado(s) no Kafka")

    def _report_processing_failure_to_dlq(self, file_id: str, sped_type: str, file_path: str, error_message: str):
        """Publica em sped.errors.dlq a falha isolada de UM arquivo — mesmo
        padrao/envelope usado pelo Bronze (_failed_stream/_failed_at
        compativeis com o filtro do republish_dlq.py), MAIS original_topic/
        kafka_value_truncated com o evento de handoff Bronze->Silver
        reconstruido — sem isso o republish_dlq.py nao consegue saber pra
        onde/o-que republicar (mesma lacuna corrigida no Bronze)."""
        try:
            original_event = {
                "_file_id": file_id,
                "_sped_type": sped_type,
                "file_path": file_path,
                "status": "PROCESSED_BRONZE",
            }
            error_df = self.spark.createDataFrame([{
                "_file_id": file_id,
                "_sped_type": sped_type,
                "status": "PROCESSING_FAILED",
                "_error_message": error_message[:1024],
                "_failed_stream": self.stream_name,
                "_failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "original_topic": TopicRegistry.PARSED_BRONZE.name,
                "kafka_value_truncated": json.dumps(original_event)[:4096],
            }])
            error_kafka_df = (
                error_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            self.write_to_kafka(error_kafka_df, TopicRegistry.ERRORS_DLQ.name)
        except Exception as dlq_error:
            logger.critical(f"[Silver] Failed to report processing failure to DLQ: {dlq_error}")