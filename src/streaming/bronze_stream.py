# dinamo_web/src/dinamo_web/streaming/bronze_stream.py
"""
Bronze Streaming Job — reads file arrival events from Kafka,
fetches SPED files from OCI bucket, parses them, and writes to
Bronze Parquet + sped.parsed.bronze topic.

Integration with existing modules:
- BucketExtractor.read_txt() for file reading
- BucketExtractor.parse_sped_records() for SPED parsing
- SPEDIdentifier for file type detection
- SilverSpedBuilder.apply_parent_child_relationships() for hierarchy
"""
import os
import re
import json
import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from streaming.base_stream import BaseStream
from streaming.stream_config import StreamConfig
from kafka.topics import TopicRegistry
from extractors.bucket_extractor import BucketExtractor
from connectors.bucket_connector import BucketConnector
from schemas.identify_type_sped import SPEDIdentifier
from schemas.register_type_sped import SPEDS_REGISTRY_TYPES

logger = logging.getLogger("dinamo.streaming.bronze")
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)


class BronzeStream(BaseStream):
    """
    Processes raw SPED file events → parsed Bronze records.
    
    Flow per micro-batch:
    1. Deserialize file arrival events from Kafka
    2. For each file: read TXT from OCI bucket (existing BucketExtractor)
    3. Identify SPED type (existing SPEDIdentifier)
    4. Parse SPED records (existing parse_sped_records)
    5. Write to Bronze Parquet (partitioned by REG)
    6. Publish parsed records to sped.parsed.bronze topic
    """

    def __init__(self, spark: SparkSession, config: StreamConfig, bucket_name: str):
        super().__init__(spark, config, stream_name="bronze-stream")
        self.bucket_name = bucket_name
        self.extractor = BucketExtractor(spark, bucket_name)
        self.bucket_connector = BucketConnector(spark, bucket_name)
        self.sped_identifier = SPEDIdentifier()
        self.processed_prefix = "processed/ingested/"

    def get_source_topic(self) -> str:
        return TopicRegistry.RAW_INGEST.name

    def process_micro_batch(self, batch_df: DataFrame, batch_id: int):
        """
        Processa um batch inteiro de eventos de chegada de arquivo de forma
        DISTRIBUIDA: em vez de um loop Python sequencial arquivo-a-arquivo
        (cada um disparando ~4 jobs Spark separados), agrupa os arquivos por
        sped_type e faz UMA leitura + UM parse + UMA escrita por grupo,
        deixando o Spark paralelizar de verdade entre executores.

        Isolamento de falha (espirito do Corte 2) e preservado por DIFF de
        conjuntos: comparamos os arquivos que entraram no grupo com os que
        efetivamente produziram alguma linha de saida. Quem "sumiu" e
        reportado individualmente no DLQ, sem contaminar o restante do grupo.

        FAIL FAST: antes de pagar o custo caro de materializar o grupo
        inteiro (cache + count, que pode levar dezenas de minutos), tira uma
        amostra bem pequena e barata (.limit(N)) e confere se pelo menos
        alguns arquivos batem com o esperado. Se a amostra inteira vier
        vazia/nao-batendo, aborta o grupo em segundos, com diagnostico,
        em vez de descobrir so no final que 100% falhou.
        """
        # 1. Extrai os eventos (path/nome) do Kafka. E so metadado, nao
        # conteudo de arquivo — cabe tranquilo em memoria do driver mesmo
        # com centenas de arquivos num /scan grande.
        events = (
            batch_df
            .select(F.from_json(F.col("kafka_value"), self._event_schema()).alias("event"))
            .select("event.*")
            .collect()
        )

        if not events:
            return

        logger.info(f"[Bronze] Processing batch {batch_id} | files={len(events)}")

        path_to_name = {row["file_path"]: row["file_name"] for row in events}
        name_to_path = {name: path for path, name in path_to_name.items()}
        all_paths = list(path_to_name.keys())

        # 2. Dedup de retificadora em LOTE (Python puro, sem Spark) — usa o
        # metodo que ja existia em SPEDIdentifier mas nunca era chamado aqui.
        paths_after_retif = self.sped_identifier.filter_retificadoras(all_paths)
        for skipped in set(all_paths) - set(paths_after_retif):
            logger.info(f"[Bronze] SPED retificadora/original superado, pulando: {skipped}")

        # 3. Classificacao de tipo por NOME/CAMINHO (Python puro, sem Spark).
        # Quem nao for resolvido aqui vai direto pro DLQ com uma mensagem
        # clara — NAO tentamos mais identificacao por conteudo dentro do
        # caminho de lote (isso ja existe pronto em process_single_file,
        # usado no reprocessamento manual via CLI; duplicar essa logica
        # aqui so pra um caso raro nao paga o custo de complexidade).
        by_sped_type: dict = {}
        for path in paths_after_retif:
            sped_type = self.sped_identifier.identify_type_sped_fast(path)
            if sped_type:
                by_sped_type.setdefault(sped_type, []).append(path)
            else:
                logger.warning(
                    f"[Bronze] Nao identificado por nome/caminho, indo para DLQ "
                    f"(reprocessar manualmente via CLI se necessario): {path}"
                )
                self._report_processing_failure_to_dlq(
                    file_name=path_to_name.get(path, os.path.basename(path)),
                    sped_type="UNKNOWN", file_path=path,
                    error_message="Nao identificado por nome/caminho no fluxo de lote.",
                )

        if not by_sped_type:
            logger.warning(f"[Bronze] Nenhum arquivo identificado no batch {batch_id}")
            return

        # 3b. Dedup de retificadora ESPECIFICO do ECF, por CONTEUDO (nao por
        # nome). Diferente do EFD_CONTRIB/EFD_FISCAL, o nome de arquivo do
        # ECF (SPEDECF-cnpj-dtini-dtfim-timestamp.txt) nao contem a palavra
        # "Original"/"Retificadora" — filter_retificadoras() (item 2 acima)
        # e sempre um no-op pra ECF, deixando passar original+retificadora
        # como se fossem arquivos independentes, duplicando o periodo na
        # Bronze. A chave de agrupamento e (CNPJ, DT_INI, DT_FIN) — NAO so
        # o ano: uma empresa com situacao especial (fusao/cisao/incorporacao)
        # no meio do ano tem MAIS DE UMA ECF legitima no mesmo ano civil,
        # cada uma cobrindo um sub-periodo diferente, e cada uma podendo ter
        # sido retificada independentemente.
        if "ECF" in by_sped_type:
            by_sped_type["ECF"] = self._dedup_ecf_retificadoras(by_sped_type["ECF"])
            if not by_sped_type["ECF"]:
                del by_sped_type["ECF"]
 
        if not by_sped_type:
            logger.warning(f"[Bronze] Nenhum arquivo restante apos dedup de ECF no batch {batch_id}")
            return
 
        # 4. Processa cada grupo de sped_type como UMA operacao distribuida.
        processed_events = []  # publicados no Kafka em lote, no final

        for sped_type, paths in by_sped_type.items():
            registry = SPEDS_REGISTRY_TYPES.get(sped_type)
            if not registry:
                logger.error(f"SPED nao registrado: {sped_type} | arquivos afetados: {len(paths)}")
                for path in paths:
                    self._report_processing_failure_to_dlq(
                        file_name=path_to_name.get(path, os.path.basename(path)),
                        sped_type=sped_type, file_path=path,
                        error_message=f"SPED nao registrado: {sped_type}",
                    )
                continue

            schemas_sped = registry["schemas"]
            fields_map_sped = registry["fields_map"]
            record_types = [r for r in schemas_sped.keys() if r in fields_map_sped]

            logger.info(f"[Bronze] Processando grupo {sped_type} | arquivos={len(paths)}")
            expected_files = {path_to_name.get(p, os.path.basename(p)) for p in paths}

            try:
                # 4a. UMA leitura distribuida para todos os arquivos do grupo
                logger.info(f"[Bronze] Lendo {len(paths)} arquivo(s) do grupo {sped_type}...")
                df_raw_group = self.extractor.read_txt(paths)

                # 4b. Parse em lote (mesmo parser de sempre, agora sobre
                # todos os arquivos do grupo de uma vez)
                df_parsed_group = self.extractor.parse_sped_records(df_raw_group, record_types)

                # 4c. file_id/cnpj/periodo extraidos DIRETO de _source_file
                # via regexp — SEM JOIN. O caminho tem a forma
                # ".../TO_CONVERT/{cnpj}/{tipo}/{periodo}/{nome_arquivo}".
                # Os digitos de cnpj/periodo e o nome do arquivo (hash/texto
                # simples, sem espaco/acento neste projeto) nunca sofrem
                # URL-encoding — so o segmento do meio (ex: "EFD
                # Contribuições") sofre, e esse a gente nem precisa ler.
                df_parsed_group = (
                    df_parsed_group
                    .withColumn("_file_id", F.element_at(F.split(F.col("_source_file"), "/"), -1))
                    .withColumn("_sped_type", F.lit(sped_type))
                    .withColumn("REG",
                        F.regexp_replace(
                            F.trim(F.upper(F.split(F.col("value"), "\\|").getItem(1))),
                            r'[^A-Z0-9]', ''
                        )
                    )
                    .withColumn("_cnpj_path", F.regexp_extract(F.col("_source_file"), r'TO_CONVERT/(\d+)/', 1))
                    .withColumn("_periodo_path", F.regexp_extract(F.col("_source_file"), r'TO_CONVERT/\d+/[^/]+/(\d{4,6})/', 1))
                )

                # 4d. FAIL FAST: amostra barata (.limit(50)) ANTES do cache
                # caro. Se nenhum _file_id da amostra bater com o esperado,
                # e quase certo que algo sistemico quebrou (ex: mudanca no
                # formato de _source_file) — aborta o grupo inteiro em
                # segundos, com a amostra real no log pra debugar na hora,
                # em vez de esperar dezenas de minutos pra descobrir que deu
                # 100% de falha.
                with self.spark_job_label(f"[Bronze] checagem rapida (amostra) — grupo={sped_type}"):
                    sample_rows = df_parsed_group.select("_file_id", "_source_file").limit(50).collect()
                sample_ids = {r["_file_id"] for r in sample_rows}
                if sample_rows and not (sample_ids & expected_files):
                    sample_preview = [(r["_file_id"], r["_source_file"]) for r in sample_rows[:5]]
                    raise RuntimeError(
                        f"Amostra de _file_id nao bate com NENHUM arquivo esperado do grupo "
                        f"{sped_type} — provavel erro de extracao/encoding em _source_file. "
                        f"Amostra (ate 5): {sample_preview}"
                    )
                logger.info(f"[Bronze] Amostra do grupo {sped_type} OK ({len(sample_ids & expected_files)} de {len(sample_ids)} na amostra batem)")

                # CACHE: a partir daqui df_parsed_group e usado em VARIAS
                # acoes separadas (combos, escritas por combo, diff de
                # arquivos produzidos). Sem cache, cada .collect()/.save()
                # reprocessa a leitura+parse INTEIRA do zero — ja observado
                # na pratica como um job de ~18min se repetindo identico
                # logo em seguida do outro. Materializa uma vez, reaproveita
                # o resto do grupo, libera no final.
                df_parsed_group = df_parsed_group.cache()
                logger.info(f"[Bronze] Materializando grupo {sped_type} ({len(paths)} arquivos) — pode levar minutos...")
                with self.spark_job_label(
                    f"[Bronze] materializar cache — grupo={sped_type} ({len(paths)} arquivos)"
                ):
                    df_parsed_group.count()

                # 4e. Escreve na Bronze — ainda 1 escrita por combinacao
                # (cnpj, periodo) presente no grupo, porque o path fisico
                # nao e particao Spark (limitacao ja existente, nao
                # resolvida nesta rodada) — mas de qualquer forma sao MUITO
                # menos escritas do que 1 por arquivo.
                with self.spark_job_label(
                    f"[Bronze] descobrir cnpj/periodo — grupo={sped_type} ({len(paths)} arquivos)"
                ):
                    combos = [
                        (r["_cnpj_path"], r["_periodo_path"])
                        for r in df_parsed_group.select("_cnpj_path", "_periodo_path").distinct().collect()
                    ]

                for cnpj_p, periodo_p in combos:
                    bronze_path = f"{self.config.bronze_path}/{cnpj_p}/{sped_type}/{periodo_p}"
                    df_subset = (
                        df_parsed_group
                        .filter((F.col("_cnpj_path") == cnpj_p) & (F.col("_periodo_path") == periodo_p))
                        .drop("_cnpj_path", "_periodo_path")
                    )

                    # IDEMPOTENCIA: foreachBatch do Structured Streaming so
                    # comita offset quando o batch INTEIRO termina sem erro.
                    # Se o processo cair no meio (ex: OOM em outro combo do
                    # mesmo grupo) e reiniciar, o Spark reprocessa o batch
                    # INTEIRO do zero — inclusive combos que ja tinham
                    # gravado com sucesso antes da queda. Escrever com
                    # replace_where restrito aos _file_id deste combo (igual
                    # a Silver ja faz por arquivo) torna isso seguro: reciclar
                    # o mesmo combo SOBRESCREVE em vez de duplicar.
                    file_ids_in_combo = [
                        r["_file_id"] for r in
                        df_subset.select("_file_id").distinct().collect()
                    ]
                    file_ids_sql_list = ", ".join(
                        "'" + fid.replace("'", "''") + "'" for fid in file_ids_in_combo
                    )
                    replace_where = f"_file_id IN ({file_ids_sql_list})"

                    logger.info(
                        f"[Bronze] Escrevendo para o caminho: {bronze_path} "
                        f"(idempotente, {len(file_ids_in_combo)} arquivo(s))"
                    )
                    with self.spark_job_label(
                        f"[Bronze] escrever bronze — {sped_type}/{cnpj_p}/{periodo_p}"
                    ):
                        self.write_to_delta(
                            df_subset, bronze_path, partition_by=["REG"],
                            replace_where=replace_where,
                        )

                # 4f. DIFF: quais arquivos do grupo nao produziram nenhuma
                # linha? (leitura falhou, arquivo vazio, ou — risco residual
                # raro — o proprio NOME do arquivo, nao o diretorio, tivesse
                # espaco/acento e saisse URL-encoded em _source_file,
                # divergindo do nome esperado). Isola so esses arquivos, sem
                # derrubar o grupo inteiro.
                with self.spark_job_label(
                    f"[Bronze] diff arquivos produzidos — grupo={sped_type}"
                ):
                    produced_files = {
                        row["_file_id"] for row in
                        df_parsed_group.select("_file_id").distinct().collect()
                    }
                missing_files = expected_files - produced_files

                for missing_name in missing_files:
                    missing_path = name_to_path.get(missing_name, missing_name)
                    logger.error(f"[Bronze] Arquivo do grupo {sped_type} nao produziu nenhuma linha (possivel falha de leitura/parse): {missing_name}")
                    self._report_processing_failure_to_dlq(
                        file_name=missing_name, sped_type=sped_type, file_path=missing_path,
                        error_message="Arquivo nao produziu nenhuma linha apos leitura/parse em lote.",
                    )

                # 4g. Enfileira eventos de sucesso pra publicacao em lote
                for path in paths:
                    file_name = path_to_name.get(path, os.path.basename(path))
                    if file_name in missing_files:
                        continue
                    processed_events.append({
                        "_file_id": file_name,
                        "_sped_type": sped_type,
                        "file_path": path,
                        "status": "PROCESSED_BRONZE",
                    })

                logger.info(f"[Bronze] Grupo {sped_type} concluido | sucesso={len(paths) - len(missing_files)} | falhas={len(missing_files)}")

                # Libera o cache — evita acumular memoria entre grupos
                # quando o batch tem mais de um sped_type.
                df_parsed_group.unpersist()

            except Exception as e:
                # Falha no grupo inteiro (ex: erro de leitura do bucket pra
                # todo o grupo, ou a checagem rapida do item 4d abortou) —
                # isola so este sped_type; outros grupos do mesmo batch
                # seguem processando normalmente.
                logger.error(f"[Bronze] Erro processando grupo {sped_type} ({len(paths)} arquivos): {e}", exc_info=True)
                try:
                    df_parsed_group.unpersist()
                except Exception:
                    pass  # df_parsed_group pode nem ter sido criado ainda
                for path in paths:
                    self._report_processing_failure_to_dlq(
                        file_name=path_to_name.get(path, os.path.basename(path)),
                        sped_type=sped_type, file_path=path,
                        error_message=str(e)[:1024],
                    )
                continue

        # 5. Publica TODOS os eventos de sucesso do batch numa unica escrita
        # Kafka, em vez de 1 write_to_kafka por arquivo.
        if processed_events:
            event_df = self.spark.createDataFrame(processed_events)
            bronze_kafka_df = (
                event_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            with self.spark_job_label(f"[Bronze] publicar Kafka batch {batch_id} ({len(processed_events)} eventos)"):
                self.write_to_kafka(bronze_kafka_df, TopicRegistry.PARSED_BRONZE.name)
            logger.info(f"[Bronze] {len(processed_events)} arquivo(s) processado(s) e evento(s) publicado(s) no Kafka")

        # 6. Move os arquivos processados com sucesso. Fica fora do bloco de
        # processamento pesado — e so metadado de bucket (rename/copy leve),
        # falha aqui nao invalida o dado ja gravado, so precisa ficar visivel.
        # for ev in processed_events:
        #     file_path = ev["file_path"]
        #     try:
        #         destination_key = f"{self.processed_prefix}{os.path.basename(file_path)}"
        #         self.bucket_connector.move_file(source_key=file_path, dest_key=destination_key)
        #         logger.info(f"[Bronze] File moved to processed: {file_path} -> {destination_key}")
        #     except Exception as move_error:
        #         logger.error(f"[Bronze] Failed to move processed file: {file_path} | error={move_error}", exc_info=True)
        #         self._report_move_failure_to_dlq(
        #             file_name=ev["_file_id"], sped_type=ev["_sped_type"],
        #             file_path=file_path, move_error=move_error,
        #         )

    def _report_processing_failure_to_dlq(self, file_name: str, sped_type: str, file_path: str, error_message: str):
        """Publica em sped.errors.dlq a falha isolada de UM arquivo dentro de
        um grupo processado em lote — preserva o isolamento por arquivo do
        Corte 2 mesmo com o processamento agora vetorizado por grupo.

        Inclui _failed_stream/_failed_at para compatibilidade com o filtro
        do republish_dlq.py (--stream/--failed-at), e ALEM disso
        original_topic + kafka_value_truncated com o evento de chegada
        original reconstruido (mesmo schema de _event_schema()) — sem isso,
        o republish_dlq.py nao tem como saber pra onde/o-que republicar
        (bug descoberto ao tentar reprocessar um incidente real: 1044
        mensagens no DLQ, 0 reconstruiveis, porque esse envelope nao
        carregava o payload original).
        """
        try:
            original_event = {
                "file_path": file_path,
                "file_name": file_name,
                "sped_type": sped_type,
            }
            error_df = self.spark.createDataFrame([{
                "_file_id": file_name,
                "_sped_type": sped_type,
                "file_path": file_path,
                "status": "PROCESSING_FAILED",
                "_error_message": error_message[:1024],
                "_failed_stream": self.stream_name,
                "_failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "original_topic": TopicRegistry.RAW_INGEST.name,
                "kafka_value_truncated": json.dumps(original_event)[:4096],
            }])
            error_kafka_df = (
                error_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            self.write_to_kafka(error_kafka_df, TopicRegistry.ERRORS_DLQ.name)
        except Exception as dlq_error:
            logger.critical(f"[Bronze] Failed to report processing failure to DLQ: {dlq_error}")

    def _report_move_failure_to_dlq(self, file_name: str, sped_type: str, file_path: str, move_error):
        """NOTA: aqui o processamento (bronze+kafka) ja teve sucesso — essa
        falha e so no 'arquivar' o arquivo de origem. Sem isso visivel em
        algum lugar, o arquivo fica esquecido em TO_CONVERT/ e um /scan
        futuro no mesmo prefixo o reprocessa do zero, sem ninguem saber."""
        try:
            move_error_df = self.spark.createDataFrame([{
                "_file_id": file_name,
                "_sped_type": sped_type,
                "file_path": file_path,
                "status": "MOVE_FAILED_AFTER_PROCESSING",
                "_error_message": str(move_error)[:1024],
                "_failed_stream": self.stream_name,
                "_failed_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            }])
            move_error_kafka_df = (
                move_error_df
                .withColumn("kafka_key", F.col("_file_id"))
                .withColumn("kafka_value", F.to_json(F.struct("*")))
            )
            self.write_to_kafka(move_error_kafka_df, TopicRegistry.ERRORS_DLQ.name)
        except Exception as dlq_error:
            logger.critical(f"[Bronze] Failed to report move failure to DLQ too: {dlq_error}")

    def _dedup_ecf_retificadoras(self, ecf_paths: list) -> list:
        """
        Le so a linha do REG 0000 (identificada pelo conteudo, "|0000|" no
        inicio — nao precisa de ordenacao/row_number, e sempre a 1a linha
        do SPED mas filtrar por conteudo e mais simples e robusto) de cada
        arquivo ECF candidato, extrai CNPJ (campo4), DT_INI (campo10),
        DT_FIN (campo11) e RETIFICADORA (campo12), agrupa por
        (cnpj, dt_ini, dt_fin) — a chave real do periodo — e mantem:
          - se existir alguma retificadora (campo12=='S') no grupo: so ela
            (a mais recente, pelo timestamp no nome do arquivo, se houver
            mais de uma retificadora pro MESMO periodo exato);
          - senao: o(s) arquivo(s) original(is) do grupo.
        """
        if not ecf_paths:
            return []
 
        with self.spark_job_label(f"[Bronze] dedup retificadora ECF ({len(ecf_paths)} candidatos)"):
            df_reg0000 = (
                self.extractor.read_txt(ecf_paths)
                .filter(F.col("value").startswith("|0000|"))
                .withColumn("_cols", F.split(F.col("value"), "\\|"))
                .select(
                    F.col("_source_file"),
                    F.element_at(F.col("_cols"), 5).alias("cnpj"),
                    F.element_at(F.col("_cols"), 11).alias("dt_ini"),
                    F.element_at(F.col("_cols"), 12).alias("dt_fin"),
                    F.element_at(F.col("_cols"), 13).alias("retificadora"),
                )
            )
            header_rows = df_reg0000.collect()
 
        # Mapeia _source_file (URI) de volta pro path original da lista de
        # entrada, pelo nome do arquivo (basename) — mesma tecnica ja usada
        # em outros pontos do Bronze pra evitar depender do formato exato
        # da URI (pode vir URL-encoded).
        name_to_path = {os.path.basename(p): p for p in ecf_paths}
 
        groups: dict = {}
        for row in header_rows:
            source_file = row["_source_file"] or ""
            basename = source_file.rsplit("/", 1)[-1]
            original_path = name_to_path.get(basename)
            if not original_path:
                continue
            key = (row["cnpj"], row["dt_ini"], row["dt_fin"])
            groups.setdefault(key, []).append({
                "path": original_path,
                "retificadora": (row["retificadora"] or "").strip().upper() == "S",
            })
 
        # Arquivos cujo REG 0000 nao foi lido/casado (ex: arquivo vazio,
        # corrompido) — deixa passar pro fluxo normal, que vai isola-los
        # individualmente no diff de arquivos-nao-produzidos mais adiante.
        matched_paths = {os.path.basename(p) for group in groups.values() for p in [item["path"] for item in group]}
        unmatched = [p for p in ecf_paths if os.path.basename(p) not in matched_paths]
 
        kept_paths = list(unmatched)
        for (cnpj, dt_ini, dt_fin), entries in groups.items():
            retificadoras = [e for e in entries if e["retificadora"]]
            candidatos = retificadoras if retificadoras else entries
 
            if len(candidatos) == 1:
                kept_paths.append(candidatos[0]["path"])
            else:
                # Mais de uma retificadora pro MESMO periodo exato — fica
                # com a mais recente, pelo timestamp no final do nome do
                # arquivo (padrao SPEDECF-cnpj-dtini-dtfim-timestamp.txt).
                def _timestamp_do_nome(path: str) -> str:
                    stem = os.path.basename(path).rsplit(".", 1)[0]
                    return stem.rsplit("-", 1)[-1]
 
                mais_recente = max(candidatos, key=lambda e: _timestamp_do_nome(e["path"]))
                kept_paths.append(mais_recente["path"])
                for e in candidatos:
                    if e["path"] != mais_recente["path"]:
                        logger.info(
                            f"[Bronze] ECF retificadora superada por outra mais recente "
                            f"do MESMO periodo ({cnpj}, {dt_ini}-{dt_fin}), pulando: {e['path']}"
                        )
 
            descartados = [e["path"] for e in entries if e["path"] not in kept_paths]
            for path in descartados:
                logger.info(
                    f"[Bronze] ECF original superado por retificadora do mesmo periodo "
                    f"({cnpj}, {dt_ini}-{dt_fin}), pulando: {path}"
                )
 
        logger.info(
            f"[Bronze] Dedup ECF: {len(ecf_paths)} candidato(s) -> {len(kept_paths)} mantido(s) "
            f"({len(ecf_paths) - len(kept_paths)} descartado(s) por retificadora/duplicidade)"
        )
        return kept_paths

    def process_single_file(self, file_path: str, sped_type: str = None):
        """
        Processa UM arquivo SPED especifico, fora do fluxo de streaming/Kafka.
 
        Usado pelo dispatcher on-demand (main.py --action REPROCESS_*), onde
        o file_path e o sped_type ja vem definidos via CLI (nao ha evento
        Kafka nem batch_df). Reaproveita a mesma logica de parsing/escrita
        de process_micro_batch, mas para um unico arquivo, sincrono.
 
        Args:
            file_path: caminho do arquivo SPED no bucket
                       (ex: "TO_CONVERT/75400218/EFD Contribuições/2024/SPED.txt")
            sped_type: tipo do SPED (ex: "EFD_CONTRIB"). Se None, tenta
                       identificar automaticamente a partir do conteudo,
                       igual ao fluxo de streaming.
 
        Raises:
            ValueError: se o SPED nao puder ser identificado/registrado.
            Exception: qualquer erro de leitura/parsing/escrita e propagado
                       (mesmo comportamento de process_micro_batch), para que
                       main.py capture e finalize com sys.exit(1).
        """
        file_name = os.path.basename(file_path)
        logger.info(f"[Bronze] Processando arquivo unico: {file_path}")
 
        # 1. Le o TXT bruto do bucket (mesmo extractor do fluxo de streaming)
        df_raw = self.extractor.read_txt(file_path)
 
        # 2. Identifica o tipo de SPED, se nao veio explicito por --action
        if not sped_type:
            sped_type = self.sped_identifier.identify_type_sped(df_raw, file_path)
            if not sped_type:
                raise ValueError(f"Nao foi possivel identificar o SPED do arquivo: {file_path}")
 
        logger.info(f"[Bronze] SPED: {sped_type} | file: {file_name}")
 
        if self.sped_identifier.is_retificadora(file_path):
            logger.info(f"[Bronze] SPED retificadora, pulando: {file_name}")
            return
 
        # 3. Busca schemas/campos no registry (mesmo registry do streaming)
        registry = SPEDS_REGISTRY_TYPES.get(sped_type)
        if not registry:
            raise ValueError(f"SPED nao registrado: {sped_type}")
 
        schemas_sped = registry["schemas"]
        fields_map_sped = registry["fields_map"]
        record_types = [r for r in schemas_sped.keys() if r in fields_map_sped]
 
        # 4. Faz o parsing dos registros SPED (mesmo extractor do streaming)
        df_parsed = self.extractor.parse_sped_records(df_raw, record_types)
        df_parsed = (
            df_parsed
            .withColumn("_file_id", F.lit(file_name))
            .withColumn("_sped_type", F.lit(sped_type))
            .withColumn("REG",
                F.regexp_replace(
                    F.trim(F.upper(F.split(F.col("value"), "\\|").getItem(1))),
                    r'[^A-Z0-9]', ''
                )
            )
        )
 
        # 5. Escreve na Bronze (Delta), particionado por REG
        import re as _re
        _meta = _re.search(r'TO_CONVERT/(\d+)/[^/]+/(\d{4,6})/', file_path)
        _cnpj_p = _meta.group(1) if _meta else "unknown"
        _per_p = _meta.group(2) if _meta else "unknown"
        bronze_path = f"{self.config.bronze_path}/{_cnpj_p}/{sped_type}/{_per_p}"
        logger.info(f"[Bronze] Escrevendo para o caminho: {bronze_path}")
 
        self.write_to_delta(df_parsed, bronze_path, partition_by=["REG"], replace_where=f"_file_id")
 
        # 6. Publica evento de nivel-arquivo no Kafka, para o Silver consumir
        event_data = {
            "_file_id":   file_name,
            "_sped_type": sped_type,
            "file_path":  file_path,
            "status":     "PROCESSED_BRONZE"
        }
        event_df = self.spark.createDataFrame([event_data])
        bronze_kafka_df = (
            event_df
            .withColumn("kafka_key", F.col("_file_id"))
            .withColumn("kafka_value", F.to_json(F.struct("*")))
        )
        self.write_to_kafka(bronze_kafka_df, TopicRegistry.PARSED_BRONZE.name)
 
        logger.info(f"[Bronze] Arquivo processado na bronze e evento Kafka publicado: {file_name}")
 
        return {"sped_type": sped_type, "bronze_path": bronze_path, "file_name": file_name}


    @staticmethod
    def _event_schema():
        """Schema for file arrival events from Kafka."""
        
        return StructType([
            StructField("file_path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_size_bytes", LongType(), True),
            StructField("sped_type", StringType(), True),
            StructField("cnpj", StringType(), True),
            StructField("periodo", StringType(), True),
            StructField("bucket_name", StringType(), True),
            StructField("event_timestamp_ms", LongType(), True),
            StructField("source", StringType(), True),
        ])