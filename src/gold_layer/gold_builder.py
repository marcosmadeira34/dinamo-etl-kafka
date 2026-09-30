from schemas import default_schemas
import os
import yaml
import re
import logging

from .gold_base import BaseGoldBuilder
# from schemas.default_schemas import SpedContribSchemas, SpedFiscalSchemas, SpedEcdSchemas, SpedEcfSchemas

from typing import Iterable, Dict, List, Optional, Tuple
from time import sleep


from pyspark.sql.functions import broadcast, coalesce, lit
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StructType, NullType, StructField
from pyspark.sql.types import StringType
# from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from functools import reduce


logger = logging.getLogger("GoldDefaultBuilder")
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',)



class GoldStreamingBuilder(BaseGoldBuilder):
    def __init__(self, spark, base_path, period):
        super().__init__(spark, base_path, period)
        self.logger = logger

    def load_silver_by_reg(self, sped_type: str, ano: int, client_cnpj: str, registers: Iterable[str]):
        """
        Carrega os registros SPED da camada Silver.

        CORRECAO (confirmada contra dado real): o comentario original dizia
        "cada REG com seu proprio delta_log", mas em pelo menos um caso real
        (CNPJ/periodo verificado em producao) o _delta_log fica UM NIVEL
        ACIMA de REG=<reg> — ou seja, REG=<reg> e uma PARTICAO Hive de uma
        tabela Delta maior, nao uma tabela propria. Carregar a pasta da
        particao diretamente (comportamento anterior) faz o Spark ignorar o
        log de transacoes do Delta e ler TODO arquivo fisico ainda presente
        na pasta — incluindo versoes antigas ja substituidas por commits
        posteriores que o VACUUM ainda nao removeu fisicamente. Isso pode
        ler dado duplicado/obsoleto muito maior que o esperado (suspeito de
        ter causado OOM em executor no modulo de Subvencao).

        Estrategia: tenta primeiro o jeito CORRETO (carregar a tabela PAI,
        que tem o _delta_log de verdade, e filtrar por REG). Se isso falhar
        (nao existe _delta_log no nivel pai), cai no comportamento antigo
        (particao direta) — mantendo compatibilidade caso existam REGs
        gravados como tabela propria em algum outro fluxo.
        """
        self.dfs = {}
        parent_path = f'{self.base_path}/{client_cnpj}/{sped_type}/{ano}'

        for reg in registers:
            partition_path = f'{parent_path}/REG={reg}'
            try:
                df_all = (
                    self.spark.read
                    .format("delta")
                    .load(parent_path)
                    .filter(F.col("REG") == reg)
                )
                self.dfs[reg] = df_all
                self.logger.info(f"REG {reg} carregado com sucesso (tabela pai + filtro REG)")
            except Exception as e_pai:
                try:
                    df_all = self.spark.read.format("delta").load(partition_path)
                    self.dfs[reg] = df_all
                    self.logger.info(f"REG {reg} carregado com sucesso (tabela propria, fallback)")
                except Exception as e_particao:
                    self.logger.warning(
                        f"REG {reg} nao carregado (tabela pai: {e_pai} | particao direta: {e_particao})"
                    )
                    self.dfs[reg] = None

        return self

    def load_silver_canonical(self, sped_type: str, ano: int, client_cnpj: str,registers: Iterable[str]):
        """
        Carrega registros SPED da camada Silver.
        Se o caminho nao existir, ignora e continua o processamento.
        """

        self.dfs = {}

        base_path = f"{self.base_path}/{client_cnpj}/{sped_type}/{ano}" 
           

        self.logger.info(f"Base path: {base_path}")

        try:
            df_all = (
                self.spark.read
                .format("delta")
                .load(base_path)
            )

        except Exception as e:
            self.logger.error(f"Erro ao carregar Silver: {base_path} - {e}")

            for reg in registers:
                self.dfs[reg] = None

            return self

        for reg in registers:

            try:
                self.dfs[reg] = df_all.filter(F.col("REG") == reg)

                self.logger.info(f"REG {reg} carregado com sucesso")

            except Exception as e:

                self.logger.warning(f"Erro ao filtrar REG {reg}: {e}")

                self.dfs[reg] = None

        return self

    def write_gold(self, output_path: str):
        if self.df_gold is None:
            self.logger.warning("df_gold nao gerado – GOLD ignorada")
            return None
            
        for field in self.df_gold.schema.fields:
            if isinstance(field.dataType, NullType):
                self.df_gold = self.df_gold.withColumn(
                    field.name,
                    F.lit(None).cast("string")
                )

        self._validate_no_duplicate_columns(self.df_gold)

        (
            self.df_gold
            # .transform(self._normalize_column_names)
            .coalesce(1)
            .write
            .mode("overwrite")
            .format("delta")
            .option("mergeSchema", "true")
            .save(output_path)
        )

        self.logger.info(f"GOLD escrita com sucesso em {output_path}")
        return self


    def _vacuum_gold(self, output_path: str, retencao_horas: int = 0) -> None:
        """Remove arquivos parquet orfaos (de overwrites anteriores) que o
        Delta mantem por padrao (retencao default de 7 dias, pra permitir
        time travel/rollback). retencao_horas=0 remove tudo que nao e a
        versao atual -- seguro aqui porque cada execucao de Subvencao e
        auto-contida (recalcula do zero a partir da Silver), entao nao
        precisamos de historico de versoes antigas da GOLD pra rollback.

        Se algum dia isso mudar (ex.: quiser auditar uma execucao anterior
        via time travel), so remover essa chamada ou aumentar retencao_horas."""
        try:
            self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            self.spark.sql(f"VACUUM delta.`{output_path}` RETAIN {retencao_horas} HOURS")
            self.logger.info(f"VACUUM executado em {output_path} (retencao={retencao_horas}h)")
        except Exception as e:
            self.logger.warning(f"VACUUM falhou em {output_path} ({e}) — arquivos orfaos podem se acumular.")

    
    def _extract_registers_from_general_config(self, config_report: dict) -> List[str]:
        """Helper para extrair os registros necessários do novo formato YAML"""
        registers = set()
        
        for doc_cfg in config_report.get("document_sources", {}).values():
            # Pega a base
            if "base" in doc_cfg:
                registers.add(str(doc_cfg.get("base")))
                # Pega os filhos
                for child in doc_cfg.get("children", []):
                    registers.add(str(child.get("source")))
            else:
                # Pega sources simples
                registers.add(str(doc_cfg.get("source")))
            
        # Pega dos enrichments
        for enrich_cfg in config_report.get("enrichments", {}).items():
            registers.add(str(enrich_cfg[1].get("source")))
            
        return list(registers)
    
    def build_general_report(self, report_key: str) -> 'ReportBuilder':
        """
        Constrói relatório geral baseado em UNION + ENRICHMENTS
        """
        config = self.config.get(report_key)
        
        if not config:
            raise ValueError(f"Configuraca '{report_key}' nao encontrada")

        self.logger.info(f"Construindo relatorio: {report_key}")

        # =====================================================
        # 1. VALIDAÇÃO DA CONFIGURAÇÃO
        # =====================================================
        self._validate_config(config)

        # =====================================================
        # 2. BUILD DOS DOCUMENTOS (UNION)
        # =====================================================
        df_union = self._build_document_union(config)
        
        self.logger.info(f"UNION concluido: {df_union.count()} linhas totais")

        # =====================================================
        # 3. ENRIQUECIMENTOS (JOINS)
        # =====================================================
        df_enriched = self._apply_enrichments(df_union, config)

        # =====================================================
        # 4. REORDENAR COLUNAS
        # =====================================================
        df_final = self._apply_output_columns(df_enriched, config)

        # =====================================================
        # 5. VALIDAÇÃO FINAL
        # =====================================================
        self._validate_final_report(df_final, report_key)

        self.df_gold = df_final
        self.logger.info(f"Relatorio '{report_key}' construido com sucesso")

        return self

    def _extract_registers_from_config(self, config: dict) -> List[str]:
        entities = config.get("entities", {})
        return list({cfg.get("source") for cfg in entities.values() if cfg.get("source")})

    def _build_output_path(self, client_cnpj: str, month_year: str, report_key: str, sped_type: str):
        
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        config_path = os.path.join(base_dir, "src/config", "main_config.yaml")

        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        return "/".join([f"s3a://{config['s3']['bucket_name']}", "data-lake", "gold",
            client_cnpj,
            sped_type,
            month_year,
            report_key
        ])      

    # Checa se a tabela GOLD está vazia
    def sanity_checks(self):
        if self.df_gold is None or self.df_gold.limit(1).count() == 0:
            raise RuntimeError("GOLD Contrib vazia")
        return self

    def _extract_registers_from_config(self, config: dict) -> List[str]:
        entities = config.get("entities", {})
        return list({cfg.get("source") for cfg in entities.values() if cfg.get("source")})

    def build_general_report(self, report_key: str):

        config = self.config.get(report_key)

        if not config:
            raise ValueError(f"Configuração {report_key} nao encontrada")

        self.logger.info(f'Construindo relatório {report_key}')

        # =====================================================
        # 🔥 NORMALIZAÇÃO E VALIDAÇÃO DO CONFIG
        # =====================================================
        entities_cfg = self._normalize_and_validate_entities(config)

        # =====================================================
        # 🔥 VALIDAÇÃO DE CONSISTÊNCIA PARENT-CHILD
        # =====================================================
        self._validate_parent_child_consistency(entities_cfg)

        # =====================================================
        # 🔥 VALIDAÇÃO DE COLUNAS REAIS (ANTES DO JOIN)
        # =====================================================
        self._validate_columns_exist(entities_cfg, self.dfs)

        base_entity_name = config.get("base_entity")

        # =====================================================
        # 🔹 CARREGAMENTO DOS DATAFRAMES
        # =====================================================
        dfs = {
            name: self.dfs.get(cfg['source'])
            for name, cfg in entities_cfg.items()
        }

        if dfs.get(base_entity_name) is None:
            self.logger.warning(f"Base entity {base_entity_name} nao encontrada")
            self.df_gold = None
            return self

        # =====================================================
        # 🔹 BUILD DOS SELECTS
        # =====================================================
        built_dfs = {}

        for name, cfg in entities_cfg.items():
            df = dfs.get(name)

            if df is None:
                built_dfs[name] = None
                continue

            built_dfs[name] = self._build_entity_select(df, name, cfg)

        # =====================================================
        # 🔹 BASE INICIAL
        # =====================================================
        df_final = built_dfs[base_entity_name]

        self.logger.info(f"Base inicial: {df_final.columns}")

        # =====================================================
        # 🔥 FUNÇÃO PADRÃO DE JOIN
        # =====================================================
        def apply_join(df_left, df_right, join_cfg, name):

            join_type = join_cfg.get("type", "left")
            strategy = join_cfg.get("strategy", "direct")
            keys = join_cfg.get("keys", [])

            if not keys:
                raise ValueError(f"{name} sem keys definidas no join")

            self.logger.info(f"JOIN {name} | type={join_type} | strategy={strategy}")

            for key_map in keys:

                left = key_map.get("left")
                right = key_map.get("right")

                if not left or not right:
                    raise ValueError(f"{name} possui key inválida: {key_map}")

                if left not in df_left.columns:
                    raise ValueError(
                        f"Coluna '{left}' nao existe em df_final ({name}). "
                        f"Disponíveis: {df_left.columns}"
                    )

                if right not in df_right.columns:
                    raise ValueError(
                        f"Coluna '{right}' nao existe em df_join ({name}). "
                        f"Disponíveis: {df_right.columns}"
                    )

                # 🔹 garante tipo
                df_left = df_left.withColumn(left, F.col(left).cast("string"))
                df_right = df_right.withColumn(right, F.col(right).cast("string"))

                # 🔹 evita duplicação de colunas
                overlapping = [
                    c for c in df_right.columns
                    if c in df_left.columns and c != right
                ]

                if overlapping:
                    self.logger.info(f"{name} dropando colunas duplicadas: {overlapping}")
                    df_right = df_right.drop(*overlapping)

                # 🔥 JOIN
                df_left = df_left.join(
                    df_right,
                    df_left[left] == df_right[right],
                    how=join_type
                ).drop(df_right[right])

                self.logger.info(f"JOIN aplicado {name} ON {left}={right}")

            return df_left

        # =====================================================
        # 🔥 APPLY JOINS
        # =====================================================
        for name, cfg in entities_cfg.items():

            if name == base_entity_name:
                continue

            df_join = built_dfs.get(name)

            if df_join is None:
                self.logger.warning(f"{name} nao encontrado — join ignorado")
                continue

            join_cfg = cfg.get("join")

            if not join_cfg:
                self.logger.warning(f"{name} sem configuracao de join — ignorado")
                continue

            # 0000 tratado depois
            if name == "0000":
                continue

            df_final = apply_join(df_final, df_join, join_cfg, name)

            self.logger.info(f"Colunas após join {name}: {df_final.columns}")

        # =====================================================
        # 🔥 JOIN COM 0000 (ROOT)
        # =====================================================
        if "0000" in entities_cfg:

            cfg_0000 = entities_cfg["0000"]
            df_root = built_dfs.get("0000")

            if df_root is None:
                self.logger.warning("0000 nao encontrado — join ignorado")
            else:
                join_cfg = cfg_0000.get("join")

                if not join_cfg:
                    raise ValueError("0000 sem configuracao de join")

                df_final = apply_join(df_final, df_root, join_cfg, "0000")

                self.logger.info(f"Depois join 0000: {df_final.columns}")

        # =====================================================
        # 🔹 REORDER
        # =====================================================
        reorder_cfg = config.get(f"reorder_columns_{report_key}")

        if reorder_cfg:
            fields = reorder_cfg.get("fields", [])

            final_fields = [c for c in fields if c in df_final.columns]

            missing = [c for c in fields if c not in df_final.columns]
            if missing:
                self.logger.warning(f"Campos nao encontrados no reorder: {missing}")

            df_final = df_final.select(*final_fields)

        # =====================================================
        # 🔹 VALIDAÇÃO FINAL
        # =====================================================
        required_metadata = ["FILE_ID"]

        missing = [c for c in required_metadata if c not in df_final.columns]

        if missing:
            raise ValueError(f"Metadados ausentes no relatório {report_key}: {missing}")

        self.logger.info(f"Colunas finais: {df_final.columns}")

        self.df_gold = df_final

        self.logger.info(f"Relatório {report_key} construído com sucesso")

        return self

    # Verifica se o arquivo GOLD já foi processado anteriormente
    def gold_contrib_already_processed(self, bucket_connector, path: str) -> bool:
        try:
            paths = bucket_connector.list_paths(path)
            return len(paths) > 0
        except Exception:
            return False

    # Reordena as colunas do DataFrame conforme configuracao
    def reorder_columns(self, df, ordered_columns: list = None, keep_others_last: bool = True, validate_missing: bool = False ):
        if not ordered_columns:
            return df

        existing_cols = df.columns

        missing = [c for c in ordered_columns if c not in existing_cols]

        if validate_missing and missing:
            raise ValueError(
                f"Colunas configuradas mas nao encontradas no DataFrame: {missing}"
            )

        ordered = [c for c in ordered_columns if c in existing_cols]

        if keep_others_last:
            remaining = [c for c in existing_cols if c not in ordered]
            final_cols = ordered + remaining
        else:
            final_cols = ordered

        return df.select(*final_cols)

    def _sanitize_ambiguous_columns(self, df: DataFrame) -> DataFrame:
        """
        Remove colunas duplicadas mantendo a primeira ocorrência.
        Utiliza renomeação posicional (.toDF) para eliminar a ambiguidade antes de selecionar.
        """
        # 1. Lista original de colunas (pode conter duplicatas, ex: ['FILE_ID', 'COD', 'FILE_ID'])
        original_columns = df.columns
        
        # Contagem para o log
        counts = {}
        for c in original_columns:
            counts[c] = counts.get(c, 0) + 1
        
        duplicates = {col for col, count in counts.items() if count > 1}
        
        if not duplicates:
            return df

        self.logger.warning(f"🔍 DEBUG AMBIGUIDADE: As seguintes colunas estão duplicadas no DataFrame e serão removidas (mantendo a primeira ocorrência): {duplicates}")

        # 2. Cria uma lista de nomes temporários ÚNICOS para todas as colunas baseados no índice
        # Ex: ['_sanit_0', '_sanit_1', '_sanit_2']
        # O toDF funciona por posição, resolvendo o problema de ambiguidade imediatamente.
        temp_names = [f"_sanit_{i}" for i in range(len(original_columns))]
        
        # Renomeia o DataFrame inteiro para nomes únicos
        df_unique = df.toDF(*temp_names)

        # 3. Reconstrói o DataFrame final
        select_exprs = []
        seen_cols = set()
        
        # Itera sobre as colunas originais e os nomes temporários juntos
        for i, original_name in enumerate(original_columns):
            temp_name = temp_names[i]
            
            # Se ainda nao vimos esse nome original, adicionamos à seleção
            if original_name not in seen_cols:
                seen_cols.add(original_name)
                # Seleciona pelo nome temporário (que e único e nao ambíguo) e usa alias para o nome original
                select_exprs.append(F.col(temp_name).alias(original_name))
            else:
                # Se já existe, ignoramos (remove a duplicata)
                pass

        # Retorna o DataFrame limpo
        return df_unique.select(*select_exprs)

    def reorder_auxiliary_columns(self, df, config):
        fields = config.get("fields", [])

        existing_cols = df.columns

        # 🔹 colunas que existem
        valid_cols = [c for c in fields if c in existing_cols]

        # 🔹 colunas que NÃO existem
        missing_cols = [c for c in fields if c not in existing_cols]

        if missing_cols:
            self.logger.warning(
                f"Colunas nao encontradas (ignoradas): {missing_cols}"
            )

        # 🔹 adiciona colunas faltantes como NULL (opcional, mas RECOMENDADO)
        for col in missing_cols:
            df = df.withColumn(col, F.lit(None).cast(StringType()))
            
        # 🔹 garante ordem final completa
        final_cols = [c for c in fields if c in df.columns]

        return df.select(*final_cols)

    def _build_entity_select(self, df, entity_name, cfg):
        

        fields = cfg.get("fields", [])
        uid = cfg.get("uid", "_row_id")
        parent_uid = cfg.get("parent_uid")

        select_exprs = []

        if uid:
            select_exprs.append(F.col(uid).alias(f"{entity_name}_UID"))

        if parent_uid:
            select_exprs.append(
                F.col(parent_uid).alias(f"{entity_name}_PARENT_UID")
            )

        for field in fields:
            if isinstance(field, dict):
                for original, alias in field.items():
                    select_exprs.append(F.col(original).alias(alias))
            else:
                select_exprs.append(F.col(field))

        return df.select(*select_exprs)

    def _normalize_and_validate_entities(self, config):

        entities = config.get("entities", {})
        base_entity = config.get("base_entity")

        if base_entity not in entities:
            raise ValueError(f"Base entity '{base_entity}' nao existe no config")

        normalized = {}

        for name, cfg in entities.items():

            cfg = cfg.copy()

            join_cfg = cfg.get("join")

            # =====================================================
            # 🔥 AUTO-DETECÇÃO DE parent_uid
            # =====================================================
            if join_cfg:
                strategy = join_cfg.get("strategy")

                if strategy == "parent_child":

                    # 🔥 se nao declarou, assume padrão
                    if "parent_uid" not in cfg:
                        cfg["parent_uid"] = "_parent_uid_final"

            normalized[name] = cfg

        # =====================================================
        # 🔥 VALIDAÇÃO DE JOINS
        # =====================================================
        for name, cfg in normalized.items():

            if name == base_entity:
                continue

            join_cfg = cfg.get("join")

            if not join_cfg:
                raise ValueError(f"{name} possui join ausente")

            keys = join_cfg.get("keys", [])

            if not keys:
                raise ValueError(f"{name} possui join sem keys")

            for key in keys:

                left = key.get("left")
                right = key.get("right")

                if not left or not right:
                    raise ValueError(f"{name} possui key inválida: {key}")

                # 🔥 valida naming pattern
                if not left.endswith(("_UID", "_PARENT_UID", "FILE_ID")):
                    raise ValueError(
                        f"{name}: coluna left '{left}' nao segue padrão esperado"
                    )

                if not right.endswith(("_UID", "_PARENT_UID", "FILE_ID")):
                    raise ValueError(
                        f"{name}: coluna right '{right}' nao segue padrão esperado"
                    )

        return normalized

    def _validate_parent_child_consistency(self, entities_cfg):

        for name, cfg in entities_cfg.items():

            join_cfg = cfg.get("join")
            if not join_cfg:
                continue

            if join_cfg.get("strategy") != "parent_child":
                continue

            keys = join_cfg.get("keys", [])

            for key in keys:

                right = key.get("right")

                # 🔥 extrai entidade alvo
                if right.endswith("_UID"):
                    parent_entity = right.replace("_UID", "")
                else:
                    continue

                parent_cfg = entities_cfg.get(parent_entity)

                if not parent_cfg:
                    raise ValueError(
                        f"{name} referencia entidade '{parent_entity}' inexistente"
                    )

                # 🔥 aqui está o erro que você pediu
                if "uid" not in parent_cfg:
                    raise ValueError(
                        f"{parent_entity} exige 'uid' mas nao foi definido"
                    )

    def _validate_columns_exist(self, entities_cfg, dfs):

        for name, cfg in entities_cfg.items():

            df = dfs.get(name)

            if df is None:
                continue

            fields = cfg.get("fields", [])

            for field in fields:

                if isinstance(field, dict):
                    for original in field.keys():
                        if original not in df.columns:
                            raise ValueError(
                                f"{name}: coluna '{original}' nao existe em df"
                            )
                else:
                    if field not in df.columns:
                        raise ValueError(f"{name}: coluna '{field}' nao existe em df")

    def _extract_registers_from_general_config(self, config_report: dict) -> List[str]:
            """Helper para extrair os registros necessários do novo formato YAML"""
            registers = set()
            
            for doc_cfg in config_report.get("document_sources", {}).values():
                # Pega a base
                if "base" in doc_cfg:
                    registers.add(str(doc_cfg.get("base")))
                    # Pega os filhos
                    for child in doc_cfg.get("children", []):
                        registers.add(str(child.get("source")))
                else:
                    # Pega sources simples
                    registers.add(str(doc_cfg.get("source")))
                
            # Pega dos enrichments
            for enrich_cfg in config_report.get("enrichments", {}).items():
                registers.add(str(enrich_cfg[1].get("source")))
                
            return list(registers)

    def build_union_report(self, report_key: str) -> "Self":
        """
        Constrói relatório no modo UNION:
        - Cada source tem seu root + enrichments (joins internos)
        - Sources são empilhados via UNION ALL com schema canônico
        - Zero risco de produto cartesiano entre sources distintos
        """
        config = self.config.get(report_key)
        if not config:
            raise ValueError(f"Config '{report_key}' nao encontrada")

        if config.get("mode") != "union":
            raise ValueError(
                f"'{report_key}' nao e modo union. Use build_general_report."
            )

        output_schema: List[str] = config.get("output_schema", [])
        sources_cfg: dict = config.get("sources", {})

        if not sources_cfg:
            raise ValueError(f"'{report_key}' nao possui sources definidos")

        built_sources: List[DataFrame] = []

        for source_name, source_cfg in sources_cfg.items():
            self.logger.info(f"Processando source: {source_name}")

            df = self._build_source(source_name, source_cfg)

            if df is None:
                self.logger.warning(f"Source '{source_name}' vazio — ignorado no union")
                continue

            built_sources.append(df)

        if not built_sources:
            self.logger.warning(f"Todos os sources de '{report_key}' vazios")
            self.df_gold = None
            return self

        # UNION ALL com alinhamento automático de schema
        df_gold = self._union_with_schema(built_sources, output_schema)

        # Validação final de metadados obrigatórios
        required = ["FILE_ID", "DOC_TYPE"]
        missing = [c for c in required if c not in df_gold.columns]
        if missing:
            raise ValueError(f"Metadados ausentes no relatório '{report_key}': {missing}")

        self.logger.info(f"Gold '{report_key}' construído — {df_gold.count()} linhas")
        self.df_gold = df_gold
        return self

    def _build_source(self, source_name: str, source_cfg: dict):
        """
        Constrói um DataFrame para um source:
        1. Carrega o root
        2. Aplica enrichments (parent_ref, child_agg, file_ref)
        3. Aplica field_mapping para schema canônico
        4. Adiciona DOC_TYPE literal
        """
        root_register = source_cfg.get("root")
        doc_type = source_cfg.get("doc_type")
        join_type = source_cfg.get("join_type", "left")
        enrichments = source_cfg.get("enrichments", [])
        field_mapping = source_cfg.get("field_mapping", {})

        df_root = self.dfs.get(root_register)
        if df_root is None:
            self.logger.warning(f"Root '{root_register}' nao encontrado")
            return None

        df = df_root

        # Aplica cada enrichment
        for enrich in enrichments:
            df = self._apply_enrichment(df, enrich, join_type)

        # Adiciona DOC_TYPE antes do mapeamento
        df = df.withColumn("DOC_TYPE", F.lit(doc_type))

        # Aplica field_mapping: seleciona e renomeia para schema canônico
        df = self._apply_field_mapping(df, field_mapping, source_name)

        return df

    def _apply_enrichment(self, df: DataFrame, enrich: dict, default_join_type: str) -> DataFrame:
        """
        Aplica um enrichment ao DataFrame do root.

        Estratégias:
        - file_ref:   join simples por FILE_ID (N:1 garantido pelo lado direito)
        - parent_ref: root e filho, busca dados do pai (N:1)
        - child_agg:  root e pai, agrega filhos antes do join (evita explosão)
        - lookup_ref: join por chave de negócio (N:1 garantido pelo lado direito)
        """
        register = enrich.get("register")
        strategy = enrich.get("strategy")
        key_cfg = enrich.get("key", {})
        fields = enrich.get("fields", [])
        join_type = enrich.get("join_type", default_join_type)

        df_right = self.dfs.get(register)
        if df_right is None:
            self.logger.warning(f"Enrich '{register}' nao encontrado — pulado")
            return df

        key_from = key_cfg.get("from")   # coluna no df esquerdo (root)
        key_to = key_cfg.get("to")       # coluna no df direito (enrich)

        if not key_from or not key_to:
            raise ValueError(f"Enrichment '{register}' com key inválida: {key_cfg}")

        # Valida que as colunas de join existem
        if key_from not in df.columns:
            raise ValueError(
                f"Coluna de join '{key_from}' nao existe no root. "
                f"Disponíveis: {df.columns}"
            )
        if key_to not in df_right.columns:
            raise ValueError(
                f"Coluna de join '{key_to}' nao existe em '{register}'. "
                f"Disponíveis: {df_right.columns}"
            )

        # Resolve os campos solicitados
        select_exprs = [F.col(key_to)]  # sempre inclui a chave de join
        for field in fields:
            if isinstance(field, dict):
                for original, alias in field.items():
                    select_exprs.append(F.col(original).alias(alias))
            else:
                select_exprs.append(F.col(field))

        if strategy == "child_agg":
            # Agrega os filhos antes do join para manter 1 linha por root
            agg_func = enrich.get("agg", "first")
            df_right = self._aggregate_children(
                df_right, key_to, fields, agg_func
            )
        else:
            # file_ref, parent_ref, lookup_ref: seleciona campos relevantes
            df_right = df_right.select(*select_exprs)

        # Remove colunas duplicadas (exceto a chave de join)
        overlapping = [
            c for c in df_right.columns
            if c in df.columns and c != key_to
        ]
        if overlapping:
            self.logger.info(
                f"'{register}': removendo colunas duplicadas: {overlapping}"
            )
            df_right = df_right.drop(*overlapping)

        # Garante tipo string nas chaves de join
        df = df.withColumn(key_from, F.col(key_from).cast("string"))
        df_right = df_right.withColumn(key_to, F.col(key_to).cast("string"))

        df = df.join(
            df_right,
            df[key_from] == df_right[key_to],
            how=join_type
        ).drop(df_right[key_to])

        self.logger.info(
            f"Enrichment '{register}' ({strategy}) aplicado: "
            f"{key_from}={key_to} | tipo={join_type}"
        )
        return df

    def _aggregate_children(self, df_child: DataFrame, group_key: str, fields: list, agg_func: str = "first") -> DataFrame:
        """
        Agrega registros filhos por group_key antes do join com o pai.
        Evita multiplicação de linhas (explosão cartesiana).

        agg_func:
        - "first": first() — para campos onde qualquer valor representa o grupo
        - "collect_list": collect_list() — para trazer todos os valores como array
        """
        agg_exprs = []

        for field in fields:
            if isinstance(field, dict):
                for original, alias in field.items():
                    col_name = original
                    out_alias = alias
            else:
                col_name = field
                out_alias = field

            if col_name not in df_child.columns:
                self.logger.warning(
                    f"Campo '{col_name}' nao existe no filho — pulado na agregação"
                )
                continue

            if agg_func == "collect_list":
                agg_exprs.append(
                    F.collect_list(F.col(col_name)).alias(out_alias)
                )
            else:
                agg_exprs.append(
                    F.first(F.col(col_name), ignorenulls=True).alias(out_alias)
                )

        return df_child.groupBy(group_key).agg(*agg_exprs)

    def _apply_field_mapping(self, df: DataFrame, field_mapping: dict, source_name: str) -> DataFrame:
        """
        Aplica o mapeamento source → schema canônico.
        Campos nao mapeados são descartados.
        DOC_TYPE e sempre incluído (já foi adicionado antes).
        """
        select_exprs = [F.col("DOC_TYPE")]

        for source_col, canonical_col in field_mapping.items():
            if source_col not in df.columns:
                self.logger.warning(
                    f"Source '{source_name}': campo '{source_col}' "
                    f"ausente no DataFrame — será null no output"
                )
                select_exprs.append(F.lit(None).cast("string").alias(canonical_col))
            elif source_col == canonical_col:
                select_exprs.append(F.col(source_col))
            else:
                select_exprs.append(F.col(source_col).alias(canonical_col))

        return df.select(*select_exprs)

    def _union_with_schema(self, dfs: List[DataFrame], output_schema: List[str]) -> DataFrame:
        """
        Faz UNION ALL alinhando todos os DataFrames ao schema canônico.
        Colunas ausentes em algum source viram null automaticamente.
        """
        aligned = []

        for df in dfs:
            select_exprs = []
            for col_name in output_schema:
                if col_name in df.columns:
                    select_exprs.append(F.col(col_name))
                else:
                    select_exprs.append(F.lit(None).cast("string").alias(col_name))
            # DOC_TYPE sempre vem primeiro, garante presença
            if "DOC_TYPE" not in output_schema:
                select_exprs.insert(0, F.col("DOC_TYPE"))
            aligned.append(df.select(*select_exprs))

        return reduce(DataFrame.union, aligned)
    
    def _normalize_dataframe_for_join(self, df: DataFrame, entity_name: str, cfg: dict) -> DataFrame:
        """
        Normaliza DF para padrão universal baseado na silver:
        - _row_id (UID real)
        - _parent_uid_final (link hierárquico)
        - campos de negócio (com rename YAML)
        """

        

        fields = cfg.get("fields", [])
        select_cols = []
        selected_names = set()

        # =====================================================
        # 1️⃣ CAMPOS DE NEGÓCIO (YAML)
        # =====================================================
        for col in fields:

            if isinstance(col, dict):

                for source, alias in col.items():

                    if source in df.columns:
                        select_cols.append(
                            F.col(source).alias(alias)
                        )
                        selected_names.add(alias)

                    else:
                        self.logger.warning(
                            f"{entity_name}: coluna '{source}' nao encontrada"
                        )

            else:

                if col in df.columns:
                    select_cols.append(F.col(col))
                    selected_names.add(col)

                else:
                    self.logger.warning(
                        f"{entity_name}: coluna '{col}' nao encontrada"
                    )

        # =====================================================
        # 2️⃣ METADADOS CANÔNICOS (AUTOMÁTICOS)
        # =====================================================
        metadata_cols = [
            "FILE_ID",
            "CNPJ",
            "NOME",
            "DT_INI",
            "OBRIG_ACESS"
        ]

        for col_name in metadata_cols:

            if (
                col_name in df.columns
                and col_name not in selected_names
            ):
                select_cols.append(F.col(col_name))
                selected_names.add(col_name)

        # =====================================================
        # 3️⃣ UID UNIVERSAL (OBRIGATÓRIO)
        # =====================================================
        if "_row_id" in df.columns:
            select_cols.append(F.col("_row_id"))
        else:
            raise ValueError(
                f"{entity_name} nao possui _row_id — silver inconsistente"
            )

        # =====================================================
        # 4️⃣ PARENT UID UNIVERSAL
        # =====================================================
        if "_parent_uid_final" in df.columns:
            select_cols.append(F.col("_parent_uid_final"))
        else:
            self.logger.warning(
                f"{entity_name} sem _parent_uid_final"
            )

        # =====================================================
        # 5️⃣ REG (DEBUG)
        # =====================================================
        if "REG" in df.columns:
            select_cols.append(F.col("REG"))

        # =====================================================
        # 6️⃣ SELECT FINAL
        # =====================================================
        df_norm = df.select(*select_cols)

        return df_norm
        
    def _create_empty_dataframe_from_config(self, spark, entity_name: str, cfg: dict) -> DataFrame:
        fields = cfg.get("fields", [])
        struct_fields = []

        for col in fields:
            if isinstance(col, dict):
                # caso { origem: destino }
                for _, alias in col.items():
                    struct_fields.append(StructField(alias, StringType(), True))
            else:
                struct_fields.append(StructField(col, StringType(), True))

        # Campos técnicos
        if cfg.get("uid"):
            struct_fields.append(StructField(f"{entity_name}_UID", StringType(), True))

        if cfg.get("parent_uid"):
            struct_fields.append(StructField(f"{entity_name}_PARENT_UID", StringType(), True))

        schema = StructType(struct_fields)

        return spark.createDataFrame([], schema)

    def _fill_nulls_with_zero(self, df: DataFrame, entities_cfg: dict) -> DataFrame:
        """
        Substitui NULL por 0 nos campos numéricos definidos no YAML
        """
        numeric_like_cols = []

        for entity, cfg in entities_cfg.items():
            for col in cfg.get("fields", []):
                if col in df.columns:
                    numeric_like_cols.append(col)

        for col in numeric_like_cols:
            df = df.withColumn(col, coalesce(F.col(col), lit(0)))

        return df
    
    def _unpivot_by_yaml_config(self, df: DataFrame, entities_cfg: dict) -> DataFrame:

        import re
        from collections import defaultdict
        

        pattern = re.compile(r"(.+)_(0000|[A-Z]\d{3})$")
        field_groups = defaultdict(dict)

        # 1️⃣ mapear colunas
        for col in df.columns:
            match = pattern.match(col)
            if match:
                base, reg = match.groups()
                field_groups[reg][base] = col

        # 2️⃣ ordem via YAML
        all_fields = []
        seen = set()

        for _, cfg in entities_cfg.items():
            for f in cfg.get("fields", []):
                base = list(f.values())[0] if isinstance(f, dict) else f
                if base not in seen:
                    all_fields.append(base)
                    seen.add(base)

        # 3️⃣ colunas fixas
        id_cols = [
            c for c in df.columns
            if not pattern.match(c)
            and c not in all_fields
            and not c.endswith("_UID")
        ]

        dfs = []

        # 4️⃣ gerar cada registro corretamente
        for reg in entities_cfg.keys():

            uid_col = f"{reg}_UID"

            if uid_col in df.columns:
                df_filtered = df.filter(F.col(uid_col).isNotNull())
            elif reg == "0000":
                df_filtered = df
            else:
                continue

            cols_map = field_groups.get(reg, {})

            select_expr = [F.col(c) for c in id_cols]
            select_expr.append(F.lit(reg).alias("TIPO_REG"))

            for base in all_fields:
                col_name = cols_map.get(base)

                if col_name:
                    select_expr.append(F.col(col_name).alias(base))
                else:
                    select_expr.append(F.lit(None).cast("string").alias(base))

            df_part = df_filtered.select(*select_expr)

            dfs.append(df_part)

        # proteção
        if not dfs:
            return df.limit(0)

        # 🔥 UNION CORRETO (um único DF final)
        from functools import reduce
        df_long = reduce(lambda a, b: a.unionByName(b), dfs)

        return df_long

    def _prefix_columns(self, df, prefix, exclude=None):
        exclude = exclude or []
        return df.select([
            F.col(c).alias(c if c in exclude else f"{prefix}__{c}")
            for c in df.columns
        ])

    def run_complete_gold_build(self, client_cnpj: str, month_year: str, reports: List[str], sped_type: str = None):
        """
        Orquestrador unificado. Detecta o tipo de relatório pelo YAML e usa o motor correto.
        """
        for report_key in reports:
            self.logger.info(f"Iniciando processamento do relatorio: {report_key}")
            config = self.config.get(report_key)
            
            if not config:
                self.logger.warning(f"Config nao encontrada para {report_key}")
                continue

            # =====================================================
            # 🔥 O GRANDE IF: Roteamento Baseado no YAML
            # =====================================================
            if "document_sources" in config:
                # É o RELATÓRIO GERAL (A100+A170 flatten, C100+C170 flatten, UNION, etc)
                self.logger.info(f"-> Detectado modo: RELATORIO GERAL (Union/Flatten)")
                registers = self._extract_registers_from_general_config(config)
                
                path_silver =self.load_silver_canonical(
                    sped_type=sped_type, ano=month_year, 
                    client_cnpj=client_cnpj, registers=registers
                )

                # self.logger.info(f'Este e o path carregado da silver {path_silver}')

                report_builder = ReportBuilder(
                    spark=self.spark,
                    dfs=self.dfs,
                    config=self.config,
                    logger=self.logger
                )
                report_builder.build_general_report(report_key)
                df_resultado = report_builder.df_gold

            else:
                # É um dos 95% (Relatórios Hierárquicos normais: base_entity + joins)
                self.logger.info(f"-> Detectado modo: HIERARQUICO (Pai-Filho)")
                registers = self._extract_registers_from_config(config)
                
                self.load_silver_canonical(
                    sped_type=sped_type, ano=month_year, 
                    client_cnpj=client_cnpj, registers=registers
                )
                
                # Usa o seu método original que já funciona
                builder = self.build(
                    fiscal_mode="AUXILIARY",
                    config=config,
                    report_key=report_key
                )
                df_resultado = builder.df_gold if builder else None

            # =====================================================
            # FIM DO IF. O resto e comum a todos os relatórios.
            # =====================================================
            if df_resultado is None or df_resultado.limit(1).count() == 0:
                self.logger.warning(f"{report_key} vazio - ignorado")
                continue

            self.df_gold = df_resultado
            
            path = self._build_output_path(client_cnpj, month_year, report_key, sped_type)
            self.logger.info(f"Caminho de saida: {path}")
            
            self.write_gold(output_path=path)
            self.logger.info(f'Relatorio {report_key} concluido com sucesso')

    def build_auxiliary_report(self, report_key: str, output_mode="wide"):         

        config = self.config.get(report_key)
        if not config:
            raise ValueError(f"Configuração {report_key} nao encontrada")

        entities_cfg = config.get("entities")
        base_entity_name = config.get("base_entity")

        self.logger.info(f"Construindo relatorio {report_key}")

        # =====================================================
        # 1️⃣ LOAD LIMPO
        # =====================================================
        built_dfs = {}

        for name, cfg in entities_cfg.items():
            df_source = self.dfs.get(cfg["source"])

            if df_source is None:
                self.logger.warning(f"{name} nao encontrado — criando DF vazio com schema do YAML")

                fields = cfg.get("fields", [])
                
                cols = []
                for f in fields:
                    if isinstance(f, dict):
                        for _, alias in f.items():
                            cols.append(alias)
                    else:
                        cols.append(f)

                cols += ["_row_id", "_parent_uid_final"]
                schema = StructType([StructField(c, StringType(), True) for c in cols])
                df = self.spark.createDataFrame([], schema)

            else:
                df = self._normalize_dataframe_for_join(df_source, name, cfg)

            df = df.drop("row_id", "parent_row_id")
            
            if "_row_id" not in df.columns:
                df = df.withColumn("_row_id", F.lit(None).cast("string"))
                self.logger.warning(f"Coluna '_row_id' nao encontrada em {name}")

            if "_parent_uid_final" not in df.columns:
                df = df.withColumn("_parent_uid_final", F.lit(None).cast("string"))
                self.logger.warning(f"Coluna '_parent_uid_final' nao encontrada em {name}")

            df = df.withColumn("row_id", F.col("_row_id").cast("string"))
            df = df.withColumn("parent_row_id", F.col("_parent_uid_final").cast("string"))

            built_dfs[name] = df

        # =====================================================
        # 2️⃣ DETECTA O NÓ FOLHA DA CADEIA PRINCIPAL
        #    (quem ninguém referencia como join_on, exceto 0000)
        # =====================================================
        declared_parents = set()
        for name, cfg in entities_cfg.items():
            parent = cfg.get("join_on")
            if parent:
                declared_parents.add(parent)

        leaf_entity = None
        for name in entities_cfg:
            if name != "0000" and name not in declared_parents:
                leaf_entity = name
                break

        if leaf_entity is None:
            leaf_entity = base_entity_name

        # self.logger.info(f"Grão do relatório (nó folha): {leaf_entity}")

        # =====================================================
        # 3️⃣ CONSTRÓI CADEIA PRINCIPAL DE BAIXO PARA CIMA
        #    Folha → Pai → Avô → ... (exceto 0000)
        # =====================================================
        parent_of = {}
        for name, cfg in entities_cfg.items():
            parent = cfg.get("join_on")
            if parent:
                parent_of[name] = parent

        join_order = []
        current = leaf_entity
        visited = set()
        while current in parent_of and current not in visited:
            visited.add(current)
            pai = parent_of[current]
            join_order.append((current, pai))
            current = pai

        # self.logger.info(f"Ordem de join (filho pai): {join_order}")

        # df_final começa com a FOLHA (grão real)
        df_final = built_dfs[leaf_entity]
        df_final = df_final.withColumn(f"{leaf_entity}_row_id", F.col("row_id"))

        top_entity = leaf_entity  # será atualizado a cada iteração

        for filho, pai in join_order:
            df_pai = built_dfs.get(pai)
            if df_pai is None:
                self.logger.warning(f"Pai '{pai}' nao encontrado — join ignorado")
                continue

            # self.logger.info(f"JOIN {filho}.parent_row_id → {pai}.row_id")

            business_cols_pai = [
                c for c in df_pai.columns
                if c not in ("row_id", "parent_row_id", "_row_id", "_parent_uid_final", "_parent_uid", "REG")
            ]
            df_pai_clean = df_pai.select(
                F.col("row_id").alias(f"{pai}_row_id"),
                F.col("parent_row_id").alias(f"{pai}_parent_row_id"),
                F.col("_parent_uid_final").alias(f"{pai}_uid_final"),
                *[F.col(c) for c in business_cols_pai]
            )

            overlapping = [
                c for c in df_pai_clean.columns
                if c in df_final.columns
            ]
            if overlapping:
                # self.logger.info(f"  Removendo overlapping: {overlapping}")
                df_pai_clean = df_pai_clean.drop(*overlapping)

            df_final = df_final.join(
                df_pai_clean,
                df_final["parent_row_id"] == df_pai_clean[f"{pai}_row_id"],
                "left"
            ).drop("parent_row_id")

            df_final = df_final.withColumnRenamed(f"{pai}_parent_row_id", "parent_row_id")

            top_entity = pai

        # =====================================================
        # 3.5️⃣ JOIN COM REGISTROS LATERAIS (fora da cadeia principal)
        #      Ex: M510 e M515 que têm join_on: M500/M510
        #      mas nao estão no join_order detectado.
        #      Garante que suas colunas apareçam (NULL se vazios)
        # =====================================================
        entities_in_chain = {"0000", leaf_entity} | {pai for _, pai in join_order}

        for name, cfg in entities_cfg.items():
            if name in entities_in_chain or name == "0000":
                continue

            parent_name = cfg.get("join_on")
            if not parent_name:
                continue

            self.logger.info(f"JOIN LATERAL {name} (pai: {parent_name})")

            df_lateral = built_dfs.get(name)
            if df_lateral is None:
                self.logger.warning(f"  {name} nao encontrado em built_dfs — pulando")
                continue

            business_cols_lateral = [
                c for c in df_lateral.columns
                if c not in ("row_id", "parent_row_id", "_row_id", "_parent_uid_final", "_parent_uid", "REG")
            ]

            parent_key_in_final = f"{parent_name}_row_id"

            if parent_key_in_final not in df_final.columns:
                self.logger.warning(
                    f"  Coluna '{parent_key_in_final}' nao existe em df_final — "
                    f"adicionando colunas de {name} como NULL"
                )
                for col_name in business_cols_lateral:
                    if col_name not in df_final.columns:
                        df_final = df_final.withColumn(col_name, F.lit(None).cast("string"))
                continue

            df_lateral_clean = df_lateral.select(
                F.col("parent_row_id").alias(f"__lateral_{name}_key"),
                *[F.col(c) for c in business_cols_lateral]
            )

            overlapping = [
                c for c in df_lateral_clean.columns
                if c in df_final.columns and c != f"__lateral_{name}_key"
            ]
            if overlapping:
                self.logger.info(f"  Removendo overlapping lateral: {overlapping}")
                df_lateral_clean = df_lateral_clean.drop(*overlapping)

            df_final = df_final.join(
                df_lateral_clean,
                df_final[parent_key_in_final] == df_lateral_clean[f"__lateral_{name}_key"],
                "left"
            ).drop(f"__lateral_{name}_key")

            self.logger.info(f"  JOIN LATERAL {name} aplicado")

        # Materializa para quebrar plano lógico acumulado
        # df_final = df_final.cache()
        # df_final.count()

        # =====================================================
        # 4️⃣ ROOT (0000)
        # =====================================================
        # if "0000" in entities_cfg:
        #     df_root = built_dfs["0000"]
        #     self.logger.info("JOIN ROOT (0000)")

        #     business_cols_root = [
        #         c for c in df_root.columns
        #         if c not in ("row_id", "parent_row_id", "_row_id", "_parent_uid_final", "_parent_uid", "REG")
        #     ]
        #     df_root_clean = df_root.select(
        #         F.col("row_id").alias("__root_row_id"),
        #         *[F.col(c) for c in business_cols_root]
        #     )

        #     overlapping = [
        #         c for c in df_root_clean.columns
        #         if c in df_final.columns and c != "__root_row_id"
        #     ]
        #     if overlapping:
        #         df_root_clean = df_root_clean.drop(*overlapping)

        #     top_uid_col = f"{top_entity}_uid_final"

        #     if top_uid_col in df_final.columns:
        #         join_left = df_final[top_uid_col].cast("string")
        #     else:
        #         self.logger.warning(f"Coluna '{top_uid_col}' nao encontrada — usando _parent_uid_final da folha")
        #         join_left = df_final["_parent_uid_final"].cast("string")

        #     df_final = df_final.join(
        #         df_root_clean,
        #         join_left == df_root_clean["__root_row_id"],
        #         "left"
        #     ).drop("__root_row_id")

        # =====================================================
        # 5️⃣ CLEAN FINAL
        # =====================================================
        if "parent_row_id" in df_final.columns:
            df_final = df_final.drop("parent_row_id")

        tech_cols = [
            c for c in df_final.columns
            if c.endswith("_row_id") and c != "row_id"
        ]
        if tech_cols:
            df_final = df_final.drop(*tech_cols)

        # Remove colunas auxiliares de uid_final
        uid_final_cols = [
            c for c in df_final.columns
            if c.endswith("_uid_final")
        ]
        if uid_final_cols:
            df_final = df_final.drop(*uid_final_cols)

        # self.logger.info(f"FINAL COUNT: {df_final.count()}")
        # self.logger.info(f"FINAL COLS: {df_final.columns}")

        self.df_gold = df_final
        return self

    # Executa a estratégia de construção da camada GOLD baseada no modo fiscal.
    def build(self, fiscal_mode: str, config: dict, report_key: str = None):
        """
        Executa a estratégia de construção da camada GOLD baseada no modo fiscal.
        """
        STRATEGIES = {
            # "DOCUMENTOS": self.build_contrib_documentos,
            # "SERVICOS": self.build_contrib_servicos,
            # "CREDITOS": self.build_contrib_creditos,
            # "CONTRIB": self.build_auxiliary_report,
            "AUXILIARY": self.build_auxiliary_report,
        }


        if fiscal_mode not in STRATEGIES:
            raise ValueError(f"Modo fiscal '{fiscal_mode}' inválido")

        if fiscal_mode == "AUXILIARY":
            STRATEGIES[fiscal_mode](report_key)
            return self

        STRATEGIES[fiscal_mode]()

        # Se o modo estiver nesta lista, NÃO chamamos o build_table_gold_fiscal.
        standalone_modes = ["DOCUMENTOS", "ENERGIA", "TRANSPORTES", "TELECOMUNICACOES", "IMOBILIZADO"]

        if fiscal_mode in standalone_modes:
            # Apenas valida se o df_gold foi criado
            if self.df_gold is None:
                self.logger.warning(f"GOLD {fiscal_mode} nao gerada (df_gold e None)")
            
            # Retorna self sem chamar build_table_gold_fiscal
            return self

        return self.build_table_gold_contrib(config)


class ReportBuilder:
    
    def __init__(self, spark, dfs: Dict[str, DataFrame], config: dict, logger: logging.Logger):
        self.spark = spark
        self.dfs = dfs
        self.config = config
        self.logger = logger
        self.df_gold = None

    def build_general_report(self, report_key: str) -> 'ReportBuilder':
        """
        Constrói relatório geral baseado em UNION + ENRICHMENTS
        """
        config = self.config.get(report_key)
        
        if not config:
            raise ValueError(f"Configuração '{report_key}' nao encontrada")

        self.logger.info(f"Construindo relatorio: {report_key}")

        # =====================================================
        # 1. VALIDAÇÃO DA CONFIGURAÇÃO
        # =====================================================
        self._validate_config(config)

        # =====================================================
        # 2. BUILD DOS DOCUMENTOS (UNION)
        # =====================================================
        df_union = self._build_document_union(config)
        
        self.logger.info(f"UNION concluido: {df_union.count()} linhas totais")

        # =====================================================
        # 3. ENRIQUECIMENTOS (JOINS)
        # =====================================================
        df_enriched = self._apply_enrichments(df_union, config)

        # =====================================================
        # 4. REORDENAR COLUNAS
        # =====================================================
        df_final = self._apply_output_columns(df_enriched, config)

        # =====================================================
        # 5. VALIDAÇÃO FINAL
        # =====================================================
        self._validate_final_report(df_final, report_key)

        self.df_gold = df_final
        self.logger.info(f"Relatorio '{report_key}' construido com sucesso")

        return self      

    def _validate_config(self, config: dict):
            """Valida estrutura mínima do config"""
            
            if "document_sources" not in config:
                raise ValueError("Config deve ter 'document_sources'")
            
            if not config["document_sources"]:
                raise ValueError("'document_sources' nao pode ser vazio")
           
            # Valida se todos os sources existem nos DataFrames
            for doc_name, doc_cfg in config["document_sources"].items():
                source = doc_cfg.get("source")
                df = self.dfs.get(source)
                
                # 🔥 VERIFICA SE É NONE (erro no load_silver) OU AUSENTE
                if df is None:
                    self.logger.warning(
                        f"Source '{source}' (doc '{doc_name}') está vazio ou nao foi carregado. Será ignorado."
                    )
                    continue  # Pula para o próximo
                
                # Valida campos (só passa aqui se o df nao for None)
                for field in doc_cfg.get("fields", []):
                    col_name = field if isinstance(field, str) else list(field.keys())[0]
                    if col_name not in df.columns:
                        self.logger.warning(f"Coluna '{col_name}' nao existe no source '{source}'")
            
            # Valida enrichments
            for enrich_name, enrich_cfg in config.get("enrichments", {}).items():
                source = enrich_cfg.get("source")
                df = self.dfs.get(source)
                
                # 🔥 VERIFICA SE É NONE TAMBÉM AQUI
                if df is None:
                    self.logger.warning(f"Enrichment '{enrich_name}': source '{source}' nao encontrado ou vazio - será ignorado")

    def _build_document_union(self, config: dict) -> DataFrame:
        document_sources = config.get("document_sources", {})
        union_dfs = []

        for doc_name, doc_cfg in document_sources.items():
            reg_type = doc_cfg.get("reg_type", doc_name)
            fields = doc_cfg.get("fields", [])
            df = None  # inicializa sempre

            if "base" in doc_cfg:
                base_source = doc_cfg.get("base")
                df_base = self.dfs.get(base_source)

                if df_base is None:
                    self.logger.warning(
                        f"  ⚠️ {doc_name}: Base '{base_source}' e None. "
                        f"Todos os campos serao NULL no union."
                    )
                    # ✅ NÃO faz continue — deixa df=None e cai no select com NULLs abaixo
                else:
                    df = df_base.alias("base")

                    for child in doc_cfg.get("children", []):
                        child_source = child.get("source")
                        child_join_col = child.get("join_on")
                        parent_join_col = child.get("join_to")

                        df_child = self.dfs.get(child_source)
                        if df_child is None:
                            self.logger.warning(
                                f"    ⚠️ Filho '{child_source}' e None. "
                                f"Campos do filho virao NULL."
                            )
                            continue

                        child_select_cols = [F.col(child_join_col)]
                        for c in df_child.columns:
                            if c not in [child_join_col, parent_join_col]:
                                child_select_cols.append(F.col(c))

                        df_child_clean = (
                            df_child
                            .select(*child_select_cols)
                            # .dropDuplicates([child_join_col])
                        )

                        overlapping = [
                            c for c in df_child_clean.columns
                            if c in df.columns and c != child_join_col
                        ]
                        if overlapping:
                            self.logger.info(f"Removendo colunas duplicadas: {overlapping}")
                            df_child_clean = df_child_clean.drop(*overlapping)

                        df = (
                            df
                            .join(
                                df_child_clean.alias("child"),
                                F.col(f"base.{parent_join_col}") == F.col(f"child.{child_join_col}"),
                                how="left"
                            )
                            .drop(F.col(f"child.{child_join_col}"))
                        )

                        # self.logger.info(f"    ✅ Flatten: {base_source} ← {child_source}")

            else:
                source = doc_cfg.get("source")
                df = self.dfs.get(source)

                if df is None:
                    self.logger.warning(
                        f"  ⚠️ {doc_name}: DataFrame '{source}' e None. "
                        f"Todos os campos serao NULL no union."
                    )
                    # ✅ NÃO faz continue — df=None e cai no select com NULLs abaixo

            # =====================================================
            # SELECT FINAL: df pode ser None — todos os campos viram NULL
            # =====================================================
            select_exprs = [F.lit(reg_type).alias("REG_TIPO")]

            field_names = [
                f if isinstance(f, str) else list(f.keys())[0]
                for f in fields
            ]
            if "FILE_ID" not in field_names:
                if df is not None and "FILE_ID" in df.columns:
                    select_exprs.append(F.col("FILE_ID"))
                else:
                    select_exprs.append(F.lit(None).cast("string").alias("FILE_ID"))

            for field in fields:
                if isinstance(field, dict):
                    original_col, alias_col = list(field.items())[0]
                    if df is not None and original_col in df.columns:
                        select_exprs.append(F.col(original_col).alias(alias_col))
                    else:
                        if df is None:
                            self.logger.warning(
                                f"    ⚠️ '{doc_name}' sem dados — campo '{original_col}' → NULL"
                            )
                        else:
                            self.logger.warning(
                                f"    ⚠️ Coluna '{original_col}' nao encontrada em '{doc_name}' — NULL"
                            )
                        select_exprs.append(F.lit(None).cast("string").alias(alias_col))
                else:
                    if df is not None and field in df.columns:
                        select_exprs.append(F.col(field))
                    else:
                        if df is None:
                            self.logger.warning(
                                f"    ⚠️ '{doc_name}' sem dados — campo '{field}' → NULL"
                            )
                        else:
                            self.logger.warning(
                                f"    ⚠️ Coluna '{field}' nao encontrada em '{doc_name}' — NULL"
                            )
                        select_exprs.append(F.lit(None).cast("string").alias(field))

            # Para criar o DataFrame de NULLs quando df e None, precisamos de uma âncora
            if df is None:
                # Cria um DataFrame vazio com schema correto — zero linhas, colunas corretas
                # Isso e intencional: o bloco nao tem dados mas garante as colunas no union
                self.logger.warning(
                    f"  ⚠️ {doc_name}: Nenhum dado disponível. "
                    f"Bloco incluído no union com 0 linhas e {len(select_exprs)} colunas NULL."
                )
                # Cria schema a partir das expressões
                
                empty_schema = StructType([
                    StructField("REG_TIPO", StringType(), True),
                    *[StructField(
                        alias_col if isinstance(f, dict) else (f if isinstance(f, str) else list(f.keys())[0]),
                        StringType(), True
                    ) for f, alias_col in [
                        (f, list(f.values())[0]) if isinstance(f, dict) else (f, f)
                        for f in fields
                    ]]
                ])
                # Adiciona FILE_ID se nao estava nos fields
                if "FILE_ID" not in field_names:
                    empty_schema = StructType(
                        [StructField("REG_TIPO", StringType(), True), StructField("FILE_ID", StringType(), True)]
                        + [f for f in empty_schema.fields if f.name not in ("REG_TIPO", "FILE_ID")]
                    )
                df_selected = self.spark.createDataFrame([], empty_schema)
            else:
                df_selected = df.select(*select_exprs)

            union_dfs.append(df_selected)
            row_count = df_selected.count()
            self.logger.info(f" {doc_name}: {row_count} linhas")

        if not union_dfs:
            raise ValueError("Nenhum documento valido para unir")

        df_union = union_dfs[0]
        for df_part in union_dfs[1:]:
            df_union = df_union.unionByName(df_part, allowMissingColumns=True)

        df_union = self._fix_void_columns(df_union)

        self.logger.info("=" * 80)
        self.logger.info("📊 AMOSTRA DO UNION (primeiras 5 linhas):")
        df_union.show(5, truncate=False, vertical=True)
        self.logger.info("📈 CONTAGEM POR REG_TIPO:")
        df_union.groupBy("REG_TIPO").count().show()

        return df_union

    def _fix_void_columns(self, df: DataFrame) -> DataFrame:
        """
        Corrige colunas com tipo NullType (que ocorrem quando F.lit(None) e usado sem cast).
        O Parquet nao suporta esse tipo, então convertemos para StringType.
        """
        
        select_exprs = []
        null_columns = []
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType
            
            # Verifica se e NullType (o tipo real quando usamos F.lit(None) sem cast)
            if isinstance(col_type, NullType):
                null_columns.append(col_name)
                select_exprs.append(F.lit(None).cast("string").alias(col_name))
            else:
                select_exprs.append(F.col(col_name))
        
        if null_columns:
            self.logger.warning(f"Colunas NullType corrigidas para STRING: {null_columns}")
            return df.select(*select_exprs)
        
        return df

    def _apply_enrichments(self, df_base: DataFrame, config: dict) -> DataFrame:
        """
        Aplica enriquecimentos via JOIN (após o UNION)
        """
        enrichments = config.get("enrichments", {})
        
        if not enrichments:
            self.logger.info("Nenhum enrichment definido")
            return df_base

        df_result = df_base

        for enrich_name, enrich_cfg in enrichments.items():
            source = enrich_cfg.get("source")
            join_type = enrich_cfg.get("join_type", "left")
            join_keys = enrich_cfg.get("join_keys", [])
            join_mode = enrich_cfg.get("join_mode", "simple")
            fields = enrich_cfg.get("fields", [])

            df_enrich = self.dfs.get(source)
            
            if df_enrich is None:
                self.logger.warning(f"Enrichment '{enrich_name}': source nao encontrado - pulando")
                # ✅ Adiciona os campos do enrichment como NULL ao invés de pular
                for field in fields:
                    if isinstance(field, dict):
                        col_name = list(field.values())[0]
                    else:
                        col_name = field
                    if col_name not in df_result.columns:
                        df_result = df_result.withColumn(
                            col_name, F.lit(None).cast("string")
                        )
                continue

            if not join_keys:
                self.logger.warning(f"Enrichment '{enrich_name}': sem join_keys - pulando")
                continue

            self.logger.info(f"Enrichment '{enrich_name}': {join_type} join via {join_keys}")

            # Prepara o DataFrame de enriquecimento (select fields + join columns)
            # Prepara o DataFrame de enriquecimento (select fields + join columns)
            enrich_select_exprs = []
            right_join_cols = []

            for key_spec in join_keys:
                if isinstance(key_spec, dict):
                    _, right_col = list(key_spec.items())[0]
                    right_join_cols.append(right_col)
                else:
                    right_join_cols.append(key_spec)

            # 🔥 CORREÇÃO: Usa um set de strings para evitar comparar String com PySpark Column
            added_cols = set()

            # Adiciona colunas de join ao select
            for col in right_join_cols:
                if col not in added_cols:
                    enrich_select_exprs.append(F.col(col))
                    added_cols.add(col)

            # Adiciona campos solicitados
            for field in fields:
                if isinstance(field, dict):
                    for original, alias in field.items():
                        enrich_select_exprs.append(F.col(original).alias(alias))
                else:
                    enrich_select_exprs.append(F.col(field))

            df_enrich_selected = df_enrich.select(*enrich_select_exprs)

            # Dropa colunas duplicadas
            df_enrich_selected = df_enrich_selected.dropDuplicates(right_join_cols)
            self.logger.info(f"Linhas duplicadas removidas: {df_enrich_selected.count()}")

            # Constroi condição de join
            join_conditions = []
            
            for key_spec in join_keys:
                if isinstance(key_spec, dict):
                    left_col, right_col = list(key_spec.items())[0]
                else:
                    left_col = key_spec
                    right_col = key_spec

                # Garante que colunas existem
                if left_col not in df_result.columns:
                    self.logger.warning(f"    Coluna '{left_col}' nao existe no df_base - pulando este key")
                    continue
                    
                # Cast para string
                df_result = df_result.withColumn(left_col, F.col(left_col).cast("string"))
                df_enrich_selected = df_enrich_selected.withColumn(right_col, F.col(right_col).cast("string"))

                join_conditions.append(F.trim(F.col(left_col)) == F.trim(F.col(right_col)))

            if not join_conditions:
                self.logger.warning(f"Enrichment '{enrich_name}': nenhuma condicao valida - pulando")
                continue

            # =====================================================
            # 🔥 NOVA LÓGICA: Renomeia chaves do lado direito para evitar AMBIGUOUS_REFERENCE
            # =====================================================
            right_join_aliases = {}
            for key_spec in join_keys:
                if isinstance(key_spec, dict):
                    left_col, right_col = list(key_spec.items())[0]
                else:
                    left_col = right_col = key_spec
                    
                unique_name = f"__join_{enrich_name}_{right_col}__"
                df_enrich_selected = df_enrich_selected.withColumnRenamed(right_col, unique_name)
                right_join_aliases[right_col] = unique_name

            # Remove as colunas duplicadas (agora as chaves originais já foram renomeadas)
            overlapping = [c for c in df_enrich_selected.columns if c in df_result.columns]
            df_enrich_clean = df_enrich_selected.drop(*overlapping)

            # Executa JOIN
            if join_mode == "coalesce" and len(join_conditions) > 1:
                # Se for coalesce, usa a função separada passando os aliases
                df_result = self._apply_coalesce_join(df_result, df_enrich_clean, join_keys, join_type, fields, right_join_aliases)
            else:
                # Reconstrói as condições de join usando os nomes únicos do lado direito
                final_conditions = []
                for key_spec in join_keys:
                    if isinstance(key_spec, dict):
                        left_col, right_col = list(key_spec.items())[0]
                    else:
                        left_col = right_col = key_spec
                        
                    unique_right = right_join_aliases[right_col]
                    final_conditions.append(F.trim(F.col(left_col)) == F.trim(F.col(unique_right)))

                # Combina as condições com AND
                condition = final_conditions[0]
                for cond in final_conditions[1:]:
                    condition = condition & cond

                df_result = df_result.join(df_enrich_clean, condition, how=join_type)

            # Remove as colunas de join temporárias após o join terminar
            for unique_name in right_join_aliases.values():
                if unique_name in df_result.columns:
                    df_result = df_result.drop(unique_name)

            self.logger.info(f"Enrichment '{enrich_name}' aplicado")

        return df_result
        
    def _apply_coalesce_join(self, df_left: DataFrame, df_right: DataFrame, 
                             join_keys: list, join_type: str, fields: list, 
                             right_join_aliases: dict) -> DataFrame:
        """
        Aplica join com fallback (tenta primeira chave, se null tenta segunda)
        Exemplo: Tenta match por CNPJ, se falhar tenta por CPF
        """
        # Pega as colunas de saída do right (sem as de join)
        output_cols = []
        for field in fields:
            if isinstance(field, dict):
                output_cols.append(list(field.values())[0])
            else:
                output_cols.append(field)

        # 🔥 REMOVE colunas de join
        output_cols = [c for c in output_cols if c not in right_join_aliases.values()]

        # Monta a lista de pares (coluna_esquerda, coluna_direita_renomeada)
        key_pairs = []
        for key_spec in join_keys:
            if isinstance(key_spec, dict):
                left_col, right_col = list(key_spec.items())[0]
            else:
                left_col = right_col = key_spec
            
            unique_right_col = right_join_aliases.get(right_col)
            if unique_right_col and unique_right_col in df_right.columns:
                key_pairs.append((left_col, unique_right_col))

        if not key_pairs:
            self.logger.warning("Nenhuma chave valida para coalesce join. Retornando df original.")
            return df_left

        left_col_1, right_col_1 = key_pairs[0]
        
        # =====================================================
        # ISOLAMENTO TOTAL: Fatia as colunas usando lista de strings
        # Isso garante que a chave 2 NÃO vá para o 1º join
        # =====================================================
        cols_join_1 = list(dict.fromkeys(
            [right_col_1] + [c for c in output_cols if c in df_right.columns]
        ))
        
        df_right_1 = df_right.select(*cols_join_1)

        # 1º JOIN
        df_result = df_left.join(
            df_right_1,
            F.trim(F.col(left_col_1)) == F.trim(F.col(right_col_1)),
            how=join_type
        ).drop(right_col_1) # Limpa a chave 1

        # =====================================================
        # 2º PASSO: Fallback
        # =====================================================
        if len(key_pairs) > 1:
            left_col_2, right_col_2 = key_pairs[1]
            
            # Fatia pro 2º join (renomeando os outputs com _fb para nao ambiguar)
            fb_select = [right_col_2]
            for c in output_cols:
                if c in df_right.columns:
                    fb_select.append(F.col(c).alias(f"{c}_fb"))
            
            df_right_fb = df_right.select(*fb_select)

            # 🔥 CORREÇÃO FINAL: Parênteses obrigatórios no == para o PySpark entender
            fallback_condition = (
                (F.trim(F.col(left_col_2)) == F.trim(F.col(right_col_2))) & 
                (F.col(output_cols[0]).isNull())
            )
            
            # 2º JOIN
            df_result = df_result.join(df_right_fb, fallback_condition, how="left").drop(right_col_2)

            # Coalesce e limpeza
            for col in output_cols:
                if f"{col}_fb" in df_result.columns:
                    df_result = df_result.withColumn(
                        col, 
                        F.coalesce(F.col(col), F.col(f"{col}_fb"))
                    ).drop(f"{col}_fb")

        return df_result

    def _apply_output_columns(self, df: DataFrame, config: dict) -> DataFrame:
        output_columns = config.get("output_columns")

        if not output_columns:
            self.logger.info("Nenhum output_columns definido - mantendo ordem original")
            return df

        valid_cols = [c for c in output_columns if c in df.columns]
        missing_cols = [c for c in output_columns if c not in df.columns]

        if missing_cols:
            self.logger.warning(
                f"⚠️ Colunas ausentes no df (serao criadas como NULL): {missing_cols}"
            )

        # Colunas extras no df que nao estão no output_columns — vão no final
        extra_cols = [c for c in df.columns if c not in output_columns]

        # ✅ CORREÇÃO: inclui missing_cols (como NULL) na ordem correta
        select_exprs = []
        for c in output_columns:
            if c in df.columns:
                select_exprs.append(F.col(c))
            else:
                # ✅ Coluna declarada no YAML mas ausente no df → NULL
                select_exprs.append(F.lit(None).cast("string").alias(c))

        # Colunas extras no final (nao declaradas no output_columns mas presentes no df)
        for c in extra_cols:
            select_exprs.append(F.col(c))

        return df.select(*select_exprs)
    
    def _validate_final_report(self, df: DataFrame, report_key: str):   
        """Validações finais do relatório"""
        
        required_cols = ["REG_TIPO", "FILE_ID"]
        missing = [c for c in required_cols if c not in df.columns]
        
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

        # Log de estatísticas por tipo
        self.logger.info("Estatisticas por REG_TIPO:")
        df.groupBy("REG_TIPO").count().show()


