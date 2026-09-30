# src/dinamo_web/gold_layer/subvencao_gold_builder.py
"""
Gold Builder do relatorio de Subvencao (diagnostico ICMS — isencao/reducao
em operacoes diretas).

Reaproveita 100% da mecanica de I/O ja validada:
    - load_silver_by_reg(...)     -> leitura Delta da camada Silver (herdado
      de GoldStreamingBuilder), particionada por REG.
    - write_gold(...)             -> escrita Delta com mergeSchema (herdado).
    - sanity_checks()             -> ja herdado, valida self.df_gold nao vazio.
    - connectors.BucketConnector  -> MESMA classe usada em todo o resto do
      dinamo para baixar tabelas de referencia (.xlsx) do bucket: aliquotas
      interestaduais e a tabela de municipios.
    - config.ConfigManager.cfop_config + spark.read.parquet — MESMA tabela
      CFOP oficial (TABELA_CFOP/) ja usada em silver_layer/silver_builder.py,
      reaproveitada aqui em vez de uma lista hardcoded.

REGRA DE NEGOCIO (revisada conforme "Mapeamento Funcional do Processo —
Diagnostico Subvencao Fiscal", versao revisada, e confirmada com a
TABELA_CFOP real):
    1. CST final 40 -> sempre isencao. CST final 20/70 -> sempre reducao.
       CST final 51 -> CONDICIONAL: com valor em VL_RED_BC vai para reducao;
       sem valor vai para isencao (secao 4.5.1 / parametro "CST de isencao").
    2. Isencao: usa a ALIQUOTA DA TABELA INTERESTADUAL (join por UF Origem/
       Destino), NAO a aliquota do proprio EFD — porque em operacao isenta
       o campo de aliquota do documento e sempre 0.
    3. Reducao: usa a ALIQUOTA DO PROPRIO EFD (ALIQ_ICMS do C190).
    4. Modelo 65 (NFC-e) sem UF Origem/Destino: usa a ALIQUOTA INTERNA do
       estado do proprio estabelecimento (secao 4.5.2) — ou seja,
       UF_ORIGEM_DESTINO = "<UF_ESTABELECIMENTO>/<UF_ESTABELECIMENTO>", NAO
       um codigo sentinela tipo "XX/XX" que nao existe na tabela real.
    5. Elegibilidade de CFOP: lista explicita e autoritativa (config.filters.
       cfops.validos), confirmada contra a TABELA_CFOP real — todos os 58
       CFOPs da lista que existem na tabela sao classificados exatamente
       como nat_cfop='Venda' ou 'Devolução de venda' (nenhum outro tipo).
       4 codigos da lista (5107, 5108, 5121, 6121) nao existem na
       TABELA_CFOP — provavelmente descontinuados; entram na elegibilidade
       mesmo assim (lista explicita manda), mas caem no fallback de sinal.
    6. Sinal por CFOP: DERIVADO de nat_cfop (Venda=+1, Devolução de venda=-1)
       quando o CFOP existe na TABELA_CFOP — NAO mais heuristica por
       primeiro digito. cred_deb_cbs_ibs foi checado e NAO e campo de sinal
       (e categoria tipo "Materiais"/"Devoluções"/"Frete" para fins de
       CBS/IBS, confirmado com dado real). A heuristica por digito so
       sobrevive como fallback para os 4 CFOPs ausentes da tabela.

DERIVACAO DE UF_ORIGEM_DESTINO (nao existe pronta em C100/C190) — validada
com amostras reais de 0000/0150:
    - UF do proprio estabelecimento: registro 0000, campo UF — constante
      por arquivo (join por _file_id).
    - UF do participante: registro 0150 (COD_PARTICIPANTE == C100.COD_PART)
      -> campo COD_MUN_PARTICIPANTE -> lookup na tabela auxiliar de
      municipios (TABELA_MUNICIPIO/municipios_brasil.xlsx, colunas cod_mun/uf).
    - Direcao (quem e origem, quem e destino): C100.IND_OPER
      (0 = entrada -> origem = participante, destino = proprio;
       1 = saida   -> origem = proprio, destino = participante).
    - Se qualquer um dos registros 0000/0150 nao estiver disponivel na
      Silver para o CNPJ/periodo, o calculo de isencao e pulado
      explicitamente (com aviso), em vez de gerar um numero incorreto
      silenciosamente. O calculo de reducao nunca depende disso.
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from connectors.bucket_connector import BucketConnector
from src.gold_layer.gold_builder import GoldStreamingBuilder

logger = logging.getLogger("SubvencaoGoldBuilder")


def carregar_referencias_subvencao(spark, bucket_name: str, cfop_key: str,
                                    municipios_key: str, aliquotas_key: str) -> dict:
    """Carrega as 3 tabelas de referencia (CFOP, municipios, aliquotas) via
    Parquet — leitura NATIVA do Spark (spark.read.parquet direto do s3a://),
    sem passar mais por download_file_bytes + pandas.read_excel. Muito mais
    rapido, e sem risco de OOM no driver por parsing de xlsx grande.

    Chamar UMA VEZ so (ex.: em SubvencaoApplication.__init__) e reaproveitar
    o resultado entre varios periodos/CNPJs — sao dados estaticos, nao
    precisam ser recarregados a cada iteracao. Cada DataFrame recebe
    .cache() (Spark, nao so referencia Python) para materializar de fato.
    """
    logger.info(f"Carregando referencias de Subvencao (Parquet): cfop={cfop_key}, "
                f"municipios={municipios_key}, aliquotas={aliquotas_key}")

    df_cfop = (
        spark.read.parquet(f"s3a://{bucket_name}/{cfop_key}")
        .select(
            F.lpad(F.col("cfop").cast("string"), 4, "0").alias("_CFOP_TABELA"),
            F.col("descricao").alias("_DESCRICAO"),
            F.col("natureza").alias("_NATUREZA"),
        )
        .dropDuplicates(["_CFOP_TABELA"])
        .cache()
    )

    df_municipios = (
        spark.read.parquet(f"s3a://{bucket_name}/{municipios_key}")
        .select(
            F.col("cod_mun").cast("string").alias("cod_mun"),
            F.col("uf"),
        )
        .dropna(subset=["cod_mun"])
        .cache()
    )

    df_aliquotas = spark.read.parquet(f"s3a://{bucket_name}/{aliquotas_key}").cache()

    # Forca materializacao agora (nao esperar o primeiro join lazy) — assim
    # o custo de leitura acontece uma vez so, de forma previsivel, no
    # __init__ do app, e nao "some" dentro do primeiro periodo do loop.
    logger.info(
        f"Referencias carregadas: cfop={df_cfop.count()} linhas, "
        f"municipios={df_municipios.count()} linhas, aliquotas={df_aliquotas.count()} linhas"
    )

    return {"cfop": df_cfop, "municipios": df_municipios, "aliquotas": df_aliquotas}


def escanear_periodo_tem_candidatos(spark, silver_base_path: str, cnpj: str, periodo: str,
                                     csts_validos: list, cfops_validos: list) -> int:
    """Escaneamento LEVE: le SO o REG=C190 (sem C100/0000/0150, sem nenhum
    join de UF/aliquota/municipio) e filtra por CST+CFOP elegiveis — exatamente
    o mesmo filtro que build_subvencao() aplicaria de qualquer forma, so que
    ANTES de pagar o custo caro (ler C100, juntar com 0000/0150 pra derivar
    UF, juntar com as tabelas de referencia).

    Uso: rodar isso para TODOS os periodos candidatos primeiro; so chamar
    build_subvencao() (pipeline completo) nos periodos onde retornar > 0.
    Isso evita gastar tempo com os 4 reads Delta + os joins de UF em
    periodos que nao tem nenhuma chance de gerar isencao/reducao.

    Retorna 0 ou 1 (existe pelo menos uma linha candidata ou nao) — NAO a
    contagem real, de proposito: .limit(1).count() para na primeira
    ocorrencia em vez de escanear a particao inteira, que e o ponto de
    isso ser "leve". Para saber quantas linhas de verdade tem, o pipeline
    completo (build_subvencao) ja loga isso.
    """
    path_c190 = f"{silver_base_path.rstrip('/')}/{cnpj}/EFD_FISCAL/{periodo}/REG=C190"
    try:
        df = spark.read.format("delta").load(path_c190)
    except Exception as e:
        logger.warning(f"[Scan] CNPJ={cnpj} periodo={periodo}: nao foi possivel ler C190 ({e}).")
        return 0

    df = df.filter(F.col("CST_ICMS").isin(*csts_validos))
    df = df.withColumn("_CFOP_INT", F.col("CFOP").cast("int")).filter(F.col("_CFOP_INT").isin(*cfops_validos))
    return df.limit(1).count()


class SubvencaoGoldBuilder(GoldStreamingBuilder):
    """
    Uso:
        builder = SubvencaoGoldBuilder(
            spark=spark,
            base_path=f"s3a://{bucket_name}/data-lake/silver",
            period=month_year,
            csts_isencao=config["filters"]["csts"]["isencao"],           # CST 40 e variantes
            csts_reducao=config["filters"]["csts"]["reducao"],           # CST 20/70 e variantes
            csts_condicionais_51=config["filters"]["csts"]["condicionais_51"],  # CST 51 e variantes
            cfops_validos=config["filters"]["cfops"]["validos"],          # lista explicita, autoritativa
            bucket_name=bucket_name,
            aliquotas_key="REFERENCE/aliquotas_interestadual.xlsx",
            municipios_key="TABELA_MUNICIPIO/municipios_brasil.xlsx",
            cfop_key="TABELA_CFOP_SUBVENCAO/tabela_cfop_subvencao.xlsx",  # so p/ derivar sinal
        )
        builder.build_subvencao(client_cnpj="12345678000199", month_year="092022")
        builder.write_gold_subvencao(f"s3a://{bucket_name}/GOLD/12345678000199/SUBVENCAO")
    """

    REGISTROS_NECESSARIOS = ["C100", "C190", "0000", "0150"]

    # Campos que chegam como texto BR (virgula decimal) direto do SPED e
    # precisam de cast antes de qualquer conta.
    CAMPOS_NUMERICOS_BR = ["VL_OPR", "VL_RED_BC", "ALIQ_ICMS"]

    def __init__(self, spark, base_path, period, csts_isencao, csts_reducao,
                 csts_condicionais_51, cfops_validos, bucket_name, aliquotas_key,
                 municipios_key, cfop_key=None, referencias=None):
        super().__init__(spark, base_path, period)
        self.csts_isencao = csts_isencao
        self.csts_reducao = csts_reducao
        self.csts_condicionais_51 = csts_condicionais_51
        self.csts_validos = list(csts_isencao) + list(csts_reducao) + list(csts_condicionais_51)
        # Lista explicita e autoritativa de CFOPs elegiveis (venda/devolucao
        # de venda), confirmada contra a TABELA_CFOP real — ver cabecalho.
        self.cfops_validos = cfops_validos
        self.bucket_name = bucket_name
        self.aliquotas_key = aliquotas_key
        self.municipios_key = municipios_key
        self.cfop_key = cfop_key
        # Mesma classe usada em toda a base para acesso ao bucket (boto3) —
        # nao cria conector novo.
        self.bucket_connector = BucketConnector(spark, bucket_name)
        self.df_isencao = None
        self.df_reducao = None

        # Referencias (CFOP/municipios/aliquotas) — se o CHAMADOR ja
        # carregou uma vez (ex.: SubvencaoApplication rodando --todos-periodos)
        # e passou aqui, reaproveita sem re-ler o bucket. Se nao passou
        # (uso avulso, streaming, ou builder isolado), carrega uma vez e
        # cacheia na instancia — nunca mais de uma vez POR BUILDER, mas
        # idealmente e o CHAMADOR quem evita recriar o builder a toa.
        self._referencias = referencias

    def _obter_referencias(self) -> dict:
        if self._referencias is None:
            self._referencias = carregar_referencias_subvencao(
                self.spark, self.bucket_name, self.cfop_key,
                self.municipios_key, self.aliquotas_key,
            )
        return self._referencias

    # -------------------------------------------------------------------
    # BUILD — filtro de CST/CFOP + calculo de isencao/reducao
    # -------------------------------------------------------------------
    def build_subvencao(self, client_cnpj: str, month_year: str):
        """Nome proposital diferente de build() (metodo generico da classe-mae
        usado pelo motor de relatorios auxiliares com assinatura distinta) —
        para nao colidir com o contrato existente."""
        self.load_silver_by_reg(
            sped_type="EFD_FISCAL",
            ano=month_year,
            client_cnpj=client_cnpj,
            registers=self.REGISTROS_NECESSARIOS,
        )

        df_c190 = self.dfs.get("C190")
        if df_c190 is None:
            logger.warning(
                f"REG C190 nao encontrado para CNPJ={client_cnpj} periodo={month_year} "
                "— Subvencao ignorada para esta combinacao."
            )
            self.df_isencao = None
            self.df_reducao = None
            return self

        self._checar_colunas_obrigatorias(df_c190)

        df = self._normalizar_campos(df_c190, client_cnpj, month_year)

        total_c190 = df.count()
        df = df.filter(F.col("CST_ICMS").isin(*self.csts_validos))
        apos_cst = df.count()
        df = df.filter(F.col("CFOP").isin(*self.cfops_validos))
        apos_cfop = df.count()
        logger.info(
            f"[Subvencao] Diagnostico de filtro CNPJ={client_cnpj} periodo={month_year}: "
            f"C190 total={total_c190} -> apos CST={apos_cst} -> apos CFOP={apos_cfop}"
        )
        df = self._deriva_sinal_cfop(df)

        sinal = F.col("_SINAL_CFOP")

        # ---- CST final 51 e CONDICIONAL (secao 4.5.1 do mapeamento) ----
        # Com valor em VL_RED_BC -> vai para REDUCAO. Sem valor -> ISENCAO.
        tem_valor_reducao = F.col("VL_RED_BC").isNotNull() & (F.col("VL_RED_BC") != 0.0)
        eh_51 = F.col("CST_ICMS").endswith("51")

        cond_isencao = F.col("CST_ICMS").isin(*self.csts_isencao) | (eh_51 & ~tem_valor_reducao)
        cond_reducao = F.col("CST_ICMS").isin(*self.csts_reducao) | (eh_51 & tem_valor_reducao)

        # ---- Enriquecimento COMUM aos dois ramos (UF/aliquota interestadual
        # + campos de documento C100 + participante 0150 + nome do municipio)
        # -- feito uma vez sobre o df inteiro, ANTES de separar isencao/
        # reducao, porque os dois ramos agora saem no mesmo layout de
        # relatorio de negocio (ver _montar_relatorio_final). uf_ok=False
        # so bloqueia ISENCAO (que depende da aliquota da tabela pro
        # CALCULO em si) -- REDUCAO continua sendo gerada mesmo assim,
        # so com UF/Municipio/Aliquota em branco no relatorio (garantia
        # original: "reducao nunca depende disso").
        df_enriquecido, uf_ok = self._enriquecer_uf_e_aliquota(df)
        df_municipios = self._obter_referencias()["municipios"]
        df_enriquecido = self._enriquecer_documento_participante_municipio(df_enriquecido, df_municipios)

        # ---- PROTECAO CONTRA FAN-OUT DOS JOINS ACIMA ----
        df_enriquecido = self._dedup_fan_out(df_enriquecido, "geral (isencao+reducao)")
        # df_enriquecido (que já carrega todos os joins com C100/0150/aliquotas) é usado depois 
        # em dois filtros separados (cond_reducao e cond_isencao). 
        # Sem .cache(), o Spark recomputa a lineage inteira (4 leituras Delta + todos os joins) duas vezes por período — uma pra cada ramo.

        df_enriquecido = df_enriquecido.cache()

        # ---- REDUCAO: usa ALIQ_ICMS do proprio EFD, nao depende de UF ----
        df_reducao_raw = (
            df_enriquecido.filter(cond_reducao)
              .withColumn(
                  "CALCULO_DA_REDUCAO",
                  F.col("VL_RED_BC") * (F.col("ALIQ_ICMS") / F.lit(100.0)) * sinal,
              )
        )
        self.df_reducao = self._montar_relatorio_final(
            df_reducao_raw, "CALCULO_DA_REDUCAO", "CALCULO_DA_REDUCAO"
        )

        # ---- ISENCAO: usa a aliquota da TABELA INTERESTADUAL (join) ----
        if uf_ok:
            df_isencao_raw = (
                df_enriquecido.filter(cond_isencao)
                  .withColumn(
                      "CALCULO_DA_ISENCAO",
                      F.col("VL_OPR") * (F.col("ALIQUOTA_TABELA") / F.lit(100.0)) * sinal,
                  )
            )
            self.df_isencao = self._montar_relatorio_final(
                df_isencao_raw, "CALCULO_DA_ISENCAO", "CALCULO_DA_ISENCAO"
            )
        else:
            logger.warning(
                "Calculo de isencao NAO sera gerado nesta execucao (UF/aliquota "
                "indisponivel). Reducao nao e afetada."
            )
            self.df_isencao = None

        return self

    def _enriquecer_uf_e_aliquota(self, df: DataFrame):
        """Deriva UF_ORIGEM_DESTINO (via C100/0000/0150) e junta a tabela de
        aliquotas interestaduais. Usada pelos DOIS ramos agora: isencao
        (a ALIQUOTA_TABELA entra no proprio CALCULO) e reducao (a
        ALIQUOTA_TABELA so aparece no relatorio como contexto/'Aliquota';
        o calculo de reducao continua usando ALIQ_ICMS do proprio EFD).

        Retorna (df, ok). Quando C100/0000/0150 nao estao disponiveis na
        Silver, retorna o df original + colunas de enriquecimento em
        branco e ok=False -- quem chama decide o que fazer (isencao e
        bloqueada por depender da ALIQUOTA_TABELA; reducao NAO e, so sai
        com UF/Municipio/Aliquota em branco no relatorio)."""
        # C190 vem do schema "largo" (uniao de todos os registros), entao ja
        # tem colunas proprias (sempre nulas) para campos de outros
        # registros — descarta antes de trazer as de verdade via join, pra
        # nao colidir/ambiguar.
        for col_do_c100 in ("COD_MOD", "UF_ORIGEM_DESTINO"):
            if col_do_c100 in df.columns:
                df = df.drop(col_do_c100)

        df_c100 = self.dfs.get("C100")
        df_0000 = self.dfs.get("0000")
        df_0150 = self.dfs.get("0150")

        if df_c100 is None or df_0000 is None or df_0150 is None:
            faltando = [n for n, d in (("C100", df_c100), ("0000", df_0000), ("0150", df_0150)) if d is None]
            logger.warning(
                f"Registros ausentes na Silver para derivar UF_ORIGEM_DESTINO: {faltando}."
            )
            for campo in ("COD_MOD", "UF_ORIGEM_DESTINO", "UF_ESTABELECIMENTO"):
                df = df.withColumn(campo, F.lit(None).cast("string"))
            df = df.withColumn("ALIQUOTA_TABELA", F.lit(None).cast("double"))
            return df, False

        df_municipios = self._obter_referencias()["municipios"]
        df_c100_com_uf = self._derivar_uf_origem_destino(df_c100, df_0000, df_0150, df_municipios)

        df = df.join(
            df_c100_com_uf.select(
                F.col("_row_id").alias("_c100_row_id"),
                F.col("COD_MOD"),
                F.col("UF_ORIGEM_DESTINO"),
                F.col("UF_ESTABELECIMENTO"),
            ),
            df["_parent_uid_final"] == F.col("_c100_row_id"),
            "left",
        ).drop("_c100_row_id")

        df = self._aplica_modelo_65(df)

        df_aliq = self._obter_referencias()["aliquotas"]
        df = self._juntar_aliquotas(df, df_aliq)

        return df, True

    def _derivar_uf_origem_destino(self, df_c100: DataFrame, df_0000: DataFrame,
                                    df_0150: DataFrame, df_municipios: DataFrame) -> DataFrame:
        """Monta UF_ORIGEM_DESTINO por nota fiscal (grao C100), a partir de:
            - UF do proprio estabelecimento (0000.UF, constante por arquivo)
            - UF do participante (0150.COD_MUN_PARTICIPANTE -> tabela de municipios)
            - direcao (C100.IND_OPER: 0=entrada, 1=saida)
        """
        uf_estabelecimento = (
            df_0000
            .select(F.col("_file_id"), F.col("UF").alias("UF_ESTABELECIMENTO"))
            .filter(F.col("UF_ESTABELECIMENTO").isNotNull())
            .dropDuplicates(["_file_id"])
        )

        # 0150 vem do schema "largo" e ja tem uma coluna UF propria (sempre
        # nula ali — 0150 nao carrega UF do participante diretamente, so
        # via COD_MUN_PARTICIPANTE). O Spark resolve nomes de coluna sem
        # diferenciar maiusculas/minusculas por padrao, entao essa UF
        # colidiria com a "uf" da tabela de municipios — descarta antes.
        df_0150_base = df_0150.drop("UF") if "UF" in df_0150.columns else df_0150

        # PROTECAO: confirmado em amostra real que o 0150 pode conter linhas
        # inteiramente duplicadas na Silver (mesmo _row_id repetido). Sem
        # deduplicar, o join abaixo causa fan-out — cada nota fiscal viraria
        # 2+ linhas no resultado (valor calculado continua certo por linha,
        # mas contagem/soma agregada dobra). dropDuplicates por chave de
        # negocio (nao pelo dataframe inteiro, que pode ter colunas tecnicas
        # variando) evita isso na origem.
        df_0150_base = df_0150_base.dropDuplicates(
            ["_file_id", "COD_PARTICIPANTE", "COD_MUN_PARTICIPANTE"]
        )

        df_0150_com_uf = (
            df_0150_base
            .join(
                df_municipios,
                df_0150_base["COD_MUN_PARTICIPANTE"].cast("string") == df_municipios["cod_mun"],
                "left",
            )
            .select(
                F.col("_file_id").alias("_0150_file_id"),
                F.col("COD_PARTICIPANTE").alias("_0150_cod_participante"),
                F.col("uf").alias("UF_PARTICIPANTE"),
            )
        )

        df_c100_enriquecido = (
            df_c100
            .join(uf_estabelecimento, on="_file_id", how="left")
            .join(
                df_0150_com_uf,
                (df_c100["COD_PART"] == df_0150_com_uf["_0150_cod_participante"])
                & (df_c100["_file_id"] == df_0150_com_uf["_0150_file_id"]),
                "left",
            )
            .drop("_0150_file_id", "_0150_cod_participante")
        )

        return df_c100_enriquecido.withColumn(
            "UF_ORIGEM_DESTINO",
            F.when(
                F.col("IND_OPER") == "1",  # saida: proprio -> participante
                F.concat_ws("/", F.col("UF_ESTABELECIMENTO"), F.col("UF_PARTICIPANTE")),
            ).when(
                F.col("IND_OPER") == "0",  # entrada: participante -> proprio
                F.concat_ws("/", F.col("UF_PARTICIPANTE"), F.col("UF_ESTABELECIMENTO")),
            ).otherwise(F.lit(None)),
        )

    @staticmethod
    def _aplica_modelo_65(df: DataFrame) -> DataFrame:
        """Secao 4.5.2 do mapeamento funcional — quando nao ha UF Origem/
        Destino (tipicamente Modelo 65 / NFC-e), usa a ALIQUOTA INTERNA do
        estado do proprio estabelecimento. Ou seja, UF_ORIGEM_DESTINO vira
        "<UF_ESTABELECIMENTO>/<UF_ESTABELECIMENTO>", que bate com a linha
        intraestadual (Origem==Destino) da tabela de aliquotas real — NAO
        um sentinela tipo 'XX/XX' que nao existe na tabela e resultaria em
        aliquota nula silenciosamente.

        Fica ativa tanto para Modelo 65 quanto para qualquer linha que
        genuinamente nao tenha UF_ORIGEM_DESTINO resolvido (mesma regra,
        aplicada por igual — a doc trata "sem UF origem/destino" e "modelo
        65" como o mesmo caso funcional)."""
        aliquota_interna = F.concat_ws("/", F.col("UF_ESTABELECIMENTO"), F.col("UF_ESTABELECIMENTO"))
        precisa_fallback = (
            (F.col("COD_MOD").cast("int") == 65) | F.col("UF_ORIGEM_DESTINO").isNull()
        )
        return df.withColumn(
            "UF_ORIGEM_DESTINO",
            F.when(precisa_fallback, aliquota_interna).otherwise(F.col("UF_ORIGEM_DESTINO")),
        )

    def _juntar_aliquotas(self, df: DataFrame, df_aliq: DataFrame) -> DataFrame:
        """Mesma logica do processador_spark.py original: monta a chave
        Origem/Destino na tabela de aliquotas e faz LEFT JOIN com
        UF_ORIGEM_DESTINO, via broadcast (tabela pequena)."""
        aliq = df_aliq.withColumn(
            "_CHAVE_UF_ORIGEM_DESTINO",
            F.concat_ws("/", F.col("Origem").cast("string"), F.col("Destino").cast("string")),
        ).withColumnRenamed("Aliquota", "ALIQUOTA_TABELA")

        return df.join(
            F.broadcast(aliq.select("_CHAVE_UF_ORIGEM_DESTINO", "ALIQUOTA_TABELA")),
            df["UF_ORIGEM_DESTINO"] == aliq["_CHAVE_UF_ORIGEM_DESTINO"],
            "left",
        ).drop("_CHAVE_UF_ORIGEM_DESTINO")

    # -------------------------------------------------------------------
    # ENRIQUECIMENTO DE DOCUMENTO/PARTICIPANTE/MUNICIPIO + RELATORIO FINAL
    # -------------------------------------------------------------------
    # C190 (schema largo) so tem os campos DO PROPRIO C190 preenchidos —
    # campos de nivel de DOCUMENTO (C100: Numero Documento, Chave NF-e,
    # Indicador Frete/Pagamento/Emitente, valores do cabecalho da nota...)
    # e de PARTICIPANTE (0150: Nome/CNPJ/CPF/IE do participante, municipio)
    # ficam sempre nulos na linha do C190 se nao forem trazidos via join —
    # mesmo _parent_uid_final == C100._row_id ja usado para derivar UF.

    def _enriquecer_documento_participante_municipio(self, df: DataFrame, df_municipios: DataFrame) -> DataFrame:
        df_c100 = self.dfs.get("C100")
        df_0150 = self.dfs.get("0150")

        campos_c100 = [
            "COD_MOD", "COD_SIT", "SER", "NUM_DOC", "CHV_NFE", "DT_DOC", "DT_E_S",
            "VL_DOC", "IND_PGTO", "VL_DESC", "VL_ABAT_NT", "VL_MERC", "IND_FRT",
            "VL_FRT", "VL_SEG", "VL_OUT_DA", "VL_BC_ICMS", "VL_ICMS", "VL_BC_ICMS_ST",
            "VL_ICMS_ST", "VL_IPI", "VL_PIS", "VL_COFINS", "VL_PIS_ST", "VL_COFINS_ST",
            "IND_OPER", "IND_EMIT", "COD_PART",
        ]

        if df_c100 is None:
            logger.warning(
                "REG C100 ausente — campos de documento (Numero Documento, Chave "
                "NF-e, Indicador Frete/Pagamento/Emitente etc.) ficarao em branco no relatorio."
            )
            for campo in campos_c100:
                df = df.withColumn(f"{campo}_C100", F.lit(None))
        else:
            df_c100_sel = df_c100.select(
                F.col("_row_id").alias("_c100_row_id_doc"),
                *[F.col(c).alias(f"{c}_C100") for c in campos_c100],
            )
            df = df.join(
                df_c100_sel, df["_parent_uid_final"] == F.col("_c100_row_id_doc"), "left"
            ).drop("_c100_row_id_doc")

        campos_participante = ["NOME_PARTICIPANTE", "CNPJ_PARTICIPANTE", "CPF_PARTICIPANTE",
                                "IE_PARTICIPANTE", "COD_MUN_PARTICIPANTE"]

        # Essas colunas ja existem no schema largo do C190 (sempre nulas
        # nessa linha, sao campos proprios do 0150) -- descarta antes de
        # trazer as de verdade via join, senao o join duplica o nome e
        # gera AMBIGUOUS_REFERENCE no select final.
        for campo in campos_participante:
            if campo in df.columns:
                df = df.drop(campo)

        if df_0150 is None or df_c100 is None:
            logger.warning("REG 0150 ausente (ou C100 ausente) — dados do Participante ficarao em branco.")
            for campo in campos_participante:
                df = df.withColumn(campo, F.lit(None))
            return df.withColumn("MUNICIPIO_PARTICIPANTE", F.lit(None))

        df_0150_sel = (
            df_0150.dropDuplicates(["_file_id", "COD_PARTICIPANTE"])
            .select(F.col("_file_id").alias("_0150_file_id_p"),
                    F.col("COD_PARTICIPANTE").alias("_0150_cod_part_p"),
                    *campos_participante)
        )
        df = df.join(
            df_0150_sel,
            (df["COD_PART_C100"] == df_0150_sel["_0150_cod_part_p"])
            & (df["_file_id"] == df_0150_sel["_0150_file_id_p"]),
            "left",
        ).drop("_0150_file_id_p", "_0150_cod_part_p")

        # Nome do municipio: a tabela de referencia (TABELA_MUNICIPIO/
        # municipios_brasil.xlsx) so tem "cod_mun"/"uf" (confirmado) -- nao
        # existe coluna de nome, entao 'Município' fica sempre em branco.
        # 'Código Município' (COD_MUN_PARTICIPANTE) continua preenchido.
        df = df.withColumn("MUNICIPIO_PARTICIPANTE", F.lit(None))

        return df

    # Rotulos "codigo - descricao" dos campos indicadores do SPED, usados
    # no relatorio final (confirmados contra os valores reais observados
    # no Excel de referencia "3_2_Operacoes_com_Reducao.xlsx").
    _ROTULO_TIPO_OPERACAO = {"0": "0 - Entrada", "1": "1 - Saida"}
    _ROTULO_IND_EMIT = {"0": "0 - Emissão própria", "1": "1 - Terceiros"}
    _ROTULO_IND_PGTO = {"0": "0 - À vista", "1": "1 - A prazo", "2": "2 - Outros"}
    _ROTULO_IND_FRT = {
        "0": "0 - Contratação do frete por conta do remetente(CIF)",
        "1": "1 - Contratação do frete por conta do destinatário(FOB)",
        "2": "2 - Contratação do frete por conta de terceiros",
        "3": "3 - Transporte próprio por conta do remetente",
        "4": "4 - Transporte próprio por conta do destinatário",
        "9": "9 - Sem ocorrência de transporte",
    }

    @staticmethod
    def _rotular(col, mapa: dict):
        """CASE WHEN cod -> 'cod - descricao'. Codigo fora do dict (ou nulo)
        vira NULL — nao inventa rotulo para codigo desconhecido."""
        expr = F.lit(None).cast("string")
        for codigo, rotulo in mapa.items():
            expr = F.when(col.cast("string") == F.lit(codigo), F.lit(rotulo)).otherwise(expr)
        return expr

    def _dedup_fan_out(self, df: DataFrame, contexto: str) -> DataFrame:
        """Dedup TECNICO pos-enriquecimento: (_file_id, _row_id) identificam
        de forma unica a linha original do C190 (uma combinacao CST/CFOP/
        aliquota dentro de um documento fiscal). Depois dos joins com C100/
        0150/aliquotas, cada linha original DEVE continuar aparecendo uma
        unica vez -- se aparecer mais de uma, algum dos joins tem fan-out
        (ex.: 0150 com mais de uma linha pro mesmo participante/arquivo que
        escapou do dropDuplicates, ou aliquotas com mais de uma linha pra
        mesma chave Origem/Destino).

        NAO cobre duplicidade de NEGOCIO (ex.: o mesmo documento fiscal
        chegando por dois _file_id diferentes, tipo arquivo reenviado ou
        retificadora) -- para isso a chave precisa ser outra (ver comentario
        em build_subvencao)."""
        chave = ["_file_id", "_row_id"]
        antes = df.count()
        df_dedup = df.dropDuplicates(chave)
        depois = df_dedup.count()

        if depois < antes:
            logger.warning(
                f"[Subvencao] Fan-out detectado no enriquecimento ({contexto}): "
                f"{antes} linhas -> {depois} apos dropDuplicates({chave}). "
                f"{antes - depois} linha(s) duplicada(s) removida(s) -- "
                f"provavelmente um dos joins (C100/0150/aliquotas) casou com "
                f"mais de uma linha do lado direito. Vale investigar a origem "
                f"antes de confiar nos totais do relatorio."
            )
        return df_dedup

    def _montar_relatorio_final(self, df: DataFrame, coluna_calculo: str, alias_calculo: str) -> DataFrame:
        """Seleciona/renomeia para o layout de negocio (mesmas colunas dos
        Excel "3_1 Operacoes com Isencao"/"3_2 Operacoes com Reducao").

        Colunas SEM fonte de dados nos registros hoje disponiveis (C100/
        C190/0000/0150 + tabelas CFOP/municipios/aliquotas) ficam em
        branco (None) por decisao explicita, ate que exista uma fonte:
            - Opção Simples / Data Opção Simples / Data Exclusão Simples
              (cadastro de regime tributario/Simples Nacional — nao existe
              nos registros SPED nem nas tabelas de referencia atuais)
            - Código Observação / Descrição Observação Lançamento Fiscal
              (viria do registro 0460 do SPED, nao carregado hoje)

        Chamar DEPOIS de: _deriva_sinal_cfop, _enriquecer_uf_e_aliquota e
        _enriquecer_documento_participante_municipio.
        """
        cfop_faturamento = (
            F.when(F.col("_NATUREZA") == "Venda", F.lit("Faturamento"))
             .when(F.col("_NATUREZA") == "Devolução de venda", F.lit("Devolução Faturamento"))
             .otherwise(F.lit(None).cast("string"))
        )

        return df.select(
            F.col("CNPJ").alias("CNPJ"),
            F.col("IE").alias("INSCRICAO_ESTADUAL"),
            F.to_date(F.col("DT_INI"), "ddMMyyyy").alias("PERIODO"),
            self._rotular(F.col("IND_OPER_C100"), self._ROTULO_TIPO_OPERACAO).alias("TIPO_OPERACAO"),
            self._rotular(F.col("IND_EMIT_C100"), self._ROTULO_IND_EMIT).alias("INDICADOR_EMITENTE"),
            F.col("COD_PART_C100").alias("CODIGO_PARTICIPANTE"),
            F.col("CNPJ_PARTICIPANTE").alias("CNPJ_PARTICIPANTE"),
            F.col("CPF_PARTICIPANTE").alias("CPF_PARTICIPANTE"),
            F.col("NOME_PARTICIPANTE").alias("NOME_PARTICIPANTE"),
            F.col("IE_PARTICIPANTE").alias("IE_PARTICIPANTE"),
            F.lit(None).cast("string").alias("OPCAO_SIMPLES"),
            F.lit(None).cast("date").alias("DATA_OPCAO_SIMPLES"),
            F.lit(None).cast("date").alias("DATA_EXCLUSAO_SIMPLES"),
            F.col("UF_ORIGEM_DESTINO").alias("UF_ORIGEM_DESTINO"),
            F.col("COD_MUN_PARTICIPANTE").alias("CODIGO_MUNICIPIO"),
            F.col("MUNICIPIO_PARTICIPANTE").alias("MUNICIPIO"),
            F.col("COD_MOD_C100").alias("MODELO"),
            F.col("COD_SIT_C100").alias("SITUACAO"),
            F.col("SER_C100").alias("SERIE"),
            F.col("NUM_DOC_C100").alias("NUMERO_DOCUMENTO"),
            F.col("CHV_NFE_C100").alias("CHAVE_NFE"),
            F.to_date(F.col("DT_DOC_C100"), "ddMMyyyy").alias("DATA_DOCUMENTO"),
            F.to_date(F.col("DT_E_S_C100"), "ddMMyyyy").alias("DATA_ENTRADA_SAIDA"),
            F.col("VL_DOC_C100").alias("VALOR_DOCUMENTO"),
            self._rotular(F.col("IND_PGTO_C100"), self._ROTULO_IND_PGTO).alias("INDICADOR_PAGAMENTO"),
            F.col("VL_DESC_C100").alias("VLR_DESCONTO_NF"),
            F.col("VL_ABAT_NT_C100").alias("VLR_ABATIMENTO_NT"),
            F.col("VL_MERC_C100").alias("VLR_MERCADORIA"),
            self._rotular(F.col("IND_FRT_C100"), self._ROTULO_IND_FRT).alias("INDICADOR_FRETE"),
            F.col("VL_FRT_C100").alias("VLR_FRETE"),
            F.col("VL_SEG_C100").alias("VLR_SEGURO"),
            F.col("VL_OUT_DA_C100").alias("VLR_OUTRAS_DA"),
            F.col("VL_BC_ICMS_C100").alias("VLR_BASE_CALCULO_ICMS_C100"),
            F.col("VL_ICMS_C100").alias("VLR_ICMS_C100"),
            F.col("VL_BC_ICMS_ST_C100").alias("VLR_BASE_CALCULO_ICMS_ST_C100"),
            F.col("VL_ICMS_ST_C100").alias("VLR_ICMS_ST_C100"),
            F.col("VL_IPI_C100").alias("VLR_IPI_C100"),
            F.col("VL_PIS_C100").alias("VLR_PIS_C100"),
            F.col("VL_COFINS_C100").alias("VLR_COFINS_C100"),
            F.col("VL_PIS_ST_C100").alias("VLR_PIS_ST_C100"),
            F.col("VL_COFINS_ST_C100").alias("VLR_COFINS_ST_C100"),
            F.col("CST_ICMS").alias("CST_ICMS"),
            F.col("CFOP").alias("CFOP"),
            F.col("_DESCRICAO").alias("DESCRICAO_CFOP"),
            cfop_faturamento.alias("CFOP_FATURAMENTO"),
            F.col("ALIQ_ICMS").alias("ALIQUOTA_ICMS"),
            F.col("VL_OPR").alias("VLR_OPERACAO"),
            F.col("VL_BC_ICMS").alias("VLR_BASE_CALCULO_ICMS"),
            F.col("VL_ICMS").alias("VLR_ICMS"),
            F.col("VL_BC_ICMS_ST").alias("VLR_BASE_CALCULO_ICMS_ST"),
            F.col("VL_ICMS_ST").alias("VLR_ICMS_ST"),
            F.col("VL_RED_BC").alias("VLR_REDUCAO_BASE_ICMS"),
            F.col("VL_IPI").alias("VLR_IPI"),
            F.lit(None).cast("string").alias("CODIGO_OBSERVACAO"),
            F.lit(None).cast("string").alias("DESCRICAO_OBSERVACAO_LANCAMENTO_FISCAL"),
            F.year(F.to_date(F.col("DT_DOC_C100"), "ddMMyyyy")).alias("ANO"),
            F.split(F.col("UF_ORIGEM_DESTINO"), "/").getItem(0).alias("ORIGEM"),
            F.split(F.col("UF_ORIGEM_DESTINO"), "/").getItem(1).alias("DESTINO"),
            F.col("ALIQUOTA_TABELA").alias("ALIQUOTA"),
            F.col("UF_ORIGEM_DESTINO").alias("CHAVE_OPERACAO_INTERESTADUAL"),
            F.col(coluna_calculo).alias(alias_calculo),
        )

    def _deriva_sinal_cfop(self, df: DataFrame) -> DataFrame:
        """Deriva o sinal (+1/-1) a partir de 'natureza' na tabela CFOP de
        Subvencao: 'Venda' -> +1, 'Devolução de venda' -> -1 — CONFIRMADO
        com amostra real dos CFOPs 5101/1201 (mesmas strings exatas da
        TABELA_CFOP generica anterior).

        Para CFOPs da lista autoritativa sem correspondencia na tabela, ou
        com 'natureza' fora dessas duas strings (caso inesperado — logado
        como aviso), ou se cfop_key nao estiver configurada, cai no
        fallback por primeiro digito do CFOP (1/2/3=entrada/negativo,
        5/6/7=saida/positivo).
        """
        df = df.withColumn("_CFOP_STR", F.lpad(F.col("CFOP").cast("string"), 4, "0"))
        sinal_fallback = self._sinal_por_cfop(F.col("CFOP"))

        if not self.cfop_key:
            logger.warning(
                "cfop_key nao configurada — sinal 100%% via heuristica por "
                "primeiro digito do CFOP, nao via natureza da tabela real. "
                "'Descrição CFOP'/'CFOP Faturamento' do relatorio final ficarao em branco."
            )
            return (
                df.withColumn("_SINAL_CFOP", sinal_fallback)
                  .withColumn("_DESCRICAO", F.lit(None).cast("string"))
                  .withColumn("_NATUREZA", F.lit(None).cast("string"))
                  .drop("_CFOP_STR")
            )

        df_cfop = self._obter_referencias()["cfop"]
        df = df.join(F.broadcast(df_cfop), df["_CFOP_STR"] == df_cfop["_CFOP_TABELA"], "left")

        cfops_sem_match = (
            df.filter(F.col("_CFOP_TABELA").isNull())
            .select("CFOP").distinct().rdd.flatMap(lambda r: r).collect()
        )
        if cfops_sem_match:
            logger.warning(
                f"CFOPs da lista autoritativa sem correspondencia na tabela CFOP "
                f"de Subvencao (sinal via fallback por digito): {sorted(set(cfops_sem_match))}"
            )

        cfops_natureza_inesperada = (
            df.filter(
                F.col("_CFOP_TABELA").isNotNull()
                & ~F.col("_NATUREZA").isin("Venda", "Devolução de venda")
            )
            .select("CFOP", "_NATUREZA").distinct().collect()
        )
        if cfops_natureza_inesperada:
            logger.warning(
                "CFOPs da lista autoritativa com 'natureza' fora de "
                "'Venda'/'Devolução de venda' (sinal via fallback por digito): "
                + ", ".join(f"CFOP={r['CFOP']} natureza={r['_NATUREZA']!r}" for r in cfops_natureza_inesperada)
            )

        sinal_por_natureza = (
            F.when(F.col("_NATUREZA") == "Venda", F.lit(1))
             .when(F.col("_NATUREZA") == "Devolução de venda", F.lit(-1))
             .otherwise(F.lit(None))
        )
        df = df.withColumn("_SINAL_CFOP", F.coalesce(sinal_por_natureza, sinal_fallback))
        # _DESCRICAO/_NATUREZA sao MANTIDAS de proposito (antes eram
        # descartadas aqui) -- o relatorio final usa _DESCRICAO como
        # "Descrição CFOP" e deriva "CFOP Faturamento" de _NATUREZA.
        return df.drop("_CFOP_STR", "_CFOP_TABELA")

    def _checar_colunas_obrigatorias(self, df_c190) -> None:
        obrigatorias = {"CST_ICMS", "CFOP", "VL_OPR", "VL_RED_BC", "ALIQ_ICMS"}
        faltando = obrigatorias - set(df_c190.columns)
        if faltando:
            raise RuntimeError(
                f"REG C190 na Silver nao contem as colunas obrigatorias: {faltando}"
            )

    def _normalizar_campos(self, df, client_cnpj: str, month_year: str):
        df = (
            df
            .withColumn("CNPJ", F.lit(client_cnpj))
            .withColumn("MMAAAA", F.lit(month_year))
            .withColumn("CFOP", F.col("CFOP").cast("int"))
        )
        for campo in self.CAMPOS_NUMERICOS_BR:
            df = df.withColumn(campo, self._br_para_double(F.col(campo)))
        return df

    @staticmethod
    def _br_para_double(col):
        """Converte string numerica no formato BR ('15.647,02' ou '15647,02')
        para double: remove separador de milhar '.', troca ',' decimal por '.'."""
        sem_milhar = F.regexp_replace(col.cast("string"), r"\.", "")
        com_ponto = F.regexp_replace(sem_milhar, ",", ".")
        return com_ponto.cast("double")

    @staticmethod
    def _sinal_por_cfop(cfop_col):
        """Convencao fiscal: CFOP 1xxx/2xxx/3xxx = entrada (sinal negativo);
        5xxx/6xxx/7xxx = saida (sinal positivo)."""
        primeiro_digito = F.floor(cfop_col / F.lit(1000))
        return F.when(primeiro_digito.isin(1, 2, 3), F.lit(-1)).otherwise(F.lit(1))

    # -------------------------------------------------------------------
    # WRITE — reaproveita write_gold() (Delta + mergeSchema) da classe-mae
    # -------------------------------------------------------------------
    def write_gold_subvencao(self, output_base_path: str) -> dict:
        """Grava isencao e reducao como duas tabelas Delta separadas.
        Reaproveita write_gold() e sanity_checks() ja existentes na
        classe-mae — nao reimplementa escrita.

        IMPORTANTE: 0 linhas apos os filtros de CST/CFOP/UF e um resultado
        VALIDO (esse CNPJ/periodo pode genuinamente nao ter nenhuma operacao
        de isencao ou de reducao elegivel) — nao e um erro. sanity_checks()
        (herdado do builder generico) trata qualquer resultado vazio como
        RuntimeError fatal, o que derrubaria o job inteiro mesmo quando SO
        um dos dois nucleos (isencao/reducao) estivesse vazio. Por isso
        checamos a contagem ANTES de chamar sanity_checks(), e pulamos com
        um log informativo em vez de deixar o builder generico explodir.
        """
        resultados = {}
        for nome, df in (("isencao", self.df_isencao), ("reducao", self.df_reducao)):
            if df is None:
                logger.warning(f"Subvencao/{nome}: nada a gravar (DataFrame ausente — ver avisos anteriores).")
                continue

            if df.limit(1).count() == 0:
                logger.info(
                    f"Subvencao/{nome}: 0 linhas apos os filtros de CST/CFOP/UF "
                    f"para este CNPJ/periodo — nada a gravar. Pode ser esperado "
                    f"(sem oportunidade identificada neste periodo), nao "
                    f"necessariamente um erro."
                )
                continue

            self.df_gold = df
            self.sanity_checks()  # ja herdado de BaseGoldBuilder/GoldStreamingBuilder
            # DIAGNOSTICO TEMPORARIO -- remover depois de descobrir a causa.
            proibidos = set(" ,;{}()\n\t=")
            colunas_com_problema = [c for c in self.df_gold.columns if proibidos & set(c)]
            if colunas_com_problema:
                logger.error(
                    f"Subvencao/{nome}: colunas com caractere proibido para o Delta "
                    f"(apos sanity_checks): {colunas_com_problema}"
                )
            else:
                logger.info(f"Subvencao/{nome}: nenhuma coluna problematica encontrada apos sanity_checks.")

            path = f"{output_base_path}/{nome}"
            # self._garantir_column_mapping(path)
            self.write_gold(path)
            self._vacuum_gold(path) # realiza limpeza de arquivos delta de overwrite anteriores
            resultados[nome] = path

        return resultados

    # def _garantir_column_mapping(self, path: str) -> None:
    #     """O layout final ('UF Origem/Destino', 'Vlr Base Cálculo ICMS - C100'
    #     etc.) tem espaco e caracteres especiais nos nomes de coluna, que o
    #     Delta so aceita com Column Mapping habilitado (mode='name') —
    #     sem isso o write_gold() falha com AnalysisException ("Found
    #     invalid character(s)...").

    #     Nao mexe em write_gold() (da classe-mae, fora deste modulo):
    #     - Tabela NOVA (path ainda nao existe): seta a config de sessao que
    #       faz TODA tabela Delta criada a partir de agora nascer com column
    #       mapping habilitado.
    #     - Tabela JA EXISTENTE no path (de uma execucao anterior, sem
    #       column mapping): faz o ALTER TABLE necessario nela antes de
    #       escrever de novo — senao a config de sessao nao adianta (so
    #       vale pra tabela nova).
    #     """
    #     self.spark.conf.set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
    #     self.spark.conf.set("spark.databricks.delta.properties.defaults.minReaderVersion", "2")
    #     self.spark.conf.set("spark.databricks.delta.properties.defaults.minWriterVersion", "5")

    #     try:
    #         from delta.tables import DeltaTable
    #         if DeltaTable.isDeltaTable(self.spark, path):
    #             logger.info(f"Tabela Delta ja existe em {path} — habilitando column mapping via ALTER TABLE.")
    #             self.spark.sql(f"""
    #                 ALTER TABLE delta.`{path}` SET TBLPROPERTIES (
    #                     'delta.columnMapping.mode' = 'name',
    #                     'delta.minReaderVersion' = '2',
    #                     'delta.minWriterVersion' = '5'
    #                 )
    #             """)
    #     except Exception as e:
    #         logger.warning(
    #             f"Nao foi possivel checar/alterar column mapping em {path} "
    #             f"({e}). Se a tabela ja existia de uma execucao anterior sem "
    #             f"column mapping, o write_gold() pode falhar de novo — nesse "
    #             f"caso rode manualmente o ALTER TABLE (ver docstring) ou "
    #             f"apague o path e deixe recriar do zero."
    #         )