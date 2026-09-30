# src/dinamo_web/apps/subvencao_app.py
"""
Entry-point batch do relatorio de Subvencao (diagnostico ICMS - isencao/
reducao em operacoes diretas).

Mesmo padrao de wiring de apps/gold_app.py e apps/bronze_app.py:
ConfigManager -> main_config -> create_spark_session -> builder especifico.
Diferenca: este e um job batch (roda sob demanda por CNPJ/periodo), nao um
streaming job Kafka — porque o relatorio de Subvencao e um diagnostico
disparado explicitamente, nao um evento continuo.

IMPORTANTE — convencao de path confirmada com amostras reais da Silver:
    s3://<bucket>/data-lake/silver/<CNPJ_8_DIGITOS>/EFD_FISCAL/<periodo>/REG=<reg>
Repare no CNPJ_8_DIGITOS: e a raiz do CNPJ (sem filial/DV), mesma convencao
ja usada em streaming/gold_stream.py (variavel cnpj_8_digits). --cnpj aceita
tanto a raiz quanto o CNPJ completo (normaliza automaticamente, com aviso).

IMPORTANTE — formato do periodo: confirmado contra o bucket real que e
AAAAMM (ano primeiro, ex.: "202106" = junho/2021, "202509" = setembro/2025)
— NAO "MMAAAA" como um comentario antigo deste arquivo sugeria por engano.

REGRA DE NEGOCIO (secao do mapeamento funcional): Subvencao sobre Operacoes
so e valida ATE 2023 — mudanca legislativa invalida o calculo a partir de
2024. Por isso --todos-periodos filtra automaticamente por ano_limite
(config em subvencao_config.yaml), nao processa tudo que existir as cegas.

Uso:
    # Um periodo especifico
    python subvencao_app.py --cnpj 01063615 --periodo 202106

    # TODOS os periodos disponiveis no bucket para o CNPJ, ate o ano_limite
    # configurado (default 2023) — descobre sozinho via listagem do bucket
    python subvencao_app.py --cnpj 01063615 --todos-periodos

    python subvencao_app.py --cnpj 01063615 --cnpj 10173887 --periodo 092022
"""
import argparse
import logging
import re
import sys

ROOT_DIR = "/opt/spark/app"
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from spark.session import create_spark_session
from config.config_manager import ConfigManager
from connectors.bucket_connector import BucketConnector
from src.streaming.stream_config import StreamConfig
from modules.subvencao.gold_layer.subvencao_gold_builder import (
    SubvencaoGoldBuilder,
    carregar_referencias_subvencao,
    escanear_periodo_tem_candidatos,
)
from observability.structured_logger import configure_structured_logging

logger = logging.getLogger("DINAMO_SUBVENCAO")

_PERIODO_REGEX = re.compile(r"/EFD_FISCAL/(\d{6})/")


def normalizar_cnpj_raiz(cnpj: str) -> str:
    """Garante CNPJ raiz de 8 digitos (sem filial/DV), que e o formato usado
    no path da Silver (data-lake/silver/<cnpj8>/...). Aceita tambem o CNPJ
    completo de 14 digitos por conveniencia, truncando com aviso — evita
    que o job rode "com sucesso" sem achar nenhum dado por um path errado."""
    digitos = "".join(c for c in cnpj if c.isdigit())
    if len(digitos) == 8:
        return digitos
    if len(digitos) == 14:
        raiz = digitos[:8]
        logger.warning(f"CNPJ {cnpj} tem 14 digitos; usando raiz {raiz} (path da Silver usa 8 digitos).")
        return raiz
    raise ValueError(f"CNPJ '{cnpj}' invalido: esperado 8 (raiz) ou 14 digitos, recebeu {len(digitos)}.")


def descobrir_periodos_disponiveis(bucket_connector: BucketConnector, cnpj: str,
                                    ano_limite: int) -> list[str]:
    """Lista o bucket sob data-lake/silver/<cnpj>/EFD_FISCAL/ e extrai os
    periodos (AAAAMM) que realmente existem, filtrando por ano_limite —
    regra do mapeamento funcional: Subvencao sobre Operacoes so vale ate
    2023 (mudanca legislativa invalida o calculo a partir de 2024).

    Nao assume nada sobre quais periodos existem — descobre pelo conteudo
    real do bucket, pra nao processar (nem deixar de processar) por engano.
    """
    prefixo = f"data-lake/silver/{cnpj}/EFD_FISCAL/"
    chaves = bucket_connector.list_paths(prefixo)

    periodos_encontrados = set()
    for chave in chaves:
        m = _PERIODO_REGEX.search("/" + chave if not chave.startswith("/") else chave)
        if m:
            periodos_encontrados.add(m.group(1))

    if not periodos_encontrados:
        logger.warning(f"Nenhum periodo encontrado em s3://.../{prefixo} para CNPJ={cnpj}")
        return []

    periodos_elegiveis = sorted(p for p in periodos_encontrados if int(p[:4]) <= ano_limite)
    periodos_descartados = sorted(periodos_encontrados - set(periodos_elegiveis))

    logger.info(
        f"[Subvencao] CNPJ={cnpj}: {len(periodos_encontrados)} periodo(s) no bucket, "
        f"{len(periodos_elegiveis)} elegivel(is) (ate {ano_limite}): {periodos_elegiveis}"
    )
    if periodos_descartados:
        logger.info(
            f"[Subvencao] CNPJ={cnpj}: {len(periodos_descartados)} periodo(s) descartado(s) "
            f"por serem posteriores a {ano_limite} (regra legislativa): {periodos_descartados}"
        )

    return periodos_elegiveis


class SubvencaoApplication:

    def __init__(self):
        configure_structured_logging(level="INFO", service_name="dinamo-subvencao")

        self.config_manager = ConfigManager()
        self.main_config = self.config_manager.main_config
        self.subvencao_config = self.config_manager.subvencao_config

        self.spark = create_spark_session("DINAMO_SUBVENCAO", self.main_config)

        self.bucket_name = self.main_config["s3"]["bucket_name"]
        # Paths vem de StreamConfig (SILVER_PATH/GOLD_PATH), MESMA fonte que
        # bronze/silver/gold_app.py ja usam via sparkConf.spark.kubernetes.
        # driverEnv.* nos manifestos reais — confirmado: data-lake/silver e
        # data-lake/gold, minusculo. NAO reconstruir path com prefixo
        # diferente (ver bug corrigido: usava "GOLD/" maiusculo antes).
        stream_config = StreamConfig()
        self.silver_base_path = stream_config.silver_path
        self.gold_base_path = stream_config.gold_path
        self.base_path = stream_config.base_path

        # ano_limite: regra do mapeamento funcional — Subvencao sobre
        # Operacoes so vale ate 2023. Configuravel (default 2023 se ausente
        # do yaml), nao hardcoded direto no fluxo.
        self.ano_limite = self.subvencao_config.get("ano_limite_subvencao", 2023)
        self.bucket_connector = BucketConnector(self.spark, self.bucket_name)

        # Referencias (CFOP/municipios/aliquotas) carregadas UMA VEZ AQUI —
        # nao dentro do loop de periodos. Antes disso, cada periodo de
        # --todos-periodos baixava as 3 tabelas de novo (48 periodos = 48x
        # o mesmo download), lendo .xlsx via pandas no driver — lento e
        # arriscado (OOM). Agora: parquet nativo do Spark, uma vez so,
        # reaproveitado por todo CNPJ/periodo processado nesta execucao.
        referencias_cfg = self.subvencao_config.get("referencias", {})
        self.referencias = carregar_referencias_subvencao(
            self.spark, self.bucket_name,
            cfop_key=referencias_cfg["cfop_key"],
            municipios_key=referencias_cfg["municipios_key"],
            aliquotas_key=referencias_cfg["aliquotas_interestadual_key"],
        )

    def run(self, cnpjs: list[str], periodo: str = None, todos_periodos: bool = False):
        filtros = self.subvencao_config["filters"]

        resultados = {}
        for cnpj_bruto in cnpjs:
            cnpj = normalizar_cnpj_raiz(cnpj_bruto)

            if todos_periodos:
                periodos = descobrir_periodos_disponiveis(self.bucket_connector, cnpj, self.ano_limite)
                if not periodos:
                    logger.warning(f"[Subvencao] CNPJ={cnpj}: nenhum periodo elegivel encontrado, pulando.")
                    resultados[cnpj] = {}
                    continue
            else:
                periodos = [periodo]

            resultados[cnpj] = {}
            csts_validos_todos = (
                filtros["csts"]["isencao"] + filtros["csts"]["reducao"] + filtros["csts"]["condicionais_51"]
            )
            for periodo_atual in periodos:
                # Scan LEVE primeiro (so C190, sem C100/0000/0150/joins) —
                # decide se vale pagar o custo caro do pipeline completo
                # neste periodo. Evita gastar tempo/recurso em periodos sem
                # nenhum CST/CFOP elegivel (comum: boa parte dos periodos
                # de um CNPJ pode nao ter nenhuma operacao de subvencao).
                tem_candidatos = escanear_periodo_tem_candidatos(
                    self.spark, self.silver_base_path, cnpj, periodo_atual,
                    csts_validos=csts_validos_todos,
                    cfops_validos=filtros["cfops"]["validos"],
                )
                if not tem_candidatos:
                    logger.info(
                        f"[Subvencao] CNPJ={cnpj} periodo={periodo_atual}: scan leve nao achou "
                        f"CST/CFOP elegivel. Pulando pipeline completo (sem C100/0000/0150/joins)."
                    )
                    resultados[cnpj][periodo_atual] = {}
                    continue

                logger.info(f"[Subvencao] Iniciando CNPJ={cnpj} periodo={periodo_atual}")

                builder = SubvencaoGoldBuilder(
                    spark=self.spark,
                    base_path=self.silver_base_path,
                    period=periodo_atual,
                    csts_isencao=filtros["csts"]["isencao"],
                    csts_reducao=filtros["csts"]["reducao"],
                    csts_condicionais_51=filtros["csts"]["condicionais_51"],
                    cfops_validos=filtros["cfops"]["validos"],
                    bucket_name=self.bucket_name,
                    aliquotas_key=None,
                    municipios_key=None,
                    cfop_key=None,
                    referencias=self.referencias,  # <-- reaproveitado, nao recarrega
                )
                builder.build_subvencao(client_cnpj=cnpj, month_year=periodo_atual)

                # output_base = f"{self.base_path}/SUBVENCAO/{cnpj}/{periodo_atual}"
                output_base = f"s3a://{self.bucket_name}/data-lake/subvencao/{cnpj}/{periodo_atual}"
                gravados = builder.write_gold_subvencao(output_base)

                logger.info(f"[Subvencao] CNPJ={cnpj} periodo={periodo_atual} concluido: {gravados}")
                resultados[cnpj][periodo_atual] = gravados

        return resultados

    def shutdown(self):
        self.spark.stop()


def main() -> int:
    ap = argparse.ArgumentParser(description="Relatorio de Subvencao (Gold, batch).")
    ap.add_argument("--cnpj", type=str, action="append", required=True,
                     help="CNPJ(s) a processar (raiz de 8 digitos ou completo de 14 — normalizado automaticamente). Pode repetir a flag.")
    ap.add_argument("--periodo", type=str, default=None,
                     help="Periodo unico no formato AAAAMM (ano+mes, ex.: 202106). Obrigatorio a menos que --todos-periodos seja usado.")
    ap.add_argument("--todos-periodos", action="store_true",
                     help="Descobre automaticamente todos os periodos disponiveis no bucket para o CNPJ "
                          "e processa cada um, filtrando pelo ano_limite_subvencao configurado (default 2023).")
    args = ap.parse_args()

    if not args.todos_periodos and not args.periodo:
        ap.error("informe --periodo OU --todos-periodos.")

    app = SubvencaoApplication()
    try:
        app.run(cnpjs=args.cnpj, periodo=args.periodo, todos_periodos=args.todos_periodos)
    finally:
        app.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
