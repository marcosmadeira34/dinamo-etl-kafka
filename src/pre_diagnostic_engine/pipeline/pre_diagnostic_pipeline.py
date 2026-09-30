# src/pre_diagnostic_engine/pipeline/pre_diagnostic_pipeline.py
"""
Pipeline principal do Pre-Diagnostic Engine.

Orquestra na ordem:
  1. Extração  (EcfExtractor, DarfExtractor, RetentionExtractor, CompanyExtractor)
  2. Análise   (RegimeAnalyzer, ContabilAnalyzer, FiscalAnalyzer,
                PatrimonioAnalyzer, DarfAnalyzer, RetentionAnalyzer,
                OpportunityL300Analyzer, OpportunityM310Analyzer)
  3. Validação (validators)
  4. Consolidação (FiscalConsolidator, FinancialConsolidator,
                   OpportunityConsolidator, FinalConsolidator)
  5. Scoring   (GoNoGoScorer, OpportunityScorer, ViabilityScorer, ViabilityAnalyzer)
  6. Persistência (GoldRepository)

Nenhum collect() ou loop Python sobre volume é usado neste pipeline.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pre_diagnostic_engine.extractors.ecf_extractor        import EcfExtractor
from pre_diagnostic_engine.extractors.darf_extractor       import DarfExtractor
from pre_diagnostic_engine.extractors.retention_extractor  import RetentionExtractor
from pre_diagnostic_engine.extractors.company_extractor    import CompanyExtractor

from pre_diagnostic_engine.analyzers.regime_analyzer           import RegimeAnalyzer
from pre_diagnostic_engine.analyzers.contabil_analyzer         import ContabilAnalyzer
from pre_diagnostic_engine.analyzers.fiscal_analyzer           import FiscalAnalyzer
from pre_diagnostic_engine.analyzers.patrimonio_analyzer       import PatrimonioAnalyzer
from pre_diagnostic_engine.analyzers.darf_analyzer             import DarfAnalyzer
from pre_diagnostic_engine.analyzers.retention_analyzer        import RetentionAnalyzer
from pre_diagnostic_engine.analyzers.opportunity_l300_analyzer import OpportunityL300Analyzer
from pre_diagnostic_engine.analyzers.opportunity_m310_analyzer import OpportunityM310Analyzer
from pre_diagnostic_engine.analyzers.viability_analyzer        import ViabilityAnalyzer

from pre_diagnostic_engine.validators.regime_validator         import RegimeValidator
from pre_diagnostic_engine.validators.darf_validator           import DarfValidator
from pre_diagnostic_engine.validators.lucro_validator          import LucroValidator
from pre_diagnostic_engine.validators.patrimonio_validator     import PatrimonioValidator
from pre_diagnostic_engine.validators.retention_validator      import RetentionValidator
from pre_diagnostic_engine.validators.fiscal_result_validator  import FiscalResultValidator

from pre_diagnostic_engine.consolidators.fiscal_consolidator       import FiscalConsolidator
from pre_diagnostic_engine.consolidators.financial_consolidator    import FinancialConsolidator
from pre_diagnostic_engine.consolidators.opportunity_consolidator  import OpportunityConsolidator
from pre_diagnostic_engine.consolidators.final_consolidator        import FinalConsolidator

from pre_diagnostic_engine.scorers.go_no_go_scorer   import GoNoGoScorer
from pre_diagnostic_engine.scorers.opportunity_score import OpportunityScorer
from pre_diagnostic_engine.scorers.viability_score   import ViabilityScorer

from pre_diagnostic_engine.repositories.gold_repository import GoldRepository
from pre_diagnostic_engine.repositories.oci_repository  import OciRepository

from pre_diagnostic_engine.utils.indicators import calc_effective_tax_rate, calc_darf_vs_contabil_ratio

from pre_diagnostic_engine.spark.session import create_spark_session
from pre_diagnostic_engine.spark.config_loader import load_config

from pre_diagnostic_engine.reports.generate_excel import TributaryReportBuilder


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(asctime)s %(message)s")
logger = logging.getLogger("dinamo.tax_intelligence.pre_diagnostic_pipeline")


class PreDiagnosticPipeline:
    """
    Ponto de entrada único do engine de pré-diagnóstico tributário.

    Uso:
        pipeline = PreDiagnosticPipeline(spark, oci_repo, gold_repo)
        pipeline.run(client_cnpj="12345678000195", month_year="202412")
    """

    def __init__(
        self,
        spark: SparkSession,
        oci_repo: OciRepository,
        gold_repo: GoldRepository,
    ):
        self.spark = spark
        self.oci   = oci_repo
        self.gold  = gold_repo

        # Extractors
        self.ecf_extractor       = EcfExtractor(spark, oci_repo)
        self.darf_extractor      = DarfExtractor(spark, oci_repo)
        self.retention_extractor = RetentionExtractor(spark, oci_repo)
        self.company_extractor   = CompanyExtractor(spark, oci_repo)

        # Analyzers
        self.regime_analyzer     = RegimeAnalyzer()
        self.contabil_analyzer   = ContabilAnalyzer()
        self.fiscal_analyzer     = FiscalAnalyzer()
        self.patrimonio_analyzer = PatrimonioAnalyzer()
        self.darf_analyzer       = DarfAnalyzer()
        self.retention_analyzer  = RetentionAnalyzer()
        self.opp_l300_analyzer   = OpportunityL300Analyzer()
        self.opp_m310_analyzer   = OpportunityM310Analyzer()
        self.viability_analyzer  = ViabilityAnalyzer()

        # Validators
        self.regime_validator     = RegimeValidator()
        self.darf_validator       = DarfValidator()
        self.lucro_validator      = LucroValidator()
        self.patrimonio_validator = PatrimonioValidator()
        self.retention_validator  = RetentionValidator()
        self.result_validator     = FiscalResultValidator()

        # Consolidators
        self.fiscal_consolidator      = FiscalConsolidator()
        self.financial_consolidator   = FinancialConsolidator()
        self.opportunity_consolidator = OpportunityConsolidator()
        self.final_consolidator       = FinalConsolidator()

        # Scorers
        self.go_no_go_scorer    = GoNoGoScorer()
        self.opportunity_scorer = OpportunityScorer()
        self.viability_scorer   = ViabilityScorer()

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC ENTRY POINT
    # ─────────────────────────────────────────────────────────────────────────

    def run(self, client_cnpj: str, month_year: str) -> None:
        """
        Executa o pipeline completo para um CNPJ e período.
        Persiste resultados nas Gold Tables via upsert.
        """
        logger.info(f"▶ PreDiagnosticPipeline.run | cnpj={client_cnpj} | período={month_year}")

        # ── 1. EXTRAÇÃO ───────────────────────────────────────────────────────
        df_0010      = self.ecf_extractor.extract_0010(client_cnpj, month_year)
        # logger.info(df_0010.show())
        
        # A partir daqui, cada relatório ECF é OPCIONAL: se não existir no bucket,
        # seguimos com None e cada analyzer devolve a métrica correspondente nula
        # (ancorada em CNPJ/DT_INI do 0010), em vez de abortar o ano inteiro.
        df_l100      = self.ecf_extractor.extract_l100(client_cnpj, month_year)
        if df_l100 is None:
            logger.warning("df_l100 não encontrado — seguindo sem dados de patrimônio (L100)")
        else:
            logger.info("df_l100 extraído")

        df_l300      = self.ecf_extractor.extract_l300(client_cnpj, month_year)
        if df_l300 is None:
            logger.warning("df_l300 não encontrado — seguindo sem dados contábeis/oportunidades L300")
        else:
            logger.info("df_l300 extraído")

        df_m310      = self.ecf_extractor.extract_m310(client_cnpj, month_year)
        if df_m310 is None:
            logger.warning("df_m310 não encontrado — seguindo sem oportunidades M310")
        else:
            logger.info("df_m310 extraído")

        df_n500      = self.ecf_extractor.extract_n500(client_cnpj, month_year)
        if df_n500 is None:
            logger.warning("df_n500 não encontrado — seguindo sem resultado fiscal (N500)")
        else:
            logger.info("df_n500 extraído")

        df_y570_raw  = self.ecf_extractor.extract_y570(client_cnpj, month_year)
        if df_y570_raw is None:
            logger.warning("df_y570_raw não encontrado — seguindo sem retenções (Y570)")
        else:
            logger.info("df_y570_raw extraído")

        df_darf_raw  = self.darf_extractor.extract_darf_payments(client_cnpj, month_year)
        if df_darf_raw is None:
            logger.warning("df_darf_raw não encontrado — seguindo sem DARFs")
        else:
            logger.info("df_darf_raw extraído")

        df_company   = self.company_extractor.extract_company_info(client_cnpj, month_year)
        if df_company is None:
            logger.warning("df_company não encontrado — seguindo sem dados cadastrais (NOME nulo)")
        else:
            logger.info("df_company extraído")

        # ── 2. ANÁLISE ────────────────────────────────────────────────────────
        # df_regime é a âncora: vem do 0010, que continua obrigatório (é dele que
        # tiramos o CNPJ/DT_INI reais usados para "ancorar" as métricas nulas
        # de qualquer relatório ausente acima).
        df_regime      = self.regime_analyzer.analyze(df_0010)
        df_contabil    = self.contabil_analyzer.analyze(df_l300, anchor=df_regime)
        df_fiscal      = self.fiscal_analyzer.analyze(df_n500, anchor=df_regime)
        df_patrimonio  = self.patrimonio_analyzer.analyze(df_l100, anchor=df_regime)
        # df_darf        = self.darf_analyzer.analyze(df_darf_raw) # Retirar comentario após integração Agenor
        df_retention   = self.retention_analyzer.analyze(df_y570_raw, anchor=df_regime) # Retirar comentario após integração Agenor
        df_opp_l300    = self.opp_l300_analyzer.analyze(df_l300, anchor=df_regime)
        df_opp_m310    = self.opp_m310_analyzer.analyze(df_m310, anchor=df_regime)

        # ── 3. VALIDAÇÃO ──────────────────────────────────────────────────────
        self.regime_validator.validate(df_regime)
        # self.darf_validator.validate(df_darf)
        self.lucro_validator.validate(
            df_contabil.join(df_fiscal, on=["CNPJ", "DT_INI"], how="outer")
        )
        self.patrimonio_validator.validate(df_patrimonio)
        #self.retention_validator.validate(df_retention)

        # ── 4. CONSOLIDAÇÃO ───────────────────────────────────────────────────
        df_fiscal_cons  = self.fiscal_consolidator.consolidate(df_contabil, df_fiscal, df_patrimonio)
        df_financial    = self.financial_consolidator.consolidate(df_retention=df_retention) # incluir o paramente df_darf quando habilitar a extração via integração Agenor
        df_opp_cons     = self.opportunity_consolidator.consolidate(df_opp_l300, df_opp_m310)

        df_final = self.final_consolidator.consolidate(
            df_regime, df_fiscal_cons, df_financial, df_opp_cons, df_company  # UTILIZAR ESTA LINHA QUANDO HABILITAR A EXTRAÇÃO DA DARF via integração Agenor
            # df_regime, df_fiscal_cons, df_opp_cons, df_company
        )

        # ── 5. SCORING ────────────────────────────────────────────────────────
        df_final = self.go_no_go_scorer.score(df_final)
        df_final = self.viability_analyzer.analyze(df_final)   # go_no_go + commercial_priority

        # Oportunidades individuais (para gold_pre_diagnostic_opportunities)
        df_opportunities = self.opportunity_scorer.build_opportunity_rows(df_final)

        # Viabilidade comercial (estimated_recovery + viability_index)
        df_final = self.viability_scorer.score(df_final, df_opportunities)

        # Indicadores derivados
        df_final = calc_effective_tax_rate(df_final)
        df_final = calc_darf_vs_contabil_ratio(df_final)

        
        # ── 5.1 VALIDAÇÃO FINAL ───────────────────────────────────────────────
        self.result_validator.validate(df_final)

        # ── 6. PERSISTÊNCIA ───────────────────────────────────────────────────
        logger.info("Persistindo resultados nas Gold Tables...")
        self.gold.upsert_pre_diagnostic_summary(
            df_final.select(
                "CNPJ", "DT_INI",
                "NOME", "final_score", "classification",
                "go_no_go", "commercial_priority",
                "commercial_viability_index", "estimated_recovery",
            )
        )
        self.gold.upsert_pre_diagnostic_scores(
            df_final.select(
                "CNPJ", "DT_INI", "cnpj_base",
                "raw_score", "final_score", "classification",
            )
        )

        go_no_go_cols = [
            "CNPJ", "DT_INI", "cnpj_base",
            "go_no_go", "commercial_priority",
            "regime_no_go_presumido"
        ]
        if "ausencia_darf" in df_final.columns:
            go_no_go_cols.append("ausencia_darf")

        self.gold.upsert_pre_diagnostic_go_no_go(
            df_final.select(*go_no_go_cols)
        )

        financial_cols = [
            "CNPJ", "DT_INI", "cnpj_base",
            "patrimonio_liquido", "resultado_contabil",
            "resultado_fiscal", "total_retencoes",
            "estimated_recovery"
        ]

        if "total_darf_pago" in df_final.columns:
            financial_cols.append("total_darf_pago")

        self.gold.upsert_pre_diagnostic_financials(
            df_final.select(*financial_cols)
        )
        self.gold.upsert_pre_diagnostic_opportunities(df_opportunities)

        self.gold.upsert_pre_diagnostic_master(df_final)

        # Relatório master
        df_historico = (
            self.final_consolidator
            .consolidar_master_historico(
                spark=self.spark,
                df_atual=df_final,
                base_path=self.gold.delta.base_path,
                cnpj=client_cnpj
            )
        )
        
        self.gold.write_pre_diagnostic_master_historical(df_historico, client_cnpj)

        logger.info(f"✔ Pipeline concluído | cnpj={client_cnpj} | período={month_year}")
        
        # ── 7. GERAÇÃO DO RELATÓRIO EXCEL ────────────────────────────────────────────

        # df_historico já contém os períodos consolidados (últimos 5 anos) gerados
        # pelo FinalConsolidator.consolidar_master_historico().
        # Convertemos para lista de dicts aqui (único toPandas() do pipeline).
        # O Excel é salvo localmente em /tmp e depois pode ser copiado para o bucket.
        
        


if __name__ == "__main__":
    config = load_config()
    spark = create_spark_session(app_name="PRE_DIAGNOSTIC_PIPELINE", config=config)
    bucket_name ="datafoundation-agtax-evollux-prd"
    gold_base_path = f"s3a://{bucket_name}/PRE_DIAGNOSTICO"
    oci_repo = OciRepository(spark, bucket_name=bucket_name)
    gold_repo = GoldRepository(spark, gold_base_path=gold_base_path)
    pipeline = PreDiagnosticPipeline(spark, oci_repo, gold_repo)
    cnpj = "00697509"
    years = ["202001", "202101", "202201", "202301", "202401"]
    
    for year in years:
        pipeline.run(cnpj, year)

    # 2. Gera o Excel consolidado UMA ÚNICA VEZ, lendo direto do bucket
    try:
        path_historico = f"{gold_base_path}/gold_pre_diagnostic_master_historical"
        df_final_excel = spark.read.format("delta").load(path_historico).filter(F.col("cnpj_base") == cnpj).orderBy("DT_INI")
        
        report_rows = df_final_excel.toPandas().to_dict("records")
        report_output = f"/home/marcosmadeira/dinamo-etl-kafka/src/pre_diagnostic_engine/downloads/relatorio_tributario_{cnpj}.xlsx"
        
        TributaryReportBuilder().build(
            rows        = report_rows,
            output_path = report_output,
        )
        logger.info(f"📊 Relatório Excel CONSOLIDADO gerado: {report_output}")
    except Exception as e:
        logger.warning(f"Falha ao gerar relatório Excel (não bloqueia pipeline): {e}")

