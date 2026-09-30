# src/pre_diagnostic_engine/reports/generate_excel.py
"""
Gerador do Relatório Excel de Análise Tributária ECF.

Lê o gold_pre_diagnostic_master (Delta Lake) e produz um .xlsx com:
    Linha 1 — Seção (Análise Tributária | Oportunidades Identificadas)
    Linha 2 — Fonte: referência ao registro/campo SPED
    Linha 3 — Header: nome amigável
    Linhas 4+ — Dados: um período por linha, ordenado por DT_INI

Uso via pipeline (já integrado em pre_diagnostic_pipeline.py):
    rows = df_historico.orderBy("DT_INI").toPandas().to_dict("records")
    TributaryReportBuilder().build(rows=rows, output_path="/tmp/relatorio.xlsx")

Uso standalone:
    python generate_excel.py --cnpj 79038097000181 --output relatorio.xlsx
"""

import logging
import argparse
import math
from typing import Optional

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

logger = logging.getLogger("dinamo.tax_intelligence.generate_excel")        
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTO DE COLUNAS
# (label_amigavel, campo_master, tipo)
# tipo: "text" | "cnpj" | "periodo" | "regime" | "num"
# ─────────────────────────────────────────────────────────────────────────────
REPORT_COLUMNS = [
    # ── Análise Tributária (cols 1-9) ─────────────────────────────────────────
    ("Nome",                               "NOME",                           "text"),
    ("CNPJ",                               "CNPJ",                           "cnpj"),
    ("Período",                            "DT_INI",                         "periodo"),
    ("Regime",                             "FORMA_TRIB_PER",                  "regime"),
    ("PL Inicial",                         "patrimonio_liquido",              "num"),
    ("Resultado Contábil",                 "resultado_contabil",              "num"),
    ("Prejuízo Fiscal / Lucro Fiscal",     "resultado_fiscal",                "num"),
    ("IRPJ Retido",                        "irpj_retido",                     "num"),
    ("CSLL Retido",                        "csll_retido",                     "num"),
    # ── Oportunidades Identificadas (cols 10-20) ──────────────────────────────
    # IRPJ/CSLL Recolhido: pendente integração com DarfAnalyzer.
    # TODO: substituir None por "total_darf_irpj" / "total_darf_csll" quando disponível.
    ("IRPJ Recolhido",                     None,                              "num"),  # pendente DarfAnalyzer
    ("CSLL Recolhido",                     None,                              "num"),  # pendente DarfAnalyzer
    ("IRPJ - Fontes Pagadoras",            None,                              "num"),  # pendente integração Fontes Pagadoras
    ("CSLL - Fontes Pagadoras",            None,                              "num"),  # pendente integração Fontes Pagadoras
    ("PAT – Alimentação do Trabalhador",   "opp_l300_pat_valor",              "num"),
    ("Receita de JCP",                     "opp_l300_receita_jcp_valor",      "num"),
    ("Despesas de JCP",                    "opp_l300_despesa_jcp_valor",      "num"),
    ("Doações/Subv. Invest.",              "opp_m310_subvencao_valor",        "num"),
    ("Doações/Subv. Art. 30",             "opp_m310_art30_valor",            "num"),
    ("Doações/Subv. Invest. Créd. IRPJ",  "opp_m310_credito_irpj_valor",     "num"),
    ("JCP Dedutível s/ Despesa",           "opp_m310_jcp_dedutivel_valor",    "num"),
]

REPORT_SOURCES = [
    "Registro 0000\nCampo NOME",
    "Registro 0000\nCampo CNPJ",
    "Registro 0000\nCampo DT_FIN",
    "Registro 0010\nFORMA_TRIB_PER",
    "Registro L100\nReferencial 2.03",
    "Registro L300\nReferencial 3.01",
    "Registro N500\nCampo VALOR",
    "Registro Y570\nIR_RET",
    "Registro Y570\nCSLL_RET",
    "DARFs IRPJ\n(pendente integração)",   # TODO: DarfAnalyzer – cód: 5993/2362/3373/0220/2456/2089
    "DARFs CSLL\n(pendente integração)",   # TODO: DarfAnalyzer – cód: 2484/6012/6773/2372
    "Fontes Pagadoras\n(pendente integração)",
    "Fontes Pagadoras\n(pendente integração)",
    "L300\n3.01.01.07.01.13",
    "L300\n3.01.01.05.01.04",
    "L300\n3.01.01.09.01.04",
    "M310\nRef 106",
    "M310\nRef 106.05",
    "M310\nRef 106.10",
    "M310\nRef 166.03",
]

# (label, col_ini, col_fim, cor_hex)
REPORT_SECTIONS = [
    ("Análise Tributária",          1,  9,  "1F3864"),
    ("Oportunidades Identificadas", 10, 20, "375623"),
]

COL_WIDTHS = [44, 22, 8, 14, 16, 18, 22, 12, 12,
              12, 12, 20, 20, 22, 16, 16, 20, 20, 26, 22]

REGIME_MAP = {"RRRR": "Lucro Real", "R": "Lucro Real", "P": "Lucro Presumido"}

C = {
    "hdr_blue_dk":  "1F3864", "hdr_blue_md":  "2E75B6",
    "hdr_grn_dk":   "375623", "hdr_grn_md":   "538135",
    "src_blue":     "D6E4F0", "src_grn":      "E2EFDA",
    "row_odd":      "FFFFFF", "row_even":     "EBF3FB",
    "white":        "FFFFFF", "dark":         "1F2D3D", "border": "BDD7EE",
}
FMT_BRL = '#,##0.00;[Red](#,##0.00);"-"'


def _fill(h): return PatternFill("solid", fgColor=h)
def _brd(c="BDD7EE", s="thin"):
    side = Side(style=s, color=c)
    return Border(left=side, right=side, top=side, bottom=side)
def _fmt_cnpj(v):
    v = str(v).strip().zfill(14)
    return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:]}"
def _fmt_periodo(v):
    v = str(v).strip()
    return v[4:] if len(v) == 8 else v[2:] if len(v) == 6 else v
def _is_null(v):
    if v is None: return True
    try: return math.isnan(float(v))
    except: return False


class TributaryReportBuilder:
    """
    Gera o relatório Excel de Análise Tributária ECF a partir do
    gold_pre_diagnostic_master.

    Dois modos:
        build()            → recebe lista de dicts (já coletados pelo pipeline)
        build_from_delta() → lê diretamente do Delta Lake
    """

    def build(self, rows: list, output_path: str) -> str:
        if not rows:
            logger.warning("Lista de rows vazia. Abortando geração do Excel.")
            return output_path

        wb = Workbook()
        ws = wb.active
        ws.title = "Análise Tributária ECF"
        ws.sheet_view.showGridLines = False

        self._section_row(ws)
        self._source_row(ws)
        self._header_row(ws)
        self._data_rows(ws, rows)
        self._col_widths(ws)
        self._row_heights(ws, len(rows))

        ws.freeze_panes = "A4"
        last = get_column_letter(len(REPORT_COLUMNS))
        ws.auto_filter.ref = f"A3:{last}{3 + len(rows)}"

        self._legend_sheet(wb)

        wb.save(output_path)
        logger.info(f"Relatório Excel salvo: {output_path}")
        return output_path

    def build_from_delta(
        self,
        spark,
        gold_base_path: str,
        cnpj: str,
        output_path: str,
        years: int = 5,
    ) -> str:

        path = f"{gold_base_path.rstrip('/')}/gold_pre_diagnostic_master"
        logger.info(f"Lendo {path} para CNPJ {cnpj}")
        df = (
            spark.read.format("delta").load(path)
            .filter(F.col("CNPJ") == cnpj)
            .filter(F.col("CNPJ").isNotNull() & F.col("DT_INI").isNotNull())
            .withColumn("_ano", F.substring("DT_INI", 5, 4).cast("int"))
            .filter(F.col("_ano") >= (F.year(F.current_date()) - years))
            .drop("_ano")
            .orderBy("DT_INI")
        )
        return self.build(rows=df.toPandas().to_dict("records"), output_path=output_path)

    # ── helpers internos ──────────────────────────────────────────────────────

    def _coerce(self, raw, kind):
        if kind == "text":   return str(raw) if not _is_null(raw) else ""
        if kind == "cnpj":   return _fmt_cnpj(raw) if not _is_null(raw) else ""
        if kind == "periodo":return _fmt_periodo(str(raw)) if not _is_null(raw) else ""
        if kind == "regime": return REGIME_MAP.get(str(raw).strip(), str(raw)) if not _is_null(raw) else ""
        if kind == "num":    return float(raw) if not _is_null(raw) else None
        return raw

    def _section_row(self, ws):
        for label, c1, c2, color in REPORT_SECTIONS:
            c = ws.cell(row=1, column=c1, value=label)
            c.font      = Font(name="Arial", bold=True, color=C["white"], size=11)
            c.fill      = _fill(color)
            c.alignment = Alignment(horizontal="center", vertical="center")
            c.border    = _brd(color, "medium")
            if c2 > c1:
                ws.merge_cells(start_row=1, start_column=c1, end_row=1, end_column=c2)

    def _source_row(self, ws):
        for i, src in enumerate(REPORT_SOURCES):
            opp = i >= 9
            c = ws.cell(row=2, column=i+1, value=src)
            c.font      = Font(name="Arial", size=7, color=C["dark"], italic=True)
            c.fill      = _fill(C["src_grn"] if opp else C["src_blue"])
            c.alignment = Alignment(horizontal="center", vertical="top", wrap_text=True)
            c.border    = _brd()

    def _header_row(self, ws):
        for i, (label, _, _) in enumerate(REPORT_COLUMNS):
            opp = i >= 9
            c = ws.cell(row=3, column=i+1, value=label)
            c.font      = Font(name="Arial", bold=True, color=C["white"], size=9)
            c.fill      = _fill(C["hdr_grn_md"] if opp else C["hdr_blue_md"])
            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            c.border    = _brd()

    def _data_rows(self, ws, rows):
        for r_idx, row in enumerate(rows):
            rn = r_idx + 4
            bg = C["row_odd"] if r_idx % 2 == 0 else C["row_even"]
            for c_idx, (_, field, kind) in enumerate(REPORT_COLUMNS):
                val = self._coerce(row.get(field) if field is not None else None, kind)
                cell = ws.cell(row=rn, column=c_idx+1, value=val)
                cell.font      = Font(name="Arial", size=9, color=C["dark"])
                cell.fill      = _fill(bg)
                cell.alignment = Alignment(
                    horizontal="left" if c_idx == 0 else "center" if c_idx <= 3 else "right",
                    vertical="center",
                )
                cell.border = _brd()
                if kind == "num" and val is not None:
                    cell.number_format = FMT_BRL

    def _col_widths(self, ws):
        for i, w in enumerate(COL_WIDTHS[:len(REPORT_COLUMNS)]):
            ws.column_dimensions[get_column_letter(i+1)].width = w

    def _row_heights(self, ws, n):
        ws.row_dimensions[1].height = 22
        ws.row_dimensions[2].height = 52
        ws.row_dimensions[3].height = 30
        for r in range(4, 4+n): ws.row_dimensions[r].height = 18

    def _legend_sheet(self, wb):
        ws = wb.create_sheet("Legenda – Campos SPED")
        ws.sheet_view.showGridLines = False
        rows = [
            ("Campo no Relatório",            "Descrição",                                "Origem no SPED ECF"),
            ("Nome",                          "Razão social da empresa",                  "Reg. 0000 – NOME"),
            ("CNPJ",                          "CNPJ completo (14 dígitos)",               "Reg. 0000 – CNPJ"),
            ("Período",                       "Ano de referência do ECF",                 "Reg. 0000 – DT_FIN → AAAA"),
            ("Regime",                        "Regime tributário do exercício",            "Reg. 0010 – FORMA_TRIB_PER (RRRR=Lucro Real | P=Presumido)"),
            ("PL Inicial",                    "Patrimônio Líquido inicial",               "L100 – Ref. 2.03 (VAL_CTA_REF_INI)"),
            ("Resultado Contábil",            "Resultado líquido do exercício (DRE)",     "L300 – startswith('3.01')"),
            ("Prej. Fiscal / Lucro Fiscal",   "Lucro ou Prejuízo Fiscal apurado",         "N500 – campo VALOR"),
            ("IRPJ Retido",                   "Total IR retido na fonte",                 "Y570 – IR_RET (soma por período)"),
            ("CSLL Retido",                   "Total CSLL retido na fonte",               "Y570 – CSLL_RET (soma por período)"),
            ("IRPJ Recolhido",                "IRPJ pago via DARF",                       "cód: 5993|2362|3373|0220|2456|2089"),
            ("CSLL Recolhido",                "CSLL pago via DARF",                       "cód: 2484|6012|6773|2372"),
            ("IRPJ – Fontes Pagadoras",       "IRPJ retido por fontes pagadoras",         "Fontes Pagadoras (pendente integração)"),
            ("CSLL – Fontes Pagadoras",       "CSLL retido por fontes pagadoras",         "Fontes Pagadoras (pendente integração)"),
            ("PAT",                           "Programa Alimentação do Trabalhador",       "L300 – Ref. 3.01.01.07.01.13"),
            ("Receita de JCP",                "JCP recebido",                             "L300 – Ref. 3.01.01.05.01.04"),
            ("Despesas de JCP",               "JCP pago / dedutível",                     "L300 – Ref. 3.01.01.09.01.04"),
            ("Doações/Subv. Invest.",         "Subvenção para investimento",              "M310 – Código 106"),
            ("Doações/Subv. Art. 30",         "Exclusão subvenção Art. 30",              "M310 – Código 106.05"),
            ("Doações/Subv. Créd. IRPJ",      "Crédito IRPJ sobre subvenção",            "M310 – Código 106.10"),
            ("JCP Dedutível s/ Despesa",      "JCP dedutível sobre despesa",              "M310 – Código 166.03"),
        ]
        for i, w in enumerate([30, 44, 52]):
            ws.column_dimensions[get_column_letter(i+1)].width = w
        for r_idx, row in enumerate(rows):
            hdr = r_idx == 0
            bg  = C["hdr_blue_dk"] if hdr else (C["row_odd"] if r_idx % 2 == 0 else C["row_even"])
            for c_idx, val in enumerate(row):
                c = ws.cell(row=r_idx+1, column=c_idx+1, value=val)
                c.font      = Font(name="Arial", bold=hdr, size=9,
                                   color=C["white"] if hdr else C["dark"])
                c.fill      = _fill(bg)
                c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                c.border    = _brd()
            ws.row_dimensions[r_idx+1].height = 18


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT STANDALONE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, "src")
    from pre_diagnostic_engine.spark.session       import create_spark_session
    from pre_diagnostic_engine.spark.config_loader import load_config

    p = argparse.ArgumentParser()
    p.add_argument("--cnpj",      required=True)
    p.add_argument("--gold-path", default="s3a://datafoundation-agtax-evollux-prd/PRE_DIAGNOSTICO")
    p.add_argument("--output",    default="relatorio_tributario_ecf.xlsx")
    p.add_argument("--years",     type=int, default=5)
    args = p.parse_args()

    config = load_config()
    spark  = create_spark_session("TributaryReportGenerator", config)
    out = TributaryReportBuilder().build_from_delta(
        spark=spark, gold_base_path=args.gold_path,
        cnpj=args.cnpj, output_path=args.output, years=args.years,
    )
    print(f"✅ {out}")
    spark.stop()