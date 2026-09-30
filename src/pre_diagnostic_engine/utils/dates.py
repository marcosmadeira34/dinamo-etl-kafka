# src/pre_diagnostic_engine/utils/dates.py
"""Utilitários de data para o engine."""
from datetime import datetime


def current_month_year() -> str:
    """Retorna o mês/ano atual no formato YYYYMM."""
    return datetime.now().strftime("%Y%m")


def parse_month_year(month_year: str) -> tuple[int, int]:
    """Parseia 'YYYYMM' em (ano, mes)."""
    dt = datetime.strptime(month_year, "%Y%m")
    return dt.year, dt.month


def ano_calendario_from_month_year(month_year: str) -> int:
    """Extrai ano_calendario de uma string YYYYMM."""
    return int(month_year[:4])