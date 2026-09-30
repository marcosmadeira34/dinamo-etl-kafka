# src/pre_diagnostic_engine/models/company_profile.py
"""Perfil Tributário da Empresa"""
from typing import List
from dataclasses import dataclass, field
from datetime import date


@dataclass
class CompanyProfile:
    cnpj: str
    razao_social: str
    status: str
    natureza_juridica: str

    # Regime
    regime_atual: str
    regime_automatico: str
    start_date: date
    end_date: date

    # Indicadores (vieses)
    indicators: List[str] = field(default_factory=list)
