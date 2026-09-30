# src/pre_diagnostic_engine/models/diagnostic_result.py
from typing import List, Optional, Dict
from dataclasses import dataclass, field
from datetime import datetime

from .company_profile import CompanyProfile
from .opportunity_result import OpportunityResult


@dataclass
class DiagnosticResult:
    client_cnpj: str
    analysis_month: str  # "2023-09"
    created_at: datetime

    # Informações da Empresa
    company_profile: CompanyProfile

    # Resultados Fiscais por Mês
    monthly_results: List[OpportunityResult] = field(default_factory=list)

    # Agregações
    total_revenue_current_year: Optional[float] = None
    total_taxes_current_year: Optional[float] = None

    # Tendências (Simplificado para esta versão)
    revenue_trend_pct: Optional[float] = None
    tax_burden_avg: Optional[float] = None  # Carga tributária média

    # Aconselhamentos
    recommendations: List[str] = field(default_factory=list)
