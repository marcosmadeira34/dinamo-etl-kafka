# src/pre_diagnostic_engine/models/opportunity_result.py
from typing import List, Optional, Dict
from dataclasses import dataclass, field


@dataclass
class OpportunityResult:
    opportunity_id: str
    client_cnpj: str
    month_year: str

    # 1. Resultado Fiscal / Lucro Fiscal (N500)
    profit_loss_fiscal: Optional[float] = None  # Valor do Lucro/Prejuízo Fiscal (LIRF)
    pretax_income: Optional[float] = None      # Lucro Antes do IR
    taxed_income: Optional[float] = None       # Lucro Tributável

    # 2. Base do IRPJ (M310 + M310 de Ajustes Positivos + N500)
    adjusted_income_base: Optional[float] = None

    # 3. Alíquotas e Adicionais IRPJ / CSLL
    irpj_quota: Optional[float] = 15.0
    csll_quota: Optional[float] = 9.0

    irpj_additional: Optional[float] = None    # 10% sobre R$ 20k
    csll_additional: Optional[float] = None    # 15% sobre R$ 20k

    # 4. Alíquotas do Simples Nacional
    simples_ir_quota: Optional[float] = None
    simples_csll_quota: Optional[float] = None
    simples_cofins_quota: Optional[float] = None
    simples_pis_quota: Optional[float] = None
    simples_icms_quota: Optional[float] = None
    simples_iss_quota: Optional[float] = None

    # 5. Resultado Bruto (DRE)
    gross_result: Optional[float] = None

    # 6. Limite de Lucro para Adicional IRPJ / CSLL
    limit_240k: float = 20000.0

    # 7. Dados de Faturamento / Receita Bruta
    gross_revenue: Optional[float] = None       # (Soma dos créditos de DRE 3.01)

    # 8. Dizeres Fiscais
    fiscal_regime: Optional[str] = None       # Lucro Presumido, Lucro Real, Simples
    distribution_authorization: Optional[bool] = None  # Distribuição de lucros autorizada
    profit_distribution_pct: Optional[float] = None     # % a distribuir
    distribution_values: Dict[str, float] = field(default_factory=dict)

    # 9. Compensação de Prejuízos
    prior_years_losses_limit: Optional[float] = None  # 30% do Lucro Fiscal
    loss_carryforward: Optional[float] = None         # Prejuízo remanescente

    # 10. Campos auxiliares de cálculo
    base_irpj_tax: Optional[float] = None
    base_csll_tax: Optional[float] = None

    # 11. Créditos de Retenção
    retentions_credits: Dict[str, float] = field(default_factory=dict)  # Y570
