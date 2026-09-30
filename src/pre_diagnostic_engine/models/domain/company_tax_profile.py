from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

@dataclass
class CompanyTaxProfile:
    cnpj: str
    razao_social: str
    ano_calendario: int
    regime_history: List[Dict[str, Any]] = field(default_factory=list)
    patrimonio_history: List[Dict[str, Any]] = field(default_factory=list)
    contabil_history: List[Dict[str, Any]] = field(default_factory=list)
    fiscal_history: List[Dict[str, Any]] = field(default_factory=list)
    retentions: List[Dict[str, Any]] = field(default_factory=list)
    darf_payments: List[Dict[str, Any]] = field(default_factory=list)
    identified_opportunities: List[Dict[str, Any]] = field(default_factory=list)
    score: float = 0.0
    classification: str = ""
    go_no_go: str = "NO_GO"
    estimated_recovery: float = 0.0
    commercial_priority: str = "BAIXA"
