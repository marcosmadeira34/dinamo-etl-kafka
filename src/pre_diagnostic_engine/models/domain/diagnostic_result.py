from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class DiagnosticResult:
    cnpj: str
    razao_social: str
    ano_calendario: int
    score: float
    classification: str
    go_no_go: str
    details: Dict[str, Any] = field(default_factory=dict)
