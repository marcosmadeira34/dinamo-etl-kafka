from dataclasses import dataclass, field
from typing import Dict

@dataclass
class ScoreResult:
    cnpj: str
    ano_calendario: int
    raw_score: float
    final_score: float
    classification: str
    breakdown: Dict[str, float] = field(default_factory=dict)
