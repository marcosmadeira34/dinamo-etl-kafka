from dataclasses import dataclass

@dataclass
class OpportunityResult:
    cnpj: str
    ano_calendario: int
    rule_id: str
    description: str
    reference_code: str
    estimated_credit: float
    score_impact: float
