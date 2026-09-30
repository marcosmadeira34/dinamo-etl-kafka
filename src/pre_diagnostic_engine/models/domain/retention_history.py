from dataclasses import dataclass

@dataclass
class RetentionHistory:
    cnpj: str
    ano_calendario: int
    irpj_retido: float
    csll_retido: float
    total_retencoes: float
    divergencia_constatada: bool
    potencial_compensacao: float
