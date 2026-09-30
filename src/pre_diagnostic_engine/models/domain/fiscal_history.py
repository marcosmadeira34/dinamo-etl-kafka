from dataclasses import dataclass

@dataclass
class FiscalHistory:
    cnpj: str
    ano_calendario: int
    regime: str
    patrimonio_liquido: float
    lucro_contabil: float
    lucro_fiscal: float
    is_lucro_recorrente: bool
    is_prejuizo_recorrente: bool
