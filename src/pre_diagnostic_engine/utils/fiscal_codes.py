# src/pre_diagnostic_engine/utils/fiscal_codes.py
"""Constantes e helpers de códigos fiscais."""

# Regimes ECF (FORMA_TRIB_PER)
LUCRO_REAL      = "R"
LUCRO_PRESUMIDO = "P"
LUCRO_ARBITRADO = "A"
SIMPLES         = "S"

# Natureza de retenção
NATUREZA_IRPJ = ["IRPJ", "2"]
NATUREZA_CSLL = ["CSLL", "4"]


def is_lucro_real(forma_trib: str) -> bool:
    return forma_trib == LUCRO_REAL


def is_lucro_presumido(forma_trib: str) -> bool:
    return forma_trib == LUCRO_PRESUMIDO