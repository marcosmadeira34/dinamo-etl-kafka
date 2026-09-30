"""
tests/contracts/test_silver_gold_contract.py

Testa o contrato de schema entre Silver e Gold.
Não requer Spark real — usa dicionários para simular o schema de saída.
"""
import pytest
from dataclasses import dataclass
from typing import Optional


# ─── Contrato formal Silver → Gold ───────────────────────────────────────────

@dataclass(frozen=True)
class SilverOutputContract:
    """
    Define os campos obrigatórios que o Silver DEVE entregar ao Gold.
    Atualizar aqui propaga a validação para todos os testes.
    """
    METADATA_FIELDS = {
        "_file_id",
        "_cnpj",
        "_nome",
        "_dt_ini",
        "_dt_fin",
        "_report_key",
    }
    HIERARCHY_FIELDS = {
        "_parent_uid_final",
        "_uid",
        "_register",
    }
    CONTROL_FIELDS = {
        "_ingestion_ts",
        "_source_file",
    }

    @classmethod
    def all_required(cls) -> set[str]:
        return cls.METADATA_FIELDS | cls.HIERARCHY_FIELDS | cls.CONTROL_FIELDS


# ─── Simulação de schema (substitui Spark StructType nos unit tests) ──────────

def make_silver_schema(overrides: Optional[dict] = None) -> dict[str, str]:
    """Retorna um schema Silver completo (simulado como dict campo→tipo)."""
    base = {f: "string" for f in SilverOutputContract.all_required()}
    base.update({
        "REG": "string",
        "VL_REC": "double",
        "CST_PIS": "string",
    })
    if overrides:
        base.update(overrides)
    return base


# ─── Validador de contrato ────────────────────────────────────────────────────

def validate_silver_schema(schema: dict[str, str]) -> list[str]:
    """
    Valida se o schema Silver respeita o contrato.
    Retorna lista de erros (vazia = válido).
    """
    errors = []
    required = SilverOutputContract.all_required()
    missing = required - set(schema.keys())
    for field in sorted(missing):
        errors.append(f"Campo obrigatório ausente: '{field}'")

    # _parent_uid_final não pode ser nulo por design
    if "_parent_uid_final" in schema and schema["_parent_uid_final"] is None:
        errors.append("'_parent_uid_final' não pode ser None (propagação falhou)")

    return errors


# ─── Testes ──────────────────────────────────────────────────────────────────

class TestSilverGoldContract:

    def test_complete_schema_is_valid(self):
        schema = make_silver_schema()
        errors = validate_silver_schema(schema)
        assert errors == [], f"Schema inválido: {errors}"

    def test_missing_file_id_breaks_contract(self):
        schema = make_silver_schema()
        del schema["_file_id"]
        errors = validate_silver_schema(schema)
        assert any("_file_id" in e for e in errors)

    def test_missing_cnpj_breaks_contract(self):
        schema = make_silver_schema()
        del schema["_cnpj"]
        errors = validate_silver_schema(schema)
        assert any("_cnpj" in e for e in errors)

    def test_missing_parent_uid_breaks_contract(self):
        schema = make_silver_schema()
        del schema["_parent_uid_final"]
        errors = validate_silver_schema(schema)
        assert any("_parent_uid_final" in e for e in errors)

    def test_missing_report_key_breaks_contract(self):
        schema = make_silver_schema()
        del schema["_report_key"]
        errors = validate_silver_schema(schema)
        assert any("_report_key" in e for e in errors)

    def test_missing_multiple_fields_reports_all(self):
        schema = make_silver_schema()
        del schema["_cnpj"]
        del schema["_nome"]
        del schema["_uid"]
        errors = validate_silver_schema(schema)
        assert len(errors) == 3

    def test_extra_domain_fields_are_allowed(self):
        """Campos de negócio adicionais não devem invalidar o contrato."""
        schema = make_silver_schema({"VL_BC_PIS": "double", "ALIQ_PIS": "double"})
        errors = validate_silver_schema(schema)
        assert errors == []


class TestMetadataFieldsSubset:
    """Testa cada campo de metadados individualmente."""

    @pytest.mark.parametrize("field", sorted(SilverOutputContract.METADATA_FIELDS))
    def test_each_metadata_field_is_required(self, field):
        schema = make_silver_schema()
        del schema[field]
        errors = validate_silver_schema(schema)
        assert any(field in e for e in errors), \
            f"Remoção de '{field}' deveria gerar erro de contrato"

    @pytest.mark.parametrize("field", sorted(SilverOutputContract.HIERARCHY_FIELDS))
    def test_each_hierarchy_field_is_required(self, field):
        schema = make_silver_schema()
        del schema[field]
        errors = validate_silver_schema(schema)
        assert any(field in e for e in errors)
