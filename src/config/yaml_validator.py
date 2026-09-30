"""
dinamo/config/yaml_validator.py
Valida schemas YAML de relatórios antes do processamento.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
import yaml


# ─── Erros de validação ───────────────────────────────────────────────────────

class YAMLValidationError(Exception):
    """Lançado quando um schema YAML é inválido."""
    def __init__(self, report_key: str, errors: list[str]):
        self.report_key = report_key
        self.errors = errors
        lines = "\n  - ".join(errors)
        super().__init__(f"[{report_key}] Schema inválido:\n  - {lines}")


# ─── Estruturas de resultado ──────────────────────────────────────────────────

@dataclass
class ValidationResult:
    report_key: str
    errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def add(self, msg: str) -> None:
        self.errors.append(msg)


# ─── Validador principal ──────────────────────────────────────────────────────

class ReportConfigValidator:
    """
    Valida a estrutura de um config de relatório YAML.

    Uso:
        validator = ReportConfigValidator()
        validator.validate_or_raise("efd_contrib", config_dict)
    """

    REQUIRED_TOP_LEVEL = {"entities", "joins"}
    REQUIRED_ENTITY_FIELDS = {"register", "uid"}
    REQUIRED_JOIN_FIELDS = {"left", "right"}

    def validate(self, report_key: str, config: dict[str, Any]) -> ValidationResult:
        result = ValidationResult(report_key=report_key)

        self._check_top_level(config, result)
        if result.is_valid:
            self._check_entities(config.get("entities", {}), result)
            self._check_joins(config.get("joins", []), config.get("entities", {}), result)
            self._check_metadata_entity(config.get("entities", {}), result)

        return result

    def validate_or_raise(self, report_key: str, config: dict[str, Any]) -> None:
        result = self.validate(report_key, config)
        if not result.is_valid:
            raise YAMLValidationError(report_key, result.errors)

    # ── Checagens internas ────────────────────────────────────────────────────

    def _check_top_level(self, config: dict, result: ValidationResult) -> None:
        missing = self.REQUIRED_TOP_LEVEL - set(config.keys())
        for key in missing:
            result.add(f"Campo obrigatório ausente no topo: '{key}'")

    def _check_entities(self, entities: dict, result: ValidationResult) -> None:
        if not isinstance(entities, dict):
            result.add("'entities' deve ser um dicionário")
            return
        for name, entity in entities.items():
            for req in self.REQUIRED_ENTITY_FIELDS:
                if req not in entity:
                    result.add(f"Entidade '{name}': campo obrigatório '{req}' ausente")
            # uid FILE_ID é aceito como fallback
            uid = entity.get("uid", "")
            if not uid:
                result.add(f"Entidade '{name}': 'uid' não pode ser vazio")

    def _check_joins(self, joins: list, entities: dict, result: ValidationResult) -> None:
        if not isinstance(joins, list):
            result.add("'joins' deve ser uma lista")
            return
        declared_entities = set(entities.keys())
        for i, join in enumerate(joins):
            for req in self.REQUIRED_JOIN_FIELDS:
                if req not in join:
                    result.add(f"Join[{i}]: campo obrigatório '{req}' ausente")
            left = join.get("left")
            right = join.get("right")
            if left and left not in declared_entities:
                result.add(f"Join[{i}]: 'left' refere entidade não declarada '{left}'")
            if right and right not in declared_entities:
                result.add(f"Join[{i}]: 'right' refere entidade não declarada '{right}'")

    def _check_metadata_entity(self, entities: dict, result: ValidationResult) -> None:
        """Garante que o registro 0000 (metadados) está declarado."""
        has_0000 = any(
            e.get("register") == "0000"
            for e in entities.values()
        )
        if not has_0000:
            result.add("Nenhuma entidade com register='0000' declarada (metadados obrigatórios)")


# ─── Utilitário de carregamento ───────────────────────────────────────────────

def load_and_validate(yaml_path: str) -> dict[str, Any]:
    """Carrega um arquivo YAML e valida antes de retornar."""
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    report_key = yaml_path.split("/")[-1].replace(".yaml", "")
    validator = ReportConfigValidator()
    validator.validate_or_raise(report_key, config)
    return config
