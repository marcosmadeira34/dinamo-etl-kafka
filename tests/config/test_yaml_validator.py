"""
tests/config/test_yaml_validator.py
"""
import pytest
from src.config.yaml_validator import ReportConfigValidator, YAMLValidationError


# ─── Fixture base ─────────────────────────────────────────────────────────────

@pytest.fixture
def valid_config():
    return {
        "entities": {
            "header": {"register": "0000", "uid": "FILE_ID"},
            "contrib": {"register": "M200", "uid": "NUM_AJ"},
        },
        "joins": [
            {"left": "header", "right": "contrib", "on": "FILE_ID"},
        ],
    }


@pytest.fixture
def validator():
    return ReportConfigValidator()


# ─── Testes: config válido ────────────────────────────────────────────────────

class TestValidConfig:
    def test_valid_config_passes(self, validator, valid_config):
        result = validator.validate("efd_contrib", valid_config)
        assert result.is_valid

    def test_valid_config_no_errors(self, validator, valid_config):
        result = validator.validate("efd_contrib", valid_config)
        assert result.errors == []

    def test_validate_or_raise_does_not_raise(self, validator, valid_config):
        # não deve lançar
        validator.validate_or_raise("efd_contrib", valid_config)


# ─── Testes: campos obrigatórios ausentes ─────────────────────────────────────

class TestMissingTopLevel:
    def test_missing_entities(self, validator, valid_config):
        del valid_config["entities"]
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("entities" in e for e in result.errors)

    def test_missing_joins(self, validator, valid_config):
        del valid_config["joins"]
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("joins" in e for e in result.errors)

    def test_missing_both_raises(self, validator):
        result = validator.validate("test", {})
        assert not result.is_valid
        assert len(result.errors) == 2


# ─── Testes: entidades ────────────────────────────────────────────────────────

class TestEntityValidation:
    def test_entity_missing_uid(self, validator, valid_config):
        valid_config["entities"]["contrib"].pop("uid")
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("uid" in e for e in result.errors)

    def test_entity_missing_register(self, validator, valid_config):
        valid_config["entities"]["contrib"].pop("register")
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("register" in e for e in result.errors)

    def test_entity_empty_uid(self, validator, valid_config):
        valid_config["entities"]["contrib"]["uid"] = ""
        result = validator.validate("test", valid_config)
        assert not result.is_valid

    def test_no_0000_entity(self, validator, valid_config):
        # remove o registro 0000
        valid_config["entities"]["header"]["register"] = "0001"
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("0000" in e for e in result.errors)


# ─── Testes: joins ────────────────────────────────────────────────────────────

class TestJoinValidation:
    def test_join_missing_left(self, validator, valid_config):
        valid_config["joins"][0].pop("left")
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("left" in e for e in result.errors)

    def test_join_missing_right(self, validator, valid_config):
        valid_config["joins"][0].pop("right")
        result = validator.validate("test", valid_config)
        assert not result.is_valid

    def test_join_references_undeclared_entity(self, validator, valid_config):
        valid_config["joins"].append(
            {"left": "header", "right": "entidade_fantasma", "on": "FILE_ID"}
        )
        result = validator.validate("test", valid_config)
        assert not result.is_valid
        assert any("entidade_fantasma" in e for e in result.errors)


# ─── Testes: validate_or_raise ───────────────────────────────────────────────

class TestValidateOrRaise:
    def test_raises_yaml_validation_error(self, validator):
        with pytest.raises(YAMLValidationError) as exc_info:
            validator.validate_or_raise("meu_relatorio", {})
        assert exc_info.value.report_key == "meu_relatorio"
        assert len(exc_info.value.errors) > 0

    def test_error_message_contains_report_key(self, validator):
        with pytest.raises(YAMLValidationError) as exc_info:
            validator.validate_or_raise("efd_fiscal", {})
        assert "efd_fiscal" in str(exc_info.value)
