"""Tests for appfx.cosmosdb._base.model_base (EntityBase Pydantic model)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from appfx.cosmosdb._base.model_base import EntityBase

# ── EntityBase Configuration ───────────────────────────────────────────────


class TestEntityBaseConfig:
    """Verify Pydantic model_config flags on EntityBase."""

    def test_arbitrary_types_allowed(self):
        assert EntityBase.model_config["arbitrary_types_allowed"] is True

    def test_validate_assignment_enabled(self):
        assert EntityBase.model_config["validate_assignment"] is True

    def test_use_enum_values_enabled(self):
        assert EntityBase.model_config["use_enum_values"] is True

    def test_populate_by_name_enabled(self):
        assert EntityBase.model_config["populate_by_name"] is True

    def test_extra_fields_allowed(self):
        assert EntityBase.model_config["extra"] == "allow"


# ── EntityBase Construction ────────────────────────────────────────────────


class _SimpleEntity(EntityBase):
    """Minimal concrete subclass for testing."""

    name: str
    value: int = 0
    optional_field: str | None = None


class TestEntityBaseConstruction:
    """Verify entity construction, defaults, and validation."""

    def test_construct_with_required_fields(self):
        entity = _SimpleEntity(name="Alice", value=10)
        assert entity.name == "Alice"
        assert entity.value == 10

    def test_defaults_applied(self):
        entity = _SimpleEntity(name="Bob")
        assert entity.value == 0
        assert entity.optional_field is None

    def test_extra_fields_accepted(self):
        entity = _SimpleEntity(name="Carol", extra_attr="surprise")
        assert entity.extra_attr == "surprise"

    def test_validate_assignment_rejects_wrong_type(self):
        entity = _SimpleEntity(name="Dave", value=5)
        with pytest.raises(ValidationError):
            entity.value = "not_an_int"


# ── EntityBase Serialization ──────────────────────────────────────────────


class TestEntityBaseSerialization:
    """Verify model_dump and model_validate round-trip."""

    def test_model_dump_excludes_none(self):
        entity = _SimpleEntity(name="Eve")
        dumped = entity.model_dump(exclude_none=True)
        assert "optional_field" not in dumped
        assert dumped["name"] == "Eve"

    def test_model_dump_includes_none_when_requested(self):
        entity = _SimpleEntity(name="Frank")
        dumped = entity.model_dump(exclude_none=False)
        assert "optional_field" in dumped
        assert dumped["optional_field"] is None

    def test_round_trip_via_model_validate(self):
        original = _SimpleEntity(name="Grace", value=42, optional_field="present")
        data = original.model_dump()
        restored = _SimpleEntity.model_validate(data)
        assert restored.name == original.name
        assert restored.value == original.value
        assert restored.optional_field == original.optional_field

    def test_json_schema_generation(self):
        schema = _SimpleEntity.model_json_schema()
        assert "properties" in schema
        assert "name" in schema["properties"]
        assert "value" in schema["properties"]
