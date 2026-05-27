"""Tests for appfx.cosmosdb.mongo.model (Mongo EntityBase and RootEntityBase)."""

from __future__ import annotations

import pytest
from pydantic import Field, ValidationError

from appfx.cosmosdb.mongo.model import EntityBase, RootEntityBase

# ── Helpers ─────────────────────────────────────────────────────────────────


class _Address(EntityBase):
    """Nested (embedded) entity — no id required."""

    street: str
    city: str
    zip_code: str | None = None


class _Tag(EntityBase):
    """Simple embedded entity."""

    label: str
    priority: int = 0


class _Person(RootEntityBase["_Person", str]):
    """Root entity stored as an independent MongoDB document."""

    name: str
    age: int = 0
    address: _Address | None = None
    tags: list[_Tag] = Field(default_factory=list)


class _IntKeyEntity(RootEntityBase["_IntKeyEntity", int]):
    """Root entity with integer key."""

    title: str


# ── EntityBase ──────────────────────────────────────────────────────────────


class TestMongoEntityBase:
    """Verify Mongo EntityBase inherits base config and works as embedded entity."""

    def test_construct_simple_embedded(self):
        addr = _Address(street="123 Main St", city="Seattle")
        assert addr.street == "123 Main St"
        assert addr.city == "Seattle"
        assert addr.zip_code is None

    def test_extra_fields_allowed(self):
        addr = _Address(street="A", city="B", country="US")
        assert addr.country == "US"

    def test_validation_rejects_wrong_type(self):
        tag = _Tag(label="urgent", priority=5)
        with pytest.raises(ValidationError):
            tag.priority = "not_a_number"


# ── RootEntityBase Construction ─────────────────────────────────────────────


class TestRootEntityBaseConstruction:
    """Verify root entity requires id and supports all field types."""

    def test_construct_with_string_id(self):
        p = _Person(id="abc-123", name="Alice", age=30)
        assert p.id == "abc-123"
        assert p.name == "Alice"
        assert p.age == 30

    def test_construct_with_int_id(self):
        e = _IntKeyEntity(id=42, title="Test")
        assert e.id == 42

    def test_missing_id_raises(self):
        with pytest.raises(ValidationError):
            _Person(name="No-ID")

    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            _Person(id="1")  # missing 'name'

    def test_nested_entity_construction(self):
        addr = _Address(street="1st Ave", city="Portland")
        p = _Person(id="p1", name="Bob", address=addr)
        assert p.address.city == "Portland"

    def test_list_of_embedded_entities(self):
        tags = [_Tag(label="a"), _Tag(label="b", priority=3)]
        p = _Person(id="p2", name="Carol", tags=tags)
        assert len(p.tags) == 2
        assert p.tags[1].priority == 3


# ── RootEntityBase.to_cosmos_dict ───────────────────────────────────────────


class TestToCosmosDict:
    """Verify to_cosmos_dict serialisation for MongoDB storage."""

    def test_simple_entity(self):
        p = _Person(id="x", name="Dave", age=25)
        d = p.to_cosmos_dict()
        assert d["id"] == "x"
        assert d["name"] == "Dave"
        assert d["age"] == 25

    def test_none_fields_included(self):
        """exclude_none=False means None values are serialised."""
        p = _Person(id="y", name="Eve")
        d = p.to_cosmos_dict()
        assert "address" in d
        assert d["address"] is None

    def test_nested_entity_serialised(self):
        addr = _Address(street="Pine", city="SF", zip_code="94102")
        p = _Person(id="z", name="Frank", address=addr)
        d = p.to_cosmos_dict()
        assert d["address"]["city"] == "SF"
        assert d["address"]["zip_code"] == "94102"

    def test_list_field_serialised(self):
        tags = [_Tag(label="x", priority=1)]
        p = _Person(id="t", name="Grace", tags=tags)
        d = p.to_cosmos_dict()
        assert isinstance(d["tags"], list)
        assert d["tags"][0]["label"] == "x"

    def test_returns_json_compatible_types(self):
        """mode='json' ensures all values are JSON-serialisable primitives."""
        p = _Person(id="j", name="Hank", age=0)
        d = p.to_cosmos_dict()
        # All values should be basic JSON types
        assert isinstance(d["id"], str)
        assert isinstance(d["age"], int)

    def test_round_trip(self):
        """Dict output can reconstruct the entity."""
        original = _Person(
            id="rt",
            name="Ivy",
            age=28,
            address=_Address(street="Oak", city="LA"),
            tags=[_Tag(label="vip", priority=10)],
        )
        d = original.to_cosmos_dict()
        restored = _Person(**d)
        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.address.city == original.address.city
        assert restored.tags[0].label == "vip"
