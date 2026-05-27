"""Tests for appfx.cosmosdb.sql.model."""

from __future__ import annotations

import hashlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from pydantic import ValidationError

from appfx.cosmosdb.sql.model import EntityBase, RootEntityBase

# ── Helpers ─────────────────────────────────────────────────────────────────


class _SqlEmbedded(EntityBase):
    """Nested entity for SQL API (no id)."""

    label: str
    score: int = 0


class _SqlRoot(RootEntityBase["_SqlRoot", str]):
    """Root entity for SQL API tests."""

    name: str
    age: int = 0
    nested: _SqlEmbedded | None = None
    items: list[_SqlEmbedded] = []  # noqa: RUF012


class _IntIdRoot(RootEntityBase["_IntIdRoot", int]):
    """Root entity with int key."""

    title: str


# ── EntityBase ──────────────────────────────────────────────────────────────


class TestSqlEntityBase:
    """Verify SQL EntityBase inherits base config."""

    def test_construct_embedded(self):
        e = _SqlEmbedded(label="test", score=5)
        assert e.label == "test"
        assert e.score == 5

    def test_extra_fields_allowed(self):
        e = _SqlEmbedded(label="x", extra_field="y")
        assert e.extra_field == "y"


# ── RootEntityBase Construction ─────────────────────────────────────────────


class TestSqlRootEntityBaseConstruction:
    """Verify root entity construction and private attribute initialisation."""

    def test_construct_with_string_id(self):
        r = _SqlRoot(id="abc", name="Alice")
        assert r.id == "abc"
        assert r.name == "Alice"

    def test_construct_with_int_id(self):
        r = _IntIdRoot(id=99, title="T")
        assert r.id == 99

    def test_missing_id_raises(self):
        with pytest.raises(ValidationError):
            _SqlRoot(name="NoID")

    def test_private_attrs_initialised(self):
        """__init__ installs _partition_key_value=None and a Lock."""
        r = _SqlRoot(id="pk", name="Bob")
        assert r._partition_key_value is None
        lock = r._partition_key_lock
        assert hasattr(lock, "acquire") and hasattr(lock, "release")


# ── get_partition_key_from_id ───────────────────────────────────────────────


class TestGetPartitionKeyFromId:
    """Verify the SHA-256 → modulo partition key algorithm."""

    def test_returns_string(self):
        pk = _SqlRoot.get_partition_key_from_id("some-id")
        assert isinstance(pk, str)

    def test_deterministic(self):
        pk1 = _SqlRoot.get_partition_key_from_id("id-1")
        pk2 = _SqlRoot.get_partition_key_from_id("id-1")
        assert pk1 == pk2

    def test_different_ids_may_differ(self):
        pk_a = _SqlRoot.get_partition_key_from_id("aaa")
        pk_b = _SqlRoot.get_partition_key_from_id("bbb")
        # Not guaranteed to differ but highly likely with SHA-256
        # Just verify both are valid
        assert pk_a.isdigit()
        assert pk_b.isdigit()

    def test_zero_padded_to_range_length(self):
        """Default 1000 partitions → 3-digit zero-padded string."""
        pk = _SqlRoot.get_partition_key_from_id("test-id")
        assert len(pk) == 3

    def test_custom_partition_count(self):
        pk = _SqlRoot.get_partition_key_from_id("test-id", number_of_partitions=10)
        assert len(pk) == 1
        assert 0 <= int(pk) < 10

    def test_large_partition_count(self):
        pk = _SqlRoot.get_partition_key_from_id("test-id", number_of_partitions=100_000)
        assert len(pk) == 5
        assert 0 <= int(pk) < 100_000

    def test_manual_sha256_verification(self):
        """Cross-check the algorithm against manual SHA-256 computation."""
        test_id = "verify-me"
        hash_bytes = hashlib.sha256(test_id.encode("utf-8")).digest()
        int_val = int.from_bytes(hash_bytes[:4], byteorder="little", signed=False)
        expected = str(int_val % 1000).zfill(3)

        actual = _SqlRoot.get_partition_key_from_id(test_id)
        assert actual == expected

    def test_integer_id_converted_to_string(self):
        """int key is str()-ified before hashing."""
        pk = _IntIdRoot.get_partition_key_from_id(42)
        expected = _IntIdRoot.get_partition_key_from_id("42")
        assert pk == expected


# ── _partitionKey computed_field ────────────────────────────────────────────


class TestPartitionKeyComputedField:
    """Verify lazy, thread-safe partition key property."""

    def test_computed_on_access(self):
        r = _SqlRoot(id="lazy", name="Test")
        assert r._partition_key_value is None  # not yet computed
        pk = r._partitionKey
        assert pk == _SqlRoot.get_partition_key_from_id("lazy")
        assert r._partition_key_value == pk  # now cached

    def test_cached_after_first_access(self):
        r = _SqlRoot(id="cached", name="Test")
        pk1 = r._partitionKey
        pk2 = r._partitionKey
        assert pk1 == pk2

    def test_thread_safety(self):
        """Many threads accessing _partitionKey should all get same value."""
        r = _SqlRoot(id="threaded", name="TS")
        expected = _SqlRoot.get_partition_key_from_id("threaded")
        results = []

        def _read():
            results.append(r._partitionKey)

        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = [pool.submit(_read) for _ in range(100)]
            for f in futures:
                f.result()

        assert all(pk == expected for pk in results)


# ── to_cosmos_dict ──────────────────────────────────────────────────────────


class TestSqlToCosmosDict:
    """Verify to_cosmos_dict output for SQL API storage."""

    def test_includes_id(self):
        r = _SqlRoot(id="d1", name="Alice")
        d = r.to_cosmos_dict()
        assert d["id"] == "d1"

    def test_includes_partition_key(self):
        r = _SqlRoot(id="d2", name="Bob")
        d = r.to_cosmos_dict()
        assert "_partitionKey" in d
        assert d["_partitionKey"] == _SqlRoot.get_partition_key_from_id("d2")

    def test_excludes_private_attrs(self):
        """_partition_key_value and _partition_key_lock must NOT appear."""
        r = _SqlRoot(id="d3", name="Carol")
        d = r.to_cosmos_dict()
        assert "_partition_key_value" not in d
        assert "_partition_key_lock" not in d

    def test_none_fields_included(self):
        r = _SqlRoot(id="d4", name="Dave")
        d = r.to_cosmos_dict()
        assert "nested" in d
        assert d["nested"] is None

    def test_nested_entity_serialised(self):
        r = _SqlRoot(id="d5", name="Eve", nested=_SqlEmbedded(label="n", score=9))
        d = r.to_cosmos_dict()
        assert d["nested"]["label"] == "n"
        assert d["nested"]["score"] == 9

    def test_list_field_serialised(self):
        r = _SqlRoot(id="d6", name="Frank", items=[_SqlEmbedded(label="i1", score=1)])
        d = r.to_cosmos_dict()
        assert isinstance(d["items"], list)
        assert d["items"][0]["label"] == "i1"

    def test_round_trip(self):
        original = _SqlRoot(
            id="rt",
            name="Grace",
            age=40,
            nested=_SqlEmbedded(label="nest", score=3),
            items=[_SqlEmbedded(label="a"), _SqlEmbedded(label="b", score=7)],
        )
        d = original.to_cosmos_dict()
        restored = _SqlRoot(**d)
        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.nested.label == "nest"
        assert len(restored.items) == 2
