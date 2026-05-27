"""Tests for appfx.cosmosdb._base.repository_base."""

from __future__ import annotations

import asyncio
from typing import Any

from appfx.cosmosdb._base.model_base import EntityBase
from appfx.cosmosdb._base.repository_base import (
    RepositoryBase,
    SortDirection,
    SortField,
)

# ── Helpers ─────────────────────────────────────────────────────────────────


class _DummyEntity(EntityBase):
    """Minimal entity with to_cosmos_dict for testing helpers."""

    name: str
    value: int = 0

    def to_cosmos_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=False, mode="json")


class _DummyRepository(RepositoryBase[_DummyEntity, str]):
    """Concrete RepositoryBase for testing non-abstract helper methods."""

    async def get_async(self, key):
        raise NotImplementedError

    async def find_async(self, predicate, sort_fields=[]):  # noqa: B006
        raise NotImplementedError

    async def add_async(self, entity):
        raise NotImplementedError

    async def update_async(self, entity):
        raise NotImplementedError

    async def delete_async(self, key):
        raise NotImplementedError

    async def delete_items_async(self, predicate):
        raise NotImplementedError

    async def all_async(self, sort_fields=None):
        raise NotImplementedError

    async def find_with_pagination_async(
        self, predicate, sort_fields=None, skip=0, limit=100
    ):
        raise NotImplementedError

    async def count_async(self, predicate=None):
        raise NotImplementedError

    async def find_one_async(self, predicate):
        raise NotImplementedError

    async def exists_async(self, predicate):
        raise NotImplementedError


# ── SortDirection ───────────────────────────────────────────────────────────


class TestSortDirection:
    """Verify SortDirection enum values."""

    def test_ascending_value(self):
        assert SortDirection.ASCENDING == 1

    def test_descending_value(self):
        assert SortDirection.DESCENDING == -1

    def test_is_int_enum(self):
        assert isinstance(SortDirection.ASCENDING, int)
        assert isinstance(SortDirection.DESCENDING, int)

    def test_membership(self):
        assert 1 in SortDirection.__members__.values()
        assert -1 in SortDirection.__members__.values()


# ── SortField ───────────────────────────────────────────────────────────────


class TestSortField:
    """Verify SortField construction and repr."""

    def test_default_order_is_ascending(self):
        sf = SortField("name")
        assert sf.field_name == "name"
        assert sf.order == SortDirection.ASCENDING

    def test_explicit_descending(self):
        sf = SortField("price", SortDirection.DESCENDING)
        assert sf.order == SortDirection.DESCENDING

    def test_repr_ascending(self):
        sf = SortField("age", SortDirection.ASCENDING)
        assert repr(sf) == "age (ASCENDING)"

    def test_repr_descending(self):
        sf = SortField("score", SortDirection.DESCENDING)
        assert repr(sf) == "score (DESCENDING)"

    def test_nested_field_name(self):
        sf = SortField("address.city")
        assert sf.field_name == "address.city"


# ── RepositoryBase._entity_to_document ──────────────────────────────────────


class TestEntityToDocument:
    """Verify _entity_to_document delegates to entity.to_cosmos_dict()."""

    def test_returns_dict_from_entity(self):
        repo = _DummyRepository()
        entity = _DummyEntity(name="Alice", value=42)
        doc = repo._entity_to_document(entity)
        assert doc == {"name": "Alice", "value": 42}

    def test_preserves_none_values(self):
        """to_cosmos_dict with exclude_none=False keeps None fields."""

        class _WithOptional(EntityBase):
            label: str | None = None

            def to_cosmos_dict(self):
                return self.model_dump(by_alias=True, exclude_none=False, mode="json")

        class _RepoOpt(RepositoryBase[_WithOptional, str]):
            async def get_async(self, key): ...
            async def find_async(self, predicate, sort_fields=[]): ...  # noqa: B006
            async def add_async(self, entity): ...
            async def update_async(self, entity): ...
            async def delete_async(self, key): ...
            async def delete_items_async(self, predicate): ...
            async def all_async(self, sort_fields=None): ...
            async def find_with_pagination_async(
                self, predicate, sort_fields=None, skip=0, limit=100
            ): ...
            async def count_async(self, predicate=None): ...
            async def find_one_async(self, predicate): ...
            async def exists_async(self, predicate): ...

        repo = _RepoOpt()
        entity = _WithOptional()
        doc = repo._entity_to_document(entity)
        assert doc["label"] is None


# ── RepositoryBase._document_to_entity ──────────────────────────────────────


class TestDocumentToEntity:
    """Verify _document_to_entity reconstructs entity from dict."""

    def test_round_trip(self):
        repo = _DummyRepository()
        entity = _DummyEntity(name="Bob", value=7)
        doc = repo._entity_to_document(entity)
        restored = repo._document_to_entity(doc)
        assert restored.name == "Bob"
        assert restored.value == 7

    def test_handles_extra_fields(self):
        """Extra keys are accepted because EntityBase allows extra='allow'."""
        repo = _DummyRepository()
        doc = {"name": "Carol", "value": 3, "bonus": True}
        restored = repo._document_to_entity(doc)
        assert restored.name == "Carol"
        assert restored.bonus is True


# ── RepositoryBase._cursor_to_entities ──────────────────────────────────────


class TestCursorToEntities:
    """Verify async cursor iteration."""

    def test_converts_cursor_to_list(self):
        async def _run():
            repo = _DummyRepository()

            async def _fake_cursor():
                for doc in [
                    {"name": "X", "value": 1},
                    {"name": "Y", "value": 2},
                ]:
                    yield doc

            result = await repo._cursor_to_entities(_fake_cursor())
            assert len(result) == 2
            assert result[0].name == "X"
            assert result[1].value == 2

        asyncio.run(_run())

    def test_empty_cursor_returns_empty_list(self):
        async def _run():
            repo = _DummyRepository()

            async def _empty_cursor():
                return
                yield  # pragma: no cover - makes this an async generator

            result = await repo._cursor_to_entities(_empty_cursor())
            assert result == []

        asyncio.run(_run())
