from __future__ import annotations

import pytest

from appfx.cosmosdb._base.repository_base import SortDirection, SortField
from appfx.cosmosdb.mongo.model import RootEntityBase
from appfx.cosmosdb.mongo.repository import RepositoryBase


class _Widget(RootEntityBase["_Widget", str]):
    name: str
    score: int = 0


class _WidgetRepository(RepositoryBase[_Widget, str]):
    pass


class _FakeAsyncCursor:
    def __init__(self, documents: list[dict]):
        self._documents = documents
        self.sort_spec = None

    def sort(self, sort_spec):
        self.sort_spec = sort_spec
        return self

    def __aiter__(self):
        self._iterator = iter(self._documents)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _FakeInsertResult:
    acknowledged = True


class _FakeCollection:
    def __init__(self, documents: list[dict] | None = None):
        self.documents = documents or []
        self.find_calls = []
        self.find_one_calls = []
        self.inserted_documents = []
        self.updated_calls = []
        self.deleted_calls = []
        self.last_cursor: _FakeAsyncCursor | None = None

    def find(self, predicate=None, projection=None):
        self.find_calls.append({"predicate": predicate, "projection": projection})
        self.last_cursor = _FakeAsyncCursor(self.documents)
        return self.last_cursor

    async def find_one(self, predicate, projection=None):
        self.find_one_calls.append({"predicate": predicate, "projection": projection})
        return self.documents[0] if self.documents else None

    async def insert_one(self, document):
        self.inserted_documents.append(document)
        return _FakeInsertResult()

    async def update_one(self, predicate, update):
        self.updated_calls.append({"predicate": predicate, "update": update})

    async def delete_one(self, predicate):
        self.deleted_calls.append(predicate)


def _ready_repo(collection: _FakeCollection | None = None) -> _WidgetRepository:
    repo = _WidgetRepository(
        connection_string="mongodb://unit-test",
        database_name="unit",
        collection_name="widgets",
    )
    repo.collection = collection or _FakeCollection()

    async def _already_ready():
        return None

    repo._ensure_collection_is_ready = _already_ready
    return repo


def test_empty_collection_name_is_derived_from_entity_type() -> None:
    repo = _WidgetRepository(
        connection_string="mongodb://unit-test",
        database_name="unit",
        collection_name="",
    )

    assert repo.collection_name == "_WidgetCollection"


@pytest.mark.asyncio
async def test_find_async_applies_filter_projection_and_sort_fields() -> None:
    collection = _FakeCollection(
        documents=[
            {"id": "w1", "name": "Alpha", "score": 10},
            {"id": "w2", "name": "Beta", "score": 5},
        ]
    )
    repo = _ready_repo(collection)

    results = await repo.find_async(
        {"score": {"$gte": 5}},
        sort_fields=[
            SortField("score", SortDirection.DESCENDING),
            SortField("name", SortDirection.ASCENDING),
        ],
    )

    assert [entity.id for entity in results] == ["w1", "w2"]
    assert collection.find_calls == [
        {
            "predicate": {"score": {"$gte": 5}},
            "projection": {"_id": False},
        }
    ]
    assert collection.last_cursor is not None
    assert collection.last_cursor.sort_spec == [("score", -1), ("name", 1)]


@pytest.mark.asyncio
async def test_get_async_uses_id_filter_and_converts_document() -> None:
    collection = _FakeCollection(documents=[{"id": "w1", "name": "Alpha"}])
    repo = _ready_repo(collection)

    result = await repo.get_async("w1")

    assert result == _Widget(id="w1", name="Alpha")
    assert collection.find_one_calls == [
        {"predicate": {"id": "w1"}, "projection": {"_id": False}}
    ]


@pytest.mark.asyncio
async def test_add_update_and_delete_delegate_to_collection_with_cosmos_document() -> (
    None
):
    collection = _FakeCollection()
    repo = _ready_repo(collection)
    entity = _Widget(id="w1", name="Alpha", score=10)

    await repo.add_async(entity)
    await repo.update_async(entity, predicate={"tenant": "t1"})
    await repo.delete_async("w1", predicate={"tenant": "t1"})

    assert collection.inserted_documents == [{"id": "w1", "name": "Alpha", "score": 10}]
    assert collection.updated_calls == [
        {
            "predicate": {"id": "w1", "tenant": "t1"},
            "update": {
                "$set": {"id": "w1", "name": "Alpha", "score": 10},
            },
        }
    ]
    assert collection.deleted_calls == [{"id": "w1", "tenant": "t1"}]
