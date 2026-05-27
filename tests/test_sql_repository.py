from __future__ import annotations

import asyncio

import pytest

import appfx.cosmosdb.sql.repository as sql_repository_module
from appfx.cosmosdb._base.repository_base import SortDirection, SortField
from appfx.cosmosdb.sql.model import RootEntityBase
from appfx.cosmosdb.sql.repository import RepositoryBase


class _Order(RootEntityBase["_Order", str]):
    status: str
    amount: int = 0


class _OrderRepository(RepositoryBase[_Order, str]):
    pass


class _FakeAsyncQuery:
    def __init__(self, items: list):
        self._items = items

    def __aiter__(self):
        self._iterator = iter(self._items)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _FakeContainer:
    def __init__(self, query_items: list | None = None, read_item: dict | None = None):
        self.query_items_result = query_items or []
        self.read_item_result = read_item
        self.query_calls = []
        self.read_calls = []
        self.created_documents = []
        self.replaced_documents = []
        self.deleted_documents = []

    def query_items(self, **kwargs):
        self.query_calls.append(kwargs)
        return _FakeAsyncQuery(self.query_items_result)

    async def read_item(self, **kwargs):
        self.read_calls.append(kwargs)
        return self.read_item_result

    async def create_item(self, **kwargs):
        self.created_documents.append(kwargs["body"])

    async def replace_item(self, **kwargs):
        self.replaced_documents.append(kwargs)

    async def delete_item(self, **kwargs):
        self.deleted_documents.append(kwargs)


class _FakeClient:
    def __init__(self) -> None:
        self.close_calls = 0
        self.logging_enable = False

    async def close(self) -> None:
        self.close_calls += 1


class _FakeCredential:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


def _ready_repo(container: _FakeContainer) -> _OrderRepository:
    repo = _OrderRepository(
        connection_string="AccountEndpoint=https://unit.test/;AccountKey=fake;",
        database_name="unit",
        container_name="orders",
    )
    repo._container = container

    async def _already_initialized():
        return None

    repo._ensure_initialized = _already_initialized
    return repo


@pytest.mark.asyncio
async def test_close_closes_repository_owned_managed_identity_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = _FakeClient()
    fake_credential = _FakeCredential()

    monkeypatch.setattr(
        sql_repository_module,
        "DefaultAzureCredential",
        lambda: fake_credential,
    )
    monkeypatch.setattr(
        sql_repository_module,
        "AsyncCosmosClient",
        lambda **_kwargs: fake_client,
    )

    repo = _OrderRepository(
        account_url="https://unit.test.documents.azure.com:443/",
        database_name="unit",
        container_name="orders",
    )

    await repo._initialize_client()
    await repo.close()

    assert fake_client.close_calls == 1
    assert fake_credential.close_calls == 1
    assert repo._client is None
    assert repo._database is None
    assert repo._container is None
    assert repo._credential is None
    assert not repo._is_initialized.is_set()


@pytest.mark.asyncio
async def test_concurrent_first_use_initializes_only_once() -> None:
    class _ConcurrentRepo(_OrderRepository):
        def __init__(self) -> None:
            super().__init__(
                connection_string="AccountEndpoint=https://unit.test/;AccountKey=fake;",
                database_name="unit",
                container_name="orders",
            )
            self.initialize_calls = 0
            self.ensure_container_calls = 0

        async def _initialize_client(self) -> None:
            self.initialize_calls += 1
            await asyncio.sleep(0)
            self._client = _FakeClient()

        async def _ensure_database_and_container_exist(self) -> None:
            self.ensure_container_calls += 1
            await asyncio.sleep(0)
            self._database = object()
            self._container = _FakeContainer()

    repo = _ConcurrentRepo()

    await asyncio.gather(*(repo._ensure_initialized() for _ in range(10)))

    assert repo.initialize_calls == 1
    assert repo.ensure_container_calls == 1
    assert repo._is_initialized.is_set()


def test_use_managed_identity_false_prefers_connection_string() -> None:
    repo = _OrderRepository(
        connection_string="AccountEndpoint=https://unit.test/;AccountKey=fake;",
        account_url="https://unit.test.documents.azure.com:443/",
        database_name="unit",
        container_name="orders",
        use_managed_identity=False,
    )

    assert repo.use_managed_identity is False
    assert (
        repo.connection_string == "AccountEndpoint=https://unit.test/;AccountKey=fake;"
    )


def test_use_managed_identity_false_requires_connection_string_with_account_url() -> (
    None
):
    with pytest.raises(ValueError, match="connection_string is required"):
        _OrderRepository(
            account_url="https://unit.test.documents.azure.com:443/",
            database_name="unit",
            container_name="orders",
            use_managed_identity=False,
        )


@pytest.mark.asyncio
async def test_find_async_uses_build_sql_query_parameters_and_partition_key() -> None:
    container = _FakeContainer(
        query_items=[
            {
                "id": "o1",
                "status": "open",
                "amount": 25,
                "_partitionKey": _Order.get_partition_key_from_id("o1"),
            }
        ]
    )
    repo = _ready_repo(container)

    results = await repo.find_async(
        {"status": "open"},
        sort_fields=[SortField("amount", SortDirection.DESCENDING)],
        partition_key="orders-open",
    )

    assert [(entity.id, entity.status, entity.amount) for entity in results] == [
        ("o1", "open", 25)
    ]
    assert len(container.query_calls) == 1
    call = container.query_calls[0]
    assert call["query"] == (
        "SELECT * FROM c WHERE c.status = @param0 ORDER BY c.amount DESC"
    )
    assert call["parameters"] == [{"name": "@param0", "value": "open"}]
    assert call["partition_key"] == "orders-open"


@pytest.mark.asyncio
async def test_get_async_derives_partition_key_and_converts_document() -> None:
    expected_partition_key = _Order.get_partition_key_from_id("o1")
    container = _FakeContainer(
        read_item={
            "id": "o1",
            "status": "open",
            "amount": 25,
            "_partitionKey": expected_partition_key,
        }
    )
    repo = _ready_repo(container)

    result = await repo.get_async("o1")

    assert result is not None
    assert (result.id, result.status, result.amount) == ("o1", "open", 25)
    assert container.read_calls == [
        {"item": "o1", "partition_key": expected_partition_key}
    ]


@pytest.mark.asyncio
async def test_add_update_and_delete_delegate_to_container() -> None:
    container = _FakeContainer()
    repo = _ready_repo(container)
    entity = _Order(id="o1", status="open", amount=25)
    expected_document = entity.to_cosmos_dict()

    await repo.add_async(entity)
    await repo.update_async(entity)
    await repo.delete_async("o1")

    assert container.created_documents == [expected_document]
    assert container.replaced_documents == [{"item": "o1", "body": expected_document}]
    assert container.deleted_documents == [
        {"item": "o1", "partition_key": _Order.get_partition_key_from_id("o1")}
    ]


@pytest.mark.asyncio
async def test_raw_query_helpers_return_entities_dynamic_rows_and_single_values() -> (
    None
):
    container = _FakeContainer(
        query_items=[
            {
                "id": "o1",
                "status": "open",
                "amount": 25,
                "_partitionKey": _Order.get_partition_key_from_id("o1"),
            }
        ]
    )
    repo = _ready_repo(container)

    entities = await repo.query_raw_async(
        "SELECT * FROM c WHERE c.amount > @amount",
        parameters={"@amount": 10},
        partition_key="orders-open",
    )

    container.query_items_result = [{"status": "open", "count": 1}]
    dynamic_rows = await repo.query_raw_dynamic_cursor_async("SELECT c.status FROM c")

    container.query_items_result = [42]
    count = await repo.query_raw_single_value_async("SELECT VALUE COUNT(1) FROM c")

    assert [(entity.id, entity.status, entity.amount) for entity in entities] == [
        ("o1", "open", 25)
    ]
    assert dynamic_rows == [{"status": "open", "count": 1}]
    assert count == 42
    assert container.query_calls[0] == {
        "query": "SELECT * FROM c WHERE c.amount > @amount",
        "parameters": [{"name": "@amount", "value": 10}],
        "partition_key": "orders-open",
    }
