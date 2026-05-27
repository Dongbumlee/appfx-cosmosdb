from __future__ import annotations

import os
import uuid

import pytest

from appfx.cosmosdb.mongo import RepositoryBase as MongoRepositoryBase
from appfx.cosmosdb.mongo import RootEntityBase as MongoRootEntityBase
from appfx.cosmosdb.sql import RepositoryBase as SqlRepositoryBase
from appfx.cosmosdb.sql import RootEntityBase as SqlRootEntityBase
from appfx.cosmosdb.sql import SortDirection, SortField

SQL_ENV_VARS = (
    "COSMOS_SQL_ACCOUNT_URL",
    "COSMOS_SQL_DATABASE",
    "COSMOS_SQL_CONTAINER",
)
MONGO_ENV_VARS = (
    "COSMOS_MONGO_CONNECTION_STRING",
    "COSMOS_MONGO_DATABASE",
    "COSMOS_MONGO_COLLECTION",
)
LIVE_OPT_IN_ENV_VAR = "COSMOS_LIVE_TESTS"


class _LiveSqlItem(SqlRootEntityBase["_LiveSqlItem", str]):
    category: str
    name: str
    score: int
    status: str


class _LiveSqlItemRepository(SqlRepositoryBase[_LiveSqlItem, str]):
    pass


class _LiveMongoItem(MongoRootEntityBase["_LiveMongoItem", str]):
    category: str
    name: str
    score: int
    status: str


class _LiveMongoItemRepository(MongoRepositoryBase[_LiveMongoItem, str]):
    pass


def _missing_env_vars(env_var_names: tuple[str, ...]) -> list[str]:
    return [name for name in env_var_names if not os.environ.get(name)]


def _skip_if_missing_env(env_var_names: tuple[str, ...]) -> None:
    missing = _missing_env_vars(env_var_names)
    if missing:
        pytest.skip(f"Missing live Cosmos DB env vars: {', '.join(missing)}")


def _skip_if_live_tests_not_requested(request: pytest.FixtureRequest) -> None:
    mark_expression = request.config.option.markexpr
    if "live" not in mark_expression and os.environ.get(LIVE_OPT_IN_ENV_VAR) != "1":
        pytest.skip(f"Live Cosmos DB tests require -m live or {LIVE_OPT_IN_ENV_VAR}=1.")


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_sql_crud_query_update_delete(
    request: pytest.FixtureRequest,
) -> None:
    _skip_if_live_tests_not_requested(request)
    _skip_if_missing_env(SQL_ENV_VARS)

    item_id = f"live-sql-{uuid.uuid4()}"
    category = f"live-category-{uuid.uuid4()}"
    created = False
    operation_error: Exception | None = None
    repo = _LiveSqlItemRepository(
        account_url=os.environ["COSMOS_SQL_ACCOUNT_URL"],
        database_name=os.environ["COSMOS_SQL_DATABASE"],
        container_name=os.environ["COSMOS_SQL_CONTAINER"],
        use_managed_identity=True,
    )

    async with repo:
        try:
            item = _LiveSqlItem(
                id=item_id,
                category=category,
                name="sql-live-created",
                score=1,
                status="created",
            )
            await repo.add_async(item)
            created = True

            fetched = await repo.get_async(item_id)
            assert fetched is not None
            assert fetched.id == item_id
            assert fetched.status == "created"

            found = await repo.find_async(
                {"category": category, "status": "created"},
                sort_fields=[SortField("score", SortDirection.DESCENDING)],
                partition_key=_LiveSqlItem.get_partition_key_from_id(item_id),
            )
            assert [result.id for result in found] == [item_id]

            updated = _LiveSqlItem(
                id=item_id,
                category=category,
                name="sql-live-updated",
                score=2,
                status="updated",
            )
            await repo.update_async(updated)

            fetched_after_update = await repo.get_async(item_id)
            assert fetched_after_update is not None
            assert fetched_after_update.name == "sql-live-updated"
            assert fetched_after_update.score == 2
            assert fetched_after_update.status == "updated"

            await repo.delete_async(item_id)

            assert await repo.get_async(item_id) is None
            created = False
        except Exception as exc:
            operation_error = exc
            raise
        finally:
            if created:
                try:
                    await repo.delete_async(item_id)
                except Exception as cleanup_error:
                    cleanup_message = (
                        f"Live SQL cleanup failed for document id {item_id}: "
                        f"{cleanup_error}"
                    )
                    if operation_error is not None:
                        operation_error.add_note(cleanup_message)
                    else:
                        raise AssertionError(cleanup_message) from cleanup_error


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_mongo_crud_query_update_delete(
    request: pytest.FixtureRequest,
) -> None:
    _skip_if_live_tests_not_requested(request)
    _skip_if_missing_env(MONGO_ENV_VARS)

    item_id = f"live-mongo-{uuid.uuid4()}"
    category = f"live-category-{uuid.uuid4()}"
    created = False
    operation_error: Exception | None = None
    repo = _LiveMongoItemRepository(
        connection_string=os.environ["COSMOS_MONGO_CONNECTION_STRING"],
        database_name=os.environ["COSMOS_MONGO_DATABASE"],
        collection_name=os.environ["COSMOS_MONGO_COLLECTION"],
    )

    async with repo:
        try:
            item = _LiveMongoItem(
                id=item_id,
                category=category,
                name="mongo-live-created",
                score=1,
                status="created",
            )
            await repo.add_async(item)
            created = True

            fetched = await repo.get_async(item_id)
            assert fetched is not None
            assert fetched.id == item_id
            assert fetched.status == "created"

            found = await repo.find_async(
                {"category": category, "status": "created"},
                sort_fields=[SortField("score", SortDirection.ASCENDING)],
            )
            assert [result.id for result in found] == [item_id]

            updated = _LiveMongoItem(
                id=item_id,
                category=category,
                name="mongo-live-updated",
                score=2,
                status="updated",
            )
            await repo.update_async(updated)

            fetched_after_update = await repo.get_async(item_id)
            assert fetched_after_update is not None
            assert fetched_after_update.name == "mongo-live-updated"
            assert fetched_after_update.score == 2
            assert fetched_after_update.status == "updated"

            await repo.delete_async(item_id)

            assert await repo.get_async(item_id) is None
            created = False
        except Exception as exc:
            operation_error = exc
            raise
        finally:
            if created:
                try:
                    await repo.delete_async(item_id)
                except Exception as cleanup_error:
                    cleanup_message = (
                        f"Live Mongo cleanup failed for document id {item_id}: "
                        f"{cleanup_error}"
                    )
                    if operation_error is not None:
                        operation_error.add_note(cleanup_message)
                    else:
                        raise AssertionError(cleanup_message) from cleanup_error
