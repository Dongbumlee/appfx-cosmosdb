# Hands-on guide: appfx-cosmosdb

This guide walks through the core workflow for modeling documents and creating
repositories with `appfx-cosmosdb`.

## 1. Install the API extra

Choose the Cosmos DB API used by your account:

```powershell
python -m pip install "appfx-cosmosdb[sql]"
python -m pip install "appfx-cosmosdb[mongo]"
```

Use `appfx-cosmosdb[all]` only when the application uses both APIs.

## 2. Model a document

The examples below use a family document similar to the Azure Cosmos DB tutorial
schema. Use API-specific imports; do not import entity bases from top-level
`appfx.cosmosdb`.

### MongoDB API model

```python
from enum import Enum

from appfx.cosmosdb.mongo import EntityBase, RootEntityBase
from pydantic import Field


class Gender(str, Enum):
    MALE = "male"
    FEMALE = "female"


class MongoParent(EntityBase):
    familyName: str
    givenName: str


class MongoChild(EntityBase):
    familyName: str
    givenName: str
    gender: Gender
    grade: int


class MongoAddress(EntityBase):
    state: str
    county: str
    city: str


class MongoFamily(RootEntityBase["MongoFamily", str]):
    id: str
    parents: list[MongoParent] = Field(default_factory=list)
    children: list[MongoChild] = Field(default_factory=list)
    address: MongoAddress | None = None
    isRegistered: bool = False
```

### SQL/Core API model

This SQL/Core API track uses a minimal family document shape with the SQL
namespace:

```python
from appfx.cosmosdb.sql import EntityBase, RootEntityBase
from pydantic import Field


class SqlAddress(EntityBase):
    state: str
    county: str
    city: str


class SqlFamily(RootEntityBase["SqlFamily", str]):
    id: str
    address: SqlAddress | None = None
    isRegistered: bool = False
    tags: list[str] = Field(default_factory=list)
```

SQL root entities compute `/_partitionKey` from `id` for storage.

## 3. Create a repository

### MongoDB API repository

```python
from appfx.cosmosdb.mongo import RepositoryBase, SortDirection, SortField


class MongoFamilyRepository(RepositoryBase[MongoFamily, str]):
    def __init__(self, connection_string: str, database_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            collection_name="families",
        )

    async def find_by_city(self, city: str) -> list[MongoFamily]:
        return await self.find_async({"address.city": city})

    async def find_registered(self) -> list[MongoFamily]:
        return await self.find_async(
            {"isRegistered": True},
            sort_fields=[SortField("address.city", SortDirection.ASCENDING)],
        )
```

### SQL/Core API repository

```python
from appfx.cosmosdb.sql import RepositoryBase


class SqlFamilyRepository(RepositoryBase[SqlFamily, str]):
    def __init__(self, connection_string: str, database_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            container_name="families",
        )

    async def find_by_city(self, city: str) -> list[SqlFamily]:
        return await self.find_async({"address.city": city})

    async def count_registered(self) -> int:
        return await self.query_raw_single_value_async(
            "SELECT VALUE COUNT(1) FROM c WHERE c.isRegistered = true"
        )
```

## 4. Run CRUD operations

Supply connection strings from your local environment or secret store. Do not
hard-code credentials in examples, tests, or committed files.
The examples below keep MongoDB and SQL/Core API usage separate so each snippet
uses the correct model and repository names.

### MongoDB API CRUD

```python
import os


async def run_mongo_demo() -> None:
    connection_string = os.environ["COSMOSDB_CONNECTION_STRING"]

    async with MongoFamilyRepository(connection_string, "tutorial") as repo:
        family = MongoFamily(
            id="WakefieldFamily",
            parents=[MongoParent(familyName="Wakefield", givenName="Robin")],
            address=MongoAddress(state="NY", county="Manhattan", city="NY"),
            isRegistered=False,
        )

        await repo.add_async(family)

        found = await repo.get_async("WakefieldFamily")
        registered = await repo.find_async({"isRegistered": True})
        total = await repo.count_async({})

        if found is not None:
            found.isRegistered = True
            await repo.update_async(found)

        await repo.delete_async("WakefieldFamily")
```

### SQL/Core API CRUD

```python
import os


async def run_sql_demo() -> None:
    connection_string = os.environ["COSMOSDB_CONNECTION_STRING"]

    async with SqlFamilyRepository(connection_string, "tutorial") as repo:
        family = SqlFamily(
            id="WakefieldFamily",
            address=SqlAddress(state="NY", county="Manhattan", city="NY"),
            isRegistered=False,
        )

        await repo.add_async(family)

        found = await repo.get_async("WakefieldFamily")
        registered = await repo.find_async({"isRegistered": True})
        total = await repo.count_async({})

        if found is not None:
            found.isRegistered = True
            await repo.update_async(found)

        await repo.delete_async("WakefieldFamily")
```

## 5. Use predicates and raw queries

Predicate dictionaries cover common query cases. Keep the repository variable
and predicate aligned with the API track and model shape.

MongoDB API example:

```python
mongo_families = await mongo_repo.find_async({
    "address.city": "Seattle",
    "children.grade": {"$gte": 1, "$lte": 12},
})
```

MongoDB repositories support MongoDB-style operators such as `$regex`,
`$elemMatch`, `$all`, and `$exists`.

SQL/Core API example:

```python
sql_families = await sql_repo.find_async({
    "address.city": "Seattle",
    "tags": {"$in": ["registered"]},
})
```

SQL repositories support common predicate operators and also provide raw SQL
methods for advanced queries:

```python
results = await repo.query_raw_dynamic_cursor_async(
    """
    SELECT c.address.state AS state, COUNT(1) AS total
    FROM c
    GROUP BY c.address.state
    """
)
```

## 6. Test safely

Run the default unit tests locally:

```powershell
python -m pytest
```

Live Cosmos DB tests should be opt-in. Configure them separately with
API-specific accounts, environment variables or CI secrets, and appropriate
network/RBAC access. Keep credentials out of documentation and source control.

## Provenance

This draft was adapted from earlier Cosmos DB helper hands-on documentation
under the MIT License and updated for the `appfx-cosmosdb` package identity.
