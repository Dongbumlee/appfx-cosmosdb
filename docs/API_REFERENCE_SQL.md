# appfx-cosmosdb SQL/Core API reference

The SQL namespace provides repository helpers for Azure Cosmos DB SQL/Core API.

## Install and import

```powershell
python -m pip install "appfx-cosmosdb[sql]"
```

```python
from appfx.cosmosdb.sql import (
    EntityBase,
    RepositoryBase,
    RootEntityBase,
    SortDirection,
    SortField,
)
```

## Entities

Use `RootEntityBase[TEntity, TKey]` for independently stored documents.
SQL root entities require an `id` and compute a deterministic `/_partitionKey`
from that id.

```python
from appfx.cosmosdb.sql import EntityBase, RootEntityBase
from pydantic import Field


class Address(EntityBase):
    city: str
    state: str


class Customer(RootEntityBase["Customer", str]):
    id: str
    name: str
    email: str
    address: Address | None = None
    tags: list[str] = Field(default_factory=list)
```

Convert an entity for storage with `to_cosmos_dict()` when needed:

```python
document = Customer(id="customer-1", name="Ada", email="ada@example.com").to_cosmos_dict()
```

## Repository

Subclass the SQL `RepositoryBase` for each aggregate root:

```python
from appfx.cosmosdb.sql import RepositoryBase


class CustomerRepository(RepositoryBase[Customer, str]):
    def __init__(self, connection_string: str, database_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            container_name="customers",
        )
```

Constructor options include:

| Parameter | Purpose |
| --- | --- |
| `connection_string` | Cosmos DB SQL/Core API connection string |
| `account_url` | Account endpoint for managed identity authentication |
| `database_name` | Database name |
| `container_name` | Container name |
| `partition_key_path` | Defaults to `/_partitionKey` |
| `throughput` | Provisioned RU/s; omit or use `None` for serverless accounts |
| `use_managed_identity` | Use Azure identity when `account_url` is supplied |

## CRUD and query operations

```python
async with CustomerRepository(connection_string, "app") as repo:
    customer = Customer(id="customer-1", name="Ada", email="ada@example.com")

    await repo.add_async(customer)
    found = await repo.get_async("customer-1")

    active = await repo.find_async(
        {"is_active": True},
        sort_fields=[SortField("name", SortDirection.ASCENDING)],
    )

    total = await repo.count_async({"is_active": True})
    await repo.update_async(customer)
    await repo.delete_async("customer-1")
```

Supported predicate operators include common comparison and logical operators
such as `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$and`, and
`$or`. SQL-specific text helpers include `$contains`, `$startswith`, and
`$endswith`.

Use dot notation for nested fields:

```python
customers = await repo.find_async({
    "address.city": "Seattle",
    "tags": {"$in": ["premium"]},
})
```

## Raw SQL operations

Use raw SQL methods when the predicate builder is not expressive enough:

```python
customers = await repo.query_raw_async(
    "SELECT * FROM c WHERE c.address.city = @city",
    {"@city": "Seattle"},
)

stats = await repo.query_raw_dynamic_cursor_async(
    """
    SELECT c.address.state AS state, COUNT(1) AS total
    FROM c
    GROUP BY c.address.state
    """
)

active_count = await repo.query_raw_single_value_async(
    "SELECT VALUE COUNT(1) FROM c WHERE c.is_active = true"
)
```

## Bulk delete

`delete_items_async(predicate)` deletes all matching documents and returns the
number deleted. The operation is irreversible; validate the scope first with
`count_async()`.

```python
count = await repo.count_async({"status": "inactive"})
if count:
    deleted = await repo.delete_items_async({"status": "inactive"})
```

## Operational notes

- Prefer `async with repo:` so clients are closed reliably.
- Monitor RU consumption for broad queries and bulk deletes.
- Live integration tests require explicit Cosmos DB SQL/Core API setup and
  should receive connection settings through environment variables or CI
  secrets, not committed files.

## Provenance

This draft was migrated from earlier Cosmos DB helper documentation under the
MIT License and updated for the `appfx.cosmosdb.sql` namespace.
