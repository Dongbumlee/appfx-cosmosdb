# appfx-cosmosdb MongoDB API reference

The Mongo namespace provides repository helpers for Azure Cosmos DB MongoDB API.

## Install and import

```powershell
python -m pip install "appfx-cosmosdb[mongo]"
```

```python
from appfx.cosmosdb.mongo import (
    EntityBase,
    RepositoryBase,
    RootEntityBase,
    SortDirection,
    SortField,
)
```

## Entities

Use `RootEntityBase[TEntity, TKey]` for independently stored documents and
`EntityBase` for nested documents.

```python
from appfx.cosmosdb.mongo import EntityBase, RootEntityBase
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

`to_cosmos_dict()` returns the JSON-compatible document representation used for
storage.

## Repository

Subclass the Mongo `RepositoryBase` for each aggregate root:

```python
from appfx.cosmosdb.mongo import RepositoryBase


class CustomerRepository(RepositoryBase[Customer, str]):
    def __init__(self, connection_string: str, database_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            collection_name="customers",
        )
```

Constructor options include:

| Parameter | Purpose |
| --- | --- |
| `connection_string` | Cosmos DB MongoDB API connection string |
| `database_name` | Database name |
| `collection_name` | Collection name |

## CRUD and query operations

```python
async with CustomerRepository(connection_string, "app") as repo:
    customer = Customer(id="customer-1", name="Ada", email="ada@example.com")

    await repo.add_async(customer)
    found = await repo.get_async("customer-1")

    premium = await repo.find_async(
        {"tags": {"$in": ["premium"]}},
        sort_fields=[SortField("name", SortDirection.ASCENDING)],
    )

    total = await repo.count_async({"tags": {"$in": ["premium"]}})
    await repo.update_async(customer)
    await repo.delete_async("customer-1")
```

Mongo predicates use MongoDB-style query dictionaries:

```python
customers = await repo.find_async({
    "age": {"$gte": 18, "$lt": 65},
    "address.city": "Seattle",
    "tags": {"$nin": ["blocked"]},
})
```

Commonly used operators include:

- Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
- Arrays: `$size`, `$elemMatch`, `$all`
- Logical: `$and`, `$or`, `$not`, `$nor`
- Text and pattern matching: `$regex`, `$text`
- Field checks: `$exists`, `$type`

## Bulk delete

`delete_items_async(predicate)` deletes all matching documents and returns the
number deleted. Validate the scope first with `count_async()`, especially for
broad predicates.

```python
count = await repo.count_async({"status": "inactive"})
if count:
    deleted = await repo.delete_items_async({"status": "inactive"})
```

## Query guidance

- Prefer indexed fields in predicates.
- Anchor regular expressions when possible to reduce collection scans.
- Use `$in` instead of long `$or` lists for matching one field against multiple
  values.
- Use `async with repo:` or `await repo.close()` to release the Mongo client.

## Testing notes

Default tests should be unit tests and should not require a live Cosmos DB
account. Live integration tests require explicit Cosmos DB MongoDB API setup and
connection settings supplied through the environment or CI secrets.

## Provenance

This draft was migrated from earlier Cosmos DB helper documentation under the
MIT License and updated for the `appfx.cosmosdb.mongo` namespace.
