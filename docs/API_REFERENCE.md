# appfx-cosmosdb API reference

This overview summarizes the public API shape. Use the API-specific references
for detailed examples:

- [SQL/Core API reference](API_REFERENCE_SQL.md)
- [MongoDB API reference](API_REFERENCE_MONGO.md)

## Installation

Install the extra that matches the Cosmos DB API your application uses.

For SQL/Core API repositories:

```powershell
python -m pip install "appfx-cosmosdb[sql]"
```

For MongoDB API repositories:

```powershell
python -m pip install "appfx-cosmosdb[mongo]"
```

For applications that use both SQL/Core API and MongoDB API repositories:

```powershell
python -m pip install "appfx-cosmosdb[all]"
```

## Public package structure

```text
appfx.cosmosdb
├── sql      # Azure Cosmos DB SQL/Core API helpers
└── mongo    # Azure Cosmos DB MongoDB API helpers
```

Use explicit API namespaces:

```python
from appfx.cosmosdb.sql import RootEntityBase, RepositoryBase
from appfx.cosmosdb.mongo import RootEntityBase, RepositoryBase
```

The top-level package is not documented as exporting `RootEntityBase` or
`RepositoryBase`.

## Common public types

Both API namespaces export:

| Type | Purpose |
| --- | --- |
| `EntityBase` | Base class for nested Pydantic models |
| `RootEntityBase[TEntity, TKey]` | Base class for independently stored documents |
| `RepositoryBase[TEntity, TKey]` | Async CRUD/query repository base |
| `SortField` | Field name and sort direction pair |
| `SortDirection` | `ASCENDING` or `DESCENDING` |

Always parameterize root entities:

```python
class Customer(RootEntityBase["Customer", str]):
    name: str
```

## Shared repository operations

The SQL and Mongo repositories share the same core pattern:

```python
await repo.add_async(entity)
found = await repo.get_async("entity-id")
items = await repo.find_async({"is_active": True})
count = await repo.count_async({"is_active": True})
await repo.update_async(entity)
await repo.delete_async("entity-id")
deleted_count = await repo.delete_items_async({"is_active": False})
```

Use `async with` for resource cleanup:

```python
async with CustomerRepository(connection_string, "app") as repo:
    customers = await repo.find_async({"is_active": True})
```

## Sorting

Import sorting helpers from the same API namespace as the repository:

```python
from appfx.cosmosdb.sql import SortDirection, SortField

customers = await repo.find_async(
    {"is_active": True},
    sort_fields=[
        SortField("name", SortDirection.ASCENDING),
        SortField("created_at", SortDirection.DESCENDING),
    ],
)
```

## Choosing an API

Use the SQL/Core API when you need:

- Automatic `/_partitionKey` generation from entity ids
- Cosmos DB SQL projections, aggregates, or scalar queries
- Managed identity authentication support in Azure-hosted workloads

Use the MongoDB API when you need:

- MongoDB-style predicates, array operators, and regex support
- Collection-oriented document modeling
- Compatibility with existing MongoDB query patterns

## Testing

Default tests are unit tests and should not require live Cosmos DB resources.
Live Cosmos DB tests require explicit integration setup, credentials supplied
through the local environment or CI secrets, and API-specific account
configuration.

## Provenance

This draft was migrated from earlier Cosmos DB helper documentation under the
MIT License and updated for the `appfx-cosmosdb` package identity.
