# appfx-cosmosdb

`appfx-cosmosdb` provides type-safe, async repository helpers for Azure Cosmos DB
using either the SQL/Core API or the MongoDB API. The package keeps the two API
surfaces explicit so applications import only the implementation they use.

## Package identity

| Purpose | Name |
| --- | --- |
| Distribution | `appfx-cosmosdb` |
| SQL/Core API namespace | `appfx.cosmosdb.sql` |
| MongoDB API namespace | `appfx.cosmosdb.mongo` |

The top-level `appfx.cosmosdb` package is not the public home for repository or
entity classes. Import from `appfx.cosmosdb.sql` or `appfx.cosmosdb.mongo`
directly.

## Installation

Install only the dependencies for the Cosmos DB API you use:

```powershell
python -m pip install "appfx-cosmosdb[sql]"
python -m pip install "appfx-cosmosdb[mongo]"
```

Install both SQL and MongoDB dependencies when one application uses both APIs:

```powershell
python -m pip install "appfx-cosmosdb[all]"
```

For local development:

```powershell
python -m pip install --upgrade pip
python -m pip install -e ".[dev,all]"
```

## SQL/Core API quick start

```python
from appfx.cosmosdb.sql import RepositoryBase, RootEntityBase, SortDirection, SortField


class Customer(RootEntityBase["Customer", str]):
    name: str
    email: str
    is_active: bool = True


class CustomerRepository(RepositoryBase[Customer, str]):
    def __init__(self, connection_string: str, database_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            container_name="customers",
        )


async with CustomerRepository(connection_string, "app") as repo:
    await repo.add_async(Customer(id="customer-1", name="Ada", email="ada@example.com"))

    active_customers = await repo.find_async(
        {"is_active": True},
        sort_fields=[SortField("name", SortDirection.ASCENDING)],
    )
```

SQL entities automatically include a computed `/_partitionKey` value derived
from the entity id. Use raw SQL methods for projections, aggregations, and other
queries that are clearer in Cosmos DB SQL syntax. SQL predicate and `SortField`
names are validated as simple or dotted field paths; see the SQL/Core API
reference for details.

## MongoDB API quick start

```python
from appfx.cosmosdb.mongo import RepositoryBase, RootEntityBase, SortDirection, SortField
from pydantic import Field


class Customer(RootEntityBase["Customer", str]):
    name: str
    email: str
    tags: list[str] = Field(default_factory=list)


class CustomerRepository(RepositoryBase[Customer, str]):
    def __init__(self, connection_string: str, database_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            collection_name="customers",
        )


async with CustomerRepository(connection_string, "app") as repo:
    await repo.add_async(Customer(id="customer-1", name="Ada", email="ada@example.com"))

    premium_customers = await repo.find_async(
        {"tags": {"$in": ["premium"]}},
        sort_fields=[SortField("name", SortDirection.ASCENDING)],
    )
```

Mongo repositories pass predicate dictionaries through to MongoDB-style query
operators, including array and regular-expression operators supported by the
driver and Cosmos DB MongoDB API.

## Core concepts

- **Entity models** inherit from `RootEntityBase["EntityName", KeyType]` for
  independently stored documents and from `EntityBase` for nested models.
- **Repositories** inherit from the API-specific `RepositoryBase[TEntity, TKey]`
  and centralize CRUD, query, count, pagination, and bulk-delete operations.
- **Sort helpers** are public from both API namespaces:
  `SortField` and `SortDirection`.
- **Resource cleanup** should use `async with repo:` or `await repo.close()`.

## Documentation

- [Documentation index](docs/index.md)
- [API overview](docs/API_REFERENCE.md)
- [SQL/Core API reference](docs/API_REFERENCE_SQL.md)
- [MongoDB API reference](docs/API_REFERENCE_MONGO.md)
- [Hands-on guide](docs/HANDS_ON_GUIDE.md)
- [Standalone examples](examples/README.md)

## Testing

The default test suite is unit-test focused:

```powershell
python -m pytest
```

Live Cosmos DB tests are marked `live` and skip unless explicitly requested and
their required environment variables are present. They are opt-in because they
use real Azure resources, credentials, and network access. Do not put account
keys, connection strings, tenant IDs, subscription IDs, or private endpoints in
source-controlled files.

For SQL/Core API live tests, sign in with a credential supported by
`DefaultAzureCredential` (for example, `az login`) and grant the identity access
to the target Cosmos DB account and existing test container. Then set:

```powershell
$env:COSMOS_SQL_ACCOUNT_URL = "https://<account>.documents.azure.com:443/"
$env:COSMOS_SQL_DATABASE = "<database-name>"
$env:COSMOS_SQL_CONTAINER = "<container-name>"
```

For MongoDB API live tests, set the connection string only in your shell or
secret store, never in committed files:

```powershell
$env:COSMOS_MONGO_CONNECTION_STRING = "<mongodb-connection-string>"
$env:COSMOS_MONGO_DATABASE = "<database-name>"
$env:COSMOS_MONGO_COLLECTION = "<collection-name>"
```

Run the live tests explicitly:

```powershell
python -m pytest -m live
```

As an alternative explicit opt-in for targeted runs, set
`COSMOS_LIVE_TESTS=1` in the current shell.

The default `python -m pytest` command remains safe for local development and CI
when these variables are not set.

## Development validation

```powershell
python -m pytest
python -m pytest --cov
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m build
python -m twine check dist/*
```

## License and provenance

This package is licensed under the MIT License. Portions of the documentation
were adapted from earlier Cosmos DB helper documentation that was also MIT
licensed; obsolete package names, repository URLs, and legacy branding were
removed during the `appfx-cosmosdb` migration.