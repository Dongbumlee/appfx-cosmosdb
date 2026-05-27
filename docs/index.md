# appfx-cosmosdb documentation

`appfx-cosmosdb` is a Python package for building type-safe async repositories
for Azure Cosmos DB SQL/Core API and MongoDB API workloads.

## Start here

- [README](../README.md) - installation, quick starts, and development commands
- [API overview](API_REFERENCE.md) - public namespaces and shared concepts
- [SQL/Core API reference](API_REFERENCE_SQL.md) - SQL entity and repository usage
- [MongoDB API reference](API_REFERENCE_MONGO.md) - Mongo entity and repository usage
- [Hands-on guide](HANDS_ON_GUIDE.md) - step-by-step model and repository examples
- [Standalone examples](../examples/README.md) - runnable-looking examples using
  environment variables for live resources

## Public namespaces

Import from the API-specific namespace:

```python
from appfx.cosmosdb.sql import RootEntityBase, RepositoryBase
from appfx.cosmosdb.mongo import RootEntityBase, RepositoryBase
```

`SortField` and `SortDirection` are also exported from both
`appfx.cosmosdb.sql` and `appfx.cosmosdb.mongo`.

Do not import repository or entity classes from top-level `appfx.cosmosdb`.