# appfx-cosmosdb examples

Standalone examples for the API-specific public namespaces:

| File | API | Required extra |
| --- | --- | --- |
| `sql_basic.py` | Cosmos DB SQL/Core API | `appfx-cosmosdb[sql]` |
| `mongo_basic.py` | Cosmos DB MongoDB API | `appfx-cosmosdb[mongo]` |

Install the package with the dependencies for the API you want to try:

```powershell
python -m pip install "appfx-cosmosdb[sql]"
python -m pip install "appfx-cosmosdb[mongo]"
```

The examples do not contain credentials and do not contact Cosmos DB unless you
run them. When run, they require live Cosmos DB resources and environment
variables for that API.

## SQL/Core API

```powershell
$env:COSMOS_SQL_CONNECTION_STRING = "<your SQL/Core API connection string>"
$env:COSMOS_SQL_DATABASE = "<database name>"
$env:COSMOS_SQL_CONTAINER = "<container name>"
python .\examples\sql_basic.py
```

## MongoDB API

```powershell
$env:COSMOS_MONGO_CONNECTION_STRING = "<your MongoDB API connection string>"
$env:COSMOS_MONGO_DATABASE = "<database name>"
$env:COSMOS_MONGO_COLLECTION = "<collection name>"
python .\examples\mongo_basic.py
```

Use placeholders only in source control. Set real values in your shell, secret
store, or deployment environment.
