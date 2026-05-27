/# appfx-cosmosdb migration plan

## Goal

Package the existing CosmosDB helper from:

```text
H:\Works\best practices\Python CosmosDB Helper
```

into a new component package under:

```text
h:\Works\best practices\components\cosmosdb
```

The new PyPI distribution should be:

```text
appfx-cosmosdb
```

The public Python namespaces should be:

```python
appfx.cosmosdb.sql
appfx.cosmosdb.mongo
```

## Recommended package model

Use one PyPI distribution with two explicit API namespaces:

- `appfx.cosmosdb.sql` for Azure Cosmos DB SQL/Core API helpers.
- `appfx.cosmosdb.mongo` for Azure Cosmos DB MongoDB API helpers.

Do not publish separate PyPI packages for SQL and Mongo initially. A single distribution keeps versioning, documentation, CI, and publishing simpler while still allowing clean import separation.

## Target structure

```text
components/cosmosdb/
├── pyproject.toml
├── README.md
├── LICENSE
├── CHANGELOG.md
├── .gitignore
├── .env.example
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── publish.yml
├── docs/
│   └── index.md
├── examples/
├── scripts/
├── src/
│   └── appfx/
│       └── cosmosdb/
│           ├── __init__.py
│           ├── py.typed
│           ├── _base/
│           │   ├── __init__.py
│           │   ├── model_base.py
│           │   └── repository_base.py
│           ├── sql/
│           │   ├── __init__.py
│           │   ├── model.py
│           │   └── repository.py
│           └── mongo/
│               ├── __init__.py
│               ├── model.py
│               └── repository.py
└── tests/
```

## Source-to-target mapping

| Existing source | New target |
| --- | --- |
| `src/sas/cosmosdb/base` | `src/appfx/cosmosdb/_base` |
| `src/sas/cosmosdb/sql` | `src/appfx/cosmosdb/sql` |
| `src/sas/cosmosdb/mongo` | `src/appfx/cosmosdb/mongo` |
| `src/sas/cosmosdb/__init__.py` | `src/appfx/cosmosdb/__init__.py` |

Keep `_base` internal-facing. Public consumers should import from `appfx.cosmosdb.sql` or `appfx.cosmosdb.mongo`.

## Public API direction

### SQL API

```python
from appfx.cosmosdb.sql import RootEntityBase, RepositoryBase
```

### MongoDB API

```python
from appfx.cosmosdb.mongo import RootEntityBase, RepositoryBase
```

### Top-level package

Keep `appfx.cosmosdb` minimal:

```python
__version__ = "0.1.0"
```

Avoid defaulting top-level `RootEntityBase` or `RepositoryBase` to SQL. The old package aliases SQL at the top level for backward compatibility, but the new `appfx` package should make SQL vs Mongo explicit.

## `pyproject.toml` recommendation

Use:

```toml
[project]
name = "appfx-cosmosdb"
version = "0.1.0"
description = "Python helpers for Azure Cosmos DB SQL and MongoDB APIs."
readme = "README.md"
requires-python = ">=3.12"
license = "MIT"
authors = [
  { name = "DB Lee", email = "dongbum@outlook.com" },
]
```

Start with `requires-python = ">=3.12"` because the source helper currently declares Python 3.12+. Lower this only after compatibility testing.

Use dependency extras to avoid forcing SQL users to install Mongo dependencies and vice versa:

```toml
[project]
dependencies = [
  "pydantic>=2.11.5",
]

[project.optional-dependencies]
sql = [
  "azure-cosmos>=4.9.0",
  "azure-identity>=1.22.0",
]
mongo = [
  "pymongo>=4.13.2",
]
all = [
  "azure-cosmos>=4.9.0",
  "azure-identity>=1.22.0",
  "pymongo>=4.13.2",
]
dev = [
  "build>=1.2",
  "mypy>=1.9",
  "pytest>=8.4",
  "pytest-asyncio>=1.0",
  "pytest-cov>=6.2",
  "ruff>=0.5",
  "twine>=6.1",
]
```

Installation examples:

```powershell
python -m pip install "appfx-cosmosdb[sql]"
python -m pip install "appfx-cosmosdb[mongo]"
python -m pip install "appfx-cosmosdb[all]"
```

## Migration phases

### Phase 1 — Scaffold package

Create the new package scaffold in `components/cosmosdb`:

- `pyproject.toml`
- `README.md`
- `LICENSE`
- `CHANGELOG.md`
- `.gitignore`
- `.env.example`
- `.github/workflows/ci.yml`
- `.github/workflows/publish.yml`
- `src/appfx/cosmosdb/`
- `tests/`

Use the existing `components/configuration` package as the style reference for CI, Trusted Publishing, README structure, and repo hygiene.

### Phase 2 — Move source modules

Copy source modules from the existing helper and rewrite imports:

```text
sas.cosmosdb -> appfx.cosmosdb
sas.cosmosdb.base -> appfx.cosmosdb._base
```

Likely import rewrites:

```python
from ..base.repository_base import ...
```

to:

```python
from .._base.repository_base import ...
```

Preserve MIT license headers and existing provenance comments unless there is a deliberate license cleanup decision.

### Phase 3 — Port tests

Move all tests into `components/cosmosdb/tests`.

Current tests are split between:

```text
H:\Works\best practices\Python CosmosDB Helper\tests
H:\Works\best practices\Python CosmosDB Helper\test_*.py
```

Consolidate them under the target `tests/` folder and update imports from `sas.cosmosdb...` to `appfx.cosmosdb...`.

### Phase 4 — Update docs

Migrate and update:

- `README.md`
- `API_REFERENCE.md`
- `API_REFERENCE_SQL.md`
- `API_REFERENCE_MONGO.md`
- `HANDS_ON_GUIDE.md`

Replace old package references:

```text
sas-cosmosdb -> appfx-cosmosdb
sas.cosmosdb.sql -> appfx.cosmosdb.sql
sas.cosmosdb.mongo -> appfx.cosmosdb.mongo
```

### Phase 5 — Validate package

Run:

```powershell
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m build
python -m twine check dist/*
```

If tests require live Cosmos DB resources, separate them into unit vs integration groups before publishing.

### Phase 6 — Publish setup

Use PyPI Trusted Publishing like `appfx-configuration`.

Expected pending publisher fields:

| Field | Value |
| --- | --- |
| PyPI project name | `appfx-cosmosdb` |
| Owner | `Dongbumlee` |
| Repository name | `appfx-cosmosdb` or the actual GitHub repository name used for this package |
| Workflow name | `publish.yml` |
| Environment name | `pypi` |

## Important risks and decisions

### 1. License and ownership

The existing source files include Microsoft copyright headers. Before publishing under `appfx-cosmosdb`, confirm that the source license permits repackaging and preserve required notices.

### 2. Dependency scope

Do not carry over unnecessary dependencies blindly. Current source depends on both SQL and Mongo stacks. Prefer extras:

- `[sql]` for `azure-cosmos` / `azure-identity`
- `[mongo]` for `pymongo`
- `[all]` for both

### 3. Integration tests

Some tests may require live Cosmos DB or secrets. Keep unit tests default and mark live tests separately so CI can run without cloud credentials.

### 4. Top-level API

Avoid `appfx.cosmosdb.RootEntityBase` as an alias to SQL. Make users choose:

```python
appfx.cosmosdb.sql.RootEntityBase
appfx.cosmosdb.mongo.RootEntityBase
```

### 5. Backward compatibility

This is a new package identity. Do not promise drop-in replacement compatibility with `sas-cosmosdb` unless all imports and behaviors are intentionally preserved.

## Acceptance criteria

- `python -m pip install "appfx-cosmosdb[sql]"` supports `appfx.cosmosdb.sql`.
- `python -m pip install "appfx-cosmosdb[mongo]"` supports `appfx.cosmosdb.mongo`.
- Unit tests pass without live Azure credentials.
- Package builds successfully.
- `twine check dist/*` passes.
- README documents SQL and Mongo install/usage separately.
- No `.env`, `.venv`, build cache, or secrets are committed.
