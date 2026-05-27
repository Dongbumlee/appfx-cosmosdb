from importlib import import_module

import appfx.cosmosdb as cosmosdb


def test_package_imports() -> None:
    assert cosmosdb.__version__ == "0.1.0"


def test_namespace_packages_import() -> None:
    assert import_module("appfx.cosmosdb._base").__name__ == "appfx.cosmosdb._base"
    assert import_module("appfx.cosmosdb.sql").__name__ == "appfx.cosmosdb.sql"
    assert import_module("appfx.cosmosdb.mongo").__name__ == "appfx.cosmosdb.mongo"
