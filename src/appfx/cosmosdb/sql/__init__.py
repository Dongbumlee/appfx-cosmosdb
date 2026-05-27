"""SQL API implementation for the appfx-cosmosdb library.

Re-exports the SQL-specific entity base classes and repository so that
consumers can import from ``appfx.cosmosdb.sql`` directly.
"""

from .._base.repository_base import SortDirection, SortField
from .model import EntityBase, RootEntityBase
from .repository import RepositoryBase

__all__ = [
    "EntityBase",
    "RepositoryBase",
    "RootEntityBase",
    "SortDirection",
    "SortField",
]
