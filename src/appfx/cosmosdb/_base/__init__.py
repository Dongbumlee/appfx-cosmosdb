"""Base abstractions for the appfx-cosmosdb library.

Exposes the shared entity model, abstract repository, and sort helpers
used by both the MongoDB and SQL API implementations.
"""

from .model_base import EntityBase
from .repository_base import RepositoryBase, SortDirection, SortField

__all__ = ["EntityBase", "RepositoryBase", "SortDirection", "SortField"]
