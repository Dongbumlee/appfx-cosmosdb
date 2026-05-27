"""Cosmos DB MongoDB API entity models.

Defines abstract base classes for MongoDB API entities, including conversion
to the document dict format expected by the MongoDB driver.
"""

from abc import ABC
from typing import Any, Generic, TypeVar

from .._base.model_base import EntityBase as BaseEntityBase

TKey = TypeVar("TKey")
TEntity = TypeVar("TEntity", bound=BaseEntityBase)


class EntityBase(BaseEntityBase, ABC):
    """MongoDB API base for embedded/nested entities.

    Responsibilities:
        1. Provide a MongoDB-specific type anchor for nested entity validation.
        2. Inherit all Pydantic configuration from the shared base.

    Use this for entities that are always embedded within a root document
    (e.g. ``Address``, ``ContactInfo``).
    """

    pass


class RootEntityBase(EntityBase, ABC, Generic[TEntity, TKey]):  # noqa: UP046
    """MongoDB API base for independently-stored (root) entities.

    Responsibilities:
        1. Require an ``id`` field on every root document.
        2. Convert entities to the MongoDB document dict via ``to_cosmos_dict``.

    Attributes:
        id: Primary key whose type is determined by the ``TKey`` type parameter.
    """

    id: TKey

    def to_cosmos_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for MongoDB storage.

        Returns:
            Dictionary with all model fields using JSON-compatible names.
        """
        return self.model_dump(by_alias=True, exclude_none=False, mode="json")
