"""Abstract repository and sort helpers for Cosmos DB.

Defines the generic repository interface (CRUD + query) and ``SortField`` /
``SortDirection`` value objects used by both the MongoDB and SQL API
implementations.
"""

import enum
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, Generic, TypeVar, cast

from .model_base import EntityBase

TEntity = TypeVar("TEntity", bound=EntityBase)
TKey = TypeVar("TKey")


class SortDirection(enum.IntEnum):
    """Sort order for repository queries."""

    ASCENDING = 1
    DESCENDING = -1


class SortField:
    """A field name paired with a sort direction.

    Attributes:
        field_name: Dot-separated path to the field (e.g. ``address.city``).
        order: Sort direction (ascending or descending).
    """

    def __init__(self, field_name: str, order: SortDirection = SortDirection.ASCENDING):
        """Initialise a sort specification.

        Args:
            field_name: The name of the field to sort by.
            order: The sort direction (ascending or descending).
        """
        self.field_name = field_name
        self.order = order

    def __repr__(self) -> str:
        """Return a human-readable representation."""
        direction = (
            "ASCENDING" if self.order == SortDirection.ASCENDING else "DESCENDING"
        )
        return f"{self.field_name} ({direction})"


class RepositoryBase(ABC, Generic[TEntity, TKey]):  # noqa: UP046
    """Abstract async repository defining CRUD and query operations.

    Responsibilities:
        1. Declare the common data-access contract for both API back-ends.
        2. Provide shared entity/document conversion helpers.
    """

    @abstractmethod
    async def get_async(self, key: TKey) -> TEntity | None:
        """
        Retrieve an entity by its key.

        Args:
            key: The entity's key value.
        Returns:
            The entity if found, else None.
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def find_async(
        self,
        predicate: dict[str, Any],
        sort_fields: list[SortField] = [],  # noqa: B006
    ) -> list[TEntity]:
        """
        Find entities matching a predicate.

        Args:
            predicate: Query conditions
            sort_fields: Fields to sort by (optional)
        Returns:
            List of matching entities.
        Raises:
            ValueError: If sort_order is provided but sort_fields is empty
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def add_async(self, entity: TEntity) -> None:
        """
        Add a new entity.

        Args:
            entity: The entity to add.
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def update_async(self, entity: TEntity) -> None:
        """
        Update an existing entity.

        Args:
            entity: The entity to update.
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def delete_async(self, key: TKey) -> None:
        """
        Delete an entity by its key.

        Args:
            key: The entity's key value.
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def delete_items_async(self, predicate: dict[str, Any]) -> int:
        """
        Delete all entities matching the predicate.

        Args:
            predicate: Query conditions (same format as find_async).

        Returns:
            The number of entities deleted.

        Raises:
            ValueError: If the predicate is invalid.
            Exception: For any deletion errors.

        Notes:
            - Subclasses should implement batching for large deletes.
            - Partial failures should be logged and surfaced.
            - Returns 0 if no entities matched.
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def all_async(
        self, sort_fields: list[SortField] | None = None
    ) -> list[TEntity]:
        """Retrieve all entities.

        Args:
            sort_fields: Fields to sort by (optional)

        Raises:
            ValueError: If sort_fields is provided but sort_order is empty
        """
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def find_with_pagination_async(
        self,
        predicate: dict[str, Any],
        sort_fields: list[SortField] | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[TEntity]:
        """Find entities with pagination support."""
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def count_async(self, predicate: dict[str, Any] | None = None) -> int:
        """Count entities matching a predicate."""
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def find_one_async(self, predicate: dict[str, Any]) -> TEntity | None:
        """Find a single entity matching a predicate."""
        raise NotImplementedError("This method must be implemented in a subclass.")

    @abstractmethod
    async def exists_async(self, predicate: dict[str, Any]) -> bool:
        """Check if any entity exists matching a predicate."""
        raise NotImplementedError("This method must be implemented in a subclass.")

    # Helper methods for entity/document conversion
    def _entity_to_document(self, entity: TEntity) -> dict[str, Any]:
        """Convert entity to Cosmos DB document."""
        to_cosmos_dict = getattr(entity, "to_cosmos_dict", None)
        if not callable(to_cosmos_dict):
            raise TypeError(
                f"{type(entity).__name__} must define a callable to_cosmos_dict()"
            )
        document = to_cosmos_dict()
        if not isinstance(document, dict):
            raise TypeError(
                f"{type(entity).__name__}.to_cosmos_dict() must return a dict"
            )
        return cast(dict[str, Any], document)

    def _document_to_entity(self, document: dict[str, Any]) -> TEntity:
        """Convert Cosmos DB document to entity."""
        entity_type = self._entity_type()
        return entity_type(**document)

    def _entity_type(self) -> type[TEntity]:
        """Return the entity type argument declared by the concrete repository."""
        for base in getattr(type(self), "__orig_bases__", ()):
            args = getattr(base, "__args__", ())
            if args:
                return cast(type[TEntity], args[0])
        raise TypeError(
            f"{type(self).__name__} must specify RepositoryBase[EntityType, KeyType]"
        )

    async def _cursor_to_entities(
        self, cursor: AsyncIterator[dict[str, Any]]
    ) -> list[TEntity]:
        """Convert Cosmos DB cursor to list of entities using async iteration."""
        entities: list[TEntity] = []
        async for document in cursor:
            entities.append(self._document_to_entity(document))
        return entities
