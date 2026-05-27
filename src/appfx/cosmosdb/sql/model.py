"""Cosmos DB SQL API entity models.

Defines abstract base classes for SQL API entities, including SHA-256-based
partition key derivation and thread-safe lazy caching of the computed key.
"""

import hashlib
import threading
from abc import ABC
from typing import Any, Generic, TypeVar

from pydantic import PrivateAttr, computed_field

from .._base.model_base import EntityBase as BaseEntityBase

TEntity = TypeVar("TEntity", bound=BaseEntityBase)
TKey = TypeVar("TKey")


class EntityBase(BaseEntityBase, ABC):
    """SQL API base for embedded/nested entities.

    Responsibilities:
        1. Provide a SQL API-specific type anchor for nested entity validation.
        2. Inherit all Pydantic configuration from the shared base.

    Use this for entities that are always embedded within a root document
    (e.g. ``Address``, ``ContactInfo``).
    """

    pass


class RootEntityBase(EntityBase, ABC, Generic[TEntity, TKey]):  # noqa: UP046
    """SQL API base for independently-stored (root) entities.

    Responsibilities:
        1. Require an ``id`` field on every root document.
        2. Compute a deterministic ``_partitionKey`` via SHA-256 hashing.
        3. Convert entities to a Cosmos DB-compatible dict via ``to_cosmos_dict``.

    Attributes:
        id: Primary key whose type is determined by the ``TKey`` type parameter.
    """

    id: TKey
    _partition_key_value: str | None = PrivateAttr(default=None)
    _partition_key_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def _partitionKey(self) -> str:
        """Computed partition key derived from the entity ID."""
        if self._partition_key_value is not None:
            return self._partition_key_value

        with self._partition_key_lock:
            if self._partition_key_value is not None:
                return self._partition_key_value

            self._partition_key_value = self.get_partition_key_from_id(self.id)
            return self._partition_key_value

    def to_cosmos_dict(self) -> dict[str, Any]:
        """Convert to a dictionary suitable for Cosmos DB SQL API storage.

        Returns:
            Dictionary with all model fields and the computed partition key.
        """
        data = self.model_dump(
            by_alias=True,
            mode="json",
            exclude_none=False,
            exclude={"_partition_key_value", "_partition_key_lock"},
        )

        if self._partitionKey:
            data["_partitionKey"] = self._partitionKey

        return data

    @staticmethod
    def get_partition_key_from_id(id: TKey, number_of_partitions: int = 1000) -> str:
        """Derive a partition key from an entity ID using SHA-256.

        Args:
            id: The entity's unique identifier.
            number_of_partitions: Logical partition count (default 1000).

        Returns:
            Zero-padded partition key string.
        """
        hash_bytes = hashlib.sha256(str(id).encode("utf-8")).digest()
        int_hashed_val = int.from_bytes(
            hash_bytes[:4], byteorder="little", signed=False
        )
        range_val = number_of_partitions - 1
        length = len(str(range_val))
        key = str(int_hashed_val % number_of_partitions)
        return key.zfill(length)
