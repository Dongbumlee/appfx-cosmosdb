"""Base Pydantic model shared by all Cosmos DB entity types.

Provides common Pydantic configuration (validation, serialization, enum
handling) that both the MongoDB and SQL API entity hierarchies inherit.
"""

from pydantic import BaseModel, ConfigDict


class EntityBase(BaseModel):
    """Pydantic base for all Cosmos DB entities.

    Responsibilities:
        1. Enforce consistent Pydantic v2 configuration across every entity.
        2. Serve as the common ancestor for both embedded and root entity types.

    Attributes:
        model_config: Pydantic v2 model configuration dict.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        use_enum_values=True,
        populate_by_name=True,
        extra="allow",
    )
