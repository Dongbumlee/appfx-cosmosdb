"""Cosmos DB MongoDB API repository implementation.

Provides async CRUD operations, automatic collection/index creation, and
throttle-resilient bulk deletion against Azure Cosmos DB's MongoDB API.
"""

import asyncio
import logging
from types import TracebackType
from typing import Any, Self, TypeVar

from pymongo import AsyncMongoClient
from pymongo.errors import WriteError

from .._base.repository_base import RepositoryBase as Repository_Base
from .._base.repository_base import SortField
from .model import RootEntityBase

TEntity = TypeVar("TEntity", bound=RootEntityBase[Any, Any])
TKey = TypeVar("TKey", bound=str)

logger = logging.getLogger(__name__)


class RepositoryBase(Repository_Base[TEntity, TKey]):
    """Async MongoDB repository with collection lifecycle management.

    Responsibilities:
        1. Lazy-initialise the MongoDB client, database, and collection.
        2. Provide CRUD, query, pagination, count, and bulk-delete operations.
        3. Handle Cosmos DB throttling (error 16500) with exponential back-off.

    Examples:
        Basic usage:

        ```python
        from appfx.cosmosdb.mongo import RootEntityBase, RepositoryBase

        class Customer(RootEntityBase):
            name: str
            email: str
            age: int
            status: str

        class CustomerRepository(RepositoryBase[Customer, str]):
            def __init__(self, connection_string: str):
                super().__init__(
                    connection_string=connection_string,
                    database_name="RetailDB",
                    collection_name="customers"
                )

        # Usage
        async def main():
            async with CustomerRepository("mongodb://...") as repo:
                # CRUD operations
                customer = Customer(
                    id="customer-123",
                    name="John Doe",
                    email="john@example.com",
                    age=30,
                    status="active"
                )
                await repo.add_async(customer)

                # Query with MongoDB operators
                active_customers = await repo.find_async({"status": "active"})
                adults = await repo.find_async({"age": {"$gte": 18}})

                # Advanced queries
                complex_query = await repo.find_async({
                    "$and": [
                        {"status": "active"},
                        {"age": {"$gte": 21}},
                        {"email": {"$regex": "@company.com$"}}
                    ]
                })
        ```

        Enterprise bulk operations:

        ```python
        # Bulk deletion with automatic throttling handling
        deleted_count = await repo.delete_items_async({
            "$and": [
                {"status": "inactive"},
                {"lastLogin": {"$lt": "2024-01-01"}}
            ]
        })

        # Large dataset handling (automatically uses batching)
        deleted_count = await repo.delete_items_async({
            "department": "legacy"  # Could be thousands of records
        })
        ```

        Context manager usage:

        ```python
        async with CustomerRepository(connection_string) as repo:
            # Repository automatically initialized and cleaned up
            customers = await repo.find_async({"status": "premium"})
            return customers
        ```
    """

    def __init__(
        self,
        connection_string: str,
        database_name: str,
        collection_name: str,
        indexes: list[str] | None = None,
    ):
        """
        Initialize the MongoDB repository.

        Args:
            connection_string: MongoDB connection string.
            database_name: Name of the database.
            collection_name: Name of the collection.
            indexes: List of field names to index (optional).

        Examples:
            Basic initialization:

            ```python
            repo = CustomerRepository(
                connection_string="mongodb://username:password@host:port/",
                database_name="RetailDB",
                collection_name="customers"
            )
            ```

            With custom indexes for performance:

            ```python
            repo = OrderRepository(
                connection_string="mongodb://...",
                database_name="OrderDB",
                collection_name="orders",
                indexes=["customerId", "orderDate", "status"]
            )
            ```

            Azure Cosmos DB MongoDB API:

            ```python
            # Cosmos DB connection string format
            cosmos_conn_str = (
                "mongodb://myaccount:password@myaccount.mongo.cosmos.azure.com:10255/"
                "?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000"
                "&appName=@myaccount@"
            )

            repo = ProductRepository(
                connection_string=cosmos_conn_str,
                database_name="Catalog",
                collection_name="products",
                indexes=["category", "price", "brand"]
            )
            ```

            Auto-generated collection name:

            ```python
            # If collection_name is None or empty, it's auto-generated
            # from the entity type name
            class UserRepository(RepositoryBase[User, str]):
                def __init__(self, connection_string: str):
                    super().__init__(
                        connection_string=connection_string,
                        database_name="UserDB",
                        collection_name=""  # Will become "UserCollection"
                    )
            ```

            Performance-optimized setup:

            ```python
            repo = AnalyticsRepository(
                connection_string=connection_string,
                database_name="Analytics",
                collection_name="events",
                indexes=[
                    "userId",           # User-based queries
                    "eventType",        # Event filtering
                    "timestamp",        # Time-based sorting
                    "userId,eventType"  # Compound index for user+event queries
                ]
            )
            ```
        """

        if collection_name is None or collection_name == "":
            entity_type = self._entity_type()
            collection_name = f"{entity_type.__name__}Collection"

        self.database_name = database_name
        self.collection_name = collection_name
        self.indexes = indexes or []
        self.connection_string = connection_string
        self._is_collection_ready = asyncio.Event()
        self._initialization_lock = asyncio.Lock()
        self.client: Any = None
        self.collection: Any = None

    async def _ensure_collection_is_ready(self) -> None:
        """Lazily create the client, collection, and indexes."""
        if not self._is_collection_ready.is_set():
            async with self._initialization_lock:
                if not self._is_collection_ready.is_set():
                    self.client = AsyncMongoClient(
                        self.connection_string,
                        maxPoolSize=100,
                        minPoolSize=10,
                        maxIdleTimeMS=30000,
                        waitQueueTimeoutMS=1000,
                    )
                    self.collection = self.client[self.database_name][
                        self.collection_name
                    ]
                    await self._create_collection()
                    await self._create_indexes()
                    self._is_collection_ready.set()

    async def _create_collection(self) -> None:
        """Create the collection if it does not exist."""
        if (
            self.collection_name
            not in await self.client[self.database_name].list_collection_names()
        ):
            await self.client[self.database_name].create_collection(
                self.collection_name
            )

    async def _create_indexes(self) -> None:
        """Create indexes for fields not already indexed."""
        existing_indexes = await self.collection.index_information()
        for index in self.indexes:
            if f"{index}_1" not in existing_indexes:
                await self.collection.create_index(index)

    async def get_async(self, key: TKey) -> TEntity | None:
        """
        Retrieve an entity by its key.

        Args:
            key: The entity's key value (typically the 'id' field).
        Returns:
            The entity if found, else None.
        """
        await self._ensure_collection_is_ready()
        document = await self.collection.find_one(
            {"id": key}, projection={"_id": False}
        )
        if document:
            return self._document_to_entity(document)
        return None

    async def find_async(
        self,
        predicate: dict[str, Any] | None = None,
        sort_fields: list[SortField] = [],  # noqa: B006
    ) -> list[TEntity]:
        """
        Find entities matching a predicate using MongoDB query syntax.

        Args:
            predicate: Query conditions (MongoDB filter dict).
            sort_fields: Fields to sort by (optional).

        Returns:
            List of matching entities.

        Examples:
            Simple equality queries:

            ```python
            # Find by exact field match
            active_users = await repo.find_async({"status": "active"})

            # Find by multiple conditions (implicit AND)
            premium_adults = await repo.find_async({
                "tier": "premium",
                "age": {"$gte": 18}
            })
            ```

            Range and comparison queries:

            ```python
            # Age ranges
            young_adults = await repo.find_async({
                "age": {"$gte": 18, "$lt": 30}
            })

            # Date ranges
            recent_orders = await repo.find_async({
                "orderDate": {"$gte": "2024-01-01"}
            })

            # Not equal
            non_test_users = await repo.find_async({
                "email": {"$ne": "test@example.com"}
            })
            ```

            Array and list operations:

            ```python
            # In list
            city_customers = await repo.find_async({
                "city": {"$in": ["Seattle", "Portland", "Vancouver"]}
            })

            # Not in list
            active_tiers = await repo.find_async({
                "status": {"$nin": ["banned", "suspended", "deleted"]}
            })

            # Array size
            users_with_friends = await repo.find_async({
                "friends": {"$size": {"$gt": 0}}
            })

            # Array element matching
            users_with_young_friends = await repo.find_async({
                "friends": {"$elemMatch": {"age": {"$lt": 25}}}
            })
            ```

            Text and pattern matching:

            ```python
            # Regex patterns
            temp_users = await repo.find_async({
                "username": {"$regex": "^temp_.*", "$options": "i"}
            })

            # Contains text (case-insensitive)
            urgent_orders = await repo.find_async({
                "notes": {"$regex": "urgent", "$options": "i"}
            })
            ```

            Complex logical operations:

            ```python
            # AND conditions
            qualified_customers = await repo.find_async({
                "$and": [
                    {"age": {"$gte": 21}},
                    {"status": "verified"},
                    {"balance": {"$gt": 1000}}
                ]
            })

            # OR conditions
            priority_customers = await repo.find_async({
                "$or": [
                    {"tier": "premium"},
                    {"balance": {"$gte": 10000}},
                    {"loyaltyPoints": {"$gte": 5000}}
                ]
            })

            # NOT conditions
            active_non_test = await repo.find_async({
                "$and": [
                    {"status": "active"},
                    {"$not": {"email": {"$regex": "test@"}}}
                ]
            })
            ```

            Field existence and type checking:

            ```python
            # Field exists
            verified_profiles = await repo.find_async({
                "verificationDate": {"$exists": True}
            })

            # Field doesn't exist
            incomplete_profiles = await repo.find_async({
                "profilePicture": {"$exists": False}
            })

            # Type checking
            numeric_ids = await repo.find_async({
                "customId": {"$type": "number"}
            })
            ```

            Nested field queries:

            ```python
            # Nested object fields
            seattle_customers = await repo.find_async({
                "address.city": "Seattle"
            })

            # Nested array elements
            corporate_contacts = await repo.find_async({
                "contacts.email": {"$regex": "@company.com$"}
            })
            ```

            Sorting and limiting:

            ```python
            from appfx.cosmosdb.mongo import SortField, SortDirection

            # Sort by multiple fields
            sorted_customers = await repo.find_async(
                {"status": "active"},
                sort_fields=[
                    SortField("tier", SortDirection.ASCENDING),
                    SortField("joinDate", SortDirection.DESCENDING)
                ]
            )

            # Advanced query with sorting
            top_customers = await repo.find_async(
                {
                    "$and": [
                        {"status": "active"},
                        {"tier": {"$in": ["gold", "platinum"]}},
                        {"balance": {"$gte": 1000}}
                    ]
                },
                sort_fields=[
                    SortField("balance", SortDirection.DESCENDING),
                    SortField("lastLogin", SortDirection.DESCENDING)
                ]
            )
            ```

            Empty and null value handling:

            ```python
            # Find all documents (no filter)
            all_customers = await repo.find_async({})

            # Find documents with null values
            customers_no_phone = await repo.find_async({
                "phoneNumber": None
            })

            # Find documents with empty arrays
            customers_no_orders = await repo.find_async({
                "orders": []
            })
            ```
        """
        await self._ensure_collection_is_ready()
        cursor = self.collection.find(predicate, projection={"_id": False})
        if sort_fields:
            sort_spec = [(field.field_name, field.order) for field in sort_fields]
            cursor = cursor.sort(sort_spec)
        return await self._cursor_to_entities(cursor)

    async def add_async(self, entity: TEntity) -> None:
        """
        Add a new entity.

        Args:
            entity: The entity to add.
        """
        await self._ensure_collection_is_ready()
        await self.collection.insert_one(entity.to_cosmos_dict())

    async def update_async(
        self, entity: TEntity, predicate: dict[str, Any] | None = None
    ) -> None:
        """
        Update an existing entity.

        Args:
            entity: The entity to update.
            predicate: Additional filter conditions (optional).
        """
        await self._ensure_collection_is_ready()
        await self.collection.update_one(
            {"id": entity.id, **(predicate or {})},
            {"$set": self._entity_to_document(entity)},
        )

    async def delete_async(
        self, key: TKey, predicate: dict[str, Any] | None = None
    ) -> None:
        """
        Delete an entity by its key.

        Args:
            key: The entity's key value.
            predicate: Additional filter conditions (optional).
        """
        await self._ensure_collection_is_ready()
        await self.collection.delete_one({"id": key, **(predicate or {})})

    async def delete_items_async(self, predicate: dict[str, Any] | None = None) -> int:
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
            - Implements retry logic for throttling (WriteError 16500).
            - Uses batching for large deletes to avoid overwhelming the database.
            - Partial failures are logged and handled gracefully.
            - Returns 0 if no entities matched.
        """
        await self._ensure_collection_is_ready()

        max_retries = 5
        base_delay = 1.0

        for attempt in range(max_retries):
            try:
                # Count matches first to avoid unexpected large deletions.
                count = await self.collection.count_documents(
                    {} if not predicate else predicate
                )
                logger.info(
                    f"Attempting to delete {count} documents matching "
                    f"predicate: {predicate}"
                )

                # If more than 100 documents, use batching
                if count > 100:
                    return await self._delete_in_batches(
                        {} if not predicate else predicate, count
                    )

                # For smaller deletions, use delete_many directly
                result = await self.collection.delete_many(
                    {} if not predicate else predicate
                )
                logger.info(f"Successfully deleted {result.deleted_count} documents")
                return int(result.deleted_count)

            except WriteError as e:
                details = e.details or {}
                error_code = details.get("code", 0)
                if (
                    error_code == 16500 and attempt < max_retries - 1
                ):  # Throttling error
                    retry_after_ms = details.get("RetryAfterMs", 1000)
                    delay = max(base_delay * (2**attempt), retry_after_ms / 1000.0)
                    logger.warning(
                        f"WriteError 16500 (throttling) on attempt "
                        f"{attempt + 1}, retrying after {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(
                        f"WriteError {error_code} after {attempt + 1} attempts: {e}"
                    )
                    raise
            except Exception as e:
                logger.error(f"Unexpected error during deletion: {e}")
                raise

        # If all retries exhausted
        raise Exception(
            f"Failed to delete documents after {max_retries} attempts due "
            f"to persistent throttling"
        )

    async def _delete_in_batches(
        self, predicate: dict[str, Any], total_count: int, batch_size: int = 50
    ) -> int:
        """
        Delete documents in batches to avoid overwhelming the database.

        Args:
            predicate: Query conditions for deletion
            total_count: Total number of documents to delete
            batch_size: Number of documents to delete per batch

        Returns:
            Total number of documents deleted
        """
        total_deleted = 0

        logger.info(
            f"Starting batched deletion of {total_count} documents "
            f"in batches of {batch_size}"
        )

        while True:
            # Find a batch of documents to delete
            documents = (
                await self.collection.find(predicate, {"_id": 1})
                .limit(batch_size)
                .to_list(length=batch_size)
            )

            if not documents:
                break

            # Extract IDs for deletion
            ids = [doc["_id"] for doc in documents]

            # Delete this batch
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = await self.collection.delete_many({"_id": {"$in": ids}})
                    total_deleted += result.deleted_count
                    logger.debug(
                        f"Deleted batch of {result.deleted_count} documents "
                        f"(total: {total_deleted})"
                    )
                    break
                except WriteError as e:
                    details = e.details or {}
                    error_code = details.get("code", 0)
                    # Check for throttling error
                    if error_code == 16500 and attempt < max_retries - 1:
                        retry_after_ms = details.get("RetryAfterMs", 1000)
                        delay = retry_after_ms / 1000.0
                        logger.warning(
                            f"Batch delete throttled, retrying after {delay:.2f}s"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(
                            f"Failed to delete batch after {attempt + 1} attempts: {e}"
                        )
                        raise

            # Small delay between batches to be gentle on the database
            await asyncio.sleep(0.1)

        logger.info(f"Completed batched deletion: {total_deleted} documents deleted")
        return total_deleted

    async def all_async(
        self,
        sort_fields: list[SortField] | None = None,
    ) -> list[TEntity]:
        """
        Retrieve all entities in the collection.

        Args:
            sort_fields: Fields to sort by (optional).
        Returns:
            List of all entities.
        """
        await self._ensure_collection_is_ready()
        cursor = self.collection.find(projection={"_id": False})
        if sort_fields:
            sort_spec = [(field.field_name, field.order) for field in sort_fields]
            cursor = cursor.sort(sort_spec)
        return await self._cursor_to_entities(cursor)

    async def find_with_pagination_async(
        self,
        predicate: dict[str, Any],
        sort_fields: list[SortField] | None = None,
        skip: int = 0,
        limit: int = 100,
        projection: dict[str, Any] | None = None,
    ) -> list[TEntity]:
        """
        Query the collection with server-side paging and optional projection.

        Args:
            predicate: Query conditions (MongoDB filter dict).
            sort_fields: Fields to sort by (optional).
            skip: Number of documents to skip.
            limit: Maximum number of documents to return.
            projection: Fields to include/exclude (optional).
        Returns:
            List of matching entities.
        """
        await self._ensure_collection_is_ready()
        cursor = (
            self.collection.find(predicate, projection)
            .skip(skip)
            .limit(limit)
            .sort(
                [(field.field_name, field.order) for field in sort_fields]
                if sort_fields
                else []
            )
        )
        return await self._cursor_to_entities(cursor)

    async def count_async(self, predicate: dict[str, Any] | None = None) -> int:
        """
        Count documents matching the predicate.

        Args:
            predicate: Query conditions (MongoDB filter dict).
        Returns:
            Number of matching documents.
        """
        if predicate is None:
            predicate = {}
        await self._ensure_collection_is_ready()
        return int(await self.collection.count_documents(predicate))

    async def find_one_async(self, predicate: dict[str, Any]) -> TEntity | None:
        """
        Find a single entity matching the predicate.

        Args:
            predicate: Query conditions (MongoDB filter dict).
        Returns:
            The entity if found, else None.
        """
        await self._ensure_collection_is_ready()
        document = await self.collection.find_one(predicate, projection={"_id": False})
        if document:
            return self._document_to_entity(document)
        return None

    async def exists_async(self, predicate: dict[str, Any]) -> bool:
        """
        Check if any document exists matching the predicate.

        Args:
            predicate: Query conditions (MongoDB filter dict).
        Returns:
            True if any document exists, else False.
        """
        await self._ensure_collection_is_ready()
        document = await self.collection.find_one(predicate, projection={"_id": False})
        return document is not None

    async def close(self) -> None:
        """Close the MongoDB client and clean up resources."""
        if hasattr(self, "client") and self.client:
            await self.client.close()
            logger.info("MongoDB client closed")

    async def __aenter__(self) -> Self:
        """Async context manager entry."""
        await self._ensure_collection_is_ready()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.close()
