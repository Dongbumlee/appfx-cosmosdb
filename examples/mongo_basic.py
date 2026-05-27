import asyncio
import os
from uuid import uuid4

from pydantic import Field

from appfx.cosmosdb.mongo import (
    RepositoryBase,
    RootEntityBase,
    SortDirection,
    SortField,
)


class Customer(RootEntityBase["Customer", str]):
    name: str
    email: str
    tags: list[str] = Field(default_factory=list)
    status: str = "active"


class CustomerRepository(RepositoryBase[Customer, str]):
    def __init__(
        self, connection_string: str, database_name: str, collection_name: str
    ):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            collection_name=collection_name,
            indexes=["status", "name"],
        )


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value

    raise SystemExit(f"Missing required environment variable: {name}")


async def run_example() -> None:
    connection_string = require_env("COSMOS_MONGO_CONNECTION_STRING")
    database_name = require_env("COSMOS_MONGO_DATABASE")
    collection_name = require_env("COSMOS_MONGO_COLLECTION")

    customer_id = f"customer-{uuid4()}"
    customer = Customer(
        id=customer_id,
        name="Ada Lovelace",
        email="ada@example.invalid",
        tags=["example", "premium"],
    )

    async with CustomerRepository(
        connection_string,
        database_name,
        collection_name,
    ) as repo:
        await repo.add_async(customer)

        found = await repo.get_async(customer_id)
        print(f"Created: {found.name if found else customer_id}")

        premium_customers = await repo.find_async(
            {"tags": {"$in": ["premium"]}, "status": "active"},
            sort_fields=[SortField("name", SortDirection.ASCENDING)],
        )
        print(f"Premium customers found: {len(premium_customers)}")

        customer.status = "inactive"
        await repo.update_async(customer)
        print("Updated customer status to inactive")

        await repo.delete_async(customer_id)
        print("Deleted example customer")


if __name__ == "__main__":
    asyncio.run(run_example())
