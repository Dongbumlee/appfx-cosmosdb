import asyncio
import os
from uuid import uuid4

from appfx.cosmosdb.sql import (
    RepositoryBase,
    RootEntityBase,
    SortDirection,
    SortField,
)


class TaskItem(RootEntityBase["TaskItem", str]):
    title: str
    owner: str
    status: str = "open"
    priority: int = 0


class TaskRepository(RepositoryBase[TaskItem, str]):
    def __init__(self, connection_string: str, database_name: str, container_name: str):
        super().__init__(
            connection_string=connection_string,
            database_name=database_name,
            container_name=container_name,
        )


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value

    raise SystemExit(f"Missing required environment variable: {name}")


async def run_example() -> None:
    connection_string = require_env("COSMOS_SQL_CONNECTION_STRING")
    database_name = require_env("COSMOS_SQL_DATABASE")
    container_name = require_env("COSMOS_SQL_CONTAINER")

    task_id = f"task-{uuid4()}"
    task = TaskItem(
        id=task_id,
        title="Try appfx-cosmosdb SQL example",
        owner="example-user",
        priority=10,
    )

    async with TaskRepository(connection_string, database_name, container_name) as repo:
        await repo.add_async(task)

        found = await repo.get_async(task_id)
        print(f"Created: {found.title if found else task_id}")

        open_tasks = await repo.find_async(
            {"status": "open", "priority": {"$gte": 1}},
            sort_fields=[SortField("priority", SortDirection.DESCENDING)],
        )
        print(f"Open tasks found: {len(open_tasks)}")

        task.status = "done"
        await repo.update_async(task)
        print("Updated task status to done")

        await repo.delete_async(task_id)
        print("Deleted example task")


if __name__ == "__main__":
    asyncio.run(run_example())
