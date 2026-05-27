"""
Thread safety tests for the appfx-cosmosdb library.

This module contains tests that verify the thread safety of various components
in multi-threaded and high-concurrency scenarios.
"""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pytest
except ImportError:
    pytest = None

from appfx.cosmosdb.mongo.model import RootEntityBase as MongoRootEntityBase
from appfx.cosmosdb.sql.model import RootEntityBase


class SqlTestEntity(RootEntityBase["SqlTestEntity", str]):
    """Test entity for SQL API thread safety tests."""

    id: str
    name: str
    value: int = 0


class MongoTestEntity(MongoRootEntityBase["MongoTestEntity", str]):
    """Test entity for MongoDB API thread safety tests."""

    id: str
    name: str
    value: int = 0


class TestThreadSafety:
    """Test suite for thread safety verification."""

    def test_sql_partition_key_thread_safety(self):
        """Test that SQL partition key computation is thread-safe."""
        # This test is marked as thread_safety in pytest.ini
        entity = SqlTestEntity(id="test123", name="Test Entity", value=42)

        results = []
        errors = []

        def worker():
            """Worker function that accesses partition key multiple times."""
            try:
                for _ in range(100):
                    pk = entity._partitionKey
                    results.append(pk)
                    # Small delay to increase chance of race condition
                    time.sleep(0.001)
            except Exception as e:
                errors.append(str(e))

        # Create multiple threads
        threads = []
        for _ in range(10):
            t = threading.Thread(target=worker)
            threads.append(t)

        # Start all threads
        start_time = time.time()
        for t in threads:
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()
        end_time = time.time()

        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 1000, f"Expected 1000 results, got {len(results)}"
        assert len(set(results)) == 1, "All partition keys should be identical"
        assert results[0] == "332", f"Expected partition key '332', got '{results[0]}'"

        print(f"Thread safety test completed in {end_time - start_time:.2f} seconds")

    def test_sql_entity_concurrent_creation(self):
        """Test concurrent creation of SQL entities."""
        results = []
        errors = []

        def create_entities(start_id: int, count: int):
            """Create multiple entities concurrently."""
            try:
                local_results = []
                for i in range(count):
                    entity = SqlTestEntity(
                        id=f"test_{start_id + i}",
                        name=f"Entity {start_id + i}",
                        value=i,
                    )
                    # Access partition key to trigger computation
                    pk = entity._partitionKey
                    local_results.append((entity.id, pk))
                results.extend(local_results)
            except Exception as e:
                errors.append(str(e))

        # Create entities across multiple threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(5):
                future = executor.submit(create_entities, i * 100, 100)
                futures.append(future)

            # Wait for all futures to complete
            for future in as_completed(futures):
                future.result()

        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 500, f"Expected 500 results, got {len(results)}"

        # Verify all entities have unique IDs and valid partition keys
        ids = [r[0] for r in results]
        partition_keys = [r[1] for r in results]

        assert len(set(ids)) == 500, "All entity IDs should be unique"
        assert all(isinstance(pk, str) for pk in partition_keys), (
            "All partition keys should be strings"
        )
        assert all(pk.isdigit() for pk in partition_keys), (
            "All partition keys should be numeric strings"
        )

    def test_mongo_entity_concurrent_creation(self):
        """Test concurrent creation of MongoDB entities."""
        results = []
        errors = []

        def create_entities(start_id: int, count: int):
            """Create multiple MongoDB entities concurrently."""
            try:
                local_results = []
                for i in range(count):
                    entity = MongoTestEntity(
                        id=f"mongo_test_{start_id + i}",
                        name=f"Mongo Entity {start_id + i}",
                        value=i,
                    )
                    # Convert to dict to trigger serialization
                    entity_dict = entity.to_cosmos_dict()
                    local_results.append((entity.id, entity_dict))
                results.extend(local_results)
            except Exception as e:
                errors.append(str(e))

        # Create entities across multiple threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(5):
                future = executor.submit(create_entities, i * 100, 100)
                futures.append(future)

            # Wait for all futures to complete
            for future in as_completed(futures):
                future.result()

        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 500, f"Expected 500 results, got {len(results)}"

        # Verify all entities have unique IDs and valid serialization
        ids = [r[0] for r in results]
        entity_dicts = [r[1] for r in results]

        assert len(set(ids)) == 500, "All entity IDs should be unique"
        assert all(isinstance(d, dict) for d in entity_dicts), (
            "All serialized entities should be dicts"
        )
        assert all("id" in d for d in entity_dicts), (
            "All entities should have 'id' field"
        )

    def test_partition_key_consistency_across_threads(self):
        """Test consistent partition keys across threads."""
        test_cases = [
            ("user1", "474"),
            ("user123", "462"),
            ("test_entity_456", "490"),
            ("very_long_entity_id_12345", "485"),
        ]

        for entity_id, expected_pk in test_cases:
            results = []
            errors = []

            def worker(
                entity_id=entity_id,
                results=results,
                errors=errors,
            ):
                """Worker that creates entity and gets partition key."""
                try:
                    entity = SqlTestEntity(id=entity_id, name="Test")
                    pk = entity._partitionKey
                    results.append(pk)
                except Exception as e:
                    errors.append(str(e))

            # Create multiple threads
            threads = []
            for _ in range(20):
                t = threading.Thread(target=worker)
                threads.append(t)

            # Start and wait for all threads
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # Verify results
            assert len(errors) == 0, f"Errors for entity_id '{entity_id}': {errors}"
            assert len(results) == 20, (
                f"Expected 20 results for '{entity_id}', got {len(results)}"
            )
            assert len(set(results)) == 1, (
                f"All partition keys should be identical for '{entity_id}'"
            )
            assert results[0] == expected_pk, (
                f"Expected '{expected_pk}' for '{entity_id}', got '{results[0]}'"
            )

    async def test_async_operations_thread_safety(self):
        """Test that async operations don't interfere with thread safety."""
        if pytest is None:
            print("Skipping async test - pytest not available")
            return
        results = []
        errors = []

        async def async_worker(worker_id: int):
            """Async worker that creates and processes entities."""
            try:
                local_results = []
                for i in range(50):
                    entity = SqlTestEntity(
                        id=f"async_{worker_id}_{i}",
                        name=f"Async Entity {worker_id}-{i}",
                        value=i,
                    )

                    # Simulate some async processing
                    await asyncio.sleep(0.001)

                    pk = entity._partitionKey
                    local_results.append((entity.id, pk))

                results.extend(local_results)
            except Exception as e:
                errors.append(str(e))

        # Run multiple async workers concurrently
        tasks = []
        for i in range(10):
            task = asyncio.create_task(async_worker(i))
            tasks.append(task)

        await asyncio.gather(*tasks)

        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 500, f"Expected 500 results, got {len(results)}"

        # Verify all entities have unique IDs and valid partition keys
        ids = [r[0] for r in results]
        partition_keys = [r[1] for r in results]

        assert len(set(ids)) == 500, "All entity IDs should be unique"
        assert all(isinstance(pk, str) for pk in partition_keys), (
            "All partition keys should be strings"
        )

    def test_high_concurrency_stress_test(self):
        """Stress test with high concurrency to verify robustness."""
        num_threads = 20
        operations_per_thread = 200

        results = []
        errors = []

        def stress_worker():
            """High-intensity worker function."""
            try:
                local_results = []
                for i in range(operations_per_thread):
                    # Create entity
                    entity = SqlTestEntity(
                        id=f"stress_{threading.current_thread().ident}_{i}",
                        name="Stress Test",
                    )

                    # Access partition key multiple times
                    pk1 = entity._partitionKey
                    pk2 = entity._partitionKey
                    pk3 = entity._partitionKey

                    # Verify consistency
                    assert pk1 == pk2 == pk3, (
                        f"Partition key inconsistency: {pk1}, {pk2}, {pk3}"
                    )

                    local_results.append(pk1)

                results.extend(local_results)
            except Exception as e:
                errors.append(str(e))

        # Run stress test
        start_time = time.time()
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=stress_worker)
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        end_time = time.time()

        # Verify results
        expected_results = num_threads * operations_per_thread
        assert len(errors) == 0, f"Errors during stress test: {errors}"
        assert len(results) == expected_results, (
            f"Expected {expected_results} results, got {len(results)}"
        )

        print(
            f"Stress test completed: {expected_results} operations across "
            f"{num_threads} threads in {end_time - start_time:.2f} seconds"
        )
        print(
            f"Operations per second: {expected_results / (end_time - start_time):.0f}"
        )


if __name__ == "__main__":
    # Run tests directly if executed as script
    test_suite = TestThreadSafety()

    print("Running thread safety tests...")

    print("1. Testing SQL partition key thread safety...")
    test_suite.test_sql_partition_key_thread_safety()

    print("2. Testing SQL entity concurrent creation...")
    test_suite.test_sql_entity_concurrent_creation()

    print("3. Testing MongoDB entity concurrent creation...")
    test_suite.test_mongo_entity_concurrent_creation()

    print("4. Testing partition key consistency...")
    test_suite.test_partition_key_consistency_across_threads()

    print("5. Running high concurrency stress test...")
    test_suite.test_high_concurrency_stress_test()

    print("✅ All thread safety tests passed!")
