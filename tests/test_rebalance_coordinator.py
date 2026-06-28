import pytest

from storage.engine import StorageEngine

from distributed.rebalance import RebalancePlanner
from distributed.migration import MigrationPlanner
from distributed.migration_executor import MigrationExecutor
from distributed.rebalance_coordinator import RebalanceCoordinator
from distributed.ring import ConsistentHashRing


@pytest.mark.asyncio
async def test_rebalance_statistics(tmp_path):
    old_ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    new_ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
            "node3",
        ]
    )

    planner = MigrationPlanner(
        RebalancePlanner(
            old_ring,
            new_ring,
        )
    )

    source_engine = StorageEngine(str(tmp_path / "node1"))
    destination_engine = StorageEngine(str(tmp_path / "node4"))

    await source_engine.open()
    await destination_engine.open()

    source_executor = MigrationExecutor(source_engine)
    destination_executor = MigrationExecutor(destination_engine)

    coordinator = RebalanceCoordinator(
        planner,
    )

    keys = [
        f"key-{i}"
        for i in range(1000)
    ]

    for key in keys:
        await source_engine.set(
            key,
            key.encode(),
        )

    stats = await coordinator.rebalance(source_executor, destination_executor, keys)

    migrated_keys = planner.keys_to_move(keys)

    for key in migrated_keys:
        assert await destination_engine.get(key) == key.encode()

    assert stats["migrated"] > 0
    assert (
        stats["migrated"]
        + stats["unchanged"]
        == len(keys)
    )

    await source_engine.close()
    await destination_engine.close()