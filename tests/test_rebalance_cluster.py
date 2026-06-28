import asyncio
import contextlib

import pytest

from network.server import LsmkvServer


@pytest.mark.asyncio
async def test_rebalance_cluster(tmp_path):
    """
    End-to-end rebalance test.

    Node1 migrates keys to Node2.
    """

    node1 = LsmkvServer(
        host="127.0.0.1",
        port=7301,
        metrics_port=9301,
        data_dir=str(tmp_path / "node1"),
        cluster_nodes=["127.0.0.1:7301"],
        node_address="127.0.0.1:7301",
    )

    node2 = LsmkvServer(
        host="127.0.0.1",
        port=7302,
        metrics_port=9302,
        data_dir=str(tmp_path / "node2"),
        cluster_nodes=[
            "127.0.0.1:7301",
            "127.0.0.1:7302",
        ],
        node_address="127.0.0.1:7302",
    )

    task1 = asyncio.create_task(node1.serve_forever())
    task2 = asyncio.create_task(node2.serve_forever())

    try:
        #
        # Give servers time to start.
        #
        await asyncio.sleep(1)

        #
        # Insert enough keys so that at least some migrate.
        #
        for i in range(500):
            await node1._engine.set(
                f"key-{i}",
                str(i).encode(),
            )

        migrations = await node1.keys_to_migrate(
            [
                "127.0.0.1:7301",
                "127.0.0.1:7302",
            ]
        )

        migrated = await node1.rebalance_cluster(
            [
                "127.0.0.1:7301",
                "127.0.0.1:7302",
            ]
        )

        for keys in migrations.values():
            for key in keys:
                assert await node1._engine.get(key) is None
                assert await node2._engine.get(key) is not None

        assert migrated > 0

        #
        # Verify at least one migrated key exists on Node2.
        #
        found = False

        for i in range(500):
            value = await node2._engine.get(f"key-{i}")

            if value is not None:
                found = True
                break

        assert found

    finally:
        task1.cancel()
        task2.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task1

        with contextlib.suppress(asyncio.CancelledError):
            await task2