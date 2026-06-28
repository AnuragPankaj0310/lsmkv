import asyncio
import contextlib

import pytest

from distributed.cluster_manager import ClusterManager
from network.server import LsmkvServer


@pytest.mark.asyncio
async def test_cluster_manager_triggers_rebalance(tmp_path):
    manager = ClusterManager(
        [
            "127.0.0.1:7401",
        ]
    )

    server = LsmkvServer(
        host="127.0.0.1",
        port=7401,
        metrics_port=9401,
        data_dir=str(tmp_path),
        cluster_nodes=[
            "127.0.0.1:7401",
        ],
        node_address="127.0.0.1:7401",
    )

    manager.register(
        "127.0.0.1:7401",
        server,
    )

    task = asyncio.create_task(server.serve_forever())

    try:
        await asyncio.sleep(1)

        await manager.add_node(
            "127.0.0.1:7402",
        )

        assert len(server._cluster_nodes) == 2

    finally:
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task