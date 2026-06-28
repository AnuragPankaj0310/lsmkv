from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from network.server import LsmkvServer


@pytest.fixture
def data_dirs(tmp_path: Path):
    """
    Creates isolated data directories for each node.
    """
    return [
        tmp_path / "node0",
        tmp_path / "node1",
        tmp_path / "node2",
    ]


@pytest.fixture
def cluster_nodes():
    """
    Addresses used by the test cluster.
    """
    return [
        "127.0.0.1:7101",
        "127.0.0.1:7102",
        "127.0.0.1:7103",
    ]


@pytest.fixture
async def running_cluster(data_dirs, cluster_nodes):
    """
    Starts a 3-node cluster for integration tests.
    """
    servers = []
    tasks = []

    for i, addr in enumerate(cluster_nodes):
        host, port = addr.split(":")

        replicas = [
            node
            for j, node in enumerate(cluster_nodes)
            if j != i
        ]

        server = LsmkvServer(
            host="127.0.0.1",
            port=int(port),
            metrics_port=9101 + i,
            data_dir=str(data_dirs[i]),
            replication_targets=replicas,
            cluster_nodes=cluster_nodes,
            node_address=addr,
            node_id=f"node-{i}",
        )

        task = asyncio.create_task(server.serve_forever())

        servers.append(server)
        tasks.append(task)

    #
    # Give the servers a moment to bind sockets.
    #
    await asyncio.sleep(1)

    yield servers

    #
    # Cleanup
    #
    for task in tasks:
        task.cancel()

    await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )
