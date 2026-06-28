import asyncio
import pytest

from distributed.cluster_manager import ClusterManager


class DummyServer:
    def __init__(self):
        self.called = False

    async def on_cluster_changed(self, nodes):
        await asyncio.sleep(0.1)
        self.called = True


@pytest.mark.asyncio
async def test_concurrent_notifications():
    manager = ClusterManager(["n1"])

    servers = [DummyServer() for _ in range(10)]

    for i, server in enumerate(servers):
        manager.register(f"n{i}", server)

    await manager.add_node("n2")

    assert all(s.called for s in servers)