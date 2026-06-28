import pytest

from distributed.cluster_manager import ClusterManager


class DummyServer:
    def __init__(self):
        self.notifications = []

    async def on_cluster_changed(self, nodes):
        self.notifications.append(nodes)


@pytest.mark.asyncio
async def test_add_node_notifies_servers():
    manager = ClusterManager(
        [
            "node1",
            "node2",
            "node3",
        ]
    )

    s1 = DummyServer()
    s2 = DummyServer()

    manager.register("node1", s1)
    manager.register("node2", s2)

    await manager.add_node("node4")

    assert s1.notifications
    assert s2.notifications

    assert s1.notifications[-1] == [
        "node1",
        "node2",
        "node3",
        "node4",
    ]