from distributed.ring import ConsistentHashRing
from distributed.rebalance import RebalancePlanner


def test_same_ring_no_migration():
    ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    planner = RebalancePlanner(ring, ring)

    assert planner.needs_migration("hello") is False


def test_node_join_changes_some_keys():
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

    planner = RebalancePlanner(old_ring, new_ring)

    moved = 0

    for i in range(1000):
        if planner.needs_migration(f"key-{i}"):
            moved += 1

    assert moved > 0
    assert moved < 1000


def test_old_new_owner():
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

    planner = RebalancePlanner(old_ring, new_ring)

    assert planner.old_owner("hello") in old_ring.nodes
    assert planner.new_owner("hello") in new_ring.nodes