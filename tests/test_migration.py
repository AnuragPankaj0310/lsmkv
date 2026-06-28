from distributed.ring import ConsistentHashRing
from distributed.rebalance import RebalancePlanner
from distributed.migration import MigrationPlanner


def test_no_keys_move_when_ring_same():
    ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    planner = MigrationPlanner(
        RebalancePlanner(ring, ring)
    )

    keys = [
        f"key-{i}"
        for i in range(100)
    ]

    assert planner.keys_to_move(keys) == []


def test_some_keys_move():
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
        RebalancePlanner(old_ring, new_ring)
    )

    keys = [
        f"key-{i}"
        for i in range(1000)
    ]

    moved = planner.keys_to_move(keys)

    assert len(moved) > 0
    assert len(moved) < len(keys)