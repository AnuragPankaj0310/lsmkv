from distributed.ring import ConsistentHashRing
from distributed.routing import RequestRouter
from distributed.shard_manager import ShardManager


def build_manager():
    ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    router = RequestRouter(ring)

    return ShardManager(router)


def test_primary_owner():
    manager = build_manager()

    owner = manager.primary("hello")

    assert owner in {
        "node0",
        "node1",
        "node2",
    }


def test_replicas():
    manager = build_manager()

    replicas = manager.replicas("hello")

    assert len(replicas) > 0


def test_owner_is_replica():
    manager = build_manager()

    owner = manager.primary("hello")

    assert manager.is_replica(owner, "hello")


def test_owns_key():
    manager = build_manager()

    owner = manager.primary("hello")

    assert manager.owns_key(owner, "hello")


def test_replication_factor():
    manager = build_manager()

    replicas = manager.replicas("hello")

    assert len(replicas) == 2