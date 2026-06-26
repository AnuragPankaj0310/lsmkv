from distributed.ring import ConsistentHashRing
from distributed.routing import RequestRouter


def test_primary_owner():
    ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    router = RequestRouter(ring)

    owner = router.primary("user123")

    assert owner in {"node0", "node1", "node2"}


def test_same_key_same_owner():
    ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    router = RequestRouter(ring)

    assert router.primary("apple") == router.primary("apple")


def test_replicas_include_primary():
    ring = ConsistentHashRing(
        [
            "node0",
            "node1",
            "node2",
        ]
    )

    router = RequestRouter(ring)

    replicas = router.replicas("hello")

    assert len(replicas) >= 1
    assert replicas[0] == router.primary("hello")