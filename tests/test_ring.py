"""Tests for ConsistentHashRing — Phase 5."""
from __future__ import annotations

import pytest

from distributed.ring import ConsistentHashRing


NODES = ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]


def test_get_node_returns_a_known_node():
    ring = ConsistentHashRing(NODES)
    node = ring.get_node("some:key")
    assert node in NODES


def test_deterministic_routing():
    ring = ConsistentHashRing(NODES)
    node1 = ring.get_node("user:42")
    node2 = ring.get_node("user:42")
    assert node1 == node2


def test_distribution_roughly_even():
    """With 150 virtual nodes, distribution should be within 40% of ideal."""
    ring = ConsistentHashRing(NODES, virtual_nodes=150)
    keys = [f"key:{i}" for i in range(3000)]
    dist = ring.distribution(keys)
    total = sum(dist.values())
    expected = total / len(NODES)
    for node, count in dist.items():
        deviation = abs(count - expected) / expected
        assert deviation < 0.40, f"Node {node} got {count} keys (deviation={deviation:.0%})"


def test_get_replicas_returns_n_unique_nodes():
    ring = ConsistentHashRing(NODES)
    replicas = ring.get_replicas("some:key", n=2)
    assert len(replicas) == 2
    assert len(set(replicas)) == 2
    for r in replicas:
        assert r in NODES


def test_get_replicas_first_is_primary():
    ring = ConsistentHashRing(NODES)
    primary = ring.get_node("test:key")
    replicas = ring.get_replicas("test:key", n=2)
    assert replicas[0] == primary


def test_remove_node_reroutes():
    ring = ConsistentHashRing(NODES)
    original = ring.get_node("reroute:key")
    ring.remove_node(original)
    new_node = ring.get_node("reroute:key")
    assert new_node != original
    assert new_node in NODES


def test_add_node_back():
    ring = ConsistentHashRing(["a:1", "b:2"])
    ring.remove_node("a:1")
    ring.add_node("a:1")
    assert "a:1" in ring.nodes


def test_empty_ring_returns_none():
    ring = ConsistentHashRing([])
    assert ring.get_node("key") is None


def test_single_node():
    ring = ConsistentHashRing(["only:1"])
    assert ring.get_node("anything") == "only:1"


def test_stability_on_node_removal():
    """
    When one node is removed, keys that were on OTHER nodes should stay
    on the same nodes (consistent hashing property: only 1/N keys remapped).
    """
    ring = ConsistentHashRing(NODES, virtual_nodes=150)
    keys = [f"k:{i}" for i in range(1000)]

    before = {k: ring.get_node(k) for k in keys}
    ring.remove_node(NODES[0])
    after = {k: ring.get_node(k) for k in keys}

    # Keys previously on nodes 1 and 2 should be unchanged
    unchanged = sum(
        1 for k in keys
        if before[k] != NODES[0] and after[k] == before[k]
    )
    unchanged_ratio = unchanged / len(keys)
    # At least 60% of keys on surviving nodes should stay put
    assert unchanged_ratio > 0.60, f"Too many keys moved: only {unchanged_ratio:.0%} stable"
