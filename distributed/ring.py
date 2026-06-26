"""
Consistent Hash Ring — Phase 5.

Ring lives in the CLIENT SDK, not a server-side coordinator.
This eliminates the coordinator as a single point of failure and removes
one network hop from every request (exactly how Amazon Dynamo works).

Algorithm:
  - Each physical node gets `virtual_nodes` positions on the ring
  - Positions are MD5 hashes of "node_addr:vnode_index"
  - Ring is a sorted list of (hash_value, node_addr)
  - get_node(key): MD5(key) → binary search → next clockwise node
  - Adding/removing a node only remaps 1/N keys on average

Virtual nodes (150 by default) ensure uniform key distribution even with
heterogeneous hardware.
"""
from __future__ import annotations

import bisect
import hashlib
import threading
from typing import Optional


def _md5_int(text: str) -> int:
    return int(hashlib.md5(text.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    """
    MD5-based consistent hash ring with virtual nodes.

    Thread-safe: protected by a read-write lock (threading.Lock for simplicity).
    """

    def __init__(self, nodes: list[str], virtual_nodes: int = 150):
        self._virtual_nodes = virtual_nodes
        self._ring: list[int] = []               # sorted hash values
        self._node_map: dict[int, str] = {}      # hash → node_addr
        self._nodes: set[str] = set()
        self._lock = threading.Lock()

        for node in nodes:
            self._add_node(node)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_node(self, node: str) -> None:
        with self._lock:
            self._add_node(node)

    def remove_node(self, node: str) -> None:
        with self._lock:
            if node not in self._nodes:
                return
            self._nodes.discard(node)
            for i in range(self._virtual_nodes):
                h = _md5_int(f"{node}:{i}")
                idx = bisect.bisect_left(self._ring, h)
                if idx < len(self._ring) and self._ring[idx] == h:
                    del self._ring[idx]
                    del self._node_map[h]

    def _add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self._virtual_nodes):
            h = _md5_int(f"{node}:{i}")
            bisect.insort(self._ring, h)
            self._node_map[h] = node

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_node(self, key: str) -> Optional[str]:
        """Return the primary node for the given key."""
        with self._lock:
            return self._get_node_unsafe(key)

    def get_replicas(self, key: str, n: int) -> list[str]:
        """
        Return the ordered list of n unique physical nodes starting at
        the primary for key.
        """
        with self._lock:
            if not self._ring:
                return []
            h = _md5_int(key)
            idx = bisect.bisect_right(self._ring, h) % len(self._ring)
            seen: set[str] = set()
            result: list[str] = []
            for offset in range(len(self._ring)):
                pos = (idx + offset) % len(self._ring)
                node = self._node_map[self._ring[pos]]
                if node not in seen:
                    seen.add(node)
                    result.append(node)
                if len(result) == n:
                    break
            return result

    def _get_node_unsafe(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = _md5_int(key)
        idx = bisect.bisect_right(self._ring, h) % len(self._ring)
        return self._node_map[self._ring[idx]]

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def nodes(self) -> list[str]:
        with self._lock:
            return sorted(self._nodes)

    def distribution(self, sample_keys: list[str]) -> dict[str, int]:
        """Show how sample_keys are distributed across nodes."""
        counts: dict[str, int] = {n: 0 for n in self._nodes}
        for k in sample_keys:
            node = self.get_node(k)
            if node:
                counts[node] = counts.get(node, 0) + 1
        return counts
