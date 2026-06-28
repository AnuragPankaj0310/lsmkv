"""
Cluster rebalancing.

Responsible for determining which keys should migrate
when nodes join or leave the cluster.

This module only computes migration plans.
It does NOT move data.
"""

from __future__ import annotations

from distributed.ring import ConsistentHashRing


class RebalancePlanner:
    """
    Computes ownership changes after topology updates.
    """

    def __init__(self, old_ring: ConsistentHashRing, new_ring: ConsistentHashRing):
        self.old_ring = old_ring
        self.new_ring = new_ring

    def new_owner(self, key: str) -> str:
        """
        Owner after rebalance.
        """
        return self.new_ring.get_node(key)

    def old_owner(self, key: str) -> str:
        """
        Owner before rebalance.
        """
        return self.old_ring.get_node(key)

    def needs_migration(self, key: str) -> bool:
        """
        Returns True if ownership changes.
        """
        return self.old_owner(key) != self.new_owner(key)