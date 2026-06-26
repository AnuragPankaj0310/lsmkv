from __future__ import annotations

from distributed.ring import ConsistentHashRing


class RequestRouter:
    """
    Determines which node owns a given key.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        replication_factor: int = 3,
    ):
        self.ring = ring
        self.replication_factor = replication_factor

    def primary(self, key: str) -> str:
        """
        Return the primary owner.
        """
        return self.ring.get_node(key)

    def replicas(self, key: str) -> list[str]:
        """
        Return replica nodes.
        """
        return self.ring.get_replicas(
            key,
            self.replication_factor,
        )