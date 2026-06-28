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

    def replicas(self, key: str, replication_factor: int = 2,
    ) -> list[str]:
        """
        Return the replica set (including the primary).

        replication_factor specifies how many nodes
        should be returned.
        """
        return self.ring.get_replicas(
            key,
            replication_factor,
        )
    
    def add_node(self, address: str) -> None:
        """
        Add a node to the routing ring.
        """
        self.ring.add_node(address)


    def remove_node(self, address: str) -> None:
        """
        Remove a node from the routing ring.
        """
        self.ring.remove_node(address)