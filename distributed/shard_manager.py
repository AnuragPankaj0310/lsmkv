"""
Shard ownership manager.
"""

from __future__ import annotations

from distributed.routing import RequestRouter


class ShardManager:
    def __init__(
        self,
        router: RequestRouter,
        replication_factor: int = 2,
    ):
        self._router = router
        self._replication_factor = replication_factor

    def primary(self, key: str) -> str:
        return self._router.primary(key)

    def replicas(self, key: str) -> list[str]:
        """
        Return replica set including the primary.
        """
        return self._router.replicas(
            key,
            self._replication_factor,
        )

    def owns_key(self, node: str, key: str) -> bool:
        return self.primary(key) == node

    def is_replica(self, node: str, key: str) -> bool:
        return node in self.replicas(key)