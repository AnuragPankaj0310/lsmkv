from __future__ import annotations
import asyncio


class ClusterManager:
    """
    Coordinates cluster membership changes and notifies
    registered servers.
    """

    def __init__(self, nodes: list[str]):
        self._nodes = list(nodes)
        self._servers = {}

    def register(self, address: str, server):
        self._servers[address] = server

    def nodes(self) -> list[str]:
        return list(self._nodes)

    async def add_node(self, address: str):
        if address in self._nodes:
            return

        self._nodes.append(address)

        #
        # Notify every running server.
        #
        await asyncio.gather(
        *[
            server.on_cluster_changed(self.nodes())
            for server in self._servers.values()
        ]
    )
    async def remove_node(self, address: str):
        if address not in self._nodes:
            return

        self._nodes.remove(address)

        for server in self._servers.values():
            await server.on_cluster_changed(self.nodes())