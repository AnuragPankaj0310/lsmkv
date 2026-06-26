"""
Client SDK — Phase 5.

The consistent hash ring lives here, not on a server coordinator.
The client routes directly to the correct primary node for every key.

Why no coordinator:
  A coordinator is a single point of failure and adds one extra network
  hop to every request. Putting ring logic in the SDK means clients route
  directly — exactly how Amazon Dynamo works.

Features:
  - Consistent hash ring with 150 virtual nodes per physical node
  - Direct routing: set/get/del → primary for key
  - Failover: on primary timeout, retry on first live replica
  - Heartbeat integration: dead nodes are removed from routing
  - Connection pooling: one persistent connection per node
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

from distributed.ring import ConsistentHashRing
from distributed.heartbeat import HeartbeatManager
from network.protocol import encode, read_message

log = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 3.0
_REQUEST_TIMEOUT = 5.0


class NodeConnection:
    """Persistent async connection to one server node."""

    def __init__(self, addr: str):
        host, port = addr.rsplit(":", 1)
        self._host = host
        self._port = int(port)
        self._addr = addr
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def send(self, msg: dict) -> dict:
        async with self._lock:
            for attempt in range(2):
                try:
                    if self._writer is None or self._writer.is_closing():
                        await self._connect()
                    self._writer.write(encode(msg))
                    await self._writer.drain()
                    return await asyncio.wait_for(
                        read_message(self._reader), timeout=_REQUEST_TIMEOUT
                    )
                except Exception as exc:
                    log.debug("Node %s attempt %d: %s", self._addr, attempt, exc)
                    await self._disconnect()
                    if attempt == 1:
                        raise

    async def _connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port),
            timeout=_CONNECT_TIMEOUT,
        )

    async def _disconnect(self) -> None:
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
        self._reader = self._writer = None

    async def close(self) -> None:
        await self._disconnect()


class LsmkvClient:
    """
    Async client SDK with embedded consistent hash ring.

    Usage:
        client = await LsmkvClient.create(nodes=["127.0.0.1:7001", ...])
        await client.set("user:1", b"Anurag")
        value = await client.get("user:1")
        await client.delete("user:1")
        await client.close()
    """

    def __init__(
        self,
        nodes: list[str],
        virtual_nodes: int = 150,
        replication_factor: int = 2,
        enable_heartbeat: bool = True,
        heartbeat_interval: float = 2.0,
        heartbeat_threshold: int = 3,
    ):
        self._nodes = list(nodes)
        self._rf = replication_factor
        self._ring = ConsistentHashRing(nodes, virtual_nodes)
        self._conns: dict[str, NodeConnection] = {
            addr: NodeConnection(addr) for addr in nodes
        }

        if enable_heartbeat and len(nodes) > 1:
            self._heartbeat = HeartbeatManager(
                peers=nodes,
                interval=heartbeat_interval,
                missed_threshold=heartbeat_threshold,
                on_node_failure=self._on_failure,
                on_node_recovery=self._on_recovery,
            )
        else:
            self._heartbeat = None

    @classmethod
    async def create(cls, nodes: list[str], **kwargs) -> "LsmkvClient":
        client = cls(nodes, **kwargs)
        if client._heartbeat:
            client._heartbeat.start()
        return client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def set(self, key: str, value: bytes | str, ttl: Optional[float] = None) -> None:
        if isinstance(value, str):
            value = value.encode("utf-8")
        msg: dict = {"cmd": "SET", "key": key, "value": value}
        if ttl is not None:
            msg["ttl"] = ttl
        resp = await self._send_to_primary(key, msg)
        if not resp.get("ok"):
            raise ClientError(f"SET failed: {resp.get('error')}")

    async def get(self, key: str) -> Optional[bytes]:
        resp = await self._send_to_primary(key, {"cmd": "GET", "key": key})
        if not resp.get("ok"):
            raise ClientError(f"GET failed: {resp.get('error')}")
        val = resp.get("value")
        return bytes(val) if val is not None else None

    async def delete(self, key: str) -> None:
        resp = await self._send_to_primary(key, {"cmd": "DEL", "key": key})
        if not resp.get("ok"):
            raise ClientError(f"DEL failed: {resp.get('error')}")

    async def ping(self, node_addr: Optional[str] = None) -> bool:
        addr = node_addr or self._nodes[0]
        try:
            resp = await self._conns[addr].send({"cmd": "PING"})
            return resp.get("ok", False)
        except Exception:
            return False

    async def metrics(self, node_addr: Optional[str] = None) -> dict:
        addr = node_addr or self._nodes[0]
        resp = await self._conns[addr].send({"cmd": "METRICS"})
        return resp.get("metrics", {})

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    async def _send_to_primary(self, key: str, msg: dict) -> dict:
        """Route to primary; failover to replica on error."""
        replicas = self._ring.get_replicas(key, self._rf)
        last_exc = None
        for addr in replicas:
            if addr not in self._conns:
                continue
            try:
                return await self._conns[addr].send(msg)
            except Exception as exc:
                last_exc = exc
                log.warning("Node %s unreachable, trying replica: %s", addr, exc)
        raise ClientError(f"All nodes failed for key {key!r}: {last_exc}")

    # ------------------------------------------------------------------
    # Heartbeat callbacks
    # ------------------------------------------------------------------

    def _on_failure(self, addr: str) -> None:
        log.warning("SDK: removing dead node %s from ring", addr)
        self._ring.remove_node(addr)

    def _on_recovery(self, addr: str) -> None:
        log.info("SDK: adding recovered node %s to ring", addr)
        self._ring.add_node(addr)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        if self._heartbeat:
            self._heartbeat.stop()
        for conn in self._conns.values():
            await conn.close()

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config_path: str = "config.json") -> "LsmkvClient":
        with open(config_path) as f:
            cfg = json.load(f)
        return cls(
            nodes=cfg["nodes"],
            virtual_nodes=cfg.get("virtual_nodes", 150),
            replication_factor=cfg.get("replication_factor", 2),
            heartbeat_interval=cfg.get("heartbeat_interval_seconds", 2.0),
            heartbeat_threshold=cfg.get("heartbeat_missed_threshold", 3),
        )


class ClientError(Exception):
    pass
