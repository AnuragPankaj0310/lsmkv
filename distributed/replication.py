"""
Synchronous Replication — Phase 5.

Primary → all replicas, write-before-ACK.

Design decision: synchronous only — no AP/CP toggle.
  Pro: clean, explainable consistency guarantee.
  Con: write latency increases with number of replicas.
  Document trade-off in design_decisions.md.

A write is considered failed if any replica returns an error.
The client receives an error and can retry (idempotent for SET/DEL).

Connection pool: each target gets a persistent asyncio TCP connection.
Connections are re-established automatically on failure.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from network.protocol import encode, read_message

log = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 3.0   # seconds to establish connection
_WRITE_TIMEOUT = 5.0     # seconds to wait for replica ACK


class ReplicaConnection:
    """Persistent connection to one replica node."""

    def __init__(self, addr: str):
        host, port = addr.rsplit(":", 1)
        self._host = host
        self._port = int(port)
        self._addr = addr
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()

    async def send(self, msg: dict) -> dict:
        """Send msg, return response. Re-connects if connection is dead."""
        async with self._lock:
            for attempt in range(2):
                try:
                    if self._writer is None or self._writer.is_closing():
                        await self._connect()
                    self._writer.write(encode(msg))
                    await self._writer.drain()
                    resp = await asyncio.wait_for(
                        read_message(self._reader), timeout=_WRITE_TIMEOUT
                    )
                    return resp
                except Exception as exc:
                    log.warning("Replica %s attempt %d failed: %s", self._addr, attempt, exc)
                    await self._disconnect()
                    if attempt == 1:
                        raise

    async def _connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port),
            timeout=_CONNECT_TIMEOUT,
        )
        log.debug("Connected to replica %s", self._addr)

    async def _disconnect(self) -> None:
        if self._writer:
            try:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except Exception:
                    pass
            except Exception:
                pass
        self._reader = None
        self._writer = None

    def close(self) -> None:
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass


# Module-level connection pool
_pool: dict[str, ReplicaConnection] = {}


def _get_conn(addr: str) -> ReplicaConnection:
    if addr not in _pool:
        _pool[addr] = ReplicaConnection(addr)
    return _pool[addr]


async def replicate_to(targets: list[str], msg: dict) -> None:
    """
    Synchronously replicate msg to all targets.
    Raises ReplicationError if any target fails.
    """
    if not targets:
        return

    tasks = [_get_conn(addr).send(msg) for addr in targets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    errors = []
    for addr, result in zip(targets, results):
        if isinstance(result, Exception):
            errors.append(f"{addr}: {result}")
        elif not result.get("ok"):
            errors.append(f"{addr}: {result.get('error', 'unknown')}")

    if errors:
        raise ReplicationError(f"Replication failed: {'; '.join(errors)}")


class ReplicationError(Exception):
    pass
