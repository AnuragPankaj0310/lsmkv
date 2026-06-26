"""
Heartbeat-based Failure Detection — Phase 5.

Each node pings its peers every heartbeat_interval seconds.
A node is marked dead after heartbeat_missed_threshold consecutive misses.

On failure:
  - Node is marked DEAD in local state
  - Client SDK ring is notified to route around the dead node

On recovery:
  - Heartbeat succeeds again → node marked ALIVE
  - Ring is updated

Design: static cluster config only — no dynamic membership (gossip).
Adding nodes requires a config reload. This keeps focus on the storage engine.
"""
from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Callable, Optional

from network.protocol import encode, read_message

log = logging.getLogger(__name__)


class NodeStatus(Enum):
    ALIVE = "alive"
    DEAD = "dead"
    UNKNOWN = "unknown"


class PeerState:
    def __init__(self, addr: str, threshold: int):
        self.addr = addr
        self.status = NodeStatus.UNKNOWN
        self.missed = 0
        self.threshold = threshold
        self.last_seen: Optional[float] = None

    def heartbeat_success(self) -> bool:
        """Returns True if status changed to ALIVE."""
        self.missed = 0
        self.last_seen = time.time()
        if self.status != NodeStatus.ALIVE:
            self.status = NodeStatus.ALIVE
            return True
        return False

    def heartbeat_failure(self) -> bool:
        """Returns True if status changed to DEAD."""
        self.missed += 1
        if self.missed >= self.threshold and self.status != NodeStatus.DEAD:
            self.status = NodeStatus.DEAD
            return True
        return False


class HeartbeatManager:
    """
    Runs one asyncio task per peer, pinging it at heartbeat_interval.
    Calls on_node_failure(addr) / on_node_recovery(addr) on status changes.
    """

    PING_MSG = {"cmd": "PING"}
    PING_TIMEOUT = 2.0  # seconds

    def __init__(
        self,
        peers: list[str],
        interval: float = 2.0,
        missed_threshold: int = 3,
        on_node_failure: Optional[Callable[[str], None]] = None,
        on_node_recovery: Optional[Callable[[str], None]] = None,
    ):
        self._peers = {addr: PeerState(addr, missed_threshold) for addr in peers}
        self._interval = interval
        self._on_failure = on_node_failure or (lambda addr: None)
        self._on_recovery = on_node_recovery or (lambda addr: None)
        self._tasks: list[asyncio.Task] = []

    def start(self) -> None:
        for addr in self._peers:
            task = asyncio.create_task(self._ping_loop(addr), name=f"hb-{addr}")
            self._tasks.append(task)

    def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    def alive_peers(self) -> list[str]:
        return [addr for addr, s in self._peers.items() if s.status == NodeStatus.ALIVE]

    def dead_peers(self) -> list[str]:
        return [addr for addr, s in self._peers.items() if s.status == NodeStatus.DEAD]

    def status(self, addr: str) -> NodeStatus:
        return self._peers.get(addr, PeerState(addr, 3)).status

    # ------------------------------------------------------------------
    # Per-peer loop
    # ------------------------------------------------------------------

    async def _ping_loop(self, addr: str) -> None:
        host, port = addr.rsplit(":", 1)
        port = int(port)

        reader: Optional[asyncio.StreamReader] = None
        writer: Optional[asyncio.StreamWriter] = None

        while True:
            await asyncio.sleep(self._interval)

            try:
                if writer is None or writer.is_closing():
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port), timeout=self.PING_TIMEOUT
                    )

                writer.write(encode(self.PING_MSG))
                await writer.drain()
                resp = await asyncio.wait_for(read_message(reader), timeout=self.PING_TIMEOUT)

                if resp.get("ok"):
                    changed = self._peers[addr].heartbeat_success()
                    if changed:
                        log.info("Peer %s is back ALIVE", addr)
                        self._on_recovery(addr)
                else:
                    raise ValueError("PING returned not-ok")

            except Exception as exc:
                log.debug("Heartbeat to %s failed: %s", addr, exc)
                # Close dead connection
                try:
                    if writer:
                        writer.close()
                        try:
                            await writer.wait_closed()
                        except Exception:
                            pass
                except Exception:
                    pass
                reader = writer = None

                changed = self._peers[addr].heartbeat_failure()
                if changed:
                    log.warning("Peer %s is DEAD (missed=%d)", addr, self._peers[addr].missed)
                    self._on_failure(addr)
