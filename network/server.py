"""
TCP Server — asyncio event-loop driven (Phase 1 + 5).

Architecture:
  asyncio.start_server() handles all connections on one event loop.
  Each connection gets its own coroutine (_handle_client).
  Heavy I/O (SSTable reads/writes) is offloaded to thread pool via asyncio.to_thread().

Prometheus /metrics endpoint runs on a separate port (HTTP, not TCP).

Replication:
  On SET/DEL, the primary node forwards the command to all replicas
  before ACK-ing the client (synchronous replication).
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Optional

from network.protocol import encode, encode_ok, encode_error, read_message, validate_command
from storage.engine import StorageEngine
from metrics.prometheus import MetricsCollector
from distributed.ring import ConsistentHashRing
from distributed.routing import RequestRouter
from distributed.replication import send_request
from distributed.shard_manager import ShardManager
from distributed.migration_executor import MigrationExecutor

log = logging.getLogger(__name__)


class LsmkvServer:
    """
    Single-node LSMKV server.

    Usage:
        server = LsmkvServer.from_config("config.json")
        asyncio.run(server.serve_forever())
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 7001,
        metrics_port: int = 9001,
        data_dir: str = "data",
        memtable_size_bytes: int = 4 * 1024 * 1024,
        l0_compaction_trigger: int = 4,
        compaction_interval: float = 30.0,
        replication_targets: Optional[list[str]] = None,
        cluster_nodes: Optional[list[str]] = None,
        node_address: Optional[str] = None,
        node_id: str = "node-1",
    ):
        self._host = host
        self._port = port
        self._metrics_port = metrics_port
        self._node_id = node_id
        self._client_tasks: set[asyncio.Task] = set()
        self._replication_targets = replication_targets or []
        self._cluster_nodes = cluster_nodes or []
        self._address = node_address or f"{host}:{port}"
        self._ring = ConsistentHashRing(self._cluster_nodes or [self._address])
        self._router = RequestRouter(self._ring)
        self._shards = ShardManager(self._router)

        self._engine = StorageEngine(
            data_dir=data_dir,
            memtable_size_bytes=memtable_size_bytes,
            l0_compaction_trigger=l0_compaction_trigger,
            compaction_interval=compaction_interval,
        )
        self._metrics = MetricsCollector(engine=self._engine, node_id=node_id)
        self._connections: int = 0

    async def _route_request(self, msg: dict) -> bytes:
        """
        Route client requests to the owning node.
        """

        cmd = msg["cmd"]

        # Commands without a key are always handled locally.
        if cmd in {"PING", "METRICS", "REPLICATE", "MIGRATE"}:
            return await self._dispatch(msg)

        # Already forwarded -> execute locally.
        if msg.get("forwarded", False):
            return await self._dispatch(msg)

        key = msg["key"]
        if self._shards.owns_key(self._address, key):
            return await self._dispatch(msg)

        owner = self._shards.primary(key)

        # We own the key.
        if owner == self._address:
            return await self._dispatch(msg)

        forward_msg = dict(msg)
        forward_msg["forwarded"] = True
        forward_msg["origin"] = self._address

        response = await send_request(owner, forward_msg)

        # Convert the decoded response back into the wire format.
        return encode(response)
    
    def update_cluster(self, cluster_nodes: list[str]) -> None:
        """
        Update the cluster membership and rebuild routing.
        """

        self._cluster_nodes = cluster_nodes

        self._ring = ConsistentHashRing(cluster_nodes)

        self._router = RequestRouter(self._ring)

        self._shards = ShardManager(self._router)

    async def keys_to_migrate(
        self,
        new_cluster_nodes: list[str],
    ) -> dict[str, list[str]]:
        """
        Return destination -> keys mapping after a topology change.
        """

        old_ring = self._ring
        new_ring = ConsistentHashRing(new_cluster_nodes)

        keys = await self._engine.all_keys()

        migrations: dict[str, list[str]] = {}

        for key in keys:
            old_owner = old_ring.get_node(key)
            new_owner = new_ring.get_node(key)

            if (
                old_owner == self._address
                and new_owner != self._address
            ):
                migrations.setdefault(new_owner, []).append(key)

        return migrations

    async def rebalance_cluster(
        self,
        new_cluster_nodes: list[str],
    ) -> int:
        """
        Perform the first stage of rebalancing.
        """

        migrations = await self.keys_to_migrate(new_cluster_nodes)

        executor = MigrationExecutor(self._engine)

        migrated = 0

        for destination, keys in migrations.items():

            async for batch_keys, exported in executor.export_key_batches(
                keys
            ):

                success = False

                for attempt in range(3):
                    try:
                        response = await send_request(
                            destination,
                            {
                                "cmd": "MIGRATE",
                                "data": exported,
                            },
                        )

                        if response.get("ok"):
                            success = True
                            break

                    except Exception:
                        if attempt == 2:
                            raise

                if not success:
                    raise RuntimeError(
                        f"Migration to {destination} failed"
                    )

                await executor.delete_keys(batch_keys)

                migrated += len(exported)

        return migrated

    async def on_cluster_changed(
        self,
        new_cluster_nodes: list[str],
    ):
        """
        Handle a cluster topology change.

        1. Rebalance keys.
        2. Update local routing.
        """

        await self.rebalance_cluster(new_cluster_nodes)

        self.update_cluster(new_cluster_nodes)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def serve_forever(self) -> None:
        await self._engine.open()
        log.info("[%s] Storage engine ready", self._node_id)

        # Start metrics HTTP server
        await self._metrics.start_http_server(self._metrics_port)
        log.info(
            "[%s] Metrics endpoint: http://%s:%d/metrics",
            self._node_id,
            self._host,
            self._metrics_port,
        )

        # Start TCP server
        server = await asyncio.start_server(self._handle_client, self._host, self._port)
        log.info("[%s] Listening on %s:%d", self._node_id, self._host, self._port)

        # Graceful shutdown on SIGTERM / SIGINT
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig, lambda: asyncio.create_task(self._shutdown(server))
                )
            except NotImplementedError:
                pass  # Windows

        try:
            async with server:
                await server.serve_forever()
        finally:
            await self._shutdown(server)

    async def _shutdown(self, server: asyncio.Server) -> None:
        log.info("[%s] Shutting down…", self._node_id)

        for task in list(self._client_tasks):
            task.cancel()

        await asyncio.gather(
            *self._client_tasks,
            return_exceptions=True,
        )

        # Let pending client handlers finish
        await asyncio.sleep(0)

        await self._metrics.close()

        await self._engine.close()

    # ------------------------------------------------------------------
    # Connection handler
    # ------------------------------------------------------------------

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._client_tasks.add(task)
        peer = writer.get_extra_info("peername")
        self._connections += 1
        self._metrics.connections_inc()
        log.debug("[%s] New connection from %s", self._node_id, peer)

        try:
            while True:
                try:
                    msg = await read_message(reader)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break  # client disconnected

                valid, err = validate_command(msg)
                if not valid:
                    writer.write(encode_error(err))
                    await writer.drain()
                    continue

                response = await self._route_request(msg)
                writer.write(response)
                await writer.drain()
        except Exception as exc:
            log.error("[%s] Handler error: %s", self._node_id, exc, exc_info=True)
        finally:
            self._connections -= 1
            self._metrics.connections_dec()

            try:
                writer.close()
                await writer.wait_closed()
            except RuntimeError:
                # Event loop already shutting down
                pass
            except Exception:
                pass
            task = asyncio.current_task()
            if task is not None:
                self._client_tasks.discard(task)

    # ------------------------------------------------------------------
    # Command dispatcher
    # ------------------------------------------------------------------

    async def _dispatch(self, msg: dict) -> bytes:
        cmd = msg["cmd"]
        start = asyncio.get_event_loop().time()

        try:
            if cmd == "PING":
                return encode_ok(value="PONG")

            elif cmd == "SET":
                key = msg["key"]
                value = bytes(msg["value"])
                ttl = msg.get("ttl")
                await self._engine.set(key, value, ttl)
                # NOTE:
                # Local write is applied before synchronous replication.
                # If replication fails, the client receives an error but the
                # primary keeps the local write. Production systems solve this
                # with distributed consensus (Raft/Paxos) or distributed
                # transactions. This project intentionally keeps the design
                # simple and documents the trade-off.
                await self._replicate(msg)
                self._metrics.record_op("SET", asyncio.get_event_loop().time() - start)
                return encode_ok()

            elif cmd == "GET":
                key = msg["key"]
                value = await self._engine.get(key)
                self._metrics.record_op("GET", asyncio.get_event_loop().time() - start)
                return encode_ok(value=value)

            elif cmd == "DEL":
                key = msg["key"]
                await self._engine.delete(key)
                # NOTE:
                # Local write is applied before synchronous replication.
                # If replication fails, the client receives an error but the
                # primary keeps the local write. Production systems solve this
                # with distributed consensus (Raft/Paxos) or distributed
                # transactions. This project intentionally keeps the design
                # simple and documents the trade-off.
                await self._replicate(msg)
                self._metrics.record_op("DEL", asyncio.get_event_loop().time() - start)
                return encode_ok()

            elif cmd == "REPLICATE":
                # Internal command from primary → apply locally without re-replicating
                key = msg["key"]
                value = msg.get("value")
                op = msg.get("op", "SET")
                if op == "SET":
                    await self._engine.set(key, bytes(value), msg.get("ttl"))
                elif op == "DEL":
                    await self._engine.delete(key)
                return encode_ok()
            
            elif cmd == "MIGRATE":
                executor = MigrationExecutor(self._engine)
                data = msg["data"]
                await executor.import_keys(data)
                return encode_ok(
                    migrated=len(data)
                )

            elif cmd == "METRICS":
                snapshot = await self._engine.metrics_snapshot_async()
                print(snapshot)
                snapshot["connections"] = self._connections

                import msgpack

                try:
                    msgpack.packb(
                        {"ok": True, "metrics": snapshot},
                        use_bin_type=True,
                    )
                    print("SERVER: metrics pack OK")
                except Exception as e:
                    print("SERVER: metrics pack FAILED:", e)
                    raise

                return encode_ok(metrics=snapshot)

            else:
                return encode_error(f"Unhandled command: {cmd}")

        except Exception as exc:
            log.error("Dispatch error for %s: %s", cmd, exc, exc_info=True)
            return encode_error(str(exc))

    # ------------------------------------------------------------------
    # Synchronous replication
    # ------------------------------------------------------------------

    async def _replicate(self, original_msg: dict) -> None:
        """Forward the write to all replica targets before returning."""
        if not self._replication_targets:
            return

        rep_msg = dict(original_msg)
        rep_msg["cmd"] = "REPLICATE"
        rep_msg["op"] = original_msg["cmd"]

        from distributed.replication import replicate_to

        key = original_msg["key"]

        targets = [
            node
            for node in self._shards.replicas(key)
            if node != self._address
        ]
        await replicate_to(targets, rep_msg)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls, config_path: str = "config.json", node_index: int = 0
    ) -> "LsmkvServer":
        with open(config_path) as f:
            cfg = json.load(f)

        nodes: list[str] = cfg.get("nodes", [])
        host = cfg.get("server_host", "0.0.0.0")
        port = cfg.get("server_port", 7001)
        metrics_port = cfg.get("metrics_port", 9001)

        if node_index < len(nodes):
            addr = nodes[node_index]
            _, port = addr.split(":")
            host = "0.0.0.0"
            port = int(port)

        replicas = [n for i, n in enumerate(nodes) if i != node_index]

        return cls(
            host=host,
            port=port,
            metrics_port=metrics_port + node_index,
            data_dir=f"data/node{node_index}",
            memtable_size_bytes=cfg.get("memtable_size_bytes", 4 * 1024 * 1024),
            l0_compaction_trigger=cfg.get("l0_compaction_trigger", 4),
            compaction_interval=cfg.get("compaction_interval_seconds", 30.0),
            replication_targets=replicas,
            cluster_nodes=nodes,
            node_address=nodes[node_index],
            node_id=f"node-{node_index}",
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    node_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    server = LsmkvServer.from_config("config.json", node_index=node_index)
    asyncio.run(server.serve_forever())
