"""
Prometheus Metrics — Phase 6.

Exposes a /metrics HTTP endpoint (prometheus-client) on a dedicated port.
Grafana scrapes this endpoint to render real-time dashboards.

Metrics exported:

Standard operational metrics:
  lsmkv_ops_total{cmd,node}           Counter   — Total GET/SET/DEL operations
  lsmkv_latency_seconds{cmd,node}     Histogram — p50/p95/p99 latency per op type
  lsmkv_connections{node}             Gauge     — Current open connections
  lsmkv_memtable_size_bytes{node}     Gauge     — MemTable size in bytes
  lsmkv_memtable_entries{node}        Gauge     — MemTable entry count

Storage Engine Metrics (from plan):
  lsmkv_write_amplification{node}          Gauge  — disk bytes / client bytes
  lsmkv_read_amplification{node}           Gauge  — SSTables read per logical GET
  lsmkv_bloom_filter_hit_rate{node}        Gauge  — fraction of lookups skipped by Bloom
  lsmkv_sstable_count{node,level}          Gauge  — SSTable files per level
  lsmkv_compaction_throughput_bytes{node}  Gauge  — bytes/sec during compaction
  lsmkv_compaction_runs_total{node}        Counter — compaction cycles
  lsmkv_bloom_skips_total{node}            Counter — SSTables skipped via Bloom filter
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
)

if TYPE_CHECKING:
    from storage.engine import StorageEngine

log = logging.getLogger(__name__)

# Histogram buckets tuned for a local KV store (ms range)
_LATENCY_BUCKETS = (
    0.0001,
    0.0005,
    0.001,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
)


class MetricsCollector:
    """
    Wraps prometheus-client metrics and provides update methods for the server.
    One instance per server node.
    """

    def __init__(self, engine: "StorageEngine", node_id: str = "node-0"):
        self._engine = engine
        self._node = node_id
        self._labels = {"node": node_id}
        self._registry = CollectorRegistry()

        # --- Operational ---
        self._ops_total = Counter(
            "lsmkv_ops_total",
            "Total operations by type",
            ["cmd", "node"],
            registry=self._registry,
        )
        self._latency = Histogram(
            "lsmkv_latency_seconds",
            "Operation latency",
            ["cmd", "node"],
            buckets=_LATENCY_BUCKETS,
            registry=self._registry,
        )
        self._connections = Gauge(
            "lsmkv_connections",
            "Current open TCP connections",
            ["node"],
            registry=self._registry,
        )

        # --- MemTable ---
        self._memtable_size = Gauge(
            "lsmkv_memtable_size_bytes",
            "Current MemTable size in bytes",
            ["node"],
            registry=self._registry,
        )

        self._memtable_entries = Gauge(
            "lsmkv_memtable_entries",
            "Current MemTable entry count",
            ["node"],
            registry=self._registry,
        )

        # --- Storage Engine Metrics ---
        self._write_amplification = Gauge(
            "lsmkv_write_amplification",
            "Write amplification: disk bytes written / client bytes written",
            ["node"],
            registry=self._registry,
        )

        self._read_amplification = Gauge(
            "lsmkv_read_amplification",
            "Read amplification: avg SSTables read per logical GET",
            ["node"],
            registry=self._registry,
        )

        self._bloom_hit_rate = Gauge(
            "lsmkv_bloom_filter_hit_rate",
            "Fraction of SSTable lookups eliminated by Bloom filter (higher = better)",
            ["node"],
            registry=self._registry,
        )

        self._sstable_count = Gauge(
            "lsmkv_sstable_count",
            "Number of SSTable files on disk",
            ["node", "level"],
            registry=self._registry,
        )

        self._compaction_throughput = Gauge(
            "lsmkv_compaction_throughput_bytes",
            "Compaction throughput in bytes per second",
            ["node"],
            registry=self._registry,
        )

        self._compaction_runs = Counter(
            "lsmkv_compaction_runs_total",
            "Total compaction cycles completed",
            ["node"],
            registry=self._registry,
        )

        self._bloom_skips = Counter(
            "lsmkv_bloom_skips_total",
            "SSTables skipped via Bloom filter (no disk read required)",
            ["node"],
            registry=self._registry,
        )

        self._prev_compaction_runs = 0
        self._prev_bloom_skips = 0

        self._runner = None
        self._site = None
        self._update_task = None

    # ------------------------------------------------------------------
    # Update methods (called by server handlers)
    # ------------------------------------------------------------------

    def record_op(self, cmd: str, latency_seconds: float) -> None:
        self._ops_total.labels(cmd=cmd, node=self._node).inc()
        self._latency.labels(cmd=cmd, node=self._node).observe(latency_seconds)

    def connections_inc(self) -> None:
        self._connections.labels(node=self._node).inc()

    def connections_dec(self) -> None:
        self._connections.labels(node=self._node).dec()

    # ------------------------------------------------------------------
    # Background scrape loop — polls engine every 5 seconds
    # ------------------------------------------------------------------

    async def start_http_server(self, port: int) -> None:
        """Start Prometheus HTTP metrics endpoint and background update loop."""
        from aiohttp import web

        app = web.Application()

        async def metrics_handler(request):
            from prometheus_client import generate_latest

            self._refresh()
            return web.Response(
                body=generate_latest(self._registry),
                content_type="text/plain",
            )

        app.router.add_get("/metrics", metrics_handler)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "0.0.0.0", port)
        await self._site.start()
        log.info("Prometheus /metrics on port %d", port)

        # Background loop to keep gauges fresh
        self._update_task = asyncio.create_task(
            self._update_loop(), name="metrics_update"
        )

    async def _update_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            try:
                self._refresh()
            except Exception as exc:
                log.debug("Metrics refresh error: %s", exc)

    def _refresh(self) -> None:
        """Pull latest values from engine and update Prometheus gauges."""
        snap = self._engine.metrics_snapshot()
        node = self._node

        self._memtable_size.labels(node=node).set(snap["memtable_size_bytes"])
        self._memtable_entries.labels(node=node).set(snap["memtable_entries"])
        self._write_amplification.labels(node=node).set(snap["write_amplification"])
        self._read_amplification.labels(node=node).set(snap["read_amplification"])
        self._bloom_hit_rate.labels(node=node).set(snap["bloom_filter_hit_rate"])
        self._compaction_throughput.labels(node=node).set(
            snap["compaction_throughput_bytes_sec"]
        )

        per_level = snap.get("sstable_count_per_level", {})
        for level, count in per_level.items():
            self._sstable_count.labels(node=node, level=str(level)).set(count)

        # Increment counters by delta (Prometheus counters are monotonically increasing)
        new_runs = snap.get("compaction_runs", 0)
        delta_runs = max(0, new_runs - self._prev_compaction_runs)
        if delta_runs:
            self._compaction_runs.labels(node=node).inc(delta_runs)
        self._prev_compaction_runs = new_runs

    async def close(self) -> None:
        if self._update_task is not None:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass

        if self._site is not None:
            await self._site.stop()

        if self._runner is not None:
            await self._runner.cleanup()
