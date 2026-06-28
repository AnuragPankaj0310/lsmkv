"""
CLI — Click-based command-line interface for LSMKV.

Usage:
    python -m client.cli set user:1 Anurag
    python -m client.cli get user:1
    python -m client.cli del user:1
    python -m client.cli ping
    python -m client.cli metrics

All commands read node addresses from config.json (or --nodes override).
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Optional

import click

from client.sdk import LsmkvClient, ClientError


def _run(coro):
    return asyncio.run(coro)


def _format_bytes(size: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


@click.group()
@click.option(
    "--config", default="config.json", show_default=True, help="Path to config.json"
)
@click.option(
    "--nodes", default=None, help="Comma-separated node addresses (overrides config)"
)
@click.pass_context
def cli(ctx, config: str, nodes: Optional[str]):
    """LSMKV — distributed key-value store CLI."""
    ctx.ensure_object(dict)

    if nodes:
        node_list = [n.strip() for n in nodes.split(",")]
    else:
        try:
            with open(config) as f:
                cfg = json.load(f)
            node_list = cfg["nodes"]
        except FileNotFoundError:
            node_list = ["127.0.0.1:7001"]

    ctx.obj["nodes"] = node_list
    ctx.obj["config"] = config


@cli.command()
@click.argument("key")
@click.argument("value")
@click.option("--ttl", default=None, type=float, help="TTL in seconds")
@click.pass_context
def set(ctx, key: str, value: str, ttl: Optional[float]):
    """SET key value — store a key."""

    async def _do():
        client = await LsmkvClient.create(ctx.obj["nodes"], enable_heartbeat=False)
        try:
            await client.set(key, value.encode("utf-8"), ttl=ttl)
            click.echo(f"OK  {key}")
        finally:
            await client.close()

    try:
        _run(_do())
    except ClientError as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument("key")
@click.pass_context
def get(ctx, key: str):
    """GET key — retrieve a value."""

    async def _do():
        client = await LsmkvClient.create(ctx.obj["nodes"], enable_heartbeat=False)
        try:
            value = await client.get(key)
            if value is None:
                click.echo("(nil)")
            else:
                click.echo(value.decode("utf-8", errors="replace"))
        finally:
            await client.close()

    try:
        _run(_do())
    except ClientError as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command("del")
@click.argument("key")
@click.pass_context
def delete(ctx, key: str):
    """DEL key — delete a key."""

    async def _do():
        client = await LsmkvClient.create(ctx.obj["nodes"], enable_heartbeat=False)
        try:
            await client.delete(key)
            click.echo(f"DEL {key}")
        finally:
            await client.close()

    try:
        _run(_do())
    except ClientError as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def ping(ctx):
    """PING — check if server is alive."""

    async def _do():
        client = await LsmkvClient.create(ctx.obj["nodes"], enable_heartbeat=False)
        try:
            for node in ctx.obj["nodes"]:
                alive = await client.ping(node)
                status = "PONG ✓" if alive else "DEAD ✗"
                click.echo(f"{node:30s} {status}")
        finally:
            await client.close()

    _run(_do())


@cli.command()
@click.option("--node", default=None, help="Specific node address")
@click.pass_context
def metrics(ctx, node: Optional[str]):
    """METRICS — show storage engine metrics."""

    async def _do():
        client = await LsmkvClient.create(ctx.obj["nodes"], enable_heartbeat=False)
        try:
            targets = [node] if node else ctx.obj["nodes"]
            for addr in targets:
                click.echo(f"\n── {addr} ──")
                try:
                    m = await client.metrics(addr)
                    for k, v in m.items():
                        if k in ("disk_usage_bytes", "memtable_size_bytes") and isinstance(v, (int, float)):
                            v = _format_bytes(v)
                        click.echo(f"  {k:<40s} {v}")
                except Exception as exc:
                    click.echo(f"  ERROR: {exc}")
        finally:
            await client.close()

    _run(_do())


@cli.command()
@click.pass_context
def stats(ctx):
    """Show cluster status."""

    async def _do():
        client = await LsmkvClient.create(
            ctx.obj["nodes"],
            enable_heartbeat=False,
        )

        try:
            online = 0
            total_sstables = 0
            total_keys = 0
            disk_usage = 0
            total_memtable_entries = 0
            total_connections = 0

            rows = []

            for node in ctx.obj["nodes"]:
                try:
                    alive = await client.ping(node)

                    if alive:
                        online += 1

                    metrics = await client.metrics(node)

                    total_keys = max(
                        total_keys,
                        metrics.get("total_keys", 0),
                    )

                    disk_usage += metrics.get(
                        "disk_usage_bytes",
                        0,
                    )

                    total_sstables += metrics.get(
                        "sstable_count",
                        0,
                    )

                    total_memtable_entries += metrics.get(
                        "memtable_entries",
                        0,
                    )

                    total_connections += metrics.get(
                        "connections",
                        0,
                    )

                    rows.append(
                        (
                            node,
                            "ONLINE",
                            metrics.get("sstable_count", 0),
                            metrics.get("total_keys", 0),
                            metrics.get("memtable_entries", 0),
                        )
                    )

                except Exception:
                    rows.append(
                        (
                            node,
                            "OFFLINE",
                            "-",
                            "-",
                            "-",
                        )
                    )

            click.echo()
            click.echo("=" * 60)
            click.echo("               LSMKV Cluster Status")
            click.echo("=" * 60)

            replication_factor = None
            try:
                with open(ctx.obj["config"]) as f:
                    cfg = json.load(f)
                replication_factor = cfg.get("replication_factor", 2)
            except Exception:
                replication_factor = None

            click.echo("\nCluster Summary")
            click.echo("----------------")
            click.echo(f"Configured Nodes : {len(ctx.obj['nodes'])}")
            click.echo(f"Online Nodes     : {online}")
            click.echo(f"Offline Nodes    : {len(ctx.obj['nodes']) - online}")
            if replication_factor is not None:
                click.echo(f"Replication Factor : {replication_factor}")

            click.echo("\nNode Health")
            click.echo("-----------")

            for node, status, sst, keys, mem in rows:
                icon = "✓" if status == "ONLINE" else "✗"

                click.echo(
                    f"{icon} {node:<22}"
                    f"{status:<8}"
                    f"SSTables={sst:<3}"
                    f" Keys={keys:<7}"
                    f" MemTable={mem}"
                )

            click.echo("\nStorage Summary")
            click.echo("----------------")
            click.echo(f"Logical Keys     : {total_keys}")
            click.echo(f"Cluster Storage  : {_format_bytes(disk_usage)}")
            click.echo(f"Total SSTables   : {total_sstables}")
            click.echo(f"MemTable Entries : {total_memtable_entries}")
            click.echo(f"Connections      : {total_connections}")

            click.echo("\nCluster Health")
            click.echo("----------------")

            if online == len(ctx.obj["nodes"]):
                click.echo("Status           : HEALTHY ✓")
            elif online > 0:
                click.echo("Status           : DEGRADED ⚠")
            else:
                click.echo("Status           : OFFLINE ✗")

            click.echo("=" * 60)

        finally:
            await client.close()

    _run(_do())


if __name__ == "__main__":
    cli()
