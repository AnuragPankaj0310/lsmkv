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
                        click.echo(f"  {k:<40s} {v}")
                except Exception as exc:
                    click.echo(f"  ERROR: {exc}")
        finally:
            await client.close()

    _run(_do())


if __name__ == "__main__":
    cli()
