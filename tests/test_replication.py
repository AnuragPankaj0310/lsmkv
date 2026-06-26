"""Replication module tests (unit-level, no live server)."""
from __future__ import annotations

import pytest

from distributed.ring import ConsistentHashRing


def test_replication_module_imports():
    """Basic smoke test that the module loads."""
    from distributed.replication import ReplicationError, replicate_to
    assert ReplicationError is not None
    assert callable(replicate_to)


def test_replicate_to_empty_targets_noop():
    """Replicating to no targets should succeed immediately."""
    import asyncio
    from distributed.replication import replicate_to

    async def _run():
        await replicate_to([], {"cmd": "SET", "key": "k", "value": b"v"})

    asyncio.run(_run())  # should not raise
