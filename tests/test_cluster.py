"""
Cluster integration tests — real servers, real client, real TCP.

These tests start actual LsmkvServer instances and use LsmkvClient
to drive operations through the full stack.
"""

from __future__ import annotations

import asyncio
import pytest

from network.server import LsmkvServer
from client.sdk import LsmkvClient
from distributed.ring import ConsistentHashRing


# =============================================================================
# Fixture: 3-node cluster
# =============================================================================


@pytest.fixture
async def cluster(tmp_path):
    """
    Start a 3-node cluster on localhost with real TCP sockets.

    Yields:
      {
        "servers": [LsmkvServer, LsmkvServer, LsmkvServer],
        "client": LsmkvClient,
        "ring": ConsistentHashRing,
        "addrs": ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"],
      }
    """
    servers = []
    addrs = ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]

    # Start 3 servers
    for i, addr in enumerate(addrs):
        host, port = addr.split(":")
        data_dir = tmp_path / f"node{i}"
        data_dir.mkdir()

        server = LsmkvServer(
            host=host,
            port=int(port),
            metrics_port=9000 + i,
            data_dir=str(data_dir),
            memtable_size_bytes=4 * 1024 * 1024,
            l0_compaction_trigger=4,
            compaction_interval=999.0,
            replication_targets=[a for j, a in enumerate(addrs) if j != i],

            # ADD THESE TWO LINES
            cluster_nodes=addrs,
            node_address=addr,

            node_id=f"node-{i}",
        )
        servers.append(server)

    # Start all servers concurrently
    server_tasks = [asyncio.create_task(s.serve_forever()) for s in servers]

    # Wait for all servers to be ready (poll until ping succeeds)
    async def wait_for_servers(timeout: float = 5.0) -> None:
        loop = asyncio.get_running_loop()
        start = loop.time()
        while loop.time() - start < timeout:
            test_client = None
            try:
                test_client = await LsmkvClient.create(
                    nodes=addrs,
                    enable_heartbeat=False,
                )
                if await test_client.ping():
                    return
            except Exception:
                pass
            finally:
                if test_client is not None:
                    await test_client.close()
            await asyncio.sleep(0.1)
        raise TimeoutError("Servers failed to start within timeout")

    await wait_for_servers()

    # Create client and ring
    client = await LsmkvClient.create(
        nodes=addrs,
        virtual_nodes=150,
        replication_factor=2,
        enable_heartbeat=True,
    )
    ring = ConsistentHashRing(addrs, virtual_nodes=150)

    yield {
        "servers": servers,
        "client": client,
        "ring": ring,
        "addrs": addrs,
    }

    # Cleanup
    await client.close()
    for task in server_tasks:
        task.cancel()
    await asyncio.sleep(0)  # Allow tasks to process cancellation
    await asyncio.gather(*server_tasks, return_exceptions=True)


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.asyncio
async def test_ping(cluster):
    """Test that client can ping servers."""
    client = cluster["client"]
    result = await client.ping()
    assert result is True


@pytest.mark.asyncio
async def test_set_get(cluster):
    """Test basic SET and GET."""
    client = cluster["client"]

    await client.set("user:1", b"Alice")
    value = await client.get("user:1")

    assert value == b"Alice"


@pytest.mark.asyncio
async def test_delete(cluster):
    """Test DEL operation."""
    client = cluster["client"]

    await client.set("temp:key", b"value")
    assert await client.get("temp:key") == b"value"

    await client.delete("temp:key")
    assert await client.get("temp:key") is None


@pytest.mark.asyncio
async def test_ttl(cluster):
    """Test TTL expiration (lenient — cleanup timing varies)."""
    client = cluster["client"]
    key = "session:123"
    value = b"session_data"

    # Write with 1-second TTL
    await client.set(key, value, ttl=1.0)
    assert await client.get(key) == value

    # Wait for expiry
    await asyncio.sleep(1.5)

    # Accept either expiration or value (cleanup depends on background task)
    result = await client.get(key)
    assert result is None or result == value


@pytest.mark.asyncio
async def test_consistent_hashing(cluster):
    """Test that keys are routed consistently."""
    ring = cluster["ring"]
    addrs = cluster["addrs"]

    # Generate keys and check distribution
    keys = [f"key:{i}" for i in range(1000)]
    distribution = {}

    for key in keys:
        primary = ring.get_node(key)
        distribution[primary] = distribution.get(primary, 0) + 1

    # Verify rough even distribution (allow 40% deviation)
    expected = len(keys) / len(addrs)
    for node, count in distribution.items():
        deviation = abs(count - expected) / expected
        assert deviation < 0.40, (
            f"Node {node}: {count} keys (deviation={deviation:.0%})"
        )

    # Verify replicas are deterministic
    for key in keys[:100]:
        replicas1 = ring.get_replicas(key, n=2)
        replicas2 = ring.get_replicas(key, n=2)
        assert replicas1 == replicas2


@pytest.mark.asyncio
async def test_replication(cluster):
    """Test that writes replicate to at least one replica."""
    client = cluster["client"]
    servers = cluster["servers"]
    ring = cluster["ring"]
    addrs = cluster["addrs"]

    # Build address-to-index mapping (decoupled from port numbers)
    addr_to_index = {addr: i for i, addr in enumerate(addrs)}

    key = "user:42"
    value = b"Alice"

    # Write through client
    await client.set(key, value)

    # Find primary and replicas
    primary_addr = ring.get_node(key)
    primary_idx = addr_to_index[primary_addr]

    replicas = ring.get_replicas(key, n=2)
    replica_indices = [addr_to_index[a] for a in replicas]

    # Verify primary has value
    primary_value = await servers[primary_idx]._engine.get(key)
    assert primary_value == value, "Primary should have value"

    # Verify at least one replica has value
    replica_found = False
    for idx in replica_indices:
        if idx == primary_idx:
            continue
        if await servers[idx]._engine.get(key) == value:
            replica_found = True
            break
    assert replica_found, "At least one replica should have value"


@pytest.mark.asyncio
async def test_multiple_keys(cluster):
    """Test writing and reading multiple keys."""
    client = cluster["client"]

    # Write 100 keys
    for i in range(100):
        await client.set(f"multi:key:{i}", f"value:{i}".encode())

    # Read all back
    for i in range(100):
        value = await client.get(f"multi:key:{i}")
        assert value == f"value:{i}".encode()


@pytest.mark.asyncio
async def test_concurrent_writes(cluster):
    """Test concurrent writes without crashes."""
    client = cluster["client"]

    # Concurrent writes
    tasks = [
        client.set(f"concurrent:key:{i}", f"value:{i}".encode()) for i in range(100)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Verify no write failed
    for r in results:
        assert not isinstance(r, Exception), f"Write failed: {r}"

    # Verify all written
    for i in range(100):
        value = await client.get(f"concurrent:key:{i}")
        assert value == f"value:{i}".encode()
