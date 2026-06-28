import asyncio
import pytest
import contextlib

from distributed.replication import send_request
from network.server import LsmkvServer


@pytest.mark.asyncio
async def test_migrate_command(tmp_path):
    """
    Verify that a remote MIGRATE request imports keys
    into another node.
    """

    server = LsmkvServer(
        host="127.0.0.1",
        port=7201,
        metrics_port=9201,
        data_dir=str(tmp_path),
        cluster_nodes=["127.0.0.1:7201"],
        node_address="127.0.0.1:7201",
    )

    task = asyncio.create_task(server.serve_forever())

    try:
        #
        # Give server time to bind.
        #
        await asyncio.sleep(1)

        response = await send_request(
            "127.0.0.1:7201",
            {
                "cmd": "MIGRATE",
                "data": {
                    "user1": b"Alice",
                    "user2": b"Bob",
                },
            },
        )

        assert response["ok"]

        assert await server._engine.get("user1") == b"Alice"
        assert await server._engine.get("user2") == b"Bob"

    finally:
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task