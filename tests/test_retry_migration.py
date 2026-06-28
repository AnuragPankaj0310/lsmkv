import pytest

from network.server import LsmkvServer


@pytest.mark.asyncio
async def test_migration_retries(monkeypatch, tmp_path):
    """
    A failed batch should be retried before succeeding.
    """

    server = LsmkvServer(
        host="127.0.0.1",
        port=7501,
        metrics_port=9501,
        data_dir=str(tmp_path),
        cluster_nodes=["127.0.0.1:7501"],
        node_address="127.0.0.1:7501",
    )

    await server._engine.open()

    #
    # Source data.
    #
    await server._engine.set(
        "key1",
        b"value1",
    )

    #
    # Force one migration.
    #
    async def fake_keys_to_migrate(nodes):
        return {
            "127.0.0.1:7502": ["key1"]
        }

    monkeypatch.setattr(
        server,
        "keys_to_migrate",
        fake_keys_to_migrate,
    )

    calls = 0

    async def fake_send_request(destination, msg):
        nonlocal calls

        calls += 1

        if calls == 1:
            raise RuntimeError(
                "temporary failure"
            )

        return {
            "ok": True,
        }

    monkeypatch.setattr(
        "network.server.send_request",
        fake_send_request,
    )

    migrated = await server.rebalance_cluster(
        [
            "127.0.0.1:7501",
            "127.0.0.1:7502",
        ]
    )

    assert migrated == 1
    assert calls == 2

    await server._engine.close()