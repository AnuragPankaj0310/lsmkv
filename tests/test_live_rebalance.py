import pytest

from network.server import LsmkvServer


@pytest.mark.asyncio
async def test_keys_to_migrate(tmp_path):
    server = LsmkvServer(
        host="127.0.0.1",
        port=7001,
        metrics_port=9001,
        data_dir=str(tmp_path),
        cluster_nodes=[
            "node1",
            "node2",
            "node3",
        ],
        node_address="node1",
    )

    await server._engine.open()

    try:
        for i in range(500):
            await server._engine.set(
                f"key-{i}",
                str(i).encode(),
            )

        migrations = await server.keys_to_migrate(
            [
                "node1",
                "node2",
                "node3",
                "node4",
            ]
        )

        assert len(migrations) > 0

        assert sum(
            len(keys)
            for keys in migrations.values()
        ) > 0 

    finally:
        await server._engine.close()
