import pytest

from storage.engine import StorageEngine
from distributed.migration_executor import MigrationExecutor


@pytest.mark.asyncio
async def test_cross_node_migration(tmp_path):
    source_engine = StorageEngine(str(tmp_path / "node1"))
    target_engine = StorageEngine(str(tmp_path / "node4"))

    await source_engine.open()
    await target_engine.open()

    source = MigrationExecutor(source_engine)
    target = MigrationExecutor(target_engine)

    #
    # Populate source node.
    #
    for i in range(100):
        await source_engine.set(
            f"key-{i}",
            f"value-{i}".encode(),
        )

    #
    # Export.
    #
    data = await source.export_keys(
        [f"key-{i}" for i in range(100)]
    )

    #
    # Import.
    #
    await target.import_keys(data)

    #
    # Verify target owns everything.
    #
    for i in range(100):
        assert (
            await target_engine.get(f"key-{i}")
            == f"value-{i}".encode()
        )

    #
    # Source still owns them.
    #
    for i in range(100):
        assert (
            await source_engine.get(f"key-{i}")
            == f"value-{i}".encode()
        )

    await source_engine.close()
    await target_engine.close()