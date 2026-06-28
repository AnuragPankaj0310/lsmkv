import pytest

from storage.engine import StorageEngine
from distributed.migration_executor import MigrationExecutor


@pytest.mark.asyncio
async def test_export_key_batches(tmp_path):
    engine = StorageEngine(str(tmp_path))

    await engine.open()

    for i in range(1200):
        await engine.set(
            f"k{i}",
            str(i).encode(),
        )

    executor = MigrationExecutor(engine)

    batches = []

    async for keys, exported in executor.export_key_batches(
        [f"k{i}" for i in range(1200)],
        batch_size=500,
    ):
        batches.append((keys, exported))

    assert len(batches) == 3

    assert len(batches[0][0]) == 500
    assert len(batches[1][0]) == 500
    assert len(batches[2][0]) == 200

    await engine.close()