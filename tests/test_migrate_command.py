import pytest

from storage.engine import StorageEngine
from distributed.migration_executor import MigrationExecutor


@pytest.mark.asyncio
async def test_import_via_executor(tmp_path):
    engine = StorageEngine(str(tmp_path))

    await engine.open()

    executor = MigrationExecutor(engine)

    await executor.import_keys(
        {
            "a": b"1",
            "b": b"2",
        }
    )

    assert await engine.get("a") == b"1"
    assert await engine.get("b") == b"2"

    await engine.close()