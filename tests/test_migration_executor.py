import pytest

from storage.engine import StorageEngine
from distributed.migration_executor import MigrationExecutor


@pytest.mark.asyncio
async def test_export_import(tmp_path):
    engine1 = StorageEngine(str(tmp_path / "a"))
    engine2 = StorageEngine(str(tmp_path / "b"))

    await engine1.open()
    await engine2.open()

    await engine1.set("a", b"1")
    await engine1.set("b", b"2")
    await engine1.set("c", b"3")

    exporter = MigrationExecutor(engine1)
    importer = MigrationExecutor(engine2)

    data = await exporter.export_keys(
        ["a", "b"]
    )

    assert len(data) == 2

    await importer.import_keys(data)

    assert await engine2.get("a") == b"1"
    assert await engine2.get("b") == b"2"
    assert await engine2.get("c") is None

    await engine1.close()
    await engine2.close()


@pytest.mark.asyncio
async def test_delete_keys(tmp_path):
    engine = StorageEngine(str(tmp_path))
    await engine.open()

    executor = MigrationExecutor(engine)

    await engine.set("a", b"1")
    await engine.set("b", b"2")

    assert await engine.get("a") == b"1"

    await executor.delete_keys(["a"])

    assert await engine.get("a") is None
    assert await engine.get("b") == b"2"

    await engine.close()