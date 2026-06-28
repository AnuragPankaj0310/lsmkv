import pytest

from storage.engine import StorageEngine


@pytest.mark.asyncio
async def test_all_keys_after_flush(tmp_path):
    engine = StorageEngine(str(tmp_path))

    await engine.open()

    await engine.set("a", b"1")
    await engine.set("b", b"2")
    await engine.set("c", b"3")

    # Force a flush to SSTable
    await engine._flush_memtable()

    keys = await engine.all_keys()

    assert set(keys) == {"a", "b", "c"}

    await engine.close()