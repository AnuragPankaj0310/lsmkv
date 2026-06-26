"""Tests for WAL — Phase 1."""

from __future__ import annotations

import pytest

from storage.wal import WAL


@pytest.fixture
def wal(tmp_path):
    w = WAL(tmp_path / "wal.log")
    yield w
    w.close()


def test_append_and_replay_set(wal):
    wal.append("SET", "user:1", b"Anurag")
    entries = list(wal.replay())
    assert len(entries) == 1
    e = entries[0]
    assert e["op"] == "SET"
    assert e["key"] == "user:1"
    assert bytes(e["val"]) == b"Anurag"


def test_append_multiple(wal):
    for i in range(100):
        wal.append("SET", f"key:{i}", f"val{i}".encode())
    entries = list(wal.replay())
    assert len(entries) == 100
    assert entries[0]["key"] == "key:0"
    assert entries[99]["key"] == "key:99"


def test_del_entry(wal):
    wal.append("DEL", "to_delete")
    entries = list(wal.replay())
    assert len(entries) == 1
    assert entries[0]["op"] == "DEL"
    assert entries[0]["key"] == "to_delete"


def test_truncate_clears_log(wal):
    wal.append("SET", "k", b"v")
    wal.truncate()
    entries = list(wal.replay())
    assert len(entries) == 0


def test_append_after_truncate(wal):
    wal.append("SET", "old", b"data")
    wal.truncate()
    wal.append("SET", "new", b"fresh")
    entries = list(wal.replay())
    assert len(entries) == 1
    assert entries[0]["key"] == "new"


def test_ttl_stored(wal):
    wal.append("SET", "ttl_key", b"val", ttl=60.0)
    entries = list(wal.replay())
    assert "ttl" in entries[0]
    assert entries[0]["ttl"] == pytest.approx(60.0, abs=1.0)


def test_replay_preserves_order(wal):
    keys = [f"key:{i:04d}" for i in range(50)]
    for k in keys:
        wal.append("SET", k, b"v")
    replayed = [e["key"] for e in wal.replay()]
    assert replayed == keys


def test_empty_wal_replay(wal):
    assert list(wal.replay()) == []


def test_size_grows(wal):
    assert wal.size_bytes == 0
    wal.append("SET", "k", b"v")
    assert wal.size_bytes > 0


def test_crash_simulation(tmp_path):
    """Simulate a truncated write at the end of the log."""
    wal_path = tmp_path / "wal.log"
    w = WAL(wal_path)
    w.append("SET", "good", b"data")
    w.close()

    # Corrupt the file by appending garbage
    with open(wal_path, "ab") as f:
        f.write(b"\xff\xff\xff\xff" + b"garbage")

    w2 = WAL(wal_path)
    entries = list(w2.replay())
    w2.close()
    # The good entry should be recovered; the garbage entry discarded
    assert len(entries) == 1
    assert entries[0]["key"] == "good"
