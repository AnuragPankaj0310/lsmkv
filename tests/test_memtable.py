"""Tests for MemTable — Phase 1."""

from __future__ import annotations

import time


from storage.memtable import MemTable


def test_set_and_get():
    mt = MemTable()
    mt.set("key1", b"value1")
    assert mt.get("key1") == b"value1"


def test_get_missing_returns_none():
    mt = MemTable()
    assert mt.get("nonexistent") is None


def test_overwrite():
    mt = MemTable()
    mt.set("k", b"v1")
    mt.set("k", b"v2")
    assert mt.get("k") == b"v2"


def test_delete_creates_tombstone():
    mt = MemTable()
    mt.set("k", b"v")
    mt.delete("k")
    assert mt.get("k") is None
    assert mt.is_tombstone("k")


def test_delete_nonexistent_key():
    mt = MemTable()
    mt.delete("no_such_key")
    assert mt.get("no_such_key") is None
    assert mt.is_tombstone("no_such_key")


def test_items_sorted():
    mt = MemTable()
    mt.set("banana", b"b")
    mt.set("apple", b"a")
    mt.set("cherry", b"c")
    keys = [k for k, *_ in mt.items()]
    assert keys == sorted(keys)


def test_items_excludes_expired():
    mt = MemTable()
    mt.set("live", b"v", ttl=3600)
    mt.set("dead", b"v", ttl=0.0001)
    time.sleep(0.01)
    items = {k for k, *_ in mt.items()}
    assert "live" in items
    assert "dead" not in items


def test_items_includes_tombstones():
    mt = MemTable()
    mt.set("k", b"v")
    mt.delete("k")
    rows = [(k, tomb) for k, _, _, tomb in mt.items()]
    assert any(k == "k" and tomb for k, tomb in rows)


def test_is_full():
    # 1-byte threshold — first set should trigger full
    mt = MemTable(max_size_bytes=1)
    mt.set("x", b"y")
    assert mt.is_full()


def test_clear():
    mt = MemTable()
    mt.set("a", b"1")
    mt.set("b", b"2")
    mt.clear()
    assert len(mt) == 0
    assert mt.size_bytes() == 0
    assert mt.get("a") is None


def test_size_increases_on_set():
    mt = MemTable()
    assert mt.size_bytes() == 0
    mt.set("hello", b"world")
    assert mt.size_bytes() > 0


def test_ttl_expiry():
    mt = MemTable()
    mt.set("temp", b"val", ttl=0.01)
    assert mt.get("temp") == b"val"
    time.sleep(0.05)
    assert mt.get("temp") is None


def test_sweep_expired():
    mt = MemTable()
    mt.set("expire", b"v", ttl=0.01)
    mt.set("keep", b"v2")
    time.sleep(0.05)
    removed = mt.sweep_expired()
    assert removed == 1
    assert mt.contains("keep")
    assert not mt.contains("expire")


def test_len():
    mt = MemTable()
    mt.set("a", b"1")
    mt.set("b", b"2")
    mt.set("c", b"3")
    assert len(mt) == 3


def test_large_write_and_read():
    mt = MemTable()
    for i in range(1000):
        mt.set(f"key:{i:05d}", f"value{i}".encode())
    for i in range(1000):
        assert mt.get(f"key:{i:05d}") == f"value{i}".encode()
