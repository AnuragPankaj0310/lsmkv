"""Tests for SSTable read/write — Phase 2."""

from __future__ import annotations


from storage.memtable import MemTable
from storage.sstable import SSTable, SSTableWriter


def _make_sst(tmp_path, entries: dict[str, bytes]) -> SSTable:
    """Helper: write a dict of key→value to a new SSTable."""
    mt = MemTable()
    for k, v in sorted(entries.items()):
        mt.set(k, v)
    path = tmp_path / "test.dat"
    writer = SSTableWriter(path, bloom_capacity=len(entries) + 1)
    return writer.write(mt.items())


def test_write_and_read_single_key(tmp_path):
    sst = _make_sst(tmp_path, {"hello": b"world"})
    assert sst.get("hello") == b"world"


def test_get_missing_key(tmp_path):
    sst = _make_sst(tmp_path, {"a": b"1", "b": b"2"})
    assert sst.get("zzz") is None
    assert sst.get("") is None


def test_bloom_filter_skips_absent(tmp_path):
    sst = _make_sst(tmp_path, {"apple": b"red"})
    # Bloom filter must say "definitely not here" for an absent key
    # (might_contain can return True, but for a key far from our set it should be False)
    assert sst.get("totally_absent_key_xyz") is None


def test_get_many_keys(tmp_path):
    n = 500
    data = {f"key:{i:05d}": f"val{i}".encode() for i in range(n)}
    sst = _make_sst(tmp_path, data)
    for k, v in data.items():
        result = sst.get(k)
        assert result == v, f"Mismatch for {k}: got {result}"


def test_sparse_index_seek(tmp_path):
    """Keys well past the first sparse index entry should be found correctly."""
    n = 1000
    data = {f"key:{i:06d}": f"v{i}".encode() for i in range(n)}
    sst = _make_sst(tmp_path, data)
    # Check first, middle, and last key
    assert sst.get("key:000000") == b"v0"
    assert sst.get("key:000499") == b"v499"
    assert sst.get("key:000999") == b"v999"


def test_tombstone_not_returned(tmp_path):
    mt = MemTable()
    mt.set("alive", b"yes")
    mt.delete("dead")  # tombstone
    path = tmp_path / "sst.dat"
    writer = SSTableWriter(path, bloom_capacity=5)
    sst = writer.write(mt.items())
    assert sst.get("alive") == b"yes"
    assert sst.get("dead") is None


def test_scan_all_entries(tmp_path):
    n = 200
    data = {f"k:{i:04d}": f"v{i}".encode() for i in range(n)}
    sst = _make_sst(tmp_path, data)
    scanned = {k: v for k, v, _, _ in sst.scan()}
    assert len(scanned) == n
    for k, v in data.items():
        assert scanned[k] == v


def test_scan_sorted_order(tmp_path):
    sst = _make_sst(tmp_path, {"z": b"3", "a": b"1", "m": b"2"})
    keys = [k for k, *_ in sst.scan()]
    assert keys == sorted(keys)


def test_entry_count(tmp_path):
    n = 77
    sst = _make_sst(tmp_path, {f"k{i}": b"v" for i in range(n)})
    assert sst.entry_count == n


def test_empty_memtable_returns_none(tmp_path):
    mt = MemTable()
    path = tmp_path / "empty.dat"
    writer = SSTableWriter(path, bloom_capacity=1)
    result = writer.write(mt.items())
    assert result is None


def test_min_key(tmp_path):
    sst = _make_sst(tmp_path, {"banana": b"b", "apple": b"a", "cherry": b"c"})
    assert sst.min_key == "apple"


def test_reload_from_disk(tmp_path):
    """SSTable should be readable after being loaded from an existing file."""
    sst1 = _make_sst(tmp_path, {"persist": b"data"})
    # Load fresh from disk path
    sst2 = SSTable(sst1.path)
    assert sst2.get("persist") == b"data"
