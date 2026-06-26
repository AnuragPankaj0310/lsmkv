"""Tests for Compaction Engine — Phase 4."""
from __future__ import annotations

import pytest

from storage.compaction import _kway_merge, SSTableRegistry
from storage.memtable import MemTable
from storage.sstable import SSTable, SSTableWriter


def _write_sst(tmp_path, seq: int, entries: dict[str, bytes], tombstones: list[str] = None) -> SSTable:
    mt = MemTable()
    for k, v in sorted(entries.items()):
        mt.set(k, v)
    for k in (tombstones or []):
        mt.delete(k)
    path = tmp_path / f"sst_{seq:07d}.dat"
    writer = SSTableWriter(path, bloom_capacity=max(len(entries) + len(tombstones or []), 1))
    return writer.write(mt.items())


def test_merge_no_duplicates(tmp_path):
    sst1 = _write_sst(tmp_path, 1, {"a": b"1", "b": b"2"})
    sst2 = _write_sst(tmp_path, 2, {"c": b"3", "d": b"4"})
    merged = list(_kway_merge([sst1, sst2]))
    keys = [k for k, *_ in merged]
    assert keys == sorted(keys)
    assert len(keys) == 4


def test_newest_wins_on_duplicate(tmp_path):
    """Higher sequence number should win when key appears in both SSTables."""
    sst_old = _write_sst(tmp_path, 1, {"x": b"old_value"})
    sst_new = _write_sst(tmp_path, 2, {"x": b"new_value"})
    merged = {k: v for k, v, _, _ in _kway_merge([sst_old, sst_new])}
    assert merged["x"] == b"new_value"


def test_tombstone_removed_on_merge(tmp_path):
    """A key deleted in the newer SSTable should not appear in merged output."""
    sst_old = _write_sst(tmp_path, 1, {"remove_me": b"data"})
    sst_new = _write_sst(tmp_path, 2, {}, tombstones=["remove_me"])
    merged = {k: v for k, v, _, _ in _kway_merge([sst_old, sst_new])}
    assert "remove_me" not in merged


def test_merge_output_sorted(tmp_path):
    sst1 = _write_sst(tmp_path, 1, {"m": b"1", "z": b"2"})
    sst2 = _write_sst(tmp_path, 2, {"a": b"3", "f": b"4"})
    merged = [k for k, *_ in _kway_merge([sst1, sst2])]
    assert merged == sorted(merged)


def test_merge_large(tmp_path):
    """K-way merge across 5 SSTables, 200 keys each (overlapping ranges)."""
    sstables = []
    for seq in range(1, 6):
        entries = {f"key:{i:06d}": f"v{seq}".encode() for i in range(seq * 100, seq * 100 + 200)}
        sst = _write_sst(tmp_path, seq, entries)
        sstables.append(sst)

    merged = list(_kway_merge(sstables))
    keys = [k for k, *_ in merged]
    assert keys == sorted(keys), "Merge output not sorted"
    assert len(keys) == len(set(keys)), "Duplicate keys in merge output"


def test_registry_add_and_count():
    reg = SSTableRegistry()
    reg.add(_MockSST(1), level=0)
    reg.add(_MockSST(2), level=0)
    assert reg.total_count() == 2


def test_registry_remove():
    reg = SSTableRegistry()
    sst = _MockSST(1)
    reg.add(sst, level=0)
    reg.remove(sst)
    assert reg.total_count() == 0


def test_registry_level_counts():
    reg = SSTableRegistry()
    reg.add(_MockSST(1), level=0)
    reg.add(_MockSST(2), level=0)
    reg.add(_MockSST(3), level=1)
    counts = reg.count_per_level()
    assert counts[0] == 2
    assert counts[1] == 1


class _MockSST:
    """Minimal SSTable mock for registry tests."""
    def __init__(self, seq: int):
        self.sequence = seq

    def size_bytes(self):
        return 0
