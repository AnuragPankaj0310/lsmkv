"""
SSTable — Immutable Sorted String Table (Phase 2 + 3).

File layout (byte-exact):
  [HEADER  — 60 bytes fixed]
  [BLOOM   — variable, bloom_length bytes]
  [INDEX   — variable, index_length bytes  (msgpack list of [key, abs_file_offset])]
  [DATA    — variable, sequential msgpack entries]

HEADER fields (struct ">8sIQQQQQQ"):
  magic        : 8 bytes  — "LSMKV001"
  version      : 4 bytes  — uint32 = 1
  entry_count  : 8 bytes  — uint64
  bloom_offset : 8 bytes  — absolute file offset of bloom bytes
  bloom_length : 8 bytes  — byte length of bloom section
  index_offset : 8 bytes  — absolute file offset of sparse index
  index_length : 8 bytes  — byte length of sparse index section
  data_offset  : 8 bytes  — absolute file offset of first data entry

Each data entry:
  msgpack-encoded list: [key: str, value: bytes, expiry_ts: float|None, is_tombstone: bool]

Sparse index:
  Every SPARSE_INTERVAL-th key → absolute file offset of its data entry.
  Enables binary search → seek → short sequential scan (RocksDB / LevelDB style).

Read path:
  1. Check Bloom filter → return None if key definitely absent
  2. Binary search sparse index → find largest index_key <= target_key
  3. Seek to that offset, scan forward until key found or key surpassed
"""

from __future__ import annotations

import os
import shutil
import struct
import time
from pathlib import Path
from typing import Iterator, Optional

import msgpack

from storage.bloom import BloomFilter

SPARSE_INTERVAL: int = 128  # one index entry per N keys
MAGIC: bytes = b"LSMKV001"
VERSION: int = 1

# Fixed 60-byte header
_HEADER_FMT = ">8sIQQQQQQ"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)  # = 60 bytes


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class SSTableWriter:
    """
    Writes a new SSTable file from a sequence of MemTable items.

    Usage:
        writer = SSTableWriter(path="data/sstables/sst_001.dat")
        sst = writer.write(memtable.items())
    """

    def __init__(self, path: str | Path, bloom_capacity: int = 100_000):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._bloom = BloomFilter(max(bloom_capacity, 1))

    def write(
        self,
        items: Iterator[tuple[str, bytes, Optional[float], bool]],
    ) -> Optional["SSTable"]:
        """
        Consume items (key, value, expiry_ts, is_tombstone) in sorted order.
        Returns a loaded SSTable, or None if there were no items.
        """
        sparse_index: list[list] = []  # [[key, relative_offset], ...]

        relative_offset = 0
        count = 0

        tmp_data_path = self._path.with_suffix(".tmp.data")
        with open(tmp_data_path, "wb") as data_f:
            for key, value, expiry, is_tombstone in items:
                self._bloom.add(key)
                entry_bytes = msgpack.packb(
                    [key, value, expiry, is_tombstone], use_bin_type=True
                )
                if count % SPARSE_INTERVAL == 0:
                    sparse_index.append([key, relative_offset])
                data_f.write(entry_bytes)
                relative_offset += len(entry_bytes)
                count += 1
            data_f.flush()
            os.fsync(data_f.fileno())

        if count == 0:
            tmp_data_path.unlink(missing_ok=True)
            return None

        bloom_bytes = self._bloom.to_bytes()
        bloom_offset = _HEADER_SIZE
        bloom_length = len(bloom_bytes)
        index_offset = bloom_offset + bloom_length
        data_offset = index_offset

        while True:
            abs_index = [[k, data_offset + rel] for k, rel in sparse_index]
            index_bytes = msgpack.packb(abs_index, use_bin_type=True)
            index_length = len(index_bytes)
            new_data_offset = index_offset + index_length
            if new_data_offset == data_offset:
                break
            data_offset = new_data_offset

        header = struct.pack(
            _HEADER_FMT,
            MAGIC,
            VERSION,
            count,
            bloom_offset,
            bloom_length,
            index_offset,
            index_length,
            data_offset,
        )

        tmp_path = self._path.with_suffix(".tmp")
        try:
            with open(tmp_path, "wb") as f, open(tmp_data_path, "rb") as data_f:
                f.write(header)
                f.write(bloom_bytes)
                f.write(index_bytes)
                shutil.copyfileobj(data_f, f)
                f.flush()
                os.fsync(f.fileno())
            tmp_path.rename(self._path)  # atomic on POSIX; near-atomic on Windows
        finally:
            tmp_data_path.unlink(missing_ok=True)

        sst = SSTable(self._path)
        sst.load()
        return sst


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


class SSTable:
    """
    Read-only view of an SSTable file.

    Sparse index and Bloom filter are loaded into memory on first access.
    Data blocks remain on disk — only the relevant region is read per GET.
    """

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._bloom: Optional[BloomFilter] = None
        self._sparse_index: list[list] = []  # [[key, abs_offset], ...]
        self._entry_count: int = 0
        self._data_offset: int = 0
        self._loaded: bool = False
        self._sequence: int = self._parse_sequence()

    def _parse_sequence(self) -> int:
        """Extract numeric sequence from filename (e.g. sst_007.dat → 7)."""
        stem = self._path.stem  # "sst_007"
        parts = stem.split("_")
        try:
            return int(parts[-1])
        except (ValueError, IndexError):
            return 0

    # ------------------------------------------------------------------
    # Load metadata
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load header, Bloom filter, and sparse index into memory."""
        if self._loaded:
            return
        with open(self._path, "rb") as f:
            hdr = f.read(_HEADER_SIZE)
            (
                magic,
                _version,
                entry_count,
                bloom_offset,
                bloom_length,
                index_offset,
                index_length,
                data_offset,
            ) = struct.unpack(_HEADER_FMT, hdr)
            if magic != MAGIC:
                raise ValueError(f"Bad SSTable magic in {self._path}: {magic!r}")
            self._entry_count = entry_count
            self._data_offset = data_offset

            f.seek(bloom_offset)
            self._bloom = BloomFilter.from_bytes(f.read(bloom_length))

            f.seek(index_offset)
            self._sparse_index = msgpack.unpackb(f.read(index_length), raw=False)

        self._loaded = True

    # ------------------------------------------------------------------
    # Point lookup
    # ------------------------------------------------------------------

    def might_contain(self, key: str) -> bool:
        self.load()
        assert self._bloom is not None
        return self._bloom.might_exist(key)

    def get(self, key: str) -> Optional[bytes]:
        """
        Returns value bytes or None (not found, tombstone, or expired).
        Bloom filter guards all disk I/O — returns None without I/O for
        ~99% of truly absent keys.
        """
        self.load()

        if not self.might_contain(key):
            return None  # Bloom says definitely not here

        seek_offset = self._find_seek_offset(key)

        with open(self._path, "rb") as f:
            f.seek(seek_offset)
            unpacker = msgpack.Unpacker(f, raw=False)
            for entry in unpacker:
                entry_key, value, expiry, is_tombstone = entry
                if entry_key == key:
                    if is_tombstone:
                        return None
                    if expiry is not None and time.time() > expiry:
                        return None
                    return bytes(value)
                if entry_key > key:
                    return None  # sorted — gone past our key
        return None

    def _find_seek_offset(self, key: str) -> int:
        """Binary search sparse index → return best absolute file offset."""
        if not self._sparse_index:
            return self._data_offset
        lo, hi = 0, len(self._sparse_index) - 1
        best = self._data_offset
        while lo <= hi:
            mid = (lo + hi) // 2
            idx_key, idx_offset = self._sparse_index[mid]
            if idx_key <= key:
                best = idx_offset
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    # ------------------------------------------------------------------
    # Full scan (used by compaction)
    # ------------------------------------------------------------------

    def scan(self) -> Iterator[tuple[str, bytes, Optional[float], bool]]:
        """Yield all entries (key, value, expiry_ts, is_tombstone) in order."""
        self.load()
        with open(self._path, "rb") as f:
            f.seek(self._data_offset)
            unpacker = msgpack.Unpacker(f, raw=False)
            for entry in unpacker:
                key, value, expiry, is_tombstone = entry
                yield key, bytes(value), expiry, is_tombstone

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    @property
    def path(self) -> Path:
        return self._path

    @property
    def sequence(self) -> int:
        return self._sequence

    @property
    def entry_count(self) -> int:
        self.load()
        return self._entry_count

    @property
    def min_key(self) -> Optional[str]:
        self.load()
        return self._sparse_index[0][0] if self._sparse_index else None

    @property
    def max_key(self) -> Optional[str]:
        """Approximate upper bound.
        Because the sparse index stores one entry every
        SPARSE_INTERVAL keys, the final sparse-index key
        may not be the true maximum key in the SSTable.
        This method should therefore only be used for
        diagnostics or rough range estimation.
        """
        self.load()
        return self._sparse_index[-1][0] if self._sparse_index else None

    def size_bytes(self) -> int:
        return self._path.stat().st_size

    def delete_file(self) -> None:
        """Remove the SSTable file from disk (called after compaction)."""
        self._path.unlink(missing_ok=True)

    def __repr__(self) -> str:
        return f"SSTable(seq={self._sequence}, path={self._path.name})"
