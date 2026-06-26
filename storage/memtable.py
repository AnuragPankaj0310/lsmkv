"""
MemTable — In-memory sorted write buffer (Phase 1).

Keys stay sorted at all times via SortedDict so SSTable flush produces
a sorted file with zero additional sorting passes.

Each entry stores: (value: bytes, expiry_ts: float|None, is_tombstone: bool)
- value       : raw bytes payload
- expiry_ts   : unix timestamp after which key is considered expired (None = no TTL)
- is_tombstone: True when key has been DEL-eted (marker for compaction)

Size tracking is approximate (key bytes + value bytes + 32 byte overhead).
"""

from __future__ import annotations

import time
from typing import Iterator, Optional

from sortedcontainers import SortedDict

_OVERHEAD = 32  # per-entry overhead estimate in bytes


class MemTable:
    """Single-writer, single-reader safe within one asyncio event loop."""

    def __init__(self, max_size_bytes: int = 4 * 1024 * 1024):
        self._data: SortedDict = SortedDict()  # key → (value, expiry_ts, is_tombstone)
        self._size: int = 0
        self._max_size: int = max_size_bytes

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def set(self, key: str, value: bytes, ttl: Optional[float] = None) -> None:
        """Insert or update a key. ttl is seconds from now."""
        expiry = time.time() + ttl if ttl is not None else None
        self._replace(key, (value, expiry, False))

    def delete(self, key: str) -> None:
        """Mark key as deleted via tombstone."""
        self._replace(key, (b"", None, True))

    def _replace(self, key: str, entry: tuple) -> None:
        old = self._data.get(key)
        if old is not None:
            self._size -= _entry_size(key, old)
        self._data[key] = entry
        self._size += _entry_size(key, entry)

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[bytes]:
        """
        Returns value bytes for key, or None if missing / tombstone / expired.
        """
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expiry, is_tombstone = entry
        if is_tombstone:
            return None
        if expiry is not None and time.time() > expiry:
            return None
        return value

    def is_tombstone(self, key: str) -> bool:
        entry = self._data.get(key)
        return entry is not None and entry[2]

    def contains(self, key: str) -> bool:
        return key in self._data

    # ------------------------------------------------------------------
    # Flush support — yields all entries in sorted key order
    # ------------------------------------------------------------------

    def items(self) -> Iterator[tuple[str, bytes, Optional[float], bool]]:
        """
        Yield (key, value, expiry_ts, is_tombstone) in ascending key order.
        Expired non-tombstone entries are omitted from the flush.
        """
        now = time.time()
        for key, (value, expiry, is_tombstone) in self._data.items():
            if not is_tombstone and expiry is not None and now > expiry:
                continue  # silently drop — will not appear in new SSTable
            yield key, value, expiry, is_tombstone

    def clear(self) -> None:
        """Called after successful flush to disk."""
        self._data.clear()
        self._size = 0

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def is_full(self) -> bool:
        return self._size >= self._max_size

    def size_bytes(self) -> int:
        return self._size

    def __len__(self) -> int:
        return len(self._data)

    # ------------------------------------------------------------------
    # Background TTL sweep
    # ------------------------------------------------------------------

    def sweep_expired(self) -> int:
        """
        Remove expired non-tombstone entries from memory.
        Called periodically by an asyncio background task.
        Returns the number of entries removed.
        """
        now = time.time()
        expired_keys = [
            k
            for k, (_, expiry, is_tombstone) in self._data.items()
            if not is_tombstone and expiry is not None and now > expiry
        ]
        for k in expired_keys:
            old = self._data.pop(k)
            self._size -= _entry_size(k, old)
        return len(expired_keys)


# ------------------------------------------------------------------
# Module-level helper (not a method — avoids repeated attribute lookups)
# ------------------------------------------------------------------


def _entry_size(key: str, entry: tuple) -> int:
    value, _, _ = entry
    return len(key.encode("utf-8")) + len(value) + _OVERHEAD
