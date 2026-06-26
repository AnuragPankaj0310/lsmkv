"""
Custom Bloom Filter — built from scratch (Phase 3).

No library used — every line is explainable in an interview.

Theory:
  A Bloom Filter is a probabilistic data structure that answers:
    "Is this key DEFINITELY NOT in the set?"   → always correct
    "Is this key POSSIBLY in the set?"          → may give false positives

Algorithm — double hashing:
  h(key, i) = (h1(key) + i * h2(key)) % m

  where m = bit array size, k = number of hash functions

Optimal parameters from target capacity n and false-positive rate p:
  m = ceil( -(n * ln(p)) / ln(2)^2 )
  k = round( (m / n) * ln(2) )

At 1% FP rate: ~9.6 bits per key, ~7 hash functions.

Serialization format (20-byte header + bit array):
  [8 bytes] m  — bit array size
  [4 bytes] k  — hash function count
  [8 bytes] count — number of keys added
  [variable] bit array bytes
"""
from __future__ import annotations

import hashlib
import math


class BloomFilter:
    """
    Space-efficient probabilistic set membership filter.

    Example:
        bf = BloomFilter(capacity=10_000, fp_rate=0.01)
        bf.add("user:42")
        bf.might_exist("user:42")   # True
        bf.might_exist("user:99")   # False with ~99% probability
    """

    _HEADER_SIZE = 20  # 8 + 4 + 8

    def __init__(self, capacity: int, fp_rate: float = 0.01):
        if capacity <= 0:
            capacity = 1
        self.capacity = capacity
        self.fp_rate = fp_rate
        self.m: int = self._optimal_m(capacity, fp_rate)
        self.k: int = self._optimal_k(self.m, capacity)
        self.bits: bytearray = bytearray((self.m + 7) // 8)
        self._count: int = 0

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def add(self, key: str) -> None:
        """Insert key into the filter. Never raises."""
        h1, h2 = self._double_hash(key)
        for i in range(self.k):
            pos = (h1 + i * h2) % self.m
            self.bits[pos >> 3] |= 1 << (pos & 7)
        self._count += 1

    def might_exist(self, key: str) -> bool:
        """
        Returns False if key is DEFINITELY not in the set.
        Returns True if key is POSSIBLY in the set (may be a false positive).
        """
        h1, h2 = self._double_hash(key)
        for i in range(self.k):
            pos = (h1 + i * h2) % self.m
            if not (self.bits[pos >> 3] & (1 << (pos & 7))):
                return False
        return True

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_bytes(self) -> bytes:
        """Serialize to bytes for storage in SSTable header."""
        header = (
            self.m.to_bytes(8, "big")
            + self.k.to_bytes(4, "big")
            + self._count.to_bytes(8, "big")
        )
        return header + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        """Deserialize from SSTable header bytes."""
        m = int.from_bytes(data[0:8], "big")
        k = int.from_bytes(data[8:12], "big")
        count = int.from_bytes(data[12:20], "big")
        bits = bytearray(data[20:])

        obj = cls.__new__(cls)
        obj.capacity = count
        obj.fp_rate = 0.01
        obj.m = m
        obj.k = k
        obj.bits = bits
        obj._count = count
        return obj

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _double_hash(self, key: str) -> tuple[int, int]:
        """
        Compute two independent hashes for double-hashing scheme.
        MD5 for h1, SHA-1 for h2. Both mod m.
        h2 is forced non-zero to prevent degenerate collisions.
        """
        encoded = key.encode("utf-8")
        h1 = int(hashlib.md5(encoded).hexdigest(), 16) % self.m
        h2 = int(hashlib.sha1(encoded).hexdigest(), 16) % self.m
        if h2 == 0:
            h2 = 1
        return h1, h2

    @staticmethod
    def _optimal_m(n: int, p: float) -> int:
        """Optimal bit array size."""
        return max(8, math.ceil(-(n * math.log(p)) / (math.log(2) ** 2)))

    @staticmethod
    def _optimal_k(m: int, n: int) -> int:
        """Optimal number of hash functions."""
        return max(1, round((m / n) * math.log(2)))

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def count(self) -> int:
        """Number of keys added."""
        return self._count

    @property
    def fill_ratio(self) -> float:
        """Fraction of bits currently set — useful for monitoring FP rate drift."""
        set_bits = sum(bin(b).count("1") for b in self.bits)
        return set_bits / self.m if self.m > 0 else 0.0

    def __repr__(self) -> str:
        return (
            f"BloomFilter(capacity={self.capacity}, fp_rate={self.fp_rate}, "
            f"m={self.m}, k={self.k}, count={self._count}, "
            f"fill={self.fill_ratio:.2%})"
        )
