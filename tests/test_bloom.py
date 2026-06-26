"""Tests for BloomFilter — Phase 3."""
from __future__ import annotations

import pytest

from storage.bloom import BloomFilter


def test_add_and_might_exist():
    bf = BloomFilter(capacity=1000)
    bf.add("user:42")
    assert bf.might_exist("user:42") is True


def test_definitely_not_present():
    bf = BloomFilter(capacity=1000)
    bf.add("apple")
    # "banana" was never added — should return False with high probability
    # (we test many keys to catch any bug, not statistical FP)
    never_added = [f"never:{i}" for i in range(100)]
    # For a well-implemented filter, all should be False (except statistically rare FPs)
    false_positives = sum(bf.might_exist(k) for k in never_added)
    assert false_positives < 5  # < 5% FP rate on 100 keys is very generous


def test_no_false_negatives():
    """A Bloom filter must never say 'definitely not here' for a key we added."""
    bf = BloomFilter(capacity=10_000, fp_rate=0.01)
    keys = [f"key:{i}" for i in range(1000)]
    for k in keys:
        bf.add(k)
    for k in keys:
        assert bf.might_exist(k), f"False negative for {k}"


def test_count_property():
    bf = BloomFilter(capacity=100)
    assert bf.count == 0
    bf.add("a")
    bf.add("b")
    assert bf.count == 2


def test_serialization_roundtrip():
    bf = BloomFilter(capacity=500, fp_rate=0.01)
    keys = [f"item:{i}" for i in range(100)]
    for k in keys:
        bf.add(k)

    data = bf.to_bytes()
    bf2 = BloomFilter.from_bytes(data)

    # All added keys must still be found
    for k in keys:
        assert bf2.might_exist(k), f"Key {k} lost after serialization"

    assert bf2.m == bf.m
    assert bf2.k == bf.k
    assert bf2.count == bf.count


def test_empty_filter_never_matches():
    bf = BloomFilter(capacity=100)
    assert not bf.might_exist("anything")


def test_fp_rate_approximation():
    """
    With 1% FP rate and capacity=10k, adding 10k keys should yield
    roughly 1% FP on unseen keys.
    """
    n = 5000
    bf = BloomFilter(capacity=n, fp_rate=0.01)
    for i in range(n):
        bf.add(f"train:{i}")
    fp_count = sum(
        bf.might_exist(f"test:{i}") for i in range(n)
    )
    fp_rate = fp_count / n
    assert fp_rate < 0.05, f"FP rate too high: {fp_rate:.2%}"


def test_fill_ratio_increases():
    bf = BloomFilter(capacity=1000)
    assert bf.fill_ratio == 0.0
    for i in range(100):
        bf.add(f"k{i}")
    assert bf.fill_ratio > 0.0


def test_optimal_parameters():
    bf = BloomFilter(capacity=10_000, fp_rate=0.01)
    # At 1% FP rate: m ≈ 9.6 * n, k ≈ 7
    assert bf.m > 9 * 10_000
    assert 5 <= bf.k <= 10
