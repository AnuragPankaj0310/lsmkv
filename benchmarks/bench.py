"""
Benchmark — LSMKV vs Redis (Phase 6).

Runs 4 workloads:
  1. 100% writes  (100k SET operations)
  2. 100% reads   (100k GET, warm cache)
  3. 80/20 mix    (80% GET / 20% SET)
  4. High miss    (30% keys not in store — tests Bloom filter)

Metrics reported:
  - Throughput (ops/sec)
  - p50 / p95 / p99 latency (ms)
  - Total time (s)

Usage:
  # Benchmark LSMKV only (default):
  python benchmarks/bench.py --host 127.0.0.1 --port 7001 --ops 10000

  # Include Redis comparison (Redis must be running on localhost:6379):
  python benchmarks/bench.py --ops 10000 --redis

  # Save results to JSON:
  python benchmarks/bench.py --ops 10000 --output results.json
"""

from __future__ import annotations
# ruff: noqa: E402

from pathlib import Path
import sys

# Add project root to Python path so the script can be run directly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import asyncio
import json
import random
import string
import time
from typing import Optional

from client.sdk import LsmkvClient


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _rand_key(existing: list[str] = None, miss_rate: float = 0.0) -> str:
    if existing and random.random() > miss_rate:
        return random.choice(existing)
    return "bench:" + "".join(random.choices(string.ascii_lowercase, k=12))


def _rand_value(size: int = 64) -> bytes:
    return random.randbytes(size)


def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    return sorted_data[min(idx, len(sorted_data) - 1)]


def _print_results(name: str, latencies: list[float], elapsed: float, ops: int) -> dict:
    p50 = _percentile(latencies, 50) * 1000
    p95 = _percentile(latencies, 95) * 1000
    p99 = _percentile(latencies, 99) * 1000
    tput = ops / elapsed if elapsed > 0 else 0

    print(f"\n{'─' * 50}")
    print(f"  {name}")
    print(f"{'─' * 50}")
    print(f"  Operations : {ops:,}")
    print(f"  Total time : {elapsed:.2f}s")
    print(f"  Throughput : {tput:,.0f} ops/sec")
    print(f"  p50 latency: {p50:.2f} ms")
    print(f"  p95 latency: {p95:.2f} ms")
    print(f"  p99 latency: {p99:.2f} ms")

    return {
        "name": name,
        "ops": ops,
        "elapsed_sec": round(elapsed, 3),
        "throughput_ops_sec": round(tput, 1),
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
    }


# ---------------------------------------------------------------------------
# LSMKV workloads
# ---------------------------------------------------------------------------


async def bench_write(client: LsmkvClient, ops: int) -> dict:
    latencies = []
    t0 = time.perf_counter()
    for i in range(ops):
        key = f"bench:write:{i:07d}"
        t = time.perf_counter()
        await client.set(key, _rand_value())
        latencies.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - t0
    return _print_results("LSMKV — 100% Writes", latencies, elapsed, ops)


async def bench_read(client: LsmkvClient, keys: list[str], ops: int) -> dict:
    latencies = []
    t0 = time.perf_counter()
    for _ in range(ops):
        key = random.choice(keys)
        t = time.perf_counter()
        await client.get(key)
        latencies.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - t0
    return _print_results("LSMKV — 100% Reads (warm)", latencies, elapsed, ops)


async def bench_mixed(client: LsmkvClient, keys: list[str], ops: int) -> dict:
    latencies = []
    t0 = time.perf_counter()
    for i in range(ops):
        t = time.perf_counter()
        if random.random() < 0.2:  # 20% writes
            await client.set(_rand_key(), _rand_value())
        else:
            await client.get(random.choice(keys))
        latencies.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - t0
    return _print_results("LSMKV — 80% Read / 20% Write", latencies, elapsed, ops)


async def bench_high_miss(client: LsmkvClient, keys: list[str], ops: int) -> dict:
    """30% of GETs are for keys that don't exist → exercises Bloom filter."""
    latencies = []
    t0 = time.perf_counter()
    for _ in range(ops):
        t = time.perf_counter()
        if random.random() < 0.30:
            await client.get(f"miss:{random.getrandbits(32)}")
        else:
            await client.get(random.choice(keys))
        latencies.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - t0
    return _print_results("LSMKV — High Miss Rate (30%)", latencies, elapsed, ops)


# ---------------------------------------------------------------------------
# Redis workload (optional comparison)
# ---------------------------------------------------------------------------


async def bench_redis(host: str, port: int, ops: int) -> Optional[list[dict]]:
    try:
        import redis.asyncio as aioredis
    except ImportError:
        print("\n[Redis] redis-py not installed. Skipping Redis benchmark.")
        return None

    try:
        r = aioredis.Redis(host=host, port=port, decode_responses=False)
        await r.ping()
    except Exception as exc:
        print(f"\n[Redis] Cannot connect to {host}:{port}: {exc}. Skipping.")
        return None

    results = []
    keys = [f"bench:{i:07d}" for i in range(ops)]

    # Warm up
    for k in keys[: min(1000, ops)]:
        await r.set(k, _rand_value())

    # Write
    latencies = []
    t0 = time.perf_counter()
    for k in keys:
        t = time.perf_counter()
        await r.set(k, _rand_value())
        latencies.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - t0
    results.append(_print_results("Redis — 100% Writes", latencies, elapsed, ops))

    # Read
    latencies = []
    t0 = time.perf_counter()
    for _ in range(ops):
        t = time.perf_counter()
        await r.get(random.choice(keys))
        latencies.append(time.perf_counter() - t)
    elapsed = time.perf_counter() - t0
    results.append(_print_results("Redis — 100% Reads (warm)", latencies, elapsed, ops))

    await r.aclose()
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run_benchmark(
    host: str,
    port: int,
    ops: int,
    redis: bool,
    redis_host: str,
    redis_port: int,
    output: Optional[str],
) -> None:
    print(f"\nLSMKV Benchmark — {ops:,} ops per workload")
    print(f"Target: {host}:{port}")

    client = await LsmkvClient.create([f"{host}:{port}"], enable_heartbeat=False)

    all_results = []

    # Phase 1: Seed data for read benchmarks
    seed_keys = [f"bench:seed:{i:07d}" for i in range(min(ops, 10_000))]
    print(f"\n[Setup] Seeding {len(seed_keys):,} keys…")
    for k in seed_keys:
        await client.set(k, _rand_value())
    print("[Setup] Done.")

    # Workloads
    all_results.append(await bench_write(client, ops))
    all_results.append(await bench_read(client, seed_keys, ops))
    all_results.append(await bench_mixed(client, seed_keys, ops))
    all_results.append(await bench_high_miss(client, seed_keys, ops))

    await client.close()

    # Redis comparison
    if redis:
        redis_results = await bench_redis(redis_host, redis_port, ops)
        if redis_results:
            all_results.extend(redis_results)

    # Save to JSON
    if output:
        with open(output, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"\nResults saved to {output}")

    print("\n" + "=" * 50)
    print("Benchmark complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="LSMKV Benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7001)
    parser.add_argument("--ops", type=int, default=10_000)
    parser.add_argument("--redis", action="store_true", help="Also benchmark Redis")
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--output", default=None, help="Save results to JSON file")
    args = parser.parse_args()

    asyncio.run(
        run_benchmark(
            host=args.host,
            port=args.port,
            ops=args.ops,
            redis=args.redis,
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            output=args.output,
        )
    )


if __name__ == "__main__":
    main()
