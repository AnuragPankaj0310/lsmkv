"""
LSMKV Benchmark Suite — measure performance against real workloads.

Metrics:
  - SET throughput (keys/sec)
  - GET throughput (keys/sec)
  - P50, P95, P99 latency (ms)
  - Recovery time from WAL replay
  - Compaction throughput
"""

from __future__ import annotations

import asyncio
import time
import statistics
from tempfile import TemporaryDirectory

from storage.engine import StorageEngine


class BenchmarkTimer:
    """Context manager for measuring operation latency."""

    def __init__(self):
        self.start: float = 0.0
        self.end: float = 0.0

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()

    @property
    def elapsed_ms(self) -> float:
        return (self.end - self.start) * 1000


async def benchmark_set_throughput(num_keys: int = 100000) -> dict:
    """
    Benchmark SET operation throughput.

    Returns:
      {
        "throughput_keys_per_sec": float,
        "total_time_sec": float,
        "num_keys": int
      }
    """
    with TemporaryDirectory() as tmp_dir:
        engine = StorageEngine(
            data_dir=tmp_dir,
            memtable_size_bytes=64 * 1024 * 1024,  # 64MB memtable
            l0_compaction_trigger=4,
            compaction_interval=999.0,  # disable auto-compaction
        )
        await engine.open()

        start = time.perf_counter()

        for i in range(num_keys):
            key = f"key:{i:08d}"
            value = f"value_{i}".encode() * 10  # ~100 bytes
            await engine.set(key, value)

        elapsed = time.perf_counter() - start
        throughput = num_keys / elapsed

        await engine.close()

        return {
            "throughput_keys_per_sec": throughput,
            "total_time_sec": elapsed,
            "num_keys": num_keys,
        }


async def benchmark_get_throughput(num_keys: int = 100000) -> dict:
    """
    Benchmark GET operation throughput (after bulk load).

    Returns:
      {
        "throughput_keys_per_sec": float,
        "total_time_sec": float,
        "num_keys": int
      }
    """
    with TemporaryDirectory() as tmp_dir:
        engine = StorageEngine(
            data_dir=tmp_dir,
            memtable_size_bytes=64 * 1024 * 1024,
            l0_compaction_trigger=4,
            compaction_interval=999.0,
        )
        await engine.open()

        # Pre-populate
        print(f"  Pre-populating {num_keys} keys...")
        for i in range(num_keys):
            key = f"key:{i:08d}"
            value = f"value_{i}".encode() * 10
            await engine.set(key, value)

        # Flush to SSTables
        await engine.flush()

        # Benchmark reads
        start = time.perf_counter()

        for i in range(num_keys):
            key = f"key:{i:08d}"
            value = await engine.get(key)
            assert value is not None

        elapsed = time.perf_counter() - start
        throughput = num_keys / elapsed

        await engine.close()

        return {
            "throughput_keys_per_sec": throughput,
            "total_time_sec": elapsed,
            "num_keys": num_keys,
        }


async def benchmark_scalability(key_counts: list[int]) -> dict:
    """
    Benchmark SET throughput at multiple scales to show scalability.

    Args:
      key_counts: List of scales to benchmark (e.g., [1000, 10000, 100000])

    Returns:
      {
        1000: {"throughput_keys_per_sec": float, ...},
        10000: {"throughput_keys_per_sec": float, ...},
        ...
      }
    """
    results = {}
    for num_keys in key_counts:
        print(f"  Running scalability test for {num_keys:,} keys...")
        result = await benchmark_set_throughput(num_keys)
        results[num_keys] = result
    return results


async def benchmark_latency(num_operations: int = 10000) -> dict:
    """
    Benchmark latency percentiles (P50, P95, P99) for SET operations.

    Returns:
      {
        "p50_ms": float,
        "p95_ms": float,
        "p99_ms": float,
        "min_ms": float,
        "max_ms": float,
      }
    """
    with TemporaryDirectory() as tmp_dir:
        engine = StorageEngine(
            data_dir=tmp_dir,
            memtable_size_bytes=64 * 1024 * 1024,
            l0_compaction_trigger=4,
            compaction_interval=999.0,
        )
        await engine.open()

        latencies = []

        for i in range(num_operations):
            key = f"lat:{i:08d}"
            value = f"value_{i}".encode() * 10

            with BenchmarkTimer() as timer:
                await engine.set(key, value)

            latencies.append(timer.elapsed_ms)

        await engine.close()

        latencies.sort()

        return {
            "p50_ms": statistics.median(latencies),
            "p95_ms": latencies[int(len(latencies) * 0.95)],
            "p99_ms": latencies[int(len(latencies) * 0.99)],
            "min_ms": min(latencies),
            "max_ms": max(latencies),
            "num_operations": num_operations,
        }


async def benchmark_recovery_time(num_keys: int = 100000) -> dict:
    """
    Benchmark recovery time from WAL replay.

    Scenario:
      1. Write keys with engine open
      2. Close engine (don't flush)
      3. Measure time to reopen and recover from WAL

    Returns:
      {
        "recovery_time_sec": float,
        "num_keys_recovered": int
      }
    """
    with TemporaryDirectory() as tmp_dir:
        # Phase 1: Write without flushing to force WAL recovery
        engine1 = StorageEngine(
            data_dir=tmp_dir,
            memtable_size_bytes=64 * 1024 * 1024,
            l0_compaction_trigger=4,
            compaction_interval=999.0,
        )
        await engine1.open()

        # Write 50% of keys
        for i in range(num_keys // 2):
            key = f"key:{i:08d}"
            value = f"value_{i}".encode() * 10
            await engine1.set(key, value)

        # Close without flush (leaves keys in WAL)
        await engine1.close()

        # Phase 2: Reopen and measure recovery
        engine2 = StorageEngine(
            data_dir=tmp_dir,
            memtable_size_bytes=64 * 1024 * 1024,
            l0_compaction_trigger=4,
            compaction_interval=999.0,
        )

        start = time.perf_counter()
        await engine2.open()
        recovery_time = time.perf_counter() - start

        # Verify recovered data
        verified = 0
        for i in range(num_keys // 2):
            key = f"key:{i:08d}"
            value = await engine2.get(key)
            if value is not None:
                verified += 1

        await engine2.close()

        return {
            "recovery_time_sec": recovery_time,
            "num_keys_recovered": verified,
        }


async def benchmark_compaction_throughput(num_keys: int = 50000) -> dict:
    """
    Benchmark compaction throughput (keys per second during compaction).

    Scenario:
      1. Write keys to trigger L0 compaction
      2. Wait for compaction to complete (deterministic wait for idle)
      3. Measure throughput

    Returns:
      {
        "compaction_throughput_keys_per_sec": float,
        "compaction_time_sec": float,
      }
    """
    with TemporaryDirectory() as tmp_dir:
        engine = StorageEngine(
            data_dir=tmp_dir,
            memtable_size_bytes=4
            * 1024
            * 1024,  # Smaller memtable to trigger compaction
            l0_compaction_trigger=2,
            compaction_interval=0.1,  # Frequent checks
        )
        await engine.open()

        # Write enough keys to trigger multiple compactions
        print(f"  Writing {num_keys} keys to trigger compaction...")
        for i in range(num_keys):
            key = f"key:{i:08d}"
            value = f"value_{i}".encode() * 10
            await engine.set(key, value)

        # Wait deterministically for compaction to complete
        # (instead of arbitrary sleep)
        print("  Waiting for compaction to idle...")
        start = time.perf_counter()
        max_wait = 60.0  # Safety timeout

        while time.perf_counter() - start < max_wait:
            metrics = engine.metrics_snapshot()
            sstable_count = metrics.get("sstable_count", 0)

            # Heuristic: if L0 (level 0) SSTables are minimal and
            # compaction run count is stable, we're idle
            if sstable_count < 5:
                await asyncio.sleep(0.5)
                metrics_after = engine.metrics_snapshot()
                if metrics_after.get("sstable_count", 0) == sstable_count:
                    # Stable → compaction is idle
                    break

            await asyncio.sleep(0.1)

        compaction_time = time.perf_counter() - start

        await engine.close()

        # Estimate throughput based on total keys and time
        throughput = num_keys / max(compaction_time, 1.0)

        return {
            "compaction_throughput_keys_per_sec": throughput,
            "compaction_time_sec": compaction_time,
            "num_keys": num_keys,
        }


async def run_all_benchmarks():
    """Run all benchmarks and print results."""
    print("\n" + "=" * 70)
    print("LSMKV Benchmark Suite")
    print("=" * 70)

    # Benchmark 1: SET throughput (single scale)
    print("\n[1/6] Benchmarking SET throughput (100k keys)...")
    set_results = await benchmark_set_throughput(100000)
    print(f"  SET throughput: {set_results['throughput_keys_per_sec']:,.0f} keys/sec")
    print(f"  Total time: {set_results['total_time_sec']:.2f} sec")

    # Benchmark 1b: SET throughput (scalability curve)
    print("\n[1b/6] Benchmarking SET throughput scalability...")
    scale_results = await benchmark_scalability([1000, 10000, 100000])
    print("  Scalability results:")
    for num_keys, metrics in scale_results.items():
        print(
            f"    {num_keys:>7,} keys: {metrics['throughput_keys_per_sec']:>10,.0f} keys/sec"
        )

    # Benchmark 2: GET throughput
    print("\n[2/6] Benchmarking GET throughput (100k keys)...")
    get_results = await benchmark_get_throughput(100000)
    print(f"  GET throughput: {get_results['throughput_keys_per_sec']:,.0f} keys/sec")
    print(f"  Total time: {get_results['total_time_sec']:.2f} sec")

    # Benchmark 3: Latency
    print("\n[3/6] Benchmarking latency percentiles (10k operations)...")
    latency_results = await benchmark_latency(10000)
    print(f"  P50: {latency_results['p50_ms']:.2f} ms")
    print(f"  P95: {latency_results['p95_ms']:.2f} ms")
    print(f"  P99: {latency_results['p99_ms']:.2f} ms")
    print(f"  Min: {latency_results['min_ms']:.2f} ms")
    print(f"  Max: {latency_results['max_ms']:.2f} ms")

    # Benchmark 4: Recovery
    print("\n[4/6] Benchmarking recovery time...")
    recovery_results = await benchmark_recovery_time(100000)
    print(f"  Recovery time: {recovery_results['recovery_time_sec']:.2f} sec")
    print(f"  Keys recovered: {recovery_results['num_keys_recovered']}")

    # Benchmark 5: Compaction
    print("\n[5/6] Benchmarking compaction throughput...")
    compaction_results = await benchmark_compaction_throughput(50000)
    print(
        f"  Compaction throughput: {compaction_results['compaction_throughput_keys_per_sec']:,.0f} keys/sec"
    )
    print(f"  Compaction time: {compaction_results['compaction_time_sec']:.2f} sec")

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(
        f"SET throughput (100k):  {set_results['throughput_keys_per_sec']:>12,.0f} keys/sec"
    )
    print(
        f"GET throughput:         {get_results['throughput_keys_per_sec']:>12,.0f} keys/sec"
    )
    print(f"P50 latency:            {latency_results['p50_ms']:>12.2f} ms")
    print(f"P95 latency:            {latency_results['p95_ms']:>12.2f} ms")
    print(f"P99 latency:            {latency_results['p99_ms']:>12.2f} ms")
    print(f"Recovery time:          {recovery_results['recovery_time_sec']:>12.2f} sec")
    print(
        f"Compaction throughput:  {compaction_results['compaction_throughput_keys_per_sec']:>12,.0f} keys/sec"
    )
    print("\nScalability (SET throughput):")
    for num_keys in sorted(scale_results.keys()):
        throughput = scale_results[num_keys]["throughput_keys_per_sec"]
        print(f"  {num_keys:>7,} keys:    {throughput:>12,.0f} keys/sec")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run_all_benchmarks())
