# LSMKV Benchmark Guide

## Overview

This document describes the benchmarking methodology used to evaluate **LSMKV**, a distributed Log-Structured Merge (LSM) Tree key-value store.

The benchmark suite measures:

* Throughput (operations/second)
* Latency (P50, P95, P99)
* Mixed workload performance
* High miss-rate behavior
* Recovery performance
* Compaction performance
* Scalability

Rather than competing with production databases, these benchmarks demonstrate the operational characteristics of the storage engine and distributed cluster.

---

# Benchmark Suites

LSMKV includes two benchmark programs.

| Script                      | Purpose                                  |
| --------------------------- | ---------------------------------------- |
| `benchmarks/bench.py`       | End-to-end distributed cluster benchmark |
| `benchmarks/bench_lsmkv.py` | Storage engine benchmark                 |

---

# Test Environment

Replace these values with your own hardware before publishing.

| Component        | Value          |
| ---------------- | -------------- |
| Processor        | *Your CPU*     |
| Memory           | *Your RAM*     |
| Storage          | SSD            |
| Operating System | Windows 11     |
| Python           | 3.10+          |
| Docker           | Docker Compose |

---

# Cluster Configuration

| Setting        | Value                                     |
| -------------- | ----------------------------------------- |
| Nodes          | 3                                         |
| Replication    | Configurable (from cluster configuration) |
| Routing        | Consistent Hashing                        |
| Storage Engine | LSM Tree                                  |
| Serialization  | MessagePack                               |
| Monitoring     | Prometheus + Grafana                      |

---

# Distributed Cluster Benchmarks

Run:

```bash
python benchmarks/bench.py --ops 10000
```

This executes four workloads.

## 1. 100% Writes

Measures sequential write throughput.

Metrics:

* Throughput
* P50 latency
* P95 latency
* P99 latency

---

## 2. 100% Reads (Warm Cache)

Measures lookup performance after preloading data.

Metrics:

* Throughput
* Read latency

---

## 3. 80% Read / 20% Write

Simulates a realistic application workload.

Metrics:

* Mixed throughput
* Mixed latency

---

## 4. High Miss Rate (30%)

Exercises Bloom Filters by querying missing keys.

Metrics:

* Lookup latency
* Negative lookup performance

---

# Storage Engine Benchmarks

Run:

```bash
python benchmarks/bench_lsmkv.py
```

This benchmark evaluates internal storage engine performance.

It measures:

* SET throughput
* GET throughput
* Latency (P50 / P95 / P99)
* WAL recovery time
* Compaction throughput
* Scalability

---

# Metrics Collected

## Performance

* Operations per second
* Total execution time
* P50 latency
* P95 latency
* P99 latency

## Storage Engine

* SSTable Count
* Write Amplification
* Read Amplification
* Bloom Filter Hit Rate
* Compaction Throughput
* Compaction Runs

## Memory

* MemTable Entries
* MemTable Size

## Storage

* Logical Keys
* Disk Usage

## Cluster

* Active Connections
* Online Nodes
* Cluster Health

---

# Monitoring During Benchmarks

Start the cluster:

```bash
docker compose up -d
```

Run the benchmark:

```bash
python benchmarks/bench.py --ops 10000
```

View cluster metrics:

```bash
python -m client.cli --config config.host.json metrics
```

View cluster status:

```bash
python -m client.cli --config config.host.json stats
```

Prometheus:

```text
http://localhost:9090
```

Grafana:

```text
http://localhost:3000
```

---

# Saving Benchmark Results

Benchmark results can be exported as JSON.

Example:

```bash
python benchmarks/bench.py \
    --ops 10000 \
    --output benchmarks/results/benchmark_results_v2.json
```

The generated JSON contains:

* Throughput
* Total execution time
* P50 latency
* P95 latency
* P99 latency

This enables reproducible benchmarking and comparison across versions.

---

# Expected Storage Engine Behavior

During write-heavy workloads:

* WAL grows sequentially.
* MemTable accumulates writes.
* MemTable flushes create immutable SSTables.
* Background compaction merges SSTables.
* Bloom Filters reduce unnecessary disk reads.
* Disk usage grows as data accumulates.

These behaviors are expected for an LSM-tree storage engine.

---

# Scalability

LSMKV scales horizontally using consistent hashing.

Adding a node triggers:

1. Hash ring update
2. Ownership recalculation
3. Streaming key migration
4. Balanced key distribution

Only the affected partitions are redistributed, minimizing data movement.

---

# Limitations

This project prioritizes correctness, clarity, and educational value.

The following production optimizations are intentionally omitted:

* Compression
* Block cache
* SIMD optimizations
* Parallel compaction
* Zero-copy networking
* Distributed consensus

---

# Reproducing Results

1. Clone the repository.
2. Install dependencies.
3. Start the Docker cluster.
4. Execute the benchmark scripts.
5. Monitor Prometheus or Grafana.
6. Compare the generated JSON results.

---

# Summary

The benchmark suite demonstrates the behavior of LSMKV under representative workloads, measuring throughput, latency, recovery, compaction, and scalability. The included scripts and JSON output provide a reproducible framework for evaluating future versions of the storage engine.
