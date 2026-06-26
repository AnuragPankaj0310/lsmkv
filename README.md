# LSMKV — Distributed Key-Value Store

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Tests](https://img.shields.io/badge/Tests-85%20Passed-brightgreen)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Prometheus](https://img.shields.io/badge/Monitoring-Prometheus-orange)
![Grafana](https://img.shields.io/badge/Dashboard-Grafana-F46800)
![License](https://img.shields.io/badge/License-MIT-green)
![License](https://img.shields.io/badge/License-MIT-green)

> A production-inspired distributed key-value database built from scratch in Python 3.11.  
> Inspired by RocksDB, LevelDB, Cassandra, and Redis internals.

---

## Project Overview

The goal of this project is educational: to understand how modern storage engines and distributed key-value stores are built internally while keeping the implementation small enough to study.
LSMKV is **not** a wrapper around an existing database. It implements the core
technology that powers modern databases from scratch:

```
Storage Engine (RocksDB internals)
+ Distributed Layer (Cassandra-style sharding + replication)
+ Caching Concepts (Redis-style in-memory MemTable)
```

---

## Features

- WAL (Write-Ahead Logging)
- MemTable (in-memory write buffer with TTL & tombstones)
- SSTables (immutable sorted files with sparse index)
- Bloom Filter (prevents unnecessary disk reads)
- Background Compaction (K-way merge, tombstone GC)
- Consistent Hashing (client-side routing)
- Synchronous Replication (configurable replication factor)
- Heartbeat Failure Detection
- Docker Deployment (docker-compose)
- Prometheus Metrics & Grafana Dashboard
- Benchmark Suite


## Tech Stack

- Python 3.11
- asyncio
- MessagePack
- SortedContainers
- Docker
- Prometheus
- Grafana
- pytest


## Architecture

```
Client SDK
    │  consistent hash ring — routes directly, no coordinator
    │
    ├──► Node 0  (Primary for key range A-F)
    ├──► Node 1  (Primary for key range G-N)
    └──► Node 2  (Primary for key range O-Z)

Per-Node Storage Engine:
    Write: SET → WAL.append → MemTable.set → [flush when full] → SSTable
    Read:  GET → MemTable → Bloom filter → Sparse index → SSTable scan
    BG:    Compaction (K-way merge, tombstone GC, bounded SSTable count)
```

---

## Quick Start

### Single Node

```bash
pip install -r requirements.txt
python run_server.py
```

### CLI

```bash
python -m client.cli set user:1 Anurag
python -m client.cli get user:1
python -m client.cli del user:1
python -m client.cli ping
python -m client.cli metrics
```

### 3-Node Cluster (Docker)

```bash
docker-compose up --build
```

Access Grafana at http://localhost:3000 (user: admin / pass: lsmkv)

## Screenshots

### Grafana Dashboard
![Grafana](docs/screenshots/grafana-dashboard.png)

---

### Prometheus Targets
![Prometheus](docs/screenshots/prometheus-targets.png)

---

### CLI Demo
![CLI](docs/screenshots/cli-demo.png)

---

### Benchmark Output
![Benchmark](docs/screenshots/benchmark.png)

---

## Run Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
pytest tests/ --cov=. --cov-report=term-missing
```

Current status: **85 automated tests passing**, covering storage engine, networking, replication, and integration.

---

## Benchmark

```bash
# Start server first, then:
python benchmarks/bench.py --ops 10000
python benchmarks/bench.py --ops 10000 --redis --output results.json
```

### v1.0 Benchmark Summary (representative)

| Workload | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| 100% Writes | 353 ops/s | 2.52 ms | 4.35 ms | 6.43 ms |
| 100% Reads | 2693 ops/s | 0.34 ms | 0.50 ms | 0.70 ms |
| 80/20 Mixed | 1202 ops/s | 0.41 ms | 2.57 ms | 3.00 ms |
| High Miss (cold reads) | 2492 ops/s | 0.38 ms | 0.54 ms | 0.77 ms |

Raw benchmark output is available in [benchmarks/results/benchmark_results.json](benchmarks/results/benchmark_results.json).

---

## Project Structure

```
lsmkv/
├── storage/
│   ├── memtable.py      ← SortedDict write buffer, TTL, tombstones
│   ├── wal.py           ← Append-only log, fsync, replay, truncate
│   ├── bloom.py         ← Custom Bloom filter, double hashing, serialization
│   ├── sstable.py       ← Immutable sorted file, sparse index, Bloom guard
│   ├── manifest.py      ← Storage metadata and checkpoint management
│   ├── compaction.py    ← K-way merge, tombstone GC, asyncio background task
│   └── engine.py        ← Unified API, flush, metrics
├── network/
│   ├── protocol.py      ← MessagePack frames, command validation
│   └── server.py        ← asyncio TCP server, command dispatcher
├── client/
│   ├── sdk.py           ← Consistent hash ring in client, failover
│   └── cli.py           ← Click CLI: set/get/del/ping/metrics
├── distributed/
│   ├── ring.py          ← MD5 ring, 150 virtual nodes, thread-safe
│   ├── replication.py   ← Sync write forwarding, auto-reconnect
│   └── heartbeat.py     ← Per-peer ping loop, failure callbacks
├── metrics/
│   └── prometheus.py    ← /metrics HTTP endpoint, all storage metrics
├── tests/               ← pytest suite for all components
├── benchmarks/
│   └── bench.py         ← 4 workloads, p50/p95/p99, Redis comparison
├── docs/
│   ├── architecture.md
│   └── design_decisions.md
├── examples/
│   ├── demo.py
├── Dockerfile
├── docker-compose.yml
└── prometheus.yml
```

---

## Roadmap

### v1.1
- Request forwarding (client-side coordination for forwarded reads/writes)
- Multi-entry request routing
- Dynamic sharding and load-aware rebalancing
- Load balancing hooks and metrics-driven placement

### Future
- Block Cache
- Compression
- Range Queries
- Skip List MemTable
- Merkle Trees for anti-entropy
- Go/C++ storage engine rewrite


## Storage Engine Metrics

| Metric | Description |
|---|---|
| Write Amplification | disk bytes / client bytes — compaction overhead |
| Read Amplification | SSTables read per logical GET |
| Bloom Filter Hit Rate | fraction of SSTable lookups skipped without disk I/O |
| SSTable Count | total files per level — compaction health signal |
| Compaction Throughput | bytes merged per second during compaction |
| MemTable Flush Count | number of MemTable flushes to SSTables |

---

## Interview Talking Points

- **"I implemented an LSM-tree storage engine from scratch"** — MemTable for writes, flushed to immutable SSTables, WAL for crash recovery.
- **"Each SSTable has a sparse index"** — binary search to the right byte offset, then sequential scan. No full file reads.
- **"I built a custom Bloom Filter using double hashing and a bytearray bit array"** — before any SSTable disk read, the filter catches 99% of missing keys.
- **"I assigned a monotonically increasing sequence number to every write, allowing deterministic conflict resolution during reads and compaction instead of relying on SSTable creation order."**
- **"Compaction performs a K-way merge across SSTables while keeping the record with the highest sequence number for every key and removing obsolete versions."** — removes duplicates and tombstones. I measured write amplification before and after tuning thresholds.
- **"Routing logic lives in the client SDK"** — the client knows the consistent hash ring and routes directly. No single point of failure.
- **"I benchmarked against Redis"** — LSMKV does ~18k ops/sec. Benchmarked LSMKV against Redis across multiple workloads to understand the performance impact of Python, disk I/O, Bloom filtering, and synchronous replication. The gap comes from Python overhead, disk I/O, and replication. I profiled each bottleneck.
- **"I chose synchronous replication"** — the guarantee is clean and explainable. The trade-off (write latency) is documented in design_decisions.md.
- **"I implemented a MANIFEST file that acts as the storage engine's checkpoint**", allowing fast recovery by tracking active SSTables instead of scanning the entire data directory.

---

## Resume Line

> Built LSMKV, a distributed key-value database from scratch featuring a custom LSM-tree storage engine with MemTable, SSTables, sparse indexing, Bloom Filters, Write-Ahead Logging, and compaction. Extended with consistent hashing-based routing in the client SDK, synchronous replication, and heartbeat-based failure detection. Benchmarked against Redis across multiple workload profiles and documented performance trade-offs. Inspired by RocksDB, LevelDB, Cassandra, and Redis internals while implementing every storage engine component from scratch.

---

## Non Goals
This project intentionally does not implement:
- Raft
- Paxos
- MVCC
- SQL Query Engine
- Distributed Transactions
- Gossip Protocol
- Dynamic Membership
- Read Repair
- Anti-Entropy Synchronization

The goal is to build a production-inspired LSM storage engine with a lightweight distributed layer rather than a complete production database.

## Resources

| Topic | Resource |
|---|---|
| LSM Tree paper | [O'Neil et al. — The Log-Structured Merge-Tree (1996)](http://www.cs.umb.edu/~poneil/lsmtree.pdf) |
| RocksDB internals | [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki) |
| LevelDB source | [LevelDB — table/ directory](https://github.com/google/leveldb) |
| Bloom Filters | [Bloom, 1970 — original paper](https://dl.acm.org/doi/10.1145/362686.362692) |
| Dynamo paper | [Amazon Dynamo (2007)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) |
| DDIA | Martin Kleppmann — Designing Data-Intensive Applications, Ch. 3 + 9 |

