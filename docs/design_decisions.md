# Design Decisions

This document explains the major engineering decisions behind **LSMKV**. Each section discusses the motivation, benefits, trade-offs, and the type of interview question the decision is intended to answer.

---

# 1. Why LSM Tree Instead of B-Tree?

**Interview Question:** *Why did you choose an LSM Tree?*

Traditional B-Trees update data in-place, requiring random disk reads and writes. While suitable for read-heavy workloads, random writes become increasingly expensive as datasets grow.

LSMKV uses a **Log-Structured Merge (LSM) Tree**, which converts random writes into sequential writes.

### Write Path

```text
Client Write
      │
      ▼
Write-Ahead Log (WAL)
      │
      ▼
   MemTable
      │
      ▼
 SSTable Flush
      │
      ▼
Background Compaction
```

### Benefits

* High write throughput
* Sequential disk I/O
* Better SSD performance
* Crash-safe with WAL

### Trade-off

Reads may need to examine multiple SSTables. This is mitigated using Bloom Filters and Sparse Indexes.

---

# 2. Why Immutable SSTables?

**Interview Question:** *Why not update SSTables in place?*

Updating files in place requires locking and introduces crash consistency challenges.

Instead, LSMKV stores data in immutable SSTables.

### Benefits

* Lock-free reads
* Simple concurrent access
* Atomic file replacement
* Safe background compaction
* Easier crash recovery

Compaction creates new SSTables and removes obsolete ones without modifying existing files.

---

# 3. Why Sparse Indexes?

**Interview Question:** *How do you avoid loading huge indexes into memory?*

A dense index stores an offset for every key, consuming significant memory.

LSMKV instead stores one index entry every fixed number of keys.

### Benefits

* Small memory footprint
* Fast binary search
* Short sequential scan
* Excellent cache locality

This approach is similar to LevelDB and RocksDB.

---

# 4. Why Bloom Filters?

**Interview Question:** *How do you reduce unnecessary disk reads?*

Without Bloom Filters, every missing key requires checking every SSTable.

```
GET key
   │
   ▼
Bloom Filter
   │
   ├── Definitely Not Present → Skip SSTable
   └── Possibly Present       → Search SSTable
```

### Benefits

* Eliminates most unnecessary disk reads
* Faster negative lookups
* Low memory overhead
* Configurable false-positive rate

---

# 5. Why Sequence Numbers?

**Interview Question:** *How do you determine the newest version of a key?*

The same key may exist in multiple SSTables.

Example:

```text
Seq 101 → SET user1 Alice
Seq 102 → SET user1 Bob
```

The storage engine always keeps the record with the highest sequence number during reads and compaction.

### Benefits

* Deterministic conflict resolution
* Immutable storage
* Simple merge logic

### Trade-off

Each record stores an additional sequence number.

---

# 6. Why Write-Ahead Logging (WAL)?

**Interview Question:** *What happens if the server crashes before flushing the MemTable?*

Every write follows this order:

```text
Append to WAL
      │
      ▼
fsync()
      │
      ▼
Update MemTable
      │
      ▼
Return Success
```

If a crash occurs:

1. Load the MANIFEST.
2. Open active SSTables.
3. Replay the remaining WAL entries.

### Guarantee

No acknowledged write is lost.

---

# 7. Why Consistent Hashing?

**Interview Question:** *Why not use `hash(key) % number_of_nodes`?*

Modulo hashing redistributes nearly every key whenever the cluster size changes.

Consistent Hashing places both nodes and keys on a logical hash ring.

### Benefits

* Minimal key movement
* Even key distribution
* Horizontal scalability
* Efficient node addition/removal

Only the partitions owned by the affected node are redistributed.

---

# 8. Why Client-Side Routing?

**Interview Question:** *Why doesn't the system use a coordinator node?*

A coordinator introduces:

* Additional network hop
* Single point of failure
* Throughput bottleneck

Instead, the client computes:

```text
Key
 │
 ▼
Hash Ring
 │
 ▼
Primary Node
```

### Benefits

* Lower latency
* No central bottleneck
* Better scalability

### Trade-off

Clients must know the cluster topology.

---

# 9. Why Synchronous Replication?

**Interview Question:** *Why synchronous instead of asynchronous replication?*

LSMKV acknowledges writes only after all configured replicas successfully persist the update.

```text
Client
  │
  ▼
Primary
  │
  ▼
Replicas
  │
  ▼
ACK
```

### Benefits

* Strong consistency
* Predictable durability
* Simple failure handling

### Trade-off

Higher write latency compared to asynchronous replication.

---

# 10. Why Streaming Key Migration?

**Interview Question:** *How do you rebalance data without stopping the cluster?*

Migrating the complete dataset at once would consume large amounts of memory and block operations.

LSMKV migrates keys in batches.

```text
Export Batch
     │
     ▼
Transfer
     │
     ▼
Import
     │
     ▼
Next Batch
```

### Benefits

* Constant memory usage
* Large dataset support
* Progress tracking
* Retry failed batches

---

# 11. Why Static Cluster Configuration?

**Interview Question:** *Why not implement automatic node discovery?*

Dynamic cluster membership requires distributed consensus or gossip protocols.

LSMKV intentionally uses a static configuration file to keep the focus on storage engine and distributed data management concepts.

### Benefits

* Simple deployment
* Easy debugging
* Deterministic testing

---

# 12. Why a MANIFEST File?

**Interview Question:** *Why store metadata separately?*

Without a MANIFEST, every startup would require scanning every SSTable.

The MANIFEST records:

* Active SSTables
* SSTable levels
* Latest checkpoint

Recovery process:

```text
Load MANIFEST
      │
      ▼
Open SSTables
      │
      ▼
Replay WAL
```

### Benefits

* Faster startup
* Efficient recovery
* Simplified metadata management

---

# 13. Why Prometheus and Grafana?

**Interview Question:** *Why expose metrics instead of relying only on logs?*

Logs describe individual events.

Metrics describe system behavior over time.

LSMKV exports metrics including:

* Logical Keys
* Cluster Storage
* SSTable Count
* MemTable Size
* MemTable Entries
* Active Connections

Prometheus periodically scrapes these metrics, while Grafana provides real-time dashboards.

### Benefits

* Operational visibility
* Historical monitoring
* Dashboarding
* Alerting support

---

# 14. Why Docker Compose?

**Interview Question:** *Why containerize the system?*

The complete deployment consists of:

* Three storage nodes
* Prometheus
* Grafana

Docker Compose provides:

* One-command deployment
* Consistent environments
* Simplified networking
* Easy reproducibility

```bash
docker compose up --build
```

---

# Summary

LSMKV intentionally favors **simple, explainable engineering decisions** over maximum feature count.

Key design choices include:

* LSM Trees instead of B-Trees
* Immutable SSTables
* Sparse Indexes
* Bloom Filters
* Write-Ahead Logging
* Consistent Hashing
* Client-Side Routing
* Synchronous Replication
* Streaming Key Migration
* Static Cluster Membership
* Prometheus Monitoring
* Docker-Based Deployment

These choices demonstrate the core architectural ideas used by modern distributed storage systems such as **LevelDB, RocksDB, Cassandra, DynamoDB, and Bigtable**, while keeping the implementation approachable, maintainable, and suitable for learning distributed systems and storage engine internals.
