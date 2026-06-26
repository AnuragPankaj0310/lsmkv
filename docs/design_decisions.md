# Design Decisions

This document records the key engineering trade-offs made in LSMKV.
Each decision is paired with the interview question it answers.

---

## 1. Why LSM Tree instead of B-Tree?

**Interview question:** *"Why did you choose an LSM tree?"*

B-Trees write data in-place on disk. A single SET can cause multiple random
page reads (to find the right leaf) and a random write. Random I/O on spinning
disk is 100–1000× slower than sequential I/O. On SSDs, random writes cause
write amplification at the device level.

LSM Trees convert random writes into sequential writes:
- Every write lands in the in-memory MemTable (no disk I/O)
- MemTable is periodically flushed as a new sequential SSTable file
- Compaction merges SSTables sequentially

**Trade-off:** Reads may need to check multiple SSTables. This is mitigated by
Bloom filters (skip 99% of SSTables for missing keys) and sparse indexes
(binary search to the right position without reading the whole file).

---

## 2. Why Sparse Index instead of Dense Index?

**Interview question:** *"How do you avoid loading entire SSTables into memory?"*

A dense index stores one offset per key — at 1M keys with 64-byte keys, that's
64MB of index per SSTable in memory. Unacceptable at scale.

A sparse index stores one offset every N keys (N=128 here). Binary search
on the sparse index gets you to within 128 keys of the target. A short
sequential scan finds the exact key.

Memory cost: 1M keys → 8K index entries → ~512KB. 125× reduction.

This is exactly how LevelDB and RocksDB handle reads internally.

---

## 3. Why Bloom Filters?

**Interview question:** *"What happens when you look up a key that doesn't exist?"*

Without Bloom filters: a missing key causes one read per SSTable file (O(N) disk
reads where N = SSTable count). At 100 SSTables, a miss costs 100 disk reads.

With Bloom filters: 99% of misses (at 1% false-positive rate) are caught in
memory by checking the per-SSTable Bloom filter. No disk I/O.

**Implementation detail:** Double hashing `h(key, i) = (h1(key) + i*h2(key)) % m`.
Built from scratch using a `bytearray` bit array. No library — every line is
explainable. Serialized into the SSTable header → loaded on startup.

**Cost:** ~9.6 bits per key at 1% FP rate. For 1M keys: ~1.2MB per SSTable. Negligible.

---

## 4. Why Immutable SSTables?

**Interview question:** *"Why not update SSTables in place?"*

Mutating a file shared between readers and writers requires locking. In a
concurrent environment this kills performance.

Immutable SSTables:
- Zero locking needed during reads
- Crash safety: a partially written SSTable is just discarded on recovery
  (we write to a `.tmp` file and rename atomically)
- Clean compaction: merge two immutable files into a new one, then delete
  the originals — no need to handle partial overwrites

---

## 6. Why Sequence Numbers?

**Interview question:** *"How do you determine the latest value when multiple SSTables contain the same key?"*

An LSM-tree may contain multiple versions of the same key across different SSTables. Rather than relying on SSTable creation time or filenames, every write is assigned a monotonically increasing sequence number.

Example:
```text
seq=101  SET user:1 Alice
seq=102  SET user:1 Bob
```

During reads and compaction, the storage engine always keeps the record with the highest sequence number. This guarantees deterministic conflict resolution while keeping SSTables immutable.

**Trade-off:** Every record stores an additional sequence number, slightly increasing storage overhead, but greatly simplifying version management.

---

## 7. Why Write-Ahead Log (WAL)?

**Interview question:** *"What happens if the process crashes before flushing to disk?"*

The MemTable is in-memory. A crash loses all unflushed writes. The WAL fixes this:

1. Append to WAL (fsync to disk)
2. Write to MemTable
3. Return OK to client

On startup:
- Load MANIFEST
- Open active SSTables
- Replay the remaining WAL entries

When flushing a MemTable:
1. Flush the MemTable to a new SSTable.
2. fsync() the SSTable.
3. Update the MANIFEST checkpoint.
4. Truncate only the WAL entries covered by that checkpoint.

This ordering guarantees that no acknowledged write is lost even if a crash occurs during flushing.

**Key invariant:** No write that was ACK-ed to the client is ever lost.

---

## 8. Why No Coordinator Node?

**Interview question:** *"How does the client know which node to talk to?"*

A coordinator is a single point of failure and adds one network hop to every
request. Dynamo, Cassandra, and Redis Cluster all avoid this.

In LSMKV, the consistent hash ring lives in the client SDK. The client
computes `hash(key) → primary_node` locally and opens a direct TCP connection.

No coordinator → no SPOF, one fewer network hop, simpler failure modes.

**Trade-off:** Every client must know the cluster topology. We solve this with
static config (`config.json`) — acceptable for a fixed 3-node cluster.

---

## 9. Why Synchronous Replication Only?

**Interview question:** *"How does replication work? What about availability vs. consistency?"*

Supporting both synchronous and asynchronous replication (an AP/CP toggle) doubles
testing complexity and requires quorum logic. For a portfolio project, the
complexity is not worth the ambiguity.

Synchronous replication gives a clean guarantee: a write is durable on N nodes
before the client receives OK. The trade-off (higher write latency with more
replicas) is documented and can be explained precisely in interviews.

**The choice is: correct and explainable beats featureful and complicated.**

---

## 10. Why Static Cluster Config?

**Interview question:** *"How do you handle adding or removing nodes?"*

Dynamic membership (gossip protocol, Raft-based cluster management) is a
project by itself. Zookeeper, etcd, and the Raft paper cover this in depth.

Static config keeps focus on the storage engine — the differentiating part of
this project. Adding a node requires updating `config.json` and restarting clients.

For a production system, the answer is: use a gossip protocol (Cassandra) or
a consensus service (etcd). This project demonstrates understanding of *why*
you need dynamic membership — not implementing it is a deliberate scope decision.

---

## 11. Why a MANIFEST File?

Scanning every SSTable on startup becomes expensive as the database grows.

The MANIFEST acts as the authoritative metadata for the storage engine, recording:

- Active SSTables
- SSTable levels
- Latest checkpoint

Recovery becomes:

Load MANIFEST
↓

Open active SSTables
↓

Replay remaining WAL

instead of scanning every file in the data directory.
