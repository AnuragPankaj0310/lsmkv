# Architecture

## Overview

LSMKV is a distributed key-value database built from scratch. It combines a
custom LSM-tree storage engine with Cassandra-style consistent-hash routing
and synchronous replication.

```
Client SDK
    │  consistent hash ring lives here (no coordinator)
    │
    ├──► Node 0 — Primary for key range [H0, H1)
    ├──► Node 1 — Primary for key range [H1, H2)
    └──► Node 2 — Primary for key range [H2, MAX)
         │
         └──► Each node runs an identical storage engine stack
```

---

## Per-Node Storage Engine

### Storage Engine API
```python
class StorageEngine:
    def put(key, value): ...
    def get(key): ...
    def delete(key): ...
    def flush(): ...
    def compact(): ...
```

### Storage Engine Responsibilities
- Coordinate WAL writes
- Manage MemTable lifecycle
- Trigger SSTable flushes
- Load MANIFEST on startup
- Route reads across MemTable and SSTables
- Trigger background compaction

### Write path

```
SET key value    │    ├──► WAL.append()    ├──► fsync()    ├──► MemTable.set()    │    ▼MemTable Full?    │    ▼Freeze MemTable    │    ▼Flush SSTable    │    ▼fsync(SSTable)    │    ▼Update MANIFEST    │    ▼Truncate WAL
```

### Read path

```
GET key
    │
    ├──► 1. MemTable.get(key)                   ← check memory (O(log n))
    │        (hit → return immediately)
    │
    └──► 2. For each SSTable (newest → oldest):
                │
                ├──► Bloom Filter
                │
                ├──► Sparse Index
                │
                ├──► Binary Search
                │
                ├──► Seek
                │
                ├──► Sequential Scan
                │
                └──► Highest Sequence Number Wins
```

### Compaction

```
Background asyncio task (every 30s):

  L0 >= 4 files?
    │
    └──► K-Way Merge (min-heap, one iterator per SSTable)
             Highest sequence number wins.
             Older versions are discarded.
             Tombstones with the highest sequence number permanently remove older values.
             Output → new L1 SSTable
             Input files → deleted atomically
```

### Background Workers

```
Background Workers
    • TTL Cleanup
    • MemTable Flush
    • Compaction
    • Metrics Collection
```

---

## Distributed Layer

### Consistent Hashing

The hash ring holds 150 virtual nodes per physical node.
All routing logic lives in the client SDK — there is no coordinator.

```
add_node(addr):
    for i in range(150):
        h = md5(f"{addr}:{i}")
        ring.insert(h → addr)

get_node(key):
    h = md5(key)
    idx = bisect_right(ring, h) % len(ring)
    return ring[idx].node
```

### Replication

```
Client → Primary:   SET user:42 Anurag
Primary:
    1. WAL.append(SET, user:42, Anurag)
    2. MemTable.set(user:42, Anurag)
    3. replicate_to([replica_1, replica_2], REPLICATE_msg)   ← synchronous
    4. → OK to client   (after ALL replicas ACK)
```

### Heartbeat Failure Detection

```
Every node pings each peer every 2 seconds via PING command.
After 3 missed pings (6 seconds):
    peer.status = DEAD
    → ring.remove_node(peer_addr)   [client SDK updated]
    → dead primary's key range served by replica

On recovery:
    ping succeeds
    peer.status = ALIVE
    → Replay local WAL
    → Load local SSTables
    → Fetch missing SSTables from primary
    → Resume synchronous replication
    → ring.add_node(peer_addr)
```

---

## File Layout

```
sst_NNNNNNN.dat:
  [60 bytes]  HEADER
                magic(8) + version(4) + entry_count(8)
                + bloom_offset(8) + bloom_length(8)
                + index_offset(8) + index_length(8)
                + data_offset(8)
  [variable]  BLOOM FILTER
                20-byte header + bit array
  [variable]  SPARSE INDEX
                msgpack list of [[key, absolute_file_offset], ...]
                one entry per 128 keys
  [variable]  DATA BLOCKS
                MessagePack Record[    sequence_number,    key,    value,    expiry_timestamp,    is_tombstone]
```

---

## Storage Engine Metrics

| Metric | Formula | Target |
|---|---|---|
| Write Amplification | disk_bytes / client_bytes | < 3× at steady state |
| Read Amplification | sstable_reads / total_gets | < 2 with Bloom filters |
| Bloom Filter Hit Rate | bloom_skips / (bloom_skips + sst_reads) | > 80% on 30% miss workload |
| SSTable Count | sum of all levels | bounded by compaction trigger |
| Compaction Throughput | total_bytes_compacted / last_run_seconds | reported in bytes/sec |
| MemTable Flush Count | number of MemTable flushes to SSTables | tracked for flush pressure |

## Storage Engine Guarantees
- Every acknowledged write is first persisted to the WAL.
- SSTables are immutable once created.
- The MANIFEST is the authoritative source of active SSTables.
- Sequence numbers determine the latest version of every key.
- Compaction never mutates existing SSTables in place.
