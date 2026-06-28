# CHANGELOG.md

# Changelog

All notable changes to this project are documented in this file.

The format is based on **Keep a Changelog**, and the project follows **Semantic Versioning (SemVer)**.

---

# [2.0.0] - 2026-06-28

## Overview

This release transforms LSMKV from a single-node LSM-tree storage engine into a feature-rich distributed key-value store with replication, cluster management, monitoring, and live rebalancing.

---

## Added

### Storage Engine

* Write-Ahead Logging (WAL) for durable writes.
* MemTable with configurable flush threshold.
* Immutable SSTables with sparse indexes.
* Bloom Filters for efficient negative lookups.
* Background compaction.
* Tombstone support for deletions.
* Time-To-Live (TTL) support.
* Crash recovery from WAL.

---

### Distributed System

* Consistent hashing for key distribution.
* Configurable synchronous replication.
* Heartbeat-based node health monitoring.
* Client-side request routing.
* Live cluster rebalancing.
* Streaming key migration.
* Migration retry mechanism.
* Dynamic ownership calculation.

---

### Networking

* Async TCP server using asyncio.
* MessagePack-based binary protocol.
* Connection pooling.
* Request/response protocol.
* Cluster communication APIs.

---

### Monitoring

* Prometheus metrics endpoint.
* Grafana dashboard integration.
* Cluster health dashboard (`stats`).
* Human-readable metrics CLI.
* Disk usage reporting.
* Logical key count.
* Connection statistics.

---

### DevOps

* Dockerfile.
* Docker Compose deployment.
* Multi-node cluster.
* Health checks.
* Configuration management.

---

### Testing

* Comprehensive unit tests.
* Integration tests.
* Replication tests.
* Migration tests.
* Rebalancing tests.
* Networking tests.
* Docker validation.

Current test status:

```text
116 passed, 1 skipped
```

---

## Improved

### Storage

* Faster lookups using Bloom Filters.
* Improved compaction scheduling.
* Better SSTable organization.
* Improved metrics reporting.

---

### Distributed Layer

* Improved replication reliability.
* Better cluster monitoring.
* Cleaner migration workflow.
* More resilient networking.

---

### CLI

* Added cluster dashboard.
* Human-readable metrics formatting.
* Better error handling.
* Improved health reporting.

---

### Documentation

* Complete project documentation.
* Architecture guide.
* Benchmark guide.
* Deployment instructions.
* Demo walkthrough.
* Future work roadmap.

---

## Fixed

* MessagePack serialization issues.
* Metrics serialization bug.
* Migration protocol handling.
* Network migration tests.
* Docker deployment issues.
* Cluster metrics reporting.
* CLI formatting inconsistencies.
* Various robustness improvements discovered during testing.

---

## Known Limitations

The following features are intentionally outside the scope of the current implementation:

* Distributed consensus (Raft/Paxos)
* Dynamic cluster membership
* Automatic failover
* Read repair
* Anti-entropy synchronization
* Parallel compaction
* Compression
* Block cache
* Transactions
* Secondary indexes

These are documented in **Future Work** and provide a roadmap for future development.

---

# [1.0.0] - Initial Release

## Added

* Basic LSM Tree implementation.
* MemTable.
* SSTables.
* Write-Ahead Log.
* Bloom Filters.
* Background compaction.
* Python client.
* Basic networking.
* Unit tests.

---

# Version Summary

| Version    | Highlights                                                                                     |
| ---------- | ---------------------------------------------------------------------------------------------- |
| **v1.0.0** | Initial LSM-tree storage engine                                                                |
| **v2.0.0** | Distributed database with replication, rebalancing, monitoring, Docker, and cluster management |

---

# Upgrade Notes

Users upgrading from **v1.0.0** should note the following:

* Configuration files have been expanded to support distributed deployment.
* Docker Compose is now the recommended deployment method.
* Prometheus and Grafana integration are included by default.
* The CLI now provides operational commands such as `stats` and `metrics`.

---

# Contributors

Project developed as an educational implementation of a distributed LSM-tree key-value store, demonstrating storage engine internals and distributed systems concepts.

---

# License

This project continues to be released under the MIT License.
