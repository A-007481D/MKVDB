# MKVDB: Technical Architecture & Reverse Engineering Notes

## 1. Executive Summary & System Philosophy
MKVDB is a production-grade distributed key-value store engineered with a **Failure-First Architecture**. It marries a high-performance Log-Structured Merge-tree (LSM) storage engine (`ApexEngine`) with a rigorous, formal implementation of the Raft consensus protocol (`openraft`).

### Core Tenets & Trade-offs
* **"Partial Failure" as a First-Class Citizen**: Systems are designed around catastrophic failure modes (network partitions, torn writes, asymmetric packet drops). 
* **Control/Data Plane Decoupling**: 
  * *Why this?* By strictly separating the consensus control plane (heartbeats, elections) from the storage data plane (WAL flushes, memtable compactions), network partitions isolating the consensus quorum do not corrupt local storage state or stall background flushes.
* **Provable Safety via Deterministic Chaos**: Invariants are validated through a Deterministic Adversarial Fault Engine (DAFE).

## 2. Cluster Topology & Networking
### 2.1 The ApexNode Coordinator
The `ApexNode` acts as the definitive bridge between the distributed `openraft` runtime and the local `ApexEngine` storage state machine.

### 2.2 Networking & Serialization (gRPC)
* **Stack**: Built on `tonic` (gRPC) and `protocol buffers`. 
* **Compression**: Implements `Gzip` compression for both `AppendEntries` and `InstallSnapshot` RPCs. 
* **Serialization**: Uses `bincode` for high-speed binary encoding of `RaftCommand` payloads. 
* **Trade-off**: While `Protobuf` is used for the envelope (interop), `bincode` is used for internal data to minimize CPU cycles during serialization/deserialization on the hot path.

### 2.3 The Stabilization Barrier (`await_cluster_stable`)
Membership changes utilize `await_cluster_stable()`, ensuring a stable leader and committed votes before mutating topology. This prevents deadlocks during joint consensus.

## 3. Storage Adapters & Logical Isolation
The system multiplexes logical streams onto a single `ApexEngine` via prefix-based keyspaces:
* `log:<index:020>`: Raft log entries.
* `meta:*`: Raft structural metadata (votes, membership, etc.).
* `data:*`: Actual user-level Key-Value pairs.
* **Why unified storage?** Eliminates the "Dual Write" problem. A single atomic `write_batch_sync` updates both the Raft log and the metadata, ensuring they never drift after a crash.

## 4. Consensus & Linearizability
### 4.1 Quorum-Backed Reads (ReadIndex)
* **Mechanism**: Initiates an `ensure_linearizable()` check against the cluster before serving a `Get`.
* **Why not Leases?** MKVDB rejects clock-based leases due to NTP drift vulnerabilities. Quorum heartbeats provide clock-independent linearizability.

### 4.2 Follower Redirection
Followers are "Read-Only and Redirecting." This shifts routing overhead to the client, simplifying the internal cluster state and avoiding internal proxying deadlocks.

## 5. Storage Engine: The LSM Path (`ApexEngine`)
### 5.1 Synchronous Write-Ahead Log (WAL)
* **Design**: Sequential binary layout with a 32-bit CRC checksum footer for every record.
* **Torn Write Recovery**: If the checksum fails at the end of the log, `WalReader` truncates the corrupted tail, assuming a power failure during the last I/O operation.
* **Durability**: Uses `write_batch_sync()` for mandatory `fsync()` to the SSD controller, meeting the Raft durability contract.

### 5.2 Lock-Free MemTable & MVCC
* **Data Structure**: Uses `crossbeam_skiplist::SkipMap`.
* **Concurrency**: A `Version` struct wraps the state using `arc_swap`. This enables snapshot-isolated reads that proceed without acquiring a single lock, even during background flushes.

### 5.3 SSTable Layout & Caching
* **Block Layout**: 4KB blocks, LZ4 compressed, with a CRC32 checksum.
* **Indexing**: A sparse index stores the first key of every block. A Bloom Filter (1% FP rate) is checked before any disk I/O.
* **Caching**: Powered by a 1GB `moka` block cache using the Tiny-LFU eviction policy.

### 5.4 Iterators & Conflict Resolution
* **MergingIterator**: Uses a `BinaryHeap` for multi-way merging.
* **Conflict Logic**: Implements a **Min-Heap for Keys** and a **Max-Heap for LSNs**. If two levels contain the same key, the one with the higher Logical Sequence Number (LSN) wins, ensuring data freshness.

## 6. Deterministic Adversarial Validation (DAFE)
DAFE is a deterministic chaos engine. It uses 64-bit seeds to reconstruct exact sequences of node crashes, network jitter, and disk corruption, making distributed race conditions 100% reproducible.

## 7. Formal Invariants
* **$\mathcal{I}_{Safety}$**: No two nodes apply different values to the state machine at index $i$.
* **$\mathcal{I}_{Durability}$**: Acknowledged writes survive total cluster restarts via mandatory `fsync`.
* **$\mathcal{I}_{Linearizability}$**: Reads initiated after a write completes see that write (via ReadIndex).

## 8. Bootstrapping & The Recovery Pipeline
On startup, `ApexEngine` follows a strict 4-stage recovery sequence:
1. **Manifest Replay**: Reconstructs the Version Set (active SSTables).
2. **Startup GC**: Deletes orphaned `.tmp` files and zombie SSTables not listed in the Manifest.
3. **WAL Replay**: Replays all WAL logs since the last recorded checkpoint into a new MemTable.
4. **LSN Reset**: Sets the next global Sequence Number to `recovered_lsn + 1`.

