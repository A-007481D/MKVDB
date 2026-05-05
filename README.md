# MKVDB

A distributed key-value store built on an LSM-tree storage engine and Raft consensus. Written in Rust.

MKVDB exposes a Redis-compatible RESP interface so you can talk to it with `redis-cli`, while underneath it replicates every write through a Raft quorum and persists data through a write-ahead log, memtable, and SSTable pipeline.

## What This Actually Is

- An **LSM-tree storage engine** (`ApexEngine`) with a WAL, lock-free skip-list memtable, LZ4-compressed SSTables with bloom filters, and background compaction.
- A **Raft consensus layer** built on [openraft](https://github.com/databendlabs/openraft) with gRPC transport, linearizable reads, and automatic leader failover.
- A **RESP protocol server** that accepts `SET`, `GET`, `DEL`, `SCAN`, and `PING` commands. Every read goes through the Raft linearizability barrier. Every write goes through Raft log replication.
- A **~5k LOC** engine core with **~2.8k LOC** of integration tests and adversarial validation harnesses.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    Client (redis-cli)                │
│                   RESP over TCP :6379                │
└──────────────────────┬───────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────┐
│                   ApexServer                         │
│         RESP codec · backpressure · dispatch         │
└──────────┬───────────────────────────┬───────────────┘
           │ writes                    │ reads
┌──────────▼───────────┐    ┌───────────▼──────────────┐
│    Raft Consensus    │    │  ensure_linearizable()   │
│  openraft + gRPC     │    │  quorum heartbeat check  │
│  AppendEntries       │    └───────────┬──────────────┘
│  RequestVote         │                │
│  InstallSnapshot     │                │
└──────────┬───────────┘    ┌───────────▼──────────────┐
           │ apply          │       ApexEngine         │
┌──────────▼───────────┐    │  get() · scan()          │
│  RaftStateMachine    │    │  MemTable → Immutables   │
│  data: keyspace      │    │  → L0 SSTables → L1      │
│  meta: keyspace      │    └──────────────────────────┘
│  log: keyspace       │
└──────────┬───────────┘
           │
┌──────────▼───────────────────────────────────────────┐
│                   ApexEngine                         │
│                                                      │
│  WAL (CRC32, fsync)                                  │
│  MemTable (crossbeam SkipMap, lock-free)             │
│  SSTable (4KB blocks, LZ4, bloom filter, sparse idx) │
│  Compaction (L0 → L1, tombstone GC)                  │
│  Block Cache (moka, 1GB default)                     │
│  MANIFEST (atomic checkpoint via rename)             │
└──────────────────────────────────────────────────────┘
```

## Key Design Decisions

### Why a single LSM tree for both Raft log and user data?

The Raft log (`log:`), metadata (`meta:vote`, `meta:last_applied`, etc.), and user data (`data:`) all live in the same `ApexEngine` instance. This means a single `write_batch_sync` atomically updates both the Raft log entry and its metadata pointer. No dual-write problem, no cross-database recovery reconciliation.

### Why quorum reads instead of leader leases?

`ApexNode::read()` calls `ensure_linearizable()` before touching the storage engine. This forces a quorum heartbeat exchange — the leader must prove it's still the leader before serving a read. Leader leases would be faster but depend on synchronized clocks. Clock drift breaks linearizability. We trade one network round-trip for clock-independent correctness.

### Why follower redirection instead of proxying?

When a follower receives a write or read, it returns a `MOVED <leader_addr>` error (same semantics as Redis Cluster). The client reconnects to the leader. Internal proxying would double the TCP connections and bandwidth. Redirection keeps the cluster simple.

### Why mandatory fsync for Raft writes?

The engine supports three sync policies (`EveryWrite`, `Buffered`, `Delayed`), but the Raft storage adapter always uses `write_batch_sync()` which forces `fsync()` regardless of the configured policy. If you acknowledge a write to Raft before it hits the disk, a power failure can lose committed data. The durability contract is non-negotiable.

### Why atomic renames for SSTables and MANIFEST?

`SSTableBuilder` writes to a `.tmp` file, calls `sync_all()`, then atomically renames to `.sst`, then syncs the parent directory. If the process crashes mid-flush, you get either the old state or the new state, never a half-written file. The MANIFEST uses the same pattern.

## Storage Engine Internals

### Write Path

1. Acquire concurrency semaphore (bounded parallelism).
2. Backpressure stall loop — if immutable memtable queue or L0 count exceeds thresholds, exponential backoff up to 5 seconds, then `EngineOverloaded` error.
3. Append to WAL under mutex. Fsync depends on `SyncPolicy` (always forced for Raft).
4. Insert into active memtable (lock-free `crossbeam_skiplist::SkipMap`).
5. If memtable exceeds 64MB, rotate: current memtable becomes immutable, new empty memtable created, WAL rotated.
6. Background task flushes immutable memtable to SSTable on disk.

### Read Path

1. Check active memtable (lock-free).
2. Check immutable memtable queue (newest first, `parking_lot::RwLock`).
3. Check L0 SSTables (newest first — bloom filter → sparse index binary search → decompress single 4KB block → binary search within block).
4. Check L1 SSTables (same process).
5. First tombstone hit at any level returns `None`.

### SSTable Format

```
┌──────────────────────────────────────┐
│  Data Blocks (4KB each, LZ4 + CRC32) │
│  ┌────────────────────────────────┐  │
│  │ [KeyLen:4][Key][LSN:8]         │  │
│  │ [IsTombstone:1][ValLen:4][Val] │  │
│  │ ... more entries ...           │  │
│  └────────────────────────────────┘  │
├──────────────────────────────────────┤
│  Sparse Index                        │
│  [NumEntries:4]                      │
│  [KeyLen:4][Key][BlockOffset:8] × N  │
├──────────────────────────────────────┤
│  Bloom Filter (1% FP rate)           │
│  [SipKeys:32][NumBits:8]             │
│  [NumHashes:4][BitmapLen:4][Bitmap]  │
├──────────────────────────────────────┤
│  Footer                              │
│  [IndexOffset:8][BloomOffset:8]      │
│  [Magic:8 (0xA9E8D8B17F1A2B3C)]      │
└──────────────────────────────────────┘
```

### WAL Record Format

```
[Checksum:4 (CRC32)] [LSN:8] [KeyLen:4] [Key] [IsTombstone:1] [ValLen:4] [Value]
```

On recovery, if a checksum fails at the end of the file, it's treated as a torn write (power cut mid-I/O) and the tail is truncated. This is correct because the WAL is append-only and the last record is the only one that can be partial.

### Recovery Sequence

1. **MANIFEST replay** — reconstruct which SSTables are active at each level.
2. **Startup GC** — delete orphaned `.tmp` files, zombie `.sst` files not in MANIFEST, obsolete `.wal` files.
3. **WAL replay** — replay all WAL files since the last checkpoint into a fresh memtable.
4. **LSN reset** — set next sequence number to `max(recovered_lsn) + 1`.

## Raft Configuration

| Parameter | Value |
|---|---|
| Heartbeat interval | 500ms |
| Election timeout | 2000–5000ms |
| Snapshot policy | Every 1000 log entries |
| Transport | gRPC (tonic) with Gzip compression |
| Serialization | Protobuf for RPC envelope, bincode for payload data |

## Running

### Prerequisites

- Rust 2024 edition (nightly or stable with edition support)
- `protoc` (Protocol Buffers compiler)

### Single Node

```bash
cd storage-engine
cargo run -- --node-id 1 --raft-addr 127.0.0.1:50051 --path /tmp/mkvdb_node1
```

This starts the RESP server on `127.0.0.1:6379` and the Raft gRPC server on `127.0.0.1:50051`.

### Talking to It

```bash
redis-cli -p 6379
> SET mykey myvalue
OK
> GET mykey
"myvalue"
> DEL mykey
(integer) 1
> SCAN start_key end_key
```

### Tests

```bash
cd storage-engine

# Unit + integration tests (includes distributed verifier harness)
cargo test --verbose

# Criterion benchmarks
cargo bench
```

### CI

The GitHub Actions pipeline (`rust.yml`) runs:
1. `cargo fmt --check`
2. `cargo clippy -- -D warnings` (zero warnings, pedantic level)
3. `cargo audit` (dependency vulnerability scan)
4. `cargo test` (full integration suite including Raft cluster bootstrap and failover)

## Project Structure

```
storage-engine/
├── proto/raft.proto              # gRPC service definition
├── src/
│   ├── main.rs                   # Binary entrypoint
│   ├── lib.rs                    # Crate root (clippy::pedantic enforced)
│   ├── engine.rs                 # ApexEngine — write path, read path, flush, checkpoint, shutdown
│   ├── wal.rs                    # Write-ahead log writer + crash recovery reader
│   ├── memtable.rs               # Lock-free SkipMap memtable + immutable queue
│   ├── compaction.rs             # L0 → L1 background compaction with tombstone GC
│   ├── manifest.rs               # Version set tracking (atomic checkpoint via rename)
│   ├── batch.rs                  # Atomic WriteBatch
│   ├── iterator.rs               # DbIterator trait, MergingIterator (min-heap), ScanStream
│   ├── metrics.rs                # Advisory atomic I/O counters
│   ├── error.rs                  # ApexError enum
│   └── sstable/
│       ├── builder.rs            # SSTable writer (LZ4, bloom, sparse index, atomic rename)
│       ├── reader.rs             # SSTable reader (bloom check → binary search → block cache)
│       ├── iterator.rs           # Sequential SSTable iteration for compaction
│       └── cache.rs              # File descriptor cache (TableCache)
│   └── network/
│       ├── node.rs               # ApexNode — Raft coordinator, linearizable reads, stabilization barrier
│       ├── storage.rs            # ApexRaftStorage — openraft LogStorage + StateMachine impl
│       ├── grpc.rs               # gRPC server/client, protobuf ↔ openraft type conversion
│       ├── resp.rs               # RESP protocol codec (tokio_util::codec)
│       └── server.rs             # ApexServer — TCP listener, command dispatch, backpressure
├── tests/
│   ├── integration_test.rs       # Core engine tests
│   ├── compaction_test.rs        # Compaction correctness
│   ├── checkpoint_test.rs        # Checkpoint/restore
│   ├── admission_control_test.rs # Backpressure under load
│   ├── distributed_verifier_harness.rs  # 3-node Raft cluster: convergence + leader failover
│   ├── phase1_raft_routing_test.rs      # RESP routing through Raft
│   ├── chaos_kill.py             # Process-level crash injection
│   └── corruption_test.py        # Bit-level disk corruption
├── benches/engine_bench.rs       # Criterion benchmarks
└── examples/chaos_test.rs        # Adversarial chaos example
```

## Technical Paper

<!--A detailed architecture document and LaTeX technical paper are in [`docs/technical-paper/`](docs/technical-paper/).-->

## License

See repository for license details.
