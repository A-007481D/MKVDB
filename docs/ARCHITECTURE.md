# ApexDB Phase 1 — Architecture & Changelog

> **Version:** 0.1.0 (Phase 1 — Single-Node Storage Engine)
> **Author:** A-007481D
> **Branch:** `feature/phase1-storage-engine`
> **Date:** 2026-04-28

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Module Reference](#3-module-reference)
4. [Concurrency Model](#4-concurrency-model)
5. [Data Flow: Write Path](#5-data-flow-write-path)
6. [Data Flow: Read Path](#6-data-flow-read-path)
7. [Crash Recovery](#7-crash-recovery)
8. [On-Disk File Formats](#8-on-disk-file-formats)
9. [Commit History & Changelog](#9-commit-history--changelog)
10. [Testing Strategy](#10-testing-strategy)
11. [Known Limitations & Future Work](#11-known-limitations--future-work)

---

## 1. Project Overview

ApexDB is a write-optimized key-value storage engine built on a **Log-Structured Merge-Tree (LSM-Tree)** architecture. Phase 1 implements the single-node engine in Rust with:

- **Write-Ahead Log (WAL)** for crash durability
- **MemTable** (concurrent SkipList) for in-memory sorting
- **SSTable** (Sorted String Table) for persistent, immutable on-disk storage
- **MANIFEST** (Version Set) for atomic state tracking
- **Block Cache** (LRU) for read performance

### Dependency Stack

| Crate | Version | Purpose |
|---|---|---|
| `tokio` | 1.36 | Async runtime for background flush tasks |
| `crossbeam-skiplist` | 0.1.3 | Lock-free concurrent SkipMap for the MemTable |
| `bytes` | 1.5.0 | Zero-copy byte buffers for keys and values |
| `crc32fast` | 1.4.0 | CRC32 checksums on WAL records |
| `bloomfilter` | 1.0.14 | Bloom filters in SSTables to avoid unnecessary disk I/O |
| `thiserror` | 1.0.57 | Ergonomic, granular error types |
| `parking_lot` | 0.12.1 | High-performance `RwLock` and `Mutex` |
| `moka` | 0.12.5 | Thread-safe concurrent LRU cache for SSTable blocks |
| `proptest` | 1.4.0 | Property-based testing / fuzzing (dev-dependency) |
| `tempfile` | 3.10 | Temporary directories for tests (dev-dependency) |

### Linting Policy

```rust
#![deny(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
```

**No `unwrap()` or `expect()` in the data path.** All fallible operations return `ApexError` via `thiserror`.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ApexEngine                               │
│                                                                 │
│  ┌─────────────┐   ┌───────────────────────────────────────┐    │
│  │  WAL Writer  │   │          VersionSet (RwLock)          │    │
│  │  (Mutex)     │   │                                       │    │
│  │              │   │  ┌─────────────┐  ┌───────────────┐   │    │
│  │  append()    │   │  │  Active      │  │   MANIFEST    │   │    │
│  │  sync()      │   │  │  MemTable    │  │   (Version    │   │    │
│  │              │   │  │  (SkipMap)   │  │    Set)       │   │    │
│  └─────────────┘   │  └─────────────┘  └───────────────┘   │    │
│                     │                                       │    │
│                     │  ┌─────────────────────────────────┐   │    │
│                     │  │  SSTable Readers (Level 0)       │   │    │
│                     │  │  ┌───────┐ ┌───────┐ ┌───────┐  │   │    │
│                     │  │  │ .sst  │ │ .sst  │ │ .sst  │  │   │    │
│                     │  │  └───────┘ └───────┘ └───────┘  │   │    │
│                     │  └─────────────────────────────────┘   │    │
│                     └───────────────────────────────────────┘    │
│                                                                 │
│  ┌───────────────────────┐   ┌──────────────────────────────┐   │
│  │ Immutable MemTable    │   │     Block Cache (moka)        │   │
│  │ Queue (RwLock<Vec>)   │   │     ~1GB / 4KB blocks         │   │
│  └───────────────────────┘   └──────────────────────────────┘   │
│                                                                 │
│  next_lsn: AtomicU64          sync_policy: SyncPolicy           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Module Reference

### `src/error.rs` — Error Types

Defines `ApexError`, a `thiserror`-based enum with variants:

| Variant | When |
|---|---|
| `Io(io::Error)` | Any filesystem or I/O failure |
| `ChecksumMismatch { expected, found }` | CRC32 mismatch during WAL replay |
| `NotFound` | Key lookup miss (used internally) |
| `ManifestError(String)` | MANIFEST parse or write failures |
| `Corruption(String)` | Data corruption detected (e.g. bad magic number) |

All public APIs return `Result<T>` which is an alias for `std::result::Result<T, ApexError>`.

---

### `src/memtable.rs` — MemTable & Immutable Queue

#### `EntryValue` (enum)

```rust
pub enum EntryValue {
    Value(Bytes),   // A live key-value pair
    Tombstone,      // A deletion marker
}
```

Tombstones are first-class citizens. When a key is deleted, a `Tombstone` entry is inserted. During reads, if a tombstone is found at any level, the search stops immediately — even if the key exists in older SSTables.

#### `MemTable` (struct)

- **Backing store:** `crossbeam_skiplist::SkipMap<Bytes, EntryValue>` — lock-free concurrent sorted map.
- **Size tracking:** `AtomicUsize` — approximate memory footprint. Incremented on every `put()`. When it crosses `64 MB`, the engine triggers a flush.
- **Sequence tracking:** `AtomicU64` for `first_seq` and `last_seq` — tracks the LSN range of entries in this memtable for recovery purposes.

Key methods:
- `put(&self, key, value, seq)` — concurrent, lock-free insert.
- `get(&self, key) -> Option<EntryValue>` — concurrent, lock-free lookup.
- `iter()` — sorted iteration for flushing to SSTable.

#### `ImmutableMemTables` (struct)

A queue (`RwLock<Vec<Arc<MemTable>>>`) of frozen memtables waiting for background flush. The read path searches this queue in **reverse order** (newest first) so that the most recent write always wins.

Key methods:
- `push(table)` — called when the active memtable is rotated.
- `get(key)` — searches newest-to-oldest under a read lock.
- `remove_flushed(table)` — called after a successful flush + MANIFEST commit.

---

### `src/wal.rs` — Write-Ahead Log

#### Record Format (on disk)

```
┌──────────┬─────────┬──────────┬──────┬──────────────┬──────────┬────────┐
│ CRC32(4) │ LSN (8) │ KeyLen(4)│ Key  │ IsTombstone(1)│ ValLen(4)│ Value  │
└──────────┴─────────┴──────────┴──────┴──────────────┴──────────┴────────┘
```

- **CRC32** covers everything after it (LSN through Value). Computed via `crc32fast`.
- **LSN** is a monotonically increasing 64-bit sequence number assigned by `AtomicU64::fetch_add`.
- **IsTombstone** is `0x00` for values, `0x01` for deletions.
- **ValLen** is `0` when `IsTombstone == 1`.

#### `WalWriter`

- Uses `BufWriter<File>` for buffered writes — multiple `append()` calls accumulate in user-space.
- `sync()` flushes the buffer and calls `sync_data()` (fdatasync) on the underlying file descriptor.
- The WAL writer is protected by its own `Mutex` in the engine, **decoupled** from the VersionSet lock.

#### `WalReader`

- Used during crash recovery.
- Reads records sequentially. If a CRC32 mismatch is detected (partial write from a crash), the reader returns an error and the engine stops replaying — all fully-written records before that point are recovered.

---

### `src/sstable/builder.rs` — SSTable Writer

#### On-Disk Layout

```
┌────────────────────────────────────────────────────────────┐
│                     DATA BLOCKS                             │
│  Block 0: [KeyLen(4)][Key][Tomb(1)][ValLen(4)][Value] ...   │
│  Block 1: ...                                               │
│  Block N: ...                                               │
├────────────────────────────────────────────────────────────┤
│                    INDEX BLOCK                              │
│  [NumEntries(4)]                                            │
│  [KeyLen(4)][FirstKey][BlockOffset(8)] × N                  │
├────────────────────────────────────────────────────────────┤
│                   BLOOM FILTER BLOCK                        │
│  [SipKey1_0(8)][SipKey1_1(8)][SipKey2_0(8)][SipKey2_1(8)]  │
│  [NumBits(8)][NumHashes(4)][BitmapLen(4)][Bitmap]           │
├────────────────────────────────────────────────────────────┤
│                      FOOTER (24 bytes)                      │
│  [IndexOffset(8)] [BloomOffset(8)] [MagicNumber(8)]         │
│                                    0xA9E8D8B17F1A2B3C       │
└────────────────────────────────────────────────────────────┘
```

- **Block size target:** 4KB. A block is flushed when adding another entry would exceed this limit.
- **Sparse index:** Records the first key of each block + its byte offset. Enables binary search.
- **Bloom filter:** 1% false-positive rate. Configured via `Bloom::new_for_fp_rate`.
- **Tombstones:** Serialized with `IsTombstone = 0x01` and `ValLen = 0`.

#### `SSTableBuilder` API

```rust
let mut builder = SSTableBuilder::new("000001.sst", 10_000)?;
builder.add(key, &EntryValue::Value(value))?;   // Must be sorted
builder.add(key2, &EntryValue::Tombstone)?;      // Tombstone
builder.finish()?;                                // Writes index + bloom + footer, fsyncs
```

---

### `src/sstable/reader.rs` — SSTable Reader

#### Key Design Decision: `get(&self)` not `get(&mut self)`

The file handle is wrapped in `parking_lot::Mutex<File>`. This allows:
- The engine to hold only a **shared** (`RwLock::read`) guard on the VersionSet during reads.
- Multiple threads to read from different SSTables concurrently.
- The `moka` block cache to serve hot blocks without touching the file mutex at all.

#### Double-Check Pattern

```rust
// 1. Check cache (no lock)
if let Some(b) = self.block_cache.get(&offset) { return search(b); }

// 2. Acquire file mutex
let mut file = self.file.lock();

// 3. Double-check cache (another thread may have populated it)
if let Some(b) = self.block_cache.get(&offset) { return search(b); }

// 4. Read from disk, populate cache, release mutex
```

This is the same pattern used in Java's `DCL` (Double-Checked Locking) but here it's safe because `moka::Cache` is inherently thread-safe.

#### `index_offset` Field

Stores the byte offset where the sparse index begins. This solves the "last block size" problem — previously the reader had to guess how many bytes to read for the final data block. Now it reads exactly `index_offset - block_offset` bytes.

---

### `src/manifest.rs` — Version Set (MANIFEST)

#### MANIFEST Record Format

```
ADD,<level>,<sst_id>\n       # SSTable added (flush or compaction)
RM,<level>,<sst_id>\n        # SSTable removed (compaction)
WAL,<wal_id>\n               # WAL watermark (all WALs with id < this have been flushed)
```

Plain-text, append-only, fsynced after every event. Designed for human readability during debugging.

#### What Changed in the Refactor

**Removed:** `ManifestEvent::UpdateSeq { seq: u64 }`

This was being logged on **every single write**, creating a second fsync bottleneck alongside the WAL. It was also redundant — the WAL already contains every LSN. During recovery, the engine replays the WAL to reconstruct sequence numbers.

**Added:** `ManifestEvent::SetWalId { wal_id: u64 }`

Logged once per flush (not per write). Tells the recovery logic: "all WALs with id < this have been fully persisted to SSTables and can be skipped during replay."

---

### `src/engine.rs` — The Coordinator

This is where the five refactor directives converge.

#### Struct Layout

```rust
pub struct ApexEngine {
    data_dir: PathBuf,
    version: Arc<RwLock<VersionSet>>,          // Structural state
    wal: Arc<Mutex<WalWriter>>,                // Decoupled I/O
    immutable_memtables: Arc<ImmutableMemTables>,
    block_cache: Cache<u64, Arc<Block>>,       // ~1GB LRU
    next_lsn: AtomicU64,                       // Global sequence counter
    sync_policy: SyncPolicy,                   // EveryWrite or Buffered
}
```

---

## 4. Concurrency Model

| Resource | Lock Type | Contention | Hold Duration |
|---|---|---|---|
| `VersionSet` (memtable, sstables, manifest) | `RwLock` | Low (shared reads, rare writes) | Nanoseconds (in-memory ops only) |
| `WalWriter` | `Mutex` | Serialized writes | Microseconds (buffered I/O) |
| `ImmutableMemTables` queue | `RwLock` | Low (read path only) | Nanoseconds |
| `File` inside `SSTableReader` | `Mutex` | Very low (cache hit = no lock) | Microseconds (disk read) |
| `block_cache` (moka) | None (internally safe) | None | N/A |
| `next_lsn` | `AtomicU64` | None (lock-free) | N/A |

### The Critical Optimization

**Before the refactor**, the write path looked like:

```
write_internal():
    state.write()          ← blocks ALL readers + writers
        wal.append()       ← disk I/O (microseconds to milliseconds)
        wal.sync()         ← fsync (MILLISECONDS)
        memtable.put()     ← nanoseconds
        manifest.log()     ← fsync again (MILLISECONDS)
    state.unlock()
```

**After the refactor:**

```
write_internal():
    lsn = next_lsn.fetch_add(1)    ← lock-free, nanoseconds
    wal.lock()                      ← serializes only WAL writes
        wal.append()                ← buffered write, microseconds
        wal.sync()                  ← fsync (only if EveryWrite)
    wal.unlock()
    version.read()                  ← shared lock, concurrent with other readers
        memtable.put()              ← nanoseconds
    version.unlock()
```

The VersionSet write-lock is now only taken during memtable **rotation** (once every 64MB of writes), not on every single put.

---

## 5. Data Flow: Write Path

```
Client calls engine.put(key, value)
          │
          ▼
    ┌─────────────────┐
    │ Assign LSN      │  AtomicU64::fetch_add (lock-free)
    │ (monotonic)     │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ WAL Append      │  Mutex<WalWriter> — serialize writes
    │ + optional sync │  CRC32 checksum computed over record
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ MemTable Insert │  RwLock<VersionSet>::read() — shared lock
    │ (SkipMap)       │  Lock-free insert into crossbeam SkipMap
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │ Size Check      │  If ≥ 64MB:
    │                 │    1. Freeze memtable → push to ImmutableQueue
    │                 │    2. Rotate WAL file
    │                 │    3. Spawn background flush (tokio::spawn)
    └─────────────────┘
```

---

## 6. Data Flow: Read Path

```
Client calls engine.get(key)
          │
          ▼
    ┌─────────────────┐
    │ Active MemTable │  RwLock::read() — concurrent with writes
    │ (SkipMap)       │  Found? → return (Value or Tombstone→None)
    └────────┬────────┘
             │ Miss
             ▼
    ┌─────────────────┐
    │ Immutable Queue │  Search newest → oldest
    │ (frozen tables) │  Found? → return (Value or Tombstone→None)
    └────────┬────────┘
             │ Miss
             ▼
    ┌─────────────────┐
    │ SSTables        │  RwLock::read() — search newest → oldest
    │ (Level 0)       │
    │                 │  For each SSTable:
    │  1. Bloom Check │    → definitely not here? skip
    │  2. Sparse Index│    → binary search for target block
    │  3. Block Cache │    → hit? scan in memory
    │  4. Disk Read   │    → miss? read 4KB block, cache it
    │  5. Block Scan  │    → linear scan within block
    └─────────────────┘
```

**Tombstone behavior:** If a tombstone is found at *any* level, the search halts and returns `None`. This prevents deleted keys from "resurrecting" from older SSTables.

---

## 7. Crash Recovery

On `ApexEngine::open()`:

```
1. Open MANIFEST
   └─ Recover: which SSTables are active, what is the WAL watermark

2. Open each SSTable listed in the MANIFEST
   └─ Populate the sstables vector

3. Scan data directory for .wal files
   └─ Filter: only replay WALs with id ≥ manifest.wal_id

4. For each WAL (sorted by id):
   └─ Read records sequentially
   └─ Verify CRC32 checksums
   └─ Insert into active MemTable
   └─ If CRC32 fails → stop (partial tail record from crash)
   └─ Track max(LSN) for the next_lsn counter

5. Open a fresh WAL for new writes
```

### Why the MANIFEST is the Last Step of Flush

```
flush_memtable():
    1. Build SSTable on disk          ← crash here? orphaned .sst file (harmless)
    2. Open SSTable reader            ← crash here? same
    3. manifest.log(AddTable)         ← ATOMIC COMMITMENT POINT
    4. manifest.log(SetWalId)         ← marks old WAL as consumed
    5. Remove from immutable queue    ← cleanup
```

If the engine crashes at steps 1-2, the SSTable file exists on disk but is not referenced by the MANIFEST. On restart, it is simply ignored (an orphan). The data is replayed from the WAL instead.

---

## 8. On-Disk File Formats

### File Naming Convention

| File | Pattern | Example |
|---|---|---|
| WAL | `{id:06}.wal` | `000001.wal` |
| SSTable | `{id:06}.sst` | `000003.sst` |
| Manifest | `MANIFEST` | `MANIFEST` |

### Directory Layout (example)

```
data/
├── MANIFEST          # Version set log
├── 000001.wal        # Oldest WAL (may be consumed)
├── 000004.wal        # Current active WAL
├── 000002.sst        # SSTable (Level 0)
└── 000003.sst        # SSTable (Level 0, newer)
```

---

## 9. Commit History & Changelog

### Initial Implementation (Phase 1 Base)

| Commit | Description |
|---|---|
| `1a78ff7` | **chore: init workspace and CI pipeline** — Created `.gitignore`, GitHub Actions CI (`rust.yml`), `Cargo.toml` with dependencies, `lib.rs` with `#![deny(clippy::all, clippy::pedantic)]`, and `error.rs`. |
| `ec6b53b` | **feat(storage): implement concurrent MemTable and Immutable Queue** — `memtable.rs`: `EntryValue` enum with Tombstone support, `MemTable` backed by `crossbeam_skiplist::SkipMap`, `ImmutableMemTables` queue with reverse-order search. |
| `5b1731b` | **feat(storage): implement batched WAL with CRC32 checksums** — `wal.rs`: `WalWriter` with `BufWriter` + `sync_data()`, `WalReader` for crash recovery, CRC32 checksum on every record, monotonic LSN. |
| `cf783c1` | **feat(storage): implement SSTable Builder and Reader with Block Cache** — `sstable/builder.rs`: 4KB block-based writer, sparse index, Bloom filter, magic number footer. `sstable/reader.rs`: binary search on sparse index, `moka` block cache. |
| `81be3b5` | **feat(storage): implement Version Set and Manifest tracking** — `manifest.rs`: append-only log, `AddTable`/`RemoveTable`/`UpdateSeq` events, fsync after each event. |
| `a37e234` | **feat(storage): implement ApexEngine coordinator and background flush** — `engine.rs`: single `RwLock<EngineState>`, WAL sync inside the lock, background flush via `tokio::spawn`. |
| `22fc41c` | **test(storage): add basic consistency, crash recovery, and fuzzing tests** — Integration tests: put/get/delete, crash recovery stub, proptest state machine harness. |

### Staff-Level Refactor

| Commit | Description | Directive |
|---|---|---|
| `c549ab0` | **refactor(sstable): make SSTableReader::get take &self for concurrent reads** — Wrapped `File` in `Mutex<File>`. Added `index_offset` field to properly size the last data block. Extracted `search_block` as a static method. Added double-check pattern on cache miss. | Directive 2: Non-blocking reads |
| `3ec247c` | **refactor(manifest): remove UpdateSeq, add SetWalId** — Deleted `ManifestEvent::UpdateSeq` (was causing an fsync on every write). Added `ManifestEvent::SetWalId` (logged once per flush). WAL is now the sole authority for LSN recovery. | Directive 3: Eliminate redundant manifest logging |
| `7431f15` | **fix(memtable): add explicit lifetime to iter() return type** — Changed `Iter<Bytes, EntryValue>` → `Iter<'_, Bytes, EntryValue>` to satisfy `mismatched_lifetime_syntaxes` warning. | Code quality |
| `5a54ee2` | **refactor(engine): decouple WAL I/O from version lock, add SyncPolicy** — Complete rewrite of `engine.rs`. WAL moved to `Arc<Mutex<WalWriter>>`. LSN managed via `AtomicU64`. Write path: WAL I/O outside version lock. Read path: `version.read()` only. Added `SyncPolicy::EveryWrite` / `SyncPolicy::Buffered`. Implemented full WAL recovery in `open()`. MANIFEST update is now the final step of flush. | Directives 1, 4, 5 |
| `f68d10d` | **test(storage): verify WAL replay recovers committed data after crash** — Crash recovery test now asserts correct values after restart. Added `test_overwrite_survives_recovery`. Suppressed dead_code warnings on proptest helpers. | Verification |

---

## 10. Testing Strategy

### Unit / Integration Tests

| Test | What It Proves |
|---|---|
| `test_basic_put_get_delete` | Standard CRUD: put → get → delete → get returns None (tombstone works) |
| `test_crash_recovery_via_wal_replay` | Drop engine mid-operation → reopen → WAL replay reconstructs correct state including tombstones |
| `test_overwrite_survives_recovery` | Multiple puts to the same key → restart → latest value wins |

### Property-Based Fuzzing (proptest)

```rust
// Generates random sequences of Put(key, value) and Delete(key) operations.
// After executing them against the engine, verifies that a HashMap "model"
// and the engine agree on every key.
fn test_engine_state_machine(ops in vec(op_strategy(), 1..100))
```

Currently gated behind a comment (uncomment `#[test]` to run). Designed to catch edge-case race conditions and state corruption.

### Running Tests

```bash
cd storage-engine
~/.cargo/bin/cargo test          # Run all tests
~/.cargo/bin/cargo test -- --nocapture  # With stdout
```

### CI Pipeline (GitHub Actions)

`.github/workflows/rust.yml` runs on every push and PR to `main`:
1. `cargo fmt -- --check`
2. `cargo clippy -- -D warnings`
3. `cargo test --verbose`

---

## 11. Known Limitations & Future Work

| Limitation | Resolution (Phase) |
|---|---|
| No compaction — Level 0 SSTables accumulate indefinitely | Phase 2: Leveled Compaction |
| No range scans / iterators exposed publicly | Phase 2 |
| Old WAL files are not garbage-collected after flush | Phase 2 |
| Single-node only — no replication | Phase 3: Raft consensus |
| No client-facing API (gRPC) | Phase 3-4: gRPC server + Go gateway |
| No Prometheus metrics | Phase 5: Observability |
| Bloom filter false-positive rate is fixed at 1% | Configurable per-level in Phase 2 |
| `SyncPolicy::Buffered` requires manual `flush_wal()` calls | Could add a background timer thread |
