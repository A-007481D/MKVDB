use crate::error::Result;
use crate::manifest::{Manifest, ManifestEvent};
use crate::memtable::{EntryValue, ImmutableMemTables, MemTable};
use crate::metrics::EngineMetrics;
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::reader::{Block, SSTableReader};
use crate::wal::{WalReader, WalWriter};
use bytes::Bytes;
use moka::sync::Cache;
use parking_lot::{Mutex, RwLock};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

const MEMTABLE_SIZE_LIMIT: usize = 64 * 1024 * 1024; // 64 MB

// ---------------------------------------------------------------------------
// Sync Policy
// ---------------------------------------------------------------------------

/// Controls how the WAL is fsynced after writes.
///
/// - `EveryWrite`: Maximum durability. `fsync` after every single `put`/`delete`.
///   Throughput is bounded by the SSD's IOPS (~few thousand on NVMe).
/// - `Buffered`: Writes are buffered in user-space. The caller is responsible
///   for calling `flush_wal()` at appropriate intervals (e.g. every N ms or
///   every N writes). This is the "Group Commit" strategy used by Postgres and
///   RocksDB to reach 150k+ writes/sec.
/// - `Delayed(interval)`: A background tokio task wakes at `interval` and
///   flushes the WAL. This is the fully automatic "Group Commit" — writes
///   return immediately without blocking on I/O, and the background worker
///   batches all pending data into a single `fsync` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncPolicy {
    EveryWrite,
    Buffered,
    Delayed(Duration),
}

// ---------------------------------------------------------------------------
// Version Set — the subset of state protected by RwLock
// ---------------------------------------------------------------------------

/// The "version set": the structural view of the database that readers need.
///
/// Protected by an `RwLock`. Readers hold a shared guard; only structural
/// mutations (memtable rotation, SSTable list changes) take a write guard.
pub struct VersionSet {
    pub active_memtable: Arc<MemTable>,
    pub manifest: Manifest,
    pub sstables: Vec<SSTableReader>,
}

// ---------------------------------------------------------------------------
// ApexEngine
// ---------------------------------------------------------------------------

/// The central coordinator for the ApexDB storage engine.
///
/// # Concurrency design
///
/// | Resource            | Lock                | Rationale                                    |
/// |---------------------|---------------------|----------------------------------------------|
/// | `VersionSet`        | `RwLock`            | Many concurrent readers, rare writers         |
/// | `WalWriter`         | `Mutex`             | Serializes appends; held during I/O only      |
/// | `ImmutableMemTables`| Internal `RwLock`   | Searched on the read path                    |
/// | `block_cache`       | None (moka is safe) | Thread-safe LRU cache                        |
///
/// The critical insight: `write_internal` performs WAL I/O **outside** the
/// `VersionSet` write-lock. The lock is only held for the nanosecond-fast
/// in-memory memtable insert + size check.
pub struct ApexEngine {
    data_dir: PathBuf,
    version: Arc<RwLock<VersionSet>>,
    wal: Arc<Mutex<WalWriter>>,
    immutable_memtables: Arc<ImmutableMemTables>,
    block_cache: Cache<u64, Arc<Block>>,
    /// Monotonically increasing sequence number. Atomic so the WAL append
    /// (under Mutex) and the memtable insert can share the value without
    /// needing the VersionSet lock.
    next_lsn: AtomicU64,
    sync_policy: SyncPolicy,
    /// Engine-wide I/O counters for observability.
    metrics: Arc<EngineMetrics>,
    /// Signals the background sync task to shut down gracefully.
    shutdown: Arc<AtomicBool>,
    /// Set to true if there is un-synced WAL data in the buffer.
    /// The background sync task checks this to avoid unnecessary fsync calls.
    wal_dirty: Arc<AtomicBool>,
}

impl ApexEngine {
    /// Opens the database at `path`, replaying WALs for crash recovery.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_policy(path, SyncPolicy::EveryWrite)
    }

    /// Opens the database with a specific WAL sync policy.
    pub fn open_with_policy<P: AsRef<Path>>(path: P, sync_policy: SyncPolicy) -> Result<Self> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let manifest_path = data_dir.join("MANIFEST");
        let mut manifest = Manifest::open(&manifest_path)?;

        let block_cache: Cache<u64, Arc<Block>> = Cache::new(1024 * 1024 * 1024 / 4096);
        let immutable_memtables = Arc::new(ImmutableMemTables::new());

        // ---- Recover SSTables listed in the MANIFEST ----
        let mut sstables = Vec::new();
        if let Some(ids) = manifest.levels.get(&0) {
            let mut sorted_ids: Vec<u64> = ids.iter().copied().collect();
            sorted_ids.sort_unstable();
            for id in sorted_ids {
                let sst_path = data_dir.join(format!("{id:06}.sst"));
                if sst_path.exists() {
                    let reader = SSTableReader::open(&sst_path, block_cache.clone())?;
                    sstables.push(reader);
                }
            }
        }

        // ---- Replay WAL files not yet flushed to SSTables ----
        let active_memtable = Arc::new(MemTable::new());
        let mut recovered_lsn: u64 = 0;

        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let fname = entry.file_name();
                let name = fname.to_string_lossy();
                if let Some(stem) = name.strip_suffix(".wal")
                    && let Ok(id) = stem.parse::<u64>()
                    && id >= manifest.wal_id
                {
                    wal_files.push((id, entry.path()));
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        for (_wal_id, wal_path) in &wal_files {
            let mut reader = WalReader::open(wal_path)?;
            while let Ok(Some(record)) = reader.next_record() {
                recovered_lsn = recovered_lsn.max(record.lsn + 1);
                active_memtable.put(record.key, record.value, record.lsn);
            }
        }

        // ---- Open a fresh WAL for new writes ----
        let wal_id = manifest.generate_file_id();
        let wal_path = data_dir.join(format!("{wal_id:06}.wal"));
        let wal = WalWriter::open(&wal_path, recovered_lsn)?;

        let version = VersionSet {
            active_memtable,
            manifest,
            sstables,
        };

        let metrics = Arc::new(EngineMetrics::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let wal_dirty = Arc::new(AtomicBool::new(false));
        let wal_arc = Arc::new(Mutex::new(wal));

        // ---- Spawn background sync task if Delayed ----
        if let SyncPolicy::Delayed(interval) = &sync_policy {
            let wal_ref = Arc::clone(&wal_arc);
            let dirty_ref = Arc::clone(&wal_dirty);
            let shutdown_ref = Arc::clone(&shutdown);
            let metrics_ref = Arc::clone(&metrics);
            let interval = *interval;

            tokio::spawn(async move {
                Self::background_sync_loop(wal_ref, dirty_ref, shutdown_ref, metrics_ref, interval)
                    .await;
            });
        }

        Ok(Self {
            data_dir,
            version: Arc::new(RwLock::new(version)),
            wal: wal_arc,
            immutable_memtables,
            block_cache,
            next_lsn: AtomicU64::new(recovered_lsn),
            sync_policy,
            metrics,
            shutdown,
            wal_dirty,
        })
    }

    /// Returns a reference to the engine's I/O metrics.
    #[must_use]
    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    // -----------------------------------------------------------------------
    // Background Sync Worker (for SyncPolicy::Delayed)
    // -----------------------------------------------------------------------

    /// Wakes at `interval`, checks the dirty flag, and issues a single
    /// batched `fsync` covering all writes since the last sync.
    async fn background_sync_loop(
        wal: Arc<Mutex<WalWriter>>,
        dirty: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
        metrics: Arc<EngineMetrics>,
        interval: Duration,
    ) {
        loop {
            tokio::time::sleep(interval).await;

            // Check if we should shut down
            if shutdown.load(Ordering::Acquire) {
                // Final sync before exit
                if dirty.swap(false, Ordering::AcqRel) {
                    let mut guard = wal.lock();
                    let _ = guard.sync();
                    metrics.record_wal_sync();
                }
                return;
            }

            // Only sync if there is un-flushed data
            if dirty.swap(false, Ordering::AcqRel) {
                let mut guard = wal.lock();
                if let Err(e) = guard.sync() {
                    eprintln!("Background WAL sync failed: {e:?}");
                } else {
                    metrics.record_wal_sync();
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.metrics.record_put();
        self.write_internal(key, EntryValue::Value(value))
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        self.write_internal(key, EntryValue::Tombstone)
    }

    /// Core write path.
    ///
    /// 1. Acquire WAL mutex → append + optional fsync  (disk I/O, no version lock)
    /// 2. Acquire version read-lock → insert into memtable (nanoseconds, in-memory)
    /// 3. If memtable is full → rotate (still under version lock, but fast)
    fn write_internal(&self, key: Bytes, value: EntryValue) -> Result<()> {
        // --- Phase 1: WAL I/O (outside version lock) ---
        let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);
        let bytes_written;

        {
            let mut wal = self.wal.lock();
            bytes_written = wal.append(&key, &value)?;

            match &self.sync_policy {
                SyncPolicy::EveryWrite => {
                    wal.sync()?;
                    self.metrics.record_wal_sync();
                }
                SyncPolicy::Buffered => {
                    // Caller is responsible for calling flush_wal()
                }
                SyncPolicy::Delayed(_) => {
                    // Mark dirty — the background task will sync
                    self.wal_dirty.store(true, Ordering::Release);
                }
            }
        } // WAL mutex released

        self.metrics.record_wal_write(bytes_written);

        // --- Phase 2: MemTable insert (under version lock, nanosecond-fast) ---
        let needs_flush;
        {
            let version = self.version.read();
            version.active_memtable.put(key, value, lsn);
            needs_flush = version.active_memtable.size() >= MEMTABLE_SIZE_LIMIT;
        } // version read-lock released

        if needs_flush {
            self.trigger_flush()?;
        }

        Ok(())
    }

    /// Explicitly flush the WAL buffer to disk.
    ///
    /// Only meaningful under `SyncPolicy::Buffered`. Under `EveryWrite` this
    /// is a no-op because every append already fsyncs.
    pub fn flush_wal(&self) -> Result<()> {
        let mut wal = self.wal.lock();
        wal.sync()?;
        self.metrics.record_wal_sync();
        self.wal_dirty.store(false, Ordering::Release);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Flush (memtable → SSTable)
    // -----------------------------------------------------------------------

    fn trigger_flush(&self) -> Result<()> {
        let (old_memtable, sst_id, sst_path, wal_id) = {
            let mut version = self.version.write();

            // Rotate memtable
            let old = std::mem::replace(&mut version.active_memtable, Arc::new(MemTable::new()));
            self.immutable_memtables.push(Arc::clone(&old));

            // Rotate WAL
            let new_wal_id = version.manifest.generate_file_id();
            let new_wal_path = self.data_dir.join(format!("{new_wal_id:06}.wal"));
            let current_lsn = self.next_lsn.load(Ordering::Relaxed);
            let new_wal = WalWriter::open(&new_wal_path, current_lsn)?;
            {
                let mut wal_guard = self.wal.lock();
                wal_guard.sync()?;
                self.metrics.record_wal_sync();
                *wal_guard = new_wal;
            }

            let sst_id = version.manifest.generate_file_id();
            let sst_path = self.data_dir.join(format!("{sst_id:06}.sst"));

            (old, sst_id, sst_path, new_wal_id)
        }; // version write-lock released

        // Spawn background flush
        let block_cache = self.block_cache.clone();
        let imm_queue = Arc::clone(&self.immutable_memtables);
        let version_lock = Arc::clone(&self.version);

        tokio::spawn(async move {
            if let Err(e) = Self::flush_memtable(
                sst_path,
                sst_id,
                wal_id,
                old_memtable,
                block_cache,
                imm_queue,
                version_lock,
            ) {
                eprintln!("Background flush failed: {e:?}");
            }
        });

        Ok(())
    }

    /// Background: write an immutable memtable to an SSTable on disk.
    ///
    /// The MANIFEST update is the **last** step. If we crash before the
    /// MANIFEST is written, the SSTable file is orphaned (harmless) and the
    /// data will be replayed from the WAL on restart.
    fn flush_memtable(
        sst_path: PathBuf,
        sst_id: u64,
        wal_id: u64,
        memtable: Arc<MemTable>,
        block_cache: Cache<u64, Arc<Block>>,
        immutable_queue: Arc<ImmutableMemTables>,
        version_lock: Arc<RwLock<VersionSet>>,
    ) -> Result<()> {
        // 1. Build SSTable on disk (no locks held)
        let mut builder = SSTableBuilder::new(&sst_path, 10_000)?;
        for entry in memtable.iter() {
            builder.add(entry.key().as_ref(), entry.value())?;
        }
        builder.finish()?;

        // 2. Update version set (short write-lock)
        let mut version = version_lock.write();
        let reader = SSTableReader::open(&sst_path, block_cache)?;
        version.sstables.push(reader);

        // 3. MANIFEST is the LAST step — atomic commitment point
        version.manifest.log_event(ManifestEvent::AddTable {
            level: 0,
            id: sst_id,
        })?;
        version
            .manifest
            .log_event(ManifestEvent::SetWalId { wal_id })?;
        drop(version);

        // 4. Remove from immutable queue (safe: MANIFEST already persisted)
        immutable_queue.remove_flushed(memtable);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    /// Point lookup. Searches: active memtable → immutable queue → SSTables.
    ///
    /// Uses only a **shared** (`read`) lock on the version set, so multiple
    /// threads can read concurrently without blocking each other or writers.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.metrics.record_get();

        // 1. Active MemTable (under shared lock)
        let version = self.version.read();
        if let Some(val) = version.active_memtable.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }
        drop(version);

        // 2. Immutable MemTables (own internal RwLock)
        if let Some(val) = self.immutable_memtables.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }

        // 3. SSTables (shared lock — SSTableReader::get takes &self)
        let version = self.version.read();
        for reader in version.sstables.iter().rev() {
            if let Some(val) = reader.get(key)? {
                return match val {
                    EntryValue::Value(v) => Ok(Some(v)),
                    EntryValue::Tombstone => Ok(None),
                };
            }
        }

        Ok(None)
    }
}

/// Graceful shutdown: signal the background sync task and perform a final
/// WAL flush so no buffered writes are lost on a clean exit.
impl Drop for ApexEngine {
    fn drop(&mut self) {
        // Signal the background sync task to exit
        self.shutdown.store(true, Ordering::Release);

        // Perform a final synchronous WAL flush
        if let Some(mut wal) = self.wal.try_lock() {
            let _ = wal.sync();
        }
    }
}
