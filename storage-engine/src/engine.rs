use crate::error::Result;
use crate::manifest::{Manifest, ManifestEvent};
use crate::memtable::{EntryValue, ImmutableMemTables, MemTable};
use crate::metrics::EngineMetrics;
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::reader::{Block, SSTableReader};
use crate::wal::{WalReader, WalWriter};
use bytes::Bytes;
use moka::sync::Cache;
use parking_lot::Mutex;
use arc_swap::ArcSwap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use crate::iterator::{DbIterator, MemTableIterator, MergingIterator, ScanStream};
use crate::sstable::iterator::SSTableIterator;
use crate::batch::{WriteBatch, BatchOp};
use tracing::{info, error};

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
// Version — the immutable structural view of the database
// ---------------------------------------------------------------------------

/// The "version": the structural view of the database that readers need.
///
/// Managed via `ArcSwap` for lock-free lock-stripping.
pub struct Version {
    pub active_memtable: Arc<MemTable>,
    pub sstables: Vec<Arc<SSTableReader>>,
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
    pub(crate) data_dir: PathBuf,
    pub(crate) version_set: Arc<ArcSwap<Version>>,
    pub(crate) manifest: Arc<Mutex<Manifest>>,
    pub(crate) wal: Arc<Mutex<WalWriter>>,
    pub(crate) immutable_memtables: Arc<ImmutableMemTables>,
    pub(crate) table_cache: crate::sstable::cache::TableCache,
    pub(crate) block_cache: Cache<(u64, u64), Arc<Block>>,
    /// Monotonically increasing sequence number. Atomic so the WAL append
    /// (under Mutex) and the memtable insert can share the value without
    /// needing the VersionSet lock.
    pub(crate) next_lsn: AtomicU64,
    pub(crate) sync_policy: SyncPolicy,
    /// Engine-wide I/O counters for observability.
    pub(crate) metrics: Arc<EngineMetrics>,
    /// Signals the background sync task to shut down gracefully.
    pub(crate) shutdown: Arc<AtomicBool>,
    /// Set to true if there is un-synced WAL data in the buffer.
    /// The background sync task checks this to avoid unnecessary fsync calls.
    wal_dirty: Arc<AtomicBool>,
    /// Notifies waiters when a background sync has completed.
    sync_notifier: Arc<tokio::sync::Notify>,
}

impl ApexEngine {
    /// Opens the database at `path`, replaying WALs for crash recovery.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        Self::open_with_policy(path, SyncPolicy::EveryWrite)
    }

    /// Opens the database with a specific WAL sync policy.
    pub fn open_with_policy<P: AsRef<Path>>(path: P, sync_policy: SyncPolicy) -> Result<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let manifest_path = data_dir.join("MANIFEST");
        let mut manifest = Manifest::open(&manifest_path)?;

        let block_cache: Cache<(u64, u64), Arc<Block>> = Cache::new(1024 * 1024 * 1024 / 4096);
        let table_cache = crate::sstable::cache::TableCache::new(&data_dir, 512); // Limit to 512 open files
        let immutable_memtables = Arc::new(ImmutableMemTables::new());

        // ---- Recover SSTables listed in the MANIFEST ----
        let sstables = Self::recover_sstables(&data_dir, &manifest, table_cache.clone(), block_cache.clone())?;


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

        // ---- Initialize Engine State ----
        let metrics = Arc::new(EngineMetrics::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let wal_dirty = Arc::new(AtomicBool::new(false));
        let sync_notifier = Arc::new(tokio::sync::Notify::new());

        let engine = Arc::new(Self {
            data_dir,
            version_set: Arc::new(ArcSwap::from_pointee(Version {
                active_memtable: active_memtable.clone(),
                sstables: sstables.clone(),
            })),
            manifest: Arc::new(Mutex::new(manifest)),
            wal: Arc::new(Mutex::new(wal)),
            immutable_memtables,
            table_cache,
            block_cache,
            next_lsn: AtomicU64::new(recovered_lsn),
            sync_policy: sync_policy.clone(),
            metrics,
            shutdown,
            wal_dirty,
            sync_notifier,
        });

        // ---- Spawn background sync task if Delayed ----
        if let SyncPolicy::Delayed(interval) = &sync_policy {
            let wal_ref = Arc::clone(&engine.wal);
            let dirty_ref = Arc::clone(&engine.wal_dirty);
            let shutdown_ref = Arc::clone(&engine.shutdown);
            let metrics_ref = Arc::clone(&engine.metrics);
            let notifier_ref = Arc::clone(&engine.sync_notifier);
            let interval = *interval;

            tokio::spawn(async move {
                Self::background_sync_loop(wal_ref, dirty_ref, shutdown_ref, metrics_ref, notifier_ref, interval)
                    .await;
            });
        }

        // ---- Spawn background compaction task ----
        let comp_version = Arc::clone(&engine.version_set);
        let comp_manifest = Arc::clone(&engine.manifest);
        let comp_cache = engine.block_cache.clone();
        let comp_table_cache = engine.table_cache.clone();
        let comp_dir = engine.data_dir.clone();
        let comp_shutdown = Arc::new(AtomicBool::new(false));

        tokio::spawn(async move {
            Self::compaction_loop(
                comp_dir,
                comp_version,
                comp_manifest,
                comp_table_cache,
                comp_cache,
                comp_shutdown,
            )
            .await;
        });

        Ok(engine)
    }

    fn recover_sstables(
        _data_dir: &Path,
        manifest: &Manifest,
        table_cache: crate::sstable::cache::TableCache,
        block_cache: Cache<(u64, u64), Arc<Block>>,
    ) -> Result<Vec<Arc<SSTableReader>>> {
        let mut sstables = Vec::new();
        for level_ssts in manifest.levels.values() {
            for &sst_id in level_ssts {
                let reader = SSTableReader::open(sst_id, table_cache.clone(), block_cache.clone())?;
                sstables.push(Arc::new(reader));
            }
        }
        Ok(sstables)
    }

    /// Returns a reference to the engine's I/O metrics.
    #[must_use]
    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    /// Creates a point-in-time snapshot of the current version.
    /// This snapshot will remain valid even if compaction deletes files.
    pub fn snapshot(self: &Arc<Self>) -> Snapshot {
        Snapshot {
            engine: Arc::clone(self),
            version: self.version_set.load_full(),
        }
    }

    /// Returns true if the engine is overwhelmed with pending flushes.
    /// Used for network backpressure to slow down producers.
    pub fn is_saturated(&self) -> bool {
        self.immutable_memtables.len() >= 5
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
        notifier: Arc<tokio::sync::Notify>,
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
                let mut wal_guard = wal.lock();
                if let Err(e) = wal_guard.sync() {
                    eprintln!("Background WAL sync failed: {e:?}");
                } else {
                    metrics.record_wal_sync();
                }
                // Notify everyone who was waiting for this sync batch
                notifier.notify_waiters();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write_batch(batch).await
    }

    pub async fn delete(&self, key: Bytes) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write_batch(batch).await
    }

    /// Applies a batch of operations atomically.
    pub async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // --- Phase 1: WAL append (under Mutex) ---
        let start_lsn;
        let bytes_written;
        {
            let mut wal = self.wal.lock();
            start_lsn = self.next_lsn.fetch_add(batch.len() as u64, Ordering::SeqCst);
            
            let mut total_bytes = 0;
            for (i, op) in batch.ops.iter().enumerate() {
                let lsn = start_lsn + i as u64;
                match op {
                    BatchOp::Put(k, v) => {
                        total_bytes += wal.append(lsn, k, &EntryValue::Value(v.clone()))?;
                    }
                    BatchOp::Delete(k) => {
                        total_bytes += wal.append(lsn, k, &EntryValue::Tombstone)?;
                    }
                }
            }
            bytes_written = total_bytes;

            match self.sync_policy {
                SyncPolicy::EveryWrite => {
                    wal.sync()?;
                    self.metrics.record_wal_sync();
                }
                SyncPolicy::Delayed(_) => {
                    self.wal_dirty.store(true, Ordering::Release);
                }
                SyncPolicy::Buffered => {}
            }
        }

        self.metrics.record_wal_write(bytes_written);

        // If Delayed, wait for the background worker to sync
        if let SyncPolicy::Delayed(_) = self.sync_policy {
            self.sync_notifier.notified().await;
        }

        // --- Phase 2: MemTable insertion ---
        let version = self.version_set.load();
        for (i, op) in batch.ops.iter().enumerate() {
            let lsn = start_lsn + i as u64;
            match op {
                BatchOp::Put(k, v) => {
                    version.active_memtable.put(k.clone(), EntryValue::Value(v.clone()), lsn);
                }
                BatchOp::Delete(k) => {
                    version.active_memtable.put(k.clone(), EntryValue::Tombstone, lsn);
                }
            }
        }

        // --- Phase 3: Flush trigger ---
        if version.active_memtable.size() > MEMTABLE_SIZE_LIMIT {
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
            let mut manifest = self.manifest.lock();

            // Rotate memtable & create new version
            let old = self.version_set.load().active_memtable.clone();
            self.immutable_memtables.push(Arc::clone(&old));

            // Rotate WAL
            let new_wal_id = manifest.generate_file_id();
            let new_wal_path = self.data_dir.join(format!("{new_wal_id:06}.wal"));
            let current_lsn = self.next_lsn.load(Ordering::Relaxed);
            let new_wal = WalWriter::open(&new_wal_path, current_lsn)?;
            {
                let mut wal_guard = self.wal.lock();
                wal_guard.sync()?;
                self.metrics.record_wal_sync();
                *wal_guard = new_wal;
            }

            let sst_id = manifest.generate_file_id();
            let sst_path = self.data_dir.join(format!("{sst_id:06}.sst"));

            let current_version = self.version_set.load();
            let new_version = Version {
                active_memtable: Arc::new(MemTable::new()),
                sstables: current_version.sstables.clone(),
            };
            self.version_set.store(Arc::new(new_version));

            (old, sst_id, sst_path, new_wal_id)
        }; // manifest lock released

        // Spawn background flush
        let block_cache = self.block_cache.clone();
        let table_cache = self.table_cache.clone();
        let imm_queue = Arc::clone(&self.immutable_memtables);
        let version_arc = Arc::clone(&self.version_set);
        let manifest_arc = Arc::clone(&self.manifest);

        tokio::spawn(async move {
            if let Err(e) = Self::flush_memtable(
                sst_path,
                sst_id,
                wal_id,
                old_memtable,
                table_cache,
                block_cache,
                imm_queue,
                version_arc,
                manifest_arc,
            ) {
                error!("Background flush failed for SSTable {}: {:?}", sst_id, e);
            } else {
                info!("Successfully flushed SSTable {}", sst_id);
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
        table_cache: crate::sstable::cache::TableCache,
        block_cache: Cache<(u64, u64), Arc<Block>>,
        immutable_queue: Arc<ImmutableMemTables>,
        version_arc: Arc<ArcSwap<Version>>,
        manifest_arc: Arc<Mutex<Manifest>>,
    ) -> Result<()> {
        // 1. Build SSTable on disk (no locks held)
        let mut builder = SSTableBuilder::new(&sst_path, 10_000)?;
        for entry in memtable.iter() {
            builder.add(entry.key().as_ref(), &entry.value().0, entry.value().1)?;
        }
        builder.finish()?;

        // 2. Commit SSTable to VersionSet (with atomic switch)
        let reader = SSTableReader::open(sst_id, table_cache, block_cache)?;
        
        // We use a simple loop for compare-and-swap if another thread (e.g. compaction) 
        // updated the version in the meantime.
        let mut current_version = version_arc.load();
        loop {
            let mut new_sstables = current_version.sstables.clone();
            new_sstables.push(Arc::new(reader.clone())); // reader cloning is cheap thanks to Arc inside

            let new_version = Arc::new(Version {
                active_memtable: Arc::clone(&current_version.active_memtable),
                sstables: new_sstables,
            });

            let prev = version_arc.compare_and_swap(&current_version, new_version);
            if Arc::ptr_eq(&prev, &current_version) {
                break;
            }
            current_version = prev;
        }

        // 3. MANIFEST is the LAST step — atomic commitment point
        let mut manifest = manifest_arc.lock();
        manifest.log_event(ManifestEvent::AddTable {
            level: 0,
            id: sst_id,
        })?;
        manifest.log_event(ManifestEvent::SetWalId { wal_id })?;
        drop(manifest);

        // 4. Remove from immutable queue (safe: MANIFEST already persisted)
        immutable_queue.remove_flushed(memtable);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.metrics.record_get();

        // 1. Active MemTable (lock-free)
        let version = self.version_set.load();
        if let Some(val) = version.active_memtable.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }

        // 2. Immutable MemTables (own internal RwLock)
        if let Some(val) = self.immutable_memtables.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }

        // 3. SSTables (lock-free)
        for reader in version.sstables.iter().rev() {
            if let Some((val, _lsn)) = reader.get(key)? {
                return match val {
                    EntryValue::Value(v) => Ok(Some(v)),
                    EntryValue::Tombstone => Ok(None),
                };
            }
        }

        Ok(None)
    }

    /// Performs a range scan, returning an async stream of key-value pairs.
    /// The stream is lazy and snapshot-consistent.
    pub fn scan(&self, start_key: Bytes, end_key: Bytes) -> Result<ScanStream> {
        let version = self.version_set.load();
        self.create_scan_stream(version.clone(), start_key, Some(end_key))
    }

    /// Internal helper to create a scan stream from a specific version snapshot.
    fn create_scan_stream(
        &self,
        version: Arc<Version>,
        start_key: Bytes,
        end_key: Option<Bytes>,
    ) -> Result<ScanStream> {
        let mut iterators: Vec<Box<dyn DbIterator>> = Vec::new();

        // 1. Active MemTable
        iterators.push(Box::new(MemTableIterator::new(version.active_memtable.clone())));

        // 2. Immutable MemTables (if this is called from the engine, we use its snapshots)
        // If this is a snapshot scan, we should technically use the immutables from the snapshot point.
        // For now, ApexEngine keeps immutables separate.
        for imm in self.immutable_memtables.snapshot() {
            iterators.push(Box::new(MemTableIterator::new(imm)));
        }

        // 3. SSTables
        for sst in &version.sstables {
            iterators.push(Box::new(SSTableIterator::new(Arc::clone(sst))?));
        }

        let mut merging_iter = MergingIterator::new(iterators)?;

        // Seek to start_key (O(N) for now, should be O(log N) in future)
        while merging_iter.is_valid() && merging_iter.key().as_ref() < start_key.as_ref() {
            merging_iter.next()?;
        }

        Ok(ScanStream::new(version, merging_iter, end_key))
    }

    /// Manually triggers a flush of the current memtable to an SSTable on disk.
    /// Useful for testing and controlled persistence.
    pub fn force_flush(&self) -> Result<()> {
        self.trigger_flush()
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
/// A point-in-time view of the database.
pub struct Snapshot {
    #[allow(dead_code)]
    engine: Arc<ApexEngine>,
    version: Arc<Version>,
}

impl Snapshot {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(val) = self.version.active_memtable.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }

        for reader in self.version.sstables.iter().rev() {
            if let Some((val, _lsn)) = reader.get(key)? {
                return match val {
                    EntryValue::Value(v) => Ok(Some(v)),
                    EntryValue::Tombstone => Ok(None),
                };
            }
        }

        Ok(None)
    }

    /// Performs a range scan on this snapshot.
    pub fn scan(&self, start_key: Bytes, end_key: Bytes) -> Result<ScanStream> {
        self.engine.create_scan_stream(self.version.clone(), start_key, Some(end_key))
    }
}
