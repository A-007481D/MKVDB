use crate::batch::{BatchOp, WriteBatch};
use crate::error::{ApexError, Result};
use crate::iterator::{DbIterator, MemTableIterator, MergingIterator, ScanStream};
use crate::manifest::{Manifest, ManifestEvent};
use crate::memtable::{EntryValue, ImmutableMemTables, MemTable};
use crate::metrics::EngineMetrics;
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::iterator::SSTableIterator;
use crate::sstable::reader::{Block, SSTableReader};
use crate::wal::{WalReader, WalWriter};
use arc_swap::ArcSwap;
use bytes::Bytes;
use moka::sync::Cache;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, copy};
use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info};

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
    pub levels: Vec<Vec<Arc<SSTableReader>>>,
}

impl Version {
    pub fn l0_count(&self) -> usize {
        self.levels.first().map_or(0, Vec::len)
    }

    pub fn all_sstables(&self) -> Vec<Arc<SSTableReader>> {
        self.levels.iter().flatten().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the ApexEngine.
#[derive(Debug, Clone)]
pub struct ApexConfig {
    pub memtable_size_limit: usize,
    pub l0_compaction_threshold: usize,
    pub immutable_memtable_threshold: usize,
    pub max_concurrent_ops: usize,
    pub sync_policy: SyncPolicy,
}

impl Default for ApexConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism().map_or(4, NonZero::get);

        Self {
            memtable_size_limit: 64 * 1024 * 1024, // 64MB
            l0_compaction_threshold: 4,
            immutable_memtable_threshold: 4, // Allow 4 immutables before stall
            max_concurrent_ops: parallelism * 8,
            sync_policy: SyncPolicy::EveryWrite,
        }
    }
}

impl ApexConfig {
    #[must_use]
    pub fn with_memtable_size(mut self, size: usize) -> Self {
        self.memtable_size_limit = size;
        self
    }

    #[must_use]
    pub fn with_l0_threshold(mut self, count: usize) -> Self {
        self.l0_compaction_threshold = count;
        self
    }

    #[must_use]
    pub fn with_immutable_threshold(mut self, count: usize) -> Self {
        self.immutable_memtable_threshold = count;
        self
    }

    #[must_use]
    pub fn with_max_concurrency(mut self, count: usize) -> Self {
        self.max_concurrent_ops = count;
        self
    }

    #[must_use]
    pub fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }
}

// ---------------------------------------------------------------------------
// ApexEngine
// ---------------------------------------------------------------------------

/// The central coordinator for the ApexDB storage engine.
pub struct ApexEngine {
    pub(crate) data_dir: PathBuf,
    pub(crate) version_set: Arc<ArcSwap<Version>>,
    pub(crate) manifest: Arc<Mutex<Manifest>>,
    pub(crate) wal: Arc<Mutex<WalWriter>>,
    pub(crate) immutable_memtables: Arc<ImmutableMemTables>,
    pub(crate) table_cache: crate::sstable::cache::TableCache,
    pub(crate) block_cache: Cache<(u64, u64), Arc<Block>>,
    pub(crate) next_lsn: AtomicU64,
    pub(crate) config: ApexConfig,
    pub(crate) concurrency_semaphore: Arc<tokio::sync::Semaphore>,
    /// Engine-wide I/O counters for observability.
    pub(crate) metrics: Arc<EngineMetrics>,
    /// Signals the background sync task to shut down gracefully.
    pub(crate) shutdown: Arc<AtomicBool>,
    /// Set to true if there is un-synced WAL data in the buffer.
    wal_dirty: Arc<AtomicBool>,
    /// Notifies waiters when a background sync has completed.
    sync_notifier: Arc<tokio::sync::Notify>,
}

impl ApexEngine {
    /// Opens the database at `path` with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        Self::open_with_config(path, ApexConfig::default())
    }

    /// Opens the database with a specific configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: ApexConfig) -> Result<Arc<Self>> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        // ---- Load Manifest ----
        let manifest_path = data_dir.join("MANIFEST");
        let mut manifest = Manifest::open(&manifest_path)?;

        // ---- Startup GC: Clean up zombie files ----
        Self::startup_gc(&data_dir, &manifest)?;

        let block_cache: Cache<(u64, u64), Arc<Block>> = Cache::new(1024 * 1024 * 1024 / 4096);
        let table_cache = crate::sstable::cache::TableCache::new(&data_dir, 512); // Limit to 512 open files
        let immutable_memtables = Arc::new(ImmutableMemTables::new());

        // ---- Recover SSTables listed in the MANIFEST ----
        let sstables = Self::recover_sstables(
            &data_dir,
            &manifest,
            table_cache.clone(),
            block_cache.clone(),
        )?;

        let active_memtable = Arc::new(MemTable::new());
        let recovered_lsn = Self::replay_wal_files(&data_dir, &manifest, &active_memtable)?;

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
                levels: vec![sstables, Vec::new()], // Initialize with L0 from recovery
            })),
            manifest: Arc::new(Mutex::new(manifest)),
            wal: Arc::new(Mutex::new(wal)),
            immutable_memtables,
            table_cache,
            block_cache,
            next_lsn: AtomicU64::new(recovered_lsn),
            concurrency_semaphore: Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_ops)),
            config: config.clone(),
            metrics,
            shutdown,
            wal_dirty,
            sync_notifier,
        });

        Self::spawn_background_tasks(&engine);

        Ok(engine)
    }

    fn replay_wal_files(data_dir: &Path, manifest: &Manifest, memtable: &MemTable) -> Result<u64> {
        let mut recovered_lsn = 0;
        let mut wal_files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if let Some(stem) = name.strip_suffix(".wal")
                    && let Ok(id) = stem.parse::<u64>()
                    && id >= manifest.wal_id
                {
                    wal_files.push((id, entry.path()));
                }
            }
        }
        wal_files.sort_by_key(|(id, _)| *id);

        for (_, wal_path) in &wal_files {
            let mut reader = WalReader::open(wal_path)?;
            while let Some(record) = reader.next_record()? {
                recovered_lsn = recovered_lsn.max(record.lsn + 1);
                memtable.put(record.key, record.value, record.lsn);
            }
        }
        Ok(recovered_lsn)
    }

    fn spawn_background_tasks(engine: &Arc<Self>) {
        if let SyncPolicy::Delayed(interval) = engine.config.sync_policy {
            let wal_ref = Arc::clone(&engine.wal);
            let dirty_ref = Arc::clone(&engine.wal_dirty);
            let shutdown_ref = Arc::clone(&engine.shutdown);
            let metrics_ref = Arc::clone(&engine.metrics);
            let notifier_ref = Arc::clone(&engine.sync_notifier);

            tokio::spawn(async move {
                Self::background_sync_loop(
                    wal_ref,
                    dirty_ref,
                    shutdown_ref,
                    metrics_ref,
                    notifier_ref,
                    interval,
                )
                .await;
            });
        }

        let comp_version = Arc::clone(&engine.version_set);
        let comp_manifest = Arc::clone(&engine.manifest);
        let comp_cache = engine.block_cache.clone();
        let comp_table_cache = engine.table_cache.clone();
        let comp_dir = engine.data_dir.clone();
        let comp_shutdown = Arc::new(AtomicBool::new(false));
        let comp_l0_threshold = engine.config.l0_compaction_threshold;

        tokio::spawn(async move {
            Self::compaction_loop(
                comp_dir,
                comp_version,
                comp_manifest,
                comp_table_cache,
                comp_cache,
                comp_shutdown,
                comp_l0_threshold,
            )
            .await;
        });
    }

    fn startup_gc(data_dir: &Path, manifest: &Manifest) -> Result<()> {
        let mut active_ssts = HashSet::new();
        for level_ssts in manifest.levels.values() {
            for &id in level_ssts {
                active_ssts.insert(id);
            }
        }

        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().unwrap().to_string_lossy();

            if name.ends_with(".sst") {
                if let Some(id_str) = name.strip_suffix(".sst")
                    && let Ok(id) = id_str.parse::<u64>()
                    && !active_ssts.contains(&id)
                {
                    info!("Startup GC: Deleting zombie SSTable {name}");
                    let _ = std::fs::remove_file(&path);
                }
            } else if name.ends_with(".wal") {
                if let Some(id_str) = name.strip_suffix(".wal")
                    && let Ok(id) = id_str.parse::<u64>()
                    && id < manifest.wal_id
                {
                    info!("Startup GC: Deleting obsolete WAL {name}");
                    let _ = std::fs::remove_file(&path);
                }
            } else if name.ends_with(".tmp") {
                info!("Startup GC: Deleting orphaned temporary file {}", name);
                let _ = std::fs::remove_file(&path);
            }
        }
        Ok(())
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
    pub fn snapshot(self: &Arc<Self>) -> Result<Snapshot> {
        let _permit = self.concurrency_semaphore.try_acquire().map_err(|_| {
            ApexError::EngineOverloaded("Max concurrent operations reached".to_string())
        })?;

        Ok(Snapshot {
            engine: Arc::clone(self),
            version: self.version_set.load_full(),
        })
    }

    /// Returns true if the engine is overwhelmed with pending flushes.
    /// Used for network backpressure to slow down producers.
    pub fn is_saturated(&self) -> bool {
        let version = self.version_set.load();
        self.immutable_memtables.len() >= self.config.immutable_memtable_threshold
            || version.l0_count() >= self.config.l0_compaction_threshold
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
        let _permit = self.concurrency_semaphore.try_acquire().map_err(|_| {
            ApexError::EngineOverloaded("Max concurrent operations reached".to_string())
        })?;

        if batch.is_empty() {
            return Ok(());
        }

        // ---- Stalling Backpressure Loop ----
        let mut backoff_ms = 10;
        let start_stall = Instant::now();

        // admission control stall loop
        loop {
            let imm_count = self.immutable_memtables.len();
            let version = self.version_set.load();
            let l0_count = version.l0_count();

            // Check if we are below thresholds. If so, we can proceed.
            if l0_count < self.config.l0_compaction_threshold
                && imm_count < self.config.immutable_memtable_threshold
            {
                break;
            }

            // Record the stall event
            self.metrics.record_stall();

            if start_stall.elapsed() >= Duration::from_secs(5) {
                // Final check before we give up, to avoid race conditions
                let final_version = self.version_set.load();
                let final_l0 = final_version.l0_count();
                let final_imm = self.immutable_memtables.len();
                if final_l0 < self.config.l0_compaction_threshold
                    && final_imm < self.config.immutable_memtable_threshold
                {
                    break;
                }

                return Err(ApexError::EngineOverloaded(format!(
                    "Engine saturated (L0: {}, Imm: {}) after {:?}",
                    final_l0,
                    final_imm,
                    start_stall.elapsed()
                )));
            }

            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

            // Exponential backoff: double the sleep, up to 500ms
            backoff_ms = std::cmp::min(500, backoff_ms * 2);
        }

        // --- Phase 1: WAL append (under Mutex) ---
        let start_lsn;
        let bytes_written;
        {
            let mut wal = self.wal.lock();
            start_lsn = self
                .next_lsn
                .fetch_add(batch.len() as u64, Ordering::SeqCst);

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

            match self.config.sync_policy {
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
        if let SyncPolicy::Delayed(_) = self.config.sync_policy {
            self.sync_notifier.notified().await;
        }

        // --- Phase 2: MemTable insertion ---
        let version = self.version_set.load();
        for (i, op) in batch.ops.iter().enumerate() {
            let lsn = start_lsn + i as u64;
            match op {
                BatchOp::Put(k, v) => {
                    version
                        .active_memtable
                        .put(k.clone(), EntryValue::Value(v.clone()), lsn);
                }
                BatchOp::Delete(k) => {
                    version
                        .active_memtable
                        .put(k.clone(), EntryValue::Tombstone, lsn);
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
                levels: current_version.levels.clone(),
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
    #[allow(clippy::too_many_arguments)]
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
            let mut new_levels = current_version.levels.clone();
            if new_levels.is_empty() {
                new_levels.push(Vec::new());
            }
            new_levels[0].push(Arc::new(reader.clone()));

            let new_version = Arc::new(Version {
                active_memtable: Arc::clone(&current_version.active_memtable),
                levels: new_levels,
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
        let _permit = self.concurrency_semaphore.try_acquire().map_err(|_| {
            ApexError::EngineOverloaded("Max concurrent operations reached".to_string())
        })?;

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

        // 3. Search L0 SSTables (newest first)
        if let Some(l0) = version.levels.first() {
            for reader in l0.iter().rev() {
                if let Some((val, _lsn)) = reader.get(key)? {
                    match val {
                        EntryValue::Value(v) => return Ok(Some(v)),
                        EntryValue::Tombstone => return Ok(None),
                    }
                }
            }
        }

        // 4. Search L1+ SSTables
        for level_idx in 1..version.levels.len() {
            if let Some(readers) = version.levels.get(level_idx) {
                for reader in readers {
                    if let Some((val, _lsn)) = reader.get(key)? {
                        match val {
                            EntryValue::Value(v) => return Ok(Some(v)),
                            EntryValue::Tombstone => return Ok(None),
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Performs a range scan, returning an async stream of key-value pairs.
    /// The stream is lazy and snapshot-consistent.
    pub fn scan(&self, start_key: Bytes, end_key: Bytes) -> Result<ScanStream> {
        let _permit = self.concurrency_semaphore.try_acquire().map_err(|_| {
            ApexError::EngineOverloaded("Max concurrent operations reached".to_string())
        })?;

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
        iterators.push(Box::new(MemTableIterator::new(
            version.active_memtable.clone(),
        )));

        // 2. Immutable MemTables (if this is called from the engine, we use its snapshots)
        // If this is a snapshot scan, we should technically use the immutables from the snapshot point.
        // For now, ApexEngine keeps immutables separate.
        for imm in self.immutable_memtables.snapshot() {
            iterators.push(Box::new(MemTableIterator::new(imm)));
        }

        // 3. SSTables
        for level in &version.levels {
            for reader in level {
                iterators.push(Box::new(SSTableIterator::new(Arc::clone(reader))?));
            }
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
        let _permit = self.concurrency_semaphore.try_acquire().map_err(|_| {
            ApexError::EngineOverloaded("Max concurrent operations reached".to_string())
        })?;

        self.trigger_flush()
    }

    /// Creates a consistent checkpoint of the database at the given path.
    /// SSTables and the MANIFEST are hard-linked for efficiency, while the
    /// active WAL is copied to ensure the checkpoint is immutable.
    ///
    /// This is a critical building block for Raft snapshotting.
    /// Creates a consistent checkpoint of the database at the given path.
    /// SSTables are hard-linked for efficiency, while the MANIFEST and the
    /// active WAL are copied to ensure the checkpoint is immutable and consistent.
    ///
    /// This is a critical building block for Raft snapshotting.
    pub fn create_checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        let _permit = self.concurrency_semaphore.try_acquire().map_err(|_| {
            ApexError::EngineOverloaded("Max concurrent operations reached".to_string())
        })?;

        if checkpoint_path.exists() {
            return Err(ApexError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "Checkpoint path already exists",
            )));
        }

        tracing::info!("Creating checkpoint at {:?}", checkpoint_path);
        std::fs::create_dir_all(checkpoint_path)?;

        // 1. Capture metadata and MANIFEST (Blocking)
        // We hold the manifest lock to "pin" the current set of SSTables so
        // compaction doesn't delete them while we are hard-linking.
        let (_sst_ids, wal_id, active_wal_id, active_wal_size) = {
            let manifest = self.manifest.lock();
            let wal_id = manifest.wal_id;

            // Collect all active SST IDs
            let mut sst_ids = Vec::new();
            for level_ssts in manifest.levels.values() {
                for &id in level_ssts {
                    sst_ids.push(id);
                }
            }

            // Copy MANIFEST while holding the lock
            std::fs::copy(
                self.data_dir.join("MANIFEST"),
                checkpoint_path.join("MANIFEST"),
            )?;

            // Capture WAL state
            let (active_id, current_size) = {
                let mut wal_guard = self.wal.lock();
                wal_guard.sync()?; // Flush user-space buffers
                let size = wal_guard.file_size()?;
                (wal_guard.id, size)
            };

            // Hard-link SSTables while still holding Manifest lock to prevent deletion
            for id in &sst_ids {
                let name = format!("{id:06}.sst");
                let src = self.data_dir.join(&name);
                let dst = checkpoint_path.join(&name);
                tracing::debug!("Checkpoint: Hard-linking {name}");
                std::fs::hard_link(src, dst)?;
            }

            (sst_ids, wal_id, active_id, current_size)
        }; // locks released

        // 2. Process WAL files (Non-blocking)
        for id in wal_id..=active_wal_id {
            let name = format!("{id:06}.wal");
            let src = self.data_dir.join(&name);
            let dst = checkpoint_path.join(&name);

            if !src.exists() {
                tracing::warn!("Checkpoint: Expected WAL {name} not found, skipping");
                continue;
            }

            if id == active_wal_id {
                // For the active WAL, we perform a partial copy up to the captured size.
                // This ensures we have a consistent point-in-time view even if writes
                // continue appending to the original file.
                tracing::debug!("Checkpoint: Performing partial copy of active WAL {name}");
                let mut src_file = File::open(&src)?;
                let mut dst_file = File::create(&dst)?;
                copy(&mut (&mut src_file).take(active_wal_size), &mut dst_file)?;
                dst_file.sync_all()?;
            } else {
                // Historical WALs are immutable, hard-link them
                tracing::debug!("Checkpoint: Hard-linking historical WAL {name}");
                std::fs::hard_link(src, dst)?;
            }
        }

        // Final sync of the checkpoint directory
        let cp_dir = File::open(checkpoint_path)?;
        cp_dir.sync_all()?;

        tracing::info!("Checkpoint created successfully at {:?}", checkpoint_path);
        Ok(())
    }

    /// Gracefully shuts down the engine, ensuring all buffered data is persisted.
    ///
    /// 1. Signals background tasks (sync, compaction) to exit.
    /// 2. Flushes the active MemTable to an SSTable.
    /// 3. Synchronously flushes and closes the WAL.
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down ApexEngine...");

        // 1. Signal background tasks
        self.shutdown.store(true, Ordering::Release);
        self.sync_notifier.notify_waiters();

        // 2. Force flush active memtable
        // This ensures all data is in SSTables and WAL can be truncated/deleted on next start.
        if self.version_set.load().active_memtable.size() > 0 {
            info!("Shutdown: Flushing active memtable...");
            self.force_flush()?;
        }

        // 3. Final WAL sync
        let mut wal = self.wal.lock();
        wal.sync()?;
        info!("ApexEngine shutdown complete.");

        Ok(())
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

        // Search L0 SSTables
        if let Some(l0) = self.version.levels.first() {
            for reader in l0.iter().rev() {
                if let Some((val, _lsn)) = reader.get(key)? {
                    match val {
                        EntryValue::Value(v) => return Ok(Some(v)),
                        EntryValue::Tombstone => return Ok(None),
                    }
                }
            }
        }

        // Search L1+ SSTables
        for level_idx in 1..self.version.levels.len() {
            if let Some(readers) = self.version.levels.get(level_idx) {
                for reader in readers {
                    if let Some((val, _lsn)) = reader.get(key)? {
                        match val {
                            EntryValue::Value(v) => return Ok(Some(v)),
                            EntryValue::Tombstone => return Ok(None),
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Performs a range scan on this snapshot.
    pub fn scan(&self, start_key: Bytes, end_key: Bytes) -> Result<ScanStream> {
        self.engine
            .create_scan_stream(self.version.clone(), start_key, Some(end_key))
    }
}
