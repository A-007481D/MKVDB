use crate::error::{ApexError, Result};
use crate::manifest::{Manifest, ManifestEvent};
use crate::memtable::{EntryValue, ImmutableMemTables, MemTable};
use crate::sstable::{builder::SSTableBuilder, reader::{Block, SSTableReader}};
use crate::wal::WalWriter;
use bytes::Bytes;
use moka::sync::Cache;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MEMTABLE_SIZE_LIMIT: usize = 64 * 1024 * 1024; // 64 MB

pub struct EngineState {
    pub active_memtable: Arc<MemTable>,
    pub wal: WalWriter,
    pub manifest: Manifest,
    pub sstables: Vec<SSTableReader>, // Simplified for Phase 1: Just Level 0
}

/// The central coordinator for the ApexDB storage engine.
pub struct ApexEngine {
    data_dir: PathBuf,
    state: Arc<RwLock<EngineState>>,
    immutable_memtables: Arc<ImmutableMemTables>,
    block_cache: Cache<u64, Arc<Block>>,
}

impl ApexEngine {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data_dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let manifest_path = data_dir.join("MANIFEST");
        let mut manifest = Manifest::open(&manifest_path)?;

        let start_lsn = manifest.current_seq;
        let block_cache = Cache::new(1024 * 1024 * 1024 / 4096); // ~1GB cache (blocks of 4KB)
        let immutable_memtables = Arc::new(ImmutableMemTables::new());

        // For simplicity in Phase 1 start, we just initialize a fresh MemTable and WAL.
        // Recovery logic would read existing WALs here.
        let active_memtable = Arc::new(MemTable::new());
        let wal_id = manifest.generate_file_id();
        let wal_path = data_dir.join(format!("{:06}.wal", wal_id));
        let wal = WalWriter::open(&wal_path, start_lsn)?;

        let state = EngineState {
            active_memtable,
            wal,
            manifest,
            sstables: Vec::new(),
        };

        Ok(Self {
            data_dir,
            state: Arc::new(RwLock::new(state)),
            immutable_memtables,
            block_cache,
        })
    }

    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.write_internal(key, EntryValue::Value(value))
    }

    pub fn delete(&self, key: Bytes) -> Result<()> {
        self.write_internal(key, EntryValue::Tombstone)
    }

    fn write_internal(&self, key: Bytes, value: EntryValue) -> Result<()> {
        let mut state = self.state.write();
        
        let lsn = state.wal.append(&key, &value)?;
        state.wal.sync()?; // Fsync for every write. In production we'd batch this or use a background sync thread.
        
        state.active_memtable.put(key, value, lsn);
        
        state.manifest.log_event(ManifestEvent::UpdateSeq { seq: lsn })?;

        // Check MemTable size
        if state.active_memtable.size() >= MEMTABLE_SIZE_LIMIT {
            self.trigger_flush(&mut state)?;
        }

        Ok(())
    }

    fn trigger_flush(&self, state: &mut EngineState) -> Result<()> {
        let old_memtable = std::mem::replace(&mut state.active_memtable, Arc::new(MemTable::new()));
        self.immutable_memtables.push(old_memtable.clone());

        let wal_id = state.manifest.generate_file_id();
        let wal_path = self.data_dir.join(format!("{:06}.wal", wal_id));
        let new_wal = WalWriter::open(&wal_path, state.manifest.current_seq)?;
        state.wal = new_wal;

        let sst_id = state.manifest.generate_file_id();
        let sst_path = self.data_dir.join(format!("{:06}.sst", sst_id));
        
        let block_cache = self.block_cache.clone();
        let immutable_queue = self.immutable_memtables.clone();
        let state_lock = self.state.clone();

        // Spawn background flush thread
        tokio::spawn(async move {
            if let Err(e) = Self::flush_memtable(sst_path, sst_id, old_memtable.clone(), block_cache, immutable_queue, state_lock) {
                eprintln!("Background flush failed: {:?}", e);
            }
        });

        Ok(())
    }

    fn flush_memtable(
        sst_path: PathBuf,
        sst_id: u64,
        memtable: Arc<MemTable>,
        block_cache: Cache<u64, Arc<Block>>,
        immutable_queue: Arc<ImmutableMemTables>,
        state_lock: Arc<RwLock<EngineState>>,
    ) -> Result<()> {
        let mut builder = SSTableBuilder::new(&sst_path, 10_000)?;
        
        // Write out entries
        for entry in memtable.iter() {
            builder.add(entry.key().as_ref(), entry.value())?;
        }
        builder.finish()?;

        // Add to version set
        let mut state = state_lock.write();
        state.manifest.log_event(ManifestEvent::AddTable { level: 0, id: sst_id })?;
        
        let reader = SSTableReader::open(&sst_path, block_cache)?;
        state.sstables.push(reader);

        // Remove from immutable queue
        immutable_queue.remove_flushed(memtable);

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // 1. Active MemTable
        let state = self.state.read();
        if let Some(val) = state.active_memtable.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }
        drop(state);

        // 2. Immutable MemTables
        if let Some(val) = self.immutable_memtables.get(key) {
            return match val {
                EntryValue::Value(v) => Ok(Some(v)),
                EntryValue::Tombstone => Ok(None),
            };
        }

        // 3. SSTables (Level 0, reverse order to get newest first)
        let mut state = self.state.write(); // Need write lock for SSTableReader get due to potential cache updates or internal seeking
        for reader in state.sstables.iter_mut().rev() {
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
