use crate::engine::ApexEngine;
use crate::error::Result;
use crate::iterator::{DbIterator, MergingIterator};
use crate::manifest::{Manifest, ManifestEvent};
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::iterator::SSTableIterator;
use crate::sstable::reader::{Block, SSTableReader};
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use parking_lot::Mutex;
use crate::engine::Version;
use std::path::{Path, PathBuf};
use moka::sync::Cache;

const TARGET_SST_SIZE: usize = 64 * 1024 * 1024; // 64 MB

impl ApexEngine {
    /// Background loop that periodically checks if compaction is needed.
    pub(crate) async fn compaction_loop(
        data_dir: PathBuf,
        version: Arc<ArcSwap<Version>>,
        manifest: Arc<Mutex<Manifest>>,
        table_cache: crate::sstable::cache::TableCache,
        block_cache: Cache<(u64, u64), Arc<Block>>,
        shutdown: Arc<AtomicBool>,
    ) {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;

            if shutdown.load(Ordering::Acquire) {
                return;
            }

            if let Err(e) = Self::maybe_compact(
                &data_dir,
                &version,
                &manifest,
                &table_cache,
                &block_cache,
            ) {
                eprintln!("Compaction error: {e:?}");
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn maybe_compact(
        data_dir: &Path,
        version_arc: &Arc<ArcSwap<Version>>,
        manifest_arc: &Arc<Mutex<Manifest>>,
        table_cache: &crate::sstable::cache::TableCache,
        block_cache: &Cache<(u64, u64), Arc<Block>>,
    ) -> Result<()> {
        let l0_ids: Vec<u64>;
        let l1_ids: Vec<u64>;

        {
            let manifest_guard = manifest_arc.lock();
            let l0 = manifest_guard.levels.get(&0).cloned().unwrap_or_default();
            
            // Only trigger if L0 > 4 files
            if l0.len() <= 4 {
                return Ok(());
            }

            l0_ids = l0.into_iter().collect();
            l1_ids = manifest_guard.levels.get(&1).cloned().unwrap_or_default().into_iter().collect();
        }

        tracing::info!(
            "Starting L0->L1 compaction: merging {} L0 files and {} L1 files",
            l0_ids.len(),
            l1_ids.len()
        );

        let current_version = version_arc.load();
        
        let mut compact_readers = Vec::new();
        let mut remaining_sstables = Vec::new();
        for reader in &current_version.sstables {
            if l0_ids.contains(&reader.id) || l1_ids.contains(&reader.id) {
                compact_readers.push(Arc::clone(reader));
            } else {
                remaining_sstables.push(Arc::clone(reader));
            }
        }

        // Create Merging Iterator
        let mut iterators: Vec<Box<dyn DbIterator>> = Vec::new();
        for sst in compact_readers {
            iterators.push(Box::new(SSTableIterator::new(sst)?));
        }
        let mut merging_iter = MergingIterator::new(iterators)?;

        // Execute Compaction Job
        let mut new_l1_ids = Vec::new();
        let mut current_builder: Option<SSTableBuilder> = None;
        let mut current_id = 0;
        let mut current_path = PathBuf::new();

        while merging_iter.is_valid() {
            let key = merging_iter.key();
            let val = merging_iter.value();
            let lsn = merging_iter.lsn();

            if current_builder.is_none() {
                {
                    let mut manifest_guard = manifest_arc.lock();
                    current_id = manifest_guard.generate_file_id();
                }
                current_path = data_dir.join(format!("{current_id:06}.sst"));
                // Estimated keys: we don't know, but let's assume 100k for the bloom filter
                current_builder = Some(SSTableBuilder::new(&current_path, 100_000)?);
                new_l1_ids.push(current_id);
            }

            // --- TOMBSTONE GC ---
            // A tombstone can only be purged if we are merging into the bottom-most level.
            // In our current 2-level system, L1 is the floor.
            let is_bottom_level = true; 
            if !val.is_tombstone() || !is_bottom_level {
                current_builder.as_mut().unwrap().add(key.as_ref(), &val, lsn)?;
            } else {
                tracing::debug!("Tombstone GC: dropping key {:?}", key);
            }

            if current_builder.as_ref().unwrap().estimated_size() > TARGET_SST_SIZE as u64 {
                let builder = current_builder.take().unwrap();
                builder.finish()?;
                let reader = SSTableReader::open(current_id, table_cache.clone(), block_cache.clone())?;
                remaining_sstables.push(Arc::new(reader));
            }

            merging_iter.next()?;
        }

        if let Some(builder) = current_builder {
            if builder.estimated_size() > 0 {
                builder.finish()?;
                let reader = SSTableReader::open(current_id, table_cache.clone(), block_cache.clone())?;
                remaining_sstables.push(Arc::new(reader));
            } else {
                // Remove empty file
                let _ = std::fs::remove_file(&current_path);
                new_l1_ids.pop();
            }
        }

        // --- Commit Compaction ---
        // 1. Update Version (CAS loop)
        loop {
            let curr = version_arc.load();
            let mut final_sstables = remaining_sstables.clone();
            
            // Re-add SSTables that were added by concurrent flushes
            for sst in &curr.sstables {
                if !l0_ids.contains(&sst.id) && 
                   !l1_ids.contains(&sst.id) && 
                   !new_l1_ids.contains(&sst.id) 
                {
                    final_sstables.push(Arc::clone(sst));
                }
            }

            let new_version = Arc::new(Version {
                active_memtable: Arc::clone(&curr.active_memtable),
                sstables: final_sstables,
            });

            let prev = version_arc.compare_and_swap(&curr, new_version);
            if Arc::ptr_eq(&prev, &curr) {
                break;
            }
        }

        // 2. Log Manifest Events
        {
            let mut manifest_guard = manifest_arc.lock();
            for id in &l0_ids {
                manifest_guard.log_event(ManifestEvent::RemoveTable { level: 0, id: *id })?;
            }
            for id in &l1_ids {
                manifest_guard.log_event(ManifestEvent::RemoveTable { level: 1, id: *id })?;
            }
            for id in &new_l1_ids {
                manifest_guard.log_event(ManifestEvent::AddTable { level: 1, id: *id })?;
            }
            
            // Harden the manifest by checkpointing
            if let Err(e) = manifest_guard.checkpoint() {
                tracing::warn!("Manifest checkpoint failed: {:?}", e);
            }
        }

        // 3. Self-Cleaning: Unlink old files
        for id in l0_ids.iter().chain(l1_ids.iter()) {
            let path = data_dir.join(format!("{id:06}.sst"));
            tracing::info!("Compaction: Purging old SSTable {}", id);
            let _ = std::fs::remove_file(path);
        }

        tracing::info!("L0->L1 compaction completed successfully. New L1 files: {:?}", new_l1_ids);
        Ok(())
    }
}
