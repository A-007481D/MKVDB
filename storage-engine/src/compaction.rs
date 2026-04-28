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
use std::path::PathBuf;
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

    fn maybe_compact(
        data_dir: &PathBuf,
        version_arc: &Arc<ArcSwap<Version>>,
        manifest_arc: &Arc<Mutex<Manifest>>,
        table_cache: &crate::sstable::cache::TableCache,
        block_cache: &Cache<(u64, u64), Arc<Block>>,
    ) -> Result<()> {
        let manifest_guard = manifest_arc.lock();
        
        let l0_count = manifest_guard.levels.get(&0).map(|s| s.len()).unwrap_or(0);
        if l0_count < 4 {
            return Ok(()); // No compaction needed
        }

        // We need to compact!
        // Select all L0 files and all L1 files (for simplicity in Phase 2, we merge all L0+L1).
        let l0_ids: Vec<u64> = manifest_guard.levels.get(&0).cloned().unwrap_or_default().into_iter().collect();
        let l1_ids: Vec<u64> = manifest_guard.levels.get(&1).cloned().unwrap_or_default().into_iter().collect();
        
        // We drop the lock here because building the new SSTable takes time.
        // But wait! If another flush happens, it will add to L0. That's fine, we only compact the ids we selected.
        drop(manifest_guard);

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
        let mut current_path = Default::default();

        while merging_iter.is_valid() {
            let key = merging_iter.key();
            let val = merging_iter.value();
            let lsn = merging_iter.lsn();

            if current_builder.is_none() {
                let mut manifest_guard = manifest_arc.lock();
                current_id = manifest_guard.generate_file_id();
                drop(manifest_guard);
                current_path = data_dir.join(format!("{current_id:06}.sst"));
                current_builder = Some(SSTableBuilder::new(&current_path, 10_000)?);
                new_l1_ids.push(current_id);
            }

            // --- TOMBSTONE SAFETY ---
            // A tombstone can only be purged if we are merging into the bottom-most level.
            // In Phase 2, Level 1 is the floor, so it is safe to purge.
            let is_bottom_level = true; 
            if !val.is_tombstone() || !is_bottom_level {
                current_builder.as_mut().unwrap().add(key.as_ref(), &val, lsn)?;
            }

            if current_builder.as_ref().unwrap().estimated_size() > TARGET_SST_SIZE as u64 {
                let builder = current_builder.take().unwrap();
                builder.finish()?;
                // Open reader for the new SSTable
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
        // 1. Update Version
        loop {
            let curr = version_arc.load();
            
            // We must preserve any SSTables that were added by concurrent flushes
            // while this compaction was running.
            let mut final_sstables = remaining_sstables.clone();
            for sst in &curr.sstables {
                // If this sst was NOT part of the compaction input, and it's NOT
                // one of our new L1 outputs (which are already in remaining_sstables),
                // then it must be a concurrent flush.
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
        let mut manifest_guard = manifest_arc.lock();
        for id in l0_ids.iter().chain(l1_ids.iter()) {
            manifest_guard.log_event(ManifestEvent::RemoveTable { level: 0, id: *id })?;
            manifest_guard.log_event(ManifestEvent::RemoveTable { level: 1, id: *id })?;
        }
        for id in new_l1_ids {
            manifest_guard.log_event(ManifestEvent::AddTable { level: 1, id })?;
        }
        
        // --- MANIFEST HARDENING ---
        // Collapse the manifest log to prevent unbounded growth (Manifest Compaction)
        if let Err(e) = manifest_guard.checkpoint() {
            eprintln!("Warning: Manifest checkpoint failed: {e:?}");
        }
        
        drop(manifest_guard);

        // 3. Unlink old files (concurrent reads still work because of Arc)
        for id in l0_ids.iter().chain(l1_ids.iter()) {
            let path = data_dir.join(format!("{id:06}.sst"));
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }
}
