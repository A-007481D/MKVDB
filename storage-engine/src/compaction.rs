use crate::engine::ApexEngine;
use crate::engine::Version;
use crate::error::Result;
use crate::iterator::{DbIterator, MergingIterator};
use crate::manifest::{Manifest, ManifestEvent};
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::iterator::SSTableIterator;
use crate::sstable::reader::{Block, SSTableReader};
use arc_swap::ArcSwap;
use moka::sync::Cache;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
        l0_threshold: usize,
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
                l0_threshold,
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
        l0_threshold: usize,
    ) -> Result<()> {
        let current_version = version_arc.load();
        // Pick L0 files from the CURRENT VERSION
        let l0_ids: Vec<u64> = current_version
            .levels
            .first()
            .map(|l0| l0.iter().map(|s| s.id).take(10).collect())
            .unwrap_or_default();

        if l0_ids.len() < l0_threshold {
            return Ok(());
        }

        // For simplicity in this version, we still get L1 IDs from manifest or just assume
        // we merge all L0s into a new set of L1s.
        // Real implementation would pick overlapping L1s.
        let l1_ids: Vec<u64> = {
            let manifest_guard = manifest_arc.lock();
            manifest_guard
                .levels
                .get(&1)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

        tracing::info!(
            "Starting L0->L1 compaction: merging {} L0 files and {} L1 files",
            l0_ids.len(),
            l1_ids.len()
        );

        let mut compact_readers = Vec::new();
        if let Some(l0) = current_version.levels.first() {
            for reader in l0 {
                if l0_ids.contains(&reader.id) {
                    compact_readers.push(Arc::clone(reader));
                }
            }
        }
        if let Some(l1) = current_version.levels.get(1) {
            for reader in l1 {
                if l1_ids.contains(&reader.id) {
                    compact_readers.push(Arc::clone(reader));
                }
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
        let mut new_readers_inner = Vec::new();
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
                current_builder
                    .as_mut()
                    .unwrap()
                    .add(key.as_ref(), &val, lsn)?;
            } else {
                tracing::debug!("Tombstone GC: dropping key {:?}", key);
            }

            if current_builder.as_ref().unwrap().estimated_size() > TARGET_SST_SIZE as u64 {
                let builder = current_builder.take().unwrap();
                builder.finish()?;
                let reader =
                    SSTableReader::open(current_id, table_cache.clone(), block_cache.clone())?;
                new_readers_inner.push(Arc::new(reader));
            }

            merging_iter.next()?;
        }

        if let Some(builder) = current_builder {
            if builder.estimated_size() > 0 {
                builder.finish()?;
                let reader =
                    SSTableReader::open(current_id, table_cache.clone(), block_cache.clone())?;
                new_readers_inner.push(Arc::new(reader));
            } else {
                // Remove empty file
                let _ = std::fs::remove_file(&current_path);
                new_l1_ids.pop();
            }
        }

        let new_readers = new_readers_inner;

        // 2. Update Version (CAS loop)
        loop {
            let curr = version_arc.load();
            let mut new_levels = curr.levels.clone();

            // 1. Remove processed L0 files
            if let Some(l0) = new_levels.get_mut(0) {
                l0.retain(|s| !l0_ids.contains(&s.id));
            }

            // 2. Replace L1 files (for now we merge all L1, so we just replace the whole level)
            if new_levels.len() < 2 {
                new_levels.push(Vec::new());
            }
            new_levels[1].clone_from(&new_readers);

            let new_version = Arc::new(Version {
                active_memtable: Arc::clone(&curr.active_memtable),
                levels: new_levels,
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

        tracing::info!(
            "L0->L1 compaction completed successfully. New L1 files: {:?}",
            new_l1_ids
        );
        Ok(())
    }
}
