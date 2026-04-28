use crate::error::Result;
use moka::sync::Cache;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Manages a pool of open file descriptors for SSTables.
/// 
/// This prevents "Too many open files" errors by evicting older
/// file handles when the limit is reached.
#[derive(Clone)]
pub struct TableCache {
    data_dir: PathBuf,
    cache: Cache<u64, Arc<File>>,
}

impl TableCache {
    pub fn new<P: AsRef<Path>>(data_dir: P, max_capacity: u64) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            cache: Cache::new(max_capacity),
        }
    }

    /// Gets an open file handle for the given SSTable ID.
    /// If not in cache, it opens the file and inserts it.
    pub fn get_file(&self, id: u64) -> Result<Arc<File>> {
        if let Some(file) = self.cache.get(&id) {
            return Ok(file);
        }

        // Cache miss, open the file
        let path = self.data_dir.join(format!("{id:06}.sst"));
        let file = File::open(path)?;
        let arc_file = Arc::new(file);
        
        self.cache.insert(id, Arc::clone(&arc_file));
        Ok(arc_file)
    }

    /// Explicitly remove a file from the cache (e.g., after compaction deletes it).
    pub fn evict(&self, id: u64) {
        self.cache.remove(&id);
    }
}
