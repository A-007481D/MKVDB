use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryValue {
    Value(Bytes),
    Tombstone,
}

impl EntryValue {
    /// Returns true if this entry is a tombstone.
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        matches!(self, EntryValue::Tombstone)
    }

    /// Returns the underlying byte slice if it's a value, or empty if tombstone.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            EntryValue::Value(v) => v.as_ref(),
            EntryValue::Tombstone => &[],
        }
    }
}

/// A highly concurrent, lock-free Memory Table based on a SkipList.
pub struct MemTable {
    map: SkipMap<Bytes, EntryValue>,
    approximate_size: AtomicUsize,
    /// The lowest sequence number contained in this memtable.
    first_seq: std::sync::atomic::AtomicU64,
    /// The highest sequence number contained in this memtable.
    last_seq: std::sync::atomic::AtomicU64,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            approximate_size: AtomicUsize::new(0),
            first_seq: std::sync::atomic::AtomicU64::new(u64::MAX),
            last_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Inserts a value into the memtable. Updates the approximate size.
    pub fn put(&self, key: Bytes, value: EntryValue, seq: u64) {
        let key_len = key.len();
        let val_len = match &value {
            EntryValue::Value(v) => v.len(),
            EntryValue::Tombstone => 0,
        };

        // If the key already exists, we are replacing it. The size calculation
        // is approximate, so we just add the new sizes. For strict limits, we might
        // want to subtract the old size if we can efficiently find it.
        // For LSM memtables, usually we just accumulate and flush when total written hits a threshold.
        let added_size = key_len + val_len;

        self.map.insert(key, value);
        self.approximate_size
            .fetch_add(added_size, Ordering::Relaxed);

        // Update sequence numbers (relaxed is fine as we serialize the actual seq assignment in the WAL)
        let _ = self.first_seq.fetch_min(seq, Ordering::Relaxed);
        let _ = self.last_seq.fetch_max(seq, Ordering::Relaxed);
    }

    /// Retrieves a value by key.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<EntryValue> {
        self.map.get(key).map(|e| e.value().clone())
    }

    /// Returns the approximate memory footprint in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Gets the sequence number range for this memtable (min, max).
    #[must_use]
    pub fn seq_range(&self) -> (u64, u64) {
        (
            self.first_seq.load(Ordering::Relaxed),
            self.last_seq.load(Ordering::Relaxed),
        )
    }

    /// Returns an iterator over the entries in the memtable.
    pub fn iter(&self) -> crossbeam_skiplist::map::Iter<'_, Bytes, EntryValue> {
        self.map.iter()
    }
}

/// A queue holding MemTables that are full and waiting to be flushed to disk.
pub struct ImmutableMemTables {
    queue: parking_lot::RwLock<Vec<Arc<MemTable>>>,
}

impl Default for ImmutableMemTables {
    fn default() -> Self {
        Self::new()
    }
}

impl ImmutableMemTables {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queue: parking_lot::RwLock::new(Vec::new()),
        }
    }

    pub fn push(&self, table: Arc<MemTable>) {
        self.queue.write().push(table);
    }

    /// Searches for a key in the immutable memtables, from newest to oldest.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<EntryValue> {
        let guard = self.queue.read();
        // Search in reverse order (newest first)
        for table in guard.iter().rev() {
            if let Some(val) = table.get(key) {
                return Some(val);
            }
        }
        None
    }

    /// Removes a memtable from the queue once it has been successfully flushed.
    pub fn remove_flushed(&self, table_id: Arc<MemTable>) {
        let mut guard = self.queue.write();
        if let Some(idx) = guard.iter().position(|t| Arc::ptr_eq(t, &table_id)) {
            guard.remove(idx);
        }
    }

    /// Returns a copy of the current queue for a flush thread to process.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Arc<MemTable>> {
        self.queue.read().clone()
    }
}
