use crate::error::Result;
use crate::memtable::EntryValue;
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// The standard Iterator interface for the engine.
/// We yield owned `Bytes` and `EntryValue` to avoid complex lifetime constraints.
/// Since `Bytes` is an Arc-backed buffer, cloning it is zero-copy (just an atomic increment).
pub trait DbIterator: Send {
    /// Advance to the next element. Returns true if the new position is valid.
    fn next(&mut self) -> Result<bool>;
    
    /// Returns the current key. Panics if !is_valid().
    fn key(&self) -> Bytes;
    
    /// Returns the current value. Panics if !is_valid().
    fn value(&self) -> EntryValue;
    
    /// Returns the current LSN. Panics if !is_valid().
    fn lsn(&self) -> u64;
    
    /// Returns true if the iterator is currently pointing to a valid element.
    fn is_valid(&self) -> bool;
}

// ---------------------------------------------------------------------------
// MemTable Iterator
// ---------------------------------------------------------------------------
use crate::memtable::MemTable;
use std::sync::Arc;
pub struct MemTableIterator {
    memtable: Arc<MemTable>,
    current: Option<(Bytes, EntryValue, u64)>,
}

impl MemTableIterator {
    pub fn new(memtable: Arc<MemTable>) -> Self {
        let mut it = Self {
            memtable,
            current: None,
        };
        // Initialize by getting the first element
        if let Some(entry) = it.memtable.map.front() {
            let (val, lsn) = entry.value();
            it.current = Some((entry.key().clone(), val.clone(), *lsn));
        }
        it
    }
}

impl DbIterator for MemTableIterator {
    fn next(&mut self) -> Result<bool> {
        let next_entry = if let Some((current_key, _, _)) = &self.current {
            self.memtable.map.lower_bound(std::ops::Bound::Excluded(current_key))
        } else {
            None
        };

        if let Some(entry) = next_entry {
            let (val, lsn) = entry.value();
            self.current = Some((entry.key().clone(), val.clone(), *lsn));
            Ok(true)
        } else {
            self.current = None;
            Ok(false)
        }
    }

    fn key(&self) -> Bytes {
        self.current.as_ref().unwrap().0.clone()
    }

    fn value(&self) -> EntryValue {
        self.current.as_ref().unwrap().1.clone()
    }

    fn lsn(&self) -> u64 {
        self.current.as_ref().unwrap().2
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }
}

// ---------------------------------------------------------------------------
// Merging Iterator
// ---------------------------------------------------------------------------

struct HeapElement {
    // We store the index into the `iterators` array
    iter_idx: usize,
    // Cached key for fast comparison
    key: Bytes,
    // Cached LSN for conflict resolution
    lsn: u64,
}

// We want a MIN-HEAP based on `key`. If keys are equal, HIGHEST `lsn` wins.
// BinaryHeap is a max-heap, so we reverse the ordering.
impl Ord for HeapElement {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.key.cmp(&self.key) {
            Ordering::Equal => self.lsn.cmp(&other.lsn), // Highest LSN wins (max heap for LSN)
            other_cmp => other_cmp, // Reverse for key (min heap for key)
        }
    }
}

impl PartialOrd for HeapElement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapElement {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.lsn == other.lsn
    }
}

impl Eq for HeapElement {}

pub struct MergingIterator {
    iterators: Vec<Box<dyn DbIterator>>,
    heap: BinaryHeap<HeapElement>,
    current_idx: Option<usize>,
}

impl MergingIterator {
    pub fn new(mut iterators: Vec<Box<dyn DbIterator>>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        
        for (i, iter) in iterators.iter_mut().enumerate() {
            if iter.is_valid() {
                heap.push(HeapElement {
                    iter_idx: i,
                    key: iter.key(),
                    lsn: iter.lsn(),
                });
            }
        }

        let mut it = Self {
            iterators,
            heap,
            current_idx: None,
        };
        it.advance()?;
        Ok(it)
    }

    fn advance(&mut self) -> Result<()> {
        // If we have a current_idx, we need to advance that iterator and push it back to the heap
        if let Some(idx) = self.current_idx.take() {
            let iter = &mut self.iterators[idx];
            if iter.next()? {
                self.heap.push(HeapElement {
                    iter_idx: idx,
                    key: iter.key(),
                    lsn: iter.lsn(),
                });
            }
        }

        // Pop the minimum element
        if let Some(top) = self.heap.pop() {
            self.current_idx = Some(top.iter_idx);
            
            // CONFLICT RESOLUTION:
            // Since we popped a key, we must discard any other iterators in the heap
            // that have the exact same key (which will have lower LSNs due to heap ordering).
            while let Some(peek) = self.heap.peek() {
                if peek.key == top.key {
                    // It's a duplicate key! Pop it, advance its iterator, and re-push.
                    let dup = self.heap.pop().unwrap();
                    let dup_iter = &mut self.iterators[dup.iter_idx];
                    if dup_iter.next()? {
                        self.heap.push(HeapElement {
                            iter_idx: dup.iter_idx,
                            key: dup_iter.key(),
                            lsn: dup_iter.lsn(),
                        });
                    }
                } else {
                    break; // No more duplicates
                }
            }
        }

        Ok(())
    }
}

impl DbIterator for MergingIterator {
    fn next(&mut self) -> Result<bool> {
        self.advance()?;
        Ok(self.is_valid())
    }

    fn key(&self) -> Bytes {
        self.iterators[self.current_idx.unwrap()].key()
    }

    fn value(&self) -> EntryValue {
        self.iterators[self.current_idx.unwrap()].value()
    }

    fn lsn(&self) -> u64 {
        self.iterators[self.current_idx.unwrap()].lsn()
    }

    fn is_valid(&self) -> bool {
        self.current_idx.is_some()
    }
}
