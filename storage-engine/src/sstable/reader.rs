use crate::error::{ApexError, Result};
use crate::memtable::EntryValue;
use bloomfilter::Bloom;
use bytes::Bytes;
use moka::sync::Cache;

use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::sync::Arc;

const MAGIC_NUMBER: u64 = 0xA9E8_D8B1_7F1A_2B3C;

#[derive(Clone)]
pub struct Block {
    pub data: Bytes,
}

/// A reader for an on-disk SSTable file.
///
/// The file handle is wrapped in an `Arc` so that `get()` can take `&self`
/// enabling concurrent reads across threads. The `moka` block cache is already thread-safe.
#[derive(Clone)]
pub struct SSTableReader {
    pub id: u64,
    pub(crate) table_cache: super::cache::TableCache,
    pub(crate) sparse_index: Vec<(Bytes, u64)>,
    pub(crate) bloom_filter: Arc<Bloom<[u8]>>,
    pub(crate) block_cache: Cache<(u64, u64), Arc<Block>>,
    /// The byte offset where the sparse index begins in the file.
    /// Used to determine the size of the last data block.
    pub(crate) index_offset: u64,
}

impl SSTableReader {
    pub fn open(
        id: u64,
        table_cache: super::cache::TableCache,
        block_cache: Cache<(u64, u64), Arc<Block>>,
    ) -> Result<Self> {
        let mut file = table_cache.get_file(id)?;

        // Read Footer (24 bytes at the end)
        file.seek(SeekFrom::End(-24))?;
        let mut footer_buf = [0u8; 24];
        file.read_exact(&mut footer_buf)?;

        let index_offset = u64::from_le_bytes(footer_buf[0..8].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer_buf[8..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer_buf[16..24].try_into().unwrap());

        if magic != MAGIC_NUMBER {
            return Err(ApexError::Corruption(
                "Invalid magic number in SSTable footer".to_string(),
            ));
        }

        // Read Index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut num_entries_buf = [0u8; 4];
        file.read_exact(&mut num_entries_buf)?;
        let num_entries = u32::from_le_bytes(num_entries_buf);

        let mut sparse_index = Vec::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let mut key_len_buf = [0u8; 4];
            file.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut key_buf = vec![0u8; key_len];
            file.read_exact(&mut key_buf)?;

            let mut offset_buf = [0u8; 8];
            file.read_exact(&mut offset_buf)?;
            let offset = u64::from_le_bytes(offset_buf);

            sparse_index.push((Bytes::from(key_buf), offset));
        }

        // Read Bloom Filter
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut sip_buf = [0u8; 32];
        file.read_exact(&mut sip_buf)?;
        let k1_0 = u64::from_le_bytes(sip_buf[0..8].try_into().unwrap());
        let k1_1 = u64::from_le_bytes(sip_buf[8..16].try_into().unwrap());
        let k2_0 = u64::from_le_bytes(sip_buf[16..24].try_into().unwrap());
        let k2_1 = u64::from_le_bytes(sip_buf[24..32].try_into().unwrap());

        let mut bits_buf = [0u8; 8];
        file.read_exact(&mut bits_buf)?;
        let num_bits = u64::from_le_bytes(bits_buf);

        let mut hashes_buf = [0u8; 4];
        file.read_exact(&mut hashes_buf)?;
        let num_hashes = u32::from_le_bytes(hashes_buf);

        let mut bitmap_len_buf = [0u8; 4];
        file.read_exact(&mut bitmap_len_buf)?;
        let bitmap_len = u32::from_le_bytes(bitmap_len_buf) as usize;

        let mut bitmap = vec![0u8; bitmap_len];
        file.read_exact(&mut bitmap)?;

        let bloom_filter =
            Bloom::from_existing(&bitmap, num_bits, num_hashes, [(k1_0, k1_1), (k2_0, k2_1)]);

        Ok(Self {
            id,
            table_cache,
            sparse_index,
            bloom_filter: Arc::new(bloom_filter),
            block_cache,
            index_offset,
        })
    }

    /// Point lookup for a key. Returns `None` if definitely not present.
    ///
    /// Takes `&self` (not `&mut self`) so the engine can serve concurrent reads
    /// while holding only a shared `RwLock::read` guard on the version set.
    /// File I/O is serialized through an internal `Mutex<File>`, but the hot
    /// path hits the `moka` block cache and never touches the mutex at all.
    pub fn get(&self, key: &[u8]) -> Result<Option<(EntryValue, u64)>> {
        // 1. Bloom Filter Check — zero I/O, zero locking
        if !self.bloom_filter.check(key) {
            return Ok(None);
        }

        // 2. Sparse Index Binary Search — pure in-memory
        let block_idx = match self
            .sparse_index
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
        {
            Ok(idx) => idx,
            Err(0) => return Ok(None),
            Err(idx) => idx - 1,
        };

        let block_offset = self.sparse_index[block_idx].1;

        // Determine size of the block to read
        let next_offset = if block_idx + 1 < self.sparse_index.len() {
            self.sparse_index[block_idx + 1].1
        } else {
            self.index_offset
        };

        // 3. Block Cache lookup — thread-safe, no file mutex needed
        let cache_key = (self.id, block_offset);
        let block = if let Some(b) = self.block_cache.get(&cache_key) {
            b
        } else {
            // Cache miss: lock-free read from disk using pread
            let size = (next_offset - block_offset) as usize;
            let mut block_data = vec![0u8; size];
            self.table_cache.get_file(self.id)?.read_exact_at(&mut block_data, block_offset)?;
 
            let b = Arc::new(Block {
                data: Bytes::from(block_data),
            });
            self.block_cache.insert(cache_key, Arc::clone(&b));
            b
        };

        Ok(Self::search_block(&block, key))
    }

    /// Fetches a block by its index in the sparse index.
    pub(crate) fn get_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.sparse_index.len() {
            return Err(ApexError::Corruption("Block index out of bounds".into()));
        }
        
        let block_offset = self.sparse_index[block_idx].1;
        let next_offset = if block_idx + 1 < self.sparse_index.len() {
            self.sparse_index[block_idx + 1].1
        } else {
            self.index_offset
        };

        let cache_key = (self.id, block_offset);
        if let Some(b) = self.block_cache.get(&cache_key) {
            Ok(b)
        } else {
            let size = (next_offset - block_offset) as usize;
            let mut block_data = vec![0u8; size];
            self.table_cache.get_file(self.id)?.read_exact_at(&mut block_data, block_offset)?;

            let b = Arc::new(Block {
                data: Bytes::from(block_data),
            });
            self.block_cache.insert(cache_key, Arc::clone(&b));
            Ok(b)
        }
    }

    /// Linear scan within a single data block for the target key.
    fn search_block(block: &Block, key: &[u8]) -> Option<(EntryValue, u64)> {
        let mut cursor = 0;
        let data = &block.data;

        while cursor < data.len() {
            if cursor + 4 > data.len() {
                break;
            }

            let key_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            if cursor + key_len > data.len() {
                break;
            }
            let entry_key = &data[cursor..cursor + key_len];
            cursor += key_len;

            if cursor + 8 > data.len() {
                break;
            }
            let lsn = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;

            if cursor + 1 > data.len() {
                break;
            }
            let is_tombstone = data[cursor] == 1;
            cursor += 1;

            if cursor + 4 > data.len() {
                break;
            }
            let val_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            let entry_val = if is_tombstone {
                EntryValue::Tombstone
            } else {
                if cursor + val_len > data.len() {
                    break;
                }
                let v = &data[cursor..cursor + val_len];
                cursor += val_len;
                EntryValue::Value(Bytes::copy_from_slice(v))
            };

            match entry_key.cmp(key) {
                std::cmp::Ordering::Equal => return Some((entry_val, lsn)),
                std::cmp::Ordering::Greater => return None,
                std::cmp::Ordering::Less => {}
            }
        }

        None
    }
}
