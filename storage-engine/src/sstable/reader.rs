use crate::error::{ApexError, Result};
use crate::memtable::EntryValue;
use bloomfilter::Bloom;
use bytes::Bytes;
use moka::sync::Cache;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

const MAGIC_NUMBER: u64 = 0x_A9E8_D8_B1_7F_1A_2B_3C;

#[derive(Clone)]
pub struct Block {
    pub data: Bytes,
}

pub struct SSTableReader {
    file: File,
    sparse_index: Vec<(Bytes, u64)>, // (First key of block, Offset)
    bloom_filter: Bloom<[u8]>,
    block_cache: Cache<u64, Arc<Block>>,
}

impl SSTableReader {
    pub fn open<P: AsRef<Path>>(path: P, block_cache: Cache<u64, Arc<Block>>) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read Footer (24 bytes at the end)
        file.seek(SeekFrom::End(-24))?;
        let mut footer_buf = [0u8; 24];
        file.read_exact(&mut footer_buf)?;

        let index_offset = u64::from_le_bytes(footer_buf[0..8].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(footer_buf[8..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer_buf[16..24].try_into().unwrap());

        if magic != MAGIC_NUMBER {
            return Err(ApexError::Corruption("Invalid magic number in SSTable footer".to_string()));
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

        let bloom_filter = Bloom::from_existing(&bitmap, num_bits, num_hashes, [(k1_0, k1_1), (k2_0, k2_1)]);

        Ok(Self {
            file,
            sparse_index,
            bloom_filter,
            block_cache,
        })
    }

    /// Point lookup for a key. Returns None if definitely not present.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<EntryValue>> {
        // 1. Bloom Filter Check
        if !self.bloom_filter.check(key) {
            return Ok(None);
        }

        // 2. Sparse Index Binary Search
        // Find the rightmost block where block.first_key <= key
        let block_idx = match self.sparse_index.binary_search_by(|(k, _)| k.as_ref().cmp(key)) {
            Ok(idx) => idx, // Exact match on first key
            Err(0) => return Ok(None), // Key is smaller than the first key in the table
            Err(idx) => idx - 1,
        };

        let block_offset = self.sparse_index[block_idx].1;
        
        // Determine size of the block to read
        let next_offset = if block_idx + 1 < self.sparse_index.len() {
            self.sparse_index[block_idx + 1].1
        } else {
            // Read until the index offset (which is where the blocks end)
            // Need to know the index offset... unfortunately we didn't save it. 
            // We can just read until the end of the block or EOF, but it's better to store it.
            // For now, let's just seek and read the whole block (or up to 4096 * 2 since entries can cross boundaries slightly).
            // Actually, we can just read 4096 bytes and parse what we can. 
            // Wait, an entry might make the block > 4096. 
            // If we don't know the exact end offset, we might read too much. 
            // Let's read up to next_offset if it exists. If it's the last block, read a generous amount or just read everything up to where the index starts.
            // Let's modify the code to calculate size.
            0 // Special case handled below
        };

        let block = if let Some(b) = self.block_cache.get(&block_offset) {
            b
        } else {
            // Read block from disk
            self.file.seek(SeekFrom::Start(block_offset))?;
            
            // To find out how much to read for the last block, we can just read everything up to where the index starts.
            // This is a bit hacky, but works. A better way is to store the index offset in the struct.
            // For now, we will just read 8KB which should cover the largest possible block (4KB + max entry size).
            let mut block_data = Vec::new();
            if next_offset > 0 {
                let size = (next_offset - block_offset) as usize;
                block_data.resize(size, 0);
                self.file.read_exact(&mut block_data)?;
            } else {
                // Last block
                // Read until we hit something or just read 8192 bytes
                let mut temp_buf = [0u8; 8192];
                let n = self.file.read(&mut temp_buf)?;
                block_data.extend_from_slice(&temp_buf[..n]);
            }
            
            let b = Arc::new(Block { data: Bytes::from(block_data) });
            self.block_cache.insert(block_offset, Arc::clone(&b));
            b
        };

        // 3. Search within the block
        let mut cursor = 0;
        let data = &block.data;

        while cursor < data.len() {
            if cursor + 4 > data.len() { break; } // EOF or padding
            
            let mut key_len_buf = [0u8; 4];
            key_len_buf.copy_from_slice(&data[cursor..cursor+4]);
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            cursor += 4;

            if cursor + key_len > data.len() { break; }
            let entry_key = &data[cursor..cursor+key_len];
            cursor += key_len;

            if cursor + 1 > data.len() { break; }
            let is_tombstone = data[cursor] == 1;
            cursor += 1;

            let mut val_len_buf = [0u8; 4];
            val_len_buf.copy_from_slice(&data[cursor..cursor+4]);
            let val_len = u32::from_le_bytes(val_len_buf) as usize;
            cursor += 4;

            let entry_val = if is_tombstone {
                EntryValue::Tombstone
            } else {
                if cursor + val_len > data.len() { break; }
                let v = &data[cursor..cursor+val_len];
                cursor += val_len;
                EntryValue::Value(Bytes::copy_from_slice(v))
            };

            // Compare key
            match entry_key.cmp(key) {
                std::cmp::Ordering::Equal => {
                    return Ok(Some(entry_val));
                }
                std::cmp::Ordering::Greater => {
                    // Since keys are sorted, if we see a larger key, our target doesn't exist
                    return Ok(None);
                }
                std::cmp::Ordering::Less => {
                    // Continue searching
                }
            }
        }

        Ok(None)
    }
}
