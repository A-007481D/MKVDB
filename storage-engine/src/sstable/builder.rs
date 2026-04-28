use crate::error::Result;
use crate::memtable::EntryValue;
use bloomfilter::Bloom;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

const BLOCK_SIZE: usize = 4096;
const MAGIC_NUMBER: u64 = 0xA9E8_D8B1_7F1A_2B3C;

/// Builds an SSTable from a sequence of sorted key-value pairs.
pub struct SSTableBuilder {
    target_path: PathBuf,
    temp_path: PathBuf,
    writer: BufWriter<File>,
    current_block: Vec<u8>,
    sparse_index: Vec<(Bytes, u64)>, // (First key of block, Block offset)
    bloom_filter: Bloom<[u8]>,
    current_offset: u64,
    keys_written: usize,
}

impl SSTableBuilder {
    pub fn new<P: AsRef<Path>>(path: P, expected_keys: usize) -> Result<Self> {
        let target_path = path.as_ref().to_path_buf();
        let temp_path = target_path.with_extension("tmp");
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        let bloom = Bloom::new_for_fp_rate(expected_keys.max(100), 0.01);

        Ok(Self {
            target_path,
            temp_path,
            writer: BufWriter::new(file),
            current_block: Vec::with_capacity(BLOCK_SIZE),
            sparse_index: Vec::new(),
            bloom_filter: bloom,
            current_offset: 0,
            keys_written: 0,
        })
    }

    /// Adds a key-value pair to the SSTable.
    /// MUST be called with strictly monotonically increasing keys.
    pub fn add(&mut self, key: &[u8], value: &EntryValue, lsn: u64) -> Result<()> {
        let mut entry_bytes = Vec::new();

        // [KeyLen(4)][Key][LSN(8)][IsTombstone(1)][ValLen(4)][Value]
        entry_bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        entry_bytes.extend_from_slice(key);
        entry_bytes.extend_from_slice(&lsn.to_le_bytes());

        match value {
            EntryValue::Value(v) => {
                entry_bytes.push(0); // 0 = Value
                entry_bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
                entry_bytes.extend_from_slice(v);
            }
            EntryValue::Tombstone => {
                entry_bytes.push(1); // 1 = Tombstone
                entry_bytes.extend_from_slice(&(0u32).to_le_bytes());
            }
        }

        // Check if we need to flush the current block before adding the new entry
        // (Unless the block is empty, we don't want to flush an empty block just because a large entry arrives).
        if !self.current_block.is_empty()
            && self.current_block.len() + entry_bytes.len() > BLOCK_SIZE
        {
            self.flush_current_block()?;
        }

        if self.current_block.is_empty() {
            // This is the first key of a new block. Add it to the sparse index.
            self.sparse_index
                .push((Bytes::copy_from_slice(key), self.current_offset));
        }

        self.current_block.extend_from_slice(&entry_bytes);
        self.bloom_filter.set(key);
        self.keys_written += 1;

        Ok(())
    }

    /// Returns the approximate size of the data written so far.
    #[must_use]
    pub fn estimated_size(&self) -> u64 {
        self.current_offset + self.current_block.len() as u64
    }

    fn flush_current_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Compress the block
        let compressed_block = lz4_flex::compress_prepend_size(&self.current_block);

        // Calculate CRC32 checksum for the COMPRESSED block
        let checksum = crc32fast::hash(&compressed_block);

        self.writer.write_all(&compressed_block)?;
        self.writer.write_all(&checksum.to_le_bytes())?; // 4-byte checksum footer

        self.current_offset += (compressed_block.len() + 4) as u64;
        self.current_block.clear();
        Ok(())
    }

    /// Finalizes the SSTable by writing the index block, bloom filter block, and footer.
    pub fn finish(mut self) -> Result<()> {
        self.flush_current_block()?;

        let index_offset = self.current_offset;

        // Write Sparse Index
        // format: [NumEntries(4)] then for each entry: [KeyLen(4)][Key][BlockOffset(8)]
        let mut index_bytes = Vec::new();
        index_bytes.extend_from_slice(&(self.sparse_index.len() as u32).to_le_bytes());
        for (key, offset) in &self.sparse_index {
            index_bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            index_bytes.extend_from_slice(key);
            index_bytes.extend_from_slice(&offset.to_le_bytes());
        }
        self.writer.write_all(&index_bytes)?;
        self.current_offset += index_bytes.len() as u64;

        let bloom_offset = self.current_offset;

        // Write Bloom Filter (Using bincode or custom serialization)
        // bloomfilter crate has a sipkeys, bitmap layout. But maybe we can just serialize it.
        // bloomfilter crate provides a `bitmap()` and `sip_keys()`.
        // Let's implement a simple serialization.
        let sip_keys = self.bloom_filter.sip_keys();
        let bitmap = self.bloom_filter.bitmap();

        let mut bloom_bytes = Vec::new();
        bloom_bytes.extend_from_slice(&sip_keys[0].0.to_le_bytes());
        bloom_bytes.extend_from_slice(&sip_keys[0].1.to_le_bytes());
        bloom_bytes.extend_from_slice(&sip_keys[1].0.to_le_bytes());
        bloom_bytes.extend_from_slice(&sip_keys[1].1.to_le_bytes());

        bloom_bytes.extend_from_slice(&self.bloom_filter.number_of_bits().to_le_bytes());
        bloom_bytes.extend_from_slice(&self.bloom_filter.number_of_hash_functions().to_le_bytes());

        bloom_bytes.extend_from_slice(&(bitmap.len() as u32).to_le_bytes());
        bloom_bytes.extend_from_slice(&bitmap);

        self.writer.write_all(&bloom_bytes)?;

        // Write Footer
        // [Index Offset (8)] [Bloom Offset (8)] [Magic (8)]
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(&bloom_offset.to_le_bytes())?;
        self.writer.write_all(&MAGIC_NUMBER.to_le_bytes())?;

        self.writer.flush()?;
        // The file handle is flushed but we also want an fsync on the SSTable
        self.writer
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?
            .sync_all()?;

        // Atomic Rename: Only move the file to its final destination after it's fully synced.
        std::fs::rename(&self.temp_path, &self.target_path)?;

        // Sync parent directory to ensure the rename is durable
        if let Some(parent) = self.target_path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        
        Ok(())
    }
}
