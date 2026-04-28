use crate::error::Result;
use crate::iterator::DbIterator;
use crate::memtable::EntryValue;
use crate::sstable::reader::{Block, SSTableReader};
use bytes::Bytes;
use std::sync::Arc;

pub struct SSTableIterator {
    reader: Arc<SSTableReader>,
    block_idx: usize,
    cursor: usize,
    current_block: Option<Arc<Block>>,
    current_entry: Option<(Bytes, EntryValue, u64)>,
}

impl SSTableIterator {
    pub fn new(reader: Arc<SSTableReader>) -> Result<Self> {
        let mut it = Self {
            reader,
            block_idx: 0,
            cursor: 0,
            current_block: None,
            current_entry: None,
        };
        it.load_block()?;
        it.next()?; // load first entry
        Ok(it)
    }

    fn load_block(&mut self) -> Result<bool> {
        if self.block_idx < self.reader.sparse_index.len() {
            self.current_block = Some(self.reader.get_block(self.block_idx)?);
            self.cursor = 0;
            Ok(true)
        } else {
            self.current_block = None;
            Ok(false)
        }
    }
}

impl DbIterator for SSTableIterator {
    fn next(&mut self) -> Result<bool> {
        loop {
            if let Some(block) = &self.current_block {
                let data = &block.data;
                if self.cursor >= data.len() {
                    // Block exhausted, load next block
                    self.block_idx += 1;
                    if !self.load_block()? {
                        self.current_entry = None;
                        return Ok(false);
                    }
                    continue; // Loop around to read from the new block
                }

                let mut cursor = self.cursor;

                if cursor + 4 > data.len() {
                    break;
                }
                let key_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;

                if cursor + key_len > data.len() {
                    break;
                }
                let entry_key = Bytes::copy_from_slice(&data[cursor..cursor + key_len]);
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
                    let v = Bytes::copy_from_slice(&data[cursor..cursor + val_len]);
                    cursor += val_len;
                    EntryValue::Value(v)
                };

                self.cursor = cursor;
                self.current_entry = Some((entry_key, entry_val, lsn));
                return Ok(true);
            } else {
                self.current_entry = None;
                return Ok(false);
            }
        }
        
        self.current_entry = None;
        Ok(false)
    }

    fn key(&self) -> Bytes {
        self.current_entry.as_ref().unwrap().0.clone()
    }

    fn value(&self) -> EntryValue {
        self.current_entry.as_ref().unwrap().1.clone()
    }

    fn lsn(&self) -> u64 {
        self.current_entry.as_ref().unwrap().2
    }

    fn is_valid(&self) -> bool {
        self.current_entry.is_some()
    }
}
