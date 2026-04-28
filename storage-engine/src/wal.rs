use crate::error::{ApexError, Result};
use crate::memtable::EntryValue;
use bytes::Bytes;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

/// Represents a single write operation in the Write-Ahead Log.
#[derive(Debug, PartialEq)]
pub struct WalRecord {
    pub lsn: u64,
    pub key: Bytes,
    pub value: EntryValue,
}

/// A batched Write-Ahead Log writer.
pub struct WalWriter {
    file: File,
    writer: BufWriter<File>,
    current_lsn: u64,
}

impl WalWriter {
    /// Opens or creates a WAL file at the given path.
    pub fn open<P: AsRef<Path>>(path: P, start_lsn: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        // Need to clone the file handle for the BufWriter to allow sync_all on the original handle,
        // or we can use BufWriter::into_inner / get_mut.
        // Actually, BufWriter has a get_ref() to access the underlying file.
        let file_clone = file.try_clone()?;

        Ok(Self {
            file: file_clone,
            writer: BufWriter::new(file),
            current_lsn: start_lsn,
        })
    }

    /// Appends a new key-value pair to the log buffer.
    /// Does NOT fsync immediately. Call `sync()` to ensure durability.
    /// Returns the total number of bytes written (for metrics).
    pub fn append(&mut self, lsn: u64, key: &[u8], value: &EntryValue) -> Result<u64> {
        self.current_lsn = lsn + 1;

        let mut record_bytes = Vec::new();

        // Serialize the record
        // [LSN (8)] [KeyLen (4)] [Key] [IsTombstone (1)] [ValLen (4)] [Value]
        record_bytes.extend_from_slice(&lsn.to_le_bytes());
        record_bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
        record_bytes.extend_from_slice(key);

        match value {
            EntryValue::Value(v) => {
                record_bytes.push(0); // 0 = Value
                record_bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
                record_bytes.extend_from_slice(v);
            }
            EntryValue::Tombstone => {
                record_bytes.push(1); // 1 = Tombstone
                record_bytes.extend_from_slice(&(0u32).to_le_bytes()); // ValLen = 0
            }
        }

        // Calculate CRC32 checksum over the record bytes
        let mut hasher = Hasher::new();
        hasher.update(&record_bytes);
        let checksum = hasher.finalize();

        // Write [Checksum (4)] [Record]
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.write_all(&record_bytes)?;

        // 4 bytes checksum + record payload
        let total_bytes = 4 + record_bytes.len() as u64;
        Ok(total_bytes)
    }

    /// Flushes the user-space buffer and issues an fsync to the OS.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.file.sync_data()?;
        Ok(())
    }
}

/// A reader to recover state from a WAL file during crash recovery.
pub struct WalReader {
    reader: std::io::BufReader<File>,
}

impl WalReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: std::io::BufReader::new(file),
        })
    }

    /// Reads the next record from the WAL. Returns Ok(None) at EOF.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        let mut checksum_buf = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut checksum_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e.into());
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        let mut lsn_buf = [0u8; 8];
        self.reader.read_exact(&mut lsn_buf)?;
        let lsn = u64::from_le_bytes(lsn_buf);

        let mut key_len_buf = [0u8; 4];
        self.reader.read_exact(&mut key_len_buf)?;
        let key_len = u32::from_le_bytes(key_len_buf) as usize;

        let mut key_buf = vec![0u8; key_len];
        self.reader.read_exact(&mut key_buf)?;

        let mut is_tombstone_buf = [0u8; 1];
        self.reader.read_exact(&mut is_tombstone_buf)?;

        let is_tombstone = is_tombstone_buf[0] == 1;

        let mut val_len_buf = [0u8; 4];
        self.reader.read_exact(&mut val_len_buf)?;
        let val_len = u32::from_le_bytes(val_len_buf) as usize;

        let value = if is_tombstone {
            EntryValue::Tombstone
        } else {
            let mut val_buf = vec![0u8; val_len];
            self.reader.read_exact(&mut val_buf)?;
            EntryValue::Value(Bytes::from(val_buf))
        };

        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&lsn_buf);
        hasher.update(&key_len_buf);
        hasher.update(&key_buf);
        hasher.update(&is_tombstone_buf);
        hasher.update(&val_len_buf);
        if let EntryValue::Value(ref v) = value {
            hasher.update(v);
        }

        let actual_checksum = hasher.finalize();
        if actual_checksum != expected_checksum {
            return Err(ApexError::ChecksumMismatch {
                expected: expected_checksum,
                found: actual_checksum,
            });
        }

        Ok(Some(WalRecord {
            lsn,
            key: Bytes::from(key_buf),
            value,
        }))
    }
}
