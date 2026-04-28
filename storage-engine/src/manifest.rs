use crate::error::Result;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Represents a version-changing event in the database.
///
/// The MANIFEST only records structural changes to the version set (SSTable
/// additions and removals). It does **not** track per-write sequence numbers —
/// that responsibility belongs to the WAL, which is the authoritative source
/// for LSN recovery.
#[derive(Debug, Clone)]
pub enum ManifestEvent {
    /// A new SSTable has been successfully flushed or produced by compaction.
    AddTable { level: u8, id: u64 },
    /// An SSTable has been removed (consumed by compaction or manual deletion).
    RemoveTable { level: u8, id: u64 },
    /// Persist the current WAL file id so recovery knows which WALs to replay.
    SetWalId { wal_id: u64 },
}

/// The Version Set (Source of Truth).
///
/// Tracks active SSTables and the file-id watermark. Sequence numbers are
/// recovered from the WAL during startup, not from the MANIFEST.
pub struct Manifest {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,

    // In-memory state
    pub levels: HashMap<u8, HashSet<u64>>,
    pub next_file_id: u64,
    /// The WAL file id that was active when the last flush completed.
    /// During recovery, any WAL with id >= this value must be replayed.
    pub wal_id: u64,
}

impl Manifest {
    /// Opens an existing MANIFEST file or creates a new one, recovering state.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let mut levels: HashMap<u8, HashSet<u64>> = HashMap::new();
        let mut next_file_id = 1;
        let mut wal_id = 0;

        // Recover state by replaying the log
        let file_for_read = File::open(&path)?;
        let reader = BufReader::new(file_for_read);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();

            match parts[0] {
                "ADD" if parts.len() == 3 => {
                    let level: u8 = parts[1].parse().unwrap_or(0);
                    let id: u64 = parts[2].parse().unwrap_or(0);
                    levels.entry(level).or_default().insert(id);
                    next_file_id = next_file_id.max(id + 1);
                }
                "RM" if parts.len() == 3 => {
                    let level: u8 = parts[1].parse().unwrap_or(0);
                    let id: u64 = parts[2].parse().unwrap_or(0);
                    if let Some(set) = levels.get_mut(&level) {
                        set.remove(&id);
                    }
                }
                "WAL" if parts.len() == 2 => {
                    wal_id = parts[1].parse().unwrap_or(wal_id);
                    next_file_id = next_file_id.max(wal_id + 1);
                }
                _ => {}
            }
        }

        Ok(Self {
            file,
            path,
            levels,
            next_file_id,
            wal_id,
        })
    }

    /// Logs an event to the MANIFEST and fsyncs it for crash consistency.
    pub fn log_event(&mut self, event: ManifestEvent) -> Result<()> {
        // 1. Mirror into in-memory state
        match event {
            ManifestEvent::AddTable { level, id } => {
                self.levels.entry(level).or_default().insert(id);
                self.next_file_id = self.next_file_id.max(id + 1);
            }
            ManifestEvent::RemoveTable { level, id } => {
                if let Some(set) = self.levels.get_mut(&level) {
                    set.remove(&id);
                }
            }
            ManifestEvent::SetWalId { wal_id } => {
                self.wal_id = wal_id;
                self.next_file_id = self.next_file_id.max(wal_id + 1);
            }
        }

        // 2. Persist atomically by rewriting the manifest
        self.checkpoint()
    }

    /// Rewrites the MANIFEST into a clean, collapsed state.
    /// 
    /// This uses the atomic swap pattern:
    /// 1. Write the current state to a temporary file.
    /// 2. Call fsync on the temp file.
    /// 3. Atomically rename the temp file to the target MANIFEST.
    /// 
    /// This ensures that even a crash during checkpointing leaves us with 
    /// either the old full history or the new collapsed state.
    pub fn checkpoint(&mut self) -> Result<()> {
        let temp_path = self.path.with_extension("tmp");
        {
            let mut temp_file = File::create(&temp_path)?;
            
            // Write WAL ID
            temp_file.write_all(format!("WAL,{}\n", self.wal_id).as_bytes())?;
            
            // Write all active tables
            for (level, sst_ids) in &self.levels {
                for &id in sst_ids {
                    temp_file.write_all(format!("ADD,{},{}\n", level, id).as_bytes())?;
                }
            }
            
            temp_file.sync_all()?;
        }

        // Atomic swap
        std::fs::rename(&temp_path, &self.path)?;
        
        // Re-open the file handle in append mode
        self.file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&self.path)?;
            
        Ok(())
    }

    /// Generates a new unique file ID for SSTables or WAL files.
    pub fn generate_file_id(&mut self) -> u64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }
}
