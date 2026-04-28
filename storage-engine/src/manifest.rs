use crate::error::{ApexError, Result};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Represents an event that modifies the database's version set.
#[derive(Debug, Clone)]
pub enum ManifestEvent {
    AddTable { level: u8, id: u64 },
    RemoveTable { level: u8, id: u64 },
    UpdateSeq { seq: u64 },
}

/// The Version Set (Source of Truth).
/// Tracks active SSTables and the current Log Sequence Number.
pub struct Manifest {
    file: File,
    path: PathBuf,
    
    // In-memory state
    pub levels: HashMap<u8, HashSet<u64>>,
    pub current_seq: u64,
    pub next_file_id: u64,
}

impl Manifest {
    /// Opens an existing MANIFEST file or creates a new one, recovering the state.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let mut levels = HashMap::new();
        let mut current_seq = 0;
        let mut next_file_id = 1;

        // Recover state
        let file_for_read = File::open(&path)?;
        let reader = BufReader::new(file_for_read);

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() { continue; }
            let parts: Vec<&str> = line.split(',').collect();
            
            match parts[0] {
                "ADD" => {
                    if parts.len() == 3 {
                        let level: u8 = parts[1].parse().unwrap_or(0);
                        let id: u64 = parts[2].parse().unwrap_or(0);
                        levels.entry(level).or_insert_with(HashSet::new).insert(id);
                        next_file_id = next_file_id.max(id + 1);
                    }
                }
                "RM" => {
                    if parts.len() == 3 {
                        let level: u8 = parts[1].parse().unwrap_or(0);
                        let id: u64 = parts[2].parse().unwrap_or(0);
                        if let Some(set) = levels.get_mut(&level) {
                            set.remove(&id);
                        }
                    }
                }
                "SEQ" => {
                    if parts.len() == 2 {
                        current_seq = parts[1].parse().unwrap_or(current_seq);
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            file,
            path,
            levels,
            current_seq,
            next_file_id,
        })
    }

    /// Logs an event to the MANIFEST and fsyncs it for crash consistency.
    pub fn log_event(&mut self, event: ManifestEvent) -> Result<()> {
        let line = match &event {
            ManifestEvent::AddTable { level, id } => format!("ADD,{},{}\n", level, id),
            ManifestEvent::RemoveTable { level, id } => format!("RM,{},{}\n", level, id),
            ManifestEvent::UpdateSeq { seq } => format!("SEQ,{}\n", seq),
        };

        self.file.write_all(line.as_bytes())?;
        self.file.sync_all()?;

        // Update in-memory state
        match event {
            ManifestEvent::AddTable { level, id } => {
                self.levels.entry(level).or_insert_with(HashSet::new).insert(id);
                self.next_file_id = self.next_file_id.max(id + 1);
            }
            ManifestEvent::RemoveTable { level, id } => {
                if let Some(set) = self.levels.get_mut(&level) {
                    set.remove(&id);
                }
            }
            ManifestEvent::UpdateSeq { seq } => {
                self.current_seq = seq;
            }
        }

        Ok(())
    }

    /// Generates a new unique file ID for SSTables or WAL files.
    pub fn generate_file_id(&mut self) -> u64 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }
}
