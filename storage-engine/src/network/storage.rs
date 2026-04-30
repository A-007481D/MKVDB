use std::sync::Arc;
use std::ops::RangeBounds;
use std::path::Path;
use bytes::Bytes;
use openraft::{
    storage::{LogState, RaftLogReader, RaftStorage, Snapshot, RaftSnapshotBuilder},
    LogId, StorageError, StorageIOError, StoredMembership, Vote, OptionalSend,
    SnapshotMeta, BasicNode,
};
use crate::error::ApexError;
use crate::engine::ApexEngine;
use crate::network::grpc::ApexRaftTypeConfig;

#[derive(Clone)]
pub struct ApexRaftStorage {
    pub engine: Arc<ApexEngine>,
}

impl ApexRaftStorage {
    pub fn new(engine: Arc<ApexEngine>) -> Self {
        Self { engine }
    }

    fn log_key(index: u64) -> Vec<u8> {
        let mut key = b"log:".to_vec();
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    fn map_engine_error(e: ApexError) -> StorageError<u64> {
        let err = std::io::Error::other(e.to_string());
        StorageError::IO {
            source: StorageIOError::new(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                &err,
            ),
        }
    }
}

impl RaftLogReader<ApexRaftTypeConfig> for ApexRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<ApexRaftTypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&i) => i + 1,
            std::ops::Bound::Excluded(&i) => i,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut entries = Vec::new();
        for index in start..end {
            let key = Self::log_key(index);
            let val = self.engine.get(&key).map_err(Self::map_engine_error)?;
            if let Some(data) = val {
                let entry: openraft::Entry<ApexRaftTypeConfig> = bincode::deserialize(&data).map_err(|_| {
                    let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to deserialize log entry");
                    StorageError::IO {
                        source: StorageIOError::new(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Read, &err),
                    }
                })?;
                entries.push(entry);
            } else {
                break;
            }
        }
        Ok(entries)
    }
}

pub struct ApexSnapshotBuilder {
    engine: Arc<ApexEngine>,
}

impl RaftSnapshotBuilder<ApexRaftTypeConfig> for ApexSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<ApexRaftTypeConfig>, StorageError<u64>> {
        let checkpoint_name = format!("checkpoint_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
        let checkpoint_path = Path::new(&checkpoint_name);
        self.engine.create_checkpoint(checkpoint_path).map_err(ApexRaftStorage::map_engine_error)?;
        
        let last_applied_data = self.engine.get(b"meta:last_applied").map_err(ApexRaftStorage::map_engine_error)?;
        let last_applied: Option<LogId<u64>> = match last_applied_data {
            Some(d) => bincode::deserialize(&d).unwrap(),
            None => None,
        };

        let last_membership_data = self.engine.get(b"meta:last_membership").map_err(ApexRaftStorage::map_engine_error)?;
        let last_membership: StoredMembership<u64, BasicNode> = match last_membership_data {
            Some(d) => bincode::deserialize(&d).unwrap(),
            None => StoredMembership::default(),
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id: format!("snap-{}", last_applied.map_or(0, |id| id.index)),
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(std::io::Cursor::new(vec![])),
        })
    }
}

impl RaftStorage<ApexRaftTypeConfig> for ApexRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = ApexSnapshotBuilder;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let data = bincode::serialize(vote).map_err(|_| {
            let err = std::io::Error::other("Serialize vote failed");
            StorageError::IO {
                source: StorageIOError::new(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write, &err),
            }
        })?;
        self.engine.put(Bytes::from("meta:vote"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let data = self.engine.get(b"meta:vote").map_err(Self::map_engine_error)?;
        match data {
            Some(d) => Ok(Some(bincode::deserialize(&d).map_err(|_| {
                let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Deserialize vote failed");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Read, &err),
                }
            })?)),
            None => Ok(None),
        }
    }

    async fn get_log_state(&mut self) -> Result<LogState<ApexRaftTypeConfig>, StorageError<u64>> {
        let last_log_id_data = self.engine.get(b"meta:last_log_id").map_err(Self::map_engine_error)?;
        let last_log_id = match last_log_id_data {
            Some(data) => bincode::deserialize(&data).map_err(|_| {
                let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to deserialize last_log_id");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, &err),
                }
            })?,
            None => None,
        };

        let last_purged_data = self.engine.get(b"meta:last_purged_log_id").map_err(Self::map_engine_error)?;
        let last_purged_log_id = match last_purged_data {
            Some(data) => bincode::deserialize(&data).map_err(|_| {
                let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to deserialize last_purged_log_id");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, &err),
                }
            })?,
            None => None,
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<ApexRaftTypeConfig>> + OptionalSend,
    {
        // Collect to avoid non-Send IntoIter issues
        let entries_vec: Vec<_> = entries.into_iter().collect();
        let mut last_log_id = None;
        for entry in entries_vec {
            last_log_id = Some(entry.log_id);
            let index = entry.log_id.index;
            let key = Self::log_key(index);
            let data = bincode::serialize(&entry).map_err(|_| {
                let err = std::io::Error::other("Serialize entry failed");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, &err),
                }
            })?;
            self.engine.put(Bytes::from(key), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        }

        if let Some(log_id) = last_log_id {
            let data = bincode::serialize(&log_id).unwrap();
            self.engine.put(Bytes::from("meta:last_log_id"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut index = log_id.index;
        loop {
            let key = Self::log_key(index);
            if self.engine.get(&key).map_err(Self::map_engine_error)?.is_none() {
                break;
            }
            self.engine.delete(Bytes::from(key)).await.map_err(Self::map_engine_error)?;
            index += 1;
        }

        let new_last_log_id = if log_id.index > 0 {
            let prev_key = Self::log_key(log_id.index - 1);
            let prev_data = self.engine.get(&prev_key).map_err(Self::map_engine_error)?;
            if let Some(data) = prev_data {
                let entry: openraft::Entry<ApexRaftTypeConfig> = bincode::deserialize(&data).unwrap();
                Some(entry.log_id)
            } else {
                None
            }
        } else {
            None
        };

        let data = bincode::serialize(&new_last_log_id).unwrap();
        self.engine.put(Bytes::from("meta:last_log_id"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let data = bincode::serialize(&log_id).unwrap();
        self.engine.put(Bytes::from("meta:last_purged_log_id"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        
        for index in 0..=log_id.index {
            let key = Self::log_key(index);
            self.engine.delete(Bytes::from(key)).await.map_err(Self::map_engine_error)?;
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let last_applied_data = self.engine.get(b"meta:last_applied").map_err(Self::map_engine_error)?;
        let last_applied = match last_applied_data {
            Some(d) => bincode::deserialize(&d).map_err(|_| {
                let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Deserialize last_applied failed");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Read, &err),
                }
            })?,
            None => None,
        };

        let last_membership_data = self.engine.get(b"meta:last_membership").map_err(Self::map_engine_error)?;
        let last_membership = match last_membership_data {
            Some(d) => bincode::deserialize(&d).map_err(|_| {
                let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Deserialize last_membership failed");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Read, &err),
                }
            })?,
            None => StoredMembership::default(),
        };

        Ok((last_applied, last_membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[openraft::Entry<ApexRaftTypeConfig>],
    ) -> Result<Vec<Vec<u8>>, StorageError<u64>> {
        let mut responses = Vec::new();
        for entry in entries {
            let last_log_id = entry.log_id;
            
            if let openraft::EntryPayload::Normal(data) = &entry.payload {
                if let Ok((key, value)) = bincode::deserialize::<(Vec<u8>, Vec<u8>)>(data) {
                    let mut full_key = b"data:".to_vec();
                    full_key.extend_from_slice(&key);
                    self.engine.put(Bytes::from(full_key), Bytes::from(value.clone())).await.map_err(Self::map_engine_error)?;
                    responses.push(vec![1]);
                } else {
                    responses.push(vec![0]);
                }
            } else if let openraft::EntryPayload::Membership(m) = &entry.payload {
                let membership = StoredMembership::new(Some(last_log_id), m.clone());
                let data = bincode::serialize(&membership).unwrap();
                self.engine.put(Bytes::from("meta:last_membership"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
                responses.push(vec![]);
            }

            let data = bincode::serialize(&Some(last_log_id)).unwrap();
            self.engine.put(Bytes::from("meta:last_applied"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ApexSnapshotBuilder { engine: self.engine.clone() }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        _snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = bincode::serialize(&meta.last_log_id).unwrap();
        self.engine.put(Bytes::from("meta:last_applied"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        
        let data = bincode::serialize(&meta.last_membership).unwrap();
        self.engine.put(Bytes::from("meta:last_membership"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        
        let data = bincode::serialize(meta).unwrap();
        self.engine.put(Bytes::from("meta:snapshot_meta"), Bytes::from(data)).await.map_err(Self::map_engine_error)?;
        
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ApexRaftTypeConfig>>, StorageError<u64>> {
        let meta_data = self.engine.get(b"meta:snapshot_meta").map_err(Self::map_engine_error)?;
        let meta = match meta_data {
            Some(d) => bincode::deserialize(&d).map_err(|_| {
                let err = std::io::Error::new(std::io::ErrorKind::InvalidData, "Deserialize snapshot_meta failed");
                StorageError::IO {
                    source: StorageIOError::new(openraft::ErrorSubject::Snapshot(None), openraft::ErrorVerb::Read, &err),
                }
            })?,
            None => return Ok(None),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(std::io::Cursor::new(vec![])),
        }))
    }
}
