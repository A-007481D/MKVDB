use std::sync::Arc;
use std::ops::RangeBounds;
use bytes::Bytes;
use crate::batch::WriteBatch;
use crate::engine::ApexEngine;
use crate::network::node::RaftCommand;
use futures_util::StreamExt;
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError,
    Vote, BasicNode, StoredMembership, ErrorSubject, ErrorVerb, AnyError,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine, LogFlushed};
use openraft::storage::SnapshotSignature;
use std::io::Cursor;
use crate::network::grpc::ApexRaftTypeConfig;

/// ApexRaftStorage bridges the ApexEngine KV store with the OpenRaft consensus engine.
/// It implements all mandatory storage traits for Raft 0.9.x.
#[derive(Clone)]
pub struct ApexRaftStorage {
    pub engine: Arc<ApexEngine>,
}

impl ApexRaftStorage {
    pub fn new(engine: Arc<ApexEngine>) -> Self {
        Self { engine }
    }

    /// Helper to convert engine errors to Raft storage errors.
    fn map_io_err<E: std::error::Error + Send + Sync + 'static>(
        subject: ErrorSubject<u64>,
        verb: ErrorVerb,
        err: E,
    ) -> StorageError<u64> {
        StorageError::IO {
            source: StorageIOError::new(subject, verb, AnyError::new(&err)),
        }
    }

    /// Internal helper to get a snapshot signature for error reporting.
    fn get_signature(meta: &SnapshotMeta<u64, BasicNode>) -> SnapshotSignature<u64> {
        SnapshotSignature {
            last_log_id: meta.last_log_id,
            last_membership_log_id: *meta.last_membership.log_id(),
            snapshot_id: meta.snapshot_id.clone(),
        }
    }
}

impl RaftLogReader<ApexRaftTypeConfig> for ApexRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ApexRaftTypeConfig>>, StorageError<u64>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&i) => i,
            std::ops::Bound::Excluded(&i) => i.saturating_sub(1),
            std::ops::Bound::Unbounded => u64::MAX,
        };

        if start > end {
            return Ok(vec![]);
        }

        let start_key = format!("log:{:020}", start);
        let mut end_key_bytes = format!("log:{:020}", end).into_bytes();
        end_key_bytes.push(0xff);
        let end_key = Bytes::from(end_key_bytes);

        let mut stream = self.engine.scan(
            Bytes::copy_from_slice(start_key.as_bytes()),
            end_key,
        ).map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;

        let mut entries = Vec::new();
        while let Some(res) = stream.next().await {
            let (_key, val) = res.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            let entry: Entry<ApexRaftTypeConfig> = bincode::deserialize(&val)
                .map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftLogStorage<ApexRaftTypeConfig> for ApexRaftStorage {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::LogState<ApexRaftTypeConfig>, StorageError<u64>> {
        let last_id_raw = self.engine.get(b"meta:last_log_id").map_err(|e| {
            Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e)
        })?;

        let mut last_log_id = last_id_raw
            .map(|d| bincode::deserialize::<LogId<u64>>(&d))
            .transpose()
            .map_err(|e| Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e))?;

        // Fallback: If metadata is missing but logs exist, scan for the last log
        if last_log_id.is_none() {
            let mut stream = self.engine.scan(
                Bytes::from_static(b"log:"),
                Bytes::from_static(b"log:\xFF"),
            ).map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            
            let mut max_entry: Option<Entry<ApexRaftTypeConfig>> = None;
            while let Some(res) = stream.next().await {
                let (_, val) = res.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
                let entry: Entry<ApexRaftTypeConfig> = bincode::deserialize(&val)
                    .map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
                max_entry = Some(entry);
            }
            
            if let Some(entry) = max_entry {
                tracing::info!("RaftStorage: Recovered last_log_id from WAL: {:?}", entry.log_id);
                last_log_id = Some(entry.log_id);
            }
        }

        let last_purged_raw = self.engine.get(b"meta:last_purged_log_id").map_err(|e| {
            Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e)
        })?;

        let last_purged_log_id = last_purged_raw
            .map(|d| bincode::deserialize::<LogId<u64>>(&d))
            .transpose()
            .map_err(|e| Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e))?;

        Ok(openraft::LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let data = bincode::serialize(vote)
            .map_err(|e| Self::map_io_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        self.engine.put_sync(Bytes::from_static(b"meta:vote"), Bytes::from(data)).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Vote, ErrorVerb::Write, e)
        })?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let data = self.engine.get(b"meta:vote").map_err(|e| {
            Self::map_io_err(ErrorSubject::Vote, ErrorVerb::Read, e)
        })?;
        Ok(data.map(|d| bincode::deserialize(&d).map_err(|e| {
            Self::map_io_err(ErrorSubject::Vote, ErrorVerb::Read, e)
        })).transpose()?)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<ApexRaftTypeConfig>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<ApexRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut batch = WriteBatch::new();
        let mut last_log_id = None;

        let entries_vec: Vec<_> = entries.into_iter().collect();
        for entry in &entries_vec {
            last_log_id = Some(entry.log_id);
            let key = format!("log:{:020}", entry.log_id.index);
            let val = bincode::serialize(&entry)
                .map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
            batch.put(Bytes::copy_from_slice(key.as_bytes()), Bytes::from(val));
        }

        if let Some(log_id) = last_log_id {
            let meta_val = bincode::serialize(&log_id)
                .map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
            batch.put(
                Bytes::from_static(b"meta:last_log_id"),
                Bytes::from(meta_val),
            );
        }

        self.engine.write_batch_sync(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e)
        })?;

        // CRITICAL VERIFICATION: Ensure the write is actually visible to subsequent reads
        if let Some(expected_id) = last_log_id {
            let actual_id_raw = self.engine.get(b"meta:last_log_id")
                .map_err(|e| Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
            
            let actual_id: Option<LogId<u64>> = actual_id_raw
                .map(|d| bincode::deserialize(&d))
                .transpose()
                .map_err(|e| Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e))?;
                
            if actual_id != Some(expected_id) {
                let msg = format!("RaftStorage persistence mismatch! Expected last_log_id {:?}, found {:?}", expected_id, actual_id);
                tracing::error!("{}", msg);
                return Err(Self::map_io_err(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    std::io::Error::new(std::io::ErrorKind::Other, msg)
                ));
            }
        }

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start_key = format!("log:{:020}", log_id.index);
        let end_key = format!("log:{:020}", u64::MAX);

        let mut batch = WriteBatch::new();

        // 1. Determine the new last_log_id (the entry just before the truncated range)
        let new_last_id = if log_id.index > 0 {
            self.try_get_log_entries(log_id.index - 1..log_id.index)
                .await?
                .first()
                .map(|e| e.log_id)
        } else {
            None
        };

        // 2. Update metadata in the same batch
        if let Some(id) = new_last_id {
            let val = bincode::serialize(&id)
                .map_err(|e| Self::map_io_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
            batch.put(Bytes::from_static(b"meta:last_log_id"), Bytes::from(val));
        } else {
            batch.delete(Bytes::from_static(b"meta:last_log_id"));
        }

        // 3. Scan and add deletions to the batch
        let mut stream = self.engine.scan(
            Bytes::copy_from_slice(start_key.as_bytes()),
            Bytes::copy_from_slice(end_key.as_bytes()),
        ).map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;

        while let Some(res) = stream.next().await {
            let (key, _) = res.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            batch.delete(key);
        }

        // 4. Commit atomically
        self.engine.write_batch_sync(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e)
        })?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start_key = format!("log:{:020}", 0);
        let end_key = format!("log:{:020}", log_id.index + 1);

        let mut stream = self.engine.scan(
            Bytes::copy_from_slice(start_key.as_bytes()),
            Bytes::copy_from_slice(end_key.as_bytes()),
        ).map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;

        let mut batch = WriteBatch::new();
        while let Some(res) = stream.next().await {
            let (key, _val) = res.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            batch.delete(key);
        }

        let val = bincode::serialize(&log_id)
            .map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        batch.put(
            Bytes::from_static(b"meta:last_purged_log_id"),
            Bytes::from(val),
        );

        self.engine.write_batch_sync(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e)
        })?;

        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl RaftStateMachine<ApexRaftTypeConfig> for ApexRaftStorage {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let last_applied_raw = self.engine.get(b"meta:last_applied").map_err(|e| {
            Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e)
        })?;

        let last_applied = last_applied_raw
            .map(|d| bincode::deserialize::<LogId<u64>>(&d))
            .transpose()
            .map_err(|e| Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?;

        let membership_raw = self.engine.get(b"meta:membership").map_err(|e| {
            Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e)
        })?;

        let membership = membership_raw
            .map(|d| bincode::deserialize::<StoredMembership<u64, BasicNode>>(&d))
            .transpose()
            .map_err(|e| Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Read, e))?
            .unwrap_or_default();

        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Vec<u8>>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<ApexRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut res = Vec::new();
        let mut batch = WriteBatch::new();
        let mut last_applied = None;

        for entry in entries {
            last_applied = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Normal(data) => {
                    let command: RaftCommand = bincode::deserialize(data)
                        .map_err(|e| Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;

                    match command {
                        RaftCommand::Put(key, value) => {
                            let mut data_key = Vec::with_capacity(5 + key.len());
                            data_key.extend_from_slice(b"data:");
                            data_key.extend_from_slice(&key);
                            batch.put(Bytes::from(data_key), Bytes::from(value));
                        }
                        RaftCommand::Delete(key) => {
                            let mut data_key = Vec::with_capacity(5 + key.len());
                            data_key.extend_from_slice(b"data:");
                            data_key.extend_from_slice(&key);
                            batch.delete(Bytes::from(data_key));
                        }
                    }
                    res.push(data.clone());
                }
                EntryPayload::Blank => res.push(vec![]),
                EntryPayload::Membership(m) => {
                    let membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    let val = bincode::serialize(&membership)
                        .map_err(|e| Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
                    batch.put(Bytes::from_static(b"meta:membership"), Bytes::from(val));
                    res.push(vec![]);
                }
            }
        }

        if let Some(log_id) = last_applied {
            let val = bincode::serialize(&log_id)
                .map_err(|e| Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
            batch.put(
                Bytes::from_static(b"meta:last_applied"),
                Bytes::from(val),
            );
        }

        self.engine.write_batch_sync(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::StateMachine, ErrorVerb::Write, e)
        })?;

        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let signature = Self::get_signature(meta);

        // Use a buffered reader for efficient streaming deserialization
        let mut reader = std::io::BufReader::new(snapshot);
        
        // 1. Read the number of entries
        let count: u64 = bincode::deserialize_from(&mut reader)
            .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Write, e))?;

        let mut batch = WriteBatch::new();

        // 1. Wipe existing state machine data
        let mut stream = self.engine.scan(
            Bytes::from_static(b"data:"),
            Bytes::from_static(b"data\xff"),
        ).map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;

        while let Some(res) = stream.next().await {
            let (key, _) = res.map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;
            batch.delete(key);
        }

        // 2. Wipe existing metadata to prevent mixed state
        batch.delete(Bytes::from_static(b"meta:last_applied"));
        batch.delete(Bytes::from_static(b"meta:membership"));
        batch.delete(Bytes::from_static(b"meta:last_log_id"));

        // 3. Re-apply state machine data from snapshot
        for _ in 0..count {
            let (k, v): (Vec<u8>, Vec<u8>) = bincode::deserialize_from(&mut reader)
                .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Write, e))?;
            batch.put(Bytes::from(k), Bytes::from(v));
        }

        let meta_val = bincode::serialize(meta)
            .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Write, e))?;
        batch.put(Bytes::from_static(b"meta:snapshot"), Bytes::from(meta_val));

        if let Some(last_id) = meta.last_log_id {
            let last_id_val = bincode::serialize(&last_id)
                .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Write, e))?;
            batch.put(Bytes::from_static(b"meta:last_applied"), Bytes::from(last_id_val));
        }

        self.engine.write_batch_sync(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Write, e)
        })?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ApexRaftTypeConfig>>, StorageError<u64>> {
        let meta_raw = self.engine.get(b"meta:snapshot").map_err(|e| {
            Self::map_io_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e)
        })?;

        let meta = match meta_raw {
            Some(d) => bincode::deserialize::<SnapshotMeta<u64, BasicNode>>(&d)
                .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(None), ErrorVerb::Read, e))?,
            None => return Ok(None),
        };

        let signature = Self::get_signature(&meta);

        let mut stream = self.engine.scan(
            Bytes::from_static(b"data:"),
            Bytes::from_static(b"data\xff"),
        ).map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;

        let mut data = Vec::new();
        
        // First, count entries to write a header (or we could use a different streaming format)
        // For simplicity with bincode's deserialize_from, we'll do two passes or just buffer keys.
        // Actually, let's just buffer the keys and values in a streaming-compatible way.
        let mut count: u64 = 0;
        let mut entries = Vec::new();
        while let Some(res) = stream.next().await {
            let (k, v) = res.map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;
            entries.push((k, v));
            count += 1;
        }

        bincode::serialize_into(&mut data, &count)
            .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;

        for (k, v) in entries {
            bincode::serialize_into(&mut data, &(k.to_vec(), v.to_vec()))
                .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;
        }

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftSnapshotBuilder<ApexRaftTypeConfig> for ApexRaftStorage {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ApexRaftTypeConfig>, StorageError<u64>> {
        let (last_applied, membership) = self.applied_state().await?;

        let snapshot_id = format!(
            "snapshot-{}",
            last_applied.map_or(0, |id| id.index)
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        let meta_val = bincode::serialize(&meta)
            .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        
        self.engine.put_sync(Bytes::from_static(b"meta:snapshot"), Bytes::from(meta_val)).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e)
        })?;

        let signature = Self::get_signature(&meta);

        self.get_current_snapshot()
            .await?
            .ok_or_else(|| StorageError::IO {
                source: StorageIOError::new(
                    ErrorSubject::Snapshot(Some(signature)),
                    ErrorVerb::Read,
                    AnyError::new(&std::io::Error::new(std::io::ErrorKind::NotFound, "Snapshot just created but not found")),
                )
            })
    }
}
