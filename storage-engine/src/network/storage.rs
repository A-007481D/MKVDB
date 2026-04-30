use std::sync::Arc;
use std::ops::RangeBounds;
use bytes::Bytes;
use crate::batch::WriteBatch;
use crate::engine::ApexEngine;
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
        let end_key = format!("log:{:020}", end);

        let mut stream = self.engine.scan(
            Bytes::copy_from_slice(start_key.as_bytes()),
            Bytes::copy_from_slice(end_key.as_bytes()),
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

        let last_log_id = last_id_raw
            .map(|d| bincode::deserialize::<LogId<u64>>(&d))
            .transpose()
            .map_err(|e| Self::map_io_err(ErrorSubject::Store, ErrorVerb::Read, e))?;

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
        self.engine.put(Bytes::from_static(b"meta:vote"), Bytes::from(data)).await.map_err(|e| {
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

        self.engine.write_batch(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e)
        })?;

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start_key = format!("log:{:020}", log_id.index);
        let end_key = format!("log:{:020}", u64::MAX);

        let mut stream = self.engine.scan(
            Bytes::copy_from_slice(start_key.as_bytes()),
            Bytes::copy_from_slice(end_key.as_bytes()),
        ).map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;

        let mut batch = WriteBatch::new();
        while let Some(res) = stream.next().await {
            let (key, _val) = res.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            batch.delete(key);
        }

        self.engine.write_batch(batch).await.map_err(|e| {
            Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e)
        })?;

        let last_log_id = self.try_get_log_entries(..log_id.index).await?.last().map(|e| e.log_id);
        if let Some(id) = last_log_id {
            let val = bincode::serialize(&id).map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
            self.engine.put(Bytes::from_static(b"meta:last_log_id"), Bytes::from(val)).await.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        } else {
            self.engine.delete(Bytes::from_static(b"meta:last_log_id")).await.map_err(|e| Self::map_io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start_key = format!("log:{:020}", 0);
        let end_key = format!("log:{:020}", log_id.index);

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

        self.engine.write_batch(batch).await.map_err(|e| {
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
                    batch.put(
                        Bytes::from(format!("data:{:020}", entry.log_id.index)),
                        Bytes::from(data.clone()),
                    );
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

        self.engine.write_batch(batch).await.map_err(|e| {
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

        let data = snapshot.into_inner();
        let state_machine_data: Vec<(Vec<u8>, Vec<u8>)> = bincode::deserialize(&data)
            .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Write, e))?;

        let mut batch = WriteBatch::new();

        let mut stream = self.engine.scan(
            Bytes::from_static(b"data:"),
            Bytes::from_static(b"data\xff"),
        ).map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;

        while let Some(res) = stream.next().await {
            let (key, _) = res.map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;
            batch.delete(key);
        }

        for (k, v) in state_machine_data {
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

        self.engine.write_batch(batch).await.map_err(|e| {
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

        let mut state_data = Vec::new();
        while let Some(res) = stream.next().await {
            let (k, v) = res.map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;
            state_data.push((k.to_vec(), v.to_vec()));
        }

        let data = bincode::serialize(&state_data)
            .map_err(|e| Self::map_io_err(ErrorSubject::Snapshot(Some(signature.clone())), ErrorVerb::Read, e))?;

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
        
        self.engine.put(Bytes::from_static(b"meta:snapshot"), Bytes::from(meta_val)).await.map_err(|e| {
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
