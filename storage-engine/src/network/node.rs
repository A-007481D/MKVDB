use std::sync::Arc;
use std::time::Duration;
use openraft::{Raft, Config};
use crate::engine::ApexEngine;
use crate::iterator::ScanStream;
use crate::network::grpc::{ApexRaftNetworkFactory, ApexRaftServer, ApexRaftTypeConfig};
use crate::network::storage::ApexRaftStorage;
use tonic::transport::Server;
use anyhow::Result;
use bytes::Bytes;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use openraft::BasicNode;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum RaftCommand {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteRedirect {
    Leader(String),
    UnknownLeader,
}

/// Error type for read operations that go through the Raft linearizability
/// barrier. On non-leader nodes, a redirect is returned so the RESP server
/// can issue a MOVED response.
#[derive(Debug)]
pub enum ReadError {
    Redirect(WriteRedirect),
    Storage(String),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redirect(r) => write!(f, "redirect: {r:?}"),
            Self::Storage(s) => write!(f, "storage error: {s}"),
        }
    }
}

/// ApexNode is the primary entry point for a distributed MKVDB node.
/// It bundles the LSM-Tree engine, the Raft consensus instance, and the gRPC server.
pub struct ApexNode {
    pub node_id: u64,
    pub raft: Raft<ApexRaftTypeConfig>,
    pub engine: Arc<ApexEngine>,
}

impl ApexNode {
    /// Starts the node, initializing the storage engine, Raft consensus, and gRPC network.
    /// 
    /// # Arguments
    /// * `node_id` - Unique identifier for this node in the cluster.
    /// * `bind_addr` - The address to bind the gRPC server to (e.g., "0.0.0.0:50051").
    /// * `engine` - The shared ApexEngine instance.
    pub async fn start(node_id: u64, bind_addr: &str, engine: Arc<ApexEngine>) -> Result<Self> {
        // 1. Initialize ApexRaftNetworkFactory (the networking adapter)
        let network_factory = ApexRaftNetworkFactory {};

        // 2. Initialize ApexRaftStorage (the storage adapter)
        let storage = ApexRaftStorage::new(engine.clone());

        // 3. Configure and initialize the openraft::Raft instance
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            // Enable snapshots for large log management
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            ..Default::default()
        };
        
        let raft = Raft::new(
            node_id,
            Arc::new(config),
            network_factory,
            storage.clone(),
            storage,
        )
        .await?;

        // 4. Wrap the Raft instance for the gRPC server
        let raft_clone = Arc::new(raft.clone());
        let server = ApexRaftServer::new(raft_clone);
        let grpc_service = server.into_grpc_service();

        // 5. Spawn the gRPC server in a background task
        let addr = bind_addr.parse()?;
        tokio::spawn(async move {
            tracing::info!("Starting Raft gRPC server on {}", addr);
            Server::builder()
                .add_service(grpc_service)
                .serve(addr)
                .await
                .expect("Failed to start Raft gRPC server");
        });

        Ok(Self {
            node_id,
            raft,
            engine,
        })
    }

    fn map_forwarded_leader(leader: &ForwardToLeader<u64, BasicNode>) -> WriteRedirect {
        if let Some(node) = &leader.leader_node {
            return WriteRedirect::Leader(node.addr.clone());
        }
        WriteRedirect::UnknownLeader
    }

    /// Waits until this node is a stable leader and has at least one committed/applied log.
    /// This barrier is required before performing config changes in tests/orchestration.
    pub async fn await_config_change_ready(&self, timeout: Duration) -> Result<()> {
        self.raft
            .wait(Some(timeout))
            .current_leader(self.node_id, "wait for current leader")
            .await?;
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(1), "wait for first committed/applied entry")
            .await?;
        Ok(())
    }

    /// Appends a noop-equivalent command and waits for it to commit/apply.
    /// We use a reserved key prefix so the entry is observable and deterministic.
    pub async fn commit_noop_barrier(&self) -> Result<(), WriteRedirect> {
        let key = format!("__raft_barrier__:{}", self.node_id).into_bytes();
        self.write_put(key, b"1".to_vec()).await
    }

    async fn write_command(&self, command: RaftCommand) -> Result<(), WriteRedirect> {
        let payload = bincode::serialize(&command).map_err(|_| WriteRedirect::UnknownLeader)?;
        match self.raft.client_write(payload).await {
            Ok(_) => Ok(()),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(leader))) => {
                Err(Self::map_forwarded_leader(&leader))
            }
            Err(_) => Err(WriteRedirect::UnknownLeader),
        }
    }

    /// Helper to submit a PUT command to the cluster.
    pub async fn write_put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), WriteRedirect> {
        self.write_command(RaftCommand::Put(key, value)).await
    }

    /// Helper to submit a DELETE command to the cluster.
    pub async fn write_delete(&self, key: Vec<u8>) -> Result<(), WriteRedirect> {
        self.write_command(RaftCommand::Delete(key)).await
    }

    /// Reads a key with linearizability guarantees.
    ///
    /// Confirms leadership with a quorum via `ensure_linearizable()` before
    /// reading the local engine. The `data:` keyspace prefix is applied
    /// automatically — callers pass the user-facing key.
    pub async fn read(&self, key: &[u8]) -> std::result::Result<Option<Vec<u8>>, ReadError> {
        self.raft.ensure_linearizable().await.map_err(|_e| {
            ReadError::Redirect(WriteRedirect::UnknownLeader)
        })?;

        let mut data_key = Vec::with_capacity(5 + key.len());
        data_key.extend_from_slice(b"data:");
        data_key.extend_from_slice(key);

        let val = self.engine.get(&data_key).map_err(|e| {
            ReadError::Storage(format!("{e:?}"))
        })?;
        Ok(val.map(|b| b.to_vec()))
    }

    /// Performs a range scan with linearizability guarantees.
    ///
    /// Confirms leadership with a quorum via `ensure_linearizable()` before
    /// scanning the local engine. The `data:` keyspace prefix is applied
    /// automatically — callers pass user-facing keys.
    pub async fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> std::result::Result<ScanStream, ReadError> {
        self.raft.ensure_linearizable().await.map_err(|_| {
            ReadError::Redirect(WriteRedirect::UnknownLeader)
        })?;

        let mut prefixed_start = Vec::with_capacity(5 + start_key.len());
        prefixed_start.extend_from_slice(b"data:");
        prefixed_start.extend_from_slice(start_key);

        let mut prefixed_end = Vec::with_capacity(5 + end_key.len());
        prefixed_end.extend_from_slice(b"data:");
        prefixed_end.extend_from_slice(end_key);

        self.engine.scan(
            Bytes::from(prefixed_start),
            Bytes::from(prefixed_end),
        ).map_err(|e| ReadError::Storage(format!("{e:?}")))
    }
}
