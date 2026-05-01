use crate::engine::ApexEngine;
use crate::iterator::ScanStream;
use crate::network::grpc::{ApexRaftNetworkFactory, ApexRaftServer, ApexRaftTypeConfig};
use crate::network::storage::ApexRaftStorage;
use anyhow::Result;
use bytes::Bytes;
use openraft::BasicNode;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use openraft::{Config, Raft};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

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
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ApexNode {
    /// Starts the node with the default gRPC networking factory.
    pub async fn start(node_id: u64, bind_addr: &str, engine: Arc<ApexEngine>) -> Result<Self> {
        Self::start_with_network(node_id, bind_addr, engine, ApexRaftNetworkFactory {}).await
    }

    /// Starts the node with a custom networking factory.
    pub async fn start_with_network<N>(
        node_id: u64,
        bind_addr: &str,
        engine: Arc<ApexEngine>,
        network_factory: N,
    ) -> Result<Self>
    where
        N: openraft::network::RaftNetworkFactory<ApexRaftTypeConfig> + Send + Sync + 'static,
        N::Network: openraft::network::RaftNetwork<ApexRaftTypeConfig> + Send + Sync + 'static,
    {
        // 1. Storage adapter initialization
        let storage = ApexRaftStorage::new(engine.clone());

        // 3. Configure and initialize the openraft::Raft instance
        let config = Config {
            cluster_name: "apex-cluster".to_string(),
            heartbeat_interval: 500,
            election_timeout_min: 2000,
            election_timeout_max: 5000,
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

        // 5. Spawn the gRPC server with a graceful shutdown signal
        let addr = bind_addr.parse()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            tracing::info!("Starting Raft gRPC server on {}", addr);
            if let Err(e) = Server::builder()
                .add_service(grpc_service)
                .serve_with_shutdown(addr, async {
                    shutdown_rx.await.ok();
                })
                .await
            {
                tracing::error!("Raft gRPC server error: {}", e);
            }
        });

        // Ensure the server is actually listening before returning
        let mut retry = 0;
        while retry < 20 {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            retry += 1;
        }

        if retry == 20 {
            return Err(anyhow::anyhow!(
                "Raft server failed to bind on {addr} after 1s"
            ));
        }

        Ok(Self {
            node_id,
            raft,
            engine,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Gracefully stop the gRPC server, releasing the bound socket.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    fn map_forwarded_leader(leader: &ForwardToLeader<u64, BasicNode>) -> WriteRedirect {
        if let Some(node) = &leader.leader_node {
            return WriteRedirect::Leader(node.addr.clone());
        }
        WriteRedirect::UnknownLeader
    }

    /// Awaits until the cluster is fully stabilized and ready for configuration changes.
    ///
    /// This is the "Raft Stabilization Barrier" required to prevent panics during
    /// membership changes. It guarantees:
    /// 1. A stable leader is elected.
    /// 2. The leader's vote is committed (essential for membership changes).
    /// 3. At least one entry (the baseline) is committed and applied.
    /// 4. A quorum is actively reachable (confirmed via linearizability check).
    pub async fn await_cluster_stable(&self, timeout: Duration) -> Result<()> {
        tracing::info!("Node {}: Starting stabilization sequence...", self.node_id);

        // 1. Wait for stable leader, committed vote, and applied index >= 1.
        self.raft
            .wait(Some(timeout))
            .metrics(
                |m| {
                    let is_stable = m.current_leader.is_some()
                        && m.last_applied.map_or(0, |id| id.index) >= 1
                        && m.vote.committed;
                    if is_stable {
                        tracing::info!(
                            "Node {}: Metrics stability reached. Vote: {:?}, Applied: {:?}",
                            m.id,
                            m.vote,
                            m.last_applied
                        );
                    }
                    is_stable
                },
                "wait for committed leadership",
            )
            .await?;

        // 2. Perform a linearizability check to force a quorum confirmation.
        self.raft.ensure_linearizable().await.map_err(|e| {
            anyhow::anyhow!(
                "Cluster stabilization failed: leader cannot confirm quorum: {e:?}"
            )
        })?;

        // 3. Sledgehammer Quiescence: Give the cluster 2 seconds to synchronize internal state.
        tracing::info!("Node {}: Entering 2s quiescence...", self.node_id);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let final_metrics = self.raft.metrics().borrow().clone();
        tracing::info!(
            "Node {}: Stabilization complete. Final Vote Committed: {}",
            self.node_id,
            final_metrics.vote.committed
        );

        Ok(())
    }

    /// Appends a noop-equivalent command and waits for it to commit/apply.
    /// We use a reserved key prefix so the entry is observable and deterministic.
    pub async fn commit_noop_barrier(&self) -> Result<(), WriteRedirect> {
        let key = format!("__raft_barrier__:{}", self.node_id).into_bytes();
        self.write_put(key, b"1".to_vec()).await
    }

    /// Safely adds a learner to the cluster, ensuring all Raft invariants are met.
    pub async fn safe_add_learner(
        &self,
        node_id: u64,
        node: BasicNode,
        blocking: bool,
    ) -> Result<openraft::raft::ClientWriteResponse<ApexRaftTypeConfig>> {
        // Enforce stabilization before the mutation
        self.await_cluster_stable(Duration::from_secs(10)).await?;

        // Final sanity write to ensure the term is "sealed"
        self.commit_noop_barrier()
            .await
            .map_err(|e| anyhow::anyhow!("Barrier failed: {e:?}"))?;

        Ok(self.raft.add_learner(node_id, node, blocking).await?)
    }

    /// Safely changes the cluster membership, ensuring all Raft invariants are met.
    pub async fn safe_change_membership(
        &self,
        members: BTreeSet<u64>,
        retain: bool,
    ) -> Result<openraft::raft::ClientWriteResponse<ApexRaftTypeConfig>> {
        // Enforce stabilization before the mutation
        self.await_cluster_stable(Duration::from_secs(10)).await?;

        Ok(self.raft.change_membership(members, retain).await?)
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
        self.raft.ensure_linearizable().await.map_err(|e| {
            if let RaftError::APIError(openraft::error::CheckIsLeaderError::ForwardToLeader(
                leader,
            )) = e
            {
                ReadError::Redirect(Self::map_forwarded_leader(&leader))
            } else {
                ReadError::Redirect(WriteRedirect::UnknownLeader)
            }
        })?;

        let mut data_key = Vec::with_capacity(5 + key.len());
        data_key.extend_from_slice(b"data:");
        data_key.extend_from_slice(key);

        let val = self
            .engine
            .get(&data_key)
            .map_err(|e| ReadError::Storage(format!("{e:?}")))?;
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
        self.raft.ensure_linearizable().await.map_err(|e| {
            if let RaftError::APIError(openraft::error::CheckIsLeaderError::ForwardToLeader(
                leader,
            )) = e
            {
                ReadError::Redirect(Self::map_forwarded_leader(&leader))
            } else {
                ReadError::Redirect(WriteRedirect::UnknownLeader)
            }
        })?;

        let mut prefixed_start = Vec::with_capacity(5 + start_key.len());
        prefixed_start.extend_from_slice(b"data:");
        prefixed_start.extend_from_slice(start_key);

        let mut prefixed_end = Vec::with_capacity(5 + end_key.len());
        prefixed_end.extend_from_slice(b"data:");
        prefixed_end.extend_from_slice(end_key);

        self.engine
            .scan(Bytes::from(prefixed_start), Bytes::from(prefixed_end))
            .map_err(|e| ReadError::Storage(format!("{e:?}")))
    }
}
