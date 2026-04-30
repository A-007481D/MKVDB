use std::sync::Arc;
use openraft::{Raft, Config};
use crate::engine::ApexEngine;
use crate::network::grpc::{ApexRaftNetworkFactory, ApexRaftServer, ApexRaftTypeConfig};
use crate::network::storage::ApexRaftStorage;
use tonic::transport::Server;
use anyhow::Result;

/// ApexNode is the primary entry point for a distributed MKVDB node.
/// It bundles the LSM-Tree engine, the Raft consensus instance, and the gRPC server.
pub struct ApexNode {
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
        let (log_store, state_machine) = openraft::storage::Adaptor::new(storage);

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
            log_store,
            state_machine,
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

        Ok(Self { raft, engine })
    }

    /// Helper to submit a write command to the cluster.
    pub async fn write(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // For now, we just pass the raw bytes. In a real system,
        // we'd have a Command enum (Put, Delete, etc.)
        let payload = bincode::serialize(&(key, value))?;
        self.raft.client_write(payload).await?;
        Ok(())
    }

    /// Helper to read data from the local engine.
    /// In a linearizable system, we might want to route this through Raft.
    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let val = self.engine.get(key)?;
        Ok(val.map(|b| b.to_vec()))
    }
}
