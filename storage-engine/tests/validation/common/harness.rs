use anyhow::Result;
use openraft::ServerState;
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use storage_engine::network::node::ApexNode;
use tempfile::TempDir;

use crate::validation::common::proxy::{FaultController, ProxyNetworkFactory};
use storage_engine::network::grpc::ApexRaftNetworkFactory;

/// Tracks the persistent state for a node across kill/restart cycles.
pub struct NodePersistence {
    pub data_dir_path: PathBuf,
    pub bind_addr: String,
    _temp_dir: TempDir,
}

/// A running node handle. Dropping this kills the node.
pub struct LiveNode {
    pub node: ApexNode,
}

/// Minimal cluster harness. Uses only existing production abstractions.
pub struct ClusterHarness {
    pub live: HashMap<u64, LiveNode>,
    pub persistence: HashMap<u64, NodePersistence>,
    pub engine_config: ApexConfig,
    pub faults: Arc<FaultController>,
}

/// Find a free port by binding to :0 and reading back the assigned port.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to :0");
    listener.local_addr().expect("local_addr").port()
}

impl ClusterHarness {
    pub async fn new(n: usize) -> Result<Self> {
        let engine_config = ApexConfig::default().with_sync_policy(SyncPolicy::EveryWrite);

        let mut harness = Self {
            live: HashMap::new(),
            persistence: HashMap::new(),
            engine_config,
            faults: Arc::new(FaultController::default()),
        };

        for i in 1..=(n as u64) {
            let temp_dir = TempDir::new()?;
            let port = free_port();
            harness.persistence.insert(
                i,
                NodePersistence {
                    data_dir_path: temp_dir.path().to_path_buf(),
                    bind_addr: format!("127.0.0.1:{}", port),
                    _temp_dir: temp_dir,
                },
            );
            harness.boot_node(i).await?;
        }

        Ok(harness)
    }

    pub async fn boot_node(&mut self, id: u64) -> Result<()> {
        let p = self
            .persistence
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("No persistence entry for node {}", id))?;

        let engine = ApexEngine::open_with_config(&p.data_dir_path, self.engine_config.clone())?;

        let network_factory = ProxyNetworkFactory {
            inner: ApexRaftNetworkFactory {},
            source_id: id,
            faults: self.faults.clone(),
        };

        let node = ApexNode::start_with_network(id, &p.bind_addr, engine, network_factory).await?;

        self.live.insert(id, LiveNode { node });
        Ok(())
    }

    pub async fn kill_node(&mut self, id: u64) {
        if let Some(mut live) = self.live.remove(&id) {
            live.node.shutdown();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    pub async fn restart_node(&mut self, id: u64) -> Result<()> {
        self.kill_node(id).await;
        self.boot_node(id).await
    }

    // --- Metrics ---

    pub fn get_leader(&self) -> Option<u64> {
        for (&id, live) in &self.live {
            let m = live.node.raft.metrics().borrow().clone();
            if m.state == ServerState::Leader {
                return Some(id);
            }
        }
        None
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<u64> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            for (&id, live) in &self.live {
                let m = live.node.raft.metrics().borrow().clone();
                if m.state == ServerState::Leader && m.vote.committed {
                    return Ok(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!("No leader emerged within {:?}", timeout)
    }

    pub async fn wait_for_new_leader(&self, old_leader: u64, timeout: Duration) -> Result<u64> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            for (&id, live) in &self.live {
                if id == old_leader {
                    continue;
                }
                let m = live.node.raft.metrics().borrow().clone();
                if m.state == ServerState::Leader && m.vote.committed {
                    return Ok(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "No new leader (other than {}) emerged within {:?}",
            old_leader,
            timeout
        )
    }

    pub async fn wait_for_applied(&self, id: u64, index: u64, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if let Some(live) = self.live.get(&id) {
                let m = live.node.raft.metrics().borrow().clone();
                if m.last_applied.map_or(0, |lid| lid.index) >= index {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!(
            "Node {} did not reach applied index {} within {:?}",
            id,
            index,
            timeout
        )
    }

    // --- Client Ops ---

    pub async fn put(&self, id: u64, key: Vec<u8>, val: Vec<u8>) -> Result<()> {
        let live = self
            .live
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("Node {} not live", id))?;
        live.node
            .write_put(key, val)
            .await
            .map_err(|e| anyhow::anyhow!("Write failed on node {}: {:?}", id, e))
    }

    pub async fn get(&self, id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let live = self
            .live
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("Node {} not live", id))?;
        live.node
            .read(key)
            .await
            .map(|opt| opt.map(|b| b.to_vec()))
            .map_err(|e| anyhow::anyhow!("Read failed on node {}: {:?}", id, e))
    }

    /// Read directly from the engine, bypassing Raft linearizability.
    /// Useful for verifying replication on followers.
    pub async fn get_stale(&self, id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let live = self
            .live
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("Node {} not live", id))?;
        // We need to add the "data:" prefix because the engine stores keys with prefixes
        let mut full_key = b"data:".to_vec();
        full_key.extend_from_slice(key);

        live.node
            .engine
            .get(&full_key)
            .map(|opt| opt.map(|b| b.to_vec()))
            .map_err(|e| anyhow::anyhow!("Engine read failed on node {}: {:?}", id, e))
    }

    pub fn assert_single_leader(&self) -> Result<u64> {
        let leaders: Vec<u64> = self
            .live
            .iter()
            .filter(|(_, live)| live.node.raft.metrics().borrow().state == ServerState::Leader)
            .map(|(&id, _)| id)
            .collect();

        match leaders.len() {
            0 => anyhow::bail!("No leader found"),
            1 => Ok(leaders[0]),
            n => anyhow::bail!("Found {} leaders: {:?}", n, leaders),
        }
    }
}
