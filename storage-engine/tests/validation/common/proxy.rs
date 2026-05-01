use std::sync::{Arc, RwLock};
use std::collections::HashSet;
use openraft::{
    BasicNode, 
    error::{NetworkError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use storage_engine::network::grpc::{ApexRaftNetworkFactory, ApexRaftNetworkConnection, ApexRaftTypeConfig};

/// Controlled network faults for the proxy.
#[derive(Default)]
pub struct FaultController {
    /// Pairs of (source, target) that are partitioned.
    partitions: RwLock<HashSet<(u64, u64)>>,
}

impl FaultController {
    pub fn partition(&self, a: u64, b: u64) {
        let mut p = self.partitions.write().unwrap();
        p.insert((a, b));
        p.insert((b, a));
    }

    pub fn heal_all(&self) {
        let mut p = self.partitions.write().unwrap();
        p.clear();
    }

    pub fn is_partitioned(&self, src: u64, dst: u64) -> bool {
        let p = self.partitions.read().unwrap();
        p.contains(&(src, dst))
    }
}

/// Wraps ApexRaftNetworkFactory to inject faults.
pub struct ProxyNetworkFactory {
    pub inner: ApexRaftNetworkFactory,
    pub source_id: u64,
    pub faults: Arc<FaultController>,
}

impl RaftNetworkFactory<ApexRaftTypeConfig> for ProxyNetworkFactory {
    type Network = ProxyNetworkConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let inner_conn = self.inner.new_client(target, node).await;
        ProxyNetworkConnection {
            inner: inner_conn,
            source: self.source_id,
            target,
            faults: self.faults.clone(),
        }
    }
}

pub struct ProxyNetworkConnection {
    inner: ApexRaftNetworkConnection,
    source: u64,
    target: u64,
    faults: Arc<FaultController>,
}

impl RaftNetwork<ApexRaftTypeConfig> for ProxyNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ApexRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.faults.is_partitioned(self.source, self.target) {
            return Err(RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Network Partitioned"))));
        }
        self.inner.append_entries(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ApexRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>> {
        if self.faults.is_partitioned(self.source, self.target) {
            return Err(RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Network Partitioned"))));
        }
        self.inner.install_snapshot(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.faults.is_partitioned(self.source, self.target) {
            return Err(RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Network Partitioned"))));
        }
        self.inner.vote(rpc, option).await
    }
}
