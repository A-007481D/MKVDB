use crate::validation::adversarial::core::event_log::{AdversarialEvent, EventLog};
use crate::validation::adversarial::core::fault_controller::FaultController;
use openraft::{
    BasicNode,
    error::{NetworkError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use std::sync::Arc;
use std::time::Duration;
use storage_engine::network::grpc::{
    ApexRaftNetworkConnection, ApexRaftNetworkFactory, ApexRaftTypeConfig,
};

pub struct AdversarialNetworkFactory {
    pub inner: ApexRaftNetworkFactory,
    pub source_id: u64,
    pub log: Arc<EventLog>,
    pub faults: Arc<FaultController>,
}

impl RaftNetworkFactory<ApexRaftTypeConfig> for AdversarialNetworkFactory {
    type Network = AdversarialNetworkConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let inner_conn = self.inner.new_client(target, node).await;
        AdversarialNetworkConnection {
            inner: inner_conn,
            source: self.source_id,
            target,
            log: self.log.clone(),
            faults: self.faults.clone(),
        }
    }
}

pub struct AdversarialNetworkConnection {
    inner: ApexRaftNetworkConnection,
    source: u64,
    target: u64,
    log: Arc<EventLog>,
    faults: Arc<FaultController>,
}

impl AdversarialNetworkConnection {
    async fn apply_faults(
        &self,
        msg_type: &str,
    ) -> Result<Option<Duration>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let action = self.faults.get_action(self.source, self.target);

        if action.drop {
            self.log.log(AdversarialEvent::FaultInjected {
                from: self.source,
                to: self.target,
                rule: format!("Drop({})", msg_type),
            });
            return Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "Dropped by Interceptor",
            ))));
        }

        if let Some(duration) = action.delay {
            self.log.log(AdversarialEvent::FaultInjected {
                from: self.source,
                to: self.target,
                rule: format!("Delay({}, {:?})", msg_type, duration),
            });
            tokio::time::sleep(duration).await;
        }

        Ok(action.delay)
    }
}

impl RaftNetwork<ApexRaftTypeConfig> for AdversarialNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ApexRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.apply_faults("AppendEntries").await?;

        self.log.log(AdversarialEvent::RpcSent {
            from: self.source,
            to: self.target,
            msg: "AppendEntries".to_string(),
        });

        let res = self.inner.append_entries(rpc, option).await;

        self.log.log(AdversarialEvent::RpcReceived {
            from: self.source,
            to: self.target,
            msg: "AppendEntries".to_string(),
            success: res.is_ok(),
        });

        res
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ApexRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        // We handle the error mapping manually since apply_faults returns a generic RaftError
        match self.apply_faults("InstallSnapshot").await {
            Ok(_) => {}
            Err(_) => {
                return Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "Dropped by Interceptor",
                ))));
            }
        }

        self.log.log(AdversarialEvent::RpcSent {
            from: self.source,
            to: self.target,
            msg: "InstallSnapshot".to_string(),
        });

        let res = self.inner.install_snapshot(rpc, option).await;

        self.log.log(AdversarialEvent::RpcReceived {
            from: self.source,
            to: self.target,
            msg: "InstallSnapshot".to_string(),
            success: res.is_ok(),
        });

        res
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.apply_faults("Vote").await?;

        self.log.log(AdversarialEvent::RpcSent {
            from: self.source,
            to: self.target,
            msg: "Vote".to_string(),
        });

        let res = self.inner.vote(rpc, option).await;

        self.log.log(AdversarialEvent::RpcReceived {
            from: self.source,
            to: self.target,
            msg: "Vote".to_string(),
            success: res.is_ok(),
        });

        res
    }
}
