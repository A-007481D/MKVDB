use openraft::{
    BasicNode, CommittedLeaderId, RaftTypeConfig, StoredMembership,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status, codec::CompressionEncoding, transport::Channel};

// Generated gRPC code from proto/raft.proto
#[allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::restriction)]
pub mod raft_proto {
    tonic::include_proto!("raft");
}

use raft_proto::raft_service_client::RaftServiceClient;
use raft_proto::raft_service_server::RaftService;

// --- 1. Raft Type Configuration ---

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    serde::Serialize,
    serde::Deserialize,
    Hash,
)]
pub struct ApexRaftTypeConfig;

impl fmt::Display for ApexRaftTypeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApexRaftTypeConfig")
    }
}

impl RaftTypeConfig for ApexRaftTypeConfig {
    type D = Vec<u8>;
    type R = Vec<u8>;
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = openraft::Entry<ApexRaftTypeConfig>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<ApexRaftTypeConfig>;
}

// --- 2. Protocol Conversion Helpers ---

impl From<openraft::Vote<u64>> for raft_proto::Vote {
    fn from(v: openraft::Vote<u64>) -> Self {
        Self {
            term: v.leader_id.term,
            node_id: v.leader_id.node_id,
            committed: v.committed,
        }
    }
}

impl From<raft_proto::Vote> for openraft::Vote<u64> {
    fn from(v: raft_proto::Vote) -> Self {
        let mut vote = openraft::Vote::new(v.term, v.node_id);
        vote.committed = v.committed;
        vote
    }
}

impl From<openraft::LogId<u64>> for raft_proto::LogId {
    fn from(id: openraft::LogId<u64>) -> Self {
        Self {
            term: id.leader_id.term,
            node_id: id.leader_id.node_id,
            index: id.index,
        }
    }
}

impl From<raft_proto::LogId> for openraft::LogId<u64> {
    fn from(id: raft_proto::LogId) -> Self {
        openraft::LogId::new(CommittedLeaderId::new(id.term, id.node_id), id.index)
    }
}

// --- 3. gRPC Server Implementation ---

pub struct ApexRaftServer {
    raft: Arc<openraft::Raft<ApexRaftTypeConfig>>,
}

impl ApexRaftServer {
    pub fn new(raft: Arc<openraft::Raft<ApexRaftTypeConfig>>) -> Self {
        Self { raft }
    }

    pub fn into_grpc_service(self) -> raft_proto::raft_service_server::RaftServiceServer<Self> {
        raft_proto::raft_service_server::RaftServiceServer::new(self)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    }
}

#[tonic::async_trait]
impl RaftService for ApexRaftServer {
    async fn append_entries(
        &self,
        request: Request<raft_proto::AppendEntriesRequest>,
    ) -> Result<Response<raft_proto::AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        let raft_req = AppendEntriesRequest {
            vote: req
                .vote
                .map(Into::into)
                .ok_or_else(|| Status::invalid_argument("missing vote"))?,
            prev_log_id: req.prev_log_id.map(Into::into),
            entries: {
                let mut decoded = Vec::with_capacity(req.entries.len());
                for e in req.entries {
                    let log_id = e
                        .log_id
                        .map(Into::into)
                        .ok_or_else(|| Status::invalid_argument("missing log_id"))?;
                    let payload = match raft_proto::EntryType::try_from(e.entry_type) {
                        Ok(raft_proto::EntryType::Blank) => openraft::EntryPayload::Blank,
                        Ok(raft_proto::EntryType::Membership) => {
                            let m = bincode::deserialize(&e.data).map_err(|err| {
                                Status::internal(format!("membership decode error: {err}"))
                            })?;
                            openraft::EntryPayload::Membership(m)
                        }
                        _ => openraft::EntryPayload::Normal(e.data),
                    };
                    decoded.push(openraft::Entry { log_id, payload });
                }
                decoded
            },
            leader_commit: req.leader_commit.map(Into::into),
        };

        let resp = self
            .raft
            .append_entries(raft_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (success, conflict_log_id) = match resp {
            AppendEntriesResponse::Success => (true, None),
            AppendEntriesResponse::PartialSuccess(id) => (true, id),
            AppendEntriesResponse::Conflict | AppendEntriesResponse::HigherVote(_) => (false, None),
        };

        let current_metrics = self.raft.metrics().borrow().clone();

        Ok(Response::new(raft_proto::AppendEntriesResponse {
            vote: Some(current_metrics.vote.into()),
            success,
            conflict_log_id: conflict_log_id.map(Into::into),
        }))
    }

    async fn request_vote(
        &self,
        request: Request<raft_proto::RequestVoteRequest>,
    ) -> Result<Response<raft_proto::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let raft_req = VoteRequest {
            vote: req
                .vote
                .map(Into::into)
                .ok_or_else(|| Status::invalid_argument("missing vote"))?,
            last_log_id: req.last_log_id.map(Into::into),
        };

        let resp = self
            .raft
            .vote(raft_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(raft_proto::RequestVoteResponse {
            vote: Some(resp.vote.into()),
            vote_granted: resp.vote_granted,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<raft_proto::InstallSnapshotRequest>,
    ) -> Result<Response<raft_proto::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let vote = req
            .vote
            .map(Into::into)
            .ok_or_else(|| Status::invalid_argument("missing vote"))?;
        let last_log_id = req
            .last_log_id
            .map(Into::into)
            .ok_or_else(|| Status::invalid_argument("missing last_log_id"))?;

        let raft_req = InstallSnapshotRequest {
            vote,
            meta: openraft::SnapshotMeta {
                last_log_id: Some(last_log_id),
                last_membership: StoredMembership::new(
                    Some(last_log_id),
                    openraft::Membership::new(vec![std::collections::BTreeSet::new()], None),
                ),
                snapshot_id: "snapshot".to_string(),
            },
            offset: 0,
            data: req.data,
            done: req.done,
        };

        let resp = self
            .raft
            .install_snapshot(raft_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(raft_proto::InstallSnapshotResponse {
            vote: Some(resp.vote.into()),
        }))
    }
}

// --- 4. gRPC Network Client Implementation ---

#[derive(Clone)]
pub struct ApexRaftNetworkConnection {
    target: u64,
    client: RaftServiceClient<Channel>,
}

pub struct ApexRaftNetworkFactory {}

impl RaftNetworkFactory<ApexRaftTypeConfig> for ApexRaftNetworkFactory {
    type Network = ApexRaftNetworkConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let addr = format!("http://{}", node.addr);
        let endpoint = tonic::transport::Endpoint::from_shared(addr)
            .expect("Invalid URI")
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(2));

        ApexRaftNetworkConnection {
            target,
            client: RaftServiceClient::new(endpoint.connect_lazy()),
        }
    }
}

impl RaftNetwork<ApexRaftTypeConfig> for ApexRaftNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<ApexRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client.clone();

        let proto_req = raft_proto::AppendEntriesRequest {
            vote: Some(rpc.vote.into()),
            prev_log_id: rpc.prev_log_id.map(Into::into),
            entries: rpc
                .entries
                .into_iter()
                .map(|e| {
                    let (data, entry_type) = match e.payload {
                        openraft::EntryPayload::Normal(d) => {
                            (d, raft_proto::EntryType::Normal as i32)
                        }
                        openraft::EntryPayload::Blank => {
                            (vec![], raft_proto::EntryType::Blank as i32)
                        }
                        openraft::EntryPayload::Membership(ref m) => (
                            bincode::serialize(m).unwrap(),
                            raft_proto::EntryType::Membership as i32,
                        ),
                    };
                    raft_proto::LogEntry {
                        log_id: Some(e.log_id.into()),
                        data,
                        entry_type,
                    }
                })
                .collect(),
            leader_commit: rpc.leader_commit.map(Into::into),
        };

        let resp = client
            .append_entries(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        if resp.success {
            if let Some(id) = resp.conflict_log_id {
                Ok(AppendEntriesResponse::PartialSuccess(Some(id.into())))
            } else {
                Ok(AppendEntriesResponse::Success)
            }
        } else if let Some(vote) = resp.vote {
            Ok(AppendEntriesResponse::HigherVote(vote.into()))
        } else {
            Ok(AppendEntriesResponse::Conflict)
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<ApexRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let mut client = self.client.clone();
        let proto_req = raft_proto::InstallSnapshotRequest {
            vote: Some(rpc.vote.into()),
            last_log_id: rpc.meta.last_log_id.map(Into::into),
            data: rpc.data,
            offset: rpc.offset,
            done: rpc.done,
        };

        let resp = client
            .install_snapshot(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?
            .into_inner();

        Ok(InstallSnapshotResponse {
            vote: resp
                .vote
                .map_or_else(|| openraft::Vote::new(0, self.target), Into::into),
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client.clone();
        let proto_req = raft_proto::RequestVoteRequest {
            vote: Some(rpc.vote.into()),
            last_log_id: rpc.last_log_id.map(Into::into),
        };

        let response = client
            .request_vote(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();

        Ok(VoteResponse {
            vote: resp
                .vote
                .map_or_else(|| openraft::Vote::new(0, self.target), Into::into),
            vote_granted: resp.vote_granted,
            last_log_id: rpc.last_log_id,
        })
    }
}
