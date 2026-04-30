use openraft::{
    BasicNode, CommittedLeaderId, LogId, RaftTypeConfig,
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

// --- 2. gRPC Server Implementation ---

/// The gRPC server that receives Raft RPCs and forwards them to the local Raft instance.
pub struct ApexRaftServer {
    raft: Arc<openraft::Raft<ApexRaftTypeConfig>>,
}

impl ApexRaftServer {
    pub fn new(raft: Arc<openraft::Raft<ApexRaftTypeConfig>>) -> Self {
        Self { raft }
    }

    /// Converts this server into a gRPC service that can be served.
    /// Enables production-grade compression settings (Gzip).
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

        let leader_id = req
            .leader_id
            .parse()
            .map_err(|_| Status::invalid_argument("Invalid leader_id"))?;

        let raft_req = AppendEntriesRequest {
            vote: openraft::Vote::new(req.term, leader_id),
            prev_log_id: if req.prev_log_index > 0 {
                Some(LogId::new(
                    CommittedLeaderId::new(req.prev_log_term, leader_id),
                    req.prev_log_index,
                ))
            } else {
                None
            },
            entries: req
                .entries
                .into_iter()
                .map(|e| openraft::Entry {
                    log_id: LogId::new(CommittedLeaderId::new(e.term, leader_id), e.index),
                    payload: openraft::EntryPayload::Normal(e.data),
                })
                .collect(),
            leader_commit: if req.leader_commit > 0 {
                Some(LogId::new(
                    CommittedLeaderId::new(req.term, leader_id),
                    req.leader_commit,
                ))
            } else {
                None
            },
        };

        let resp = self
            .raft
            .append_entries(raft_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match resp {
            AppendEntriesResponse::Success => {
                Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: 0,
                    higher_vote: None,
                    is_conflict: false,
                }))
            }
            AppendEntriesResponse::PartialSuccess(log_id) => {
                Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: log_id.map_or(0, |id| id.index),
                    higher_vote: None,
                    is_conflict: false,
                }))
            }
            AppendEntriesResponse::Conflict => {
                Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: req.term,
                    success: false,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: 0,
                    higher_vote: None,
                    is_conflict: true,
                }))
            }
            AppendEntriesResponse::HigherVote(vote) => {
                Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: vote.leader_id.term,
                    success: false,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: 0,
                    higher_vote: Some(raft_proto::HigherVote {
                        term: vote.leader_id.term,
                        node_id: vote.leader_id.node_id,
                    }),
                    is_conflict: false,
                }))
            }
        }
    }

    async fn request_vote(
        &self,
        request: Request<raft_proto::RequestVoteRequest>,
    ) -> Result<Response<raft_proto::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let candidate_id = req
            .candidate_id
            .parse()
            .map_err(|_| Status::invalid_argument("Invalid candidate_id"))?;

        let raft_req = VoteRequest {
            vote: openraft::Vote::new(req.term, candidate_id),
            last_log_id: if req.last_log_index > 0 {
                Some(LogId::new(
                    CommittedLeaderId::new(req.last_log_term, candidate_id),
                    req.last_log_index,
                ))
            } else {
                None
            },
        };

        let resp = self
            .raft
            .vote(raft_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(raft_proto::RequestVoteResponse {
            term: resp.vote.leader_id.term,
            vote_granted: resp.vote_granted,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<raft_proto::InstallSnapshotRequest>,
    ) -> Result<Response<raft_proto::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let leader_id = req
            .leader_id
            .parse()
            .map_err(|_| Status::invalid_argument("Invalid leader_id"))?;

        let raft_req = InstallSnapshotRequest {
            vote: openraft::Vote::new(req.term, leader_id),
            meta: openraft::SnapshotMeta {
                last_log_id: Some(LogId::new(
                    CommittedLeaderId::new(req.last_included_term, leader_id),
                    req.last_included_index,
                )),
                last_membership: openraft::StoredMembership::new(
                    None,
                    openraft::Membership::new(
                        vec![std::collections::BTreeSet::from_iter(vec![leader_id])],
                        None,
                    ),
                ),
                snapshot_id: format!("{}-{}-{}", req.term, leader_id, req.offset),
            },
            offset: req.offset,
            data: req.data,
            done: req.done,
        };

        let resp = self
            .raft
            .install_snapshot(raft_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(raft_proto::InstallSnapshotResponse {
            term: resp.vote.leader_id.term,
        }))
    }
}

// --- 3. gRPC Network Client Implementation ---

#[derive(Clone)]
pub struct ApexRaftNetworkConnection {
    target: u64,
    #[allow(dead_code)]
    target_node: BasicNode,
    client: RaftServiceClient<Channel>,
}

pub struct ApexRaftNetworkFactory {}

impl RaftNetworkFactory<ApexRaftTypeConfig> for ApexRaftNetworkFactory {
    type Network = ApexRaftNetworkConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let addr = format!("http://{}", node.addr);
        let endpoint = tonic::transport::Endpoint::from_shared(addr)
            .expect("Invalid URI")
            .timeout(Duration::from_secs(1))
            .connect_timeout(Duration::from_millis(500))
            .tcp_keepalive(Some(Duration::from_secs(15)))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_while_idle(true);

        let channel = endpoint.connect_lazy();
        let client = RaftServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);

        ApexRaftNetworkConnection {
            target,
            target_node: node.clone(),
            client,
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
            term: rpc.vote.leader_id.term,
            leader_id: rpc.vote.leader_id.node_id.to_string(),
            prev_log_index: rpc.prev_log_id.map_or(0, |id| id.index),
            prev_log_term: rpc.prev_log_id.map_or(0, |id| id.leader_id.term),
            entries: rpc
                .entries
                .into_iter()
                .map(|e| raft_proto::LogEntry {
                    index: e.log_id.index,
                    term: e.log_id.leader_id.term,
                    data: match e.payload {
                        openraft::EntryPayload::Normal(data) => data,
                        _ => vec![],
                    },
                })
                .collect(),
            leader_commit: rpc.leader_commit.map_or(0, |id| id.index),
        };

        let response = client
            .append_entries(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();

        if resp.is_conflict {
            return Ok(AppendEntriesResponse::Conflict);
        }

        if let Some(ref higher) = resp.higher_vote {
            return Ok(AppendEntriesResponse::HigherVote(openraft::Vote::new(
                higher.term,
                higher.node_id,
            )));
        }

        if resp.success {
            if resp.last_log_index > 0 {
                Ok(AppendEntriesResponse::PartialSuccess(Some(LogId::new(
                    CommittedLeaderId::new(resp.term, self.target),
                    resp.last_log_index,
                ))))
            } else {
                Ok(AppendEntriesResponse::Success)
            }
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
            term: rpc.vote.leader_id.term,
            leader_id: rpc.vote.leader_id.node_id.to_string(),
            last_included_index: rpc.meta.last_log_id.map_or(0, |id| id.index),
            last_included_term: rpc
                .meta
                .last_log_id
                .map_or(0, |id| id.leader_id.term),
            offset: rpc.offset,
            data: rpc.data,
            done: rpc.done,
        };

        let response = client
            .install_snapshot(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();

        Ok(InstallSnapshotResponse {
            vote: openraft::Vote::new(resp.term, self.target),
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let mut client = self.client.clone();

        let proto_req = raft_proto::RequestVoteRequest {
            term: rpc.vote.leader_id.term,
            candidate_id: rpc.vote.leader_id.node_id.to_string(),
            last_log_index: rpc.last_log_id.map_or(0, |id| id.index),
            last_log_term: rpc.last_log_id.map_or(0, |id| id.leader_id.term),
        };

        let response = client
            .request_vote(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();

        Ok(VoteResponse {
            vote: openraft::Vote::new(resp.term, self.target),
            vote_granted: resp.vote_granted,
            last_log_id: rpc.last_log_id,
        })
    }
}
