use std::sync::Arc;
use tonic::{transport::Channel, Request, Response, Status};
use openraft::{
    error::{NetworkError, RPCError, RaftError, InstallSnapshotError},
    network::{RaftNetwork, RaftNetworkFactory, RPCOption},
    BasicNode, RaftTypeConfig, LogId, CommittedLeaderId,
};
use std::fmt;

// Generated gRPC code from proto/raft.proto
pub mod raft_proto {
    tonic::include_proto!("raft");
}

use raft_proto::raft_service_client::RaftServiceClient;
use raft_proto::raft_service_server::RaftService;

// --- 1. Raft Type Configuration ---

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize, Hash)]
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
pub struct ApexRaftServer
{
    raft: Arc<openraft::Raft<ApexRaftTypeConfig>>,
}

impl ApexRaftServer
{
    pub fn new(raft: Arc<openraft::Raft<ApexRaftTypeConfig>>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftService for ApexRaftServer
{
    async fn append_entries(
        &self,
        request: Request<raft_proto::AppendEntriesRequest>,
    ) -> Result<Response<raft_proto::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        
        let leader_id = req.leader_id.parse().map_err(|_| Status::invalid_argument("Invalid leader_id"))?;
        
        let raft_req = openraft::raft::AppendEntriesRequest {
            vote: openraft::Vote::new(req.term, leader_id),
            prev_log_id: if req.prev_log_index > 0 {
                Some(LogId::new(CommittedLeaderId::new(req.prev_log_term, leader_id), req.prev_log_index))
            } else {
                None
            },
            entries: req.entries.into_iter().map(|e| {
                openraft::Entry {
                    log_id: LogId::new(CommittedLeaderId::new(e.term, leader_id), e.index),
                    payload: openraft::EntryPayload::Normal(e.data),
                }
            }).collect(),
            leader_commit: if req.leader_commit > 0 {
                Some(LogId::new(CommittedLeaderId::new(req.term, leader_id), req.leader_commit))
            } else {
                None
            },
        };

        let resp = self.raft.append_entries(raft_req).await.map_err(|e| Status::internal(e.to_string()))?;
        
        match resp {
            openraft::raft::AppendEntriesResponse::Success => {
                Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: 0,
                }))
            }
            openraft::raft::AppendEntriesResponse::PartialSuccess(log_id) => {
                 Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: log_id.map(|id| id.index).unwrap_or(0),
                }))
            }
            openraft::raft::AppendEntriesResponse::Conflict => {
                 Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: req.term,
                    success: false,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: 0,
                }))
            }
            openraft::raft::AppendEntriesResponse::HigherVote(vote) => {
                 Ok(Response::new(raft_proto::AppendEntriesResponse {
                    term: vote.leader_id.term,
                    success: false,
                    conflict_index: 0,
                    conflict_term: 0,
                    last_log_index: 0,
                }))
            }
        }
    }

    async fn request_vote(
        &self,
        request: Request<raft_proto::RequestVoteRequest>,
    ) -> Result<Response<raft_proto::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let candidate_id = req.candidate_id.parse().map_err(|_| Status::invalid_argument("Invalid candidate_id"))?;
        
        let raft_req = openraft::raft::VoteRequest {
            vote: openraft::Vote::new(req.term, candidate_id),
            last_log_id: if req.last_log_index > 0 {
                Some(LogId::new(CommittedLeaderId::new(req.last_log_term, candidate_id), req.last_log_index))
            } else {
                None
            },
        };

        let resp = self.raft.vote(raft_req).await.map_err(|e| Status::internal(e.to_string()))?;
        
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
        let leader_id = req.leader_id.parse().map_err(|_| Status::invalid_argument("Invalid leader_id"))?;
        
        let raft_req = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote::new(req.term, leader_id),
            meta: openraft::SnapshotMeta {
                last_log_id: Some(LogId::new(CommittedLeaderId::new(req.last_included_term, leader_id), req.last_included_index)),
                last_membership: openraft::StoredMembership::new(None, openraft::Membership::new(vec![std::collections::BTreeSet::from_iter(vec![leader_id])], None)),
                snapshot_id: format!("{}-{}-{}", req.term, leader_id, req.offset),
            },
            offset: req.offset,
            data: req.data,
            done: req.done,
        };

        let resp = self.raft.install_snapshot(raft_req).await.map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(raft_proto::InstallSnapshotResponse {
            term: resp.vote.leader_id.term,
        }))
    }
}

// --- 3. gRPC Network Client Implementation ---

#[derive(Clone, Default)]
pub struct ApexRaftNetworkFactory;

impl RaftNetworkFactory<ApexRaftTypeConfig> for ApexRaftNetworkFactory {
    type Network = ApexRaftNetworkConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let addr = format!("http://{}", node.addr);
        let client = RaftServiceClient::connect(addr).await.ok();
        
        ApexRaftNetworkConnection {
            target,
            client,
        }
    }
}

pub struct ApexRaftNetworkConnection {
    target: u64,
    client: Option<RaftServiceClient<Channel>>,
}

impl RaftNetwork<ApexRaftTypeConfig> for ApexRaftNetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<ApexRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let client = self.client.as_mut().ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::NotConnected, "gRPC client not connected")))
        })?;

        let proto_req = raft_proto::AppendEntriesRequest {
            term: rpc.vote.leader_id.term,
            leader_id: rpc.vote.leader_id.node_id.to_string(),
            prev_log_index: rpc.prev_log_id.map(|id| id.index).unwrap_or(0),
            prev_log_term: rpc.prev_log_id.map(|id| id.leader_id.term).unwrap_or(0),
            entries: rpc.entries.into_iter().map(|e| {
                raft_proto::LogEntry {
                    index: e.log_id.index,
                    term: e.log_id.leader_id.term,
                    data: match e.payload {
                        openraft::EntryPayload::Normal(data) => data,
                        _ => vec![],
                    },
                }
            }).collect(),
            leader_commit: rpc.leader_commit.map(|id| id.index).unwrap_or(0),
        };

        let response = client.append_entries(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();
        
        if resp.success {
            if resp.last_log_index > 0 {
                 // Map to PartialSuccess if we have a log index
                 Ok(openraft::raft::AppendEntriesResponse::PartialSuccess(Some(LogId::new(
                    CommittedLeaderId::new(resp.term, self.target),
                    resp.last_log_index
                ))))
            } else {
                Ok(openraft::raft::AppendEntriesResponse::Success)
            }
        } else {
            if resp.term > rpc.vote.leader_id.term {
                Ok(openraft::raft::AppendEntriesResponse::HigherVote(openraft::Vote::new(resp.term, self.target)))
            } else {
                Ok(openraft::raft::AppendEntriesResponse::Conflict)
            }
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<ApexRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>> {
        let client = self.client.as_mut().ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::NotConnected, "gRPC client not connected")))
        })?;

        let proto_req = raft_proto::InstallSnapshotRequest {
            term: rpc.vote.leader_id.term,
            leader_id: rpc.vote.leader_id.node_id.to_string(),
            last_included_index: rpc.meta.last_log_id.map(|id| id.index).unwrap_or(0),
            last_included_term: rpc.meta.last_log_id.map(|id| id.leader_id.term).unwrap_or(0),
            offset: rpc.offset,
            data: rpc.data,
            done: rpc.done,
        };

        let response = client.install_snapshot(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();
        
        Ok(openraft::raft::InstallSnapshotResponse {
            vote: openraft::Vote::new(resp.term, self.target),
        })
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let client = self.client.as_mut().ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::new(std::io::ErrorKind::NotConnected, "gRPC client not connected")))
        })?;

        let proto_req = raft_proto::RequestVoteRequest {
            term: rpc.vote.leader_id.term,
            candidate_id: rpc.vote.leader_id.node_id.to_string(),
            last_log_index: rpc.last_log_id.map(|id| id.index).unwrap_or(0),
            last_log_term: rpc.last_log_id.map(|id| id.leader_id.term).unwrap_or(0),
        };

        let response = client.request_vote(proto_req)
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let resp = response.into_inner();
        
        Ok(openraft::raft::VoteResponse {
            vote: openraft::Vote::new(resp.term, self.target),
            vote_granted: resp.vote_granted,
            last_log_id: rpc.last_log_id,
        })
    }
}
