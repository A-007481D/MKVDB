use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::BasicNode;
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use storage_engine::network::{ApexNode, WriteRedirect};
use tempfile::tempdir;

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let addr = listener.local_addr().expect("read local addr");
    format!("127.0.0.1:{}", addr.port())
}

fn new_engine() -> Arc<ApexEngine> {
    let dir = tempdir().expect("tempdir");
    let config = ApexConfig::default().with_sync_policy(SyncPolicy::EveryWrite);
    ApexEngine::open_with_config(dir.path(), config).expect("open engine")
}

async fn bootstrap_single(node: &ApexNode, node_id: u64, raft_addr: &str) {
    let mut members = BTreeMap::new();
    members.insert(
        node_id,
        BasicNode {
            addr: raft_addr.to_string(),
        },
    );
    node.raft.initialize(members).await.expect("initialize");
}

#[tokio::test]
async fn leader_accepts_write_via_raft() {
    let leader_addr = free_addr();
    let leader = ApexNode::start(1, &leader_addr, new_engine())
        .await
        .expect("start leader");
    bootstrap_single(&leader, 1, &leader_addr).await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    let res = leader
        .write_put(b"phase1-key".to_vec(), b"phase1-val".to_vec())
        .await;
    assert!(res.is_ok(), "leader write should succeed, got: {res:?}");

    let got = leader.engine.get(b"data:phase1-key").expect("read");
    assert_eq!(got, Some(Bytes::from_static(b"phase1-val")));
}


    #[ignore = "Test bootstrap sequence needs await_config_change_ready barrier"]
    #[tokio::test]
async fn follower_returns_moved_on_write() {
    let leader_addr = free_addr();
    let follower_addr = free_addr();

    let leader = ApexNode::start(1, &leader_addr, new_engine())
        .await
        .expect("start leader");
    let follower = ApexNode::start(2, &follower_addr, new_engine())
        .await
        .expect("start follower");

    bootstrap_single(&leader, 1, &leader_addr).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::time::timeout(Duration::from_secs(3), leader.raft.ensure_linearizable())
        .await
        .expect("leader linearizable barrier timed out")
        .expect("leader linearizable barrier");
    tokio::time::timeout(
        Duration::from_secs(5),
        leader
        .raft
        .add_learner(
            2,
            BasicNode {
                addr: follower_addr.clone(),
            },
            true,
        ),
    )
    .await
    .expect("add learner timed out")
    .expect("add learner");
    tokio::time::timeout(
        Duration::from_secs(5),
        leader
        .raft
        .change_membership(BTreeSet::from([1_u64, 2_u64]), false),
    )
    .await
    .expect("change membership timed out")
    .expect("change membership");

    tokio::time::sleep(Duration::from_secs(1)).await;
    let res = follower
        .write_put(b"phase1-follow-key".to_vec(), b"phase1-follow-val".to_vec())
        .await;
    assert_eq!(res, Err(WriteRedirect::Leader(leader_addr)));
}


    #[ignore = "Test bootstrap sequence needs await_config_change_ready barrier"]
    #[tokio::test]
async fn writes_continue_after_leader_shutdown() {
    let addr1 = free_addr();
    let addr2 = free_addr();
    let addr3 = free_addr();

    let n1 = ApexNode::start(1, &addr1, new_engine()).await.expect("n1");
    let n2 = ApexNode::start(2, &addr2, new_engine()).await.expect("n2");
    let n3 = ApexNode::start(3, &addr3, new_engine()).await.expect("n3");

    bootstrap_single(&n1, 1, &addr1).await;
    n1.raft
        .add_learner(2, BasicNode { addr: addr2.clone() }, true)
        .await
        .expect("add learner 2");
    n1.raft
        .add_learner(3, BasicNode { addr: addr3.clone() }, true)
        .await
        .expect("add learner 3");
    n1.raft
        .change_membership(BTreeSet::from([1_u64, 2_u64, 3_u64]), false)
        .await
        .expect("membership 1,2,3");

    tokio::time::sleep(Duration::from_secs(1)).await;
    n1.raft.shutdown().await.expect("shutdown leader");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let r2 = n2.write_put(b"post-failover".to_vec(), b"ok".to_vec()).await;
    let r3 = n3.write_put(b"post-failover".to_vec(), b"ok".to_vec()).await;
    assert!(
        r2.is_ok() || r3.is_ok(),
        "one surviving node should accept write after failover; r2={r2:?} r3={r3:?}"
    );
}
