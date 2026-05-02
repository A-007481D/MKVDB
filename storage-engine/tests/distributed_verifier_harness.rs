use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::BasicNode;
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use storage_engine::network::ApexNode;
use tempfile::TempDir;

struct TestNode {
    id: u64,
    raft_addr: String,
    _dir: TempDir,
    node: Arc<ApexNode>,
}

impl TestNode {
    fn data_key(key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + key.len());
        out.extend_from_slice(b"data:");
        out.extend_from_slice(key);
        out
    }
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let addr = listener.local_addr().expect("read local addr");
    format!("127.0.0.1:{}", addr.port())
}

async fn start_node(id: u64, raft_addr: String) -> TestNode {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ApexConfig::default().with_sync_policy(SyncPolicy::EveryWrite);
    let engine = ApexEngine::open_with_config(dir.path(), config).expect("open engine");
    let node = Arc::new(
        ApexNode::start(id, &raft_addr, engine)
            .await
            .expect("start raft node"),
    );
    TestNode {
        id,
        raft_addr,
        _dir: dir,
        node,
    }
}

async fn bootstrap_cluster(nodes: &[TestNode]) {
    // 1. Initialize Node 1 as a single-node cluster
    let mut members = BTreeMap::new();
    members.insert(
        nodes[0].id,
        BasicNode {
            addr: nodes[0].raft_addr.clone(),
        },
    );

    println!("Initializing Node 1...");
    nodes[0]
        .node
        .raft
        .initialize(members)
        .await
        .expect("initialize failed");

    // 2. Wait for Node 1 to stabilize as Leader
    nodes[0]
        .node
        .await_cluster_stable(Duration::from_secs(10))
        .await
        .expect("Node 1 failed to stabilize");

    // 3. Write "Seal" entry to ensure vote is committed
    nodes[0]
        .node
        .write_put(b"bootstrap".to_vec(), b"leader-stable".to_vec())
        .await
        .expect("Seal write failed");

    // 4. Incrementally add Node 2 and Node 3 as Learners
    for i in 1..nodes.len() {
        let n = &nodes[i];
        println!("Adding Node {} as learner...", n.id);

        nodes[0]
            .node
            .safe_add_learner(
                n.id,
                BasicNode {
                    addr: n.raft_addr.clone(),
                },
                true,
            )
            .await
            .expect("safe_add_learner failed");

        // Wait for replication to catch up
        let node_id = n.id;
        nodes[0]
            .node
            .raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                move |m| {
                    m.replication.as_ref().is_some_and(|r| {
                        r.get(&node_id)
                            .and_then(|matched| matched.as_ref().map(|id| id.index >= 1))
                            .unwrap_or(false)
                    })
                },
                "learner catchup",
            )
            .await
            .expect("learner catchup timed out");
    }

    // 5. Promote to full 3-node cluster
    println!("Promoting cluster to 3 nodes...");
    let membership = BTreeSet::from_iter(nodes.iter().map(|n| n.id));
    nodes[0]
        .node
        .safe_change_membership(membership, false)
        .await
        .expect("safe_change_membership failed");

    // 6. Final stabilization
    nodes[0]
        .node
        .await_cluster_stable(Duration::from_secs(10))
        .await
        .expect("Final stabilization failed");
}

async fn find_leader_idx(nodes: &[TestNode]) -> Option<usize> {
    for (i, n) in nodes.iter().enumerate() {
        let key = format!("leader-probe-{}", n.id).into_bytes();
        let value = b"ok".to_vec();
        let res = tokio::time::timeout(Duration::from_secs(2), n.node.write_put(key, value)).await;
        if let Ok(Ok(())) = res {
            return Some(i);
        }
    }
    None
}

fn local_get(node: &TestNode, key: &[u8]) -> Option<Bytes> {
    node.node
        .engine
        .get(&TestNode::data_key(key))
        .expect("engine get failed")
}

#[tokio::test]
async fn full_distributed_verifier_harness() {
    let _ = tracing_subscriber::fmt::try_init();

    let n1 = start_node(1, free_addr()).await;
    let n2 = start_node(2, free_addr()).await;
    let n3 = start_node(3, free_addr()).await;
    let nodes = vec![n1, n2, n3];

    bootstrap_cluster(&nodes).await;

    // Test 1: Convergence
    println!("Testing write convergence...");
    let leader_idx = find_leader_idx(&nodes)
        .await
        .expect("no stable leader found");
    let leader = &nodes[leader_idx];
    leader
        .node
        .write_put(b"stress".to_vec(), b"start".to_vec())
        .await
        .expect("stress write failed");

    tokio::time::sleep(Duration::from_secs(2)).await;

    for n in &nodes {
        let got = local_get(n, b"stress");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"start")),
            "Node {} diverged",
            n.id
        );
    }

    // Test 2: Crash Failover
    println!("Shutting down leader {}...", leader.id);
    tokio::time::timeout(Duration::from_secs(5), leader.node.raft.shutdown())
        .await
        .expect("shutdown timeout")
        .expect("shutdown failed");

    tokio::time::sleep(Duration::from_secs(10)).await;

    let survivors: Vec<&TestNode> = nodes.iter().filter(|n| n.id != leader.id).collect();
    let mut wrote = false;
    for n in &survivors {
        if n.node
            .write_put(b"failover".to_vec(), b"ok".to_vec())
            .await
            .is_ok()
        {
            wrote = true;
            break;
        }
    }
    assert!(wrote, "Failover election failed");

    tokio::time::sleep(Duration::from_secs(2)).await;
    for n in &survivors {
        let got = local_get(n, b"failover");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"ok")),
            "Survivor Node {} diverged",
            n.id
        );
    }
}
