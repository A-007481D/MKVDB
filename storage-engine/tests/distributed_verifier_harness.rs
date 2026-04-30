use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::{BasicNode, Entry};
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use storage_engine::network::{ApexNode, WriteRedirect};
use storage_engine::network::grpc::ApexRaftTypeConfig;
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
    let mut members = BTreeMap::new();
    members.insert(
        nodes[0].id,
        BasicNode {
            addr: nodes[0].raft_addr.clone(),
        },
    );
    tokio::time::timeout(Duration::from_secs(5), nodes[0].node.raft.initialize(members))
        .await
        .expect("initialize timed out")
        .expect("initialize failed");

    tokio::time::timeout(
        Duration::from_secs(8),
        nodes[0]
            .node
            .await_config_change_ready(Duration::from_secs(8)),
    )
    .await
    .expect("leader readiness barrier timeout")
    .expect("leader readiness barrier failed");
    tokio::time::timeout(Duration::from_secs(3), nodes[0].node.commit_noop_barrier())
        .await
        .expect("noop barrier timeout")
        .expect("noop barrier failed");
    tokio::time::timeout(
        Duration::from_secs(8),
        nodes[0]
            .node
            .await_config_change_ready(Duration::from_secs(8)),
    )
    .await
    .expect("leader post-noop readiness barrier timeout")
    .expect("leader post-noop readiness barrier failed");

    for n in &nodes[1..] {
        tokio::time::timeout(
            Duration::from_secs(5),
            nodes[0].node.raft.add_learner(
                n.id,
                BasicNode {
                    addr: n.raft_addr.clone(),
                },
                true,
            ),
        )
        .await
        .expect("add_learner timeout")
        .expect("add_learner failed");
    }

    let membership = BTreeSet::from_iter(nodes.iter().map(|n| n.id));
    tokio::time::timeout(
        Duration::from_secs(12),
        nodes[0].node.raft.change_membership(membership, false),
    )
    .await
    .expect("change_membership timeout")
    .expect("change_membership failed");
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

async fn log_fingerprint(node: &TestNode) -> HashMap<u64, (u64, u32)> {
    let mut out = HashMap::new();
    let mut stream = node
        .node
        .engine
        .scan(Bytes::from_static(b"log:"), Bytes::from_static(b"log:\xFF"))
        .expect("scan logs");
    use futures_util::StreamExt;
    while let Some(item) = stream.next().await {
        let (_, val) = item.expect("log scan item");
        let entry: Entry<ApexRaftTypeConfig> = bincode::deserialize(&val).expect("decode log");
        let payload_bytes = bincode::serialize(&entry.payload).expect("serialize payload");
        let payload_crc = crc32fast::hash(&payload_bytes);
        out.insert(entry.log_id.index, (entry.log_id.leader_id.term, payload_crc));
    }
    out
}

#[tokio::test]
async fn full_distributed_verifier_harness() {
    // PHASE 1: hard cluster integrity bootstrap
    let n1 = start_node(1, free_addr()).await;
    let n2 = start_node(2, free_addr()).await;
    let n3 = start_node(3, free_addr()).await;
    let nodes = vec![n1, n2, n3];

    bootstrap_cluster(&nodes).await;

    // Test 1: leader write should converge
    let leader_idx = find_leader_idx(&nodes).await.expect("no leader accepted write");
    let leader = &nodes[leader_idx];
    tokio::time::timeout(
        Duration::from_secs(3),
        leader.node.write_put(b"x".to_vec(), b"100".to_vec()),
    )
    .await
    .expect("leader write timeout")
    .expect("leader write failed");
    tokio::time::sleep(Duration::from_secs(1)).await;

    for n in &nodes {
        let got = local_get(n, b"x");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"100")),
            "state divergence on node {} for key x",
            n.id
        );
    }

    // Test 2: follower write safety
    let follower = nodes
        .iter()
        .find(|n| n.id != leader.id)
        .expect("no follower");
    let before = local_get(follower, b"y");
    let res = tokio::time::timeout(
        Duration::from_secs(3),
        follower.node.write_put(b"y".to_vec(), b"200".to_vec()),
    )
    .await
    .expect("follower write timeout");

    match res {
        Ok(()) => panic!("follower returned success on write"),
        Err(WriteRedirect::Leader(_)) | Err(WriteRedirect::UnknownLeader) => {}
    }
    let after = local_get(follower, b"y");
    assert_eq!(before, after, "follower storage mutated on rejected write");

    // Test 3: leader crash failover
    tokio::time::timeout(Duration::from_secs(5), leader.node.raft.shutdown())
        .await
        .expect("leader shutdown timeout")
        .expect("leader shutdown failed");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let survivors: Vec<&TestNode> = nodes.iter().filter(|n| n.id != leader.id).collect();
    let mut wrote = false;
    for n in &survivors {
        if tokio::time::timeout(
            Duration::from_secs(3),
            n.node.write_put(b"a".to_vec(), b"2".to_vec()),
        )
        .await
        .ok()
        .and_then(Result::ok)
        .is_some()
        {
            wrote = true;
            break;
        }
    }
    assert!(wrote, "no survivor accepted write after leader crash");
    tokio::time::sleep(Duration::from_secs(1)).await;

    for n in &survivors {
        let got = local_get(n, b"a");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"2")),
            "survivor node {} diverged after failover write",
            n.id
        );
    }

    // Test 4: log consistency (same index => same term/payload hash)
    let fp_a = log_fingerprint(survivors[0]).await;
    let fp_b = log_fingerprint(survivors[1]).await;
    for (idx, (term_a, hash_a)) in &fp_a {
        if let Some((term_b, hash_b)) = fp_b.get(idx) {
            assert_eq!(term_a, term_b, "term mismatch at index {idx}");
            assert_eq!(hash_a, hash_b, "command mismatch at index {idx}");
        }
    }

    // Test 5: randomized stress with repeated writes and leader disruptions
    for i in 0..100 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        let mut accepted = false;
        for n in &survivors {
            if tokio::time::timeout(
                Duration::from_secs(2),
                n.node.write_put(key.clone().into_bytes(), val.clone().into_bytes()),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .is_some()
            {
                accepted = true;
                break;
            }
        }
        assert!(accepted, "no node accepted randomized write {i}");
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    for i in 0..100 {
        let key = format!("k{i}");
        let expected = Bytes::from(format!("v{i}"));
        for n in &survivors {
            let got = local_get(n, key.as_bytes());
            assert_eq!(
                got,
                Some(expected.clone()),
                "randomized convergence failed for key {key} on node {}",
                n.id
            );
        }
    }
}
