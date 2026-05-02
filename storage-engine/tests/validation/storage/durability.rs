use crate::validation::common::harness::ClusterHarness;
use anyhow::Result;
use std::time::Duration;

/// Bootstrap cluster, expansion logic as before.
async fn bootstrap_cluster(harness: &ClusterHarness) -> Result<u64> {
    let node1 = &harness.live[&1].node;
    let mut members = std::collections::BTreeMap::new();
    members.insert(
        1u64,
        openraft::BasicNode {
            addr: harness.persistence[&1].bind_addr.clone(),
        },
    );
    node1.raft.initialize(members).await?;
    let leader = harness.wait_for_leader(Duration::from_secs(10)).await?;

    node1
        .raft
        .add_learner(
            2,
            openraft::BasicNode {
                addr: harness.persistence[&2].bind_addr.clone(),
            },
            true,
        )
        .await?;
    node1
        .raft
        .add_learner(
            3,
            openraft::BasicNode {
                addr: harness.persistence[&3].bind_addr.clone(),
            },
            true,
        )
        .await?;

    let mut voter_ids = std::collections::BTreeSet::new();
    voter_ids.insert(1);
    voter_ids.insert(2);
    voter_ids.insert(3);
    node1.raft.change_membership(voter_ids, true).await?;
    harness.wait_for_leader(Duration::from_secs(10)).await?;
    Ok(leader)
}

/// DURABILITY INVARIANT: Data must survive full cluster restart and recover from snapshots.
#[tokio::test]
async fn snapshot_and_restart_durability() -> Result<()> {
    let mut harness = ClusterHarness::new(3).await?;
    let leader = bootstrap_cluster(&harness).await?;

    eprintln!("[test] Writing 1500 entries to trigger snapshot...");
    for i in 0..1500 {
        let key = format!("key_{}", i).into_bytes();
        let val = format!("val_{}", i).into_bytes();
        harness.put(leader, key, val).await?;
        if i % 100 == 0 {
            eprintln!("[test] Written {} entries...", i);
        }
    }

    let metrics = harness.live[&leader].node.raft.metrics().borrow().clone();
    let last_index = metrics.last_log_index.unwrap();
    eprintln!(
        "[test] All entries written. Last index: {}. Waiting for convergence...",
        last_index
    );

    for &id in harness.live.keys() {
        harness
            .wait_for_applied(id, last_index, Duration::from_secs(15))
            .await?;
    }
    eprintln!("[test] Convergence reached.");

    // Verify snapshot was triggered
    let m_leader = harness.live[&leader].node.raft.metrics().borrow().clone();
    assert!(
        m_leader.snapshot.is_some(),
        "Snapshot should have been triggered by 1500 entries"
    );
    eprintln!(
        "[test] Snapshot confirmed at index: {:?}",
        m_leader.snapshot
    );

    eprintln!("[test] Performing full cluster restart...");
    for id in 1..=3 {
        harness.restart_node(id).await?;
    }

    eprintln!("[test] Waiting for cluster to stabilize after restart...");
    let new_leader = harness.wait_for_leader(Duration::from_secs(20)).await?;
    eprintln!("[test] Cluster back online. New leader: {}", new_leader);

    eprintln!("[test] Verifying data integrity...");
    for i in (0..1500).step_by(100) {
        // Check every 100th key to be efficient
        let key = format!("key_{}", i).into_bytes();
        let expected_val = format!("val_{}", i).into_bytes();

        let val = harness.get(new_leader, &key).await?;
        assert_eq!(
            val,
            Some(expected_val),
            "Data mismatch after restart at index {}",
            i
        );
    }

    eprintln!("[test] Durability and snapshot recovery verified.");
    Ok(())
}
