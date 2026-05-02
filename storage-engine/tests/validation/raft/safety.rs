use crate::validation::common::harness::ClusterHarness;
use anyhow::Result;
use openraft::{BasicNode, ServerState};
use std::collections::BTreeMap;
use std::time::Duration;

/// Bootstrap a single-leader cluster, then expand to 3 voters.
async fn bootstrap_cluster(harness: &ClusterHarness) -> Result<u64> {
    let node1 = &harness.live[&1].node;
    let addr1 = harness.persistence[&1].bind_addr.clone();
    let addr2 = harness.persistence[&2].bind_addr.clone();
    let addr3 = harness.persistence[&3].bind_addr.clone();

    eprintln!("[bootstrap] Initializing node 1 at {}", addr1);

    // Initialize node 1 as single-node cluster
    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode { addr: addr1 });
    node1.raft.initialize(members).await?;

    eprintln!("[bootstrap] Waiting for node 1 to become leader...");
    let leader = harness.wait_for_leader(Duration::from_secs(10)).await?;
    eprintln!("[bootstrap] Leader elected: {}", leader);

    // Stabilize before adding learners
    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("[bootstrap] Adding node 2 as learner at {}", addr2);
    node1
        .raft
        .add_learner(2, BasicNode { addr: addr2 }, true)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("[bootstrap] Adding node 3 as learner at {}", addr3);
    node1
        .raft
        .add_learner(3, BasicNode { addr: addr3 }, true)
        .await?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("[bootstrap] Promoting to 3-voter cluster");
    let mut voter_ids = std::collections::BTreeSet::new();
    voter_ids.insert(1);
    voter_ids.insert(2);
    voter_ids.insert(3);
    node1.raft.change_membership(voter_ids, true).await?;

    eprintln!("[bootstrap] Waiting for stable 3-node cluster...");
    let leader = harness.wait_for_leader(Duration::from_secs(10)).await?;
    eprintln!("[bootstrap] Cluster stable. Leader: {}", leader);

    Ok(leader)
}

#[tokio::test]
async fn election_safety() -> Result<()> {
    let mut harness = ClusterHarness::new(3).await?;
    eprintln!("[test] Harness created. Bootstrapping...");
    let initial_leader = bootstrap_cluster(&harness).await?;
    eprintln!(
        "[test] Bootstrap complete. Initial leader: {}",
        initial_leader
    );

    for cycle in 0..3 {
        let leader = harness.assert_single_leader()?;
        eprintln!("[test] Cycle {}: killing leader {}", cycle, leader);

        harness.kill_node(leader).await;

        eprintln!("[test] Cycle {}: waiting for new election...", cycle);
        let new_leader = harness.wait_for_leader(Duration::from_secs(15)).await?;
        assert_ne!(
            new_leader, leader,
            "New leader must differ from killed node"
        );
        eprintln!("[test] Cycle {}: new leader is {}", cycle, new_leader);

        harness.assert_single_leader()?;

        eprintln!("[test] Cycle {}: restarting old leader {}", cycle, leader);
        harness.restart_node(leader).await?;
        tokio::time::sleep(Duration::from_secs(3)).await;

        let final_leader = harness.assert_single_leader()?;
        eprintln!(
            "[test] Cycle {}: after restart, leader is {}",
            cycle, final_leader
        );
    }

    Ok(())
}

/// RAFT COMMIT SAFETY INVARIANT: A committed entry must survive failover.
///
/// Procedure:
/// 1. Bootstrap 3-node cluster
/// 2. Write key "commit_test" = "v1" to leader
/// 3. Wait for all nodes to apply the write
/// 4. Kill leader
/// 5. Wait for new leader election
/// 6. Verify "commit_test" == "v1" on new leader and surviving follower
#[tokio::test]
async fn commit_safety_under_failover() -> Result<()> {
    let mut harness = ClusterHarness::new(3).await?;
    let leader = bootstrap_cluster(&harness).await?;

    let key = b"commit_test".to_vec();
    let val = b"v1".to_vec();

    eprintln!("[test] Writing committed data to leader {}...", leader);
    harness.put(leader, key.clone(), val.clone()).await?;

    // Get the index of the write
    let metrics = harness.live[&leader].node.raft.metrics().borrow().clone();
    let commit_index = metrics.last_log_index.unwrap_or(0);
    eprintln!(
        "[test] Data written at index {}. Waiting for convergence...",
        commit_index
    );

    // Wait for all nodes to apply the write
    for &id in harness.live.keys() {
        harness
            .wait_for_applied(id, commit_index, Duration::from_secs(10))
            .await?;
    }
    eprintln!("[test] All nodes converged.");

    // Kill leader
    eprintln!("[test] Killing leader {}...", leader);
    harness.kill_node(leader).await;

    // Wait for new leader
    eprintln!("[test] Waiting for failover...");
    let new_leader = harness.wait_for_leader(Duration::from_secs(15)).await?;
    eprintln!("[test] New leader elected: {}", new_leader);

    // Verify data on new leader
    let read_val = harness.get(new_leader, &key).await?;
    assert_eq!(
        read_val,
        Some(val.clone()),
        "New leader must have committed data"
    );

    // Verify data on surviving follower
    let survivor = harness
        .live
        .keys()
        .cloned()
        .find(|&id| id != new_leader)
        .unwrap();
    let read_val_survivor = harness.get_stale(survivor, &key).await?;
    assert_eq!(
        read_val_survivor,
        Some(val),
        "Surviving follower must have committed data on disk"
    );

    eprintln!("[test] Commit safety verified.");
    Ok(())
}

/// RAFT SAFETY INVARIANT: A stale leader must not be able to commit writes.
///
/// Procedure:
/// 1. Bootstrap 3-node cluster
/// 2. Partition current leader (1) from {2, 3}
/// 3. Attempt write to leader 1 -> must not commit
/// 4. Verify nodes 2 & 3 elect a new leader
/// 5. Heal partition
/// 6. Verify leader 1 steps down and sees new leader
#[tokio::test]
async fn stale_leader_write_rejection() -> Result<()> {
    let harness = ClusterHarness::new(3).await?;
    let leader = bootstrap_cluster(&harness).await?;
    assert_eq!(leader, 1, "Initial leader should be 1");

    eprintln!("[test] Partitioning leader 1 from the rest of the cluster...");
    harness.faults.partition(1, 2);
    harness.faults.partition(1, 3);

    // Attempt write to partitioned leader
    let key = b"stale_test".to_vec();
    let val = b"v_stale".to_vec();

    eprintln!("[test] Attempting write to partitioned leader 1 (should timeout/fail)...");
    let put_future = harness.put(1, key.clone(), val.clone());

    // We expect this to NOT complete successfully within a short timeout
    match tokio::time::timeout(Duration::from_secs(3), put_future).await {
        Ok(Err(_)) => eprintln!("[test] Write correctly failed or is pending."),
        Err(_) => eprintln!("[test] Write timed out as expected (pending quorum)."),
        Ok(Ok(_)) => anyhow::bail!("Stale leader managed to commit write while partitioned!"),
    }

    eprintln!("[test] Waiting for nodes 2 & 3 to elect a new leader...");
    // Nodes 2 & 3 are still connected to each other, so they should form a majority
    let new_leader = harness
        .wait_for_new_leader(1, Duration::from_secs(15))
        .await?;
    assert_ne!(new_leader, 1, "New leader must be node 2 or 3");
    eprintln!("[test] New leader elected: {}", new_leader);

    eprintln!("[test] Healing partition...");
    harness.faults.heal_all();

    // Wait for node 1 to see the new leader and step down
    tokio::time::sleep(Duration::from_secs(5)).await;

    let m1 = harness.live[&1].node.raft.metrics().borrow().clone();
    assert_ne!(
        m1.state,
        ServerState::Leader,
        "Old leader must have stepped down"
    );
    eprintln!(
        "[test] Old leader stepped down. Current state: {:?}",
        m1.state
    );

    // Verify that a write to the NEW leader works
    let key2 = b"new_leader_test".to_vec();
    let val2 = b"v2".to_vec();
    harness.put(new_leader, key2.clone(), val2.clone()).await?;

    // Verify node 1 eventually gets the new data
    let commit_index = harness.live[&new_leader]
        .node
        .raft
        .metrics()
        .borrow()
        .last_log_index
        .unwrap();
    harness
        .wait_for_applied(1, commit_index, Duration::from_secs(10))
        .await?;

    let read_val = harness.get_stale(1, &key2).await?;
    assert_eq!(
        read_val,
        Some(val2),
        "Old leader must have replicated data from new leader"
    );

    eprintln!("[test] Stale leader rejection verified.");
    Ok(())
}
