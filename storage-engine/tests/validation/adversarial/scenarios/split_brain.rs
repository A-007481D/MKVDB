use std::time::Duration;
use anyhow::Result;
use crate::validation::adversarial::core::harness::AdversarialHarness;
use crate::validation::adversarial::core::fault_controller::FaultRule;
use crate::validation::adversarial::core::event_log::AdversarialEvent;

#[tokio::test]
async fn adversarial_split_brain_survival() -> Result<()> {
    let mut harness = AdversarialHarness::new(3, 12345).await?;
    harness.log.log(AdversarialEvent::TestInfo("Starting Split Brain Survival Test".to_string()));
    
    let mut members = std::collections::BTreeMap::new();
    for id in 1..=3 {
        members.insert(id, openraft::BasicNode { addr: harness.inner.persistence[&id].bind_addr.clone() });
    }
    
    harness.inner.live[&1].node.raft.initialize(members).await?;
    let leader = harness.inner.wait_for_leader(Duration::from_secs(10)).await?;
    harness.log.log(AdversarialEvent::TestInfo(format!("Initial leader: {}", leader)));

    // Verify membership
    let m = harness.inner.live[&leader].node.raft.metrics().borrow().clone();
    harness.log.log(AdversarialEvent::TestInfo(format!("Leader {} metrics: membership={:?}, state={:?}", leader, m.membership_config, m.state)));
    assert_eq!(m.membership_config.voter_ids().count(), 3, "Cluster must have 3 voters");

    // Partition leader from the rest
    let followers: Vec<u64> = (1..=3).filter(|&id| id != leader).collect();
    for &f in &followers {
        harness.faults.add_rule(FaultRule::Partition(leader, f));
    }
    harness.log.log(AdversarialEvent::TestInfo(format!("Partitioned leader {} from followers {:?}", leader, followers)));

    // Attempt to write to the partitioned leader
    harness.log.log(AdversarialEvent::TestInfo("Attempting write to partitioned leader (should fail to commit)".to_string()));
    let key = b"split_brain_key".to_vec();
    let val = b"val".to_vec();
    
    // This should timeout or return an error because it can't reach a majority
    let write_res = tokio::time::timeout(
        Duration::from_secs(2), 
        harness.inner.put(leader, key.clone(), val.clone())
    ).await;
    
    assert!(write_res.is_err() || write_res.unwrap().is_err(), "Write to partitioned leader must not succeed");
    harness.log.log(AdversarialEvent::TestInfo("Confirmed: Write to partitioned leader did not commit".to_string()));

    // Wait for the majority to elect a new leader
    harness.log.log(AdversarialEvent::TestInfo("Waiting for majority to elect new leader".to_string()));
    let new_leader = harness.inner.wait_for_new_leader(leader, Duration::from_secs(15)).await?;
    harness.log.log(AdversarialEvent::TestInfo(format!("New leader elected: {}", new_leader)));

    // Heal partition
    harness.faults.clear_rules();
    harness.log.log(AdversarialEvent::TestInfo("Partition healed".to_string()));

    // Verify consistency: New leader can write and all nodes eventually see it
    harness.log.log(AdversarialEvent::TestInfo("Verifying consistency across all nodes".to_string()));
    let key2 = b"post_heal_key".to_vec();
    let val2 = b"val2".to_vec();
    harness.inner.put(new_leader, key2.clone(), val2.clone()).await?;
    
    // Get the commit index from the leader to wait for it on others
    let leader_metrics = harness.inner.live[&new_leader].node.raft.metrics().borrow().clone();
    let commit_index = leader_metrics.last_log_index.unwrap_or(0); // At least this index must be applied
    
    for &id in harness.inner.live.keys() {
        harness.inner.wait_for_applied(id, commit_index, Duration::from_secs(10)).await?;
        let read_val = harness.inner.get_stale(id, &key2).await?;
        assert_eq!(read_val, Some(val2.clone()), "Node {} has inconsistent data", id);
    }
    
    harness.log.log(AdversarialEvent::TestInfo("Split brain survival verified".to_string()));
    Ok(())
}
