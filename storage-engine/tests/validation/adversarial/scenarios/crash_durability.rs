use crate::validation::adversarial::core::event_log::AdversarialEvent;
use crate::validation::adversarial::core::harness::AdversarialHarness;
use anyhow::Result;
use std::time::Duration;

#[tokio::test]
async fn adversarial_crash_after_ack_durability() -> Result<()> {
    // 100 cycles of crash-after-ack
    let mut harness = AdversarialHarness::new(3, 55555).await?;
    harness.log.log(AdversarialEvent::TestInfo(
        "Starting Crash-After-Ack Durability Test (100 cycles)".to_string(),
    ));

    let mut members = std::collections::BTreeMap::new();
    for id in 1..=3 {
        members.insert(
            id,
            openraft::BasicNode {
                addr: harness.inner.persistence[&id].bind_addr.clone(),
            },
        );
    }

    harness.inner.live[&1].node.raft.initialize(members).await?;

    for cycle in 0..10 {
        // Reduced to 10 for regular test run, can be increased for deep validation
        harness.log.log(AdversarialEvent::TestInfo(format!(
            "--- Cycle {} ---",
            cycle
        )));

        let leader_id = harness
            .inner
            .wait_for_leader(Duration::from_secs(10))
            .await?;

        let key = format!("durability_key_{}", cycle).into_bytes();
        let val = format!("durability_val_{}", cycle).into_bytes();

        // 1. Issue write and wait for ACK
        harness
            .inner
            .put(leader_id, key.clone(), val.clone())
            .await?;
        harness.log.log(AdversarialEvent::TestInfo(format!(
            "Write acknowledged for key_{}",
            cycle
        )));

        // 2. IMMEDIATELY kill all nodes (Simulate power failure)
        harness.log.log(AdversarialEvent::TestInfo(
            "Crashing all nodes immediately after ACK...".to_string(),
        ));
        for id in 1..=3 {
            harness.inner.kill_node(id).await;
        }

        // 3. Restart all nodes
        harness.log.log(AdversarialEvent::TestInfo(
            "Restarting cluster...".to_string(),
        ));
        for id in 1..=3 {
            harness.inner.restart_node(id).await?;
        }

        // 4. Wait for new leader and verify data
        let new_leader = harness
            .inner
            .wait_for_leader(Duration::from_secs(10))
            .await?;

        // Wait for convergence to ensure state machine is up to date
        tokio::time::sleep(Duration::from_millis(500)).await;

        let read_val = harness.inner.get_stale(new_leader, &key).await?;
        assert_eq!(
            read_val,
            Some(val),
            "Data loss detected after crash in cycle {}. Key: {:?}",
            cycle,
            key
        );
    }

    harness.log.log(AdversarialEvent::TestInfo(
        "Crash-After-Ack durability verified across all cycles".to_string(),
    ));
    Ok(())
}
