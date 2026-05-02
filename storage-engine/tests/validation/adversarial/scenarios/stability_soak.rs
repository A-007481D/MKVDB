use crate::validation::adversarial::core::event_log::AdversarialEvent;
use crate::validation::adversarial::core::fault_controller::FaultRule;
use crate::validation::adversarial::core::harness::AdversarialHarness;
use anyhow::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::test]
async fn adversarial_stability_soak_3min() -> Result<()> {
    let harness = Arc::new(AdversarialHarness::new(3, 99999).await?);
    harness.log.log(AdversarialEvent::TestInfo(
        "Starting 3-minute Stability Soak Test".to_string(),
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

    let duration = Duration::from_secs(180); // 3 minutes
    let start = Instant::now();

    // Background Writers
    let h_write = harness.clone();
    let writer_task = tokio::spawn(async move {
        let mut count = 0;
        while start.elapsed() < duration {
            let leader_id = h_write.inner.assert_single_leader().ok();
            if let Some(leader) = leader_id {
                let key = format!("soak_key_{}", count).into_bytes();
                let val = b"soak_val".to_vec();
                let _ = h_write.inner.put(leader, key, val).await;
                count += 1;
            }
            sleep(Duration::from_millis(50)).await;
        }
        count
    });

    // Chaos Injector
    let h_chaos = harness.clone();
    let chaos_task = tokio::spawn(async move {
        while start.elapsed() < duration {
            // Random Partition
            let src = (rand::random::<u64>() % 3) + 1;
            let dst = (rand::random::<u64>() % 3) + 1;
            if src != dst {
                h_chaos.log.log(AdversarialEvent::TestInfo(format!(
                    "Injecting temporary partition {} <-> {}",
                    src, dst
                )));
                h_chaos.faults.add_rule(FaultRule::Partition(src, dst));
                h_chaos.faults.add_rule(FaultRule::Partition(dst, src));

                sleep(Duration::from_secs(5)).await;

                h_chaos.faults.clear_rules();
                h_chaos.log.log(AdversarialEvent::TestInfo(format!(
                    "Healed partition {} <-> {}",
                    src, dst
                )));
            }
            sleep(Duration::from_secs(10)).await;
        }
    });

    // Wait for tasks
    let _ = tokio::join!(writer_task, chaos_task);

    harness.log.log(AdversarialEvent::TestInfo(
        "Soak duration reached. Waiting for final convergence...".to_string(),
    ));

    // Clear all faults and let cluster stabilize
    harness.faults.clear_rules();
    sleep(Duration::from_secs(10)).await;

    let leader = harness
        .inner
        .wait_for_leader(Duration::from_secs(20))
        .await?;
    harness.log.log(AdversarialEvent::TestInfo(format!(
        "Final leader established: {}. Verifying consistency...",
        leader
    )));

    // Verify a sample of keys
    for i in (0..100).step_by(10) {
        let key = format!("soak_key_{}", i).into_bytes();
        let mut first_val = None;
        for &id in harness.inner.live.keys() {
            let val = harness.inner.get_stale(id, &key).await?;
            if first_val.is_none() {
                first_val = Some(val);
            } else {
                assert_eq!(
                    first_val,
                    Some(val),
                    "Divergence detected after soak for key_{}",
                    i
                );
            }
        }
    }

    harness.log.log(AdversarialEvent::TestInfo(
        "Stability soak verified successfully".to_string(),
    ));
    Ok(())
}
