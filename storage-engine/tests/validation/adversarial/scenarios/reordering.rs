use std::time::Duration;
use std::sync::Arc;
use anyhow::Result;
use crate::validation::adversarial::core::harness::AdversarialHarness;
use crate::validation::adversarial::core::fault_controller::FaultRule;
use crate::validation::adversarial::core::event_log::AdversarialEvent;

#[tokio::test]
async fn adversarial_reordering_safety() -> Result<()> {
    let harness = Arc::new(AdversarialHarness::new(3, 54321).await?);
    harness.log.log(AdversarialEvent::TestInfo("Starting Reordering Safety Test".to_string()));
    
    let mut members = std::collections::BTreeMap::new();
    for id in 1..=3 {
        members.insert(id, openraft::BasicNode { addr: harness.inner.persistence[&id].bind_addr.clone() });
    }
    
    harness.inner.live[&1].node.raft.initialize(members).await?;
    let leader = harness.inner.wait_for_leader(Duration::from_secs(10)).await?;

    for src in 1..=3 {
        for dst in 1..=3 {
            if src != dst {
                harness.faults.add_rule(FaultRule::RandomDelay(
                    src, dst, 
                    Duration::from_millis(10), 
                    Duration::from_millis(500)
                ));
            }
        }
    }
    harness.log.log(AdversarialEvent::TestInfo("Applied 10ms-500ms random jitter to all links".to_string()));

    harness.log.log(AdversarialEvent::TestInfo("Issuing concurrent writes under jitter...".to_string()));
    let mut tasks = Vec::new();
    for i in 0..10 {
        let key = format!("reorder_key_{}", i).into_bytes();
        let val = b"val".to_vec();
        let h = harness.clone();
        
        tasks.push(tokio::spawn(async move {
            h.inner.put(leader, key, val).await
        }));
    }

    for t in tasks {
        let _ = t.await?;
    }
    harness.log.log(AdversarialEvent::TestInfo("All writes completed".to_string()));

    tokio::time::sleep(Duration::from_secs(2)).await;

    harness.log.log(AdversarialEvent::TestInfo("Verifying final consistency...".to_string()));
    for i in 0..10 {
        let key = format!("reorder_key_{}", i).into_bytes();
        let mut first_val = None;
        
        for &id in harness.inner.live.keys() {
            let val = harness.inner.get_stale(id, &key).await?;
            if first_val.is_none() {
                first_val = Some(val);
            } else {
                assert_eq!(first_val, Some(val), "Divergence detected for key reorder_key_{} on node {}", i, id);
            }
        }
    }
    
    harness.log.log(AdversarialEvent::TestInfo("Reordering safety verified".to_string()));
    Ok(())
}
