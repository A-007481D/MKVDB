use crate::validation::adversarial::core::event_log::AdversarialEvent;
use crate::validation::adversarial::core::harness::AdversarialHarness;
use storage_engine::network::node::ReadError;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn adversarial_read_after_write_linearizability() -> Result<()> {
    let harness = Arc::new(AdversarialHarness::new(3, 33333).await?);
    harness.log.log(AdversarialEvent::TestInfo(
        "Starting Read-After-Write Linearizability Test".to_string(),
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
    let leader_id = harness
        .inner
        .wait_for_leader(Duration::from_secs(10))
        .await?;

    let key = b"linear_key".to_vec();
    let val = b"linear_val".to_vec();

    harness.log.log(AdversarialEvent::TestInfo(
        format!("Issuing write to leader {}", leader_id),
    ));
    harness.inner.put(leader_id, key.clone(), val.clone()).await?;

    harness.log.log(AdversarialEvent::TestInfo(
        "Verifying immediate read from leader...".to_string(),
    ));
    let read_val = harness.inner.get(leader_id, &key).await?;
    assert_eq!(read_val, Some(val.clone()), "Leader did not return the written value immediately");

    harness.log.log(AdversarialEvent::TestInfo(
        "Read-After-Write linearizability verified on leader".to_string(),
    ));
    Ok(())
}

#[tokio::test]
async fn adversarial_follower_read_redirection() -> Result<()> {
    let harness = Arc::new(AdversarialHarness::new(3, 44444).await?);
    harness.log.log(AdversarialEvent::TestInfo(
        "Starting Follower Read Redirection Test".to_string(),
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
    let leader_id = harness
        .inner
        .wait_for_leader(Duration::from_secs(10))
        .await?;

    let follower_id = harness.inner.live.keys().find(|&&id| id != leader_id).cloned().unwrap();

    harness.log.log(AdversarialEvent::TestInfo(
        format!("Attempting linearizable read from follower {}", follower_id),
    ));

    // The harness.inner.get() might hide the redirection if it follows it.
    // Let's check ApexNode::read directly to see if it returns Redirect.
    let follower_node = &harness.inner.live[&follower_id].node;
    let res = follower_node.read(b"any_key").await;

    match res {
        Err(ReadError::Redirect(redirect)) => {
            harness.log.log(AdversarialEvent::TestInfo(
                format!("Correctly received redirection to leader {:?}", redirect),
            ));
        }
        _ => panic!("Expected Redirect error when reading from follower, got {:?}", res),
    }

    harness.log.log(AdversarialEvent::TestInfo(
        "Follower read redirection verified".to_string(),
    ));
    Ok(())
}
