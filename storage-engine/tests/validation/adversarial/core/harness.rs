use crate::validation::adversarial::core::event_log::EventLog;
use crate::validation::adversarial::core::fault_controller::FaultController;
use crate::validation::adversarial::net::interceptor::AdversarialNetworkFactory;
use crate::validation::common::harness::{ClusterHarness, LiveNode};
use anyhow::Result;
use std::sync::Arc;
use storage_engine::engine::ApexEngine;
use storage_engine::network::grpc::ApexRaftNetworkFactory;
use storage_engine::network::node::ApexNode;

pub struct AdversarialHarness {
    pub inner: ClusterHarness,
    pub log: Arc<EventLog>,
    pub faults: Arc<FaultController>,
}

impl AdversarialHarness {
    pub async fn new(n: usize, seed: u64) -> Result<Self> {
        let log = EventLog::new();
        let faults = FaultController::new(seed);

        let inner = ClusterHarness::new(n).await?;

        let mut harness = Self { inner, log, faults };

        for id in 1..=(n as u64) {
            harness.reboot_with_adversarial(id).await?;
        }

        Ok(harness)
    }

    pub async fn reboot_with_adversarial(&mut self, id: u64) -> Result<()> {
        // Kill existing node in inner harness
        self.inner.kill_node(id).await;

        let p = self
            .inner
            .persistence
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("No persistence entry for node {}", id))?;

        let engine =
            ApexEngine::open_with_config(&p.data_dir_path, self.inner.engine_config.clone())?;

        let network_factory = AdversarialNetworkFactory {
            inner: ApexRaftNetworkFactory {},
            source_id: id,
            log: self.log.clone(),
            faults: self.faults.clone(),
        };

        let node = ApexNode::start_with_network(id, &p.bind_addr, engine, network_factory).await?;

        self.inner.live.insert(id, LiveNode { node });
        Ok(())
    }
}
