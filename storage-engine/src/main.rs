use std::sync::Arc;
use std::time::Duration;
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use storage_engine::network::{ApexNode, ApexServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    tracing_subscriber::fmt::init();

    tracing::info!("Starting MKVDB Nitro (High-Performance Engine)...");

    let args: Vec<String> = std::env::args().collect();
    let data_dir = if let Some(pos) = args.iter().position(|a| a == "--path") {
        args.get(pos + 1)
            .map(|s| s.as_str())
            .unwrap_or("/tmp/mkvdb_data")
    } else {
        "/tmp/mkvdb_data"
    };

    std::fs::create_dir_all(data_dir)?;

    let config =
        ApexConfig::default().with_sync_policy(SyncPolicy::Delayed(Duration::from_millis(10)));

    let engine = ApexEngine::open_with_config(data_dir, config)?;

    let node_id = if let Some(pos) = args.iter().position(|a| a == "--node-id") {
        args.get(pos + 1)
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1)
    } else {
        1
    };
    let raft_bind_addr = if let Some(pos) = args.iter().position(|a| a == "--raft-addr") {
        args.get(pos + 1)
            .map(|s| s.as_str())
            .unwrap_or("127.0.0.1:50051")
    } else {
        "127.0.0.1:50051"
    };

    let node = Arc::new(ApexNode::start(node_id, raft_bind_addr, Arc::clone(&engine)).await?);
    let server = ApexServer::new(Arc::clone(&engine), node.clone());


    // We will check first if we need to bootstrap a new cluster !

    if args.iter().any(|a| a == "--bootstrap") {

        use openraft::BasicNode;

        use std::collections::BTreeMap;

        tracing::info!("Bootstrapping new cluster..");

        let mut members = BTreeMap::new();

        members.insert(node_id, BasicNode {addr : raft_bind_addr.to_string()});


        // we force the cluser to elect this node (itself and grant it rights to vote)
        match node.raft.initialize(members).await {

            Ok(_) => tracing::info!("Cluster bootstrap successful"),
            Err(e) =>  tracing::warn!("Cluster bootstrap failed (already initialized ?). {:?}", e),
        }


    }



    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for event");
        tracing::info!("Received Ctrl+C, shutting down...");
        let _ = tx.send(());
    });

    server.run("127.0.0.1:6379", rx).await?;

    tracing::info!("MKVDB shutdown complete. Stay safe!");
    Ok(())
}
