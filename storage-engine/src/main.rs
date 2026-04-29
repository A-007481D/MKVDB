use std::sync::Arc;
use std::time::Duration;
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use storage_engine::network::ApexServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
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

    // Initialize the engine with "Nitro" Group Commit enabled
    let config =
        ApexConfig::default().with_sync_policy(SyncPolicy::Delayed(Duration::from_millis(10)));

    let engine = ApexEngine::open_with_config(data_dir, config)?;

    // Initialize the RESP Server
    let server = ApexServer::new(Arc::clone(&engine));

    // Setup graceful shutdown channel
    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for event");
        tracing::info!("Received Ctrl+C, shutting down...");
        let _ = tx.send(());
    });

    // Listen on standard Redis port
    server.run("127.0.0.1:6379", rx).await?;

    tracing::info!("MKVDB shutdown complete. Stay safe!");
    Ok(())
}
