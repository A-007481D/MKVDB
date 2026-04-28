use storage_engine::ApexEngine;
use storage_engine::engine::SyncPolicy;
use storage_engine::network::ApexServer;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize professional tracing
    tracing_subscriber::fmt::init();
    
    let data_dir = "/tmp/mkvdb_data";
    std::fs::create_dir_all(data_dir)?;

    // Initialize the engine with "Nitro" Group Commit enabled
    let engine = Arc::new(ApexEngine::open_with_policy(
        data_dir, 
        SyncPolicy::Delayed(Duration::from_millis(10))
    )?);
    
    // Initialize the RESP Server
    let server = ApexServer::new(Arc::clone(&engine));
    
    // Setup graceful shutdown channel
    let (tx, rx) = tokio::sync::oneshot::channel();
    
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("failed to listen for event");
        tracing::info!("Received Ctrl+C, shutting down...");
        let _ = tx.send(());
    });

    // Listen on standard Redis port
    server.run("127.0.0.1:6379", rx).await?;

    tracing::info!("MKVDB shutdown complete. Stay safe!");
    Ok(())
}
