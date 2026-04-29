use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use storage_engine::{ApexConfig, ApexEngine, WriteBatch};
use tempfile::TempDir;

#[tokio::test]
async fn test_concurrency_shedding() {
    let temp_dir = TempDir::new().unwrap();
    // Set max concurrency to 1 and threshold to 0 to force a permanent stall
    let config = ApexConfig::default()
        .with_max_concurrency(1)
        .with_immutable_threshold(0);
    let engine = Arc::new(ApexEngine::open_with_config(temp_dir.path(), config).unwrap());

    // 1. Start a task that will acquire the only permit and stall forever
    let engine_stall = engine.clone();
    tokio::spawn(async move {
        let mut batch = WriteBatch::new();
        batch.put(Bytes::from("stall"), Bytes::from("val"));
        let _ = engine_stall.write_batch(batch).await;
    });

    // Give it a moment to acquire the semaphore and start stalling
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut shed_count = 0;
    let mut success_count = 0;

    // 2. Try to start 10 more tasks. They should all be shed because the permit is held.
    for i in 0..10 {
        let mut batch = WriteBatch::new();
        batch.put(Bytes::from(format!("k{}", i)), Bytes::from("v"));
        match engine.write_batch(batch).await {
            Ok(_) => success_count += 1,
            Err(e) if e.to_string().contains("Max concurrent") => shed_count += 1,
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    println!(
        "Shedding Test - Success: {}, Shed: {}",
        success_count, shed_count
    );
    assert!(
        shed_count > 0,
        "Should have shed at least one task. Success: {}, Shed: {}",
        success_count,
        shed_count
    );
}

#[tokio::test]
async fn test_backpressure_stall() {
    let temp_dir = TempDir::new().unwrap();
    // Set low L0 threshold to trigger stalls easily
    let config = ApexConfig::default()
        .with_l0_threshold(1)
        .with_max_concurrency(10);
    let engine = ApexEngine::open_with_config(temp_dir.path(), config).unwrap();

    // 1. Create an L0 file
    engine
        .put(Bytes::from("key"), Bytes::from("val"))
        .await
        .unwrap();
    engine.force_flush().unwrap();

    // Wait for background flush to finish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Now writes should stall because L0 count is 1 and threshold is 1
    let mut batch = WriteBatch::new();
    batch.put(Bytes::from("stall"), Bytes::from("val"));

    let _ = engine.write_batch(batch).await;

    let metrics = engine.metrics();
    assert!(
        metrics
            .stall_count
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "Engine should have recorded at least one stall"
    );
}
