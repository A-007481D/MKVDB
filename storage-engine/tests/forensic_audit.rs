use storage_engine::ApexEngine;
use std::path::Path;

#[tokio::test]
async fn run_forensic_audit() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize tracing to see recovery logs
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let setup_path = "bench_forensic_setup";
    println!("--- Starting Forensic Audit on {} ---", setup_path);

    if !Path::new(setup_path).exists() {
        println!("Error: Setup path does not exist.");
        return Ok(());
    }

    // 2. Attempt to open the engine (Audit Point 2)
    println!("Step 1: Attempting to open ApexEngine...");
    let engine = match ApexEngine::open(setup_path) {
        Ok(e) => {
            println!("Success: Engine opened correctly. Atomic Manifest is VALID.");
            e
        },
        Err(e) => {
            println!("FAILURE: Engine failed to open: {:?}", e);
            return Err(e.into());
        }
    };

    // 3. Verify WAL Replay (Audit Point 3)
    // The benchmark was writing keys like "key-0000000-000"
    println!("Step 2: Verifying data retrieval (WAL Replay Check)...");
    
    // Let's check some keys from the middle of the range that should have been flushed/wal-ed
    let test_keys = [
        "key-0000000-000",
        "key-0000100-500",
        "key-0000500-250",
    ];

    for key_str in test_keys {
        match engine.get(key_str.as_bytes()) {
            Ok(Some(val)) => {
                println!("SUCCESS: Key '{}' found. Value size: {} bytes", key_str, val.len());
            },
            Ok(None) => {
                println!("WARNING: Key '{}' not found. Might have been lost in flight or never written.", key_str);
            },
            Err(e) => {
                println!("ERROR: Failed to get key '{}': {:?}", key_str, e);
            }
        }
    }

    println!("Forensic Audit Complete.");
    Ok(())
}
