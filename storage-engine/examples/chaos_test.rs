//! # ApexDB Chaos Test — Phase 1 Graduation
//!
//! Proves crash durability by writing 1,000 keys with `SyncPolicy::EveryWrite`,
//! then waiting for you to kill the process. A second invocation with `--recover`
//! reopens the engine and asserts every single key survived.
//!
//! ## Usage
//!
//! ```bash
//! # Terminal 1: Write 1,000 keys and wait for kill
//! cargo run --example chaos_test
//!
//! # (Hit Ctrl+C or `kill -9 <pid>` from another terminal)
//!
//! # Terminal 1: Verify all keys survived
//! cargo run --example chaos_test -- --recover
//!
//! # Clean up when done
//! rm -rf /tmp/apexdb_chaos
//! ```

use bytes::Bytes;
use std::process;
use storage_engine::engine::ApexEngine;

const DATA_DIR: &str = "/tmp/apexdb_chaos";
const NUM_KEYS: usize = 1_000;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let recover_mode = args.iter().any(|a| a == "--recover");

    if recover_mode {
        run_recover().await;
    } else {
        run_write().await;
    }
}

/// Phase A: Write 1,000 keys, then block forever waiting to be killed.
async fn run_write() {
    println!("=== ApexDB Chaos Test ===");
    println!("Data dir: {DATA_DIR}");
    println!("Writing {NUM_KEYS} keys with SyncPolicy::EveryWrite...\n");

    let engine = ApexEngine::open(DATA_DIR).unwrap_or_else(|e| {
        eprintln!("Failed to open engine: {e}");
        process::exit(1);
    });

    for i in 0..NUM_KEYS {
        let key = Bytes::from(format!("key-{i:05}"));
        let value = Bytes::from(format!("value-{i:05}-payload-{}", "X".repeat(64)));
        engine.put(key, value).await.unwrap_or_else(|e| {
            eprintln!("Write failed at key {i}: {e}");
            process::exit(1);
        });

        if (i + 1) % 100 == 0 {
            println!("  Written {}/{NUM_KEYS}", i + 1);
        }
    }

    println!("\n✅ All {NUM_KEYS} keys written and fsynced to WAL.");
    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  READY TO CRASH                                        ║");
    println!("║                                                        ║");
    println!("║  Hit Ctrl+C, or from another terminal:                 ║");
    println!("║    kill -9 $(pgrep chaos_test)                         ║");
    println!("║                                                        ║");
    println!("║  Then run:                                              ║");
    println!("║    cargo run --example chaos_test -- --recover          ║");
    println!("╚══════════════════════════════════════════════════════════╝");

    // Block forever — the only way out is a signal.
    loop {
        std::thread::park();
    }
}

/// Phase B: Reopen the engine and verify every key survived the crash.
async fn run_recover() {
    println!("=== ApexDB Recovery Verification ===");
    println!("Data dir: {DATA_DIR}");
    println!("Opening engine (WAL replay will happen automatically)...\n");

    let engine = ApexEngine::open(DATA_DIR).unwrap_or_else(|e| {
        eprintln!("Failed to open engine: {e}");
        process::exit(1);
    });

    let mut found = 0;
    let mut missing = 0;
    let mut corrupted = 0;

    for i in 0..NUM_KEYS {
        let key = format!("key-{i:05}");
        let expected_value = format!("value-{i:05}-payload-{}", "X".repeat(64));

        match engine.get(key.as_bytes()) {
            Ok(Some(val)) => {
                if val.as_ref() == expected_value.as_bytes() {
                    found += 1;
                } else {
                    corrupted += 1;
                    eprintln!("  ✗ CORRUPTED key={key}");
                    eprintln!("    expected: {expected_value}");
                    eprintln!("    got:      {}", String::from_utf8_lossy(&val));
                }
            }
            Ok(None) => {
                missing += 1;
                if missing <= 10 {
                    eprintln!("  ✗ MISSING key={key}");
                }
            }
            Err(e) => {
                corrupted += 1;
                eprintln!("  ✗ ERROR reading key={key}: {e}");
            }
        }

        if (i + 1) % 100 == 0 {
            println!("  Verified {}/{NUM_KEYS}...", i + 1);
        }
    }

    println!();
    println!("═══════════════════════════════════════");
    println!("  Results:");
    println!("    Found:     {found}/{NUM_KEYS}");
    println!("    Missing:   {missing}/{NUM_KEYS}");
    println!("    Corrupted: {corrupted}/{NUM_KEYS}");
    println!("═══════════════════════════════════════");

    if missing == 0 && corrupted == 0 {
        println!();
        println!("🏆 PHASE 1 GRADUATION: PASSED");
        println!("   All {NUM_KEYS} keys survived a crash with zero data loss.");
        println!("   The WAL + CRC32 + fsync pipeline is battle-tested.");
    } else {
        println!();
        println!("❌ PHASE 1 GRADUATION: FAILED");
        println!("   {missing} keys missing, {corrupted} keys corrupted.");
        process::exit(1);
    }
}
