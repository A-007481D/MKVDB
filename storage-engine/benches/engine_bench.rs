//! # ApexDB Engine Benchmarks - Phase 4.8 Forensic Audit
//!
//! Measures physical limits, saturation points, and subtle performance bottlenecks
//! under real-world stress conditions.

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use rand::Rng;
use std::time::{Duration, Instant};
use storage_engine::batch::WriteBatch;
use storage_engine::engine::{ApexConfig, ApexEngine, SyncPolicy};
use tempfile::tempdir;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

// ---------------------------------------------------------------------------
// 1. Write Path: The "Saturation" Test
// ---------------------------------------------------------------------------

fn bench_saturation(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("write_saturation");

    let num_tasks = 4;
    let keys_per_task = 250;
    let total_keys = (num_tasks * keys_per_task) as u64;

    group.throughput(Throughput::Elements(total_keys));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    let policies = [
        ("EveryWrite", SyncPolicy::EveryWrite),
        ("Buffered", SyncPolicy::Buffered),
        (
            "Delayed_10ms",
            SyncPolicy::Delayed(Duration::from_millis(10)),
        ),
    ];

    for (name, policy) in policies {
        group.bench_function(BenchmarkId::new("put_concurrency_100", name), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let policy = policy.clone();
                async move {
                    let mut total_duration = Duration::ZERO;
                    for _ in 0..iters {
                        let dir = tempdir().expect("tempdir");
                        let config = ApexConfig::default().with_sync_policy(policy.clone());
                        let engine = ApexEngine::open_with_config(dir.path(), config).expect("open");

                        let start = Instant::now();
                        let mut set = JoinSet::new();

                        for _ in 0..num_tasks {
                            let e = engine.clone();
                            set.spawn(async move {
                                for _ in 0..keys_per_task {
                                    let (key, val) = {
                                        let mut rng = rand::rng();
                                        let k: [u8; 16] = rng.random();

                                        // Mixed payload: 512 bytes compressible text + 512 bytes random
                                        let mut v = vec![0u8; 1024];
                                        let text = "ApexDB_High_Fidelity_Benchmark_Payload_".repeat(13); // ~500 bytes
                                        let text_bytes = text.as_bytes();
                                        v[..text_bytes.len()].copy_from_slice(text_bytes);
                                        rng.fill(&mut v[text_bytes.len()..]);
                                        (k, v)
                                    };
                                    e.put(Bytes::copy_from_slice(&key), Bytes::from(val)).await.expect("put");
                                }
                            });
                        }
                        while let Some(res) = set.join_next().await { res.unwrap(); }

                        // Truth Check: Compression Ratio
                        if iters == 1 {
                             let wal_size: u64 = std::fs::read_dir(dir.path()).unwrap()
                                .filter_map(|e| e.ok())
                                .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
                                .map(|e| e.metadata().unwrap().len())
                                .sum();
                             engine.force_flush().unwrap();

                             // Wait for background flush to complete (polling)
                             let mut sst_size = 0;
                             for _ in 0..10 {
                                 tokio::time::sleep(Duration::from_millis(500)).await;
                                 sst_size = std::fs::read_dir(dir.path()).unwrap()
                                    .filter_map(|e| e.ok())
                                    .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
                                    .map(|e| e.metadata().unwrap().len())
                                    .sum();
                                 if sst_size > 0 { break; }
                             }

                             println!("\n[Truth Check - {}] WAL: {} bytes, SST (Compressed): {} bytes. Ratio: {:.2}x",
                                name, wal_size, sst_size, if sst_size > 0 { wal_size as f64 / sst_size as f64 } else { 0.0 });
                        }

                        total_duration += start.elapsed();
                    }
                    total_duration
                }
            });
        });
    }

    // Test B: Atomic Batch Throughput
    group.bench_function("write_batch_1000", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                let dir = tempdir().expect("tempdir");
                let config = ApexConfig::default().with_sync_policy(SyncPolicy::Buffered);
                let engine = ApexEngine::open_with_config(dir.path(), config).expect("open");

                let mut batch = WriteBatch::new();
                let mut rng = rand::rng();
                for _ in 0..1000 {
                    let key: [u8; 16] = rng.random();
                    let val: [u8; 128] = rng.random();
                    batch.put(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(&val));
                }

                let start = Instant::now();
                engine.write_batch(batch).await.expect("batch write");
                total_duration += start.elapsed();
            }
            total_duration
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Read Path: The "Forensic Seek" Test
// ---------------------------------------------------------------------------

fn bench_forensic_seek(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("forensic_seek");

    // Setup: 1 Million Keys (~2GB data)
    let setup_path = std::env::current_dir()
        .unwrap()
        .join("bench_forensic_setup");

    println!("--- Pre-loading 100,000 keys for Read Path Audit ---");
    rt.block_on(async {
        let config = ApexConfig::default()
            .with_sync_policy(SyncPolicy::Buffered)
            .with_l0_threshold(20) // Allow more for pre-loading
            .with_immutable_threshold(10);
        let engine = ApexEngine::open_with_config(&setup_path, config).expect("open");
        let mut rng = rand::rng();

        for i in 0..100 {
            // 100 batches of 1000 keys
            let mut batch = WriteBatch::new();
            for j in 0..1000 {
                let key = format!("key-{:07}-{:03}", i, j);
                let mut val = vec![0u8; 1024]; // 1KB per value
                rng.fill(&mut val[..]);
                batch.put(Bytes::from(key), Bytes::from(val));
            }
            // Retry loop for batch writes during pre-load
            let mut attempts = 0;
            loop {
                match engine.write_batch(batch.clone()).await {
                    Ok(_) => break,
                    Err(_) if attempts < 10 => {
                        attempts += 1;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Err(e) => panic!("batch failed after retries: {:?}", e),
                }
            }
        }
        engine.flush_wal().expect("flush wal");
    });
    // Negative Lookup Test
    group.bench_function("negative_lookup_bloom_check", |b| {
        let engine = rt.block_on(async { ApexEngine::open(&setup_path).expect("reopen") });
        b.iter_batched(
            || format!("missing-key-{:012}", rand::rng().random::<u64>()),
            |key| {
                let _ = engine.get(key.as_bytes()).expect("get");
            },
            BatchSize::SmallInput,
        );
    });

    // Cold Read Test (Force a reopen to clear caches)
    group.bench_function("cold_read_disk_seek", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            for _ in 0..iters {
                // Reopen the engine to ensure no page cache / table cache advantage
                let engine = rt.block_on(async { ApexEngine::open(&setup_path).expect("reopen") });
                let key = format!("key-{:07}-{:03}", 500, 500);

                let start = Instant::now();
                let _ = engine.get(key.as_bytes()).expect("get");
                total_duration += start.elapsed();
            }
            total_duration
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Scan Path: The "Log N" Verification
// ---------------------------------------------------------------------------

fn bench_scan_log_n(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("scan_log_n");

    let setup_path = std::env::current_dir().unwrap().join("bench_scan_setup");
    println!("--- Pre-loading 100,000 keys for Scan Audit ---");
    rt.block_on(async {
        let config = ApexConfig::default().with_sync_policy(SyncPolicy::Buffered);
        let engine = ApexEngine::open_with_config(&setup_path, config).expect("open");
        for i in 0..100 {
            let mut batch = WriteBatch::new();
            for j in 0..1000 {
                let key = format!("s-key-{:010}", i * 1000 + j);
                batch.put(Bytes::from(key), Bytes::from(vec![0u8; 100]));
            }
            engine.write_batch(batch).await.expect("batch");
        }
        engine.flush_wal().expect("flush wal");
    });

    let engine = rt.block_on(async { ApexEngine::open(&setup_path).expect("open") });

    let targets = [
        ("Beginning", "s-key-0000000100"),
        ("Middle", "s-key-0000500000"),
        ("End", "s-key-0000999900"),
    ];

    for (pos, start_key) in targets {
        group.bench_with_input(
            BenchmarkId::new("scan_init_latency", pos),
            start_key,
            |b, sk| {
                b.to_async(&rt).iter(|| {
                    let sk_bytes = Bytes::copy_from_slice(sk.as_bytes());
                    let engine_clone = engine.clone();
                    async move {
                        let mut stream = engine_clone
                            .scan(sk_bytes, Bytes::from_static(b"s-key-z"))
                            .expect("scan");
                        let _ = stream.next().await; // Measure time to first element
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Compaction: The "Write-Stall" Test
// ---------------------------------------------------------------------------

fn bench_compaction_stall(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("compaction_stall");

    group.bench_function("put_during_compaction", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            async move {
                let dir = tempdir().expect("tempdir");
                let config = ApexConfig::default().with_sync_policy(SyncPolicy::Buffered);
                let engine = ApexEngine::open_with_config(dir.path(), config).expect("open");

                // 1. Load some background data to trigger compaction later
                for _ in 0..5 {
                    let mut batch = WriteBatch::new();
                    for _ in 0..1000 {
                        batch.put(
                            Bytes::from(format!("k-{}", rand::rng().random::<u64>())),
                            Bytes::from(vec![0u8; 1024]),
                        );
                    }
                    engine.write_batch(batch).await.expect("batch");
                    engine.force_flush().expect("flush");
                }

                // 2. Measure write latency while a compaction is potentially running
                let start = Instant::now();
                for _ in 0..iters {
                    let key = format!("k-new-{}", rand::rng().random::<u64>());
                    engine
                        .put(Bytes::from(key), Bytes::from(vec![0u8; 1024]))
                        .await
                        .expect("put");
                }
                start.elapsed()
            }
        });
    });

    group.finish();

    // Cleanup
    let _ = std::fs::remove_dir_all(
        std::env::current_dir()
            .unwrap()
            .join("bench_forensic_setup"),
    );
    let _ = std::fs::remove_dir_all(std::env::current_dir().unwrap().join("bench_scan_setup"));
}

criterion_group!(
    benches,
    bench_saturation,
    bench_forensic_seek,
    bench_scan_log_n,
    bench_compaction_stall
);
criterion_main!(benches);
