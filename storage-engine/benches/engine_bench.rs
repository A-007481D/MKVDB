//! # ApexDB Engine Benchmarks
//!
//! Measures write throughput and read latency across all three `SyncPolicy`
//! modes. This suite specifically tests high-concurrency contention.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;
use storage_engine::engine::{ApexEngine, SyncPolicy};
use tempfile::tempdir;
use tokio::task::JoinSet;

// ---------------------------------------------------------------------------
// Concurrent Write throughput benchmarks
// ---------------------------------------------------------------------------

fn bench_concurrent_put(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let num_tasks = 10;
    let keys_per_task = 100;
    let total_keys = num_tasks * keys_per_task;

    let mut group = c.benchmark_group("concurrent_put");
    group.throughput(Throughput::Elements(total_keys as u64));
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10); // Since these are heavy tests

    let policies = [
        ("EveryWrite", SyncPolicy::EveryWrite),
        ("Buffered", SyncPolicy::Buffered),
        ("Delayed_10ms", SyncPolicy::Delayed(Duration::from_millis(10))),
    ];

    for (name, policy) in policies {
        group.bench_function(BenchmarkId::new("sync_policy", name), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let policy = policy.clone();
                async move {
                    let mut total_duration = Duration::ZERO;
                    
                    for _ in 0..iters {
                        let bench_dir = std::path::PathBuf::from("bench_data");
                        if bench_dir.exists() {
                            std::fs::remove_dir_all(&bench_dir).expect("cleanup");
                        }
                        std::fs::create_dir_all(&bench_dir).expect("create dir");

                        let engine = ApexEngine::open_with_policy(&bench_dir, policy.clone()).expect("open engine");
                        
                        let start = std::time::Instant::now();
                        let mut set = JoinSet::new();
                        
                        for t in 0..num_tasks {
                            let engine_clone = engine.clone();
                            set.spawn(async move {
                                for i in 0..keys_per_task {
                                    let key = Bytes::from(format!("t{:03}-k{:05}", t, i));
                                    let val = Bytes::from(format!("v-{:05}-{}", i, "X".repeat(64)));
                                    engine_clone.put(key, val).await.expect("put");
                                }
                            });
                        }
                        
                        while let Some(res) = set.join_next().await {
                            res.expect("task join");
                        }
                        
                        total_duration += start.elapsed();
                    }
                    total_duration
                }
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Read latency benchmarks (Point Lookup)
// ---------------------------------------------------------------------------

fn bench_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    // Pre-populate engine with 10,000 keys
    let dir = tempdir().expect("tempdir");
    let engine = rt.block_on(async {
        let e = ApexEngine::open_with_policy(dir.path(), SyncPolicy::Buffered).expect("open engine");
        for i in 0..10_000 {
            let key = Bytes::from(format!("k-{:012}", i));
            let val = Bytes::from(format!("v-{:012}-{}", i, "X".repeat(64)));
            e.put(key, val).await.expect("put");
        }
        e
    });

    // --- Hot read (same key, guaranteed in memtable / cache) ---
    group.bench_function("hot_memtable_hit", |b| {
        let key_bytes = format!("k-{:012}", 5000);
        b.iter(|| {
            rt.block_on(async {
                engine.get(key_bytes.as_bytes()).expect("get");
            });
        });
    });

    // --- Random read (across the keyspace) ---
    group.bench_function("random_key", |b| {
        let mut rng_counter: u64 = 0;
        b.iter(|| {
            rng_counter = rng_counter.wrapping_add(7919); // prime step
            let key = format!("k-{:012}", rng_counter % 10_000);
            rt.block_on(async {
                engine.get(key.as_bytes()).expect("get");
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_put, bench_get);
criterion_main!(benches);
