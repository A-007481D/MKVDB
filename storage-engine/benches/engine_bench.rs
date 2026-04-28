//! # ApexDB Engine Benchmarks
//!
//! Measures write throughput and read latency across all three `SyncPolicy`
//! modes. Run with:
//!
//! ```bash
//! cargo bench                        # All benchmarks
//! cargo bench -- "put/"              # Write benchmarks only
//! cargo bench -- "get/"              # Read benchmarks only
//! ```
//!
//! HTML reports are generated in `target/criterion/`.

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use storage_engine::engine::{ApexEngine, SyncPolicy};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Write throughput benchmarks
// ---------------------------------------------------------------------------

fn bench_put(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("put");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    // --- EveryWrite (baseline — bounded by SSD IOPS) ---
    group.bench_function(BenchmarkId::new("sync_policy", "EveryWrite"), |b| {
        let dir = tempdir().expect("tempdir");
        let _guard = rt.enter();
        let engine = ApexEngine::open_with_policy(dir.path(), SyncPolicy::EveryWrite)
            .expect("open engine");

        let mut i: u64 = 0;
        b.iter(|| {
            let key = Bytes::from(format!("k-{i:012}"));
            let val = Bytes::from(format!("v-{i:012}-{}", "X".repeat(64)));
            engine.put(key, val).expect("put");
            i += 1;
        });

        let snap = engine.metrics().snapshot();
        eprintln!("[EveryWrite] {snap}");
    });

    // --- Buffered (no sync at all — pure throughput ceiling) ---
    group.bench_function(BenchmarkId::new("sync_policy", "Buffered"), |b| {
        let dir = tempdir().expect("tempdir");
        let _guard = rt.enter();
        let engine = ApexEngine::open_with_policy(dir.path(), SyncPolicy::Buffered)
            .expect("open engine");

        let mut i: u64 = 0;
        b.iter(|| {
            let key = Bytes::from(format!("k-{i:012}"));
            let val = Bytes::from(format!("v-{i:012}-{}", "X".repeat(64)));
            engine.put(key, val).expect("put");
            i += 1;
        });

        let snap = engine.metrics().snapshot();
        eprintln!("[Buffered]   {snap}");
    });

    // --- Delayed(10ms) (background group commit — the sweet spot) ---
    group.bench_function(BenchmarkId::new("sync_policy", "Delayed_10ms"), |b| {
        let dir = tempdir().expect("tempdir");
        let engine = rt.block_on(async {
            ApexEngine::open_with_policy(dir.path(), SyncPolicy::Delayed(Duration::from_millis(10)))
                .expect("open engine")
        });

        let mut i: u64 = 0;
        b.iter(|| {
            let key = Bytes::from(format!("k-{i:012}"));
            let val = Bytes::from(format!("v-{i:012}-{}", "X".repeat(64)));
            engine.put(key, val).expect("put");
            i += 1;
        });

        let snap = engine.metrics().snapshot();
        eprintln!("[Delayed]    {snap}");
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Read latency benchmarks
// ---------------------------------------------------------------------------

fn bench_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    // Pre-populate engine with 10,000 keys
    let dir = tempdir().expect("tempdir");
    let _guard = rt.enter();
    let engine = ApexEngine::open_with_policy(dir.path(), SyncPolicy::Buffered)
        .expect("open engine");

    let num_keys: u64 = 10_000;
    for i in 0..num_keys {
        let key = Bytes::from(format!("k-{i:012}"));
        let val = Bytes::from(format!("v-{i:012}-{}", "X".repeat(64)));
        engine.put(key, val).expect("put");
    }
    engine.flush_wal().expect("flush");

    // --- Hot read (same key, guaranteed in memtable / cache) ---
    group.bench_function("hot_memtable_hit", |b| {
        let key_bytes = format!("k-{:012}", 5000);
        b.iter(|| {
            engine.get(key_bytes.as_bytes()).expect("get");
        });
    });

    // --- Random read (across the keyspace) ---
    group.bench_function("random_key", |b| {
        let mut rng_counter: u64 = 0;
        b.iter_batched(
            || {
                rng_counter = rng_counter.wrapping_add(7919); // prime step
                format!("k-{:012}", rng_counter % num_keys)
            },
            |key| {
                engine.get(key.as_bytes()).expect("get");
            },
            BatchSize::SmallInput,
        );
    });

    // --- Cold read (key that doesn't exist — exercises bloom filter) ---
    group.bench_function("miss_bloom_filter", |b| {
        let missing = "k-ZZZZZZZZZZZZ";
        b.iter(|| {
            engine.get(missing.as_bytes()).expect("get");
        });
    });

    let snap = engine.metrics().snapshot();
    eprintln!("[Read bench] {snap}");

    group.finish();
}

criterion_group!(benches, bench_put, bench_get);
criterion_main!(benches);
