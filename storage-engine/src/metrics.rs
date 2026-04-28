use std::sync::atomic::{AtomicU64, Ordering};

/// Engine-wide I/O and performance counters.
///
/// All counters are `AtomicU64` with `Relaxed` ordering — they are advisory
/// metrics, not synchronization primitives. Reading a stale value by a few
/// nanoseconds is perfectly acceptable for dashboards and benchmarks.
#[derive(Debug)]
pub struct EngineMetrics {
    /// Total bytes written to the WAL (before compression, if any).
    pub total_bytes_written: AtomicU64,
    /// Total number of `fsync` / `fdatasync` calls on the WAL.
    pub total_wal_syncs: AtomicU64,
    /// Total block cache hits (block found in `moka` without disk I/O).
    pub cache_hits: AtomicU64,
    /// Total block cache misses (required a disk read).
    pub cache_misses: AtomicU64,
    /// Total number of `put` operations executed.
    pub total_puts: AtomicU64,
    /// Total number of `get` operations executed.
    pub total_gets: AtomicU64,
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self {
            total_bytes_written: AtomicU64::new(0),
            total_wal_syncs: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            total_puts: AtomicU64::new(0),
            total_gets: AtomicU64::new(0),
        }
    }

    pub fn record_wal_write(&self, bytes: u64) {
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_wal_sync(&self) {
        self.total_wal_syncs.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_put(&self) {
        self.total_puts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_get(&self) {
        self.total_gets.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a snapshot of all counters for display / export.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_bytes_written: self.total_bytes_written.load(Ordering::Relaxed),
            total_wal_syncs: self.total_wal_syncs.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            total_puts: self.total_puts.load(Ordering::Relaxed),
            total_gets: self.total_gets.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time snapshot of engine metrics (non-atomic, safe to print).
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_bytes_written: u64,
    pub total_wal_syncs: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub total_puts: u64,
    pub total_gets: u64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hit_rate = if self.cache_hits + self.cache_misses > 0 {
            (self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64) * 100.0
        } else {
            0.0
        };
        write!(
            f,
            "puts={} gets={} wal_bytes={} wal_syncs={} cache_hit_rate={:.1}% (hits={} misses={})",
            self.total_puts,
            self.total_gets,
            self.total_bytes_written,
            self.total_wal_syncs,
            hit_rate,
            self.cache_hits,
            self.cache_misses,
        )
    }
}
