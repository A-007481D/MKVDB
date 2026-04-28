#![deny(clippy::all, clippy::pedantic)]
// Allowed pedantic exceptions — justified for a low-level storage engine:
//   module_name_repetitions: e.g. SSTableReader in sstable module
//   cast_possible_truncation: we control sizes (keys < 4GB, blocks < 4GB)
//   cast_precision_loss: metrics display uses f64 for hit-rate percentages
//   missing_errors_doc / missing_panics_doc: internal crate, not a public API crate
//   doc_markdown: SSTable, MemTable etc. are proper nouns, not code references
//   needless_pass_by_value: Arc<T> is cheap to clone; passing by value is idiomatic
//   iter_without_into_iter: MemTable::iter is not a standard collection
#![allow(
    clippy::module_name_repetitions,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::needless_pass_by_value,
    clippy::iter_without_into_iter
)]

pub mod engine;
pub mod error;
pub mod manifest;
pub mod memtable;
pub mod metrics;
pub mod sstable;
pub mod wal;

pub use engine::{ApexEngine, SyncPolicy};
pub use error::{ApexError, Result};
pub use metrics::{EngineMetrics, MetricsSnapshot};
