#![deny(clippy::all, clippy::pedantic)]
// Allowed exceptions that make sense in certain contexts, but keep strict by default
#![allow(clippy::module_name_repetitions)]

pub mod engine;
pub mod error;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;

pub use engine::{ApexEngine, SyncPolicy};
pub use error::{ApexError, Result};
