use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApexError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Checksum mismatch: expected {expected}, found {found}")]
    ChecksumMismatch { expected: u32, found: u32 },

    #[error("Key not found")]
    NotFound,

    #[error("Manifest error: {0}")]
    ManifestError(String),

    #[error("Data corruption: {0}")]
    Corruption(String),

    #[error("Engine overloaded: {0}")]
    EngineOverloaded(String),
}

pub type Result<T> = std::result::Result<T, ApexError>;
