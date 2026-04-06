//! Spark-free COBOL copybook parsing + fixed-length record decoding.
//!
//! This crate provides an initial milestone from `RUST_LIBRARY_PLAN.md`:
//! - copybook parse (flat/group fields)
//! - fixed-length record extraction
//! - row decoding to typed values

mod parser;
mod reader;
mod schema;

pub use parser::{parse_copybook, ParserConfig};
pub use reader::{stream_rows, DecodeConfig, FixedRecordReader, TextEncoding};
pub use schema::{Field, Picture, Row, Schema, Value};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CobolError {
    #[error("invalid copybook: {0}")]
    InvalidCopybook(String),
    #[error("invalid picture clause: {0}")]
    InvalidPicture(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode error at field '{field}': {message}")]
    Decode { field: String, message: String },
}

pub type Result<T> = std::result::Result<T, CobolError>;
