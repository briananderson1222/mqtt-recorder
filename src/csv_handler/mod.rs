//! CSV handler module
//!
//! Handles reading and writing message records to CSV files.

pub mod encoding;
pub mod reader;
pub mod record;
pub mod writer;

// Re-export all public items to maintain API compatibility
pub use encoding::{is_binary_payload, AUTO_ENCODE_MARKER};
pub use reader::CsvReader;
pub use record::MessageRecord;
pub use writer::CsvWriter;

// Library-only re-exports (used by external consumers, not the binary)
#[allow(unused_imports)]
pub use record::{MessageRecordBytes, WriteStats};
