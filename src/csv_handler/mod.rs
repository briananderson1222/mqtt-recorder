//! CSV handler module
//!
//! Handles reading and writing message records to CSV files.

pub mod encoding;
pub mod reader;
pub mod record;
pub mod writer;

// Re-export all public items to maintain API compatibility
#[allow(unused_imports)]
pub use encoding::{is_binary_payload, AUTO_ENCODE_MARKER};
#[allow(unused_imports)]
pub use reader::CsvReader;
#[allow(unused_imports)]
pub use record::{MessageRecord, MessageRecordBytes, WriteStats};
#[allow(unused_imports)]
pub use writer::CsvWriter;
