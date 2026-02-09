//! CSV handler module
//!
//! Handles reading and writing message records to CSV files.
//!
//! This module provides the [`MessageRecord`] struct for representing MQTT messages
//! in a serializable format suitable for CSV storage, along with [`CsvWriter`] for
//! recording messages to CSV files. The CSV format follows RFC 4180
//! with the column order: timestamp, topic, payload, qos, retain.
//!
//! # CSV File Format
//!
//! ```csv
//! timestamp,topic,payload,qos,retain
//! 2024-01-15T10:30:00.123Z,sensors/temperature,{"value": 23.5},0,false
//! 2024-01-15T10:30:01.456Z,sensors/humidity,{"value": 65},1,true
//! ```
//!
//! # Example
//!
//! ```no_run
//! use mqtt_recorder::csv_handler::{CsvWriter, MessageRecord};
//! use chrono::Utc;
//! use std::path::Path;
//!
//! let mut writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
//! let record = MessageRecord::new(
//!     Utc::now(),
//!     "sensors/temperature".to_string(),
//!     r#"{"value": 23.5}"#.to_string(),
//!     0,
//!     false,
//! );
//! writer.write(&record).unwrap();
//! writer.flush().unwrap();
//! ```

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Utc};
use csv::{Reader, ReaderBuilder, Writer};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::error::MqttRecorderError;

/// The marker prefix used for auto-encoded binary payloads.
///
/// When `encode_b64` is false and a payload is detected as binary (containing
/// non-UTF8 bytes or control characters), the payload is automatically base64
/// encoded and prefixed with this marker. This allows the reader to distinguish
/// between intentionally base64-encoded content and automatically-encoded binary data.
///
/// # Example
///
/// A binary payload `[0x08, 0x0A, 0x12, 0x18]` would be stored as:
/// `b64:CAoSGA==`
///
/// # Requirements
///
/// - **2.1**: WHEN encode_b64 is false and a payload is classified as binary,
///   THE CSV_Handler SHALL base64 encode the payload and prefix it with "b64:"
/// - **3.1**: WHEN decode_b64 is false and a payload starts with "b64:",
///   THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
pub const AUTO_ENCODE_MARKER: &str = "b64:";

/// Determines if a payload contains binary data that requires encoding.
///
/// Binary payloads are those containing:
/// - Non-UTF8 byte sequences
/// - Control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F)
///
/// Tab (0x09), newline (0x0A), and carriage return (0x0D) are allowed
/// as CSV quoting handles these correctly.
///
/// # Arguments
///
/// * `payload` - The raw bytes of the MQTT message payload
///
/// # Returns
///
/// Returns `true` if the payload contains binary data that requires encoding,
/// `false` if the payload is safe to store as plain text in CSV.
///
/// # Examples
///
/// ```
/// use mqtt_recorder::csv_handler::is_binary_payload;
///
/// // Plain ASCII text is not binary
/// assert!(!is_binary_payload(b"Hello, World!"));
///
/// // UTF-8 text with emoji is not binary
/// assert!(!is_binary_payload("Hello 🌍".as_bytes()));
///
/// // Text with tabs and newlines is not binary (CSV handles these)
/// assert!(!is_binary_payload(b"line1\nline2\ttab"));
///
/// // Payload with NUL byte is binary
/// assert!(is_binary_payload(b"Hello\x00World"));
///
/// // Payload with control character (backspace) is binary
/// assert!(is_binary_payload(b"Hello\x08World"));
///
/// // Invalid UTF-8 sequence is binary
/// assert!(is_binary_payload(&[0xFF, 0xFE, 0x00, 0x01]));
/// ```
///
/// # Requirements
///
/// - **1.1**: WHEN encode_b64 is false and a payload contains non-UTF8 bytes,
///   THE Payload_Validator SHALL classify the payload as binary
/// - **1.2**: WHEN encode_b64 is false and a payload contains control characters
///   (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F), THE Payload_Validator SHALL classify
///   the payload as binary
/// - **1.3**: WHEN a payload contains only valid UTF8 with no control characters,
///   THE Payload_Validator SHALL classify the payload as text
/// - **1.5**: THE Payload_Validator SHALL allow tab (0x09), newline (0x0A),
///   and carriage return (0x0D) in text payloads since CSV quoting handles these
pub fn is_binary_payload(payload: &[u8]) -> bool {
    // First, check if the payload is valid UTF-8
    // If it's not valid UTF-8, it's definitely binary (Requirement 1.1)
    if std::str::from_utf8(payload).is_err() {
        return true;
    }

    // Check for control characters that would corrupt CSV
    // Control characters are 0x00-0x1F, but we allow:
    // - 0x09 (TAB) - CSV quoting handles this
    // - 0x0A (LF/newline) - CSV quoting handles this
    // - 0x0D (CR/carriage return) - CSV quoting handles this
    //
    // Binary control characters (Requirement 1.2):
    // - 0x00-0x08: NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, BS
    // - 0x0B-0x0C: VT, FF
    // - 0x0E-0x1F: SO through US
    for &byte in payload {
        if is_binary_control_char(byte) {
            return true;
        }
    }

    // Payload is valid UTF-8 with no problematic control characters (Requirement 1.3)
    false
}

/// Checks if a byte is a control character that indicates binary content.
///
/// Returns `true` for control characters that would corrupt CSV:
/// - 0x00-0x08: NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, BS
/// - 0x0B-0x0C: VT, FF
/// - 0x0E-0x1F: SO through US
///
/// Returns `false` for allowed whitespace characters:
/// - 0x09: TAB
/// - 0x0A: LF (newline)
/// - 0x0D: CR (carriage return)
#[inline]
fn is_binary_control_char(byte: u8) -> bool {
    matches!(byte, 0x00..=0x08 | 0x0B..=0x0C | 0x0E..=0x1F)
}

/// A single recorded MQTT message.
///
/// This struct represents an MQTT message with all relevant metadata for
/// recording and replay purposes. It is designed to be serialized to and
/// deserialized from CSV format.
///
/// # Fields
///
/// * `timestamp` - ISO 8601 timestamp with millisecond precision indicating when
///   the message was received
/// * `topic` - The MQTT topic the message was published to
/// * `payload` - The message payload, stored as a string (raw or base64 encoded)
/// * `qos` - Quality of Service level (0, 1, or 2)
/// * `retain` - Whether the message had the retain flag set
///
/// # Example
///
/// ```
/// use mqtt_recorder::csv_handler::MessageRecord;
/// use chrono::Utc;
///
/// let record = MessageRecord {
///     timestamp: Utc::now(),
///     topic: "sensors/temperature".to_string(),
///     payload: r#"{"value": 23.5}"#.to_string(),
///     qos: 0,
///     retain: false,
/// };
/// ```
///
/// # Requirements
///
/// - **4.2**: WHEN a message is received, write a record containing: timestamp, topic,
///   payload, QoS, and retain flag
/// - **6.1**: Use column order: timestamp, topic, payload, qos, retain
/// - **6.5**: Store timestamps in ISO 8601 format with millisecond precision
/// - **6.6**: Store QoS as an integer (0, 1, or 2)
/// - **6.7**: Store retain flag as a boolean string ("true" or "false")
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageRecord {
    /// ISO 8601 timestamp with millisecond precision indicating when the message was received.
    ///
    /// The timestamp is stored in UTC and serialized in ISO 8601 format
    /// (e.g., "2024-01-15T10:30:00.123Z").
    pub timestamp: DateTime<Utc>,

    /// The MQTT topic the message was published to.
    ///
    /// This preserves the exact topic string, including any hierarchy
    /// (e.g., "sensors/temperature/room1").
    pub topic: String,

    /// The message payload as a string.
    ///
    /// The payload can be stored as:
    /// - Raw string when `encode_b64` is false
    /// - Base64-encoded string when `encode_b64` is true
    ///
    /// The encoding choice is determined at the CsvWriter/CsvReader level,
    /// not in this struct.
    pub payload: String,

    /// Quality of Service level for the message.
    ///
    /// Valid values are:
    /// - 0: At most once delivery
    /// - 1: At least once delivery
    /// - 2: Exactly once delivery
    pub qos: u8,

    /// Whether the message had the MQTT retain flag set.
    ///
    /// When true, the broker stores this message and delivers it to new
    /// subscribers immediately upon subscription.
    pub retain: bool,
}

impl MessageRecord {
    /// Creates a new MessageRecord with the given values.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp when the message was received
    /// * `topic` - The MQTT topic
    /// * `payload` - The message payload as a string
    /// * `qos` - Quality of Service level (0, 1, or 2)
    /// * `retain` - Whether the retain flag was set
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::csv_handler::MessageRecord;
    /// use chrono::Utc;
    ///
    /// let record = MessageRecord::new(
    ///     Utc::now(),
    ///     "sensors/temperature".to_string(),
    ///     r#"{"value": 23.5}"#.to_string(),
    ///     0,
    ///     false,
    /// );
    /// ```
    pub fn new(
        timestamp: DateTime<Utc>,
        topic: String,
        payload: String,
        qos: u8,
        retain: bool,
    ) -> Self {
        Self {
            timestamp,
            topic,
            payload,
            qos,
            retain,
        }
    }
}

/// A message record with payload as raw bytes instead of String.
///
/// This struct is similar to [`MessageRecord`] but stores the payload as a
/// `Vec<u8>` instead of a `String`. This is useful when working with binary
/// MQTT payloads that may not be valid UTF-8, such as protobuf messages,
/// compressed data, or raw sensor readings.
///
/// # Fields
///
/// * `timestamp` - ISO 8601 timestamp with millisecond precision indicating when
///   the message was received
/// * `topic` - The MQTT topic the message was published to
/// * `payload` - The message payload as raw bytes
/// * `qos` - Quality of Service level (0, 1, or 2)
/// * `retain` - Whether the message had the retain flag set
///
/// # Example
///
/// ```
/// use mqtt_recorder::csv_handler::MessageRecordBytes;
/// use chrono::Utc;
///
/// let record = MessageRecordBytes {
///     timestamp: Utc::now(),
///     topic: "binary/data".to_string(),
///     payload: vec![0x08, 0x0A, 0x12, 0x18],
///     qos: 0,
///     retain: false,
/// };
/// ```
///
/// # Requirements
///
/// - **3.1**: WHEN decode_b64 is false and a payload starts with "b64:",
///   THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
#[allow(dead_code)] // Public API for library users
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRecordBytes {
    /// ISO 8601 timestamp with millisecond precision indicating when the message was received.
    ///
    /// The timestamp is stored in UTC and serialized in ISO 8601 format
    /// (e.g., "2024-01-15T10:30:00.123Z").
    pub timestamp: DateTime<Utc>,

    /// The MQTT topic the message was published to.
    ///
    /// This preserves the exact topic string, including any hierarchy
    /// (e.g., "sensors/temperature/room1").
    pub topic: String,

    /// The message payload as raw bytes.
    ///
    /// Unlike [`MessageRecord::payload`] which stores the payload as a String,
    /// this field stores the raw bytes, allowing proper handling of binary
    /// data that may not be valid UTF-8.
    pub payload: Vec<u8>,

    /// Quality of Service level for the message.
    ///
    /// Valid values are:
    /// - 0: At most once delivery
    /// - 1: At least once delivery
    /// - 2: Exactly once delivery
    pub qos: u8,

    /// Whether the message had the MQTT retain flag set.
    ///
    /// When true, the broker stores this message and delivers it to new
    /// subscribers immediately upon subscription.
    pub retain: bool,
}

impl MessageRecordBytes {
    /// Creates a new MessageRecordBytes with the given values.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp when the message was received
    /// * `topic` - The MQTT topic
    /// * `payload` - The message payload as raw bytes
    /// * `qos` - Quality of Service level (0, 1, or 2)
    /// * `retain` - Whether the retain flag was set
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::csv_handler::MessageRecordBytes;
    /// use chrono::Utc;
    ///
    /// let record = MessageRecordBytes::new(
    ///     Utc::now(),
    ///     "binary/data".to_string(),
    ///     vec![0x08, 0x0A, 0x12, 0x18],
    ///     0,
    ///     false,
    /// );
    /// ```
    #[allow(dead_code)] // Public API for library users
    pub fn new(
        timestamp: DateTime<Utc>,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
    ) -> Self {
        Self {
            timestamp,
            topic,
            payload,
            qos,
            retain,
        }
    }
}

/// CSV writer for recording MQTT messages to a file.
///
/// The `CsvWriter` handles writing [`MessageRecord`]s to a CSV file with proper
/// formatting according to RFC 4180. It supports optional base64 encoding of
/// message payloads for handling binary data.
///
/// # Features
///
/// - Writes a header row automatically on creation
/// - Supports base64 encoding of payloads when `encode_b64` is true
/// - Properly escapes CSV special characters (commas, quotes, newlines)
/// - Provides explicit flush control for data persistence
///
/// # CSV Format
///
/// The output CSV follows RFC 4180 with the column order:
/// `timestamp,topic,payload,qos,retain`
///
/// Fields containing commas, double quotes, or newlines are automatically
/// quoted and escaped by the underlying csv crate.
///
/// # Example
///
/// ```no_run
/// use mqtt_recorder::csv_handler::{CsvWriter, MessageRecord};
/// use chrono::Utc;
/// use std::path::Path;
///
/// let mut writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
///
/// let record = MessageRecord::new(
///     Utc::now(),
///     "sensors/temperature".to_string(),
///     r#"{"value": 23.5}"#.to_string(),
///     0,
///     false,
/// );
///
/// writer.write(&record).unwrap();
/// writer.flush().unwrap();
/// ```
///
/// # Requirements
///
/// - **4.3**: WHEN encode_b64 is true, store message payloads as base64-encoded strings
/// - **4.4**: WHEN encode_b64 is false, store message payloads as raw strings
/// - **4.5**: Write a header row as the first line of the CSV file
/// - **4.6**: Properly escape CSV special characters in payloads
/// - **4.7**: Flush writes to ensure data persistence
/// - **6.2**: Use RFC 4180 compliant CSV formatting
/// - **6.3**: Use double quotes to escape fields containing commas, quotes, or newlines
pub struct CsvWriter {
    /// The underlying CSV writer wrapping a file handle.
    writer: Writer<File>,
    /// Whether to encode payloads as base64.
    encode_b64: bool,
    /// Statistics for written records.
    stats: WriteStats,
}

/// Statistics collected during CSV writing.
///
/// This struct tracks various metrics about the records written to a CSV file,
/// including counts of different payload types and the largest payload size.
/// These statistics are useful for validation reporting and understanding
/// the composition of recorded data.
///
/// # Fields
///
/// * `total_records` - Total number of records written to the CSV file
/// * `text_payloads` - Number of payloads written as plain text (not auto-encoded)
/// * `auto_encoded_payloads` - Number of payloads that were automatically base64 encoded
///   due to binary content detection (prefixed with "b64:")
/// * `largest_payload` - Size in bytes of the largest payload encountered
///
/// # Example
///
/// ```
/// use mqtt_recorder::csv_handler::WriteStats;
///
/// let stats = WriteStats::default();
/// assert_eq!(stats.total_records, 0);
/// assert_eq!(stats.text_payloads, 0);
/// assert_eq!(stats.auto_encoded_payloads, 0);
/// assert_eq!(stats.largest_payload, 0);
/// ```
///
/// # Requirements
///
/// - **6.1**: WHEN validation completes, THE CSV_Validator SHALL report the total number of records processed
/// - **6.2**: WHEN validation completes, THE CSV_Validator SHALL report the number of text payloads
/// - **6.3**: WHEN validation completes, THE CSV_Validator SHALL report the number of binary payloads (auto-encoded with "b64:" prefix)
/// - **6.6**: WHEN validation completes, THE CSV_Validator SHALL report the largest payload size encountered
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteStats {
    /// Total number of records written to the CSV file.
    pub total_records: u64,
    /// Number of payloads written as plain text (not auto-encoded).
    pub text_payloads: u64,
    /// Number of payloads that were automatically base64 encoded due to binary content.
    pub auto_encoded_payloads: u64,
    /// Size in bytes of the largest payload encountered.
    pub largest_payload: usize,
}

impl CsvWriter {
    /// Creates a new CSV writer, writing the header row to the file.
    ///
    /// This constructor creates a new CSV file at the specified path and writes
    /// the header row (`timestamp,topic,payload,qos,retain`) as the first line.
    /// If the file already exists, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `path` - The path where the CSV file will be created
    /// * `encode_b64` - If true, payloads will be base64-encoded when written
    ///
    /// # Returns
    ///
    /// Returns `Ok(CsvWriter)` on success, or an error if the file cannot be
    /// created or the header cannot be written.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Csv`] if:
    /// - The file cannot be created (e.g., permission denied, invalid path)
    /// - The header row cannot be written
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::CsvWriter;
    /// use std::path::Path;
    ///
    /// // Create writer without base64 encoding
    /// let writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
    ///
    /// // Create writer with base64 encoding for binary payloads
    /// let writer_b64 = CsvWriter::new(Path::new("output_b64.csv"), true).unwrap();
    /// ```
    ///
    /// # Requirements
    ///
    /// - **4.5**: Write a header row as the first line of the CSV file
    pub fn new(path: &Path, encode_b64: bool) -> Result<Self, MqttRecorderError> {
        let mut writer = Writer::from_path(path)?;

        // Write the header row as required by 4.5
        writer.write_record(["timestamp", "topic", "payload", "qos", "retain"])?;

        Ok(Self {
            writer,
            encode_b64,
            stats: WriteStats::default(),
        })
    }

    /// Writes a message record to the CSV file.
    ///
    /// This method writes a single [`MessageRecord`] to the CSV file. If base64
    /// encoding was enabled during construction, the payload will be encoded
    /// before writing. The csv crate automatically handles RFC 4180 escaping
    /// for fields containing special characters.
    ///
    /// # Arguments
    ///
    /// * `record` - The message record to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the record cannot be written.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Csv`] if the record cannot be written to the file.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::{CsvWriter, MessageRecord};
    /// use chrono::Utc;
    /// use std::path::Path;
    ///
    /// let mut writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
    ///
    /// let record = MessageRecord::new(
    ///     Utc::now(),
    ///     "sensors/temperature".to_string(),
    ///     r#"{"value": 23.5}"#.to_string(),
    ///     0,
    ///     false,
    /// );
    ///
    /// writer.write(&record).unwrap();
    /// ```
    ///
    /// # Requirements
    ///
    /// - **4.3**: WHEN encode_b64 is true, store message payloads as base64-encoded strings
    /// - **4.4**: WHEN encode_b64 is false, store message payloads as raw strings
    /// - **4.6**: Properly escape CSV special characters in payloads
    /// - **6.2**: Use RFC 4180 compliant CSV formatting
    /// - **6.3**: Use double quotes to escape fields containing commas, quotes, or newlines
    pub fn write(&mut self, record: &MessageRecord) -> Result<(), MqttRecorderError> {
        // Convert string payload to bytes and delegate to write_bytes
        // This ensures consistent behavior between write and write_bytes,
        // including automatic binary detection and encoding
        self.write_bytes(
            record.timestamp,
            &record.topic,
            record.payload.as_bytes(),
            record.qos,
            record.retain,
        )
    }

    /// Flushes pending writes to disk.
    ///
    /// This method ensures that all buffered data is written to the underlying
    /// file. It should be called periodically during recording and always before
    /// closing the writer to ensure data persistence.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the flush operation fails.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Io`] if the flush operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::{CsvWriter, MessageRecord};
    /// use chrono::Utc;
    /// use std::path::Path;
    ///
    /// let mut writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
    ///
    /// // Write some records...
    /// let record = MessageRecord::new(
    ///     Utc::now(),
    ///     "test/topic".to_string(),
    ///     "payload".to_string(),
    ///     0,
    ///     false,
    /// );
    /// writer.write(&record).unwrap();
    ///
    /// // Ensure data is persisted to disk
    /// writer.flush().unwrap();
    /// ```
    ///
    /// # Requirements
    ///
    /// - **4.7**: Flush writes to ensure data persistence
    pub fn flush(&mut self) -> Result<(), MqttRecorderError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Write a message record from raw bytes (for binary payloads).
    ///
    /// This method accepts the payload as bytes rather than a string,
    /// allowing proper handling of non-UTF8 binary data. It automatically
    /// detects binary content and applies appropriate encoding.
    ///
    /// # Encoding Strategy
    ///
    /// - When `encode_b64` is false and payload is binary: prefix with "b64:" and base64 encode
    /// - When `encode_b64` is false and payload is text: write as-is (convert bytes to string)
    /// - When `encode_b64` is true: base64 encode all payloads without prefix (existing behavior)
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp when the message was received
    /// * `topic` - The MQTT topic
    /// * `payload` - The message payload as raw bytes
    /// * `qos` - Quality of Service level (0, 1, or 2)
    /// * `retain` - Whether the retain flag was set
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the record cannot be written.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Csv`] if the record cannot be written to the file.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::CsvWriter;
    /// use chrono::Utc;
    /// use std::path::Path;
    ///
    /// let mut writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
    ///
    /// // Write a text payload
    /// writer.write_bytes(
    ///     Utc::now(),
    ///     "sensors/temperature",
    ///     b"Hello World",
    ///     0,
    ///     false,
    /// ).unwrap();
    ///
    /// // Write a binary payload (will be auto-encoded with "b64:" prefix)
    /// writer.write_bytes(
    ///     Utc::now(),
    ///     "binary/data",
    ///     &[0x08, 0x0A, 0x12, 0x18],
    ///     0,
    ///     true,
    /// ).unwrap();
    ///
    /// writer.flush().unwrap();
    /// ```
    ///
    /// # Requirements
    ///
    /// - **2.1**: WHEN encode_b64 is false and a payload is classified as binary,
    ///   THE CSV_Handler SHALL base64 encode the payload and prefix it with "b64:"
    /// - **2.2**: WHEN encode_b64 is false and a payload is classified as text,
    ///   THE CSV_Handler SHALL write the payload as-is without any prefix
    /// - **2.3**: WHEN encode_b64 is true, THE CSV_Handler SHALL base64 encode
    ///   all payloads without any prefix (existing behavior)
    /// - **5.4**: WHEN a payload contains the literal string "b64:" at the start,
    ///   THE CSV_Handler SHALL handle it correctly without misinterpreting it as an auto-encode marker
    pub fn write_bytes(
        &mut self,
        timestamp: DateTime<Utc>,
        topic: &str,
        payload: &[u8],
        qos: u8,
        retain: bool,
    ) -> Result<(), MqttRecorderError> {
        // Update largest payload statistic
        if payload.len() > self.stats.largest_payload {
            self.stats.largest_payload = payload.len();
        }

        // Determine the payload string based on encoding strategy
        let payload_str = if self.encode_b64 {
            // When encode_b64 is true: base64 encode all payloads without prefix (Requirement 2.3)
            BASE64_STANDARD.encode(payload)
        } else if is_binary_payload(payload) {
            // When encode_b64 is false and payload is binary: prefix with "b64:" and base64 encode (Requirement 2.1)
            self.stats.auto_encoded_payloads += 1;
            format!("{}{}", AUTO_ENCODE_MARKER, BASE64_STANDARD.encode(payload))
        } else {
            // When encode_b64 is false and payload is text: check for marker collision (Requirement 5.4)
            // If the text payload starts with "b64:", we must encode it to avoid ambiguity
            // during read-back, where it would be misinterpreted as an auto-encoded payload.
            let text = String::from_utf8(payload.to_vec())
                .expect("is_binary_payload returned false but payload is not valid UTF-8");

            if text.starts_with(AUTO_ENCODE_MARKER) {
                // Marker collision: encode the payload to preserve it exactly
                self.stats.auto_encoded_payloads += 1;
                format!("{}{}", AUTO_ENCODE_MARKER, BASE64_STANDARD.encode(payload))
            } else {
                // Normal text payload: write as-is (Requirement 2.2)
                self.stats.text_payloads += 1;
                text
            }
        };

        // Update total records statistic
        self.stats.total_records += 1;

        // Format timestamp in ISO 8601 format with millisecond precision
        let timestamp_str = timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        // Write the record - the csv crate handles RFC 4180 escaping automatically
        self.writer.write_record([
            &timestamp_str,
            topic,
            &payload_str,
            &qos.to_string(),
            &retain.to_string(),
        ])?;

        Ok(())
    }

    /// Returns a reference to the current write statistics.
    ///
    /// This method provides access to the statistics collected during writing,
    /// including the total number of records, counts of text and auto-encoded
    /// payloads, and the largest payload size encountered.
    ///
    /// # Returns
    ///
    /// A reference to the [`WriteStats`] struct containing the current statistics.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::{CsvWriter, MessageRecord};
    /// use chrono::Utc;
    /// use std::path::Path;
    ///
    /// let mut writer = CsvWriter::new(Path::new("output.csv"), false).unwrap();
    ///
    /// // Write some records...
    /// let record = MessageRecord::new(
    ///     Utc::now(),
    ///     "test/topic".to_string(),
    ///     "payload".to_string(),
    ///     0,
    ///     false,
    /// );
    /// writer.write(&record).unwrap();
    ///
    /// // Get statistics
    /// let stats = writer.stats();
    /// println!("Total records: {}", stats.total_records);
    /// println!("Text payloads: {}", stats.text_payloads);
    /// println!("Auto-encoded payloads: {}", stats.auto_encoded_payloads);
    /// println!("Largest payload: {} bytes", stats.largest_payload);
    /// ```
    ///
    /// # Requirements
    ///
    /// - **6.1**: Report the total number of records processed
    /// - **6.2**: Report the number of text payloads
    /// - **6.3**: Report the number of binary payloads (auto-encoded with "b64:" prefix)
    /// - **6.6**: Report the largest payload size encountered
    #[allow(dead_code)] // Public API for library users
    pub fn stats(&self) -> &WriteStats {
        &self.stats
    }
}

/// CSV reader for replaying MQTT messages from a file.
///
/// The `CsvReader` handles reading [`MessageRecord`]s from a CSV file for replay
/// purposes. It supports optional base64 decoding of message payloads and
/// configurable field size limits.
///
/// # Features
///
/// - Reads CSV files with the standard header format
/// - Supports base64 decoding of payloads when `decode_b64` is true
/// - Enforces optional field size limits for security
/// - Implements `Iterator` for convenient sequential reading
/// - Supports `reset()` for loop replay functionality
///
/// # CSV Format
///
/// The input CSV must have the column order:
/// `timestamp,topic,payload,qos,retain`
///
/// # Example
///
/// ```no_run
/// use mqtt_recorder::csv_handler::CsvReader;
/// use std::path::Path;
///
/// let mut reader = CsvReader::new(Path::new("input.csv"), false, None).unwrap();
///
/// for result in reader {
///     match result {
///         Ok(record) => println!("Topic: {}, Payload: {}", record.topic, record.payload),
///         Err(e) => eprintln!("Error reading record: {}", e),
///     }
/// }
/// ```
///
/// # Requirements
///
/// - **5.1**: WHEN mode is "replay", read messages from the CSV file
/// - **5.2**: WHEN replaying messages, preserve the original topic
/// - **5.3**: WHEN replaying messages, preserve the original QoS level
/// - **5.4**: WHEN replaying messages, preserve the original retain flag
/// - **5.5**: WHEN replaying messages, maintain relative timing based on timestamps
/// - **5.6**: WHEN encode_b64 was used during recording, decode base64 payloads
/// - **6.4**: WHEN csv_field_size_limit is specified, enforce the maximum field size
pub struct CsvReader {
    /// The underlying CSV reader wrapping a file handle.
    reader: Reader<File>,
    /// Whether to decode payloads from base64.
    decode_b64: bool,
    /// Optional maximum field size limit.
    field_size_limit: Option<usize>,
    /// Path to the CSV file (stored for reset functionality).
    path: PathBuf,
    /// Current line number (1-indexed, accounts for header row).
    /// Used for providing descriptive error messages with line context.
    current_line: u64,
}

impl CsvReader {
    /// Creates a new CSV reader for the specified file.
    ///
    /// This constructor opens the CSV file and configures the reader with
    /// optional base64 decoding and field size limits. The CSV file must
    /// have a header row that will be automatically skipped.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the CSV file to read
    /// * `decode_b64` - If true, payloads will be decoded from base64 when read
    /// * `field_size_limit` - Optional maximum size for any CSV field
    ///
    /// # Returns
    ///
    /// Returns `Ok(CsvReader)` on success, or an error if the file cannot be
    /// opened or the reader cannot be configured.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Csv`] if:
    /// - The file cannot be opened (e.g., file not found, permission denied)
    /// - The CSV reader cannot be configured
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::CsvReader;
    /// use std::path::Path;
    ///
    /// // Create reader without base64 decoding
    /// let reader = CsvReader::new(Path::new("input.csv"), false, None).unwrap();
    ///
    /// // Create reader with base64 decoding and field size limit
    /// let reader_b64 = CsvReader::new(
    ///     Path::new("input_b64.csv"),
    ///     true,
    ///     Some(1024 * 1024), // 1MB limit
    /// ).unwrap();
    /// ```
    ///
    /// # Requirements
    ///
    /// - **5.1**: Read messages from the CSV file
    /// - **6.4**: WHEN csv_field_size_limit is specified, enforce the maximum field size
    pub fn new(
        path: &Path,
        decode_b64: bool,
        field_size_limit: Option<usize>,
    ) -> Result<Self, MqttRecorderError> {
        let reader = Self::create_reader(path, field_size_limit)?;

        Ok(Self {
            reader,
            decode_b64,
            field_size_limit,
            path: path.to_path_buf(),
            current_line: 1, // Start at 1 (header row is line 1, first data row is line 2)
        })
    }

    /// Creates a CSV reader with the specified configuration.
    ///
    /// This is a helper method used by both `new()` and `reset()` to create
    /// a properly configured CSV reader.
    fn create_reader(
        path: &Path,
        _field_size_limit: Option<usize>,
    ) -> Result<Reader<File>, MqttRecorderError> {
        let mut builder = ReaderBuilder::new();
        builder.has_headers(true);

        // Note: The csv crate doesn't have a built-in field size limit.
        // We enforce the limit manually in parse_record() when reading fields.

        let reader = builder.from_path(path)?;
        Ok(reader)
    }

    /// Reads the next message record from the CSV file.
    ///
    /// This method reads and parses the next record from the CSV file,
    /// optionally decoding the payload from base64 if configured.
    ///
    /// # Returns
    ///
    /// Returns:
    /// - `Some(Ok(MessageRecord))` if a record was successfully read
    /// - `Some(Err(MqttRecorderError))` if an error occurred while reading
    /// - `None` if the end of file has been reached
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::CsvReader;
    /// use std::path::Path;
    ///
    /// let mut reader = CsvReader::new(Path::new("input.csv"), false, None).unwrap();
    ///
    /// while let Some(result) = reader.next() {
    ///     match result {
    ///         Ok(record) => println!("Read: {} - {}", record.topic, record.payload),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// ```
    ///
    /// # Requirements
    ///
    /// - **5.2**: Preserve the original topic for each message
    /// - **5.3**: Preserve the original QoS level for each message
    /// - **5.4**: Preserve the original retain flag for each message
    /// - **5.6**: WHEN encode_b64 was used during recording, decode base64 payloads
    pub fn read_next(&mut self) -> Option<Result<MessageRecord, MqttRecorderError>> {
        // Get the next record from the CSV reader
        let result = self.reader.records().next()?;

        // Increment line counter for error reporting
        self.current_line += 1;

        Some(self.parse_record(result))
    }

    /// Read the next record and return payload as bytes.
    ///
    /// This method is useful when the caller needs the raw binary payload
    /// rather than a UTF-8 string representation. It handles automatic
    /// decoding of binary payloads that were auto-encoded with the "b64:" prefix.
    ///
    /// # Decoding Strategy
    ///
    /// - When `decode_b64` is false and payload starts with "b64:": strip prefix and base64 decode
    /// - When `decode_b64` is false and payload does not start with "b64:": return payload as bytes
    /// - When `decode_b64` is true: base64 decode all payloads (existing behavior)
    ///
    /// # Returns
    ///
    /// Returns:
    /// - `Some(Ok(MessageRecordBytes))` if a record was successfully read
    /// - `Some(Err(MqttRecorderError))` if an error occurred while reading (including invalid base64)
    /// - `None` if the end of file has been reached
    ///
    /// # Errors
    ///
    /// Returns an error with descriptive message including line number if:
    /// - The CSV record cannot be parsed
    /// - A payload with "b64:" prefix contains invalid base64
    /// - A payload in decode_b64 mode contains invalid base64
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::CsvReader;
    /// use std::path::Path;
    ///
    /// let mut reader = CsvReader::new(Path::new("input.csv"), false, None).unwrap();
    ///
    /// while let Some(result) = reader.read_next_bytes() {
    ///     match result {
    ///         Ok(record) => {
    ///             println!("Topic: {}, Payload bytes: {:?}", record.topic, record.payload);
    ///         }
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// ```
    ///
    /// # Requirements
    ///
    /// - **3.1**: WHEN decode_b64 is false and a payload starts with "b64:",
    ///   THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
    /// - **3.2**: WHEN decode_b64 is false and a payload does not start with "b64:",
    ///   THE CSV_Handler SHALL use the payload as-is
    /// - **3.3**: WHEN decode_b64 is true, THE CSV_Handler SHALL base64 decode
    ///   all payloads without checking for prefix (existing behavior)
    /// - **3.4**: IF a payload starts with "b64:" but the remaining content is not valid base64,
    ///   THEN THE CSV_Handler SHALL return an error with a descriptive message
    #[allow(dead_code)] // Public API for library users
    pub fn read_next_bytes(&mut self) -> Option<Result<MessageRecordBytes, MqttRecorderError>> {
        // Get the next record from the CSV reader
        let result = self.reader.records().next()?;

        // Increment line counter for error reporting
        self.current_line += 1;
        let line_number = self.current_line;

        Some(self.parse_record_bytes(result, line_number))
    }

    /// Parses a CSV string record into a MessageRecordBytes.
    ///
    /// This method handles the decoding logic for binary payloads:
    /// - When decode_b64 is true: decode all payloads from base64
    /// - When decode_b64 is false and payload starts with "b64:": strip prefix and decode
    /// - When decode_b64 is false and no prefix: return payload bytes as-is
    #[allow(dead_code)] // Used by read_next_bytes which is a public API
    fn parse_record_bytes(
        &self,
        result: Result<csv::StringRecord, csv::Error>,
        line_number: u64,
    ) -> Result<MessageRecordBytes, MqttRecorderError> {
        let record = result.map_err(|e| {
            MqttRecorderError::InvalidArgument(format!(
                "CSV parse error at line {}: {}",
                line_number, e
            ))
        })?;

        // Ensure we have exactly 5 fields
        if record.len() != 5 {
            return Err(MqttRecorderError::InvalidArgument(format!(
                "Line {}: Expected 5 fields but got {}",
                line_number,
                record.len()
            )));
        }

        // Check field size limits if configured (Requirement 6.4)
        if let Some(limit) = self.field_size_limit {
            for (i, field) in record.iter().enumerate() {
                if field.len() > limit {
                    return Err(MqttRecorderError::InvalidArgument(format!(
                        "Line {}: Field {} exceeds size limit of {} bytes (actual: {} bytes)",
                        line_number,
                        i,
                        limit,
                        field.len()
                    )));
                }
            }
        }

        // Parse timestamp (ISO 8601 format)
        let timestamp_str = &record[0];
        let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|_| {
                // Try parsing with the format we write (which may not have timezone offset)
                chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.3fZ")
                    .map(|ndt| ndt.and_utc())
            })
            .map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Line {}: Invalid timestamp '{}': {}",
                    line_number, timestamp_str, e
                ))
            })?;

        // Parse topic
        let topic = record[1].to_string();

        // Parse payload as bytes, handling auto-encoded binary payloads
        let payload_str = &record[2];
        let payload: Vec<u8> = if self.decode_b64 {
            // When decode_b64 is true: decode all payloads from base64 (Requirement 3.3)
            BASE64_STANDARD.decode(payload_str).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Line {}: Invalid base64 payload: {}",
                    line_number, e
                ))
            })?
        } else if let Some(encoded_content) = payload_str.strip_prefix(AUTO_ENCODE_MARKER) {
            // When decode_b64 is false and payload starts with "b64:": strip prefix and decode (Requirement 3.1)
            BASE64_STANDARD.decode(encoded_content).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Line {}: Invalid base64 in auto-encoded payload (after 'b64:' prefix): {}",
                    line_number, e
                ))
            })?
        } else {
            // When decode_b64 is false and no prefix: return payload as bytes (Requirement 3.2)
            payload_str.as_bytes().to_vec()
        };

        // Parse QoS
        let qos: u8 = record[3].parse().map_err(|e| {
            MqttRecorderError::InvalidArgument(format!(
                "Line {}: Invalid QoS '{}': {}",
                line_number, &record[3], e
            ))
        })?;

        // Validate QoS range
        if qos > 2 {
            return Err(MqttRecorderError::InvalidArgument(format!(
                "Line {}: QoS must be 0, 1, or 2, got {}",
                line_number, qos
            )));
        }

        // Parse retain flag
        let retain: bool = record[4].parse().map_err(|e| {
            MqttRecorderError::InvalidArgument(format!(
                "Line {}: Invalid retain flag '{}': {}",
                line_number, &record[4], e
            ))
        })?;

        Ok(MessageRecordBytes {
            timestamp,
            topic,
            payload,
            qos,
            retain,
        })
    }

    /// Parses a CSV string record into a MessageRecord.
    ///
    /// This method handles the decoding logic for payloads:
    /// - When decode_b64 is true: decode all payloads from base64, then convert to UTF-8
    /// - When decode_b64 is false and payload starts with "b64:": strip prefix, decode, and convert to UTF-8 (lossy)
    /// - When decode_b64 is false and no prefix: return payload as-is
    ///
    /// # Requirements
    ///
    /// - **3.1**: WHEN decode_b64 is false and a payload starts with "b64:",
    ///   THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
    /// - **3.2**: WHEN decode_b64 is false and a payload does not start with "b64:",
    ///   THE CSV_Handler SHALL use the payload as-is
    /// - **3.3**: WHEN decode_b64 is true, THE CSV_Handler SHALL base64 decode
    ///   all payloads without checking for prefix (existing behavior)
    fn parse_record(
        &self,
        result: Result<csv::StringRecord, csv::Error>,
    ) -> Result<MessageRecord, MqttRecorderError> {
        let record = result?;

        // Ensure we have exactly 5 fields
        if record.len() != 5 {
            return Err(MqttRecorderError::InvalidArgument(format!(
                "Expected 5 fields but got {}",
                record.len()
            )));
        }

        // Check field size limits if configured (Requirement 6.4)
        if let Some(limit) = self.field_size_limit {
            for (i, field) in record.iter().enumerate() {
                if field.len() > limit {
                    return Err(MqttRecorderError::InvalidArgument(format!(
                        "Field {} exceeds size limit of {} bytes (actual: {} bytes)",
                        i,
                        limit,
                        field.len()
                    )));
                }
            }
        }

        // Parse timestamp (ISO 8601 format)
        let timestamp_str = &record[0];
        let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .or_else(|_| {
                // Try parsing with the format we write (which may not have timezone offset)
                chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.3fZ")
                    .map(|ndt| ndt.and_utc())
            })
            .map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Invalid timestamp '{}': {}",
                    timestamp_str, e
                ))
            })?;

        // Parse topic
        let topic = record[1].to_string();

        // Parse payload, handling auto-encoded binary payloads
        let payload_str = &record[2];
        let payload = if self.decode_b64 {
            // When decode_b64 is true: decode all payloads from base64 (Requirement 3.3)
            let decoded_bytes = BASE64_STANDARD.decode(payload_str).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!("Invalid base64 payload: {}", e))
            })?;
            // Convert to UTF-8, using lossy conversion for binary data
            String::from_utf8_lossy(&decoded_bytes).into_owned()
        } else if let Some(encoded_content) = payload_str.strip_prefix(AUTO_ENCODE_MARKER) {
            // When decode_b64 is false and payload starts with "b64:": strip prefix and decode (Requirement 3.1)
            let decoded_bytes = BASE64_STANDARD.decode(encoded_content).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Invalid base64 in auto-encoded payload (after 'b64:' prefix): {}",
                    e
                ))
            })?;
            // Convert decoded bytes to UTF-8 string using lossy conversion for binary data
            String::from_utf8_lossy(&decoded_bytes).into_owned()
        } else {
            // When decode_b64 is false and no prefix: return payload as-is (Requirement 3.2)
            payload_str.to_string()
        };

        // Parse QoS
        let qos: u8 = record[3].parse().map_err(|e| {
            MqttRecorderError::InvalidArgument(format!("Invalid QoS '{}': {}", &record[3], e))
        })?;

        // Validate QoS range
        if qos > 2 {
            return Err(MqttRecorderError::InvalidArgument(format!(
                "QoS must be 0, 1, or 2, got {}",
                qos
            )));
        }

        // Parse retain flag
        let retain: bool = record[4].parse().map_err(|e| {
            MqttRecorderError::InvalidArgument(format!(
                "Invalid retain flag '{}': {}",
                &record[4], e
            ))
        })?;

        Ok(MessageRecord {
            timestamp,
            topic,
            payload,
            qos,
            retain,
        })
    }

    /// Resets the reader to the beginning of the file.
    ///
    /// This method closes the current reader and reopens the file from the
    /// beginning, allowing the CSV to be read again. This is useful for
    /// implementing loop replay functionality.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the file cannot be reopened.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Csv`] if the file cannot be reopened.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::csv_handler::CsvReader;
    /// use std::path::Path;
    ///
    /// let mut reader = CsvReader::new(Path::new("input.csv"), false, None).unwrap();
    ///
    /// // Read all records
    /// while let Some(_) = reader.next() {}
    ///
    /// // Reset to beginning for another pass
    /// reader.reset().unwrap();
    ///
    /// // Read all records again
    /// while let Some(_) = reader.next() {}
    /// ```
    ///
    /// # Requirements
    ///
    /// - **5.7**: WHEN loop is true, restart replay from the beginning
    pub fn reset(&mut self) -> Result<(), MqttRecorderError> {
        // Recreate the reader from the stored path
        self.reader = Self::create_reader(&self.path, self.field_size_limit)?;
        // Reset line counter (header row is line 1, first data row is line 2)
        self.current_line = 1;
        Ok(())
    }
}

impl Iterator for CsvReader {
    type Item = Result<MessageRecord, MqttRecorderError>;

    /// Returns the next message record from the CSV file.
    ///
    /// This implementation allows `CsvReader` to be used in for loops and
    /// with iterator adapters.
    ///
    /// # Returns
    ///
    /// Returns:
    /// - `Some(Ok(MessageRecord))` if a record was successfully read
    /// - `Some(Err(MqttRecorderError))` if an error occurred while reading
    /// - `None` if the end of file has been reached
    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_message_record_creation() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            r#"{"value": 23.5}"#.to_string(),
            0,
            false,
        );

        assert_eq!(record.timestamp, timestamp);
        assert_eq!(record.topic, "sensors/temperature");
        assert_eq!(record.payload, r#"{"value": 23.5}"#);
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_message_record_with_retain_flag() {
        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "actuators/light".to_string(),
            "ON".to_string(),
            1,
            true,
        );

        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_message_record_clone() {
        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "payload".to_string(),
            2,
            false,
        );

        let cloned = record.clone();
        assert_eq!(record, cloned);
    }

    #[test]
    fn test_message_record_debug() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record =
            MessageRecord::new(timestamp, "test".to_string(), "data".to_string(), 0, false);

        let debug_str = format!("{:?}", record);
        assert!(debug_str.contains("MessageRecord"));
        assert!(debug_str.contains("test"));
        assert!(debug_str.contains("data"));
    }

    #[test]
    fn test_message_record_qos_values() {
        let timestamp = Utc::now();

        // Test QoS 0
        let record0 = MessageRecord::new(timestamp, "t".to_string(), "p".to_string(), 0, false);
        assert_eq!(record0.qos, 0);

        // Test QoS 1
        let record1 = MessageRecord::new(timestamp, "t".to_string(), "p".to_string(), 1, false);
        assert_eq!(record1.qos, 1);

        // Test QoS 2
        let record2 = MessageRecord::new(timestamp, "t".to_string(), "p".to_string(), 2, false);
        assert_eq!(record2.qos, 2);
    }

    #[test]
    fn test_message_record_serialization() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            r#"{"value": 23.5}"#.to_string(),
            0,
            false,
        );

        // Test JSON serialization
        let json = serde_json::to_string(&record).expect("Failed to serialize");
        assert!(json.contains("sensors/temperature"));
        // The payload is stored as a string, so JSON escapes the inner quotes
        assert!(json.contains("value"));
        assert!(json.contains("23.5"));

        // Test JSON deserialization - this is the important test
        // The round-trip should preserve the original payload exactly
        let deserialized: MessageRecord =
            serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(record, deserialized);
        assert_eq!(deserialized.payload, r#"{"value": 23.5}"#);
    }

    #[test]
    fn test_message_record_with_special_characters_in_payload() {
        let timestamp = Utc::now();
        let payload_with_special_chars = "Hello, \"World\"!\nNew line\tTab";
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            payload_with_special_chars.to_string(),
            0,
            false,
        );

        assert_eq!(record.payload, payload_with_special_chars);
    }

    #[test]
    fn test_message_record_with_empty_payload() {
        let timestamp = Utc::now();
        let record =
            MessageRecord::new(timestamp, "test/topic".to_string(), String::new(), 0, false);

        assert!(record.payload.is_empty());
    }

    #[test]
    fn test_message_record_with_mqtt_wildcard_topic() {
        let timestamp = Utc::now();

        // Single-level wildcard
        let record_plus = MessageRecord::new(
            timestamp,
            "sensors/+/temperature".to_string(),
            "data".to_string(),
            0,
            false,
        );
        assert_eq!(record_plus.topic, "sensors/+/temperature");

        // Multi-level wildcard
        let record_hash = MessageRecord::new(
            timestamp,
            "sensors/#".to_string(),
            "data".to_string(),
            0,
            false,
        );
        assert_eq!(record_hash.topic, "sensors/#");
    }

    // MessageRecordBytes tests

    #[test]
    fn test_message_record_bytes_creation() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecordBytes::new(
            timestamp,
            "binary/data".to_string(),
            vec![0x08, 0x0A, 0x12, 0x18],
            0,
            false,
        );

        assert_eq!(record.timestamp, timestamp);
        assert_eq!(record.topic, "binary/data");
        assert_eq!(record.payload, vec![0x08, 0x0A, 0x12, 0x18]);
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_message_record_bytes_with_retain_flag() {
        let timestamp = Utc::now();
        let record = MessageRecordBytes::new(
            timestamp,
            "actuators/light".to_string(),
            b"ON".to_vec(),
            1,
            true,
        );

        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_message_record_bytes_clone() {
        let timestamp = Utc::now();
        let record = MessageRecordBytes::new(
            timestamp,
            "test/topic".to_string(),
            vec![1, 2, 3, 4, 5],
            2,
            false,
        );

        let cloned = record.clone();
        assert_eq!(record, cloned);
    }

    #[test]
    fn test_message_record_bytes_debug() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecordBytes::new(
            timestamp,
            "test".to_string(),
            vec![0xDE, 0xAD, 0xBE, 0xEF],
            0,
            false,
        );

        let debug_str = format!("{:?}", record);
        assert!(debug_str.contains("MessageRecordBytes"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_message_record_bytes_qos_values() {
        let timestamp = Utc::now();

        // Test QoS 0
        let record0 = MessageRecordBytes::new(timestamp, "t".to_string(), vec![1], 0, false);
        assert_eq!(record0.qos, 0);

        // Test QoS 1
        let record1 = MessageRecordBytes::new(timestamp, "t".to_string(), vec![1], 1, false);
        assert_eq!(record1.qos, 1);

        // Test QoS 2
        let record2 = MessageRecordBytes::new(timestamp, "t".to_string(), vec![1], 2, false);
        assert_eq!(record2.qos, 2);
    }

    #[test]
    fn test_message_record_bytes_with_empty_payload() {
        let timestamp = Utc::now();
        let record =
            MessageRecordBytes::new(timestamp, "test/topic".to_string(), Vec::new(), 0, false);

        assert!(record.payload.is_empty());
    }

    #[test]
    fn test_message_record_bytes_with_binary_data() {
        let timestamp = Utc::now();
        // Simulate protobuf-like binary data with control characters
        let binary_payload = vec![0x08, 0x00, 0x12, 0x0A, 0xFF, 0xFE];
        let record = MessageRecordBytes::new(
            timestamp,
            "binary/protobuf".to_string(),
            binary_payload.clone(),
            0,
            false,
        );

        assert_eq!(record.payload, binary_payload);
    }

    #[test]
    fn test_message_record_bytes_with_text_as_bytes() {
        let timestamp = Utc::now();
        let text = "Hello, World!";
        let record = MessageRecordBytes::new(
            timestamp,
            "text/topic".to_string(),
            text.as_bytes().to_vec(),
            0,
            false,
        );

        assert_eq!(record.payload, text.as_bytes());
        // Verify we can convert back to string
        assert_eq!(String::from_utf8(record.payload).unwrap(), text);
    }

    #[test]
    fn test_message_record_bytes_equality() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record1 =
            MessageRecordBytes::new(timestamp, "test".to_string(), vec![1, 2, 3], 0, false);
        let record2 =
            MessageRecordBytes::new(timestamp, "test".to_string(), vec![1, 2, 3], 0, false);
        let record3 = MessageRecordBytes::new(
            timestamp,
            "test".to_string(),
            vec![1, 2, 4], // Different payload
            0,
            false,
        );

        assert_eq!(record1, record2);
        assert_ne!(record1, record3);
    }

    // WriteStats tests

    #[test]
    fn test_write_stats_default() {
        let stats = WriteStats::default();
        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.auto_encoded_payloads, 0);
        assert_eq!(stats.largest_payload, 0);
    }

    #[test]
    fn test_write_stats_debug() {
        let stats = WriteStats {
            total_records: 100,
            text_payloads: 80,
            auto_encoded_payloads: 20,
            largest_payload: 1024,
        };
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("WriteStats"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("80"));
        assert!(debug_str.contains("20"));
        assert!(debug_str.contains("1024"));
    }

    #[test]
    fn test_write_stats_clone() {
        let stats = WriteStats {
            total_records: 50,
            text_payloads: 40,
            auto_encoded_payloads: 10,
            largest_payload: 512,
        };
        let cloned = stats.clone();
        assert_eq!(stats, cloned);
    }

    #[test]
    fn test_write_stats_equality() {
        let stats1 = WriteStats {
            total_records: 10,
            text_payloads: 8,
            auto_encoded_payloads: 2,
            largest_payload: 256,
        };
        let stats2 = WriteStats {
            total_records: 10,
            text_payloads: 8,
            auto_encoded_payloads: 2,
            largest_payload: 256,
        };
        let stats3 = WriteStats {
            total_records: 10,
            text_payloads: 7, // Different
            auto_encoded_payloads: 3,
            largest_payload: 256,
        };
        assert_eq!(stats1, stats2);
        assert_ne!(stats1, stats3);
    }

    #[test]
    fn test_csv_writer_stats_initial() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let writer = CsvWriter::new(&file_path, false).unwrap();
        let stats = writer.stats();

        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.auto_encoded_payloads, 0);
        assert_eq!(stats.largest_payload, 0);
    }

    // CsvWriter tests

    #[test]
    fn test_csv_writer_creates_file_with_header() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.starts_with("timestamp,topic,payload,qos,retain"));
    }

    #[test]
    fn test_csv_writer_writes_record_without_base64() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            r#"{"value": 23.5}"#.to_string(),
            0,
            false,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "timestamp,topic,payload,qos,retain");
        // The payload should be raw (not base64 encoded)
        // The csv crate may quote the payload due to special characters
        assert!(lines[1].contains("sensors/temperature"));
        // Check that the payload contains the JSON value (may be quoted by csv crate)
        assert!(lines[1].contains("value") && lines[1].contains("23.5"));
        assert!(lines[1].contains(",0,false"));
    }

    #[test]
    fn test_csv_writer_writes_record_with_base64() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            "Hello World".to_string(),
            1,
            true,
        );

        {
            let mut writer = CsvWriter::new(&file_path, true).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        // The payload should be base64 encoded: "Hello World" -> "SGVsbG8gV29ybGQ="
        assert!(lines[1].contains("SGVsbG8gV29ybGQ="));
        assert!(lines[1].contains(",1,true"));
    }

    #[test]
    fn test_csv_writer_escapes_special_characters() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        // Test payload with comma
        let record_comma = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "value1,value2".to_string(),
            0,
            false,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record_comma).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // The csv crate should quote fields containing commas
        assert!(content.contains("\"value1,value2\""));
    }

    #[test]
    fn test_csv_writer_escapes_double_quotes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            r#"He said "Hello""#.to_string(),
            0,
            false,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // RFC 4180: double quotes are escaped by doubling them
        assert!(content.contains(r#""He said ""Hello""""#));
    }

    #[test]
    fn test_csv_writer_escapes_newlines() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "line1\nline2".to_string(),
            0,
            false,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // The csv crate should quote fields containing newlines
        assert!(content.contains("\"line1\nline2\""));
    }

    #[test]
    fn test_csv_writer_writes_multiple_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let timestamp2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 1).unwrap();

        let record1 = MessageRecord::new(
            timestamp1,
            "topic1".to_string(),
            "payload1".to_string(),
            0,
            false,
        );

        let record2 = MessageRecord::new(
            timestamp2,
            "topic2".to_string(),
            "payload2".to_string(),
            1,
            true,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record1).unwrap();
            writer.write(&record2).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 3); // header + 2 records
        assert!(lines[1].contains("topic1"));
        assert!(lines[2].contains("topic2"));
    }

    #[test]
    fn test_csv_writer_timestamp_format() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Create a timestamp with milliseconds
        let timestamp = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap()
            .checked_add_signed(chrono::Duration::milliseconds(123))
            .unwrap();

        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "payload".to_string(),
            0,
            false,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // Check ISO 8601 format with milliseconds
        assert!(content.contains("2024-01-15T10:30:00.123Z"));
    }

    #[test]
    fn test_csv_writer_empty_payload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let record =
            MessageRecord::new(timestamp, "test/topic".to_string(), String::new(), 0, false);

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        // Empty payload should result in empty field
        assert!(lines[1].contains("test/topic,,0,false"));
    }

    #[test]
    fn test_csv_writer_all_qos_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();

            for qos in 0..=2 {
                let record = MessageRecord::new(
                    timestamp,
                    format!("topic/qos{}", qos),
                    "payload".to_string(),
                    qos,
                    false,
                );
                writer.write(&record).unwrap();
            }
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains(",0,false"));
        assert!(content.contains(",1,false"));
        assert!(content.contains(",2,false"));
    }

    #[test]
    fn test_csv_writer_retain_flag_values() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();

            let record_false = MessageRecord::new(
                timestamp,
                "topic1".to_string(),
                "payload".to_string(),
                0,
                false,
            );
            writer.write(&record_false).unwrap();

            let record_true = MessageRecord::new(
                timestamp,
                "topic2".to_string(),
                "payload".to_string(),
                0,
                true,
            );
            writer.write(&record_true).unwrap();

            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains(",false\n") || content.contains(",false\r\n"));
        assert!(
            content.contains(",true\n")
                || content.contains(",true\r\n")
                || content.ends_with(",true")
        );
    }

    // CsvReader tests

    #[test]
    fn test_csv_reader_reads_single_record() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let original_record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            "payload data".to_string(),
            0,
            false,
        );

        // Write a record
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&original_record).unwrap();
            writer.flush().unwrap();
        }

        // Read it back
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.next().unwrap().unwrap();

        assert_eq!(read_record.topic, original_record.topic);
        assert_eq!(read_record.payload, original_record.payload);
        assert_eq!(read_record.qos, original_record.qos);
        assert_eq!(read_record.retain, original_record.retain);
    }

    #[test]
    fn test_csv_reader_reads_multiple_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let timestamp2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 1).unwrap();

        let record1 = MessageRecord::new(
            timestamp1,
            "topic1".to_string(),
            "payload1".to_string(),
            0,
            false,
        );
        let record2 = MessageRecord::new(
            timestamp2,
            "topic2".to_string(),
            "payload2".to_string(),
            1,
            true,
        );

        // Write records
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record1).unwrap();
            writer.write(&record2).unwrap();
            writer.flush().unwrap();
        }

        // Read them back
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        let read1 = reader.next().unwrap().unwrap();
        assert_eq!(read1.topic, "topic1");
        assert_eq!(read1.payload, "payload1");
        assert_eq!(read1.qos, 0);
        assert!(!read1.retain);

        let read2 = reader.next().unwrap().unwrap();
        assert_eq!(read2.topic, "topic2");
        assert_eq!(read2.payload, "payload2");
        assert_eq!(read2.qos, 1);
        assert!(read2.retain);

        // No more records
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_csv_reader_with_base64_decoding() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let original_payload = "Hello World";
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            original_payload.to_string(),
            0,
            false,
        );

        // Write with base64 encoding
        {
            let mut writer = CsvWriter::new(&file_path, true).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read with base64 decoding
        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let read_record = reader.next().unwrap().unwrap();

        assert_eq!(read_record.payload, original_payload);
    }

    #[test]
    fn test_csv_reader_without_base64_decoding() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "raw payload".to_string(),
            0,
            false,
        );

        // Write without base64 encoding
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read without base64 decoding
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.next().unwrap().unwrap();

        assert_eq!(read_record.payload, "raw payload");
    }

    #[test]
    fn test_csv_reader_preserves_special_characters() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let special_payload = "value1,value2\nline2\t\"quoted\"";
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            special_payload.to_string(),
            0,
            false,
        );

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.next().unwrap().unwrap();

        assert_eq!(read_record.payload, special_payload);
    }

    #[test]
    fn test_csv_reader_reset() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "payload".to_string(),
            0,
            false,
        );

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read all records
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let _ = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());

        // Reset and read again
        reader.reset().unwrap();
        let read_record = reader.next().unwrap().unwrap();
        assert_eq!(read_record.topic, "test/topic");
        assert!(reader.next().is_none());
    }

    #[test]
    fn test_csv_reader_reset_multiple_times() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "payload".to_string(),
            0,
            false,
        );

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        // Read, reset, and read multiple times
        for _ in 0..3 {
            let read_record = reader.next().unwrap().unwrap();
            assert_eq!(read_record.topic, "test/topic");
            assert!(reader.next().is_none());
            reader.reset().unwrap();
        }
    }

    #[test]
    fn test_csv_reader_iterator_trait() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        // Write multiple records
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            for i in 0..5 {
                let record = MessageRecord::new(
                    timestamp,
                    format!("topic{}", i),
                    format!("payload{}", i),
                    0,
                    false,
                );
                writer.write(&record).unwrap();
            }
            writer.flush().unwrap();
        }

        // Use iterator to collect all records
        let reader = CsvReader::new(&file_path, false, None).unwrap();
        let records: Vec<_> = reader.collect();

        assert_eq!(records.len(), 5);
        for (i, result) in records.iter().enumerate() {
            let record = result.as_ref().unwrap();
            assert_eq!(record.topic, format!("topic{}", i));
            assert_eq!(record.payload, format!("payload{}", i));
        }
    }

    #[test]
    fn test_csv_reader_preserves_qos_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        // Write records with different QoS levels
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            for qos in 0..=2 {
                let record = MessageRecord::new(
                    timestamp,
                    format!("topic{}", qos),
                    "payload".to_string(),
                    qos,
                    false,
                );
                writer.write(&record).unwrap();
            }
            writer.flush().unwrap();
        }

        // Read and verify QoS levels
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        for expected_qos in 0..=2 {
            let record = reader.next().unwrap().unwrap();
            assert_eq!(record.qos, expected_qos);
        }
    }

    #[test]
    fn test_csv_reader_preserves_retain_flag() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        // Write records with different retain flags
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            let record_false = MessageRecord::new(
                timestamp,
                "topic1".to_string(),
                "payload".to_string(),
                0,
                false,
            );
            let record_true = MessageRecord::new(
                timestamp,
                "topic2".to_string(),
                "payload".to_string(),
                0,
                true,
            );
            writer.write(&record_false).unwrap();
            writer.write(&record_true).unwrap();
            writer.flush().unwrap();
        }

        // Read and verify retain flags
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read1 = reader.next().unwrap().unwrap();
        assert!(!read1.retain);
        let read2 = reader.next().unwrap().unwrap();
        assert!(read2.retain);
    }

    #[test]
    fn test_csv_reader_empty_payload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record =
            MessageRecord::new(timestamp, "test/topic".to_string(), String::new(), 0, false);

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.next().unwrap().unwrap();
        assert!(read_record.payload.is_empty());
    }

    #[test]
    fn test_csv_reader_file_not_found() {
        let result = CsvReader::new(Path::new("/nonexistent/path/file.csv"), false, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_csv_reader_with_field_size_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let small_payload = "small";
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            small_payload.to_string(),
            0,
            false,
        );

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read with a field size limit that allows the record
        let mut reader = CsvReader::new(&file_path, false, Some(1024)).unwrap();
        let read_record = reader.next().unwrap().unwrap();
        assert_eq!(read_record.payload, small_payload);
    }

    #[test]
    fn test_csv_reader_field_size_limit_exceeded() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        // Create a payload larger than the limit we'll set
        let large_payload = "x".repeat(100);
        let record =
            MessageRecord::new(timestamp, "test/topic".to_string(), large_payload, 0, false);

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read with a very small field size limit
        let mut reader = CsvReader::new(&file_path, false, Some(10)).unwrap();
        let result = reader.next().unwrap();

        // Should fail due to field size limit
        assert!(result.is_err());
    }

    #[test]
    fn test_csv_reader_timestamp_parsing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Create a timestamp with milliseconds
        let timestamp = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap()
            .checked_add_signed(chrono::Duration::milliseconds(123))
            .unwrap();

        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "payload".to_string(),
            0,
            false,
        );

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read and verify timestamp
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.next().unwrap().unwrap();

        // Timestamps should match (within millisecond precision)
        assert_eq!(
            read_record
                .timestamp
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string(),
            timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
        );
    }

    #[test]
    fn test_csv_roundtrip_preserves_all_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap()
            .checked_add_signed(chrono::Duration::milliseconds(456))
            .unwrap();

        let original = MessageRecord::new(
            timestamp,
            "sensors/room1/temperature".to_string(),
            r#"{"value": 23.5, "unit": "celsius"}"#.to_string(),
            2,
            true,
        );

        // Write
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&original).unwrap();
            writer.flush().unwrap();
        }

        // Read
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read = reader.next().unwrap().unwrap();

        // Verify all fields
        assert_eq!(read.topic, original.topic);
        assert_eq!(read.payload, original.payload);
        assert_eq!(read.qos, original.qos);
        assert_eq!(read.retain, original.retain);
        // Verify timestamp (compare formatted strings for millisecond precision)
        assert_eq!(
            read.timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            original
                .timestamp
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string()
        );
    }

    #[test]
    fn test_csv_roundtrip_with_base64() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let original_payload = "Binary data: \x00\x01\x02 and text";
        let original = MessageRecord::new(
            timestamp,
            "test/binary".to_string(),
            original_payload.to_string(),
            1,
            false,
        );

        // Write with base64 encoding
        {
            let mut writer = CsvWriter::new(&file_path, true).unwrap();
            writer.write(&original).unwrap();
            writer.flush().unwrap();
        }

        // Read with base64 decoding
        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let read = reader.next().unwrap().unwrap();

        assert_eq!(read.payload, original_payload);
    }

    // Tests for is_binary_payload function and AUTO_ENCODE_MARKER constant

    #[test]
    fn test_auto_encode_marker_value() {
        assert_eq!(super::AUTO_ENCODE_MARKER, "b64:");
    }

    #[test]
    fn test_is_binary_payload_empty() {
        // Empty payload is not binary (Requirement 1.4)
        assert!(!super::is_binary_payload(&[]));
    }

    #[test]
    fn test_is_binary_payload_plain_ascii() {
        // Plain ASCII text is not binary
        assert!(!super::is_binary_payload(b"Hello, World!"));
        assert!(!super::is_binary_payload(b"Simple text"));
        assert!(!super::is_binary_payload(b"123456789"));
        assert!(!super::is_binary_payload(b"!@#$%^&*()"));
    }

    #[test]
    fn test_is_binary_payload_utf8_text() {
        // Valid UTF-8 text with non-ASCII characters is not binary
        assert!(!super::is_binary_payload("Hello 🌍".as_bytes()));
        assert!(!super::is_binary_payload("Привет мир".as_bytes()));
        assert!(!super::is_binary_payload("日本語テスト".as_bytes()));
        assert!(!super::is_binary_payload("Ñoño".as_bytes()));
    }

    #[test]
    fn test_is_binary_payload_allowed_whitespace() {
        // Tab (0x09), newline (0x0A), and carriage return (0x0D) are allowed
        // (Requirement 1.5)
        assert!(!super::is_binary_payload(b"line1\nline2"));
        assert!(!super::is_binary_payload(b"col1\tcol2"));
        assert!(!super::is_binary_payload(b"line1\r\nline2"));
        assert!(!super::is_binary_payload(b"mixed\ttab\nand\rnewlines"));
    }

    #[test]
    fn test_is_binary_payload_nul_byte() {
        // NUL byte (0x00) indicates binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"Hello\x00World"));
        assert!(super::is_binary_payload(&[0x00]));
        assert!(super::is_binary_payload(b"\x00"));
    }

    #[test]
    fn test_is_binary_payload_control_chars_0x01_to_0x08() {
        // Control characters 0x01-0x08 indicate binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"test\x01data")); // SOH
        assert!(super::is_binary_payload(b"test\x02data")); // STX
        assert!(super::is_binary_payload(b"test\x03data")); // ETX
        assert!(super::is_binary_payload(b"test\x04data")); // EOT
        assert!(super::is_binary_payload(b"test\x05data")); // ENQ
        assert!(super::is_binary_payload(b"test\x06data")); // ACK
        assert!(super::is_binary_payload(b"test\x07data")); // BEL
        assert!(super::is_binary_payload(b"test\x08data")); // BS (backspace)
    }

    #[test]
    fn test_is_binary_payload_control_chars_0x0b_to_0x0c() {
        // Control characters 0x0B-0x0C indicate binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"test\x0Bdata")); // VT (vertical tab)
        assert!(super::is_binary_payload(b"test\x0Cdata")); // FF (form feed)
    }

    #[test]
    fn test_is_binary_payload_control_chars_0x0e_to_0x1f() {
        // Control characters 0x0E-0x1F indicate binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"test\x0Edata")); // SO
        assert!(super::is_binary_payload(b"test\x0Fdata")); // SI
        assert!(super::is_binary_payload(b"test\x10data")); // DLE
        assert!(super::is_binary_payload(b"test\x1Bdata")); // ESC
        assert!(super::is_binary_payload(b"test\x1Fdata")); // US
    }

    #[test]
    fn test_is_binary_payload_invalid_utf8() {
        // Invalid UTF-8 sequences indicate binary (Requirement 1.1)
        assert!(super::is_binary_payload(&[0xFF, 0xFE]));
        assert!(super::is_binary_payload(&[0x80, 0x81, 0x82]));
        assert!(super::is_binary_payload(&[0xC0, 0xC1])); // Overlong encoding
        assert!(super::is_binary_payload(&[0xF5, 0xF6, 0xF7])); // Invalid start bytes
    }

    #[test]
    fn test_is_binary_payload_protobuf_like() {
        // Protobuf-like binary data should be detected as binary
        let protobuf_data = vec![
            0x08, 0x96, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6E, 0x67,
        ];
        assert!(super::is_binary_payload(&protobuf_data));
    }

    #[test]
    fn test_is_binary_payload_json() {
        // JSON payloads are not binary
        assert!(!super::is_binary_payload(br#"{"key": "value"}"#));
        assert!(!super::is_binary_payload(
            br#"{"temperature": 23.5, "unit": "celsius"}"#
        ));
        assert!(!super::is_binary_payload(br#"[1, 2, 3, 4, 5]"#));
    }

    #[test]
    fn test_is_binary_payload_xml() {
        // XML payloads are not binary
        assert!(!super::is_binary_payload(
            b"<root><child>value</child></root>"
        ));
        assert!(!super::is_binary_payload(b"<?xml version=\"1.0\"?><data/>"));
    }

    #[test]
    fn test_is_binary_payload_printable_range() {
        // All printable ASCII characters (0x20-0x7E) are not binary
        let printable: Vec<u8> = (0x20..=0x7E).collect();
        assert!(!super::is_binary_payload(&printable));
    }

    #[test]
    fn test_is_binary_payload_mixed_valid_and_control() {
        // If any control character is present, the payload is binary
        assert!(super::is_binary_payload(b"Valid text\x00with NUL"));
        assert!(super::is_binary_payload(b"Start\x08middle\x00end"));
    }

    #[test]
    fn test_is_binary_control_char() {
        // Test the helper function directly
        // Binary control characters
        assert!(super::is_binary_control_char(0x00)); // NUL
        assert!(super::is_binary_control_char(0x01)); // SOH
        assert!(super::is_binary_control_char(0x08)); // BS
        assert!(super::is_binary_control_char(0x0B)); // VT
        assert!(super::is_binary_control_char(0x0C)); // FF
        assert!(super::is_binary_control_char(0x0E)); // SO
        assert!(super::is_binary_control_char(0x1F)); // US

        // Allowed whitespace characters
        assert!(!super::is_binary_control_char(0x09)); // TAB
        assert!(!super::is_binary_control_char(0x0A)); // LF
        assert!(!super::is_binary_control_char(0x0D)); // CR

        // Regular printable characters
        assert!(!super::is_binary_control_char(0x20)); // Space
        assert!(!super::is_binary_control_char(0x41)); // 'A'
        assert!(!super::is_binary_control_char(0x7E)); // '~'
    }

    // Tests for write_bytes method

    #[test]
    fn test_write_bytes_text_payload_no_encoding() {
        // Requirement 2.2: When encode_b64 is false and payload is text, write as-is
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", b"Hello World", 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // Payload should be written as-is without any prefix
        assert!(content.contains("Hello World"));
        assert!(!content.contains("b64:"));
    }

    #[test]
    fn test_write_bytes_binary_payload_auto_encode() {
        // Requirement 2.1: When encode_b64 is false and payload is binary,
        // base64 encode with "b64:" prefix
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let binary_payload = vec![0x08, 0x0A, 0x12, 0x18]; // Contains control chars

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "binary/data", &binary_payload, 0, true)
                .unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // Payload should be prefixed with "b64:" and base64 encoded
        assert!(content.contains("b64:"));
        // Base64 of [0x08, 0x0A, 0x12, 0x18] is "CAoSGA=="
        assert!(content.contains("b64:CAoSGA=="));
    }

    #[test]
    fn test_write_bytes_global_base64_encoding() {
        // Requirement 2.3: When encode_b64 is true, base64 encode without prefix
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, true).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", b"Hello World", 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // Payload should be base64 encoded without prefix
        // "Hello World" -> "SGVsbG8gV29ybGQ="
        assert!(content.contains("SGVsbG8gV29ybGQ="));
        assert!(!content.contains("b64:"));
    }

    #[test]
    fn test_write_bytes_updates_stats_text() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let mut writer = CsvWriter::new(&file_path, false).unwrap();
        writer
            .write_bytes(timestamp, "test/topic", b"Hello World", 0, false)
            .unwrap();

        let stats = writer.stats();
        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.text_payloads, 1);
        assert_eq!(stats.auto_encoded_payloads, 0);
        assert_eq!(stats.largest_payload, 11); // "Hello World" is 11 bytes
    }

    #[test]
    fn test_write_bytes_updates_stats_binary() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let binary_payload = vec![0x08, 0x0A, 0x12, 0x18];

        let mut writer = CsvWriter::new(&file_path, false).unwrap();
        writer
            .write_bytes(timestamp, "binary/data", &binary_payload, 0, false)
            .unwrap();

        let stats = writer.stats();
        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.auto_encoded_payloads, 1);
        assert_eq!(stats.largest_payload, 4);
    }

    #[test]
    fn test_write_bytes_updates_stats_mixed() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let mut writer = CsvWriter::new(&file_path, false).unwrap();

        // Write text payload
        writer
            .write_bytes(timestamp, "text/topic", b"Hello", 0, false)
            .unwrap();

        // Write binary payload
        writer
            .write_bytes(timestamp, "binary/topic", &[0x00, 0x01, 0x02], 0, false)
            .unwrap();

        // Write another text payload (larger)
        writer
            .write_bytes(timestamp, "text/topic2", b"Hello World!", 0, false)
            .unwrap();

        let stats = writer.stats();
        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.text_payloads, 2);
        assert_eq!(stats.auto_encoded_payloads, 1);
        assert_eq!(stats.largest_payload, 12); // "Hello World!" is 12 bytes
    }

    #[test]
    fn test_write_bytes_empty_payload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", &[], 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        // Empty payload should result in empty field
        assert!(lines[1].contains("test/topic,,0,false"));

        // Stats should show text payload (empty is classified as text)
        let mut writer = CsvWriter::new(&file_path, false).unwrap();
        writer
            .write_bytes(timestamp, "test/topic", &[], 0, false)
            .unwrap();
        let stats = writer.stats();
        assert_eq!(stats.text_payloads, 1);
        assert_eq!(stats.auto_encoded_payloads, 0);
    }

    #[test]
    fn test_write_bytes_preserves_all_fields() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
            .unwrap()
            .checked_add_signed(chrono::Duration::milliseconds(456))
            .unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "sensors/room1/temp", b"23.5", 2, true)
                .unwrap();
            writer.flush().unwrap();
        }

        // Read back and verify
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.next().unwrap().unwrap();

        assert_eq!(record.topic, "sensors/room1/temp");
        assert_eq!(record.payload, "23.5");
        assert_eq!(record.qos, 2);
        assert!(record.retain);
        assert_eq!(
            record
                .timestamp
                .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                .to_string(),
            "2024-01-15T10:30:00.456Z"
        );
    }

    #[test]
    fn test_write_bytes_utf8_with_special_chars() {
        // UTF-8 text with tabs and newlines should be written as-is
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let payload = b"line1\nline2\ttab";

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", payload, 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        // Read back and verify
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.next().unwrap().unwrap();
        assert_eq!(record.payload, "line1\nline2\ttab");
    }

    #[test]
    fn test_write_bytes_invalid_utf8_auto_encodes() {
        // Invalid UTF-8 should be auto-encoded
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let invalid_utf8 = vec![0xFF, 0xFE, 0x00, 0x01];

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", &invalid_utf8, 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        // Should be auto-encoded with b64: prefix
        assert!(content.contains("b64:"));

        let stats_writer = CsvWriter::new(&file_path, false).unwrap();
        // Create a new writer to check stats
        let temp_dir2 = tempfile::tempdir().unwrap();
        let file_path2 = temp_dir2.path().join("test2.csv");
        let mut writer2 = CsvWriter::new(&file_path2, false).unwrap();
        writer2
            .write_bytes(timestamp, "test/topic", &invalid_utf8, 0, false)
            .unwrap();
        assert_eq!(writer2.stats().auto_encoded_payloads, 1);
        drop(stats_writer);
    }

    #[test]
    fn test_write_bytes_json_payload() {
        // JSON payloads should be written as-is
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let json_payload = br#"{"temperature": 23.5, "unit": "celsius"}"#;

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "sensors/temp", json_payload, 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        // Read back and verify
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.next().unwrap().unwrap();
        assert_eq!(
            record.payload,
            r#"{"temperature": 23.5, "unit": "celsius"}"#
        );
    }

    #[test]
    fn test_write_bytes_all_qos_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            for qos in 0..=2 {
                writer
                    .write_bytes(
                        timestamp,
                        &format!("topic/qos{}", qos),
                        b"payload",
                        qos,
                        false,
                    )
                    .unwrap();
            }
            writer.flush().unwrap();
        }

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains(",0,false"));
        assert!(content.contains(",1,false"));
        assert!(content.contains(",2,false"));
    }

    // ==================== read_next_bytes tests ====================

    #[test]
    fn test_read_next_bytes_text_payload() {
        // When decode_b64 is false and payload has no prefix, return as bytes
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", b"Hello World", 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, b"Hello World");
        assert_eq!(record.topic, "test/topic");
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_read_next_bytes_auto_encoded_payload() {
        // When decode_b64 is false and payload starts with "b64:", strip prefix and decode
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let binary_payload = vec![0x08, 0x0A, 0x12, 0x18, 0x00, 0xFF];

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "binary/data", &binary_payload, 1, true)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, binary_payload);
        assert_eq!(record.topic, "binary/data");
        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_read_next_bytes_global_base64_decode() {
        // When decode_b64 is true, decode all payloads from base64
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, true).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", b"Hello World", 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, b"Hello World");
    }

    #[test]
    fn test_read_next_bytes_invalid_base64_in_auto_encoded() {
        // When payload has "b64:" prefix but invalid base64, return error with line number
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with invalid base64 after b64: prefix
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,test/topic,b64:not-valid-base64!!!,0,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Line 2"));
        assert!(error_msg.contains("Invalid base64"));
        assert!(error_msg.contains("auto-encoded"));
    }

    #[test]
    fn test_read_next_bytes_invalid_base64_global_decode() {
        // When decode_b64 is true and payload is invalid base64, return error with line number
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with invalid base64
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,test/topic,not-valid-base64!!!,0,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let result = reader.read_next_bytes().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Line 2"));
        assert!(error_msg.contains("Invalid base64"));
    }

    #[test]
    fn test_read_next_bytes_empty_payload() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", &[], 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert!(record.payload.is_empty());
    }

    #[test]
    fn test_read_next_bytes_multiple_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            // Text payload
            writer
                .write_bytes(timestamp, "text/topic", b"Hello", 0, false)
                .unwrap();
            // Binary payload (will be auto-encoded)
            writer
                .write_bytes(timestamp, "binary/topic", &[0x00, 0x01, 0x02], 1, true)
                .unwrap();
            // Another text payload
            writer
                .write_bytes(timestamp, "text/topic2", b"World", 2, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        // First record - text
        let record1 = reader.read_next_bytes().unwrap().unwrap();
        assert_eq!(record1.payload, b"Hello");
        assert_eq!(record1.topic, "text/topic");

        // Second record - binary (auto-decoded)
        let record2 = reader.read_next_bytes().unwrap().unwrap();
        assert_eq!(record2.payload, vec![0x00, 0x01, 0x02]);
        assert_eq!(record2.topic, "binary/topic");

        // Third record - text
        let record3 = reader.read_next_bytes().unwrap().unwrap();
        assert_eq!(record3.payload, b"World");
        assert_eq!(record3.topic, "text/topic2");

        // No more records
        assert!(reader.read_next_bytes().is_none());
    }

    #[test]
    fn test_read_next_bytes_preserves_qos_levels() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            for qos in 0..=2 {
                writer
                    .write_bytes(
                        timestamp,
                        &format!("topic/qos{}", qos),
                        b"payload",
                        qos,
                        false,
                    )
                    .unwrap();
            }
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        for expected_qos in 0..=2 {
            let record = reader.read_next_bytes().unwrap().unwrap();
            assert_eq!(record.qos, expected_qos);
        }
    }

    #[test]
    fn test_read_next_bytes_preserves_retain_flag() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "topic/retain", b"payload", 0, true)
                .unwrap();
            writer
                .write_bytes(timestamp, "topic/no-retain", b"payload", 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        let record1 = reader.read_next_bytes().unwrap().unwrap();
        assert!(record1.retain);

        let record2 = reader.read_next_bytes().unwrap().unwrap();
        assert!(!record2.retain);
    }

    #[test]
    fn test_read_next_bytes_invalid_qos() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with invalid QoS
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,test/topic,payload,3,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Line 2"));
        assert!(error_msg.contains("QoS"));
    }

    #[test]
    fn test_read_next_bytes_invalid_retain() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with invalid retain flag
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,test/topic,payload,0,maybe\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Line 2"));
        assert!(error_msg.contains("retain"));
    }

    #[test]
    fn test_read_next_bytes_invalid_timestamp() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with invalid timestamp
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             not-a-timestamp,test/topic,payload,0,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Line 2"));
        assert!(error_msg.contains("timestamp"));
    }

    #[test]
    fn test_read_next_bytes_wrong_field_count() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with wrong number of fields
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,test/topic,payload\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        // The error should mention the field count issue
        assert!(
            error_msg.contains("5 fields") || error_msg.contains("Expected 5"),
            "Error message should mention field count: {}",
            error_msg
        );
    }

    #[test]
    fn test_read_next_bytes_roundtrip_binary() {
        // Verify round-trip integrity for binary payloads
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let binary_payload: Vec<u8> = (0..=255).collect(); // All possible byte values

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "binary/all-bytes", &binary_payload, 2, true)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, binary_payload);
        assert_eq!(record.topic, "binary/all-bytes");
        assert_eq!(record.qos, 2);
        assert!(record.retain);
    }

    #[test]
    fn test_read_next_bytes_line_number_tracking() {
        // Verify line numbers are correctly tracked across multiple records
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Write CSV with error on line 4 (3rd data row)
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,topic1,payload1,0,false\n\
             2024-01-15T10:30:01.000Z,topic2,payload2,1,true\n\
             2024-01-15T10:30:02.000Z,topic3,payload3,invalid,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        // First two records should succeed
        assert!(reader.read_next_bytes().unwrap().is_ok());
        assert!(reader.read_next_bytes().unwrap().is_ok());

        // Third record should fail with line 4
        let result = reader.read_next_bytes().unwrap();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Line 4"));
    }

    #[test]
    fn test_read_next_bytes_reset_resets_line_counter() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Write CSV with error on line 2 (1st data row)
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             invalid-timestamp,topic1,payload1,0,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();

        // First read should fail with line 2
        let result1 = reader.read_next_bytes().unwrap();
        assert!(result1.is_err());
        assert!(result1.unwrap_err().to_string().contains("Line 2"));

        // Reset and read again - should still report line 2
        reader.reset().unwrap();
        let result2 = reader.read_next_bytes().unwrap();
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Line 2"));
    }

    #[test]
    fn test_read_next_auto_encoded_payload() {
        // When decode_b64 is false and payload starts with "b64:", strip prefix and decode
        // Then convert to UTF-8 string (lossy for binary data)
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let binary_payload = vec![0x08, 0x0A, 0x12, 0x18, 0x00, 0xFF];

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "binary/data", &binary_payload, 1, true)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next().unwrap().unwrap();

        // The payload should be decoded and converted to UTF-8 (lossy)
        // Since the binary data contains non-UTF8 bytes, they will be replaced with U+FFFD
        let expected_payload = String::from_utf8_lossy(&binary_payload).into_owned();
        assert_eq!(record.payload, expected_payload);
        assert_eq!(record.topic, "binary/data");
        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_read_next_auto_encoded_text_payload() {
        // When decode_b64 is false and payload starts with "b64:", strip prefix and decode
        // For valid UTF-8 text, the result should be the original text
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let text_payload = "Hello, World! 🌍";

        // Manually write a CSV with b64: prefix (simulating auto-encoded text)
        let encoded = BASE64_STANDARD.encode(text_payload.as_bytes());
        std::fs::write(
            &file_path,
            format!(
                "timestamp,topic,payload,qos,retain\n\
                 2024-01-15T10:30:00.000Z,test/topic,b64:{},0,false\n",
                encoded
            ),
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next().unwrap().unwrap();

        assert_eq!(record.payload, text_payload);
    }

    #[test]
    fn test_read_next_text_payload_no_prefix() {
        // When decode_b64 is false and payload does not start with "b64:", return as-is
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "text/topic", b"Hello World", 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next().unwrap().unwrap();

        assert_eq!(record.payload, "Hello World");
    }

    #[test]
    fn test_read_next_global_base64_decode_lossy() {
        // When decode_b64 is true, decode all payloads from base64
        // and convert to UTF-8 (lossy for binary data)
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let binary_payload = vec![0x08, 0x0A, 0x12, 0x18, 0x00, 0xFF];

        {
            let mut writer = CsvWriter::new(&file_path, true).unwrap();
            writer
                .write_bytes(timestamp, "binary/data", &binary_payload, 0, false)
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let record = reader.read_next().unwrap().unwrap();

        // The payload should be decoded and converted to UTF-8 (lossy)
        let expected_payload = String::from_utf8_lossy(&binary_payload).into_owned();
        assert_eq!(record.payload, expected_payload);
    }

    #[test]
    fn test_read_next_invalid_base64_in_auto_encoded() {
        // When payload has "b64:" prefix but invalid base64, return error
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        // Manually write a CSV with invalid base64 after b64: prefix
        std::fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\n\
             2024-01-15T10:30:00.000Z,test/topic,b64:not-valid-base64!!!,0,false\n",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next().unwrap();

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid base64"));
        assert!(error_msg.contains("auto-encoded"));
    }

    // Tests for marker collision handling (Requirement 5.4)

    #[test]
    fn test_marker_collision_text_starting_with_b64_prefix() {
        // Requirement 5.4: When a text payload literally starts with "b64:",
        // it should round-trip correctly without being misinterpreted as auto-encoded.
        // The solution is to auto-encode such payloads to avoid ambiguity.
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        // This payload literally starts with "b64:" but is NOT base64 encoded
        let payload_with_marker = b"b64:this is not actually base64";

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", payload_with_marker, 0, false)
                .unwrap();
            writer.flush().unwrap();

            // The payload should be auto-encoded to avoid collision
            let stats = writer.stats();
            assert_eq!(stats.auto_encoded_payloads, 1);
            assert_eq!(stats.text_payloads, 0);
        }

        // Read back and verify the payload is preserved exactly
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, payload_with_marker);
        assert_eq!(record.topic, "test/topic");
    }

    #[test]
    fn test_marker_collision_exact_b64_prefix() {
        // Test with payload that is exactly "b64:"
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let payload = b"b64:";

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", payload, 0, false)
                .unwrap();
            writer.flush().unwrap();

            // Should be auto-encoded
            assert_eq!(writer.stats().auto_encoded_payloads, 1);
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_marker_collision_b64_prefix_with_valid_base64_content() {
        // Test with payload that starts with "b64:" followed by valid base64
        // This should still round-trip correctly
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        // "b64:SGVsbG8=" is literally the text "b64:" followed by base64 of "Hello"
        let payload = b"b64:SGVsbG8=";

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", payload, 0, false)
                .unwrap();
            writer.flush().unwrap();

            // Should be auto-encoded to avoid misinterpretation
            assert_eq!(writer.stats().auto_encoded_payloads, 1);
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        // The payload should be exactly "b64:SGVsbG8=", NOT "Hello"
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_marker_collision_via_write_method() {
        // Test marker collision handling via the write() method (string payload)
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "b64:some literal text".to_string(),
            0,
            false,
        );

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();

            // Should be auto-encoded
            assert_eq!(writer.stats().auto_encoded_payloads, 1);
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.read_next().unwrap().unwrap();

        assert_eq!(read_record.payload, "b64:some literal text");
    }

    #[test]
    fn test_no_marker_collision_for_normal_text() {
        // Verify that normal text payloads are NOT auto-encoded
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let payload = b"Hello World";

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", payload, 0, false)
                .unwrap();
            writer.flush().unwrap();

            // Should NOT be auto-encoded
            assert_eq!(writer.stats().text_payloads, 1);
            assert_eq!(writer.stats().auto_encoded_payloads, 0);
        }

        // Verify the CSV contains the raw payload, not base64
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("Hello World"));
        assert!(!content.contains("b64:"));
    }

    #[test]
    fn test_marker_collision_case_sensitive() {
        // Verify that "B64:" (uppercase) is NOT treated as a marker collision
        // Only "b64:" (lowercase) is the marker
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let payload = b"B64:uppercase prefix";

        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer
                .write_bytes(timestamp, "test/topic", payload, 0, false)
                .unwrap();
            writer.flush().unwrap();

            // Should NOT be auto-encoded (uppercase B64: is not the marker)
            assert_eq!(writer.stats().text_payloads, 1);
            assert_eq!(writer.stats().auto_encoded_payloads, 0);
        }

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(record.payload, payload);
    }
}
