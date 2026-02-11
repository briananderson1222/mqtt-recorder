use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Utc};
use csv::Writer;
use std::fs::File;
use std::path::Path;

use super::encoding::{is_binary_payload, AUTO_ENCODE_MARKER};
use super::record::{MessageRecord, WriteStats};
use crate::error::MqttRecorderError;

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
pub struct CsvWriter {
    /// The underlying CSV writer wrapping a file handle.
    writer: Writer<File>,
    /// Whether to encode payloads as base64.
    encode_b64: bool,
    /// Statistics for written records.
    stats: WriteStats,
}

impl CsvWriter {
    /// Creates a new CSV writer, writing the header row to the file.
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
    pub fn flush(&mut self) -> Result<(), MqttRecorderError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Write a message record from raw bytes (for binary payloads).
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
    #[allow(dead_code)] // Public API for library users
    pub fn stats(&self) -> &WriteStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
    }
}
