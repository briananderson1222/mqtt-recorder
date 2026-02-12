use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Utc};
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::{Path, PathBuf};

use super::encoding::AUTO_ENCODE_MARKER;
use super::record::{MessageRecord, MessageRecordBytes};
use crate::error::MqttRecorderError;

/// Intermediate parsed fields shared between parse_record and parse_record_bytes.
#[derive(Debug)]
struct ParsedFields {
    timestamp: DateTime<Utc>,
    topic: String,
    payload_str: String,
    qos: u8,
    retain: bool,
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
    pub fn read_next(&mut self) -> Option<Result<MessageRecord, MqttRecorderError>> {
        // Get the next record from the CSV reader
        let result = self.reader.records().next()?;

        // Increment line counter for error reporting
        self.current_line += 1;

        Some(self.parse_record(result))
    }

    /// Resets the reader to the beginning of the file.
    pub fn reset(&mut self) -> Result<(), MqttRecorderError> {
        // Recreate the reader from the stored path
        self.reader = Self::create_reader(&self.path, self.field_size_limit)?;
        // Reset line counter (header row is line 1, first data row is line 2)
        self.current_line = 1;
        Ok(())
    }

    /// Read the next record and return payload as bytes.
    #[allow(dead_code)] // Public API for library users
    pub fn read_next_bytes(&mut self) -> Option<Result<MessageRecordBytes, MqttRecorderError>> {
        // Get the next record from the CSV reader
        let result = self.reader.records().next()?;

        // Increment line counter for error reporting
        self.current_line += 1;
        let line_number = self.current_line;

        Some(self.parse_record_bytes(result, line_number))
    }

    /// Parses common fields from a CSV record, leaving payload conversion to the caller.
    fn parse_common_fields(
        &self,
        record: &csv::StringRecord,
        line_number: u64,
    ) -> Result<ParsedFields, MqttRecorderError> {
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
        let timestamp = crate::util::parse_timestamp(timestamp_str).map_err(|_| {
            MqttRecorderError::InvalidArgument(format!(
                "Line {}: Invalid timestamp '{}'",
                line_number, timestamp_str
            ))
        })?;

        // Parse topic
        let topic = record[1].to_string();

        // Get raw payload string (no decoding yet)
        let payload_str = record[2].to_string();

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

        Ok(ParsedFields {
            timestamp,
            topic,
            payload_str,
            qos,
            retain,
        })
    }

    /// Parses a CSV string record into a MessageRecord.
    fn parse_record(
        &self,
        result: Result<csv::StringRecord, csv::Error>,
    ) -> Result<MessageRecord, MqttRecorderError> {
        let record = result?;
        let fields = self.parse_common_fields(&record, self.current_line)?;

        // Parse payload, handling auto-encoded binary payloads
        let payload = if self.decode_b64 {
            // When decode_b64 is true: decode all payloads from base64 (Requirement 3.3)
            let decoded_bytes = BASE64_STANDARD.decode(&fields.payload_str).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!("Invalid base64 payload: {}", e))
            })?;
            // Convert to UTF-8, using lossy conversion for binary data
            String::from_utf8_lossy(&decoded_bytes).into_owned()
        } else if let Some(encoded_content) = fields.payload_str.strip_prefix(AUTO_ENCODE_MARKER) {
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
            fields.payload_str
        };

        Ok(MessageRecord {
            timestamp: fields.timestamp,
            topic: fields.topic,
            payload,
            qos: fields.qos,
            retain: fields.retain,
        })
    }

    /// Parses a CSV string record into a MessageRecordBytes.
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
        let fields = self.parse_common_fields(&record, line_number)?;

        // Parse payload as bytes, handling auto-encoded binary payloads
        let payload: Vec<u8> = if self.decode_b64 {
            // When decode_b64 is true: decode all payloads from base64 (Requirement 3.3)
            BASE64_STANDARD.decode(&fields.payload_str).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Line {}: Invalid base64 payload: {}",
                    line_number, e
                ))
            })?
        } else if let Some(encoded_content) = fields.payload_str.strip_prefix(AUTO_ENCODE_MARKER) {
            // When decode_b64 is false and payload starts with "b64:": strip prefix and decode (Requirement 3.1)
            BASE64_STANDARD.decode(encoded_content).map_err(|e| {
                MqttRecorderError::InvalidArgument(format!(
                    "Line {}: Invalid base64 in auto-encoded payload (after 'b64:' prefix): {}",
                    line_number, e
                ))
            })?
        } else {
            // When decode_b64 is false and no prefix: return payload as bytes (Requirement 3.2)
            fields.payload_str.as_bytes().to_vec()
        };

        Ok(MessageRecordBytes {
            timestamp: fields.timestamp,
            topic: fields.topic,
            payload,
            qos: fields.qos,
            retain: fields.retain,
        })
    }
}

impl Iterator for CsvReader {
    type Item = Result<MessageRecord, MqttRecorderError>;

    /// Returns the next message record from the CSV file.
    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csv_handler::CsvWriter;
    use chrono::TimeZone;
    use std::fs;

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

    // parse_common_fields tests
    #[test]
    fn test_parse_common_fields_valid() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,payload,1,true").unwrap();

        let reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "1",
            "true",
        ]);
        let fields = reader.parse_common_fields(&record, 2).unwrap();

        assert_eq!(fields.topic, "test/topic");
        assert_eq!(fields.payload_str, "payload");
        assert_eq!(fields.qos, 1);
        assert!(fields.retain);
    }

    #[test]
    fn test_parse_common_fields_wrong_field_count() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n").unwrap();

        let reader = CsvReader::new(&file_path, false, None).unwrap();

        // 3 fields
        let record =
            csv::StringRecord::from(vec!["2024-01-15T10:30:00.000Z", "test/topic", "payload"]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected 5 fields but got 3"));

        // 6 fields
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "1",
            "true",
            "extra",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected 5 fields but got 6"));
    }

    #[test]
    fn test_parse_common_fields_invalid_timestamp() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n").unwrap();

        let reader = CsvReader::new(&file_path, false, None).unwrap();
        let record = csv::StringRecord::from(vec![
            "invalid-timestamp",
            "test/topic",
            "payload",
            "1",
            "true",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid timestamp"));
    }

    #[test]
    fn test_parse_common_fields_invalid_qos() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n").unwrap();

        let reader = CsvReader::new(&file_path, false, None).unwrap();

        // QoS 3 (out of range)
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "3",
            "true",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("QoS must be 0, 1, or 2, got 3"));

        // QoS 'abc' (not a number)
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "abc",
            "true",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid QoS 'abc'"));
    }

    #[test]
    fn test_parse_common_fields_invalid_retain() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n").unwrap();

        let reader = CsvReader::new(&file_path, false, None).unwrap();

        // 'yes' instead of boolean
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "1",
            "yes",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid retain flag 'yes'"));

        // 'abc' instead of boolean
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "1",
            "abc",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid retain flag 'abc'"));
    }

    #[test]
    fn test_parse_common_fields_field_size_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n").unwrap();

        let reader = CsvReader::new(&file_path, false, Some(10)).unwrap();
        let record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.000Z",
            "test/topic",
            "payload",
            "1",
            "true",
        ]);
        let result = reader.parse_common_fields(&record, 2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeds size limit"));
    }

    // parse_record tests
    #[test]
    fn test_parse_record_base64_decode_true() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let base64_payload = base64::engine::general_purpose::STANDARD.encode("Hello World");
        fs::write(
            &file_path,
            format!(
                "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,{},1,true",
                base64_payload
            ),
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let result = reader.next().unwrap().unwrap();

        assert_eq!(result.payload, "Hello World");
    }

    #[test]
    fn test_parse_record_auto_decode_b64_prefix() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let base64_payload = base64::engine::general_purpose::STANDARD.encode("Binary Data");
        let prefixed_payload = format!("b64:{}", base64_payload);
        fs::write(
            &file_path,
            format!(
                "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,{},1,true",
                prefixed_payload
            ),
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.next().unwrap().unwrap();

        assert_eq!(result.payload, "Binary Data");
    }

    #[test]
    fn test_parse_record_plain_text_passthrough() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,plain text,1,true").unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.next().unwrap().unwrap();

        assert_eq!(result.payload, "plain text");
    }

    #[test]
    fn test_parse_record_invalid_base64() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,invalid-base64!,1,true").unwrap();

        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let result = reader.next().unwrap();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid base64 payload"));
    }

    // parse_record_bytes tests
    #[test]
    fn test_parse_record_bytes_text() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,text payload,1,true").unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(result.payload, b"text payload");
    }

    #[test]
    fn test_parse_record_bytes_b64_prefix() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let binary_data = vec![0x08, 0x0A, 0x12, 0x18];
        let base64_payload = base64::engine::general_purpose::STANDARD.encode(&binary_data);
        let prefixed_payload = format!("b64:{}", base64_payload);
        fs::write(
            &file_path,
            format!(
                "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,{},1,true",
                prefixed_payload
            ),
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(result.payload, binary_data);
    }

    #[test]
    fn test_parse_record_bytes_base64_decode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let binary_data = vec![0xFF, 0xFE, 0x00, 0x01];
        let base64_payload = base64::engine::general_purpose::STANDARD.encode(&binary_data);
        fs::write(
            &file_path,
            format!(
                "timestamp,topic,payload,qos,retain\n2024-01-15T10:30:00.000Z,test/topic,{},1,true",
                base64_payload
            ),
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, true, None).unwrap();
        let result = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(result.payload, binary_data);
    }

    // Iterator tests
    #[test]
    fn test_iterator_multiple_records() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let records = vec![
            MessageRecord::new(
                timestamp,
                "topic1".to_string(),
                "payload1".to_string(),
                0,
                false,
            ),
            MessageRecord::new(
                timestamp,
                "topic2".to_string(),
                "payload2".to_string(),
                1,
                true,
            ),
        ];

        // Write records
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            for record in &records {
                writer.write(record).unwrap();
            }
            writer.flush().unwrap();
        }

        // Read using iterator
        let reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_records: Result<Vec<_>, _> = reader.collect();
        let read_records = read_records.unwrap();

        assert_eq!(read_records.len(), 2);
        assert_eq!(read_records[0].topic, "topic1");
        assert_eq!(read_records[1].topic, "topic2");
    }

    #[test]
    fn test_iterator_empty_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(&file_path, "timestamp,topic,payload,qos,retain\n").unwrap();

        let reader = CsvReader::new(&file_path, false, None).unwrap();
        let records: Vec<_> = reader.collect();
        assert!(records.is_empty());
    }

    // Error cases
    #[test]
    fn test_nonexistent_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("nonexistent.csv");

        let result = CsvReader::new(&file_path, false, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_malformed_csv() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");
        fs::write(
            &file_path,
            "timestamp,topic,payload,qos,retain\nmalformed\"csv\"line",
        )
        .unwrap();

        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let result = reader.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    // read_next_bytes tests
    #[test]
    fn test_read_next_bytes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "test payload".to_string(),
            0,
            false,
        );

        // Write record
        {
            let mut writer = CsvWriter::new(&file_path, false).unwrap();
            writer.write(&record).unwrap();
            writer.flush().unwrap();
        }

        // Read as bytes
        let mut reader = CsvReader::new(&file_path, false, None).unwrap();
        let read_record = reader.read_next_bytes().unwrap().unwrap();

        assert_eq!(read_record.topic, "test/topic");
        assert_eq!(read_record.payload, b"test payload");
        assert_eq!(read_record.qos, 0);
        assert!(!read_record.retain);
    }
}
