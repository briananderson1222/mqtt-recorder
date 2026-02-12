//! CSV Repair Module
//!
//! This module provides functionality for repairing corrupted CSV files
//! that contain unencoded binary payloads. It can detect and re-encode
//! binary payloads to produce valid CSV files.
//!
//! # Overview
//!
//! When MQTT messages containing binary data (protobuf, raw bytes, etc.) are
//! recorded without base64 encoding enabled, the binary payloads with control
//! characters can corrupt the CSV structure. This module provides the [`CsvFixer`]
//! struct to repair such corrupted files.
//!
//! # Example
//!
//! ```no_run
//! use mqtt_recorder::fixer::CsvFixer;
//! use std::path::Path;
//!
//! let fixer = CsvFixer::new(false);
//! let stats = fixer.repair(
//!     Path::new("corrupted.csv"),
//!     Path::new("repaired.csv"),
//! ).unwrap();
//!
//! println!("Repaired {} records", stats.repaired_records);
//! println!("Skipped {} unrecoverable records", stats.skipped_records);
//! ```

use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use csv::ReaderBuilder;

use crate::csv_handler::{is_binary_payload, CsvWriter, MessageRecord, AUTO_ENCODE_MARKER};
use crate::error::MqttRecorderError;

/// Statistics collected during repair.
///
/// This struct tracks the results of a CSV repair operation, including
/// how many records were processed, repaired, or skipped.
///
/// # Fields
///
/// * `total_records` - Total number of records processed from the input file
/// * `valid_records` - Number of records that were already valid and preserved unchanged
/// * `repaired_records` - Number of records that were successfully repaired
/// * `skipped_records` - Number of records that could not be repaired and were skipped
/// * `skipped_lines` - Line numbers of records that were skipped
///
/// # Requirements
///
/// - **8.5**: WHEN fix mode is active, THE CSV_Handler SHALL report the number of records repaired
/// - **8.7**: IF a record cannot be repaired, THEN THE CSV_Handler SHALL skip it and report the line number
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RepairStats {
    /// Total number of records processed from the input file
    pub total_records: u64,
    /// Number of records that were already valid and preserved unchanged
    pub valid_records: u64,
    /// Number of records that were successfully repaired
    pub repaired_records: u64,
    /// Number of records that could not be repaired and were skipped
    pub skipped_records: u64,
    /// Line numbers of records that were skipped
    pub skipped_lines: Vec<u64>,
}

impl RepairStats {
    /// Returns true if the repair operation was successful.
    ///
    /// A repair is considered successful if no records were skipped.
    /// Even if no records needed repair, the operation is successful.
    pub fn is_success(&self) -> bool {
        self.skipped_records == 0
    }
}

impl fmt::Display for RepairStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "CSV Repair Report")?;
        writeln!(f, "=================")?;
        writeln!(
            f,
            "Status: {}",
            if self.is_success() {
                "SUCCESS"
            } else {
                "PARTIAL"
            }
        )?;
        writeln!(f)?;
        writeln!(f, "Statistics:")?;
        writeln!(f, "  Total records:     {:>10}", self.total_records)?;
        writeln!(f, "  Valid records:     {:>10}", self.valid_records)?;
        writeln!(f, "  Repaired records:  {:>10}", self.repaired_records)?;
        writeln!(f, "  Skipped records:   {:>10}", self.skipped_records)?;

        if !self.skipped_lines.is_empty() {
            writeln!(f)?;
            writeln!(f, "Skipped lines (unrecoverable):")?;
            for line_num in &self.skipped_lines {
                writeln!(f, "  Line {}", line_num)?;
            }
        }

        Ok(())
    }
}

/// CSV file repair utility.
///
/// The `CsvFixer` reads corrupted CSV files that contain unencoded binary payloads
/// and writes a repaired version with properly encoded payloads. It attempts to
/// recover as many records as possible while skipping unrecoverable ones.
///
/// # Repair Strategy
///
/// 1. Read the input file line by line
/// 2. For each line, attempt to parse it as a valid CSV record
/// 3. If valid, write it to the output unchanged
/// 4. If corrupted (likely due to binary payload), attempt to repair by:
///    - Detecting the binary content
///    - Re-encoding the payload with proper base64 encoding
/// 5. If unrecoverable, skip the record and log the line number
///
/// # Example
///
/// ```no_run
/// use mqtt_recorder::fixer::CsvFixer;
/// use std::path::Path;
///
/// let fixer = CsvFixer::new(false);
/// let stats = fixer.repair(
///     Path::new("corrupted.csv"),
///     Path::new("repaired.csv"),
/// ).unwrap();
///
/// println!("Total records: {}", stats.total_records);
/// println!("Valid records: {}", stats.valid_records);
/// println!("Repaired records: {}", stats.repaired_records);
/// println!("Skipped records: {}", stats.skipped_records);
/// if !stats.skipped_lines.is_empty() {
///     println!("Skipped lines: {:?}", stats.skipped_lines);
/// }
/// ```
///
/// # Requirements
///
/// - **8.2**: WHEN fix mode is active, THE CSV_Handler SHALL read the input file and write a repaired version to a new file
/// - **8.3**: WHEN fix mode is active, THE CSV_Handler SHALL detect corrupted records caused by unencoded binary payloads
/// - **8.4**: WHEN fix mode is active and a corrupted record is detected, THE CSV_Handler SHALL attempt to recover the record by re-encoding the payload
/// - **8.5**: WHEN fix mode is active, THE CSV_Handler SHALL report the number of records repaired
/// - **8.6**: WHEN fix mode is active, THE CSV_Handler SHALL preserve valid records unchanged
/// - **8.7**: IF a record cannot be repaired, THEN THE CSV_Handler SHALL skip it and report the line number
pub struct CsvFixer {
    /// Whether to encode all payloads as base64 in the output.
    /// When false, only binary payloads are auto-encoded with "b64:" prefix.
    /// When true, all payloads are base64 encoded without prefix.
    encode_b64: bool,
}

impl CsvFixer {
    /// Creates a new CSV fixer with the specified encoding option.
    ///
    /// # Arguments
    ///
    /// * `encode_b64` - If true, all payloads in the output will be base64 encoded.
    ///   If false, only binary payloads will be auto-encoded with "b64:" prefix.
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::fixer::CsvFixer;
    ///
    /// // Create fixer that auto-encodes only binary payloads
    /// let fixer = CsvFixer::new(false);
    ///
    /// // Create fixer that base64 encodes all payloads
    /// let fixer_b64 = CsvFixer::new(true);
    /// ```
    pub fn new(encode_b64: bool) -> Self {
        Self { encode_b64 }
    }

    /// Repair a corrupted CSV file.
    ///
    /// Reads the input file, detects corrupted records caused by unencoded binary
    /// payloads, and writes a repaired version to the output path.
    ///
    /// # Arguments
    ///
    /// * `input` - Path to the corrupted CSV file to repair
    /// * `output` - Path where the repaired CSV file will be written
    ///
    /// # Returns
    ///
    /// Returns `Ok(RepairStats)` with statistics about the repair operation,
    /// or an error if the files cannot be opened or written.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Io`] if:
    /// - The input file cannot be opened
    /// - The output file cannot be created
    ///
    /// Returns [`MqttRecorderError::Csv`] if:
    /// - The output CSV writer cannot be initialized
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mqtt_recorder::fixer::CsvFixer;
    /// use std::path::Path;
    ///
    /// let fixer = CsvFixer::new(false);
    /// match fixer.repair(Path::new("input.csv"), Path::new("output.csv")) {
    ///     Ok(stats) => {
    ///         println!("Repair complete!");
    ///         println!("  Valid: {}", stats.valid_records);
    ///         println!("  Repaired: {}", stats.repaired_records);
    ///         println!("  Skipped: {}", stats.skipped_records);
    ///     }
    ///     Err(e) => eprintln!("Repair failed: {}", e),
    /// }
    /// ```
    ///
    /// # Requirements
    ///
    /// - **8.2**: Read the input file and write a repaired version to a new file
    /// - **8.3**: Detect corrupted records caused by unencoded binary payloads
    /// - **8.4**: Attempt to recover corrupted records by re-encoding the payload
    /// - **8.5**: Report the number of records repaired
    /// - **8.6**: Preserve valid records unchanged
    /// - **8.7**: Skip unrecoverable records and report their line numbers
    pub fn repair(&self, input: &Path, output: &Path) -> Result<RepairStats, MqttRecorderError> {
        let mut stats = RepairStats::default();

        // Open input file for reading
        let input_file = File::open(input)?;
        let reader = BufReader::new(input_file);

        // Create output CSV writer
        let mut writer = CsvWriter::new(output, self.encode_b64)?;

        // Read lines from input file
        let mut lines = reader.lines();

        // Skip header line (line 1)
        if let Some(header_result) = lines.next() {
            let header = header_result?;
            // Verify it looks like a header
            if !header.starts_with("timestamp") {
                // Not a standard header, might be a data line - try to process it
                // This handles files without headers
                if let Some(record) = self.try_repair_record(&header, 1) {
                    stats.total_records += 1;
                    stats.repaired_records += 1;
                    writer.write(&record)?;
                }
            }
        }

        // Process remaining lines (data records)
        let mut line_number: u64 = 1; // Start at 1 because header is line 1

        for line_result in lines {
            line_number += 1;
            stats.total_records += 1;

            let line = match line_result {
                Ok(l) => l,
                Err(_) => {
                    // IO error reading line - skip it
                    stats.skipped_records += 1;
                    stats.skipped_lines.push(line_number);
                    continue;
                }
            };

            // Skip empty lines
            if line.trim().is_empty() {
                stats.total_records -= 1; // Don't count empty lines
                continue;
            }

            // First, try to parse as a valid CSV record
            if let Some(record) = self.try_parse_valid_record(&line) {
                // Record is valid - write it unchanged (Requirement 8.6)
                stats.valid_records += 1;
                writer.write(&record)?;
            } else {
                // Record is corrupted - try to repair it (Requirement 8.4)
                if let Some(repaired_record) = self.try_repair_record(&line, line_number) {
                    stats.repaired_records += 1;
                    writer.write(&repaired_record)?;
                } else {
                    // Unrecoverable - skip and report (Requirement 8.7)
                    stats.skipped_records += 1;
                    stats.skipped_lines.push(line_number);
                }
            }
        }

        // Flush the writer to ensure all data is written
        writer.flush()?;

        Ok(stats)
    }

    /// Attempt to parse a line as a valid CSV record.
    ///
    /// This method tries to parse the line using the standard CSV parser.
    /// If successful and all fields are valid, returns the parsed record.
    /// Otherwise returns None.
    ///
    /// A record is considered valid only if:
    /// 1. It can be parsed as valid CSV with exactly 5 fields
    /// 2. All fields have valid values (timestamp, QoS, retain)
    /// 3. The payload does NOT contain unencoded binary content
    ///
    /// If the payload contains binary content (control characters or invalid UTF-8),
    /// the record is considered corrupted and needs repair (Requirement 8.3).
    fn try_parse_valid_record(&self, raw_line: &str) -> Option<MessageRecord> {
        // Use CSV reader to parse the single line
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(raw_line.as_bytes());

        let record = reader.records().next()?.ok()?;

        // Must have exactly 5 fields
        if record.len() != 5 {
            return None;
        }

        // Parse timestamp
        let timestamp_str = &record[0];
        let timestamp = crate::util::parse_timestamp(timestamp_str).ok()?;

        // Parse topic
        let topic = record[1].to_string();

        // Get payload
        let payload = record[2].to_string();

        // Check if payload contains unencoded binary content (Requirement 8.3)
        // If the payload contains binary control characters, it's corrupted and needs repair
        // Exception: if it starts with "b64:" it's already properly encoded
        if !payload.starts_with(AUTO_ENCODE_MARKER) && is_binary_payload(payload.as_bytes()) {
            // Payload contains unencoded binary - this record needs repair
            return None;
        }

        // Parse QoS
        let qos: u8 = record[3].parse().ok()?;
        if qos > 2 {
            return None;
        }

        // Parse retain
        let retain: bool = record[4].parse().ok()?;

        Some(MessageRecord::new(timestamp, topic, payload, qos, retain))
    }

    /// Attempt to repair a single corrupted record.
    ///
    /// This method attempts to recover a corrupted CSV record by:
    /// 1. Detecting the likely field boundaries
    /// 2. Extracting the payload (which may contain binary data)
    /// 3. Re-encoding the payload with proper base64 encoding
    ///
    /// # Arguments
    ///
    /// * `raw_line` - The raw line from the CSV file
    /// * `line_number` - The line number (for logging purposes)
    ///
    /// # Returns
    ///
    /// Returns `Some(MessageRecord)` if repair was successful,
    /// `None` if the record is unrecoverable.
    ///
    /// # Repair Strategy
    ///
    /// Corrupted records typically have binary data in the payload field that
    /// breaks CSV parsing. The repair strategy is:
    ///
    /// 1. Try to identify the timestamp at the start (ISO 8601 format)
    /// 2. Try to identify the topic after the timestamp
    /// 3. Extract everything between topic and the last two fields as payload
    /// 4. Try to identify QoS and retain at the end
    /// 5. Re-encode the payload if it contains binary data
    ///
    /// # Requirements
    ///
    /// - **8.3**: Detect corrupted records caused by unencoded binary payloads
    /// - **8.4**: Attempt to recover the record by re-encoding the payload
    fn try_repair_record(&self, raw_line: &str, _line_number: u64) -> Option<MessageRecord> {
        // Strategy: Try to extract fields from a corrupted line
        // The corruption is typically in the payload field due to binary data
        //
        // Expected format: timestamp,topic,payload,qos,retain
        // Where payload may contain unescaped commas, quotes, newlines, etc.

        // Step 1: Try to find the timestamp at the beginning
        // Timestamps are in ISO 8601 format: YYYY-MM-DDTHH:MM:SS.sssZ
        let timestamp_end = self.find_timestamp_end(raw_line)?;
        let timestamp_str = &raw_line[..timestamp_end];
        let timestamp = crate::util::parse_timestamp(timestamp_str).ok()?;

        // Step 2: Skip the comma after timestamp and find the topic
        let after_timestamp = &raw_line[timestamp_end..];
        if !after_timestamp.starts_with(',') {
            return None;
        }
        let after_timestamp = &after_timestamp[1..]; // Skip comma

        // Step 3: Find the topic (ends at next comma, unless quoted)
        let (topic, after_topic) = self.extract_field(after_timestamp)?;

        // Step 4: Find QoS and retain from the end
        // Work backwards to find the last two fields
        let (qos, retain, payload_end) = self.find_trailing_fields(after_topic)?;

        // Step 5: Extract the payload (everything between topic and trailing fields)
        let payload_bytes = &after_topic.as_bytes()[..payload_end];

        // Step 6: Check if payload needs encoding
        let payload_str = if is_binary_payload(payload_bytes) {
            // Binary payload - encode it
            if self.encode_b64 {
                BASE64_STANDARD.encode(payload_bytes)
            } else {
                format!(
                    "{}{}",
                    AUTO_ENCODE_MARKER,
                    BASE64_STANDARD.encode(payload_bytes)
                )
            }
        } else {
            // Text payload - check for marker collision
            let text = String::from_utf8(payload_bytes.to_vec()).ok()?;
            if !self.encode_b64 && text.starts_with(AUTO_ENCODE_MARKER) {
                // Marker collision - encode to preserve
                format!(
                    "{}{}",
                    AUTO_ENCODE_MARKER,
                    BASE64_STANDARD.encode(payload_bytes)
                )
            } else if self.encode_b64 {
                BASE64_STANDARD.encode(payload_bytes)
            } else {
                text
            }
        };

        Some(MessageRecord::new(
            timestamp,
            topic,
            payload_str,
            qos,
            retain,
        ))
    }

    /// Find the end position of the timestamp in the line.
    ///
    /// Timestamps are in ISO 8601 format: YYYY-MM-DDTHH:MM:SS.sssZ
    /// This method looks for the 'Z' that ends the timestamp.
    fn find_timestamp_end(&self, line: &str) -> Option<usize> {
        // Look for the pattern: digits-digits-digitsTdigits:digits:digits.digitsZ
        // The timestamp should be at least 20 characters (without milliseconds)
        // and at most 30 characters (with milliseconds)

        // Find the first 'Z' which should end the timestamp
        let z_pos = line.find('Z')?;

        // Verify it looks like a timestamp
        if !(19..=30).contains(&z_pos) {
            return None;
        }

        // Check for 'T' separator
        if !line[..z_pos].contains('T') {
            return None;
        }

        Some(z_pos + 1) // Include the 'Z'
    }

    /// Extract a field from the beginning of a string.
    ///
    /// Handles both quoted and unquoted fields according to CSV rules.
    /// Returns the field value and the remaining string after the field separator.
    fn extract_field<'a>(&self, s: &'a str) -> Option<(String, &'a str)> {
        if s.is_empty() {
            return None;
        }

        if let Some(inner) = s.strip_prefix('"') {
            // Quoted field - find the closing quote
            // Handle escaped quotes (doubled quotes)
            let mut chars = inner.char_indices();
            let mut field = String::new();

            while let Some((i, c)) = chars.next() {
                if c == '"' {
                    // Check if this is an escaped quote or end of field
                    if let Some((_, next_c)) = chars.next() {
                        if next_c == '"' {
                            // Escaped quote
                            field.push('"');
                        } else if next_c == ',' {
                            // End of field
                            return Some((field, &s[i + 3..])); // +1 for initial quote, +1 for closing quote, +1 for comma
                        } else {
                            // Malformed - closing quote not followed by comma
                            return None;
                        }
                    } else {
                        // End of string after closing quote
                        return Some((field, ""));
                    }
                } else {
                    field.push(c);
                }
            }
            // Unclosed quote
            None
        } else {
            // Unquoted field - find the next comma
            if let Some(comma_pos) = s.find(',') {
                let field = s[..comma_pos].to_string();
                Some((field, &s[comma_pos + 1..]))
            } else {
                // No comma - rest of string is the field
                Some((s.to_string(), ""))
            }
        }
    }

    /// Find QoS and retain fields from the end of the payload section.
    ///
    /// Works backwards from the end of the string to find:
    /// - retain: "true" or "false"
    /// - qos: "0", "1", or "2"
    ///
    /// Returns (qos, retain, payload_end_position) where payload_end_position
    /// is the index where the payload ends (before the comma preceding qos).
    fn find_trailing_fields(&self, s: &str) -> Option<(u8, bool, usize)> {
        // Work backwards to find retain field
        // The string should end with: ,qos,retain

        // Find the last comma (before retain)
        let last_comma = s.rfind(',')?;
        let retain_str = s[last_comma + 1..].trim();
        let retain: bool = retain_str.parse().ok()?;

        // Find the second-to-last comma (before qos)
        let before_retain = &s[..last_comma];
        let second_last_comma = before_retain.rfind(',')?;
        let qos_str = before_retain[second_last_comma + 1..].trim();
        let qos: u8 = qos_str.parse().ok()?;

        if qos > 2 {
            return None;
        }

        // The payload ends at the second-to-last comma
        Some((qos, retain, second_last_comma))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_repair_stats_default() {
        let stats = RepairStats::default();
        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.valid_records, 0);
        assert_eq!(stats.repaired_records, 0);
        assert_eq!(stats.skipped_records, 0);
        assert!(stats.skipped_lines.is_empty());
    }

    #[test]
    fn test_csv_fixer_new() {
        let fixer = CsvFixer::new(false);
        assert!(!fixer.encode_b64);

        let fixer_b64 = CsvFixer::new(true);
        assert!(fixer_b64.encode_b64);
    }

    #[test]
    fn test_repair_valid_csv() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a valid CSV file
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(
            input_file,
            "2024-01-15T10:30:00.123Z,test/topic,hello world,0,false"
        )
        .unwrap();
        writeln!(
            input_file,
            "2024-01-15T10:30:01.456Z,test/topic2,\"payload with, comma\",1,true"
        )
        .unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        assert_eq!(stats.total_records, 2);
        assert_eq!(stats.valid_records, 2);
        assert_eq!(stats.repaired_records, 0);
        assert_eq!(stats.skipped_records, 0);
    }

    #[test]
    fn test_find_timestamp_end() {
        let fixer = CsvFixer::new(false);

        // Valid timestamp
        let line = "2024-01-15T10:30:00.123Z,topic,payload,0,false";
        assert_eq!(fixer.find_timestamp_end(line), Some(24));

        // Timestamp without milliseconds
        let line = "2024-01-15T10:30:00Z,topic,payload,0,false";
        assert_eq!(fixer.find_timestamp_end(line), Some(20));

        // No Z in line
        let line = "not a timestamp,topic,payload,0,false";
        assert_eq!(fixer.find_timestamp_end(line), None);
    }

    #[test]
    fn test_extract_field_unquoted() {
        let fixer = CsvFixer::new(false);

        let (field, rest) = fixer.extract_field("topic,payload,0,false").unwrap();
        assert_eq!(field, "topic");
        assert_eq!(rest, "payload,0,false");
    }

    #[test]
    fn test_extract_field_quoted() {
        let fixer = CsvFixer::new(false);

        let (field, rest) = fixer
            .extract_field("\"topic with, comma\",payload,0,false")
            .unwrap();
        assert_eq!(field, "topic with, comma");
        assert_eq!(rest, "payload,0,false");
    }

    #[test]
    fn test_find_trailing_fields() {
        let fixer = CsvFixer::new(false);

        let (qos, retain, end) = fixer.find_trailing_fields("payload,0,false").unwrap();
        assert_eq!(qos, 0);
        assert!(!retain);
        assert_eq!(end, 7); // Position of comma before "0"

        let (qos, retain, end) = fixer.find_trailing_fields("payload,2,true").unwrap();
        assert_eq!(qos, 2);
        assert!(retain);
        assert_eq!(end, 7);
    }

    #[test]
    fn test_try_parse_valid_record() {
        let fixer = CsvFixer::new(false);

        let record = fixer
            .try_parse_valid_record("2024-01-15T10:30:00.123Z,test/topic,hello,0,false")
            .unwrap();
        assert_eq!(record.topic, "test/topic");
        assert_eq!(record.payload, "hello");
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_try_parse_valid_record_with_quoted_payload() {
        let fixer = CsvFixer::new(false);

        let record = fixer
            .try_parse_valid_record(
                "2024-01-15T10:30:00.123Z,test/topic,\"payload with, comma\",1,true",
            )
            .unwrap();
        assert_eq!(record.topic, "test/topic");
        assert_eq!(record.payload, "payload with, comma");
        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_try_repair_record_simple() {
        let fixer = CsvFixer::new(false);

        // A simple corrupted line (binary in payload)
        let line = "2024-01-15T10:30:00.123Z,test/topic,hello\x00world,0,false";
        let record = fixer.try_repair_record(line, 2).unwrap();

        assert_eq!(record.topic, "test/topic");
        // Payload should be auto-encoded because it contains binary
        assert!(record.payload.starts_with(AUTO_ENCODE_MARKER));
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_repair_empty_file() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create an empty CSV file with just header
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.valid_records, 0);
        assert_eq!(stats.repaired_records, 0);
        assert_eq!(stats.skipped_records, 0);
    }

    // ==========================================================================
    // Task 7.3: Implement repair logic - Additional tests for requirements
    // ==========================================================================

    /// Test Requirement 8.3: Detect corrupted records caused by unencoded binary payloads
    #[test]
    fn test_repair_detects_corrupted_binary_records() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a CSV file with corrupted binary payload (NUL byte)
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        // Write a line with binary content that would corrupt CSV parsing
        write!(input_file, "2024-01-15T10:30:00.123Z,test/topic,hello").unwrap();
        input_file.write_all(&[0x00]).unwrap(); // NUL byte
        writeln!(input_file, "world,0,false").unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        // The corrupted record should be detected and repaired
        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.repaired_records, 1);
        assert_eq!(stats.valid_records, 0);
    }

    /// Test Requirement 8.4: Re-encode binary payloads with proper encoding
    #[test]
    fn test_repair_reencodes_binary_payloads() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a CSV file with binary payload containing control characters
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        // Binary payload with backspace (0x08) control character
        write!(input_file, "2024-01-15T10:30:00.123Z,binary/topic,data").unwrap();
        input_file.write_all(&[0x08]).unwrap(); // Backspace control char
        writeln!(input_file, "more,1,true").unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        assert_eq!(stats.repaired_records, 1);

        // Verify the output file has properly encoded payload
        let output_content = std::fs::read_to_string(&output_path).unwrap();
        assert!(
            output_content.contains(AUTO_ENCODE_MARKER),
            "Repaired payload should be auto-encoded with b64: prefix"
        );
    }

    /// Test Requirement 8.6: Preserve valid records unchanged
    #[test]
    fn test_repair_preserves_valid_records_unchanged() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a CSV file with valid records (simple text payloads to avoid escaping issues)
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(
            input_file,
            "2024-01-15T10:30:00.123Z,sensors/temp,temperature reading 23.5,0,false"
        )
        .unwrap();
        writeln!(
            input_file,
            "2024-01-15T10:30:01.456Z,sensors/humidity,humidity reading 65,1,true"
        )
        .unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        // All records should be valid and preserved
        assert_eq!(stats.total_records, 2);
        assert_eq!(stats.valid_records, 2);
        assert_eq!(stats.repaired_records, 0);
        assert_eq!(stats.skipped_records, 0);

        // Verify output content matches input (excluding header)
        let output_content = std::fs::read_to_string(&output_path).unwrap();
        assert!(output_content.contains("sensors/temp"));
        assert!(output_content.contains("sensors/humidity"));
        assert!(
            output_content.contains("temperature reading 23.5"),
            "Payload should be preserved unchanged"
        );
        assert!(
            output_content.contains("humidity reading 65"),
            "Payload should be preserved unchanged"
        );
    }

    /// Test Requirement 8.7: Skip unrecoverable records and report line numbers
    #[test]
    fn test_repair_skips_unrecoverable_records() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a CSV file with an unrecoverable record (no valid timestamp)
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(
            input_file,
            "2024-01-15T10:30:00.123Z,valid/topic,valid payload,0,false"
        )
        .unwrap();
        // Unrecoverable: no valid timestamp format
        writeln!(input_file, "not-a-timestamp,broken/topic,payload,0,false").unwrap();
        writeln!(
            input_file,
            "2024-01-15T10:30:02.789Z,another/valid,another payload,2,true"
        )
        .unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        // One record should be skipped (line 3 - the unrecoverable one)
        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.valid_records, 2);
        assert_eq!(stats.skipped_records, 1);
        assert!(
            stats.skipped_lines.contains(&3),
            "Line 3 should be reported as skipped"
        );
    }

    /// Test Requirement 8.7: Report multiple skipped line numbers
    #[test]
    fn test_repair_reports_multiple_skipped_lines() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a CSV file with multiple unrecoverable records
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(input_file, "invalid-line-2").unwrap(); // Line 2 - unrecoverable
        writeln!(
            input_file,
            "2024-01-15T10:30:00.123Z,valid/topic,payload,0,false"
        )
        .unwrap(); // Line 3 - valid
        writeln!(input_file, "another-invalid-line-4").unwrap(); // Line 4 - unrecoverable
        writeln!(
            input_file,
            "2024-01-15T10:30:01.456Z,valid/topic2,payload2,1,true"
        )
        .unwrap(); // Line 5 - valid
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        assert_eq!(stats.total_records, 4);
        assert_eq!(stats.valid_records, 2);
        assert_eq!(stats.skipped_records, 2);
        assert!(
            stats.skipped_lines.contains(&2),
            "Line 2 should be reported as skipped"
        );
        assert!(
            stats.skipped_lines.contains(&4),
            "Line 4 should be reported as skipped"
        );
    }

    /// Test mixed scenario: valid, corrupted (repairable), and unrecoverable records
    #[test]
    fn test_repair_mixed_records() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        // Line 2: Valid record
        writeln!(
            input_file,
            "2024-01-15T10:30:00.123Z,valid/topic,text payload,0,false"
        )
        .unwrap();
        // Line 3: Corrupted but repairable (binary in payload)
        write!(input_file, "2024-01-15T10:30:01.456Z,binary/topic,binary").unwrap();
        input_file.write_all(&[0x00, 0x01, 0x02]).unwrap();
        writeln!(input_file, "data,1,true").unwrap();
        // Line 4: Unrecoverable (invalid format)
        writeln!(input_file, "garbage-data-no-timestamp").unwrap();
        // Line 5: Another valid record
        writeln!(
            input_file,
            "2024-01-15T10:30:02.789Z,another/valid,more text,2,false"
        )
        .unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();

        assert_eq!(stats.total_records, 4);
        assert_eq!(stats.valid_records, 2); // Lines 2 and 5
        assert_eq!(stats.repaired_records, 1); // Line 3
        assert_eq!(stats.skipped_records, 1); // Line 4
        assert!(stats.skipped_lines.contains(&4));
    }

    /// Test that repaired output file passes validation (can be read back)
    #[test]
    fn test_repaired_file_is_valid_csv() {
        use crate::csv_handler::CsvReader;

        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a file with binary payload that needs repair
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        write!(input_file, "2024-01-15T10:30:00.123Z,test/topic,binary").unwrap();
        input_file.write_all(&[0x00, 0x08, 0x0B]).unwrap(); // Binary control chars
        writeln!(input_file, "payload,0,false").unwrap();
        drop(input_file);

        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path).unwrap();
        assert_eq!(stats.repaired_records, 1);

        // Verify the output file can be read by CsvReader
        let mut reader =
            CsvReader::new(&output_path, false, None).expect("Should be able to read repaired CSV");
        let record = reader.next().expect("Should have one record");
        assert!(record.is_ok(), "Record should be valid");

        let record = record.unwrap();
        assert_eq!(record.topic, "test/topic");
        assert!(
            record.payload.starts_with(AUTO_ENCODE_MARKER),
            "Payload should be auto-encoded"
        );
    }

    /// Test repair with encode_b64=true (global base64 mode)
    #[test]
    fn test_repair_with_global_b64_encoding() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.csv");
        let output_path = dir.path().join("output.csv");

        // Create a file with binary payload
        let mut input_file = File::create(&input_path).unwrap();
        writeln!(input_file, "timestamp,topic,payload,qos,retain").unwrap();
        write!(input_file, "2024-01-15T10:30:00.123Z,test/topic,binary").unwrap();
        input_file.write_all(&[0x00]).unwrap();
        writeln!(input_file, "data,0,false").unwrap();
        drop(input_file);

        // Use encode_b64=true
        let fixer = CsvFixer::new(true);
        let stats = fixer.repair(&input_path, &output_path).unwrap();
        assert_eq!(stats.repaired_records, 1);

        // Verify the output doesn't have the b64: prefix (global mode)
        let output_content = std::fs::read_to_string(&output_path).unwrap();
        // In global b64 mode, payload should be base64 encoded without prefix
        assert!(
            !output_content.contains(AUTO_ENCODE_MARKER),
            "Global b64 mode should not use b64: prefix"
        );
    }

    // ==========================================================================
    // Task 8.5: Tests for RepairStats is_success and Display implementation
    // ==========================================================================

    #[test]
    fn test_repair_stats_is_success_true_when_no_skipped() {
        let stats = RepairStats {
            total_records: 10,
            valid_records: 8,
            repaired_records: 2,
            skipped_records: 0,
            skipped_lines: vec![],
        };
        assert!(stats.is_success());
    }

    #[test]
    fn test_repair_stats_is_success_false_when_skipped() {
        let stats = RepairStats {
            total_records: 10,
            valid_records: 7,
            repaired_records: 2,
            skipped_records: 1,
            skipped_lines: vec![5],
        };
        assert!(!stats.is_success());
    }

    #[test]
    fn test_repair_stats_is_success_true_for_empty() {
        let stats = RepairStats::default();
        assert!(stats.is_success());
    }

    #[test]
    fn test_repair_stats_display_success() {
        let stats = RepairStats {
            total_records: 100,
            valid_records: 90,
            repaired_records: 10,
            skipped_records: 0,
            skipped_lines: vec![],
        };
        let display = format!("{}", stats);
        assert!(display.contains("CSV Repair Report"));
        assert!(display.contains("Status: SUCCESS"));
        assert!(display.contains("Total records:"));
        assert!(display.contains("100"));
        assert!(display.contains("Valid records:"));
        assert!(display.contains("90"));
        assert!(display.contains("Repaired records:"));
        assert!(display.contains("10"));
        assert!(display.contains("Skipped records:"));
        assert!(display.contains("0"));
        assert!(!display.contains("Skipped lines"));
    }

    #[test]
    fn test_repair_stats_display_partial() {
        let stats = RepairStats {
            total_records: 100,
            valid_records: 85,
            repaired_records: 10,
            skipped_records: 5,
            skipped_lines: vec![10, 25, 50, 75, 99],
        };
        let display = format!("{}", stats);
        assert!(display.contains("CSV Repair Report"));
        assert!(display.contains("Status: PARTIAL"));
        assert!(display.contains("Skipped lines (unrecoverable):"));
        assert!(display.contains("Line 10"));
        assert!(display.contains("Line 25"));
        assert!(display.contains("Line 50"));
        assert!(display.contains("Line 75"));
        assert!(display.contains("Line 99"));
    }
}
