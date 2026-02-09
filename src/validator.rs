//! CSV Validation Module
//!
//! This module provides functionality for validating CSV files containing
//! recorded MQTT messages. It verifies that each record has the correct
//! format, valid field values, and proper encoding.
//!
//! # Overview
//!
//! The validator checks:
//! - Field count (exactly 5 fields: timestamp, topic, payload, qos, retain)
//! - Timestamp format (ISO 8601)
//! - QoS values (0, 1, or 2)
//! - Retain values ("true" or "false")
//! - Base64 encoding validity when applicable
//!
//! # Example
//!
//! ```rust,ignore
//! use mqtt_recorder::validator::{CsvValidator, ValidationStats};
//! use std::path::Path;
//!
//! let validator = CsvValidator::new(false, None);
//! let stats = validator.validate(Path::new("recording.csv"))?;
//!
//! println!("Total records: {}", stats.total_records);
//! println!("Valid records: {}", stats.valid_records);
//! println!("Invalid records: {}", stats.invalid_records);
//! ```

use std::fmt;

/// Result of validating a single CSV record.
///
/// This enum represents all possible validation outcomes for a single record.
/// A record can be valid, or it can have one of several types of validation errors.
///
/// # Variants
///
/// - `Valid`: The record passed all validation checks
/// - `InvalidFieldCount`: The record has the wrong number of fields
/// - `InvalidTimestamp`: The timestamp field is not valid ISO 8601 format
/// - `InvalidQos`: The QoS field is not 0, 1, or 2
/// - `InvalidRetain`: The retain field is not "true" or "false"
/// - `InvalidBase64`: The payload contains invalid base64 encoding
/// - `ParseError`: A general parsing error occurred
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationResult {
    /// The record is valid.
    Valid,

    /// The record has an incorrect number of fields.
    ///
    /// CSV records for MQTT messages must have exactly 5 fields:
    /// timestamp, topic, payload, qos, retain.
    InvalidFieldCount {
        /// The expected number of fields (always 5).
        expected: usize,
        /// The actual number of fields found.
        actual: usize,
    },

    /// The timestamp field is not valid ISO 8601 format.
    ///
    /// Timestamps must be in ISO 8601 format with timezone information,
    /// e.g., "2024-01-15T10:30:00.123Z".
    InvalidTimestamp {
        /// The invalid timestamp value.
        value: String,
        /// A description of the parsing error.
        error: String,
    },

    /// The QoS field is not a valid MQTT QoS level.
    ///
    /// Valid QoS values are 0, 1, or 2.
    InvalidQos {
        /// The invalid QoS value.
        value: String,
    },

    /// The retain field is not a valid boolean string.
    ///
    /// Valid retain values are "true" or "false".
    InvalidRetain {
        /// The invalid retain value.
        value: String,
    },

    /// The payload contains invalid base64 encoding.
    ///
    /// This error occurs when:
    /// - `decode_b64` is true and the payload is not valid base64
    /// - `decode_b64` is false and a payload with "b64:" prefix has invalid base64
    InvalidBase64 {
        /// A description of the base64 decoding error.
        error: String,
    },

    /// A general parsing error occurred.
    ///
    /// This covers errors that don't fit into the other categories,
    /// such as CSV parsing failures.
    ParseError {
        /// A description of the parsing error.
        error: String,
    },
}

impl fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationResult::Valid => write!(f, "Valid"),
            ValidationResult::InvalidFieldCount { expected, actual } => {
                write!(
                    f,
                    "Invalid field count: expected {} fields, found {}",
                    expected, actual
                )
            }
            ValidationResult::InvalidTimestamp { value, error } => {
                write!(f, "Invalid timestamp \"{}\": {}", value, error)
            }
            ValidationResult::InvalidQos { value } => {
                write!(f, "Invalid QoS value \"{}\": expected 0, 1, or 2", value)
            }
            ValidationResult::InvalidRetain { value } => {
                write!(
                    f,
                    "Invalid retain value \"{}\": expected \"true\" or \"false\"",
                    value
                )
            }
            ValidationResult::InvalidBase64 { error } => {
                write!(f, "Invalid base64 encoding: {}", error)
            }
            ValidationResult::ParseError { error } => {
                write!(f, "Parse error: {}", error)
            }
        }
    }
}

/// A validation error with line number and details.
///
/// This struct associates a validation result with the line number
/// where the error occurred, making it easier to locate and fix issues
/// in the CSV file.
///
/// # Example
///
/// ```rust,ignore
/// use mqtt_recorder::validator::{ValidationError, ValidationResult};
///
/// let error = ValidationError {
///     line_number: 42,
///     result: ValidationResult::InvalidQos { value: "3".to_string() },
/// };
///
/// println!("Line {}: {}", error.line_number, error.result);
/// // Output: Line 42: Invalid QoS value "3": expected 0, 1, or 2
/// ```
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// The line number in the CSV file where the error occurred.
    ///
    /// Line numbers are 1-indexed, with line 1 being the header row.
    /// Data records start at line 2.
    pub line_number: u64,

    /// The validation result describing the error.
    pub result: ValidationResult,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Line {}: {}", self.line_number, self.result)
    }
}

/// Statistics collected during validation.
///
/// This struct accumulates statistics about the CSV file being validated,
/// including counts of valid and invalid records, payload types, and
/// a list of all validation errors encountered.
///
/// # Example
///
/// ```rust,ignore
/// use mqtt_recorder::validator::ValidationStats;
///
/// let stats = ValidationStats::default();
/// println!("Total: {}, Valid: {}, Invalid: {}",
///     stats.total_records, stats.valid_records, stats.invalid_records);
/// ```
#[derive(Debug, Default, Clone)]
pub struct ValidationStats {
    /// Total number of records processed (excluding header).
    pub total_records: u64,

    /// Number of records that passed all validation checks.
    pub valid_records: u64,

    /// Number of records that failed one or more validation checks.
    pub invalid_records: u64,

    /// Number of payloads classified as text (no encoding prefix).
    pub text_payloads: u64,

    /// Number of payloads that were auto-encoded as binary (with "b64:" prefix).
    pub binary_payloads: u64,

    /// Number of payloads that are base64 encoded (when decode_b64 is true).
    pub base64_payloads: u64,

    /// Size of the largest payload encountered (in bytes).
    pub largest_payload: usize,

    /// List of all validation errors encountered.
    ///
    /// This list contains one entry for each invalid record,
    /// with the line number and error details.
    pub errors: Vec<ValidationError>,
}

impl ValidationStats {
    /// Creates a new `ValidationStats` with all counters set to zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if validation passed (no errors found).
    pub fn is_valid(&self) -> bool {
        self.invalid_records == 0
    }

    /// Records a valid record and updates statistics.
    ///
    /// # Arguments
    ///
    /// * `payload_size` - The size of the payload in bytes
    /// * `is_binary` - Whether the payload was auto-encoded (has "b64:" prefix)
    /// * `is_base64` - Whether the payload is base64 encoded (decode_b64 mode)
    pub fn record_valid(&mut self, payload_size: usize, is_binary: bool, is_base64: bool) {
        self.total_records += 1;
        self.valid_records += 1;

        if is_base64 {
            self.base64_payloads += 1;
        } else if is_binary {
            self.binary_payloads += 1;
        } else {
            self.text_payloads += 1;
        }

        if payload_size > self.largest_payload {
            self.largest_payload = payload_size;
        }
    }

    /// Records an invalid record with its error.
    ///
    /// # Arguments
    ///
    /// * `line_number` - The line number where the error occurred
    /// * `result` - The validation result describing the error
    pub fn record_invalid(&mut self, line_number: u64, result: ValidationResult) {
        self.total_records += 1;
        self.invalid_records += 1;
        self.errors.push(ValidationError {
            line_number,
            result,
        });
    }
}

impl fmt::Display for ValidationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "CSV Validation Report")?;
        writeln!(f, "=====================")?;
        writeln!(
            f,
            "Status: {}",
            if self.is_valid() { "PASSED" } else { "FAILED" }
        )?;
        writeln!(f)?;
        writeln!(f, "Statistics:")?;
        writeln!(f, "  Total records:     {:>10}", self.total_records)?;
        writeln!(f, "  Valid records:     {:>10}", self.valid_records)?;
        writeln!(f, "  Invalid records:   {:>10}", self.invalid_records)?;
        writeln!(f, "  Text payloads:     {:>10}", self.text_payloads)?;
        writeln!(f, "  Binary payloads:   {:>10}", self.binary_payloads)?;
        writeln!(f, "  Base64 payloads:   {:>10}", self.base64_payloads)?;
        writeln!(f, "  Largest payload:   {:>10} bytes", self.largest_payload)?;

        if !self.errors.is_empty() {
            writeln!(f)?;
            writeln!(f, "Errors:")?;
            for error in &self.errors {
                writeln!(f, "  {}", error)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_valid_display() {
        let result = ValidationResult::Valid;
        assert_eq!(result.to_string(), "Valid");
    }

    #[test]
    fn test_validation_result_invalid_field_count_display() {
        let result = ValidationResult::InvalidFieldCount {
            expected: 5,
            actual: 3,
        };
        assert_eq!(
            result.to_string(),
            "Invalid field count: expected 5 fields, found 3"
        );
    }

    #[test]
    fn test_validation_result_invalid_timestamp_display() {
        let result = ValidationResult::InvalidTimestamp {
            value: "not-a-date".to_string(),
            error: "invalid format".to_string(),
        };
        assert_eq!(
            result.to_string(),
            "Invalid timestamp \"not-a-date\": invalid format"
        );
    }

    #[test]
    fn test_validation_result_invalid_qos_display() {
        let result = ValidationResult::InvalidQos {
            value: "3".to_string(),
        };
        assert_eq!(
            result.to_string(),
            "Invalid QoS value \"3\": expected 0, 1, or 2"
        );
    }

    #[test]
    fn test_validation_result_invalid_retain_display() {
        let result = ValidationResult::InvalidRetain {
            value: "yes".to_string(),
        };
        assert_eq!(
            result.to_string(),
            "Invalid retain value \"yes\": expected \"true\" or \"false\""
        );
    }

    #[test]
    fn test_validation_result_invalid_base64_display() {
        let result = ValidationResult::InvalidBase64 {
            error: "invalid character at position 5".to_string(),
        };
        assert_eq!(
            result.to_string(),
            "Invalid base64 encoding: invalid character at position 5"
        );
    }

    #[test]
    fn test_validation_result_parse_error_display() {
        let result = ValidationResult::ParseError {
            error: "unexpected end of input".to_string(),
        };
        assert_eq!(result.to_string(), "Parse error: unexpected end of input");
    }

    #[test]
    fn test_validation_error_display() {
        let error = ValidationError {
            line_number: 42,
            result: ValidationResult::InvalidQos {
                value: "3".to_string(),
            },
        };
        assert_eq!(
            error.to_string(),
            "Line 42: Invalid QoS value \"3\": expected 0, 1, or 2"
        );
    }

    #[test]
    fn test_validation_stats_default() {
        let stats = ValidationStats::default();
        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.valid_records, 0);
        assert_eq!(stats.invalid_records, 0);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.binary_payloads, 0);
        assert_eq!(stats.base64_payloads, 0);
        assert_eq!(stats.largest_payload, 0);
        assert!(stats.errors.is_empty());
    }

    #[test]
    fn test_validation_stats_new() {
        let stats = ValidationStats::new();
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validation_stats_record_valid_text() {
        let mut stats = ValidationStats::new();
        stats.record_valid(100, false, false);

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.valid_records, 1);
        assert_eq!(stats.invalid_records, 0);
        assert_eq!(stats.text_payloads, 1);
        assert_eq!(stats.binary_payloads, 0);
        assert_eq!(stats.base64_payloads, 0);
        assert_eq!(stats.largest_payload, 100);
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validation_stats_record_valid_binary() {
        let mut stats = ValidationStats::new();
        stats.record_valid(200, true, false);

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.valid_records, 1);
        assert_eq!(stats.binary_payloads, 1);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.largest_payload, 200);
    }

    #[test]
    fn test_validation_stats_record_valid_base64() {
        let mut stats = ValidationStats::new();
        stats.record_valid(150, false, true);

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.valid_records, 1);
        assert_eq!(stats.base64_payloads, 1);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.binary_payloads, 0);
    }

    #[test]
    fn test_validation_stats_record_invalid() {
        let mut stats = ValidationStats::new();
        stats.record_invalid(
            10,
            ValidationResult::InvalidQos {
                value: "5".to_string(),
            },
        );

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.valid_records, 0);
        assert_eq!(stats.invalid_records, 1);
        assert!(!stats.is_valid());
        assert_eq!(stats.errors.len(), 1);
        assert_eq!(stats.errors[0].line_number, 10);
    }

    #[test]
    fn test_validation_stats_largest_payload_tracking() {
        let mut stats = ValidationStats::new();
        stats.record_valid(100, false, false);
        stats.record_valid(500, false, false);
        stats.record_valid(200, false, false);

        assert_eq!(stats.largest_payload, 500);
    }

    #[test]
    fn test_validation_stats_display_passed() {
        let mut stats = ValidationStats::new();
        stats.record_valid(100, false, false);
        stats.record_valid(200, true, false);

        let output = stats.to_string();
        assert!(output.contains("PASSED"));
        assert!(output.contains("Total records:"));
        assert!(output.contains("2"));
    }

    #[test]
    fn test_validation_stats_display_failed() {
        let mut stats = ValidationStats::new();
        stats.record_valid(100, false, false);
        stats.record_invalid(
            5,
            ValidationResult::InvalidQos {
                value: "3".to_string(),
            },
        );

        let output = stats.to_string();
        assert!(output.contains("FAILED"));
        assert!(output.contains("Errors:"));
        assert!(output.contains("Line 5"));
    }

    #[test]
    fn test_validation_result_equality() {
        let result1 = ValidationResult::InvalidQos {
            value: "3".to_string(),
        };
        let result2 = ValidationResult::InvalidQos {
            value: "3".to_string(),
        };
        let result3 = ValidationResult::InvalidQos {
            value: "4".to_string(),
        };

        assert_eq!(result1, result2);
        assert_ne!(result1, result3);
    }

    #[test]
    fn test_validation_result_clone() {
        let result = ValidationResult::InvalidTimestamp {
            value: "bad".to_string(),
            error: "parse error".to_string(),
        };
        let cloned = result.clone();
        assert_eq!(result, cloned);
    }

    #[test]
    fn test_validation_error_clone() {
        let error = ValidationError {
            line_number: 100,
            result: ValidationResult::Valid,
        };
        let cloned = error.clone();
        assert_eq!(cloned.line_number, 100);
    }

    #[test]
    fn test_validation_stats_clone() {
        let mut stats = ValidationStats::new();
        stats.record_valid(100, false, false);
        stats.record_invalid(5, ValidationResult::Valid);

        let cloned = stats.clone();
        assert_eq!(cloned.total_records, stats.total_records);
        assert_eq!(cloned.errors.len(), stats.errors.len());
    }
}

use std::path::Path;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::DateTime;
use csv::ReaderBuilder;

use crate::csv_handler::AUTO_ENCODE_MARKER;
use crate::error::MqttRecorderError;

/// CSV file validator.
///
/// The `CsvValidator` validates CSV files containing recorded MQTT messages.
/// It checks that each record has the correct format, valid field values,
/// and proper encoding. The validator continues processing even after finding
/// errors to provide a complete report.
///
/// # Features
///
/// - Validates field count (exactly 5 fields)
/// - Validates timestamp format (ISO 8601)
/// - Validates QoS values (0, 1, or 2)
/// - Validates retain values ("true" or "false")
/// - Validates base64 encoding when applicable
/// - Collects statistics about payload types
/// - Continues processing after errors for complete reporting
///
/// # Example
///
/// ```rust,ignore
/// use mqtt_recorder::validator::CsvValidator;
/// use std::path::Path;
///
/// let validator = CsvValidator::new(false, None);
/// let stats = validator.validate(Path::new("recording.csv"))?;
///
/// if stats.is_valid() {
///     println!("Validation passed!");
///     println!("Total records: {}", stats.total_records);
/// } else {
///     println!("Validation failed with {} errors", stats.invalid_records);
///     for error in &stats.errors {
///         println!("  {}", error);
///     }
/// }
/// ```
///
/// # Requirements
///
/// - **4.2**: WHEN validate mode is active, THE CSV_Validator SHALL read all records from the CSV file
/// - **4.11**: THE CSV_Validator SHALL continue checking all records even after finding errors to provide a complete report
pub struct CsvValidator {
    /// Whether to decode all payloads from base64.
    ///
    /// When true, all payloads are expected to be base64 encoded.
    /// When false, only payloads with the "b64:" prefix are decoded.
    decode_b64: bool,

    /// Optional maximum field size limit in bytes.
    ///
    /// If set, fields exceeding this size will cause a validation error.
    field_size_limit: Option<usize>,
}

impl CsvValidator {
    /// Creates a new CSV validator with the specified options.
    ///
    /// # Arguments
    ///
    /// * `decode_b64` - If true, all payloads are expected to be base64 encoded.
    ///   If false, only payloads with the "b64:" prefix are validated as base64.
    /// * `field_size_limit` - Optional maximum size for any CSV field in bytes.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::validator::CsvValidator;
    ///
    /// // Validator for files with auto-encoding (decode_b64 = false)
    /// let validator = CsvValidator::new(false, None);
    ///
    /// // Validator for files with global base64 encoding
    /// let validator_b64 = CsvValidator::new(true, None);
    ///
    /// // Validator with field size limit
    /// let validator_limited = CsvValidator::new(false, Some(1024 * 1024));
    /// ```
    pub fn new(decode_b64: bool, field_size_limit: Option<usize>) -> Self {
        Self {
            decode_b64,
            field_size_limit,
        }
    }

    /// Validate a CSV file and return statistics.
    ///
    /// This method reads all records in the file, validating each one
    /// and collecting statistics. It continues even after finding errors
    /// to provide a complete report.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the CSV file to validate
    ///
    /// # Returns
    ///
    /// Returns `Ok(ValidationStats)` with the validation results, or an error
    /// if the file cannot be opened or read.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError::Io`] if the file cannot be opened.
    /// Returns [`MqttRecorderError::Csv`] if the CSV reader cannot be created.
    ///
    /// Note: Individual record validation errors are collected in the
    /// `ValidationStats::errors` vector rather than causing this method to fail.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::validator::CsvValidator;
    /// use std::path::Path;
    ///
    /// let validator = CsvValidator::new(false, None);
    /// let stats = validator.validate(Path::new("recording.csv"))?;
    ///
    /// println!("Total: {}, Valid: {}, Invalid: {}",
    ///     stats.total_records, stats.valid_records, stats.invalid_records);
    /// ```
    ///
    /// # Requirements
    ///
    /// - **4.2**: WHEN validate mode is active, THE CSV_Validator SHALL read all records from the CSV file
    /// - **4.11**: THE CSV_Validator SHALL continue checking all records even after finding errors to provide a complete report
    pub fn validate(&self, path: &Path) -> Result<ValidationStats, MqttRecorderError> {
        let mut stats = ValidationStats::new();

        // Open the CSV file with headers enabled and flexible mode to allow
        // records with different field counts (we validate field count ourselves)
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_path(path)?;

        // Line 1 is the header row, data records start at line 2
        let mut line_number: u64 = 1;

        // Process each record, continuing even after errors (Requirement 4.11)
        for result in reader.records() {
            line_number += 1;

            match result {
                Ok(record) => {
                    // Validate the record
                    let validation_result = self.validate_record(&record, line_number);

                    match validation_result {
                        ValidationResult::Valid => {
                            // Determine payload type for statistics
                            let payload = &record[2];
                            let payload_size = payload.len();
                            let is_binary =
                                !self.decode_b64 && payload.starts_with(AUTO_ENCODE_MARKER);
                            let is_base64 = self.decode_b64;

                            stats.record_valid(payload_size, is_binary, is_base64);
                        }
                        _ => {
                            stats.record_invalid(line_number, validation_result);
                        }
                    }
                }
                Err(e) => {
                    // CSV parsing error - record as parse error and continue
                    stats.record_invalid(
                        line_number,
                        ValidationResult::ParseError {
                            error: e.to_string(),
                        },
                    );
                }
            }
        }

        Ok(stats)
    }

    /// Validate a single CSV record.
    ///
    /// This method validates all fields of a single CSV record:
    /// - Field count (exactly 5)
    /// - Timestamp format (ISO 8601)
    /// - QoS value (0, 1, or 2)
    /// - Retain value ("true" or "false")
    /// - Base64 encoding validity when applicable
    ///
    /// # Arguments
    ///
    /// * `record` - The CSV string record to validate
    /// * `line_number` - The line number for error reporting
    ///
    /// # Returns
    ///
    /// Returns `ValidationResult::Valid` if all checks pass, or a specific
    /// error variant describing the first validation failure found.
    ///
    /// # Requirements
    ///
    /// - **4.3**: WHEN validate mode is active, THE CSV_Validator SHALL verify each record has exactly 5 fields
    /// - **4.4**: WHEN validate mode is active, THE CSV_Validator SHALL verify timestamps are valid ISO 8601 format
    /// - **4.5**: WHEN validate mode is active, THE CSV_Validator SHALL verify QoS values are 0, 1, or 2
    /// - **4.6**: WHEN validate mode is active, THE CSV_Validator SHALL verify retain values are "true" or "false"
    /// - **4.7**: WHEN validate mode is active and decode_b64 is true, THE CSV_Validator SHALL verify all payloads are valid base64
    /// - **4.8**: WHEN validate mode is active and decode_b64 is false, THE CSV_Validator SHALL verify payloads with "b64:" prefix contain valid base64
    fn validate_record(&self, record: &csv::StringRecord, _line_number: u64) -> ValidationResult {
        // Requirement 4.3: Verify field count (exactly 5)
        if record.len() != 5 {
            return ValidationResult::InvalidFieldCount {
                expected: 5,
                actual: record.len(),
            };
        }

        // Check field size limits if configured
        if let Some(limit) = self.field_size_limit {
            for field in record.iter() {
                if field.len() > limit {
                    return ValidationResult::ParseError {
                        error: format!(
                            "Field exceeds size limit of {} bytes (actual: {} bytes)",
                            limit,
                            field.len()
                        ),
                    };
                }
            }
        }

        // Requirement 4.4: Verify timestamp is valid ISO 8601 format
        let timestamp_str = &record[0];
        if let Err(e) = Self::validate_timestamp(timestamp_str) {
            return ValidationResult::InvalidTimestamp {
                value: timestamp_str.to_string(),
                error: e,
            };
        }

        // Requirement 4.5: Verify QoS is 0, 1, or 2
        let qos_str = &record[3];
        if Self::validate_qos(qos_str).is_err() {
            return ValidationResult::InvalidQos {
                value: qos_str.to_string(),
            };
        }

        // Requirement 4.6: Verify retain is "true" or "false"
        let retain_str = &record[4];
        if Self::validate_retain(retain_str).is_err() {
            return ValidationResult::InvalidRetain {
                value: retain_str.to_string(),
            };
        }

        // Validate payload encoding
        let payload_str = &record[2];
        if let Err(e) = self.validate_payload(payload_str) {
            return ValidationResult::InvalidBase64 { error: e };
        }

        ValidationResult::Valid
    }

    /// Validates a timestamp string is in ISO 8601 format.
    ///
    /// Accepts timestamps in RFC 3339 format or the format used by the writer
    /// (e.g., "2024-01-15T10:30:00.123Z").
    fn validate_timestamp(timestamp_str: &str) -> Result<(), String> {
        // Try RFC 3339 format first
        if DateTime::parse_from_rfc3339(timestamp_str).is_ok() {
            return Ok(());
        }

        // Try the format we write (which may not have timezone offset)
        if chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.3fZ").is_ok() {
            return Ok(());
        }

        // Try without milliseconds
        if chrono::NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").is_ok() {
            return Ok(());
        }

        Err("expected ISO 8601 format (e.g., 2024-01-15T10:30:00.123Z)".to_string())
    }

    /// Validates a QoS string is 0, 1, or 2.
    fn validate_qos(qos_str: &str) -> Result<u8, ()> {
        match qos_str {
            "0" => Ok(0),
            "1" => Ok(1),
            "2" => Ok(2),
            _ => Err(()),
        }
    }

    /// Validates a retain string is "true" or "false".
    fn validate_retain(retain_str: &str) -> Result<bool, ()> {
        match retain_str {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(()),
        }
    }

    /// Validates payload encoding based on decode_b64 setting.
    ///
    /// - When decode_b64 is true: validates that the entire payload is valid base64
    /// - When decode_b64 is false: validates that payloads with "b64:" prefix contain valid base64
    fn validate_payload(&self, payload_str: &str) -> Result<(), String> {
        if self.decode_b64 {
            // Requirement 4.7: When decode_b64 is true, verify all payloads are valid base64
            BASE64_STANDARD
                .decode(payload_str)
                .map_err(|e| e.to_string())?;
        } else if let Some(encoded_content) = payload_str.strip_prefix(AUTO_ENCODE_MARKER) {
            // Requirement 4.8: When decode_b64 is false, verify payloads with "b64:" prefix contain valid base64
            BASE64_STANDARD
                .decode(encoded_content)
                .map_err(|e| format!("invalid base64 after 'b64:' prefix: {}", e))?;
        }
        // Text payloads without prefix are always valid

        Ok(())
    }
}

#[cfg(test)]
mod csv_validator_tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Helper function to create a temporary CSV file with the given content.
    fn create_temp_csv(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(content.as_bytes())
            .expect("Failed to write to temp file");
        file.flush().expect("Failed to flush temp file");
        file
    }

    #[test]
    fn test_csv_validator_new() {
        let validator = CsvValidator::new(false, None);
        assert!(!validator.decode_b64);
        assert!(validator.field_size_limit.is_none());

        let validator_b64 = CsvValidator::new(true, Some(1024));
        assert!(validator_b64.decode_b64);
        assert_eq!(validator_b64.field_size_limit, Some(1024));
    }

    #[test]
    fn test_validate_empty_file_with_header() {
        let csv_content = "timestamp,topic,payload,qos,retain\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.valid_records, 0);
        assert_eq!(stats.invalid_records, 0);
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validate_single_valid_record() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,{\"value\": 23.5},0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.valid_records, 1);
        assert_eq!(stats.invalid_records, 0);
        assert_eq!(stats.text_payloads, 1);
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validate_multiple_valid_records() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,{\"value\": 23.5},0,false\n\
                          2024-01-15T10:30:01.456Z,sensors/humidity,{\"value\": 65},1,true\n\
                          2024-01-15T10:30:02.789Z,text/msg,Hello World,2,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.valid_records, 3);
        assert_eq!(stats.invalid_records, 0);
        assert_eq!(stats.text_payloads, 3);
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validate_binary_payload_with_prefix() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,binary/data,b64:SGVsbG8gV29ybGQ=,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.valid_records, 1);
        assert_eq!(stats.binary_payloads, 1);
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validate_invalid_field_count() {
        // Use 6 fields instead of 5 to ensure we get InvalidFieldCount
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,payload,0,false,extra\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.invalid_records, 1);
        assert!(!stats.is_valid());
        assert_eq!(stats.errors.len(), 1);
        assert!(matches!(
            stats.errors[0].result,
            ValidationResult::InvalidFieldCount {
                expected: 5,
                actual: 6
            }
        ));
    }

    #[test]
    fn test_validate_too_few_fields() {
        // Test with fewer fields - CSV parser may return a parse error or short record
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,payload\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.invalid_records, 1);
        assert!(!stats.is_valid());
        // The error could be InvalidFieldCount or ParseError depending on CSV parser behavior
        assert_eq!(stats.errors.len(), 1);
    }

    #[test]
    fn test_validate_invalid_timestamp() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          not-a-timestamp,sensors/temp,payload,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.invalid_records, 1);
        assert!(matches!(
            &stats.errors[0].result,
            ValidationResult::InvalidTimestamp { value, .. } if value == "not-a-timestamp"
        ));
    }

    #[test]
    fn test_validate_invalid_qos() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,payload,3,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.invalid_records, 1);
        assert!(matches!(
            &stats.errors[0].result,
            ValidationResult::InvalidQos { value } if value == "3"
        ));
    }

    #[test]
    fn test_validate_invalid_retain() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,payload,0,yes\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.invalid_records, 1);
        assert!(matches!(
            &stats.errors[0].result,
            ValidationResult::InvalidRetain { value } if value == "yes"
        ));
    }

    #[test]
    fn test_validate_invalid_base64_with_prefix() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,binary/data,b64:not-valid-base64!!!,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.invalid_records, 1);
        assert!(matches!(
            &stats.errors[0].result,
            ValidationResult::InvalidBase64 { .. }
        ));
    }

    #[test]
    fn test_validate_invalid_base64_decode_b64_mode() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,not-valid-base64!!!,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(true, None); // decode_b64 = true
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.invalid_records, 1);
        assert!(matches!(
            &stats.errors[0].result,
            ValidationResult::InvalidBase64 { .. }
        ));
    }

    #[test]
    fn test_validate_continues_after_errors() {
        // Requirement 4.11: Continue checking all records even after finding errors
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,valid,0,false\n\
                          invalid-timestamp,sensors/temp,payload,0,false\n\
                          2024-01-15T10:30:02.123Z,sensors/temp,valid,0,false\n\
                          2024-01-15T10:30:03.123Z,sensors/temp,payload,5,false\n\
                          2024-01-15T10:30:04.123Z,sensors/temp,valid,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        // Should have processed all 5 records
        assert_eq!(stats.total_records, 5);
        assert_eq!(stats.valid_records, 3);
        assert_eq!(stats.invalid_records, 2);
        assert_eq!(stats.errors.len(), 2);

        // Verify error line numbers
        assert_eq!(stats.errors[0].line_number, 3); // invalid timestamp
        assert_eq!(stats.errors[1].line_number, 5); // invalid qos
    }

    #[test]
    fn test_validate_file_not_found() {
        let validator = CsvValidator::new(false, None);
        let result = validator.validate(Path::new("/nonexistent/path/file.csv"));

        assert!(result.is_err());
    }

    #[test]
    fn test_validate_with_field_size_limit() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,sensors/temp,this_is_a_very_long_payload_that_exceeds_the_limit,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, Some(10)); // 10 byte limit
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.invalid_records, 1);
        assert!(matches!(
            &stats.errors[0].result,
            ValidationResult::ParseError { error } if error.contains("exceeds size limit")
        ));
    }

    #[test]
    fn test_validate_largest_payload_tracking() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,t,short,0,false\n\
                          2024-01-15T10:30:01.123Z,t,this_is_a_longer_payload,0,false\n\
                          2024-01-15T10:30:02.123Z,t,medium_payload,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.largest_payload, "this_is_a_longer_payload".len());
    }

    #[test]
    fn test_validate_mixed_payload_types() {
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,text/msg,Hello World,0,false\n\
                          2024-01-15T10:30:01.123Z,binary/data,b64:SGVsbG8=,0,false\n\
                          2024-01-15T10:30:02.123Z,text/msg,Another text,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.text_payloads, 2);
        assert_eq!(stats.binary_payloads, 1);
    }

    #[test]
    fn test_validate_base64_mode_counts() {
        // Valid base64 content
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,t,SGVsbG8=,0,false\n\
                          2024-01-15T10:30:01.123Z,t,V29ybGQ=,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(true, None); // decode_b64 = true
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 2);
        assert_eq!(stats.base64_payloads, 2);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.binary_payloads, 0);
    }

    #[test]
    fn test_validate_timestamp_formats() {
        // Test various valid ISO 8601 timestamp formats
        let csv_content = "timestamp,topic,payload,qos,retain\n\
                          2024-01-15T10:30:00.123Z,t,p,0,false\n\
                          2024-01-15T10:30:00Z,t,p,0,false\n\
                          2024-01-15T10:30:00.000Z,t,p,0,false\n";
        let file = create_temp_csv(csv_content);

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(file.path()).expect("Validation failed");

        assert_eq!(stats.total_records, 3);
        assert_eq!(stats.valid_records, 3);
        assert!(stats.is_valid());
    }

    #[test]
    fn test_validate_record_directly() {
        let validator = CsvValidator::new(false, None);

        // Valid record
        let valid_record = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.123Z",
            "topic",
            "payload",
            "0",
            "false",
        ]);
        assert_eq!(
            validator.validate_record(&valid_record, 2),
            ValidationResult::Valid
        );

        // Invalid field count
        let invalid_count = csv::StringRecord::from(vec!["2024-01-15T10:30:00.123Z", "topic"]);
        assert!(matches!(
            validator.validate_record(&invalid_count, 2),
            ValidationResult::InvalidFieldCount { .. }
        ));

        // Invalid QoS
        let invalid_qos = csv::StringRecord::from(vec![
            "2024-01-15T10:30:00.123Z",
            "topic",
            "payload",
            "invalid",
            "false",
        ]);
        assert!(matches!(
            validator.validate_record(&invalid_qos, 2),
            ValidationResult::InvalidQos { .. }
        ));
    }
}
