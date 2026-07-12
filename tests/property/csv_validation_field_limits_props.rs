//! Property-based tests for CSV validation error detection
//!
//! This module contains property tests that validate the CSV_Validator's ability to
//! detect and report errors (wrong field count, invalid timestamp, invalid QoS,
//! invalid retain, invalid base64) with the correct line number, and to continue
//! processing remaining records after finding errors.
//!
//! **Feature: csv-validation**

use proptest::prelude::*;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use std::path::Path;
use tempfile::tempdir;

// =============================================================================
// Property 4: Validation Error Detection
// =============================================================================
//
// Feature: csv-validation, Property 4: Validation Error Detection
// *For any* CSV record with invalid content (wrong field count, invalid timestamp,
// invalid QoS, invalid retain, invalid base64), the CSV_Validator SHALL detect and
// report the error with the correct line number. The validator SHALL continue
// processing remaining records after finding errors.
//
// **Validates: Requirements 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.11**

use mqtt_recorder::validator::{CsvValidator, ValidationResult};
use std::io::Write;

/// Strategy for generating invalid field counts (not 5)
fn invalid_field_count_strategy() -> impl Strategy<Value = usize> {
    prop_oneof![
        // Note: 0 fields (empty line) may be skipped by CSV parser, so we start at 1
        Just(1usize),
        Just(2usize),
        Just(3usize),
        Just(4usize),
        Just(6usize),
        Just(7usize),
        Just(10usize),
    ]
}

/// Strategy for generating invalid timestamp strings
fn invalid_timestamp_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("not-a-timestamp".to_string()),
        Just("2024-13-01T00:00:00Z".to_string()), // Invalid month
        Just("2024-01-32T00:00:00Z".to_string()), // Invalid day
        Just("2024-01-01T25:00:00Z".to_string()), // Invalid hour
        Just("2024-01-01T00:60:00Z".to_string()), // Invalid minute
        Just("2024-01-01T00:00:99Z".to_string()), // Invalid second (clearly invalid)
        Just("2024/01/01 00:00:00".to_string()),  // Wrong format
        Just("01-01-2024T00:00:00Z".to_string()), // Wrong order
        Just("".to_string()),                     // Empty
        Just("12345".to_string()),                // Just numbers
        "[a-zA-Z0-9]{5,20}".prop_map(|s: String| s), // Random strings
    ]
}

/// Strategy for generating invalid QoS values
fn invalid_qos_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("3".to_string()),
        Just("4".to_string()),
        Just("-1".to_string()),
        Just("10".to_string()),
        Just("".to_string()),
        Just("one".to_string()),
        Just("0.5".to_string()),
        Just("00".to_string()),
        "[a-zA-Z]{1,5}".prop_map(|s: String| s),
    ]
}

/// Strategy for generating invalid retain values
fn invalid_retain_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("True".to_string()),
        Just("False".to_string()),
        Just("TRUE".to_string()),
        Just("FALSE".to_string()),
        Just("yes".to_string()),
        Just("no".to_string()),
        Just("1".to_string()),
        Just("0".to_string()),
        Just("".to_string()),
        "[a-zA-Z]{1,5}".prop_map(|s: String| s),
    ]
}

/// Strategy for generating invalid base64 strings
fn invalid_base64_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("!!!invalid!!!".to_string()),
        Just("not base64 at all".to_string()),
        Just("SGVsbG8gV29ybGQ".to_string()), // Missing padding (might be valid in some decoders)
        Just("====".to_string()),            // Only padding
        Just("@#$%^&*()".to_string()),       // Special characters
        Just("b64:!!!invalid!!!".to_string()), // Invalid after marker
    ]
}

/// Strategy for generating valid timestamps for use in test CSV files
fn valid_timestamp_for_csv_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("2024-01-15T10:30:00.123Z".to_string()),
        Just("2024-06-20T15:45:30.000Z".to_string()),
        Just("2023-12-31T23:59:59.999Z".to_string()),
    ]
}

/// Helper function to create a CSV file with raw lines (for testing malformed CSV)
fn create_raw_csv_file(path: &Path, lines: &[&str]) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    // Write header
    writeln!(file, "timestamp,topic,payload,qos,retain")?;
    // Write raw lines
    for line in lines {
        writeln!(file, "{}", line)?;
    }
    file.flush()?;
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Wrong field count is detected and reported with correct line number
    // **Validates: Requirements 4.3, 4.11**
    #[test]
    fn property_4_wrong_field_count_detected(
        field_count in invalid_field_count_strategy(),
        line_offset in 0usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create records: some valid, then one with wrong field count
        let mut lines: Vec<String> = Vec::new();

        // Add valid records before the invalid one
        for _ in 0..line_offset {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
        }

        // Add record with wrong field count
        let invalid_line = match field_count {
            1 => "only_one_field".to_string(),
            2 => "field1,field2".to_string(),
            3 => "field1,field2,field3".to_string(),
            4 => "2024-01-15T10:30:00.123Z,topic,payload,0".to_string(),
            6 => "2024-01-15T10:30:00.123Z,topic,payload,0,false,extra".to_string(),
            _ => (0..field_count).map(|i| format!("field{}", i)).collect::<Vec<_>>().join(","),
        };
        lines.push(invalid_line);

        // Add more valid records after
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,1,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Expected line number: header is line 1, first data record is line 2
        let expected_error_line = (line_offset + 2) as u64;

        // Verify: Error is detected
        prop_assert!(
            stats.invalid_records > 0,
            "Should detect invalid record with {} fields",
            field_count
        );

        // Verify: Correct error type and line number
        let error = stats.errors.iter().find(|e| e.line_number == expected_error_line);
        prop_assert!(
            error.is_some(),
            "Should report error at line {}. Errors: {:?}",
            expected_error_line,
            stats.errors
        );

        if let Some(err) = error {
            match &err.result {
                ValidationResult::InvalidFieldCount { expected, actual } => {
                    prop_assert_eq!(*expected, 5, "Expected field count should be 5");
                    prop_assert_eq!(*actual, field_count, "Actual field count should match");
                }
                other => {
                    prop_assert!(false, "Expected InvalidFieldCount, got {:?}", other);
                }
            }
        }

        // Verify: Validator continues after error (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            (lines.len()) as u64,
            "Should process all records including those after error"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Invalid timestamp is detected and reported with correct line number
    // **Validates: Requirements 4.4, 4.11**
    #[test]
    fn property_4_invalid_timestamp_detected(
        invalid_ts in invalid_timestamp_strategy(),
        line_offset in 0usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create records: some valid, then one with invalid timestamp
        let mut lines: Vec<String> = Vec::new();

        // Add valid records before the invalid one
        for _ in 0..line_offset {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
        }

        // Add record with invalid timestamp (escape quotes for CSV)
        let escaped_ts = invalid_ts.replace("\"", "\"\"");
        let invalid_line = format!("\"{}\",test/topic,payload,0,false", escaped_ts);
        lines.push(invalid_line);

        // Add more valid records after
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,1,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Expected line number: header is line 1, first data record is line 2
        let expected_error_line = (line_offset + 2) as u64;

        // Verify: Error is detected
        prop_assert!(
            stats.invalid_records > 0,
            "Should detect invalid timestamp: {}",
            invalid_ts
        );

        // Verify: Correct error type and line number
        let error = stats.errors.iter().find(|e| e.line_number == expected_error_line);
        prop_assert!(
            error.is_some(),
            "Should report error at line {}. Errors: {:?}",
            expected_error_line,
            stats.errors
        );

        if let Some(err) = error {
            match &err.result {
                ValidationResult::InvalidTimestamp { value, error: _ } => {
                    prop_assert_eq!(value, &invalid_ts, "Should report the invalid timestamp value");
                }
                other => {
                    prop_assert!(false, "Expected InvalidTimestamp, got {:?}", other);
                }
            }
        }

        // Verify: Validator continues after error (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            (lines.len()) as u64,
            "Should process all records including those after error"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Invalid QoS is detected and reported with correct line number
    // **Validates: Requirements 4.5, 4.11**
    #[test]
    fn property_4_invalid_qos_detected(
        invalid_qos in invalid_qos_strategy(),
        line_offset in 0usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create records: some valid, then one with invalid QoS
        let mut lines: Vec<String> = Vec::new();

        // Add valid records before the invalid one
        for _ in 0..line_offset {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
        }

        // Add record with invalid QoS (escape quotes for CSV)
        let escaped_qos = invalid_qos.replace("\"", "\"\"");
        let invalid_line = format!("2024-01-15T10:30:00.123Z,test/topic,payload,\"{}\",false", escaped_qos);
        lines.push(invalid_line);

        // Add more valid records after
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,1,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Expected line number: header is line 1, first data record is line 2
        let expected_error_line = (line_offset + 2) as u64;

        // Verify: Error is detected
        prop_assert!(
            stats.invalid_records > 0,
            "Should detect invalid QoS: {}",
            invalid_qos
        );

        // Verify: Correct error type and line number
        let error = stats.errors.iter().find(|e| e.line_number == expected_error_line);
        prop_assert!(
            error.is_some(),
            "Should report error at line {}. Errors: {:?}",
            expected_error_line,
            stats.errors
        );

        if let Some(err) = error {
            match &err.result {
                ValidationResult::InvalidQos { value } => {
                    prop_assert_eq!(value, &invalid_qos, "Should report the invalid QoS value");
                }
                other => {
                    prop_assert!(false, "Expected InvalidQos, got {:?}", other);
                }
            }
        }

        // Verify: Validator continues after error (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            (lines.len()) as u64,
            "Should process all records including those after error"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Invalid retain value is detected and reported with correct line number
    // **Validates: Requirements 4.6, 4.11**
    #[test]
    fn property_4_invalid_retain_detected(
        invalid_retain in invalid_retain_strategy(),
        line_offset in 0usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create records: some valid, then one with invalid retain
        let mut lines: Vec<String> = Vec::new();

        // Add valid records before the invalid one
        for _ in 0..line_offset {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
        }

        // Add record with invalid retain (escape quotes for CSV)
        let escaped_retain = invalid_retain.replace("\"", "\"\"");
        let invalid_line = format!("2024-01-15T10:30:00.123Z,test/topic,payload,0,\"{}\"", escaped_retain);
        lines.push(invalid_line);

        // Add more valid records after
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,1,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Expected line number: header is line 1, first data record is line 2
        let expected_error_line = (line_offset + 2) as u64;

        // Verify: Error is detected
        prop_assert!(
            stats.invalid_records > 0,
            "Should detect invalid retain: {}",
            invalid_retain
        );

        // Verify: Correct error type and line number
        let error = stats.errors.iter().find(|e| e.line_number == expected_error_line);
        prop_assert!(
            error.is_some(),
            "Should report error at line {}. Errors: {:?}",
            expected_error_line,
            stats.errors
        );

        if let Some(err) = error {
            match &err.result {
                ValidationResult::InvalidRetain { value } => {
                    prop_assert_eq!(value, &invalid_retain, "Should report the invalid retain value");
                }
                other => {
                    prop_assert!(false, "Expected InvalidRetain, got {:?}", other);
                }
            }
        }

        // Verify: Validator continues after error (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            (lines.len()) as u64,
            "Should process all records including those after error"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Invalid base64 is detected when decode_b64 is true
    // **Validates: Requirements 4.7, 4.11**
    #[test]
    fn property_4_invalid_base64_detected_global_mode(
        invalid_b64 in invalid_base64_strategy(),
        line_offset in 0usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create records: some valid base64, then one with invalid base64
        let mut lines: Vec<String> = Vec::new();

        // Add valid base64 records before the invalid one
        for _ in 0..line_offset {
            // "payload" in base64 is "cGF5bG9hZA=="
            lines.push("2024-01-15T10:30:00.123Z,test/topic,cGF5bG9hZA==,0,false".to_string());
        }

        // Add record with invalid base64 (escape quotes for CSV)
        let escaped_b64 = invalid_b64.replace("\"", "\"\"");
        let invalid_line = format!("2024-01-15T10:30:00.123Z,test/topic,\"{}\",0,false", escaped_b64);
        lines.push(invalid_line);

        // Add more valid base64 records after
        lines.push("2024-01-15T10:30:00.123Z,test/topic,cGF5bG9hZA==,1,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate with decode_b64 = true (global base64 mode)
        let validator = CsvValidator::new(true, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Expected line number: header is line 1, first data record is line 2
        let expected_error_line = (line_offset + 2) as u64;

        // Verify: Error is detected
        prop_assert!(
            stats.invalid_records > 0,
            "Should detect invalid base64: {}",
            invalid_b64
        );

        // Verify: Correct error type and line number
        let error = stats.errors.iter().find(|e| e.line_number == expected_error_line);
        prop_assert!(
            error.is_some(),
            "Should report error at line {}. Errors: {:?}",
            expected_error_line,
            stats.errors
        );

        if let Some(err) = error {
            match &err.result {
                ValidationResult::InvalidBase64 { error: _ } => {
                    // Success - correct error type detected
                }
                other => {
                    prop_assert!(false, "Expected InvalidBase64, got {:?}", other);
                }
            }
        }

        // Verify: Validator continues after error (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            (lines.len()) as u64,
            "Should process all records including those after error"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Invalid base64 after "b64:" prefix is detected when decode_b64 is false
    // **Validates: Requirements 4.8, 4.11**
    #[test]
    fn property_4_invalid_base64_after_marker_detected(
        line_offset in 0usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create records: some valid, then one with invalid base64 after marker
        let mut lines: Vec<String> = Vec::new();

        // Add valid records before the invalid one
        for _ in 0..line_offset {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,text_payload,0,false".to_string());
        }

        // Add record with "b64:" prefix but invalid base64 content
        let invalid_line = "2024-01-15T10:30:00.123Z,test/topic,b64:!!!not-valid-base64!!!,0,false".to_string();
        lines.push(invalid_line);

        // Add more valid records after
        lines.push("2024-01-15T10:30:00.123Z,test/topic,another_payload,1,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate with decode_b64 = false (auto-decoding mode)
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Expected line number: header is line 1, first data record is line 2
        let expected_error_line = (line_offset + 2) as u64;

        // Verify: Error is detected
        prop_assert!(
            stats.invalid_records > 0,
            "Should detect invalid base64 after b64: marker"
        );

        // Verify: Correct error type and line number
        let error = stats.errors.iter().find(|e| e.line_number == expected_error_line);
        prop_assert!(
            error.is_some(),
            "Should report error at line {}. Errors: {:?}",
            expected_error_line,
            stats.errors
        );

        if let Some(err) = error {
            match &err.result {
                ValidationResult::InvalidBase64 { error: _ } => {
                    // Success - correct error type detected
                }
                other => {
                    prop_assert!(false, "Expected InvalidBase64, got {:?}", other);
                }
            }
        }

        // Verify: Validator continues after error (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            (lines.len()) as u64,
            "Should process all records including those after error"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Multiple errors in same file are all detected with correct line numbers
    // **Validates: Requirements 4.3, 4.4, 4.5, 4.6, 4.11**
    #[test]
    fn property_4_multiple_errors_all_detected(
        num_valid_between in 0usize..3,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a file with multiple different types of errors
        let mut lines: Vec<String> = Vec::new();
        let mut expected_errors: Vec<(u64, &str)> = Vec::new();

        // Error 1: Invalid field count
        lines.push("field1,field2,field3".to_string());
        expected_errors.push((2, "InvalidFieldCount"));

        // Add some valid records
        for _ in 0..num_valid_between {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
        }

        // Error 2: Invalid timestamp
        let ts_error_line = (lines.len() + 2) as u64;
        lines.push("not-a-timestamp,test/topic,payload,0,false".to_string());
        expected_errors.push((ts_error_line, "InvalidTimestamp"));

        // Add some valid records
        for _ in 0..num_valid_between {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,1,false".to_string());
        }

        // Error 3: Invalid QoS
        let qos_error_line = (lines.len() + 2) as u64;
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,99,false".to_string());
        expected_errors.push((qos_error_line, "InvalidQos"));

        // Add some valid records
        for _ in 0..num_valid_between {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,2,true".to_string());
        }

        // Error 4: Invalid retain
        let retain_error_line = (lines.len() + 2) as u64;
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,maybe".to_string());
        expected_errors.push((retain_error_line, "InvalidRetain"));

        // Add final valid record
        lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,true".to_string());

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Verify: All errors are detected
        prop_assert_eq!(
            stats.invalid_records,
            expected_errors.len() as u64,
            "Should detect all {} errors. Found: {:?}",
            expected_errors.len(),
            stats.errors
        );

        // Verify: Each error has correct line number
        for (expected_line, error_type) in &expected_errors {
            let found = stats.errors.iter().any(|e| e.line_number == *expected_line);
            prop_assert!(
                found,
                "Should find {} error at line {}. Errors: {:?}",
                error_type,
                expected_line,
                stats.errors
            );
        }

        // Verify: All records were processed (Requirement 4.11)
        prop_assert_eq!(
            stats.total_records,
            lines.len() as u64,
            "Should process all {} records",
            lines.len()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Validator continues processing after finding errors (Requirement 4.11)
    // **Validates: Requirements 4.11**
    #[test]
    fn property_4_continues_after_errors(
        num_errors in 1usize..5,
        num_valid_after_each in 1usize..3,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a file with errors interspersed with valid records
        let mut lines: Vec<String> = Vec::new();
        let mut error_count = 0;
        let mut valid_count = 0;

        for i in 0..num_errors {
            // Add an error (alternate between different error types)
            match i % 4 {
                0 => lines.push("too,few,fields".to_string()),
                1 => lines.push("bad-timestamp,topic,payload,0,false".to_string()),
                2 => lines.push("2024-01-15T10:30:00.123Z,topic,payload,9,false".to_string()),
                _ => lines.push("2024-01-15T10:30:00.123Z,topic,payload,0,invalid".to_string()),
            }
            error_count += 1;

            // Add valid records after each error
            for _ in 0..num_valid_after_each {
                lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
                valid_count += 1;
            }
        }

        // Write the CSV file
        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Verify: All records were processed
        prop_assert_eq!(
            stats.total_records,
            (error_count + valid_count) as u64,
            "Should process all {} records (errors + valid)",
            error_count + valid_count
        );

        // Verify: Correct number of errors detected
        prop_assert_eq!(
            stats.invalid_records,
            error_count as u64,
            "Should detect all {} errors",
            error_count
        );

        // Verify: Correct number of valid records counted
        prop_assert_eq!(
            stats.valid_records,
            valid_count as u64,
            "Should count all {} valid records",
            valid_count
        );
    }
}

// Additional edge case tests for Property 4

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Valid records are correctly identified (no false positives)
    // **Validates: Requirements 4.3, 4.4, 4.5, 4.6**
    #[test]
    fn property_4_valid_records_pass_validation(
        topic in "[a-zA-Z][a-zA-Z0-9/]{0,20}",
        payload in "[a-zA-Z0-9 ]{0,50}",
        qos in 0u8..=2u8,
        retain in any::<bool>(),
        timestamp in valid_timestamp_for_csv_strategy(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a valid record
        let retain_str = if retain { "true" } else { "false" };
        let line = format!("{},{},{},{},{}", timestamp, topic, payload, qos, retain_str);

        // Write the CSV file
        create_raw_csv_file(&file_path, &[&line])
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Verify: No errors detected
        prop_assert_eq!(
            stats.invalid_records,
            0,
            "Valid record should not produce errors. Errors: {:?}",
            stats.errors
        );

        // Verify: Record counted as valid
        prop_assert_eq!(
            stats.valid_records,
            1,
            "Should count one valid record"
        );
    }

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Error at first record is detected correctly
    // **Validates: Requirements 4.3, 4.11**
    #[test]
    fn property_4_error_at_first_record_detected(
        _dummy in Just(())
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create file with error at first record
        let lines = [
            "invalid,record",  // Error at line 2 (first data record)
            "2024-01-15T10:30:00.123Z,test/topic,payload,0,false",  // Valid
        ];

        create_raw_csv_file(&file_path, &lines)
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Verify: Error detected at line 2
        prop_assert_eq!(stats.invalid_records, 1, "Should detect one error");
        prop_assert_eq!(stats.errors[0].line_number, 2, "Error should be at line 2");

        // Verify: Second record still processed
        prop_assert_eq!(stats.valid_records, 1, "Should count one valid record");
        prop_assert_eq!(stats.total_records, 2, "Should process both records");
    }

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Error at last record is detected correctly
    // **Validates: Requirements 4.3, 4.11**
    #[test]
    fn property_4_error_at_last_record_detected(
        num_valid in 1usize..5,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create file with valid records followed by error at last record
        let mut lines: Vec<String> = Vec::new();
        for _ in 0..num_valid {
            lines.push("2024-01-15T10:30:00.123Z,test/topic,payload,0,false".to_string());
        }
        lines.push("invalid,record,at,end".to_string());  // Error at last record

        let expected_error_line = (num_valid + 2) as u64;  // +1 for header, +1 for 1-indexing

        create_raw_csv_file(&file_path, &lines.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
            .expect("Failed to create test CSV");

        // Validate
        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        // Verify: Error detected at last line
        prop_assert_eq!(stats.invalid_records, 1, "Should detect one error");
        prop_assert_eq!(
            stats.errors[0].line_number,
            expected_error_line,
            "Error should be at line {}",
            expected_error_line
        );

        // Verify: All valid records counted
        prop_assert_eq!(stats.valid_records, num_valid as u64, "Should count all valid records");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: All valid QoS values (0, 1, 2) pass validation
    // **Validates: Requirements 4.5**
    #[test]
    fn property_4_all_valid_qos_values_pass(
        qos in 0u8..=2u8,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let line = format!("2024-01-15T10:30:00.123Z,test/topic,payload,{},false", qos);

        create_raw_csv_file(&file_path, &[&line])
            .expect("Failed to create test CSV");

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        prop_assert_eq!(
            stats.invalid_records,
            0,
            "QoS {} should be valid. Errors: {:?}",
            qos,
            stats.errors
        );
    }

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Both valid retain values (true, false) pass validation
    // **Validates: Requirements 4.6**
    #[test]
    fn property_4_both_retain_values_pass(
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let retain_str = if retain { "true" } else { "false" };
        let line = format!("2024-01-15T10:30:00.123Z,test/topic,payload,0,{}", retain_str);

        create_raw_csv_file(&file_path, &[&line])
            .expect("Failed to create test CSV");

        let validator = CsvValidator::new(false, None);
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        prop_assert_eq!(
            stats.invalid_records,
            0,
            "Retain '{}' should be valid. Errors: {:?}",
            retain_str,
            stats.errors
        );
    }

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Valid base64 payloads pass validation in global base64 mode
    // **Validates: Requirements 4.7**
    #[test]
    fn property_4_valid_base64_passes_global_mode(
        payload in prop::collection::vec(any::<u8>(), 0..100),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let base64_payload = BASE64_STANDARD.encode(&payload);
        let line = format!("2024-01-15T10:30:00.123Z,test/topic,{},0,false", base64_payload);

        create_raw_csv_file(&file_path, &[&line])
            .expect("Failed to create test CSV");

        let validator = CsvValidator::new(true, None);  // decode_b64 = true
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        prop_assert_eq!(
            stats.invalid_records,
            0,
            "Valid base64 should pass validation. Errors: {:?}",
            stats.errors
        );
    }

    // Feature: csv-validation, Property 4: Validation Error Detection
    // Test: Valid base64 after "b64:" marker passes validation in auto mode
    // **Validates: Requirements 4.8**
    #[test]
    fn property_4_valid_base64_after_marker_passes(
        payload in prop::collection::vec(any::<u8>(), 0..100),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let base64_payload = BASE64_STANDARD.encode(&payload);
        let line = format!("2024-01-15T10:30:00.123Z,test/topic,b64:{},0,false", base64_payload);

        create_raw_csv_file(&file_path, &[&line])
            .expect("Failed to create test CSV");

        let validator = CsvValidator::new(false, None);  // decode_b64 = false (auto mode)
        let stats = validator.validate(&file_path).expect("Validation should not fail");

        prop_assert_eq!(
            stats.invalid_records,
            0,
            "Valid base64 after b64: marker should pass validation. Errors: {:?}",
            stats.errors
        );
    }
}
