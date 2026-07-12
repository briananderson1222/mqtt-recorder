//! Property-based tests for CSV base64 ("b64:") auto-encode marker handling
//!
//! This module contains property tests that validate the encoding/decoding strategy
//! for the auto-encode marker used to safely round-trip binary payloads through CSV,
//! including collision handling when text payloads literally start with the marker.
//!
//! **Feature: csv-validation**

use proptest::prelude::*;

/// Strategy for generating valid UTF-8 text payloads (no control characters except allowed ones)
/// These should be classified as TEXT
fn valid_text_payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Pure ASCII printable characters (0x20-0x7E)
        prop::collection::vec(0x20u8..=0x7E, 0..256),
        // ASCII with allowed whitespace (tab 0x09, newline 0x0A, carriage return 0x0D)
        prop::collection::vec(
            prop_oneof![
                0x20u8..=0x7E, // Printable ASCII
                Just(0x09u8),  // Tab
                Just(0x0Au8),  // Newline (LF)
                Just(0x0Du8),  // Carriage return (CR)
            ],
            0..256
        ),
        // Empty payload (should be text per Requirement 1.4)
        Just(vec![]),
    ]
}

/// Strategy for generating payloads with binary control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F)
/// These should be classified as BINARY
fn binary_control_char_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![
        0x00u8..=0x08, // NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, BS
        0x0Bu8..=0x0C, // VT, FF
        0x0Eu8..=0x1F, // SO through US
    ]
}

/// Strategy for generating payloads containing at least one binary control character
/// These should be classified as BINARY
fn payload_with_binary_control_char_strategy() -> impl Strategy<Value = Vec<u8>> {
    (
        prop::collection::vec(0x20u8..=0x7E, 0..100), // Prefix of printable chars
        binary_control_char_strategy(),               // At least one binary control char
        prop::collection::vec(0x20u8..=0x7E, 0..100), // Suffix of printable chars
    )
        .prop_map(|(prefix, control_char, suffix)| {
            let mut result = prefix;
            result.push(control_char);
            result.extend(suffix);
            result
        })
}

/// Strategy for generating invalid UTF-8 byte sequences
/// These should be classified as BINARY
fn invalid_utf8_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Invalid continuation byte without start byte
        Just(vec![0x80u8]),
        // Incomplete 2-byte sequence
        Just(vec![0xC2u8]),
        // Incomplete 3-byte sequence
        Just(vec![0xE0u8, 0xA0u8]),
        // Incomplete 4-byte sequence
        Just(vec![0xF0u8, 0x90u8, 0x80u8]),
        // Overlong encoding (2-byte encoding of ASCII)
        Just(vec![0xC0u8, 0xAFu8]),
        // Invalid start byte
        Just(vec![0xFEu8]),
        Just(vec![0xFFu8]),
        // Surrogate half (invalid in UTF-8)
        Just(vec![0xED, 0xA0, 0x80]),
        // Mix of valid ASCII with invalid UTF-8 in the middle
        (
            prop::collection::vec(0x20u8..=0x7E, 1..10),
            prop_oneof![Just(vec![0x80u8]), Just(vec![0xC2u8]), Just(vec![0xFEu8]),],
            prop::collection::vec(0x20u8..=0x7E, 1..10),
        )
            .prop_map(|(prefix, invalid, suffix)| {
                let mut result = prefix;
                result.extend(invalid);
                result.extend(suffix);
                result
            }),
    ]
}

/// Strategy for generating valid UTF-8 strings with multi-byte characters (emoji, CJK, etc.)
/// These should be classified as TEXT (valid UTF-8 with no control characters)
fn valid_utf8_multibyte_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Emoji (4-byte UTF-8)
        Just("Hello 🌍 World".as_bytes().to_vec()),
        Just("🎉🎊🎈".as_bytes().to_vec()),
        // CJK characters (3-byte UTF-8)
        Just("你好世界".as_bytes().to_vec()),
        Just("日本語テスト".as_bytes().to_vec()),
        // Mixed ASCII and multi-byte
        Just("Hello 世界 🌍".as_bytes().to_vec()),
        // Accented characters (2-byte UTF-8)
        Just("Héllo Wörld".as_bytes().to_vec()),
        Just("Ñoño".as_bytes().to_vec()),
        // Greek letters
        Just("αβγδ".as_bytes().to_vec()),
        // Cyrillic
        Just("Привет".as_bytes().to_vec()),
    ]
}

/// Strategy for generating valid MQTT topics
/// Topics can contain alphanumeric characters, slashes, underscores, and hyphens
fn valid_topic_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple single-level topics
        "[a-zA-Z][a-zA-Z0-9_-]{0,20}".prop_map(|s: String| s),
        // Multi-level topics with slashes
        "[a-zA-Z][a-zA-Z0-9_-]{0,10}(/[a-zA-Z][a-zA-Z0-9_-]{0,10}){1,3}".prop_map(|s: String| s),
        // Common MQTT topic patterns
        Just("sensors/temperature".to_string()),
        Just("home/living-room/light".to_string()),
        Just("device/status".to_string()),
    ]
}

/// Strategy for generating timestamps with millisecond precision
/// Returns a timestamp that can be compared after round-trip
fn timestamp_strategy() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps within a reasonable range (year 2020-2030)
    // We use fixed milliseconds to ensure precision is preserved
    (0i64..315360000000i64).prop_map(|millis| {
        // Base timestamp: 2020-01-01 00:00:00 UTC
        Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap() + Duration::milliseconds(millis)
    })
}

// =============================================================================
// Property 2: Encoding Strategy Correctness
// =============================================================================
//
// Feature: csv-validation, Property 2: Encoding Strategy Correctness
// *For any* payload and encode_b64 setting:
// - When encode_b64 is false and payload is binary: output SHALL start with "b64:" followed by valid base64
// - When encode_b64 is false and payload is text: output SHALL equal the original payload with no prefix
// - When encode_b64 is true: output SHALL be valid base64 of the payload with no prefix
//
// **Validates: Requirements 2.1, 2.2, 2.3**

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Duration, TimeZone, Utc};
use mqtt_recorder::csv_handler::{CsvWriter, AUTO_ENCODE_MARKER};
use std::path::Path;
use tempfile::tempdir;

/// Helper function to read the payload field from a CSV file (first data row, third column)
/// Returns the raw payload string as stored in the CSV
/// This properly handles CSV quoting for fields containing newlines, commas, etc.
fn read_csv_payload_from_file(path: &Path) -> Option<String> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .ok()?;

    // Get the first data record
    if let Some(Ok(record)) = reader.records().next() {
        // Payload is the third field (index 2)
        return record.get(2).map(|s| s.to_string());
    }
    None
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: When encode_b64 is false and payload is binary, output SHALL start with "b64:" followed by valid base64
    // **Validates: Requirements 2.1**
    #[test]
    fn property_2_binary_payload_auto_encoded_with_prefix(
        payload in payload_with_binary_control_char_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read the payload from the CSV file
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to parse CSV payload");

        // Verify: output SHALL start with "b64:" prefix
        prop_assert!(
            stored_payload.starts_with(AUTO_ENCODE_MARKER),
            "Binary payload should be prefixed with '{}'. Got: {}",
            AUTO_ENCODE_MARKER,
            stored_payload
        );

        // Verify: content after prefix SHALL be valid base64
        let base64_part = &stored_payload[AUTO_ENCODE_MARKER.len()..];
        let decode_result = BASE64_STANDARD.decode(base64_part);
        prop_assert!(
            decode_result.is_ok(),
            "Content after prefix should be valid base64. Got: {}",
            base64_part
        );

        // Verify: decoded content SHALL equal original payload
        let decoded = decode_result.unwrap();
        prop_assert_eq!(
            decoded,
            payload,
            "Decoded payload should match original"
        );
    }

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: When encode_b64 is false and payload is text, output SHALL equal the original payload with no prefix
    // **Validates: Requirements 2.2**
    #[test]
    fn property_2_text_payload_stored_as_is(
        payload in valid_text_payload_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read the payload from the CSV file
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to parse CSV payload");

        // Convert original payload to string for comparison
        let original_str = String::from_utf8(payload.clone()).expect("Payload should be valid UTF-8");

        // Verify: output SHALL NOT start with "b64:" prefix
        prop_assert!(
            !stored_payload.starts_with(AUTO_ENCODE_MARKER),
            "Text payload should NOT be prefixed with '{}'. Got: {}",
            AUTO_ENCODE_MARKER,
            stored_payload
        );

        // Verify: output SHALL equal the original payload
        prop_assert_eq!(
            stored_payload,
            original_str,
            "Stored payload should match original text payload"
        );
    }

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: When encode_b64 is true, output SHALL be valid base64 of the payload with no prefix
    // **Validates: Requirements 2.3**
    #[test]
    fn property_2_global_b64_encodes_without_prefix(
        payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = true (global base64 encoding)
        {
            let mut writer = CsvWriter::new(&file_path, true).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read the payload from the CSV file
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to parse CSV payload");

        // Verify: output SHALL NOT start with "b64:" prefix (no auto-encode marker)
        prop_assert!(
            !stored_payload.starts_with(AUTO_ENCODE_MARKER),
            "Global base64 mode should NOT use '{}' prefix. Got: {}",
            AUTO_ENCODE_MARKER,
            stored_payload
        );

        // Verify: output SHALL be valid base64
        let decode_result = BASE64_STANDARD.decode(&stored_payload);
        prop_assert!(
            decode_result.is_ok(),
            "Stored payload should be valid base64. Got: {}",
            stored_payload
        );

        // Verify: decoded content SHALL equal original payload
        let decoded = decode_result.unwrap();
        prop_assert_eq!(
            decoded,
            payload,
            "Decoded payload should match original"
        );
    }

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: Invalid UTF-8 payloads should be auto-encoded with prefix when encode_b64 is false
    // **Validates: Requirements 2.1**
    #[test]
    fn property_2_invalid_utf8_auto_encoded_with_prefix(
        payload in invalid_utf8_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read the payload from the CSV file
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to parse CSV payload");

        // Verify: output SHALL start with "b64:" prefix (binary detected)
        prop_assert!(
            stored_payload.starts_with(AUTO_ENCODE_MARKER),
            "Invalid UTF-8 payload should be prefixed with '{}'. Got: {}",
            AUTO_ENCODE_MARKER,
            stored_payload
        );

        // Verify: content after prefix SHALL be valid base64
        let base64_part = &stored_payload[AUTO_ENCODE_MARKER.len()..];
        let decode_result = BASE64_STANDARD.decode(base64_part);
        prop_assert!(
            decode_result.is_ok(),
            "Content after prefix should be valid base64. Got: {}",
            base64_part
        );

        // Verify: decoded content SHALL equal original payload
        let decoded = decode_result.unwrap();
        prop_assert_eq!(
            decoded,
            payload,
            "Decoded payload should match original"
        );
    }

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: WriteStats correctly tracks text vs auto-encoded payloads
    // **Validates: Requirements 2.1, 2.2**
    #[test]
    fn property_2_write_stats_track_encoding_types(
        text_payloads in prop::collection::vec(valid_text_payload_strategy(), 1..5),
        binary_payloads in prop::collection::vec(payload_with_binary_control_char_strategy(), 1..5),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let expected_text_count = text_payloads.len() as u64;
        let expected_binary_count = binary_payloads.len() as u64;

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");

            // Write text payloads
            for payload in &text_payloads {
                writer.write_bytes(
                    Utc::now(),
                    "test/topic",
                    payload,
                    0,
                    false,
                ).expect("Failed to write text record");
            }

            // Write binary payloads
            for payload in &binary_payloads {
                writer.write_bytes(
                    Utc::now(),
                    "test/topic",
                    payload,
                    0,
                    false,
                ).expect("Failed to write binary record");
            }

            writer.flush().expect("Failed to flush");

            // Verify statistics
            let stats = writer.stats();
            prop_assert_eq!(
                stats.total_records,
                expected_text_count + expected_binary_count,
                "Total records should match"
            );
            prop_assert_eq!(
                stats.text_payloads,
                expected_text_count,
                "Text payload count should match"
            );
            prop_assert_eq!(
                stats.auto_encoded_payloads,
                expected_binary_count,
                "Auto-encoded payload count should match"
            );
        }
    }
}

// Additional edge case tests for Property 2

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: Empty payload is treated as text (not auto-encoded)
    // **Validates: Requirements 2.2**
    #[test]
    fn property_2_empty_payload_stored_as_text(
        _dummy in Just(())
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let payload: Vec<u8> = vec![];

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");

            // Verify stats show it as text payload
            let stats = writer.stats();
            prop_assert_eq!(stats.text_payloads, 1, "Empty payload should be counted as text");
            prop_assert_eq!(stats.auto_encoded_payloads, 0, "Empty payload should not be auto-encoded");
        }

        // Read the payload from the CSV file
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to parse CSV payload");

        // Verify: empty payload should be stored as empty string without prefix
        prop_assert!(
            !stored_payload.starts_with(AUTO_ENCODE_MARKER),
            "Empty payload should NOT be prefixed. Got: {}",
            stored_payload
        );
        prop_assert_eq!(
            stored_payload,
            "",
            "Empty payload should be stored as empty string"
        );
    }

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: UTF-8 multi-byte characters (emoji, CJK) are stored as text without encoding
    // **Validates: Requirements 2.2**
    #[test]
    fn property_2_utf8_multibyte_stored_as_text(
        payload in valid_utf8_multibyte_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");

            // Verify stats show it as text payload
            let stats = writer.stats();
            prop_assert_eq!(stats.text_payloads, 1, "UTF-8 multi-byte should be counted as text");
            prop_assert_eq!(stats.auto_encoded_payloads, 0, "UTF-8 multi-byte should not be auto-encoded");
        }

        // Read the payload from the CSV file
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to parse CSV payload");

        // Convert original payload to string for comparison
        let original_str = String::from_utf8(payload.clone()).expect("Payload should be valid UTF-8");

        // Verify: output SHALL NOT start with "b64:" prefix
        prop_assert!(
            !stored_payload.starts_with(AUTO_ENCODE_MARKER),
            "UTF-8 multi-byte payload should NOT be prefixed. Got: {}",
            stored_payload
        );

        // Verify: output SHALL equal the original payload
        prop_assert_eq!(
            stored_payload,
            original_str,
            "Stored payload should match original UTF-8 multi-byte payload"
        );
    }

    // Feature: csv-validation, Property 2: Encoding Strategy Correctness
    // Test: Largest payload size is tracked correctly
    // **Validates: Requirements 6.6**
    #[test]
    fn property_2_largest_payload_tracked(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..100), 2..5)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let expected_largest = payloads.iter().map(|p| p.len()).max().unwrap_or(0);

        // Write with encode_b64 = true to avoid prefix complications
        {
            let mut writer = CsvWriter::new(&file_path, true).expect("Failed to create writer");

            for payload in &payloads {
                writer.write_bytes(
                    Utc::now(),
                    "test/topic",
                    payload,
                    0,
                    false,
                ).expect("Failed to write record");
            }

            writer.flush().expect("Failed to flush");

            // Verify largest payload statistic
            let stats = writer.stats();
            prop_assert_eq!(
                stats.largest_payload,
                expected_largest,
                "Largest payload size should be tracked correctly"
            );
        }
    }
}

// =============================================================================
// Property 3: Decoding Strategy Correctness
// =============================================================================
//
// Feature: csv-validation, Property 3: Decoding Strategy Correctness
// *For any* stored payload string and decode_b64 setting:
// - When decode_b64 is false and payload starts with "b64:": result SHALL be base64 decode of content after prefix
// - When decode_b64 is false and payload does not start with "b64:": result SHALL equal the original payload
// - When decode_b64 is true: result SHALL be base64 decode of entire payload
//
// **Validates: Requirements 3.1, 3.2, 3.3**

use mqtt_recorder::csv_handler::CsvReader;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: When decode_b64 is false and payload starts with "b64:", result SHALL be base64 decode of content after prefix
    // **Validates: Requirements 3.1**
    #[test]
    fn property_3_auto_encoded_payload_decoded_correctly(
        original_payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a CSV file with a "b64:" prefixed payload (simulating auto-encoded binary)
        let base64_content = BASE64_STANDARD.encode(&original_payload);
        let stored_payload = format!("{}{}", AUTO_ENCODE_MARKER, base64_content);

        // Write CSV file manually with the prefixed payload
        {
            let mut writer = csv::Writer::from_path(&file_path).expect("Failed to create CSV writer");
            writer.write_record(["timestamp", "topic", "payload", "qos", "retain"]).expect("Failed to write header");
            writer.write_record([
                "2024-01-15T10:30:00.123Z",
                "test/topic",
                &stored_payload,
                "0",
                "false",
            ]).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: result SHALL be base64 decode of content after prefix
        prop_assert_eq!(
            record.payload,
            original_payload,
            "Decoded payload should match original bytes"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: When decode_b64 is false and payload does not start with "b64:", result SHALL equal the original payload
    // **Validates: Requirements 3.2**
    #[test]
    fn property_3_text_payload_returned_as_is(
        payload in valid_text_payload_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Convert payload to string (we know it's valid UTF-8 from the strategy)
        let payload_str = String::from_utf8(payload.clone()).expect("Payload should be valid UTF-8");

        // Write CSV file with the text payload (no prefix)
        {
            let mut writer = csv::Writer::from_path(&file_path).expect("Failed to create CSV writer");
            writer.write_record(["timestamp", "topic", "payload", "qos", "retain"]).expect("Failed to write header");
            writer.write_record([
                "2024-01-15T10:30:00.123Z",
                "test/topic",
                &payload_str,
                "0",
                "false",
            ]).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: result SHALL equal the original payload (as bytes)
        prop_assert_eq!(
            record.payload,
            payload,
            "Text payload should be returned as-is (as bytes)"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: When decode_b64 is true, result SHALL be base64 decode of entire payload
    // **Validates: Requirements 3.3**
    #[test]
    fn property_3_global_b64_decodes_entire_payload(
        original_payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a CSV file with base64-encoded payload (no prefix, as written by encode_b64=true)
        let base64_payload = BASE64_STANDARD.encode(&original_payload);

        // Write CSV file with the base64 payload
        {
            let mut writer = csv::Writer::from_path(&file_path).expect("Failed to create CSV writer");
            writer.write_record(["timestamp", "topic", "payload", "qos", "retain"]).expect("Failed to write header");
            writer.write_record([
                "2024-01-15T10:30:00.123Z",
                "test/topic",
                &base64_payload,
                "0",
                "false",
            ]).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = true (global base64 decoding)
        let mut reader = CsvReader::new(&file_path, true, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: result SHALL be base64 decode of entire payload
        prop_assert_eq!(
            record.payload,
            original_payload,
            "Decoded payload should match original bytes when decode_b64=true"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Round-trip through write_bytes and read_next_bytes preserves binary payloads
    // **Validates: Requirements 3.1, 3.2**
    #[test]
    fn property_3_roundtrip_binary_payload_with_auto_encoding(
        payload in payload_with_binary_control_char_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: round-trip preserves the original binary payload
        prop_assert_eq!(
            record.payload,
            payload,
            "Binary payload should be preserved through write/read round-trip"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Round-trip through write_bytes and read_next_bytes preserves text payloads
    // **Validates: Requirements 3.2**
    #[test]
    fn property_3_roundtrip_text_payload_without_encoding(
        payload in valid_text_payload_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: round-trip preserves the original text payload
        prop_assert_eq!(
            record.payload,
            payload,
            "Text payload should be preserved through write/read round-trip"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Round-trip with global base64 encoding/decoding preserves any payload
    // **Validates: Requirements 3.3**
    #[test]
    fn property_3_roundtrip_global_b64_preserves_payload(
        payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = true (global base64 encoding)
        {
            let mut writer = CsvWriter::new(&file_path, true).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = true (global base64 decoding)
        let mut reader = CsvReader::new(&file_path, true, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: round-trip preserves the original payload
        prop_assert_eq!(
            record.payload,
            payload,
            "Payload should be preserved through global base64 write/read round-trip"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Invalid UTF-8 payloads are correctly round-tripped through auto-encoding
    // **Validates: Requirements 3.1**
    #[test]
    fn property_3_roundtrip_invalid_utf8_payload(
        payload in invalid_utf8_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: round-trip preserves the original invalid UTF-8 payload
        prop_assert_eq!(
            record.payload,
            payload,
            "Invalid UTF-8 payload should be preserved through write/read round-trip"
        );
    }
}

// Additional edge case tests for Property 3

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Empty payload is correctly handled in auto-decoding mode
    // **Validates: Requirements 3.2**
    #[test]
    fn property_3_empty_payload_roundtrip(
        _dummy in Just(())
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let payload: Vec<u8> = vec![];

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: empty payload is preserved
        prop_assert_eq!(
            record.payload,
            payload,
            "Empty payload should be preserved through round-trip"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: UTF-8 multi-byte characters are correctly handled in auto-decoding mode
    // **Validates: Requirements 3.2**
    #[test]
    fn property_3_utf8_multibyte_roundtrip(
        payload in valid_utf8_multibyte_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                Utc::now(),
                "test/topic",
                &payload,
                0,
                false,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: UTF-8 multi-byte payload is preserved
        prop_assert_eq!(
            record.payload,
            payload,
            "UTF-8 multi-byte payload should be preserved through round-trip"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Metadata (topic, qos, retain) is preserved through read_next_bytes
    // **Validates: Requirements 3.1, 3.2, 3.3**
    #[test]
    fn property_3_metadata_preserved_through_read(
        payload in prop::collection::vec(any::<u8>(), 0..100),
        topic in "[a-zA-Z0-9/]{1,50}",
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(
                timestamp,
                &topic,
                &payload,
                qos,
                retain,
            ).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: all metadata is preserved
        prop_assert_eq!(record.topic, topic, "Topic should be preserved");
        prop_assert_eq!(record.qos, qos, "QoS should be preserved");
        prop_assert_eq!(record.retain, retain, "Retain flag should be preserved");
        prop_assert_eq!(record.payload, payload, "Payload should be preserved");
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: When decode_b64 is true, payloads with "b64:" prefix are decoded as full base64 (not stripped)
    // This tests that decode_b64=true ignores the auto-encode marker
    // **Validates: Requirements 3.3**
    #[test]
    fn property_3_global_b64_ignores_prefix_marker(
        original_payload in prop::collection::vec(any::<u8>(), 0..100)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a payload that includes "b64:" as part of the content
        let mut payload_with_marker = AUTO_ENCODE_MARKER.as_bytes().to_vec();
        payload_with_marker.extend(&original_payload);

        // Base64 encode the entire payload (including the "b64:" prefix)
        let base64_payload = BASE64_STANDARD.encode(&payload_with_marker);

        // Write CSV file with the base64 payload
        {
            let mut writer = csv::Writer::from_path(&file_path).expect("Failed to create CSV writer");
            writer.write_record(["timestamp", "topic", "payload", "qos", "retain"]).expect("Failed to write header");
            writer.write_record([
                "2024-01-15T10:30:00.123Z",
                "test/topic",
                &base64_payload,
                "0",
                "false",
            ]).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read with decode_b64 = true (global base64 decoding)
        let mut reader = CsvReader::new(&file_path, true, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: result SHALL be base64 decode of entire payload (including the "b64:" prefix)
        prop_assert_eq!(
            record.payload,
            payload_with_marker,
            "When decode_b64=true, entire payload should be decoded including any 'b64:' content"
        );
    }

    // Feature: csv-validation, Property 3: Decoding Strategy Correctness
    // Test: Multiple records are correctly decoded in sequence
    // **Validates: Requirements 3.1, 3.2**
    #[test]
    fn property_3_multiple_records_decoded_correctly(
        text_payloads in prop::collection::vec(valid_text_payload_strategy(), 1..3),
        binary_payloads in prop::collection::vec(payload_with_binary_control_char_strategy(), 1..3),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Combine payloads in alternating order
        let mut all_payloads: Vec<Vec<u8>> = Vec::new();
        let max_len = text_payloads.len().max(binary_payloads.len());
        for i in 0..max_len {
            if i < text_payloads.len() {
                all_payloads.push(text_payloads[i].clone());
            }
            if i < binary_payloads.len() {
                all_payloads.push(binary_payloads[i].clone());
            }
        }

        // Write all payloads with encode_b64 = false (auto-encoding mode)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            for payload in &all_payloads {
                writer.write_bytes(
                    Utc::now(),
                    "test/topic",
                    payload,
                    0,
                    false,
                ).expect("Failed to write record");
            }
            writer.flush().expect("Failed to flush");
        }

        // Read all payloads with decode_b64 = false (auto-decoding mode)
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let mut read_payloads: Vec<Vec<u8>> = Vec::new();
        while let Some(result) = reader.read_next_bytes() {
            let record = result.expect("Failed to read record");
            read_payloads.push(record.payload);
        }

        // Verify: all payloads are correctly decoded
        prop_assert_eq!(
            read_payloads.len(),
            all_payloads.len(),
            "Should read same number of records as written"
        );

        for (i, (read, original)) in read_payloads.iter().zip(all_payloads.iter()).enumerate() {
            prop_assert_eq!(
                read,
                original,
                "Payload {} should match original",
                i
            );
        }
    }
}

// =============================================================================
// Property 6: Marker Collision Handling
// =============================================================================
//
// Feature: csv-validation, Property 6: Marker Collision Handling
// *For any* text payload that literally starts with the string "b64:", the round-trip
// through CSV write and read SHALL preserve the payload exactly. The system SHALL NOT
// misinterpret literal "b64:" prefixes in text payloads as auto-encode markers.
//
// **Validates: Requirements 5.4**

/// Strategy for generating text payloads that start with the "b64:" marker
/// These are valid UTF-8 text payloads that happen to start with the auto-encode marker
fn text_payload_with_marker_prefix_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Just the marker itself
        Just(AUTO_ENCODE_MARKER.as_bytes().to_vec()),
        // Marker followed by random text
        "[a-zA-Z0-9 ]{0,100}".prop_map(|suffix| {
            let mut payload = AUTO_ENCODE_MARKER.as_bytes().to_vec();
            payload.extend(suffix.as_bytes());
            payload
        }),
        // Marker followed by what looks like base64 but is actually text
        Just("b64:SGVsbG8gV29ybGQ=".as_bytes().to_vec()),
        Just("b64:not-actually-base64".as_bytes().to_vec()),
        Just("b64:this is just text".as_bytes().to_vec()),
        // Marker followed by JSON-like content
        Just(r#"b64:{"key": "value"}"#.as_bytes().to_vec()),
        // Multiple markers
        Just("b64:b64:nested".as_bytes().to_vec()),
        Just("b64:b64:b64:triple".as_bytes().to_vec()),
        // Marker with special characters (but still valid UTF-8 text)
        Just("b64:Hello 🌍 World".as_bytes().to_vec()),
        Just("b64:日本語テスト".as_bytes().to_vec()),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Text payloads starting with "b64:" are preserved exactly through round-trip
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_marker_collision_payload_preserved(
        payload in text_payload_with_marker_prefix_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Payload is preserved exactly (including the "b64:" prefix)
        prop_assert_eq!(
            &record.payload,
            &payload,
            "Text payload starting with 'b64:' should be preserved exactly. Original: {:?}, Read: {:?}",
            String::from_utf8_lossy(&payload),
            String::from_utf8_lossy(&record.payload)
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: System does not misinterpret literal "b64:" as auto-encode marker
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_literal_marker_not_misinterpreted(
        suffix in "[a-zA-Z0-9 ]{0,50}"
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a text payload that starts with "b64:" followed by arbitrary text
        let payload_str = format!("{}{}", AUTO_ENCODE_MARKER, suffix);
        let payload = payload_str.as_bytes().to_vec();

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: The payload is NOT misinterpreted as base64
        // If it were misinterpreted, the payload would be different (decoded base64)
        prop_assert_eq!(
            &record.payload,
            &payload,
            "Literal 'b64:' prefix should not be misinterpreted as auto-encode marker"
        );

        // Verify: The payload still starts with "b64:" after round-trip
        let read_str = String::from_utf8(record.payload).expect("Should be valid UTF-8");
        prop_assert!(
            read_str.starts_with(AUTO_ENCODE_MARKER),
            "Payload should still start with 'b64:' after round-trip. Got: {}",
            read_str
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Payloads with marker in middle or end are not affected (no collision)
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_marker_not_at_start_no_collision(
        prefix in "[a-zA-Z0-9]{1,20}",
        suffix in "[a-zA-Z0-9]{0,20}"
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a text payload with "b64:" in the middle (not at start)
        let payload_str = format!("{}{}{}", prefix, AUTO_ENCODE_MARKER, suffix);
        let payload = payload_str.as_bytes().to_vec();

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read the raw CSV to verify it was stored as plain text (not encoded)
        let stored_payload = read_csv_payload_from_file(&file_path).expect("Failed to read CSV");

        // Verify: Payload was stored as-is (not encoded) since marker is not at start
        prop_assert_eq!(
            stored_payload,
            payload_str,
            "Payload with 'b64:' not at start should be stored as plain text"
        );

        // Read back through CsvReader
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Payload is preserved exactly
        prop_assert_eq!(
            record.payload,
            payload,
            "Payload with 'b64:' not at start should be preserved exactly"
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Nested markers (b64:b64:...) are handled correctly
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_nested_markers_preserved(
        depth in 1usize..5,
        suffix in "[a-zA-Z0-9]{0,20}"
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a payload with multiple nested "b64:" prefixes
        let mut payload_str = String::new();
        for _ in 0..depth {
            payload_str.push_str(AUTO_ENCODE_MARKER);
        }
        payload_str.push_str(&suffix);
        let payload = payload_str.as_bytes().to_vec();

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Nested markers are preserved exactly
        prop_assert_eq!(
            &record.payload,
            &payload,
            "Nested 'b64:' markers should be preserved exactly. Depth: {}, Original: {}, Read: {}",
            depth,
            payload_str,
            String::from_utf8_lossy(&record.payload)
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Marker collision with valid base64-looking content is handled correctly
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_marker_with_base64_like_content_preserved(
        // Generate strings that look like base64 (alphanumeric + /+=)
        base64_like in "[A-Za-z0-9+/]{4,20}={0,2}"
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a text payload that looks like "b64:<base64>" but is actually just text
        let payload_str = format!("{}{}", AUTO_ENCODE_MARKER, base64_like);
        let payload = payload_str.as_bytes().to_vec();

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: The payload is preserved exactly, not decoded as base64
        prop_assert_eq!(
            &record.payload,
            &payload,
            "Text payload 'b64:<base64-like>' should be preserved exactly, not decoded. Original: {}, Read: {}",
            payload_str,
            String::from_utf8_lossy(&record.payload)
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: All metadata is preserved alongside marker collision payloads
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_metadata_preserved_with_marker_collision(
        payload in text_payload_with_marker_prefix_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
        timestamp in timestamp_strategy(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: All fields are preserved
        prop_assert_eq!(record.payload, payload, "Payload should be preserved");
        prop_assert_eq!(record.topic, topic, "Topic should be preserved");
        prop_assert_eq!(record.qos, qos, "QoS should be preserved");
        prop_assert_eq!(record.retain, retain, "Retain flag should be preserved");

        // Timestamp to millisecond precision
        let original_millis = timestamp.timestamp_millis();
        let read_millis = record.timestamp.timestamp_millis();
        prop_assert_eq!(
            read_millis,
            original_millis,
            "Timestamp should be preserved to millisecond precision"
        );
    }
}

// Additional edge case tests for Property 6

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Just the marker "b64:" alone is preserved
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_marker_only_payload_preserved(
        _dummy in Just(())
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Payload is exactly "b64:" with nothing after
        let payload = AUTO_ENCODE_MARKER.as_bytes().to_vec();
        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: "b64:" alone is preserved
        prop_assert_eq!(
            &record.payload,
            &payload,
            "Payload 'b64:' alone should be preserved exactly"
        );

        let read_str = String::from_utf8(record.payload.clone()).expect("Should be valid UTF-8");
        prop_assert_eq!(
            read_str,
            AUTO_ENCODE_MARKER,
            "Payload should be exactly 'b64:'"
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Marker collision with UTF-8 multi-byte characters is handled correctly
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_marker_with_multibyte_utf8_preserved(
        multibyte in valid_utf8_multibyte_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create a payload starting with "b64:" followed by multi-byte UTF-8
        let mut payload = AUTO_ENCODE_MARKER.as_bytes().to_vec();
        payload.extend(&multibyte);

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Payload with marker + multi-byte UTF-8 is preserved
        prop_assert_eq!(
            record.payload,
            payload,
            "Payload 'b64:' + multi-byte UTF-8 should be preserved exactly"
        );
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Multiple records with marker collisions are all preserved
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_multiple_marker_collision_records_preserved(
        payloads in prop::collection::vec(text_payload_with_marker_prefix_strategy(), 2..5)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write all payloads with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            for payload in &payloads {
                writer.write_bytes(
                    Utc::now(),
                    "test/topic",
                    payload,
                    0,
                    false,
                ).expect("Failed to write record");
            }
            writer.flush().expect("Failed to flush");
        }

        // Read back all records
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let mut read_payloads: Vec<Vec<u8>> = Vec::new();
        while let Some(result) = reader.read_next_bytes() {
            let record = result.expect("Failed to read record");
            read_payloads.push(record.payload);
        }

        // Verify: Same number of records
        prop_assert_eq!(
            read_payloads.len(),
            payloads.len(),
            "Should read same number of records as written"
        );

        // Verify: Each payload is preserved exactly
        for (i, (read, original)) in read_payloads.iter().zip(payloads.iter()).enumerate() {
            prop_assert_eq!(
                read,
                original,
                "Record {} payload should be preserved. Original: {:?}, Read: {:?}",
                i,
                String::from_utf8_lossy(original),
                String::from_utf8_lossy(read)
            );
        }
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Mixed records (with and without marker collision) are all preserved
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_mixed_marker_and_normal_records_preserved(
        marker_payloads in prop::collection::vec(text_payload_with_marker_prefix_strategy(), 1..3),
        normal_payloads in prop::collection::vec(valid_text_payload_strategy(), 1..3),
        binary_payloads in prop::collection::vec(payload_with_binary_control_char_strategy(), 1..3),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Interleave all payload types
        let mut all_payloads: Vec<Vec<u8>> = Vec::new();
        let max_len = marker_payloads.len().max(normal_payloads.len()).max(binary_payloads.len());
        for i in 0..max_len {
            if i < marker_payloads.len() {
                all_payloads.push(marker_payloads[i].clone());
            }
            if i < normal_payloads.len() {
                all_payloads.push(normal_payloads[i].clone());
            }
            if i < binary_payloads.len() {
                all_payloads.push(binary_payloads[i].clone());
            }
        }

        // Write all payloads with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            for payload in &all_payloads {
                writer.write_bytes(
                    Utc::now(),
                    "test/topic",
                    payload,
                    0,
                    false,
                ).expect("Failed to write record");
            }
            writer.flush().expect("Failed to flush");
        }

        // Read back all records
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let mut read_payloads: Vec<Vec<u8>> = Vec::new();
        while let Some(result) = reader.read_next_bytes() {
            let record = result.expect("Failed to read record");
            read_payloads.push(record.payload);
        }

        // Verify: Same number of records
        prop_assert_eq!(
            read_payloads.len(),
            all_payloads.len(),
            "Should read same number of records as written"
        );

        // Verify: Each payload is preserved exactly
        for (i, (read, original)) in read_payloads.iter().zip(all_payloads.iter()).enumerate() {
            prop_assert_eq!(
                read,
                original,
                "Record {} payload should be preserved",
                i
            );
        }
    }

    // Feature: csv-validation, Property 6: Marker Collision Handling
    // Test: Marker collision payloads work correctly with global base64 mode
    // **Validates: Requirements 5.4**
    #[test]
    fn property_6_marker_collision_with_global_b64_mode(
        payload in text_payload_with_marker_prefix_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with global base64 encoding (encode_b64 = true)
        {
            let mut writer = CsvWriter::new(&file_path, true).expect("Failed to create writer");
            writer.write_bytes(timestamp, "test/topic", &payload, 0, false).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back with global base64 decoding (decode_b64 = true)
        let mut reader = CsvReader::new(&file_path, true, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Payload is preserved exactly even with global base64 mode
        prop_assert_eq!(
            record.payload,
            payload,
            "Marker collision payload should be preserved with global base64 mode"
        );
    }
}
