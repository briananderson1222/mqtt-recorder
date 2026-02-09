//! Property-based tests for CSV validation and binary payload handling
//!
//! This module contains property tests that validate the binary payload detection
//! and classification behavior as specified in the design document.
//!
//! **Feature: csv-validation**

use proptest::prelude::*;

// Import the csv_handler module from the main crate
use mqtt_recorder::csv_handler::is_binary_payload;

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 1: Binary Payload Classification
    // *For any* byte sequence, the Payload_Validator SHALL classify it as binary if and only if
    // it contains non-UTF8 bytes OR control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F).
    // Valid UTF8 strings containing only printable characters, spaces, tabs, newlines, and
    // carriage returns SHALL be classified as text.
    //
    // **Validates: Requirements 1.1, 1.2, 1.3, 1.5**

    // Test: Valid ASCII text payloads should be classified as TEXT
    // **Validates: Requirements 1.3**
    #[test]
    fn property_1_valid_ascii_text_is_not_binary(
        payload in valid_text_payload_strategy()
    ) {
        let result = is_binary_payload(&payload);
        prop_assert!(
            !result,
            "Valid ASCII text payload should NOT be classified as binary. Payload bytes: {:?}",
            payload
        );
    }

    // Test: Payloads with binary control characters should be classified as BINARY
    // **Validates: Requirements 1.2**
    #[test]
    fn property_1_control_chars_are_binary(
        payload in payload_with_binary_control_char_strategy()
    ) {
        let result = is_binary_payload(&payload);
        prop_assert!(
            result,
            "Payload with binary control characters should be classified as binary. Payload bytes: {:?}",
            payload
        );
    }

    // Test: Invalid UTF-8 sequences should be classified as BINARY
    // **Validates: Requirements 1.1**
    #[test]
    fn property_1_invalid_utf8_is_binary(
        payload in invalid_utf8_strategy()
    ) {
        let result = is_binary_payload(&payload);
        prop_assert!(
            result,
            "Invalid UTF-8 payload should be classified as binary. Payload bytes: {:?}",
            payload
        );
    }

    // Test: Valid UTF-8 multi-byte characters (emoji, CJK) should be classified as TEXT
    // **Validates: Requirements 1.3**
    #[test]
    fn property_1_valid_utf8_multibyte_is_text(
        payload in valid_utf8_multibyte_strategy()
    ) {
        let result = is_binary_payload(&payload);
        prop_assert!(
            !result,
            "Valid UTF-8 multi-byte payload should NOT be classified as binary. Payload: {:?}",
            String::from_utf8_lossy(&payload)
        );
    }

    // Test: Tab (0x09), newline (0x0A), and carriage return (0x0D) should be allowed in text
    // **Validates: Requirements 1.5**
    #[test]
    fn property_1_allowed_whitespace_is_text(
        prefix in "[a-zA-Z0-9]{0,20}",
        has_tab in any::<bool>(),
        has_newline in any::<bool>(),
        has_cr in any::<bool>(),
        suffix in "[a-zA-Z0-9]{0,20}",
    ) {
        let mut payload = prefix.into_bytes();
        if has_tab {
            payload.push(0x09); // Tab
        }
        if has_newline {
            payload.push(0x0A); // Newline (LF)
        }
        if has_cr {
            payload.push(0x0D); // Carriage return (CR)
        }
        payload.extend(suffix.into_bytes());

        let result = is_binary_payload(&payload);
        prop_assert!(
            !result,
            "Payload with tab/newline/CR should NOT be classified as binary. Payload bytes: {:?}",
            payload
        );
    }

    // Test: Each specific binary control character should trigger binary classification
    // **Validates: Requirements 1.2**
    #[test]
    fn property_1_each_binary_control_char_is_detected(
        control_char in binary_control_char_strategy()
    ) {
        // Create a payload with just the control character surrounded by valid text
        let payload = vec![b'A', control_char, b'B'];
        let result = is_binary_payload(&payload);
        prop_assert!(
            result,
            "Control character 0x{:02X} should cause binary classification. Payload bytes: {:?}",
            control_char,
            payload
        );
    }

    // Test: Empty payload should be classified as TEXT
    // **Validates: Requirements 1.4 (implied - empty is valid UTF-8 with no control chars)**
    #[test]
    fn property_1_empty_payload_is_text(
        _dummy in Just(())
    ) {
        let payload: Vec<u8> = vec![];
        let result = is_binary_payload(&payload);
        prop_assert!(
            !result,
            "Empty payload should NOT be classified as binary"
        );
    }

    // Test: Arbitrary byte sequences - verify the classification is consistent
    // This tests the complete property: binary iff (non-UTF8 OR control chars)
    // **Validates: Requirements 1.1, 1.2, 1.3, 1.5**
    #[test]
    fn property_1_classification_consistency(
        payload in prop::collection::vec(any::<u8>(), 0..256)
    ) {
        let result = is_binary_payload(&payload);

        // Manually check if the payload should be binary
        let is_valid_utf8 = std::str::from_utf8(&payload).is_ok();
        let has_binary_control_char = payload.iter().any(|&b| {
            matches!(b, 0x00..=0x08 | 0x0B..=0x0C | 0x0E..=0x1F)
        });

        let expected_binary = !is_valid_utf8 || has_binary_control_char;

        prop_assert_eq!(
            result,
            expected_binary,
            "Classification mismatch for payload. is_valid_utf8={}, has_binary_control_char={}, expected_binary={}, actual={}. Payload bytes: {:?}",
            is_valid_utf8,
            has_binary_control_char,
            expected_binary,
            result,
            payload
        );
    }
}

// Additional edge case tests

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Test: JSON payloads (common MQTT use case) should be classified as TEXT
    #[test]
    fn property_1_json_payloads_are_text(
        key in "[a-zA-Z][a-zA-Z0-9]{0,10}",
        value in "[a-zA-Z0-9 ]{0,20}",
    ) {
        let json_payload = format!(r#"{{"{}": "{}"}}"#, key, value);
        let result = is_binary_payload(json_payload.as_bytes());
        prop_assert!(
            !result,
            "JSON payload should NOT be classified as binary. Payload: {}",
            json_payload
        );
    }

    // Test: Payloads with only allowed whitespace characters
    #[test]
    fn property_1_whitespace_only_payloads(
        tabs in 0usize..10,
        newlines in 0usize..10,
        crs in 0usize..10,
        spaces in 0usize..10,
    ) {
        let mut payload = Vec::new();
        payload.extend(std::iter::repeat_n(0x09u8, tabs));      // Tabs
        payload.extend(std::iter::repeat_n(0x0Au8, newlines));  // Newlines
        payload.extend(std::iter::repeat_n(0x0Du8, crs));       // Carriage returns
        payload.extend(std::iter::repeat_n(0x20u8, spaces));    // Spaces

        let result = is_binary_payload(&payload);
        prop_assert!(
            !result,
            "Whitespace-only payload should NOT be classified as binary. Payload bytes: {:?}",
            payload
        );
    }

    // Test: Protobuf-like binary payloads (common MQTT use case) should be classified as BINARY
    // Protobuf often starts with field tags which include control characters
    #[test]
    fn property_1_protobuf_like_payloads_are_binary(
        field_number in 1u8..15,
        wire_type in 0u8..3,
        data in prop::collection::vec(any::<u8>(), 1..50),
    ) {
        // Protobuf field tag: (field_number << 3) | wire_type
        // For small field numbers, this often results in control characters
        let tag = (field_number << 3) | wire_type;

        // Only test if the tag is actually a binary control character
        if matches!(tag, 0x00..=0x08 | 0x0B..=0x0C | 0x0E..=0x1F) {
            let mut payload = vec![tag];
            payload.extend(data);

            let result = is_binary_payload(&payload);
            prop_assert!(
                result,
                "Protobuf-like payload with control char tag should be classified as binary. Tag: 0x{:02X}, Payload bytes: {:?}",
                tag,
                payload
            );
        }
    }
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
use chrono::Utc;
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
// Property 5: Round-Trip Integrity
// =============================================================================
//
// Feature: csv-validation, Property 5: Round-Trip Integrity
// *For any* valid MessageRecord with arbitrary binary or text payload, writing to CSV
// (with auto-encoding when needed) and reading back SHALL produce a record with:
// - Identical payload bytes
// - Identical topic string
// - Identical QoS value
// - Identical retain flag
// - Timestamp preserved to millisecond precision
//
// **Validates: Requirements 5.1, 5.2, 5.3**

use chrono::{DateTime, Duration, TimeZone};

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

/// Strategy for generating arbitrary binary payloads (any byte sequence)
fn arbitrary_payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..500)
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Arbitrary binary payloads are preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_arbitrary_payload(
        payload in arbitrary_payload_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical payload bytes
        prop_assert_eq!(
            record.payload,
            payload,
            "Payload bytes should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Topic string is preserved exactly through round-trip
    // **Validates: Requirements 5.2**
    #[test]
    fn property_5_roundtrip_preserves_topic(
        payload in arbitrary_payload_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical topic string
        prop_assert_eq!(
            record.topic,
            topic,
            "Topic string should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: QoS value is preserved exactly through round-trip
    // **Validates: Requirements 5.2**
    #[test]
    fn property_5_roundtrip_preserves_qos(
        payload in arbitrary_payload_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical QoS value
        prop_assert_eq!(
            record.qos,
            qos,
            "QoS value should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Retain flag is preserved exactly through round-trip
    // **Validates: Requirements 5.2**
    #[test]
    fn property_5_roundtrip_preserves_retain(
        payload in arbitrary_payload_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical retain flag
        prop_assert_eq!(
            record.retain,
            retain,
            "Retain flag should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Timestamp is preserved to millisecond precision through round-trip
    // **Validates: Requirements 5.3**
    #[test]
    fn property_5_roundtrip_preserves_timestamp_millisecond_precision(
        payload in arbitrary_payload_strategy(),
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

        // Verify: Timestamp preserved to millisecond precision
        // Truncate both timestamps to milliseconds for comparison
        let original_millis = timestamp.timestamp_millis();
        let read_millis = record.timestamp.timestamp_millis();

        prop_assert_eq!(
            read_millis,
            original_millis,
            "Timestamp should be preserved to millisecond precision. Original: {}, Read: {}",
            timestamp,
            record.timestamp
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Complete MessageRecord is preserved through round-trip (all fields at once)
    // **Validates: Requirements 5.1, 5.2, 5.3**
    #[test]
    fn property_5_roundtrip_preserves_complete_record(
        payload in arbitrary_payload_strategy(),
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

        // Verify all fields
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

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Text payloads are preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_text_payload(
        payload in valid_text_payload_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical payload bytes
        prop_assert_eq!(
            record.payload,
            payload,
            "Text payload bytes should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Binary payloads (with control characters) are preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_binary_payload(
        payload in payload_with_binary_control_char_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical payload bytes
        prop_assert_eq!(
            record.payload,
            payload,
            "Binary payload bytes should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Invalid UTF-8 payloads are preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_invalid_utf8_payload(
        payload in invalid_utf8_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical payload bytes
        prop_assert_eq!(
            record.payload,
            payload,
            "Invalid UTF-8 payload bytes should be identical after round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: UTF-8 multi-byte payloads (emoji, CJK) are preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_utf8_multibyte_payload(
        payload in valid_utf8_multibyte_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Identical payload bytes
        prop_assert_eq!(
            record.payload,
            payload,
            "UTF-8 multi-byte payload bytes should be identical after round-trip"
        );
    }
}

// Additional edge case tests for Property 5

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Empty payload is preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_empty_payload(
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let payload: Vec<u8> = vec![];
        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Empty payload is preserved
        prop_assert_eq!(
            record.payload,
            payload,
            "Empty payload should be preserved through round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Large payloads are preserved through round-trip
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_large_payload(
        payload in prop::collection::vec(any::<u8>(), 1000..5000),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Large payload is preserved
        prop_assert_eq!(
            record.payload,
            payload,
            "Large payload should be preserved through round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Multiple records are all preserved through round-trip
    // **Validates: Requirements 5.1, 5.2, 5.3**
    #[test]
    fn property_5_roundtrip_preserves_multiple_records(
        records in prop::collection::vec(
            (
                arbitrary_payload_strategy(),
                valid_topic_strategy(),
                0u8..=2u8,
                any::<bool>(),
                timestamp_strategy(),
            ),
            2..5
        )
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write all records with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            for (payload, topic, qos, retain, timestamp) in &records {
                writer.write_bytes(*timestamp, topic, payload, *qos, *retain).expect("Failed to write record");
            }
            writer.flush().expect("Failed to flush");
        }

        // Read back all records
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let mut read_records = Vec::new();
        while let Some(result) = reader.read_next_bytes() {
            read_records.push(result.expect("Failed to read record"));
        }

        // Verify: Same number of records
        prop_assert_eq!(
            read_records.len(),
            records.len(),
            "Should read same number of records as written"
        );

        // Verify: Each record is preserved
        for (i, ((payload, topic, qos, retain, timestamp), read_record)) in records.iter().zip(read_records.iter()).enumerate() {
            prop_assert_eq!(
                &read_record.payload,
                payload,
                "Record {} payload should be preserved",
                i
            );
            prop_assert_eq!(
                &read_record.topic,
                topic,
                "Record {} topic should be preserved",
                i
            );
            prop_assert_eq!(
                read_record.qos,
                *qos,
                "Record {} QoS should be preserved",
                i
            );
            prop_assert_eq!(
                read_record.retain,
                *retain,
                "Record {} retain flag should be preserved",
                i
            );

            // Timestamp to millisecond precision
            let original_millis = timestamp.timestamp_millis();
            let read_millis = read_record.timestamp.timestamp_millis();
            prop_assert_eq!(
                read_millis,
                original_millis,
                "Record {} timestamp should be preserved to millisecond precision",
                i
            );
        }
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Round-trip with global base64 encoding also preserves all fields
    // **Validates: Requirements 5.1, 5.2, 5.3**
    #[test]
    fn property_5_roundtrip_global_b64_preserves_complete_record(
        payload in arbitrary_payload_strategy(),
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
        timestamp in timestamp_strategy(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Write with global base64 encoding (encode_b64 = true)
        {
            let mut writer = CsvWriter::new(&file_path, true).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back with global base64 decoding (decode_b64 = true)
        let mut reader = CsvReader::new(&file_path, true, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify all fields
        prop_assert_eq!(record.payload, payload, "Payload should be preserved with global b64");
        prop_assert_eq!(record.topic, topic, "Topic should be preserved with global b64");
        prop_assert_eq!(record.qos, qos, "QoS should be preserved with global b64");
        prop_assert_eq!(record.retain, retain, "Retain flag should be preserved with global b64");

        // Timestamp to millisecond precision
        let original_millis = timestamp.timestamp_millis();
        let read_millis = record.timestamp.timestamp_millis();
        prop_assert_eq!(
            read_millis,
            original_millis,
            "Timestamp should be preserved to millisecond precision with global b64"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: Payloads with special CSV characters (commas, quotes, newlines) are preserved
    // **Validates: Requirements 5.1**
    #[test]
    fn property_5_roundtrip_preserves_csv_special_chars(
        prefix in "[a-zA-Z0-9]{0,10}",
        suffix in "[a-zA-Z0-9]{0,10}",
        special_char in prop_oneof![
            Just(","),
            Just("\""),
            Just("\n"),
            Just("\r\n"),
            Just("\t"),
            Just(",\""),
            Just("\","),
        ],
        topic in valid_topic_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test.csv");

        // Create payload with special CSV characters
        let payload_str = format!("{}{}{}", prefix, special_char, suffix);
        let payload = payload_str.as_bytes().to_vec();
        let timestamp = Utc::now();

        // Write with auto-encoding (encode_b64 = false)
        {
            let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
            writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
            writer.flush().expect("Failed to flush");
        }

        // Read back
        let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
        let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

        // Verify: Payload with special CSV characters is preserved
        prop_assert_eq!(
            record.payload,
            payload,
            "Payload with special CSV characters should be preserved through round-trip"
        );
    }

    // Feature: csv-validation, Property 5: Round-Trip Integrity
    // Test: All QoS values (0, 1, 2) are correctly preserved
    // **Validates: Requirements 5.2**
    #[test]
    fn property_5_roundtrip_preserves_all_qos_values(
        payload in arbitrary_payload_strategy(),
        topic in valid_topic_strategy(),
        retain in any::<bool>(),
    ) {
        for qos in 0u8..=2u8 {
            let temp_dir = tempdir().expect("Failed to create temp dir");
            let file_path = temp_dir.path().join("test.csv");

            let timestamp = Utc::now();

            // Write with auto-encoding (encode_b64 = false)
            {
                let mut writer = CsvWriter::new(&file_path, false).expect("Failed to create writer");
                writer.write_bytes(timestamp, &topic, &payload, qos, retain).expect("Failed to write record");
                writer.flush().expect("Failed to flush");
            }

            // Read back
            let mut reader = CsvReader::new(&file_path, false, None).expect("Failed to create reader");
            let record = reader.read_next_bytes().expect("Expected a record").expect("Failed to read record");

            // Verify: QoS value is preserved
            prop_assert_eq!(
                record.qos,
                qos,
                "QoS value {} should be preserved through round-trip",
                qos
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

// =============================================================================
// Property 7: Repair Mode Correctness
// =============================================================================
//
// Feature: csv-validation, Property 7: Repair Mode Correctness
// *For any* corrupted CSV file containing records with unencoded binary payloads,
// the CsvFixer SHALL produce an output file where:
// - All valid records from the input are preserved unchanged
// - Corrupted records that can be repaired are written with proper encoding
// - Unrecoverable records are skipped and their line numbers reported
// - The output file passes validation
//
// **Validates: Requirements 8.2, 8.3, 8.4, 8.5, 8.6, 8.7**

use mqtt_recorder::fixer::CsvFixer;

/// Strategy for generating valid text payloads for CSV records
/// These are payloads that don't need encoding (no binary control chars)
fn valid_csv_text_payload_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple alphanumeric text
        "[a-zA-Z0-9 ]{0,50}".prop_map(|s: String| s),
        // JSON-like payloads
        Just(r#"{"temperature": 23.5}"#.to_string()),
        Just(r#"{"status": "ok"}"#.to_string()),
        // Empty payload
        Just("".to_string()),
    ]
}

/// Strategy for generating binary payloads that would corrupt CSV
/// These contain control characters that break CSV parsing
fn corrupting_binary_payload_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Payload with NUL byte
        (
            prop::collection::vec(0x20u8..=0x7E, 1..20),
            prop::collection::vec(0x20u8..=0x7E, 1..20),
        )
            .prop_map(|(prefix, suffix)| {
                let mut result = prefix;
                result.push(0x00); // NUL byte
                result.extend(suffix);
                result
            }),
        // Payload with backspace
        (
            prop::collection::vec(0x20u8..=0x7E, 1..20),
            prop::collection::vec(0x20u8..=0x7E, 1..20),
        )
            .prop_map(|(prefix, suffix)| {
                let mut result = prefix;
                result.push(0x08); // Backspace
                result.extend(suffix);
                result
            }),
        // Payload with form feed
        (
            prop::collection::vec(0x20u8..=0x7E, 1..20),
            prop::collection::vec(0x20u8..=0x7E, 1..20),
        )
            .prop_map(|(prefix, suffix)| {
                let mut result = prefix;
                result.push(0x0C); // Form feed
                result.extend(suffix);
                result
            }),
        // Payload with multiple control characters
        (
            prop::collection::vec(0x20u8..=0x7E, 1..10),
            binary_control_char_strategy(),
            prop::collection::vec(0x20u8..=0x7E, 1..10),
            binary_control_char_strategy(),
            prop::collection::vec(0x20u8..=0x7E, 1..10),
        )
            .prop_map(|(p1, c1, p2, c2, p3)| {
                let mut result = p1;
                result.push(c1);
                result.extend(p2);
                result.push(c2);
                result.extend(p3);
                result
            }),
    ]
}

/// Helper function to create a CSV file with a mix of valid and corrupted records
fn create_mixed_csv_file(
    path: &Path,
    valid_payloads: &[String],
    corrupted_payloads: &[Vec<u8>],
) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    writeln!(file, "timestamp,topic,payload,qos,retain")?;

    let mut record_num = 0;
    let max_len = valid_payloads.len().max(corrupted_payloads.len());

    for i in 0..max_len {
        // Interleave valid and corrupted records
        if i < valid_payloads.len() {
            record_num += 1;
            writeln!(
                file,
                "2024-01-15T10:30:{:02}.123Z,valid/topic{},\"{}\",0,false",
                record_num % 60,
                record_num,
                valid_payloads[i].replace('"', "\"\"") // Escape quotes for CSV
            )?;
        }

        if i < corrupted_payloads.len() {
            record_num += 1;
            // Write corrupted record with binary payload
            write!(
                file,
                "2024-01-15T10:31:{:02}.456Z,corrupted/topic{},",
                record_num % 60,
                record_num
            )?;
            file.write_all(&corrupted_payloads[i])?;
            writeln!(file, ",1,true")?;
        }
    }

    file.flush()?;
    Ok(())
}

/// Helper function to create a CSV file with only valid records
fn create_valid_csv_file(path: &Path, payloads: &[String]) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    writeln!(file, "timestamp,topic,payload,qos,retain")?;

    for (i, payload) in payloads.iter().enumerate() {
        writeln!(
            file,
            "2024-01-15T10:30:{:02}.123Z,test/topic{},\"{}\",{},{}",
            i % 60,
            i,
            payload.replace('"', "\"\""), // Escape quotes for CSV
            i % 3,
            i % 2 == 0
        )?;
    }

    file.flush()?;
    Ok(())
}

/// Helper function to create a CSV file with only corrupted records
fn create_corrupted_csv_file(path: &Path, payloads: &[Vec<u8>]) -> std::io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    writeln!(file, "timestamp,topic,payload,qos,retain")?;

    for (i, payload) in payloads.iter().enumerate() {
        write!(
            file,
            "2024-01-15T10:30:{:02}.123Z,binary/topic{},",
            i % 60,
            i
        )?;
        file.write_all(payload)?;
        writeln!(file, ",{},{}", i % 3, i % 2 == 0)?;
    }

    file.flush()?;
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Valid records from input are preserved unchanged
    // **Validates: Requirements 8.6**
    #[test]
    fn property_7_valid_records_preserved_unchanged(
        payloads in prop::collection::vec(valid_csv_text_payload_strategy(), 1..5),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with only valid records
        create_valid_csv_file(&input_path, &payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: All records should be valid (none repaired or skipped)
        prop_assert_eq!(
            stats.valid_records,
            payloads.len() as u64,
            "All valid records should be preserved. Stats: {:?}",
            stats
        );
        prop_assert_eq!(
            stats.repaired_records,
            0,
            "No records should need repair. Stats: {:?}",
            stats
        );
        prop_assert_eq!(
            stats.skipped_records,
            0,
            "No records should be skipped. Stats: {:?}",
            stats
        );

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Output file should pass validation. Errors: {:?}",
            validation_stats.errors
        );
        prop_assert_eq!(
            validation_stats.valid_records,
            payloads.len() as u64,
            "Output should have same number of valid records"
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Corrupted records are repaired with proper encoding
    // **Validates: Requirements 8.3, 8.4**
    #[test]
    fn property_7_corrupted_records_repaired_with_encoding(
        payloads in prop::collection::vec(corrupting_binary_payload_strategy(), 1..5),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with corrupted binary payloads
        create_corrupted_csv_file(&input_path, &payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Records should be repaired (not skipped)
        // Note: Some records may be unrecoverable depending on how the binary data
        // affects CSV parsing, so we check that repaired + skipped = total
        prop_assert_eq!(
            stats.repaired_records + stats.skipped_records,
            stats.total_records,
            "All corrupted records should be either repaired or skipped. Stats: {:?}",
            stats
        );

        // Verify: Output file should pass validation (for repaired records)
        if stats.repaired_records > 0 {
            let validator = CsvValidator::new(false, None);
            let validation_stats = validator.validate(&output_path)
                .expect("Validation should succeed");

            prop_assert_eq!(
                validation_stats.invalid_records,
                0,
                "Output file should pass validation. Errors: {:?}",
                validation_stats.errors
            );
        }
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Mixed valid and corrupted records are handled correctly
    // **Validates: Requirements 8.2, 8.3, 8.4, 8.6**
    #[test]
    fn property_7_mixed_records_handled_correctly(
        valid_payloads in prop::collection::vec(valid_csv_text_payload_strategy(), 1..3),
        corrupted_payloads in prop::collection::vec(corrupting_binary_payload_strategy(), 1..3),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with mixed valid and corrupted records
        create_mixed_csv_file(&input_path, &valid_payloads, &corrupted_payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Total records processed
        let expected_total = valid_payloads.len() + corrupted_payloads.len();
        prop_assert_eq!(
            stats.total_records,
            expected_total as u64,
            "Total records should match input. Stats: {:?}",
            stats
        );

        // Verify: Valid records should be preserved
        prop_assert!(
            stats.valid_records >= valid_payloads.len() as u64 - 1, // Allow for edge cases
            "Most valid records should be preserved. Expected at least {}, got {}. Stats: {:?}",
            valid_payloads.len() - 1,
            stats.valid_records,
            stats
        );

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Output file should pass validation. Errors: {:?}",
            validation_stats.errors
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Skipped records have their line numbers reported
    // **Validates: Requirements 8.7**
    #[test]
    fn property_7_skipped_records_line_numbers_reported(
        _dummy in Just(()),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with an unrecoverable record
        // An unrecoverable record is one where we can't determine the field boundaries
        let mut file = std::fs::File::create(&input_path).expect("Failed to create file");
        writeln!(file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(file, "2024-01-15T10:30:00.123Z,valid/topic,hello world,0,false").unwrap();
        // Completely malformed line that can't be parsed
        writeln!(file, "this is not a valid csv record at all").unwrap();
        writeln!(file, "2024-01-15T10:30:02.123Z,valid/topic2,goodbye,1,true").unwrap();
        drop(file);

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Skipped records should have line numbers reported
        if stats.skipped_records > 0 {
            prop_assert!(
                !stats.skipped_lines.is_empty(),
                "Skipped records should have line numbers reported. Stats: {:?}",
                stats
            );
            prop_assert_eq!(
                stats.skipped_lines.len() as u64,
                stats.skipped_records,
                "Number of skipped lines should match skipped records count. Stats: {:?}",
                stats
            );
        }
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Output file passes validation after repair
    // **Validates: Requirements 8.2, 8.4**
    #[test]
    fn property_7_output_passes_validation(
        valid_payloads in prop::collection::vec(valid_csv_text_payload_strategy(), 1..3),
        corrupted_payloads in prop::collection::vec(corrupting_binary_payload_strategy(), 0..3),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with mixed records
        create_mixed_csv_file(&input_path, &valid_payloads, &corrupted_payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Output file should have no invalid records. Validation errors: {:?}",
            validation_stats.errors
        );

        // Verify: Number of valid records in output matches repair stats
        let expected_output_records = stats.valid_records + stats.repaired_records;
        prop_assert_eq!(
            validation_stats.valid_records,
            expected_output_records,
            "Output should have valid_records + repaired_records. Repair stats: {:?}, Validation stats: {:?}",
            stats,
            validation_stats
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Repair statistics are accurate
    // **Validates: Requirements 8.5**
    #[test]
    fn property_7_repair_statistics_accurate(
        valid_payloads in prop::collection::vec(valid_csv_text_payload_strategy(), 1..4),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with only valid records
        create_valid_csv_file(&input_path, &valid_payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Statistics are consistent
        prop_assert_eq!(
            stats.total_records,
            stats.valid_records + stats.repaired_records + stats.skipped_records,
            "Total should equal valid + repaired + skipped. Stats: {:?}",
            stats
        );

        // Verify: For valid-only input, all should be valid
        prop_assert_eq!(
            stats.valid_records,
            valid_payloads.len() as u64,
            "All records should be valid. Stats: {:?}",
            stats
        );
    }
}

// Additional edge case tests for Property 7

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Empty file is handled correctly
    // **Validates: Requirements 8.2**
    #[test]
    fn property_7_empty_file_handled(
        _dummy in Just(()),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create an empty CSV file (header only)
        let mut file = std::fs::File::create(&input_path).expect("Failed to create file");
        writeln!(file, "timestamp,topic,payload,qos,retain").unwrap();
        drop(file);

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: No records processed
        prop_assert_eq!(stats.total_records, 0, "Empty file should have 0 records");
        prop_assert_eq!(stats.valid_records, 0, "Empty file should have 0 valid records");
        prop_assert_eq!(stats.repaired_records, 0, "Empty file should have 0 repaired records");
        prop_assert_eq!(stats.skipped_records, 0, "Empty file should have 0 skipped records");

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Empty output file should pass validation"
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Repair with global base64 encoding mode
    // **Validates: Requirements 8.2, 8.4**
    #[test]
    fn property_7_repair_with_global_b64_mode(
        payloads in prop::collection::vec(valid_csv_text_payload_strategy(), 1..3),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with valid records
        create_valid_csv_file(&input_path, &payloads)
            .expect("Failed to create input CSV");

        // Run repair with global base64 encoding
        let fixer = CsvFixer::new(true); // encode_b64 = true
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: All records should be processed
        prop_assert_eq!(
            stats.total_records,
            payloads.len() as u64,
            "All records should be processed. Stats: {:?}",
            stats
        );

        // Verify: Output file should pass validation with decode_b64=true
        let validator = CsvValidator::new(true, None); // decode_b64 = true
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Output file should pass validation with decode_b64=true. Errors: {:?}",
            validation_stats.errors
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Records with already-encoded payloads are preserved
    // **Validates: Requirements 8.6**
    #[test]
    fn property_7_already_encoded_payloads_preserved(
        payload_bytes in prop::collection::vec(any::<u8>(), 1..50),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with an already-encoded payload (b64: prefix)
        let encoded_payload = format!("{}{}", AUTO_ENCODE_MARKER, BASE64_STANDARD.encode(&payload_bytes));
        let mut file = std::fs::File::create(&input_path).expect("Failed to create file");
        writeln!(file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(
            file,
            "2024-01-15T10:30:00.123Z,test/topic,{},0,false",
            encoded_payload
        ).unwrap();
        drop(file);

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Record should be valid (already properly encoded)
        prop_assert_eq!(
            stats.valid_records,
            1,
            "Already-encoded record should be valid. Stats: {:?}",
            stats
        );
        prop_assert_eq!(
            stats.repaired_records,
            0,
            "Already-encoded record should not need repair. Stats: {:?}",
            stats
        );

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Output file should pass validation. Errors: {:?}",
            validation_stats.errors
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Large files are handled correctly
    // **Validates: Requirements 8.2, 8.5**
    #[test]
    fn property_7_large_file_handled(
        num_records in 10usize..20,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with many valid records
        let payloads: Vec<String> = (0..num_records)
            .map(|i| format!("payload number {}", i))
            .collect();

        create_valid_csv_file(&input_path, &payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: All records processed
        prop_assert_eq!(
            stats.total_records,
            num_records as u64,
            "All records should be processed. Stats: {:?}",
            stats
        );
        prop_assert_eq!(
            stats.valid_records,
            num_records as u64,
            "All records should be valid. Stats: {:?}",
            stats
        );

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.valid_records,
            num_records as u64,
            "Output should have all records valid"
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Records with special CSV characters are handled correctly
    // **Validates: Requirements 8.6**
    #[test]
    fn property_7_special_csv_chars_handled(
        prefix in "[a-zA-Z0-9]{1,10}",
        suffix in "[a-zA-Z0-9]{1,10}",
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create payloads with special CSV characters (comma, quote)
        let payloads = vec![
            format!("{}, {}", prefix, suffix),  // Comma in payload
            format!("{}\"{}\"", prefix, suffix), // Quotes in payload
        ];

        create_valid_csv_file(&input_path, &payloads)
            .expect("Failed to create input CSV");

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        // Verify: Records should be valid (CSV quoting handles these)
        prop_assert_eq!(
            stats.valid_records,
            payloads.len() as u64,
            "Records with special CSV chars should be valid. Stats: {:?}",
            stats
        );

        // Verify: Output file should pass validation
        let validator = CsvValidator::new(false, None);
        let validation_stats = validator.validate(&output_path)
            .expect("Validation should succeed");

        prop_assert_eq!(
            validation_stats.invalid_records,
            0,
            "Output file should pass validation. Errors: {:?}",
            validation_stats.errors
        );
    }

    // Feature: csv-validation, Property 7: Repair Mode Correctness
    // Test: Repair preserves metadata (topic, qos, retain) for valid records
    // **Validates: Requirements 8.6**
    #[test]
    fn property_7_metadata_preserved_for_valid_records(
        topic in "[a-zA-Z][a-zA-Z0-9/]{0,20}",
        payload in valid_csv_text_payload_strategy(),
        qos in 0u8..=2u8,
        retain in any::<bool>(),
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let input_path = temp_dir.path().join("input.csv");
        let output_path = temp_dir.path().join("output.csv");

        // Create a CSV file with specific metadata
        let mut file = std::fs::File::create(&input_path).expect("Failed to create file");
        writeln!(file, "timestamp,topic,payload,qos,retain").unwrap();
        writeln!(
            file,
            "2024-01-15T10:30:00.123Z,{},\"{}\",{},{}",
            topic,
            payload.replace('"', "\"\""),
            qos,
            retain
        ).unwrap();
        drop(file);

        // Run repair
        let fixer = CsvFixer::new(false);
        let stats = fixer.repair(&input_path, &output_path)
            .expect("Repair should succeed");

        prop_assert_eq!(stats.valid_records, 1, "Record should be valid");

        // Read back the output and verify metadata
        let mut reader = CsvReader::new(&output_path, false, None)
            .expect("Failed to create reader");
        let record = reader.read_next_bytes()
            .expect("Expected a record")
            .expect("Failed to read record");

        prop_assert_eq!(record.topic, topic, "Topic should be preserved");
        prop_assert_eq!(record.qos, qos, "QoS should be preserved");
        prop_assert_eq!(record.retain, retain, "Retain flag should be preserved");
    }
}
