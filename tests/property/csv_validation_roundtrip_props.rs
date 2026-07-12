//! Property-based tests for CSV round-trip integrity
//!
//! This module contains property tests that validate that writing a MessageRecord
//! to CSV (with auto-encoding when needed) and reading it back SHALL produce a
//! record that is identical to the original.
//!
//! **Feature: csv-validation**

use proptest::prelude::*;

use chrono::Utc;
use mqtt_recorder::csv_handler::{CsvReader, CsvWriter};
use tempfile::tempdir;

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
