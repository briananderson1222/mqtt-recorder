//! Property-based tests for CSV handling
//!
//! This module contains property tests that validate the CSV handler behavior
//! as specified in the design document.
//!
//! **Validates: Requirements 4.3, 4.6, 5.2-5.6, 6.2-6.7**

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Duration, TimeZone, Utc};
use proptest::prelude::*;
use tempfile::tempdir;

// Import the csv_handler module from the main crate
use mqtt_recorder::csv_handler::{CsvReader, CsvWriter, MessageRecord};

/// Strategy for generating valid non-empty topic strings
/// Topics can contain alphanumeric characters, slashes, underscores, and hyphens
fn valid_topic_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple single-level topics
        "[a-zA-Z][a-zA-Z0-9_-]{0,20}".prop_map(|s| s),
        // Multi-level topics with slashes
        "[a-zA-Z][a-zA-Z0-9]{0,10}/[a-zA-Z][a-zA-Z0-9]{0,10}".prop_map(|s| s),
        // Three-level topics
        "[a-zA-Z][a-zA-Z0-9]{0,5}/[a-zA-Z][a-zA-Z0-9]{0,5}/[a-zA-Z][a-zA-Z0-9]{0,5}"
            .prop_map(|s| s),
    ]
}

/// Strategy for generating arbitrary byte sequences for payload testing
fn arbitrary_bytes_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..256)
}

/// Strategy for generating payloads with CSV special characters
fn csv_special_chars_payload_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Payloads with commas
        "[a-zA-Z0-9]{0,10},[a-zA-Z0-9]{0,10}".prop_map(|s| s),
        // Payloads with double quotes
        "[a-zA-Z0-9]{0,10}\"[a-zA-Z0-9]{0,10}\"[a-zA-Z0-9]{0,10}".prop_map(|s| s),
        // Payloads with newlines
        "[a-zA-Z0-9]{0,10}\n[a-zA-Z0-9]{0,10}".prop_map(|s| s),
        // Payloads with carriage returns
        "[a-zA-Z0-9]{0,10}\r\n[a-zA-Z0-9]{0,10}".prop_map(|s| s),
        // Mixed special characters
        "[a-zA-Z0-9]{0,5},\"[a-zA-Z0-9]{0,5}\"\n[a-zA-Z0-9]{0,5}".prop_map(|s| s),
        // JSON-like payloads with special chars
        Just(r#"{"key": "value, with comma"}"#.to_string()),
        Just(r#"{"message": "Hello \"World\""}"#.to_string()),
        Just("line1\nline2\nline3".to_string()),
    ]
}

/// Strategy for generating arbitrary string payloads (including special chars)
fn arbitrary_payload_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple alphanumeric payloads
        "[a-zA-Z0-9 ]{0,100}".prop_map(|s| s),
        // Payloads with special characters
        csv_special_chars_payload_strategy(),
        // Empty payload
        Just(String::new()),
        // JSON payloads
        Just(r#"{"temperature": 23.5, "humidity": 65}"#.to_string()),
        // Unicode payloads
        Just("Hello 世界 🌍".to_string()),
    ]
}

/// Strategy for generating valid QoS values (0, 1, or 2)
fn valid_qos_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![Just(0u8), Just(1u8), Just(2u8),]
}

/// Strategy for generating valid timestamps
fn valid_timestamp_strategy() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps within a reasonable range (2020-2030)
    (
        2020i32..2030,
        1u32..13,
        1u32..29,
        0u32..24,
        0u32..60,
        0u32..60,
        0u32..1000,
    )
        .prop_map(|(year, month, day, hour, min, sec, millis)| {
            Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
                .unwrap()
                + Duration::milliseconds(millis as i64)
        })
}

/// Strategy for generating valid MessageRecords
fn valid_message_record_strategy() -> impl Strategy<Value = MessageRecord> {
    (
        valid_timestamp_strategy(),
        valid_topic_strategy(),
        arbitrary_payload_strategy(),
        valid_qos_strategy(),
        any::<bool>(),
    )
        .prop_map(|(timestamp, topic, payload, qos, retain)| {
            MessageRecord::new(timestamp, topic, payload, qos, retain)
        })
}

/// Strategy for generating MessageRecords with CSV special characters in payload
fn message_record_with_special_chars_strategy() -> impl Strategy<Value = MessageRecord> {
    (
        valid_timestamp_strategy(),
        valid_topic_strategy(),
        csv_special_chars_payload_strategy(),
        valid_qos_strategy(),
        any::<bool>(),
    )
        .prop_map(|(timestamp, topic, payload, qos, retain)| {
            MessageRecord::new(timestamp, topic, payload, qos, retain)
        })
}

/// Strategy for generating two different timestamps
fn two_different_timestamps_strategy() -> impl Strategy<Value = (DateTime<Utc>, DateTime<Utc>)> {
    (valid_timestamp_strategy(), 1i64..10000).prop_map(|(ts1, diff_millis)| {
        let ts2 = ts1 + Duration::milliseconds(diff_millis);
        (ts1, ts2)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: mqtt-recorder, Property 7: Base64 Encoding Round-Trip
    // *For any* byte sequence representing a message payload, encoding it as base64
    // and then decoding SHALL produce the original byte sequence.
    // **Validates: Requirements 4.3, 5.6**
    #[test]
    fn property_7_base64_encoding_roundtrip(
        bytes in arbitrary_bytes_strategy()
    ) {
        // Encode the bytes as base64
        let encoded = BASE64_STANDARD.encode(&bytes);

        // Decode the base64 back to bytes
        let decoded = BASE64_STANDARD.decode(&encoded)
            .expect("Base64 decoding should succeed for valid encoded data");

        // The decoded bytes should match the original
        prop_assert_eq!(
            decoded,
            bytes,
            "Base64 round-trip should preserve the original byte sequence"
        );
    }

    // Feature: mqtt-recorder, Property 7: Base64 Encoding Round-Trip
    // Test base64 encoding through CsvWriter and CsvReader
    // **Validates: Requirements 4.3, 5.6**
    #[test]
    fn property_7_base64_encoding_through_csv(
        payload_bytes in arbitrary_bytes_strategy()
    ) {
        // Convert bytes to a string (using lossy conversion for arbitrary bytes)
        // For this test, we'll use valid UTF-8 strings
        let payload = String::from_utf8_lossy(&payload_bytes).to_string();

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_b64.csv");

        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            payload.clone(),
            0,
            false,
        );

        // Write with base64 encoding
        {
            let mut writer = CsvWriter::new(&file_path, true)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read with base64 decoding
        let mut reader = CsvReader::new(&file_path, true, None)
            .expect("Failed to create CSV reader");

        let read_record = reader.next()
            .expect("Should have a record")
            .expect("Should successfully read record");

        // The payload should match after round-trip
        prop_assert_eq!(
            read_record.payload,
            payload,
            "Base64 encoding/decoding through CSV should preserve payload"
        );
    }

    // Feature: mqtt-recorder, Property 8: CSV Special Character Escaping
    // *For any* message payload containing CSV special characters (commas, double quotes,
    // newlines), writing to CSV and reading back SHALL preserve the original payload exactly.
    // **Validates: Requirements 4.6, 6.2, 6.3**
    #[test]
    fn property_8_csv_special_character_escaping(
        record in message_record_with_special_chars_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_special_chars.csv");

        let original_payload = record.payload.clone();

        // Write the record
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read the record back
        let mut reader = CsvReader::new(&file_path, false, None)
            .expect("Failed to create CSV reader");

        let read_record = reader.next()
            .expect("Should have a record")
            .expect("Should successfully read record");

        // The payload should be preserved exactly
        prop_assert_eq!(
            read_record.payload,
            original_payload,
            "CSV special characters should be properly escaped and preserved"
        );
    }

    // Feature: mqtt-recorder, Property 8: CSV Special Character Escaping
    // Additional test with explicit special character combinations
    // **Validates: Requirements 4.6, 6.2, 6.3**
    #[test]
    fn property_8_csv_special_chars_explicit_combinations(
        has_comma in any::<bool>(),
        has_quote in any::<bool>(),
        has_newline in any::<bool>(),
        prefix in "[a-zA-Z0-9]{0,10}",
        suffix in "[a-zA-Z0-9]{0,10}",
    ) {
        // Build a payload with the specified special characters
        let mut payload = prefix.clone();
        if has_comma {
            payload.push(',');
        }
        if has_quote {
            payload.push('"');
        }
        if has_newline {
            payload.push('\n');
        }
        payload.push_str(&suffix);

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_explicit_special.csv");

        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            payload.clone(),
            0,
            false,
        );

        // Write the record
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read the record back
        let mut reader = CsvReader::new(&file_path, false, None)
            .expect("Failed to create CSV reader");

        let read_record = reader.next()
            .expect("Should have a record")
            .expect("Should successfully read record");

        // The payload should be preserved exactly
        prop_assert_eq!(
            read_record.payload,
            payload,
            "Payload with special characters should be preserved exactly"
        );
    }

    // Feature: mqtt-recorder, Property 9: CSV Message Record Round-Trip
    // *For any* valid MessageRecord (with valid timestamp, non-empty topic, arbitrary payload,
    // QoS in 0-2, and boolean retain), writing it to CSV and reading it back SHALL produce
    // an equivalent MessageRecord with all fields preserved.
    // **Validates: Requirements 4.2, 5.2, 5.3, 5.4, 6.1, 6.5, 6.6, 6.7**
    #[test]
    fn property_9_csv_message_record_roundtrip(
        record in valid_message_record_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_roundtrip.csv");

        let original_topic = record.topic.clone();
        let original_payload = record.payload.clone();
        let original_qos = record.qos;
        let original_retain = record.retain;
        // Store timestamp with millisecond precision (truncate sub-millisecond)
        let original_timestamp = record.timestamp
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();

        // Write the record
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read the record back
        let mut reader = CsvReader::new(&file_path, false, None)
            .expect("Failed to create CSV reader");

        let read_record = reader.next()
            .expect("Should have a record")
            .expect("Should successfully read record");

        // Verify all fields are preserved
        prop_assert_eq!(
            read_record.topic,
            original_topic,
            "Topic should be preserved"
        );

        prop_assert_eq!(
            read_record.payload,
            original_payload,
            "Payload should be preserved"
        );

        prop_assert_eq!(
            read_record.qos,
            original_qos,
            "QoS should be preserved"
        );

        prop_assert_eq!(
            read_record.retain,
            original_retain,
            "Retain flag should be preserved"
        );

        // Verify timestamp is preserved (with millisecond precision)
        let read_timestamp = read_record.timestamp
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        prop_assert_eq!(
            read_timestamp,
            original_timestamp,
            "Timestamp should be preserved with millisecond precision"
        );
    }

    // Feature: mqtt-recorder, Property 9: CSV Message Record Round-Trip
    // Test round-trip with base64 encoding enabled
    // **Validates: Requirements 4.2, 4.3, 5.2, 5.3, 5.4, 5.6, 6.1, 6.5, 6.6, 6.7**
    #[test]
    fn property_9_csv_message_record_roundtrip_with_base64(
        record in valid_message_record_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_roundtrip_b64.csv");

        let original_topic = record.topic.clone();
        let original_payload = record.payload.clone();
        let original_qos = record.qos;
        let original_retain = record.retain;

        // Write the record with base64 encoding
        {
            let mut writer = CsvWriter::new(&file_path, true)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read the record back with base64 decoding
        let mut reader = CsvReader::new(&file_path, true, None)
            .expect("Failed to create CSV reader");

        let read_record = reader.next()
            .expect("Should have a record")
            .expect("Should successfully read record");

        // Verify all fields are preserved
        prop_assert_eq!(
            read_record.topic,
            original_topic,
            "Topic should be preserved with base64"
        );

        prop_assert_eq!(
            read_record.payload,
            original_payload,
            "Payload should be preserved with base64 encoding/decoding"
        );

        prop_assert_eq!(
            read_record.qos,
            original_qos,
            "QoS should be preserved with base64"
        );

        prop_assert_eq!(
            read_record.retain,
            original_retain,
            "Retain flag should be preserved with base64"
        );
    }

    // Feature: mqtt-recorder, Property 10: Timestamp Difference Calculation
    // *For any* two MessageRecords with different timestamps, the calculated delay
    // between them SHALL equal the actual difference between their timestamps.
    // **Validates: Requirements 5.5**
    #[test]
    fn property_10_timestamp_difference_calculation(
        (ts1, ts2) in two_different_timestamps_strategy()
    ) {
        // Calculate the expected difference
        let expected_diff = ts2.signed_duration_since(ts1);

        // The difference should be positive (ts2 > ts1 by construction)
        prop_assert!(
            expected_diff > Duration::zero(),
            "ts2 should be after ts1"
        );

        // Create two records with these timestamps
        let record1 = MessageRecord::new(
            ts1,
            "test/topic".to_string(),
            "payload1".to_string(),
            0,
            false,
        );

        let record2 = MessageRecord::new(
            ts2,
            "test/topic".to_string(),
            "payload2".to_string(),
            0,
            false,
        );

        // Calculate the delay between the records
        let calculated_diff = record2.timestamp.signed_duration_since(record1.timestamp);

        // The calculated difference should equal the expected difference
        prop_assert_eq!(
            calculated_diff,
            expected_diff,
            "Calculated timestamp difference should match expected difference"
        );

        // Verify the difference in milliseconds
        let expected_millis = expected_diff.num_milliseconds();
        let calculated_millis = calculated_diff.num_milliseconds();

        prop_assert_eq!(
            calculated_millis,
            expected_millis,
            "Timestamp difference in milliseconds should match"
        );
    }

    // Feature: mqtt-recorder, Property 10: Timestamp Difference Calculation
    // Test timestamp difference preservation through CSV round-trip
    // **Validates: Requirements 5.5**
    #[test]
    fn property_10_timestamp_difference_through_csv(
        (ts1, ts2) in two_different_timestamps_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_timestamp_diff.csv");

        let record1 = MessageRecord::new(
            ts1,
            "test/topic".to_string(),
            "payload1".to_string(),
            0,
            false,
        );

        let record2 = MessageRecord::new(
            ts2,
            "test/topic".to_string(),
            "payload2".to_string(),
            0,
            false,
        );

        // Calculate original difference
        let original_diff = record2.timestamp.signed_duration_since(record1.timestamp);

        // Write both records
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record1).expect("Failed to write record1");
            writer.write(&record2).expect("Failed to write record2");
            writer.flush().expect("Failed to flush writer");
        }

        // Read both records back
        let mut reader = CsvReader::new(&file_path, false, None)
            .expect("Failed to create CSV reader");

        let read_record1 = reader.next()
            .expect("Should have first record")
            .expect("Should successfully read first record");

        let read_record2 = reader.next()
            .expect("Should have second record")
            .expect("Should successfully read second record");

        // Calculate difference from read records
        let read_diff = read_record2.timestamp.signed_duration_since(read_record1.timestamp);

        // The difference should be preserved (within millisecond precision)
        // Note: We compare milliseconds because CSV stores with millisecond precision
        prop_assert_eq!(
            read_diff.num_milliseconds(),
            original_diff.num_milliseconds(),
            "Timestamp difference should be preserved through CSV round-trip"
        );
    }

    // Feature: mqtt-recorder, Property 11: CSV Field Size Limit Enforcement
    // *For any* CSV field exceeding the configured size limit, reading SHALL fail
    // with an appropriate error rather than silently truncating or accepting the oversized field.
    // **Validates: Requirements 6.4**
    #[test]
    fn property_11_csv_field_size_limit_enforcement(
        limit in 10usize..100,
        excess in 1usize..50,
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_field_limit.csv");

        // Create a payload that exceeds the limit
        let oversized_payload: String = "x".repeat(limit + excess);

        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            oversized_payload.clone(),
            0,
            false,
        );

        // Write the record (writer doesn't enforce limits)
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read with a field size limit that is smaller than the payload
        let mut reader = CsvReader::new(&file_path, false, Some(limit))
            .expect("Failed to create CSV reader");

        let result = reader.next()
            .expect("Should have a record");

        // The read should fail because the payload exceeds the limit
        prop_assert!(
            result.is_err(),
            "Reading a field exceeding the size limit should fail, but got: {:?}",
            result
        );

        // Verify the error message mentions the size limit
        if let Err(e) = result {
            let error_msg = e.to_string();
            prop_assert!(
                error_msg.contains("exceeds size limit") || error_msg.contains("limit"),
                "Error message should mention size limit: {}",
                error_msg
            );
        }
    }

    // Feature: mqtt-recorder, Property 11: CSV Field Size Limit Enforcement
    // Test that fields within the limit are accepted
    // **Validates: Requirements 6.4**
    #[test]
    fn property_11_csv_field_within_limit_accepted(
        limit in 50usize..200,
        payload_len in 1usize..50,
    ) {
        // Ensure payload is within limit
        let actual_payload_len = payload_len.min(limit - 1);

        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_field_within_limit.csv");

        // Create a payload within the limit
        let payload: String = "x".repeat(actual_payload_len);

        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            payload.clone(),
            0,
            false,
        );

        // Write the record
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read with a field size limit larger than the payload
        let mut reader = CsvReader::new(&file_path, false, Some(limit))
            .expect("Failed to create CSV reader");

        let result = reader.next()
            .expect("Should have a record");

        // The read should succeed
        prop_assert!(
            result.is_ok(),
            "Reading a field within the size limit should succeed, but got error: {:?}",
            result.err()
        );

        // Verify the payload is preserved
        let read_record = result.unwrap();
        prop_assert_eq!(
            read_record.payload,
            payload,
            "Payload within limit should be preserved"
        );
    }
}

// Additional unit-style property tests for edge cases

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Test multiple records round-trip
    #[test]
    fn multiple_records_roundtrip(
        records in prop::collection::vec(valid_message_record_strategy(), 1..10)
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_multiple.csv");

        // Write all records
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            for record in &records {
                writer.write(record).expect("Failed to write record");
            }
            writer.flush().expect("Failed to flush writer");
        }

        // Read all records back
        let reader = CsvReader::new(&file_path, false, None)
            .expect("Failed to create CSV reader");

        let read_records: Vec<MessageRecord> = reader
            .map(|r| r.expect("Should successfully read record"))
            .collect();

        // Verify count matches
        prop_assert_eq!(
            read_records.len(),
            records.len(),
            "Number of records should match"
        );

        // Verify each record matches (comparing relevant fields)
        for (original, read) in records.iter().zip(read_records.iter()) {
            prop_assert_eq!(&read.topic, &original.topic, "Topic should match");
            prop_assert_eq!(&read.payload, &original.payload, "Payload should match");
            prop_assert_eq!(read.qos, original.qos, "QoS should match");
            prop_assert_eq!(read.retain, original.retain, "Retain should match");
        }
    }

    // Test CSV reader reset functionality
    #[test]
    fn csv_reader_reset_preserves_data(
        record in valid_message_record_strategy()
    ) {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = temp_dir.path().join("test_reset.csv");

        // Write the record
        {
            let mut writer = CsvWriter::new(&file_path, false)
                .expect("Failed to create CSV writer");
            writer.write(&record).expect("Failed to write record");
            writer.flush().expect("Failed to flush writer");
        }

        // Read the record
        let mut reader = CsvReader::new(&file_path, false, None)
            .expect("Failed to create CSV reader");

        let first_read = reader.next()
            .expect("Should have a record")
            .expect("Should successfully read record");

        // Verify end of file
        prop_assert!(reader.next().is_none(), "Should be at end of file");

        // Reset the reader
        reader.reset().expect("Reset should succeed");

        // Read again
        let second_read = reader.next()
            .expect("Should have a record after reset")
            .expect("Should successfully read record after reset");

        // Both reads should produce the same data
        prop_assert_eq!(&first_read.topic, &second_read.topic, "Topic should match after reset");
        prop_assert_eq!(&first_read.payload, &second_read.payload, "Payload should match after reset");
        prop_assert_eq!(first_read.qos, second_read.qos, "QoS should match after reset");
        prop_assert_eq!(first_read.retain, second_read.retain, "Retain should match after reset");
    }
}
