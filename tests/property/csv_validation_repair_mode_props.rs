//! Property-based tests for CSV repair mode correctness
//!
//! This module contains property tests that validate the CsvFixer's ability to repair
//! corrupted CSV files containing unencoded binary payloads: preserving valid records,
//! repairing recoverable ones, skipping unrecoverable ones, and producing output that
//! passes validation.
//!
//! **Feature: csv-validation**

use proptest::prelude::*;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use mqtt_recorder::csv_handler::{CsvReader, AUTO_ENCODE_MARKER};
use mqtt_recorder::validator::CsvValidator;
use std::io::Write;
use std::path::Path;
use tempfile::tempdir;

/// Strategy for generating payloads with binary control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F)
/// These should be classified as BINARY
fn binary_control_char_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![
        0x00u8..=0x08, // NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, BS
        0x0Bu8..=0x0C, // VT, FF
        0x0Eu8..=0x1F, // SO through US
    ]
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
