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
