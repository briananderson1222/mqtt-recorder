/// The marker prefix used for auto-encoded binary payloads.
///
/// When `encode_b64` is false and a payload is detected as binary (containing
/// non-UTF8 bytes or control characters), the payload is automatically base64
/// encoded and prefixed with this marker. This allows the reader to distinguish
/// between intentionally base64-encoded content and automatically-encoded binary data.
///
/// # Example
///
/// A binary payload `[0x08, 0x0A, 0x12, 0x18]` would be stored as:
/// `b64:CAoSGA==`
///
/// # Requirements
///
/// - **2.1**: WHEN encode_b64 is false and a payload is classified as binary,
///   THE CSV_Handler SHALL base64 encode the payload and prefix it with "b64:"
/// - **3.1**: WHEN decode_b64 is false and a payload starts with "b64:",
///   THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
pub const AUTO_ENCODE_MARKER: &str = "b64:";

/// Determines if a payload contains binary data that requires encoding.
///
/// Binary payloads are those containing:
/// - Non-UTF8 byte sequences
/// - Control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F)
///
/// Tab (0x09), newline (0x0A), and carriage return (0x0D) are allowed
/// as CSV quoting handles these correctly.
///
/// # Arguments
///
/// * `payload` - The raw bytes of the MQTT message payload
///
/// # Returns
///
/// Returns `true` if the payload contains binary data that requires encoding,
/// `false` if the payload is safe to store as plain text in CSV.
///
/// # Examples
///
/// ```
/// use mqtt_recorder::csv_handler::is_binary_payload;
///
/// // Plain ASCII text is not binary
/// assert!(!is_binary_payload(b"Hello, World!"));
///
/// // UTF-8 text with emoji is not binary
/// assert!(!is_binary_payload("Hello 🌍".as_bytes()));
///
/// // Text with tabs and newlines is not binary (CSV handles these)
/// assert!(!is_binary_payload(b"line1\nline2\ttab"));
///
/// // Payload with NUL byte is binary
/// assert!(is_binary_payload(b"Hello\x00World"));
///
/// // Payload with control character (backspace) is binary
/// assert!(is_binary_payload(b"Hello\x08World"));
///
/// // Invalid UTF-8 sequence is binary
/// assert!(is_binary_payload(&[0xFF, 0xFE, 0x00, 0x01]));
/// ```
///
/// # Requirements
///
/// - **1.1**: WHEN encode_b64 is false and a payload contains non-UTF8 bytes,
///   THE Payload_Validator SHALL classify the payload as binary
/// - **1.2**: WHEN encode_b64 is false and a payload contains control characters
///   (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F), THE Payload_Validator SHALL classify
///   the payload as binary
/// - **1.3**: WHEN a payload contains only valid UTF8 with no control characters,
///   THE Payload_Validator SHALL classify the payload as text
/// - **1.5**: THE Payload_Validator SHALL allow tab (0x09), newline (0x0A),
///   and carriage return (0x0D) in text payloads since CSV quoting handles these
#[must_use]
pub fn is_binary_payload(payload: &[u8]) -> bool {
    // First, check if the payload is valid UTF-8
    // If it's not valid UTF-8, it's definitely binary (Requirement 1.1)
    if std::str::from_utf8(payload).is_err() {
        return true;
    }

    // Check for control characters that would corrupt CSV
    // Control characters are 0x00-0x1F, but we allow:
    // - 0x09 (TAB) - CSV quoting handles this
    // - 0x0A (LF/newline) - CSV quoting handles this
    // - 0x0D (CR/carriage return) - CSV quoting handles this
    //
    // Binary control characters (Requirement 1.2):
    // - 0x00-0x08: NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, BS
    // - 0x0B-0x0C: VT, FF
    // - 0x0E-0x1F: SO through US
    for &byte in payload {
        if is_binary_control_char(byte) {
            return true;
        }
    }

    // Payload is valid UTF-8 with no problematic control characters (Requirement 1.3)
    false
}

/// Checks if a byte is a control character that indicates binary content.
///
/// Returns `true` for control characters that would corrupt CSV:
/// - 0x00-0x08: NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL, BS
/// - 0x0B-0x0C: VT, FF
/// - 0x0E-0x1F: SO through US
///
/// Returns `false` for allowed whitespace characters:
/// - 0x09: TAB
/// - 0x0A: LF (newline)
/// - 0x0D: CR (carriage return)
#[inline]
pub(crate) fn is_binary_control_char(byte: u8) -> bool {
    matches!(byte, 0x00..=0x08 | 0x0B..=0x0C | 0x0E..=0x1F)
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_auto_encode_marker_value() {
        assert_eq!(super::AUTO_ENCODE_MARKER, "b64:");
    }

    #[test]
    fn test_is_binary_payload_empty() {
        // Empty payload is not binary (Requirement 1.4)
        assert!(!super::is_binary_payload(&[]));
    }

    #[test]
    fn test_is_binary_payload_plain_ascii() {
        // Plain ASCII text is not binary
        assert!(!super::is_binary_payload(b"Hello, World!"));
        assert!(!super::is_binary_payload(b"Simple text"));
        assert!(!super::is_binary_payload(b"123456789"));
        assert!(!super::is_binary_payload(b"!@#$%^&*()"));
    }

    #[test]
    fn test_is_binary_payload_utf8_text() {
        // Valid UTF-8 text with non-ASCII characters is not binary
        assert!(!super::is_binary_payload("Hello 🌍".as_bytes()));
        assert!(!super::is_binary_payload("Привет мир".as_bytes()));
        assert!(!super::is_binary_payload("日本語テスト".as_bytes()));
        assert!(!super::is_binary_payload("Ñoño".as_bytes()));
    }

    #[test]
    fn test_is_binary_payload_allowed_whitespace() {
        // Tab (0x09), newline (0x0A), and carriage return (0x0D) are allowed
        // (Requirement 1.5)
        assert!(!super::is_binary_payload(b"line1\nline2"));
        assert!(!super::is_binary_payload(b"col1\tcol2"));
        assert!(!super::is_binary_payload(b"line1\r\nline2"));
        assert!(!super::is_binary_payload(b"mixed\ttab\nand\rnewlines"));
    }

    #[test]
    fn test_is_binary_payload_nul_byte() {
        // NUL byte (0x00) indicates binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"Hello\x00World"));
        assert!(super::is_binary_payload(&[0x00]));
        assert!(super::is_binary_payload(b"\x00"));
    }

    #[test]
    fn test_is_binary_payload_control_chars_0x01_to_0x08() {
        // Control characters 0x01-0x08 indicate binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"test\x01data")); // SOH
        assert!(super::is_binary_payload(b"test\x02data")); // STX
        assert!(super::is_binary_payload(b"test\x03data")); // ETX
        assert!(super::is_binary_payload(b"test\x04data")); // EOT
        assert!(super::is_binary_payload(b"test\x05data")); // ENQ
        assert!(super::is_binary_payload(b"test\x06data")); // ACK
        assert!(super::is_binary_payload(b"test\x07data")); // BEL
        assert!(super::is_binary_payload(b"test\x08data")); // BS (backspace)
    }

    #[test]
    fn test_is_binary_payload_control_chars_0x0b_to_0x0c() {
        // Control characters 0x0B-0x0C indicate binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"test\x0Bdata")); // VT (vertical tab)
        assert!(super::is_binary_payload(b"test\x0Cdata")); // FF (form feed)
    }

    #[test]
    fn test_is_binary_payload_control_chars_0x0e_to_0x1f() {
        // Control characters 0x0E-0x1F indicate binary (Requirement 1.2)
        assert!(super::is_binary_payload(b"test\x0Edata")); // SO
        assert!(super::is_binary_payload(b"test\x0Fdata")); // SI
        assert!(super::is_binary_payload(b"test\x10data")); // DLE
        assert!(super::is_binary_payload(b"test\x1Bdata")); // ESC
        assert!(super::is_binary_payload(b"test\x1Fdata")); // US
    }

    #[test]
    fn test_is_binary_payload_invalid_utf8() {
        // Invalid UTF-8 sequences indicate binary (Requirement 1.1)
        assert!(super::is_binary_payload(&[0xFF, 0xFE]));
        assert!(super::is_binary_payload(&[0x80, 0x81, 0x82]));
        assert!(super::is_binary_payload(&[0xC0, 0xC1])); // Overlong encoding
        assert!(super::is_binary_payload(&[0xF5, 0xF6, 0xF7])); // Invalid start bytes
    }

    #[test]
    fn test_is_binary_payload_protobuf_like() {
        // Protobuf-like binary data should be detected as binary
        let protobuf_data = vec![
            0x08, 0x96, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6E, 0x67,
        ];
        assert!(super::is_binary_payload(&protobuf_data));
    }

    #[test]
    fn test_is_binary_payload_json() {
        // JSON payloads are not binary
        assert!(!super::is_binary_payload(br#"{"key": "value"}"#));
        assert!(!super::is_binary_payload(
            br#"{"temperature": 23.5, "unit": "celsius"}"#
        ));
        assert!(!super::is_binary_payload(br#"[1, 2, 3, 4, 5]"#));
    }

    #[test]
    fn test_is_binary_payload_xml() {
        // XML payloads are not binary
        assert!(!super::is_binary_payload(
            b"<root><child>value</child></root>"
        ));
        assert!(!super::is_binary_payload(b"<?xml version=\"1.0\"?><data/>"));
    }

    #[test]
    fn test_is_binary_payload_printable_range() {
        // All printable ASCII characters (0x20-0x7E) are not binary
        let printable: Vec<u8> = (0x20..=0x7E).collect();
        assert!(!super::is_binary_payload(&printable));
    }

    #[test]
    fn test_is_binary_payload_mixed_valid_and_control() {
        // If any control character is present, the payload is binary
        assert!(super::is_binary_payload(b"Valid text\x00with NUL"));
        assert!(super::is_binary_payload(b"Start\x08middle\x00end"));
    }

    #[test]
    fn test_is_binary_control_char() {
        // Test the helper function directly
        // Binary control characters
        assert!(super::is_binary_control_char(0x00)); // NUL
        assert!(super::is_binary_control_char(0x01)); // SOH
        assert!(super::is_binary_control_char(0x08)); // BS
        assert!(super::is_binary_control_char(0x0B)); // VT
        assert!(super::is_binary_control_char(0x0C)); // FF
        assert!(super::is_binary_control_char(0x0E)); // SO
        assert!(super::is_binary_control_char(0x1F)); // US

        // Allowed whitespace characters
        assert!(!super::is_binary_control_char(0x09)); // TAB
        assert!(!super::is_binary_control_char(0x0A)); // LF
        assert!(!super::is_binary_control_char(0x0D)); // CR

        // Regular printable characters
        assert!(!super::is_binary_control_char(0x20)); // Space
        assert!(!super::is_binary_control_char(0x41)); // 'A'
        assert!(!super::is_binary_control_char(0x7E)); // '~'
    }
}
