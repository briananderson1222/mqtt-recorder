//! MQTT message record types for CSV serialization.
//!
//! Defines [`MessageRecord`] (string payloads), [`MessageRecordBytes`] (binary payloads),
//! and [`WriteStats`] for tracking CSV write statistics.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single recorded MQTT message.
///
/// This struct represents an MQTT message with all relevant metadata for
/// recording and replay purposes. It is designed to be serialized to and
/// deserialized from CSV format.
///
/// # Fields
///
/// * `timestamp` - ISO 8601 timestamp with millisecond precision indicating when
///   the message was received
/// * `topic` - The MQTT topic the message was published to
/// * `payload` - The message payload, stored as a string (raw or base64 encoded)
/// * `qos` - Quality of Service level (0, 1, or 2)
/// * `retain` - Whether the message had the retain flag set
///
/// # Example
///
/// ```
/// use mqtt_recorder::csv_handler::MessageRecord;
/// use chrono::Utc;
///
/// let record = MessageRecord {
///     timestamp: Utc::now(),
///     topic: "sensors/temperature".to_string(),
///     payload: r#"{"value": 23.5}"#.to_string(),
///     qos: 0,
///     retain: false,
/// };
/// ```
///
/// # Requirements
///
/// - **4.2**: WHEN a message is received, write a record containing: timestamp, topic,
///   payload, QoS, and retain flag
/// - **6.1**: Use column order: timestamp, topic, payload, qos, retain
/// - **6.5**: Store timestamps in ISO 8601 format with millisecond precision
/// - **6.6**: Store QoS as an integer (0, 1, or 2)
/// - **6.7**: Store retain flag as a boolean string ("true" or "false")
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MessageRecord {
    /// ISO 8601 timestamp with millisecond precision indicating when the message was received.
    ///
    /// The timestamp is stored in UTC and serialized in ISO 8601 format
    /// (e.g., "2024-01-15T10:30:00.123Z").
    pub timestamp: DateTime<Utc>,

    /// The MQTT topic the message was published to.
    ///
    /// This preserves the exact topic string, including any hierarchy
    /// (e.g., "sensors/temperature/room1").
    pub topic: String,

    /// The message payload as a string.
    ///
    /// The payload can be stored as:
    /// - Raw string when `encode_b64` is false
    /// - Base64-encoded string when `encode_b64` is true
    ///
    /// The encoding choice is determined at the CsvWriter/CsvReader level,
    /// not in this struct.
    pub payload: String,

    /// Quality of Service level for the message.
    ///
    /// Valid values are:
    /// - 0: At most once delivery
    /// - 1: At least once delivery
    /// - 2: Exactly once delivery
    pub qos: u8,

    /// Whether the message had the MQTT retain flag set.
    ///
    /// When true, the broker stores this message and delivers it to new
    /// subscribers immediately upon subscription.
    pub retain: bool,
}

impl MessageRecord {
    /// Creates a new MessageRecord with the given values.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp when the message was received
    /// * `topic` - The MQTT topic
    /// * `payload` - The message payload as a string
    /// * `qos` - Quality of Service level (0, 1, or 2)
    /// * `retain` - Whether the retain flag was set
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::csv_handler::MessageRecord;
    /// use chrono::Utc;
    ///
    /// let record = MessageRecord::new(
    ///     Utc::now(),
    ///     "sensors/temperature".to_string(),
    ///     r#"{"value": 23.5}"#.to_string(),
    ///     0,
    ///     false,
    /// );
    /// ```
    pub fn new(
        timestamp: DateTime<Utc>,
        topic: String,
        payload: String,
        qos: u8,
        retain: bool,
    ) -> Self {
        Self {
            timestamp,
            topic,
            payload,
            qos,
            retain,
        }
    }
}

/// A message record with payload as raw bytes instead of String.
///
/// This struct is similar to [`MessageRecord`] but stores the payload as a
/// `Vec<u8>` instead of a `String`. This is useful when working with binary
/// MQTT payloads that may not be valid UTF-8, such as protobuf messages,
/// compressed data, or raw sensor readings.
///
/// # Fields
///
/// * `timestamp` - ISO 8601 timestamp with millisecond precision indicating when
///   the message was received
/// * `topic` - The MQTT topic the message was published to
/// * `payload` - The message payload as raw bytes
/// * `qos` - Quality of Service level (0, 1, or 2)
/// * `retain` - Whether the message had the retain flag set
///
/// # Example
///
/// ```
/// use mqtt_recorder::csv_handler::MessageRecordBytes;
/// use chrono::Utc;
///
/// let record = MessageRecordBytes {
///     timestamp: Utc::now(),
///     topic: "binary/data".to_string(),
///     payload: vec![0x08, 0x0A, 0x12, 0x18],
///     qos: 0,
///     retain: false,
/// };
/// ```
///
/// # Requirements
///
/// - **3.1**: WHEN decode_b64 is false and a payload starts with "b64:",
///   THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
#[allow(dead_code)] // Public API for library users
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRecordBytes {
    /// ISO 8601 timestamp with millisecond precision indicating when the message was received.
    ///
    /// The timestamp is stored in UTC and serialized in ISO 8601 format
    /// (e.g., "2024-01-15T10:30:00.123Z").
    pub timestamp: DateTime<Utc>,

    /// The MQTT topic the message was published to.
    ///
    /// This preserves the exact topic string, including any hierarchy
    /// (e.g., "sensors/temperature/room1").
    pub topic: String,

    /// The message payload as raw bytes.
    ///
    /// Unlike [`MessageRecord::payload`] which stores the payload as a String,
    /// this field stores the raw bytes, allowing proper handling of binary
    /// data that may not be valid UTF-8.
    pub payload: Vec<u8>,

    /// Quality of Service level for the message.
    ///
    /// Valid values are:
    /// - 0: At most once delivery
    /// - 1: At least once delivery
    /// - 2: Exactly once delivery
    pub qos: u8,

    /// Whether the message had the MQTT retain flag set.
    ///
    /// When true, the broker stores this message and delivers it to new
    /// subscribers immediately upon subscription.
    pub retain: bool,
}

impl MessageRecordBytes {
    /// Creates a new MessageRecordBytes with the given values.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - The timestamp when the message was received
    /// * `topic` - The MQTT topic
    /// * `payload` - The message payload as raw bytes
    /// * `qos` - Quality of Service level (0, 1, or 2)
    /// * `retain` - Whether the retain flag was set
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::csv_handler::MessageRecordBytes;
    /// use chrono::Utc;
    ///
    /// let record = MessageRecordBytes::new(
    ///     Utc::now(),
    ///     "binary/data".to_string(),
    ///     vec![0x08, 0x0A, 0x12, 0x18],
    ///     0,
    ///     false,
    /// );
    /// ```
    #[allow(dead_code)] // Public API for library users
    pub fn new(
        timestamp: DateTime<Utc>,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
    ) -> Self {
        Self {
            timestamp,
            topic,
            payload,
            qos,
            retain,
        }
    }
}

/// Statistics collected during CSV writing.
///
/// This struct tracks various metrics about the records written to a CSV file,
/// including counts of different payload types and the largest payload size.
/// These statistics are useful for validation reporting and understanding
/// the composition of recorded data.
///
/// # Fields
///
/// * `total_records` - Total number of records written to the CSV file
/// * `text_payloads` - Number of payloads written as plain text (not auto-encoded)
/// * `auto_encoded_payloads` - Number of payloads that were automatically base64 encoded
///   due to binary content detection (prefixed with "b64:")
/// * `largest_payload` - Size in bytes of the largest payload encountered
///
/// # Example
///
/// ```
/// use mqtt_recorder::csv_handler::WriteStats;
///
/// let stats = WriteStats::default();
/// assert_eq!(stats.total_records, 0);
/// assert_eq!(stats.text_payloads, 0);
/// assert_eq!(stats.auto_encoded_payloads, 0);
/// assert_eq!(stats.largest_payload, 0);
/// ```
///
/// # Requirements
///
/// - **6.1**: WHEN validation completes, THE CSV_Validator SHALL report the total number of records processed
/// - **6.2**: WHEN validation completes, THE CSV_Validator SHALL report the number of text payloads
/// - **6.3**: WHEN validation completes, THE CSV_Validator SHALL report the number of binary payloads (auto-encoded with "b64:" prefix)
/// - **6.6**: WHEN validation completes, THE CSV_Validator SHALL report the largest payload size encountered
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteStats {
    /// Total number of records written to the CSV file.
    pub total_records: u64,
    /// Number of payloads written as plain text (not auto-encoded).
    pub text_payloads: u64,
    /// Number of payloads that were automatically base64 encoded due to binary content.
    pub auto_encoded_payloads: u64,
    /// Size in bytes of the largest payload encountered.
    pub largest_payload: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_message_record_creation() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            r#"{"value": 23.5}"#.to_string(),
            0,
            false,
        );

        assert_eq!(record.timestamp, timestamp);
        assert_eq!(record.topic, "sensors/temperature");
        assert_eq!(record.payload, r#"{"value": 23.5}"#);
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_message_record_with_retain_flag() {
        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "actuators/light".to_string(),
            "ON".to_string(),
            1,
            true,
        );

        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_message_record_clone() {
        let timestamp = Utc::now();
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            "payload".to_string(),
            2,
            false,
        );

        let cloned = record.clone();
        assert_eq!(record, cloned);
    }

    #[test]
    fn test_message_record_debug() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record =
            MessageRecord::new(timestamp, "test".to_string(), "data".to_string(), 0, false);

        let debug_str = format!("{:?}", record);
        assert!(debug_str.contains("MessageRecord"));
        assert!(debug_str.contains("test"));
        assert!(debug_str.contains("data"));
    }

    #[test]
    fn test_message_record_qos_values() {
        let timestamp = Utc::now();

        // Test QoS 0
        let record0 = MessageRecord::new(timestamp, "t".to_string(), "p".to_string(), 0, false);
        assert_eq!(record0.qos, 0);

        // Test QoS 1
        let record1 = MessageRecord::new(timestamp, "t".to_string(), "p".to_string(), 1, false);
        assert_eq!(record1.qos, 1);

        // Test QoS 2
        let record2 = MessageRecord::new(timestamp, "t".to_string(), "p".to_string(), 2, false);
        assert_eq!(record2.qos, 2);
    }

    #[test]
    fn test_message_record_serialization() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecord::new(
            timestamp,
            "sensors/temperature".to_string(),
            r#"{"value": 23.5}"#.to_string(),
            0,
            false,
        );

        // Test JSON serialization
        let json = serde_json::to_string(&record).expect("Failed to serialize");
        assert!(json.contains("sensors/temperature"));
        // The payload is stored as a string, so JSON escapes the inner quotes
        assert!(json.contains("value"));
        assert!(json.contains("23.5"));

        // Test JSON deserialization - this is the important test
        // The round-trip should preserve the original payload exactly
        let deserialized: MessageRecord =
            serde_json::from_str(&json).expect("Failed to deserialize");
        assert_eq!(record, deserialized);
        assert_eq!(deserialized.payload, r#"{"value": 23.5}"#);
    }

    #[test]
    fn test_message_record_with_special_characters_in_payload() {
        let timestamp = Utc::now();
        let payload_with_special_chars = "Hello, \"World\"!\nNew line\tTab";
        let record = MessageRecord::new(
            timestamp,
            "test/topic".to_string(),
            payload_with_special_chars.to_string(),
            0,
            false,
        );

        assert_eq!(record.payload, payload_with_special_chars);
    }

    #[test]
    fn test_message_record_with_empty_payload() {
        let timestamp = Utc::now();
        let record =
            MessageRecord::new(timestamp, "test/topic".to_string(), String::new(), 0, false);

        assert!(record.payload.is_empty());
    }

    #[test]
    fn test_message_record_with_mqtt_wildcard_topic() {
        let timestamp = Utc::now();

        // Single-level wildcard
        let record_plus = MessageRecord::new(
            timestamp,
            "sensors/+/temperature".to_string(),
            "data".to_string(),
            0,
            false,
        );
        assert_eq!(record_plus.topic, "sensors/+/temperature");

        // Multi-level wildcard
        let record_hash = MessageRecord::new(
            timestamp,
            "sensors/#".to_string(),
            "data".to_string(),
            0,
            false,
        );
        assert_eq!(record_hash.topic, "sensors/#");
    }

    // MessageRecordBytes tests

    #[test]
    fn test_message_record_bytes_creation() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecordBytes::new(
            timestamp,
            "binary/data".to_string(),
            vec![0x08, 0x0A, 0x12, 0x18],
            0,
            false,
        );

        assert_eq!(record.timestamp, timestamp);
        assert_eq!(record.topic, "binary/data");
        assert_eq!(record.payload, vec![0x08, 0x0A, 0x12, 0x18]);
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_message_record_bytes_with_retain_flag() {
        let timestamp = Utc::now();
        let record = MessageRecordBytes::new(
            timestamp,
            "actuators/light".to_string(),
            b"ON".to_vec(),
            1,
            true,
        );

        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_message_record_bytes_clone() {
        let timestamp = Utc::now();
        let record = MessageRecordBytes::new(
            timestamp,
            "test/topic".to_string(),
            vec![1, 2, 3, 4, 5],
            2,
            false,
        );

        let cloned = record.clone();
        assert_eq!(record, cloned);
    }

    #[test]
    fn test_message_record_bytes_debug() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record = MessageRecordBytes::new(
            timestamp,
            "test".to_string(),
            vec![0xDE, 0xAD, 0xBE, 0xEF],
            0,
            false,
        );

        let debug_str = format!("{:?}", record);
        assert!(debug_str.contains("MessageRecordBytes"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_message_record_bytes_qos_values() {
        let timestamp = Utc::now();

        // Test QoS 0
        let record0 = MessageRecordBytes::new(timestamp, "t".to_string(), vec![1], 0, false);
        assert_eq!(record0.qos, 0);

        // Test QoS 1
        let record1 = MessageRecordBytes::new(timestamp, "t".to_string(), vec![1], 1, false);
        assert_eq!(record1.qos, 1);

        // Test QoS 2
        let record2 = MessageRecordBytes::new(timestamp, "t".to_string(), vec![1], 2, false);
        assert_eq!(record2.qos, 2);
    }

    #[test]
    fn test_message_record_bytes_with_empty_payload() {
        let timestamp = Utc::now();
        let record =
            MessageRecordBytes::new(timestamp, "test/topic".to_string(), Vec::new(), 0, false);

        assert!(record.payload.is_empty());
    }

    #[test]
    fn test_message_record_bytes_with_binary_data() {
        let timestamp = Utc::now();
        // Simulate protobuf-like binary data with control characters
        let binary_payload = vec![0x08, 0x00, 0x12, 0x0A, 0xFF, 0xFE];
        let record = MessageRecordBytes::new(
            timestamp,
            "binary/protobuf".to_string(),
            binary_payload.clone(),
            0,
            false,
        );

        assert_eq!(record.payload, binary_payload);
    }

    #[test]
    fn test_message_record_bytes_with_text_as_bytes() {
        let timestamp = Utc::now();
        let text = "Hello, World!";
        let record = MessageRecordBytes::new(
            timestamp,
            "text/topic".to_string(),
            text.as_bytes().to_vec(),
            0,
            false,
        );

        assert_eq!(record.payload, text.as_bytes());
        // Verify we can convert back to string
        assert_eq!(String::from_utf8(record.payload).unwrap(), text);
    }

    #[test]
    fn test_message_record_bytes_equality() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let record1 =
            MessageRecordBytes::new(timestamp, "test".to_string(), vec![1, 2, 3], 0, false);
        let record2 =
            MessageRecordBytes::new(timestamp, "test".to_string(), vec![1, 2, 3], 0, false);
        let record3 = MessageRecordBytes::new(
            timestamp,
            "test".to_string(),
            vec![1, 2, 4], // Different payload
            0,
            false,
        );

        assert_eq!(record1, record2);
        assert_ne!(record1, record3);
    }

    // WriteStats tests

    #[test]
    fn test_write_stats_default() {
        let stats = WriteStats::default();
        assert_eq!(stats.total_records, 0);
        assert_eq!(stats.text_payloads, 0);
        assert_eq!(stats.auto_encoded_payloads, 0);
        assert_eq!(stats.largest_payload, 0);
    }

    #[test]
    fn test_write_stats_debug() {
        let stats = WriteStats {
            total_records: 100,
            text_payloads: 80,
            auto_encoded_payloads: 20,
            largest_payload: 1024,
        };
        let debug_str = format!("{:?}", stats);
        assert!(debug_str.contains("WriteStats"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("80"));
        assert!(debug_str.contains("20"));
        assert!(debug_str.contains("1024"));
    }

    #[test]
    fn test_write_stats_clone() {
        let stats = WriteStats {
            total_records: 50,
            text_payloads: 40,
            auto_encoded_payloads: 10,
            largest_payload: 512,
        };
        let cloned = stats.clone();
        assert_eq!(stats, cloned);
    }

    #[test]
    fn test_write_stats_equality() {
        let stats1 = WriteStats {
            total_records: 10,
            text_payloads: 8,
            auto_encoded_payloads: 2,
            largest_payload: 256,
        };
        let stats2 = WriteStats {
            total_records: 10,
            text_payloads: 8,
            auto_encoded_payloads: 2,
            largest_payload: 256,
        };
        let stats3 = WriteStats {
            total_records: 10,
            text_payloads: 7, // Different
            auto_encoded_payloads: 3,
            largest_payload: 256,
        };
        assert_eq!(stats1, stats2);
        assert_ne!(stats1, stats3);
    }
}
