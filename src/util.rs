//! Utility functions for the MQTT Recorder application.
//!
//! This module provides common utility functions used across different modules,
//! including QoS conversions, client ID generation, and error classification.

use crate::error::MqttRecorderError;
use rumqttc::QoS;

/// Timeout in seconds for graceful MQTT disconnect operations.
pub const DISCONNECT_TIMEOUT_SECS: u64 = 2;

/// Number of messages between CSV flush operations.
pub const FLUSH_INTERVAL: u64 = 100;

/// Convert QoS enum to u8 value.
///
/// # Arguments
///
/// * `qos` - The QoS level to convert
///
/// # Returns
///
/// * `0` for `QoS::AtMostOnce`
/// * `1` for `QoS::AtLeastOnce`
/// * `2` for `QoS::ExactlyOnce`
pub fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

/// Convert u8 value to QoS enum.
///
/// # Arguments
///
/// * `qos` - The QoS level as u8
///
/// # Returns
///
/// * `QoS::AtMostOnce` for `0`
/// * `QoS::AtLeastOnce` for `1`
/// * `QoS::ExactlyOnce` for `2`
/// * `QoS::AtMostOnce` for any other value (default)
pub fn u8_to_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

/// Generate a client ID from an optional string.
///
/// # Arguments
///
/// * `client_id` - Optional client ID string
///
/// # Returns
///
/// If `client_id` is `Some` and non-empty, returns a clone of the string.
/// Otherwise, generates a unique client ID using timestamp-based hashing.
pub fn generate_client_id(client_id: &Option<String>) -> String {
    match client_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let hash = timestamp ^ (timestamp >> 32);
            format!("mqtt-recorder-{:08x}", hash as u32)
        }
    }
}

/// Determine if an error is fatal based on its type and recovery settings.
///
/// # Arguments
///
/// * `error` - The error to classify
/// * `recoverable_connection` - Whether connection errors should be treated as recoverable
///
/// # Returns
///
/// `true` if the error is fatal and should terminate the application,
/// `false` if the error is recoverable and the application can continue.
pub fn is_fatal_error(error: &MqttRecorderError, recoverable_connection: bool) -> bool {
    match error {
        MqttRecorderError::Connection(_) => !recoverable_connection,
        MqttRecorderError::Client(_) => false,
        MqttRecorderError::Io(_) => true,
        MqttRecorderError::Csv(_) => true,
        MqttRecorderError::InvalidArgument(_) => true,
        MqttRecorderError::Json(_) => false,
        MqttRecorderError::ValidationFailed(_) => false,
        MqttRecorderError::Tls(_) => false,
        MqttRecorderError::Broker(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos_to_u8() {
        assert_eq!(qos_to_u8(QoS::AtMostOnce), 0);
        assert_eq!(qos_to_u8(QoS::AtLeastOnce), 1);
        assert_eq!(qos_to_u8(QoS::ExactlyOnce), 2);
    }

    #[test]
    fn test_u8_to_qos() {
        assert_eq!(u8_to_qos(0), QoS::AtMostOnce);
        assert_eq!(u8_to_qos(1), QoS::AtLeastOnce);
        assert_eq!(u8_to_qos(2), QoS::ExactlyOnce);
        assert_eq!(u8_to_qos(3), QoS::AtMostOnce); // default case
        assert_eq!(u8_to_qos(255), QoS::AtMostOnce); // default case
    }

    #[test]
    fn test_generate_client_id_with_some_non_empty() {
        let client_id = Some("test-client".to_string());
        assert_eq!(generate_client_id(&client_id), "test-client");
    }

    #[test]
    fn test_generate_client_id_with_some_empty() {
        let client_id = Some("".to_string());
        let result = generate_client_id(&client_id);
        assert!(result.starts_with("mqtt-recorder-"));
        assert_eq!(result.len(), "mqtt-recorder-".len() + 8); // 8 hex chars
    }

    #[test]
    fn test_generate_client_id_with_none() {
        let client_id = None;
        let result = generate_client_id(&client_id);
        assert!(result.starts_with("mqtt-recorder-"));
        assert_eq!(result.len(), "mqtt-recorder-".len() + 8); // 8 hex chars
    }

    #[test]
    fn test_generate_client_id_unique() {
        let client_id = None;
        let result1 = generate_client_id(&client_id);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let result2 = generate_client_id(&client_id);
        assert_ne!(result1, result2);
    }

    #[test]
    fn test_is_fatal_error_connection_recoverable() {
        let error = MqttRecorderError::Connection(Box::new(rumqttc::ConnectionError::Io(
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused"),
        )));
        assert!(!is_fatal_error(&error, true)); // recoverable
        assert!(is_fatal_error(&error, false)); // not recoverable
    }

    #[test]
    fn test_is_fatal_error_client() {
        let error = MqttRecorderError::Client(Box::new(rumqttc::ClientError::Request(
            rumqttc::Request::Publish(rumqttc::Publish::new("test", QoS::AtMostOnce, "payload")),
        )));
        assert!(!is_fatal_error(&error, true));
        assert!(!is_fatal_error(&error, false));
    }

    #[test]
    fn test_is_fatal_error_io() {
        let error = MqttRecorderError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        assert!(is_fatal_error(&error, true));
        assert!(is_fatal_error(&error, false));
    }

    #[test]
    fn test_is_fatal_error_csv() {
        let io_error = std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid csv");
        let error = MqttRecorderError::Csv(csv::Error::from(io_error));
        assert!(is_fatal_error(&error, true));
        assert!(is_fatal_error(&error, false));
    }

    #[test]
    fn test_is_fatal_error_invalid_argument() {
        let error = MqttRecorderError::InvalidArgument("missing argument".to_string());
        assert!(is_fatal_error(&error, true));
        assert!(is_fatal_error(&error, false));
    }

    #[test]
    fn test_is_fatal_error_non_fatal_types() {
        let json_error = MqttRecorderError::Json(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid json",
        )));
        assert!(!is_fatal_error(&json_error, true));
        assert!(!is_fatal_error(&json_error, false));

        let validation_error = MqttRecorderError::ValidationFailed("validation failed".to_string());
        assert!(!is_fatal_error(&validation_error, true));
        assert!(!is_fatal_error(&validation_error, false));

        let tls_error = MqttRecorderError::Tls("tls error".to_string());
        assert!(!is_fatal_error(&tls_error, true));
        assert!(!is_fatal_error(&tls_error, false));

        let broker_error = MqttRecorderError::Broker("broker error".to_string());
        assert!(!is_fatal_error(&broker_error, true));
        assert!(!is_fatal_error(&broker_error, false));
    }
}
