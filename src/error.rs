//! Error module
//!
//! Defines custom error types using `thiserror` for the MQTT Recorder application.
//! This module provides a unified error type that wraps all possible error sources
//! and implements the `From` trait for automatic conversion from underlying error types.

use thiserror::Error;

/// The main error type for the MQTT Recorder application.
///
/// This enum represents all possible errors that can occur during the operation
/// of the MQTT Recorder, including connection errors, file I/O errors, and
/// configuration errors.
///
/// # Error Categories
///
/// - **Connection/Client errors**: MQTT broker connection and client operation failures
/// - **File I/O errors**: CSV file reading/writing and general I/O failures
/// - **Configuration errors**: Invalid arguments, TLS configuration, and broker setup
///
/// # Example
///
/// ```rust,ignore
/// use mqtt_recorder::error::MqttRecorderError;
///
/// fn example() -> Result<(), MqttRecorderError> {
///     // Errors from underlying types are automatically converted
///     let file = std::fs::File::open("nonexistent.csv")?;
///     Ok(())
/// }
/// ```
#[derive(Error, Debug)]
pub enum MqttRecorderError {
    /// MQTT connection error from the rumqttc client.
    ///
    /// This error occurs when the MQTT client fails to establish or maintain
    /// a connection to the broker.
    ///
    /// Note: The error is boxed to reduce the size of the Result type,
    /// as rumqttc::ConnectionError is 144 bytes.
    #[error("MQTT connection error: {0}")]
    Connection(#[source] Box<rumqttc::ConnectionError>),

    /// MQTT client operation error from the rumqttc client.
    ///
    /// This error occurs when an MQTT client operation (publish, subscribe, etc.)
    /// fails.
    #[error("MQTT client error: {0}")]
    Client(#[source] Box<rumqttc::ClientError>),

    /// CSV file handling error.
    ///
    /// This error occurs when reading from or writing to CSV files fails,
    /// including parsing errors and field size limit violations.
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    /// General I/O error.
    ///
    /// This error occurs for file system operations like opening, reading,
    /// or writing files.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parsing error.
    ///
    /// This error occurs when parsing JSON files (e.g., topics configuration)
    /// fails due to invalid JSON syntax or structure.
    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid command-line argument error.
    ///
    /// This error occurs when CLI arguments are invalid or have incompatible
    /// combinations (e.g., missing required arguments for a specific mode).
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// CSV validation failure error.
    ///
    /// This error occurs when CSV validation finds invalid records in the file.
    /// The validation report has already been printed; this error signals
    /// that the process should exit with code 3.
    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    /// TLS configuration error.
    ///
    /// This error occurs when TLS/SSL configuration fails, such as invalid
    /// certificate paths or certificate parsing errors.
    #[error("TLS configuration error: {0}")]
    Tls(String),

    /// Embedded broker error.
    ///
    /// This error occurs when the embedded MQTT broker (rumqttd) fails to
    /// start, configure, or operate correctly.
    #[error("Broker error: {0}")]
    Broker(String),
}

// Manual From implementations for boxed error types
impl From<rumqttc::ConnectionError> for MqttRecorderError {
    fn from(err: rumqttc::ConnectionError) -> Self {
        MqttRecorderError::Connection(Box::new(err))
    }
}

impl From<rumqttc::ClientError> for MqttRecorderError {
    fn from(err: rumqttc::ClientError) -> Self {
        MqttRecorderError::Client(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_argument_error_display() {
        let error = MqttRecorderError::InvalidArgument("missing --host".to_string());
        assert_eq!(error.to_string(), "Invalid argument: missing --host");
    }

    #[test]
    fn test_validation_failed_error_display() {
        let error = MqttRecorderError::ValidationFailed("5 invalid records found".to_string());
        assert_eq!(
            error.to_string(),
            "Validation failed: 5 invalid records found"
        );
    }

    #[test]
    fn test_tls_error_display() {
        let error = MqttRecorderError::Tls("certificate not found".to_string());
        assert_eq!(
            error.to_string(),
            "TLS configuration error: certificate not found"
        );
    }

    #[test]
    fn test_broker_error_display() {
        let error = MqttRecorderError::Broker("failed to bind port".to_string());
        assert_eq!(error.to_string(), "Broker error: failed to bind port");
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error: MqttRecorderError = io_error.into();
        assert!(matches!(error, MqttRecorderError::Io(_)));
        assert!(error.to_string().contains("IO error"));
    }

    #[test]
    fn test_json_error_conversion() {
        let json_str = "{ invalid json }";
        let json_result: Result<serde_json::Value, _> = serde_json::from_str(json_str);
        let json_error = json_result.unwrap_err();
        let error: MqttRecorderError = json_error.into();
        assert!(matches!(error, MqttRecorderError::Json(_)));
        assert!(error.to_string().contains("JSON parsing error"));
    }

    #[test]
    fn test_error_is_debug() {
        let error = MqttRecorderError::InvalidArgument("test".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("InvalidArgument"));
    }
}
