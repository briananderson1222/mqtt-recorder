//! Replayer mode handler
//!
//! Handles replaying MQTT messages from CSV files.
//!
//! This module provides the [`Replayer`] struct which reads messages from a CSV file
//! and publishes them to an MQTT broker with timing preservation. It supports loop
//! replay and graceful shutdown via a broadcast channel.
//!
//! # Example
//!
//! ```rust,ignore
//! use mqtt_recorder::replayer::Replayer;
//! use mqtt_recorder::mqtt::MqttClient;
//! use mqtt_recorder::csv_handler::CsvReader;
//! use tokio::sync::broadcast;
//!
//! let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
//! let mut replayer = Replayer::new(client, reader, false).await;
//! let message_count = replayer.run(shutdown_rx).await?;
//! println!("Replayed {} messages", message_count);
//! ```
//!
//! # Requirements
//!
//! - **5.1**: WHEN mode is "replay", read messages from the CSV file and publish them to the broker
//! - **5.2**: WHEN replaying messages, preserve the original topic for each message
//! - **5.3**: WHEN replaying messages, preserve the original QoS level for each message
//! - **5.4**: WHEN replaying messages, preserve the original retain flag for each message
//! - **5.5**: WHEN replaying messages, maintain the relative timing between messages based on recorded timestamps
//! - **5.6**: WHEN encode_b64 was used during recording, decode base64 payloads before publishing
//! - **5.7**: WHEN loop is true, restart replay from the beginning after reaching the end of the file
//! - **5.8**: WHEN loop is false, exit after replaying all messages
//! - **5.9**: IF the input file cannot be read, report the error and exit
//! - **5.10**: IF the CSV file contains invalid data, report the parsing error and exit
//! - **5.11**: WHEN the user sends an interrupt signal (Ctrl+C), gracefully disconnect from the broker

use chrono::{DateTime, Utc};
use rumqttc::QoS;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};

use crate::csv_handler::{CsvReader, MessageRecord};
use crate::error::MqttRecorderError;
use crate::mqtt::MqttClient;

/// Replayer for publishing MQTT messages from a CSV file.
///
/// The `Replayer` struct manages the process of reading messages from a CSV file
/// and publishing them to an MQTT broker with timing preservation. It handles
/// graceful shutdown when receiving a signal through the broadcast channel.
///
/// # Fields
///
/// * `client` - The MQTT client for broker communication
/// * `reader` - The CSV reader for reading recorded messages
/// * `loop_replay` - Whether to loop replay continuously
///
/// # Requirements
///
/// - **5.1**: Read messages from CSV and publish to broker
/// - **5.5**: Maintain relative timing between messages
/// - **5.7**: Support loop replay
/// - **5.11**: Handle interrupt signals for graceful shutdown
pub struct Replayer {
    /// The MQTT client for broker communication.
    client: MqttClient,
    /// The CSV reader for reading recorded messages.
    reader: CsvReader,
    /// Whether to loop replay continuously.
    loop_replay: bool,
}

impl Replayer {
    /// Creates a new Replayer with the given components.
    ///
    /// # Arguments
    ///
    /// * `client` - An MQTT client connected to the broker
    /// * `reader` - A CSV reader for the input file
    /// * `loop_replay` - Whether to loop replay continuously
    ///
    /// # Returns
    ///
    /// A new `Replayer` instance ready to start replaying.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::replayer::Replayer;
    /// use mqtt_recorder::mqtt::{MqttClient, MqttClientConfig};
    /// use mqtt_recorder::csv_handler::CsvReader;
    /// use std::path::Path;
    ///
    /// let config = MqttClientConfig::new("localhost".to_string(), 1883, "replayer".to_string());
    /// let client = MqttClient::new(config).await?;
    /// let reader = CsvReader::new(Path::new("input.csv"), false, None)?;
    ///
    /// let replayer = Replayer::new(client, reader, false).await;
    /// ```
    pub async fn new(client: MqttClient, reader: CsvReader, loop_replay: bool) -> Self {
        Self {
            client,
            reader,
            loop_replay,
        }
    }

    /// Runs the replay loop until complete or a shutdown signal is received.
    ///
    /// This method:
    /// 1. Reads messages from the CSV file
    /// 2. Calculates delays based on timestamp differences
    /// 3. Publishes each message to the broker with preserved topic, QoS, and retain flag
    /// 4. Handles loop replay by resetting the reader when reaching end of file
    /// 5. Handles graceful shutdown on receiving the shutdown signal
    /// 6. Logs the total message count on completion
    ///
    /// # Arguments
    ///
    /// * `shutdown` - A broadcast receiver for the shutdown signal
    ///
    /// # Returns
    ///
    /// Returns `Ok(u64)` with the number of messages replayed on success,
    /// or an error if replay fails.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError`] if:
    /// - Reading from the CSV file fails (Requirement 5.9, 5.10)
    /// - Publishing to the broker fails
    /// - The MQTT connection is lost
    ///
    /// # Requirements
    ///
    /// - **5.1**: Read messages from CSV and publish to broker
    /// - **5.2**: Preserve the original topic for each message
    /// - **5.3**: Preserve the original QoS level for each message
    /// - **5.4**: Preserve the original retain flag for each message
    /// - **5.5**: Maintain relative timing between messages
    /// - **5.7**: Restart replay from beginning when loop is true
    /// - **5.8**: Exit after replaying all messages when loop is false
    /// - **5.11**: Handle interrupt signals for graceful shutdown
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio::sync::broadcast;
    ///
    /// let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    /// let mut replayer = Replayer::new(client, reader, false).await;
    ///
    /// // Run in a separate task
    /// let handle = tokio::spawn(async move {
    ///     replayer.run(shutdown_rx).await
    /// });
    ///
    /// // Later, trigger shutdown
    /// shutdown_tx.send(()).unwrap();
    /// let count = handle.await??;
    /// println!("Replayed {} messages", count);
    /// ```
    pub async fn run(
        &mut self,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<u64, MqttRecorderError> {
        let mut message_count: u64 = 0;
        let mut previous_timestamp: Option<DateTime<Utc>> = None;

        eprintln!(
            "Starting replay (loop: {})",
            if self.loop_replay {
                "enabled"
            } else {
                "disabled"
            }
        );

        // Initial connection check - poll once to establish connection
        match self.client.poll().await {
            Ok(_) => eprintln!("Connected to MQTT broker"),
            Err(e) => {
                // Log but continue - connection might be established on first publish
                eprintln!("Initial connection status: {}", e);
            }
        }

        loop {
            // Read the next message from CSV (Requirements 5.1, 5.9, 5.10)
            let record_result = self.reader.next();

            match record_result {
                Some(Ok(record)) => {
                    // Calculate delay based on timestamp difference (Requirement 5.5)
                    if let Some(prev_ts) = previous_timestamp {
                        let delay = Self::calculate_delay(&prev_ts, &record.timestamp);
                        if delay > Duration::ZERO {
                            // Use tokio::select to allow shutdown during delay
                            tokio::select! {
                                _ = shutdown.recv() => {
                                    eprintln!("Shutdown signal received during delay, stopping replayer...");
                                    break;
                                }
                                _ = sleep(delay) => {
                                    // Delay completed, continue with publish
                                }
                            }
                        }
                    }

                    // Update previous timestamp for next iteration
                    previous_timestamp = Some(record.timestamp);

                    // Publish the message (Requirements 5.2, 5.3, 5.4)
                    match self.publish_message(&record).await {
                        Ok(()) => {
                            message_count += 1;
                        }
                        Err(e) => {
                            eprintln!("Error publishing message: {}", e);
                            // Check if this is a fatal error
                            if Self::is_fatal_error(&e) {
                                return Err(e);
                            }
                        }
                    }

                    // Poll the event loop to process any pending events
                    // This is needed to maintain the connection
                    if let Err(e) = self.poll_events().await {
                        if Self::is_fatal_error(&e) {
                            return Err(e);
                        }
                    }

                    // Check for shutdown signal (non-blocking)
                    match shutdown.try_recv() {
                        Ok(_) => {
                            eprintln!("Shutdown signal received, stopping replayer...");
                            break;
                        }
                        Err(broadcast::error::TryRecvError::Empty) => {
                            // No shutdown signal, continue
                        }
                        Err(broadcast::error::TryRecvError::Closed) => {
                            // Channel closed, treat as shutdown
                            eprintln!("Shutdown channel closed, stopping replayer...");
                            break;
                        }
                        Err(broadcast::error::TryRecvError::Lagged(_)) => {
                            // Missed some signals, but continue
                        }
                    }
                }
                Some(Err(e)) => {
                    // CSV parsing error (Requirement 5.10)
                    eprintln!("Error reading CSV record: {}", e);
                    return Err(e);
                }
                None => {
                    // End of file reached
                    if self.loop_replay {
                        // Reset reader and continue (Requirement 5.7)
                        eprintln!(
                            "End of file reached, restarting replay... ({} messages so far)",
                            message_count
                        );
                        self.reader.reset()?;
                        previous_timestamp = None;
                    } else {
                        // Exit after replaying all messages (Requirement 5.8)
                        break;
                    }
                }
            }
        }

        // Disconnect from the broker (Requirement 5.11)
        if let Err(e) = self.client.disconnect().await {
            eprintln!("Warning: Error disconnecting from broker: {}", e);
        }

        // Log message count on completion (Requirement 8.4)
        eprintln!("Replay complete. {} messages replayed.", message_count);

        Ok(message_count)
    }

    /// Publishes a message record to the MQTT broker.
    ///
    /// This method converts the message record to the appropriate format
    /// and publishes it to the broker, preserving the original topic,
    /// QoS level, and retain flag.
    ///
    /// # Arguments
    ///
    /// * `record` - The message record to publish
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if publishing fails.
    ///
    /// # Requirements
    ///
    /// - **5.2**: Preserve the original topic for each message
    /// - **5.3**: Preserve the original QoS level for each message
    /// - **5.4**: Preserve the original retain flag for each message
    /// - **5.6**: Decode base64 payloads before publishing (handled by CsvReader)
    async fn publish_message(&self, record: &MessageRecord) -> Result<(), MqttRecorderError> {
        // Convert QoS u8 to rumqttc QoS enum (Requirement 5.3)
        let qos = Self::u8_to_qos(record.qos);

        // Publish with preserved topic, payload, QoS, and retain flag
        // (Requirements 5.2, 5.3, 5.4)
        // Note: Base64 decoding is handled by CsvReader if configured (Requirement 5.6)
        self.client
            .publish(&record.topic, record.payload.as_bytes(), qos, record.retain)
            .await
    }

    /// Calculates the delay between two timestamps.
    ///
    /// This method computes the time difference between two message timestamps
    /// to maintain the relative timing during replay.
    ///
    /// # Arguments
    ///
    /// * `previous` - The timestamp of the previous message
    /// * `current` - The timestamp of the current message
    ///
    /// # Returns
    ///
    /// Returns the duration to wait before publishing the current message.
    /// Returns `Duration::ZERO` if the current timestamp is before or equal
    /// to the previous timestamp.
    ///
    /// # Requirements
    ///
    /// - **5.5**: Maintain relative timing between messages based on recorded timestamps
    fn calculate_delay(previous: &DateTime<Utc>, current: &DateTime<Utc>) -> Duration {
        let diff = *current - *previous;

        if diff.num_milliseconds() > 0 {
            Duration::from_millis(diff.num_milliseconds() as u64)
        } else {
            Duration::ZERO
        }
    }

    /// Converts a u8 QoS value to rumqttc QoS enum.
    ///
    /// # Arguments
    ///
    /// * `qos` - The QoS value as u8 (0, 1, or 2)
    ///
    /// # Returns
    ///
    /// The corresponding rumqttc QoS enum value.
    /// Defaults to `QoS::AtMostOnce` for invalid values.
    fn u8_to_qos(qos: u8) -> QoS {
        match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => QoS::AtMostOnce, // Default to QoS 0 for invalid values
        }
    }

    /// Polls the MQTT event loop to process pending events.
    ///
    /// This is needed to maintain the connection and handle acknowledgments.
    async fn poll_events(&self) -> Result<(), MqttRecorderError> {
        // Use a short timeout to avoid blocking
        match tokio::time::timeout(Duration::from_millis(10), self.client.poll()).await {
            Ok(result) => result.map(|_| ()),
            Err(_) => Ok(()), // Timeout is fine, just means no events pending
        }
    }

    /// Determines if an error is fatal and should stop replay.
    ///
    /// Some errors are recoverable (e.g., temporary network issues),
    /// while others indicate a permanent failure.
    ///
    /// # Arguments
    ///
    /// * `error` - The error to check
    ///
    /// # Returns
    ///
    /// Returns `true` if the error is fatal and replay should stop.
    fn is_fatal_error(error: &MqttRecorderError) -> bool {
        match error {
            // Connection errors are typically fatal
            MqttRecorderError::Connection(_) => true,
            // Client errors might be recoverable
            MqttRecorderError::Client(_) => false,
            // IO errors are fatal
            MqttRecorderError::Io(_) => true,
            // CSV errors are fatal
            MqttRecorderError::Csv(_) => true,
            // Invalid argument errors are fatal (bad CSV data)
            MqttRecorderError::InvalidArgument(_) => true,
            // Other errors
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_calculate_delay_positive() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 1).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2);
        assert_eq!(delay, Duration::from_secs(1));
    }

    #[test]
    fn test_calculate_delay_milliseconds() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let ts2 = ts1
            .checked_add_signed(chrono::Duration::milliseconds(500))
            .unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2);
        assert_eq!(delay, Duration::from_millis(500));
    }

    #[test]
    fn test_calculate_delay_zero_for_same_timestamp() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let delay = Replayer::calculate_delay(&ts, &ts);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_calculate_delay_zero_for_earlier_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 1).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_u8_to_qos_valid_values() {
        assert!(matches!(Replayer::u8_to_qos(0), QoS::AtMostOnce));
        assert!(matches!(Replayer::u8_to_qos(1), QoS::AtLeastOnce));
        assert!(matches!(Replayer::u8_to_qos(2), QoS::ExactlyOnce));
    }

    #[test]
    fn test_u8_to_qos_invalid_values() {
        // Invalid values should default to QoS 0
        assert!(matches!(Replayer::u8_to_qos(3), QoS::AtMostOnce));
        assert!(matches!(Replayer::u8_to_qos(255), QoS::AtMostOnce));
    }

    #[test]
    fn test_is_fatal_error() {
        // IO errors should be fatal
        let io_error = MqttRecorderError::Io(std::io::Error::other("test"));
        assert!(Replayer::is_fatal_error(&io_error));

        // CSV errors should be fatal
        let csv_error = MqttRecorderError::Csv(csv::Error::from(std::io::Error::other("test")));
        assert!(Replayer::is_fatal_error(&csv_error));

        // Invalid argument errors should be fatal
        let arg_error = MqttRecorderError::InvalidArgument("test".to_string());
        assert!(Replayer::is_fatal_error(&arg_error));

        // TLS errors should not be fatal (they occur during setup, not replay)
        let tls_error = MqttRecorderError::Tls("test".to_string());
        assert!(!Replayer::is_fatal_error(&tls_error));

        // Broker errors should not be fatal
        let broker_error = MqttRecorderError::Broker("test".to_string());
        assert!(!Replayer::is_fatal_error(&broker_error));
    }

    #[test]
    fn test_calculate_delay_large_difference() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 11, 0, 0).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2);
        assert_eq!(delay, Duration::from_secs(3600)); // 1 hour
    }
}
