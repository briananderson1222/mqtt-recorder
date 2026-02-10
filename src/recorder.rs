//! Recorder mode handler
//!
//! Handles recording MQTT messages to CSV files.
//!
//! This module provides the [`Recorder`] struct which subscribes to MQTT topics
//! and writes received messages to a CSV file. It supports graceful shutdown
//! via a broadcast channel and logs the message count on completion.
//!
//! # Example
//!
//! ```rust,ignore
//! use mqtt_recorder::recorder::Recorder;
//! use mqtt_recorder::mqtt::MqttClient;
//! use mqtt_recorder::csv_handler::CsvWriter;
//! use mqtt_recorder::topics::TopicFilter;
//! use rumqttc::QoS;
//! use tokio::sync::broadcast;
//!
//! let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
//! let mut recorder = Recorder::new(client, writer, topics, QoS::AtMostOnce).await;
//! let message_count = recorder.run(shutdown_rx).await?;
//! println!("Recorded {} messages", message_count);
//! ```
//!
//! # Requirements
//!
//! - **4.1**: WHEN mode is "record", subscribe to the specified topics and write received messages to the CSV file
//! - **4.2**: WHEN a message is received, write a record containing: timestamp, topic, payload, QoS, and retain flag
//! - **4.3**: WHEN encode_b64 is true, store message payloads as base64-encoded strings
//! - **4.4**: WHEN encode_b64 is false, store message payloads as raw strings
//! - **4.5**: Write a header row as the first line of the CSV file
//! - **4.6**: Properly escape CSV special characters in payloads
//! - **4.7**: Flush writes to ensure data persistence
//! - **4.8**: IF the output file cannot be created or written, report the error and exit
//! - **4.9**: WHEN the user sends an interrupt signal (Ctrl+C), gracefully close the file and disconnect from the broker

use chrono::{DateTime, Utc};
use rumqttc::QoS;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::csv_handler::CsvWriter;
use crate::error::MqttRecorderError;
use crate::mqtt::{AnyMqttClient, MqttIncoming};
use crate::topics::TopicFilter;
use crate::tui::TuiState;

/// Raw message data extracted from an MQTT publish event.
///
/// This struct holds the raw bytes of the payload rather than converting
/// to a string, allowing the CSV writer to properly detect binary content
/// and apply appropriate encoding.
struct RawMessage {
    timestamp: DateTime<Utc>,
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
}

/// Recorder for capturing MQTT messages to a CSV file.
///
/// The `Recorder` struct manages the process of subscribing to MQTT topics,
/// receiving messages, and writing them to a CSV file. It handles graceful
/// shutdown when receiving a signal through the broadcast channel.
///
/// # Fields
///
/// * `client` - The MQTT client for broker communication
/// * `writer` - The CSV writer for persisting messages
/// * `topics` - The topic filter specifying which topics to subscribe to
/// * `qos` - The Quality of Service level for subscriptions
///
/// # Requirements
///
/// - **4.1**: Subscribe to topics and write received messages to CSV
/// - **4.9**: Handle interrupt signals for graceful shutdown
pub struct Recorder {
    /// The MQTT client for broker communication.
    client: AnyMqttClient,
    /// The CSV writer for persisting messages.
    writer: CsvWriter,
    /// The topic filter specifying which topics to subscribe to.
    topics: TopicFilter,
    /// The Quality of Service level for subscriptions.
    qos: QoS,
}

impl Recorder {
    /// Creates a new Recorder with the given components.
    ///
    /// # Arguments
    ///
    /// * `client` - An MQTT client connected to the broker
    /// * `writer` - A CSV writer for the output file
    /// * `topics` - The topics to subscribe to
    /// * `qos` - The QoS level for subscriptions
    ///
    /// # Returns
    ///
    /// A new `Recorder` instance ready to start recording.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::recorder::Recorder;
    /// use mqtt_recorder::mqtt::{MqttClient, MqttClientConfig};
    /// use mqtt_recorder::csv_handler::CsvWriter;
    /// use mqtt_recorder::topics::TopicFilter;
    /// use rumqttc::QoS;
    /// use std::path::Path;
    ///
    /// let config = MqttClientConfig::new("localhost".to_string(), 1883, "recorder".to_string());
    /// let client = MqttClient::new(config).await?;
    /// let writer = CsvWriter::new(Path::new("output.csv"), false)?;
    /// let topics = TopicFilter::wildcard();
    ///
    /// let recorder = Recorder::new(client, writer, topics, QoS::AtMostOnce).await;
    /// ```
    pub async fn new(client: AnyMqttClient, writer: CsvWriter, topics: TopicFilter, qos: QoS) -> Self {
        Self {
            client,
            writer,
            topics,
            qos,
        }
    }

    /// Runs the recording loop until a shutdown signal is received.
    ///
    /// This method:
    /// 1. Subscribes to the configured topics
    /// 2. Polls for incoming messages
    /// 3. Writes each received message to the CSV file
    /// 4. Flushes writes periodically to ensure data persistence
    /// 5. Handles graceful shutdown on receiving the shutdown signal
    /// 6. Logs the total message count on completion
    ///
    /// # Arguments
    ///
    /// * `shutdown` - A broadcast receiver for the shutdown signal
    ///
    /// # Returns
    ///
    /// Returns `Ok(u64)` with the number of messages recorded on success,
    /// or an error if recording fails.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError`] if:
    /// - Subscription to topics fails
    /// - Writing to the CSV file fails
    /// - The MQTT connection is lost
    ///
    /// # Requirements
    ///
    /// - **4.1**: Subscribe to topics and write received messages to CSV
    /// - **4.2**: Write records with timestamp, topic, payload, QoS, and retain flag
    /// - **4.7**: Flush writes to ensure data persistence
    /// - **4.9**: Handle interrupt signals for graceful shutdown
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio::sync::broadcast;
    ///
    /// let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    /// let mut recorder = Recorder::new(client, writer, topics, QoS::AtMostOnce).await;
    ///
    /// // Run in a separate task
    /// let handle = tokio::spawn(async move {
    ///     recorder.run(shutdown_rx).await
    /// });
    ///
    /// // Later, trigger shutdown
    /// shutdown_tx.send(()).unwrap();
    /// let count = handle.await??;
    /// println!("Recorded {} messages", count);
    /// ```
    #[allow(dead_code)] // Public API for library users
    pub async fn run(
        &mut self,
        shutdown: broadcast::Receiver<()>,
    ) -> Result<u64, MqttRecorderError> {
        self.run_with_tui(shutdown, None).await
    }

    /// Runs the recording loop with optional TUI state updates.
    ///
    /// This is a wrapper around `run` that also updates the TUI state
    /// with the message count as messages are recorded.
    pub async fn run_with_tui(
        &mut self,
        mut shutdown: broadcast::Receiver<()>,
        tui_state: Option<Arc<TuiState>>,
    ) -> Result<u64, MqttRecorderError> {
        // Subscribe to the configured topics (Requirement 4.1)
        let topics = self.topics.topics();
        if !topics.is_empty() {
            self.client.subscribe(topics, self.qos).await?;
            eprintln!("Subscribed to {} topic(s)", topics.len());
        }

        let mut message_count: u64 = 0;
        let mut flush_counter: u64 = 0;
        const FLUSH_INTERVAL: u64 = 100;

        loop {
            // Check if TUI requested quit
            if let Some(ref state) = tui_state {
                if state.is_quit_requested() {
                    eprintln!("Quit requested from TUI, stopping recorder...");
                    break;
                }
            }

            tokio::select! {
                _ = shutdown.recv() => {
                    eprintln!("Shutdown signal received, stopping recorder...");
                    break;
                }
                event_result = self.client.poll() => {
                    match event_result {
                        Ok(event) => {
                            if let Some(msg) = self.process_event(event) {
                                self.writer.write_bytes(
                                    msg.timestamp,
                                    &msg.topic,
                                    &msg.payload,
                                    msg.qos,
                                    msg.retain,
                                )?;
                                message_count += 1;
                                flush_counter += 1;

                                // Update TUI state
                                if let Some(ref state) = tui_state {
                                    state.increment_received();
                                    state.increment_recorded();
                                }

                                if flush_counter >= FLUSH_INTERVAL {
                                    self.writer.flush()?;
                                    flush_counter = 0;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("MQTT error: {}", e);
                            if Self::is_fatal_error(&e) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        self.writer.flush()?;

        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            self.client.disconnect(),
        ).await;

        Ok(message_count)
    }

    /// Processes an MQTT event and extracts raw message data if applicable.
    ///
    /// This method handles incoming MQTT events and converts publish messages
    /// into [`RawMessage`] instances for CSV storage. The payload is kept as
    /// raw bytes to allow proper binary detection and encoding by the CSV writer.
    ///
    /// # Arguments
    ///
    /// * `event` - The MQTT event to process
    ///
    /// # Returns
    ///
    /// Returns `Some(RawMessage)` if the event contains a publish message,
    /// or `None` for other event types.
    fn process_event(&self, event: MqttIncoming) -> Option<RawMessage> {
        match event {
            MqttIncoming::Publish {
                topic,
                payload,
                qos,
                retain,
            } => {
                let timestamp = Utc::now();
                let qos_u8 = match qos {
                    QoS::AtMostOnce => 0,
                    QoS::AtLeastOnce => 1,
                    QoS::ExactlyOnce => 2,
                };

                Some(RawMessage {
                    timestamp,
                    topic,
                    payload: payload.to_vec(),
                    qos: qos_u8,
                    retain,
                })
            }
            MqttIncoming::ConnAck => {
                eprintln!("Connected to MQTT broker");
                None
            }
            MqttIncoming::SubAck => {
                eprintln!("Subscription acknowledged");
                None
            }
            MqttIncoming::Other => None,
        }
    }

    /// Determines if an error is fatal and should stop recording.
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
    /// Returns `true` if the error is fatal and recording should stop.
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
            // Other errors
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos_conversion() {
        // Test that QoS values are correctly mapped
        assert_eq!(
            match QoS::AtMostOnce {
                QoS::AtMostOnce => 0u8,
                QoS::AtLeastOnce => 1u8,
                QoS::ExactlyOnce => 2u8,
            },
            0
        );
        assert_eq!(
            match QoS::AtLeastOnce {
                QoS::AtMostOnce => 0u8,
                QoS::AtLeastOnce => 1u8,
                QoS::ExactlyOnce => 2u8,
            },
            1
        );
        assert_eq!(
            match QoS::ExactlyOnce {
                QoS::AtMostOnce => 0u8,
                QoS::AtLeastOnce => 1u8,
                QoS::ExactlyOnce => 2u8,
            },
            2
        );
    }

    #[test]
    fn test_is_fatal_error() {
        // IO errors should be fatal
        let io_error = MqttRecorderError::Io(std::io::Error::other("test"));
        assert!(Recorder::is_fatal_error(&io_error));

        // Invalid argument errors should not be fatal
        let arg_error = MqttRecorderError::InvalidArgument("test".to_string());
        assert!(!Recorder::is_fatal_error(&arg_error));

        // TLS errors should not be fatal (they occur during setup, not recording)
        let tls_error = MqttRecorderError::Tls("test".to_string());
        assert!(!Recorder::is_fatal_error(&tls_error));

        // Broker errors should not be fatal
        let broker_error = MqttRecorderError::Broker("test".to_string());
        assert!(!Recorder::is_fatal_error(&broker_error));
    }
}
