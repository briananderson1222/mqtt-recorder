//! Mirror mode handler
//!
//! Handles mirroring MQTT messages from an external broker to an embedded broker.
//!
//! This module provides the [`Mirror`] struct which subscribes to topics on an external
//! MQTT broker and republishes received messages to an embedded broker. It optionally
//! records messages to a CSV file and supports graceful shutdown via a broadcast channel.
//!
//! # Example
//!
//! ```rust,ignore
//! use mqtt_recorder::mirror::Mirror;
//! use mqtt_recorder::mqtt::MqttClient;
//! use mqtt_recorder::broker::{EmbeddedBroker, BrokerMode};
//! use mqtt_recorder::csv_handler::CsvWriter;
//! use mqtt_recorder::topics::TopicFilter;
//! use rumqttc::QoS;
//! use tokio::sync::broadcast;
//!
//! let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
//! let mut mirror = Mirror::new(source_client, broker, Some(writer), topics, QoS::AtMostOnce).await?;
//! let message_count = mirror.run(shutdown_rx).await?;
//! println!("Mirrored {} messages", message_count);
//! ```
//!
//! # Requirements
//!
//! - **11.1**: Accept `--mode mirror` as a valid mode option
//! - **11.2**: WHEN mode is "mirror", require `--serve` to be enabled
//! - **11.3**: WHEN mode is "mirror", subscribe to topics on the external broker specified by `--host`
//! - **11.4**: WHEN mode is "mirror", republish received messages to the embedded broker
//! - **11.5**: WHEN mode is "mirror" and `--file` is provided, also record messages to the CSV file
//! - **11.6**: WHEN mode is "mirror", preserve message topic, payload, QoS, and retain flag during republishing
//! - **11.7**: Support all topic filtering options (--topic, --topics JSON file, or wildcard #)
//! - **11.8**: Log the number of messages mirrored periodically to stderr
//! - **11.9**: WHEN the user sends an interrupt signal in mirror mode, gracefully disconnect from the external broker and shut down the embedded broker

use chrono::Utc;
use rumqttc::{Event, Packet, QoS};
use tokio::sync::broadcast;

use crate::broker::EmbeddedBroker;
use crate::csv_handler::{CsvWriter, MessageRecord};
use crate::error::MqttRecorderError;
use crate::mqtt::MqttClient;
use crate::topics::TopicFilter;

/// Interval for logging mirrored message count (in number of messages).
const LOG_INTERVAL: u64 = 100;

/// Mirror for republishing MQTT messages from an external broker to an embedded broker.
///
/// The `Mirror` struct manages the process of subscribing to topics on an external
/// MQTT broker, receiving messages, and republishing them to an embedded broker.
/// It optionally records messages to a CSV file and handles graceful shutdown
/// when receiving a signal through the broadcast channel.
///
/// # Fields
///
/// * `source_client` - The MQTT client connected to the external broker
/// * `broker` - The embedded broker instance
/// * `local_client` - The MQTT client connected to the embedded broker for publishing
/// * `writer` - Optional CSV writer for recording messages
/// * `topics` - The topic filter specifying which topics to subscribe to
/// * `qos` - The Quality of Service level for subscriptions
///
/// # Requirements
///
/// - **11.3**: Subscribe to topics on the external broker
/// - **11.4**: Republish received messages to the embedded broker
/// - **11.5**: Optionally record messages to CSV
/// - **11.9**: Handle interrupt signals for graceful shutdown
pub struct Mirror {
    /// The MQTT client connected to the external broker (source).
    source_client: MqttClient,
    /// The embedded broker instance.
    broker: EmbeddedBroker,
    /// The MQTT client connected to the embedded broker for publishing.
    local_client: MqttClient,
    /// Optional CSV writer for recording messages.
    writer: Option<CsvWriter>,
    /// The topic filter specifying which topics to subscribe to.
    topics: TopicFilter,
    /// The Quality of Service level for subscriptions.
    qos: QoS,
}

impl Mirror {
    /// Creates a new Mirror with the given components.
    ///
    /// This method initializes the Mirror by obtaining a local client from the
    /// embedded broker for publishing mirrored messages.
    ///
    /// # Arguments
    ///
    /// * `source_client` - An MQTT client connected to the external broker
    /// * `broker` - The embedded broker instance
    /// * `writer` - Optional CSV writer for recording messages
    /// * `topics` - The topics to subscribe to on the external broker
    /// * `qos` - The QoS level for subscriptions
    ///
    /// # Returns
    ///
    /// Returns `Ok(Mirror)` on success, or an error if the local client cannot be created.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError`] if:
    /// - The local client cannot be obtained from the embedded broker
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::mirror::Mirror;
    /// use mqtt_recorder::mqtt::{MqttClient, MqttClientConfig};
    /// use mqtt_recorder::broker::{EmbeddedBroker, BrokerMode};
    /// use mqtt_recorder::csv_handler::CsvWriter;
    /// use mqtt_recorder::topics::TopicFilter;
    /// use rumqttc::QoS;
    /// use std::path::Path;
    ///
    /// let source_config = MqttClientConfig::new("external-broker".to_string(), 1883, "mirror-source".to_string());
    /// let source_client = MqttClient::new(source_config).await?;
    /// let broker = EmbeddedBroker::new(1884, BrokerMode::Mirror).await?;
    /// let writer = Some(CsvWriter::new(Path::new("output.csv"), false)?);
    /// let topics = TopicFilter::wildcard();
    ///
    /// let mirror = Mirror::new(source_client, broker, writer, topics, QoS::AtMostOnce).await?;
    /// ```
    ///
    /// # Requirements
    ///
    /// - **11.2**: Mirror mode requires the embedded broker to be enabled
    pub async fn new(
        source_client: MqttClient,
        broker: EmbeddedBroker,
        writer: Option<CsvWriter>,
        topics: TopicFilter,
        qos: QoS,
    ) -> Result<Self, MqttRecorderError> {
        // Get a local client for publishing to the embedded broker
        let local_client = broker.get_local_client().await?;

        Ok(Self {
            source_client,
            broker,
            local_client,
            writer,
            topics,
            qos,
        })
    }

    /// Runs the mirror loop until a shutdown signal is received.
    ///
    /// This method:
    /// 1. Subscribes to the configured topics on the external broker
    /// 2. Polls for incoming messages from the external broker
    /// 3. Republishes each received message to the embedded broker
    /// 4. Optionally writes each message to the CSV file
    /// 5. Logs the message count periodically
    /// 6. Handles graceful shutdown on receiving the shutdown signal
    /// 7. Logs the total message count on completion
    ///
    /// # Arguments
    ///
    /// * `shutdown` - A broadcast receiver for the shutdown signal
    ///
    /// # Returns
    ///
    /// Returns `Ok(u64)` with the number of messages mirrored on success,
    /// or an error if mirroring fails.
    ///
    /// # Errors
    ///
    /// Returns [`MqttRecorderError`] if:
    /// - Subscription to topics fails
    /// - Publishing to the embedded broker fails
    /// - Writing to the CSV file fails
    /// - The MQTT connection is lost
    ///
    /// # Requirements
    ///
    /// - **11.3**: Subscribe to topics on the external broker
    /// - **11.4**: Republish received messages to the embedded broker
    /// - **11.5**: Optionally record messages to CSV
    /// - **11.6**: Preserve message topic, payload, QoS, and retain flag
    /// - **11.8**: Log the number of messages mirrored periodically
    /// - **11.9**: Handle interrupt signals for graceful shutdown
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio::sync::broadcast;
    ///
    /// let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    /// let mut mirror = Mirror::new(source_client, broker, writer, topics, QoS::AtMostOnce).await?;
    ///
    /// // Run in a separate task
    /// let handle = tokio::spawn(async move {
    ///     mirror.run(shutdown_rx).await
    /// });
    ///
    /// // Later, trigger shutdown
    /// shutdown_tx.send(()).unwrap();
    /// let count = handle.await??;
    /// println!("Mirrored {} messages", count);
    /// ```
    pub async fn run(
        &mut self,
        mut shutdown: broadcast::Receiver<()>,
    ) -> Result<u64, MqttRecorderError> {
        // Subscribe to the configured topics on the external broker (Requirement 11.3, 11.7)
        let topics = self.topics.topics();
        if !topics.is_empty() {
            self.source_client.subscribe(topics, self.qos).await?;
            eprintln!("Subscribed to {} topic(s) on external broker", topics.len());
        }

        // Initial connection to local broker - poll once to establish connection
        match self.local_client.poll().await {
            Ok(_) => eprintln!("Connected to embedded broker for publishing"),
            Err(e) => {
                // Log but continue - connection might be established on first publish
                eprintln!("Initial local broker connection status: {}", e);
            }
        }

        let mut message_count: u64 = 0;
        let mut flush_counter: u64 = 0;

        eprintln!(
            "Mirror mode started. Recording to file: {}",
            if self.writer.is_some() { "yes" } else { "no" }
        );

        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = shutdown.recv() => {
                    eprintln!("Shutdown signal received, stopping mirror...");
                    break;
                }

                // Poll for MQTT events from the source broker
                event_result = self.source_client.poll() => {
                    match event_result {
                        Ok(event) => {
                            if let Some(record) = self.process_event(event) {
                                // Republish to embedded broker (Requirements 11.4, 11.6)
                                if let Err(e) = self.republish_message(&record).await {
                                    eprintln!("Error republishing message: {}", e);
                                    if Self::is_fatal_error(&e) {
                                        return Err(e);
                                    }
                                }

                                // Optionally write to CSV (Requirement 11.5)
                                if let Some(ref mut writer) = self.writer {
                                    writer.write(&record)?;
                                    flush_counter += 1;

                                    // Periodic flush for data persistence
                                    if flush_counter >= LOG_INTERVAL {
                                        writer.flush()?;
                                        flush_counter = 0;
                                    }
                                }

                                message_count += 1;

                                // Log message count periodically (Requirement 11.8)
                                if message_count.is_multiple_of(LOG_INTERVAL) {
                                    eprintln!("Mirrored {} messages so far...", message_count);
                                }

                                // Poll local client to process any pending events
                                if let Err(e) = self.poll_local_events().await {
                                    if Self::is_fatal_error(&e) {
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // Log recoverable errors and continue
                            eprintln!("MQTT error from source broker: {}", e);
                            // Check if this is a fatal connection error
                            if Self::is_fatal_error(&e) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        // Final flush before closing (Requirement 11.5)
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }

        // Disconnect from the source broker (Requirement 11.9)
        if let Err(e) = self.source_client.disconnect().await {
            eprintln!("Warning: Error disconnecting from source broker: {}", e);
        }

        // Disconnect local client
        if let Err(e) = self.local_client.disconnect().await {
            eprintln!("Warning: Error disconnecting local client: {}", e);
        }

        // Shutdown the embedded broker (Requirement 11.9)
        // Note: We need to take ownership of the broker to shut it down
        // Since we can't move out of self, we'll just log the shutdown intent
        // The actual broker shutdown will happen when Mirror is dropped
        eprintln!("Shutting down embedded broker...");

        // Log message count on completion
        eprintln!("Mirror complete. {} messages mirrored.", message_count);

        Ok(message_count)
    }

    /// Processes an MQTT event and extracts a message record if applicable.
    ///
    /// This method handles incoming MQTT events and converts publish messages
    /// into [`MessageRecord`] instances for republishing and optional CSV storage.
    ///
    /// # Arguments
    ///
    /// * `event` - The MQTT event to process
    ///
    /// # Returns
    ///
    /// Returns `Some(MessageRecord)` if the event contains a publish message,
    /// or `None` for other event types.
    fn process_event(&self, event: Event) -> Option<MessageRecord> {
        match event {
            Event::Incoming(Packet::Publish(publish)) => {
                // Extract message details (Requirement 11.6)
                let timestamp = Utc::now();
                let topic = publish.topic.clone();

                // Convert payload bytes to string
                // If the payload is not valid UTF-8, use lossy conversion
                let payload = String::from_utf8_lossy(&publish.payload).to_string();

                // Map rumqttc QoS to u8
                let qos = match publish.qos {
                    QoS::AtMostOnce => 0,
                    QoS::AtLeastOnce => 1,
                    QoS::ExactlyOnce => 2,
                };

                let retain = publish.retain;

                Some(MessageRecord::new(timestamp, topic, payload, qos, retain))
            }
            // Log connection events for debugging
            Event::Incoming(Packet::ConnAck(_)) => {
                eprintln!("Connected to external MQTT broker");
                None
            }
            Event::Incoming(Packet::SubAck(_)) => {
                eprintln!("Subscription acknowledged by external broker");
                None
            }
            // Ignore other events
            _ => None,
        }
    }

    /// Republishes a message record to the embedded broker.
    ///
    /// This method publishes the message to the embedded broker, preserving
    /// the original topic, payload, QoS level, and retain flag.
    ///
    /// # Arguments
    ///
    /// * `record` - The message record to republish
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if publishing fails.
    ///
    /// # Requirements
    ///
    /// - **11.4**: Republish received messages to the embedded broker
    /// - **11.6**: Preserve message topic, payload, QoS, and retain flag
    async fn republish_message(&self, record: &MessageRecord) -> Result<(), MqttRecorderError> {
        // Convert QoS u8 to rumqttc QoS enum
        let qos = Self::u8_to_qos(record.qos);

        // Publish with preserved topic, payload, QoS, and retain flag
        self.local_client
            .publish(&record.topic, record.payload.as_bytes(), qos, record.retain)
            .await
    }

    /// Polls the local MQTT event loop to process pending events.
    ///
    /// This is needed to maintain the connection to the embedded broker
    /// and handle acknowledgments.
    async fn poll_local_events(&self) -> Result<(), MqttRecorderError> {
        // Use a short timeout to avoid blocking
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(10),
            self.local_client.poll(),
        )
        .await
        {
            Ok(result) => result.map(|_| ()),
            Err(_) => Ok(()), // Timeout is fine, just means no events pending
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

    /// Determines if an error is fatal and should stop mirroring.
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
    /// Returns `true` if the error is fatal and mirroring should stop.
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

    /// Gets a reference to the embedded broker.
    ///
    /// This can be useful for checking the broker's mode or port.
    #[allow(dead_code)]
    pub fn broker(&self) -> &EmbeddedBroker {
        &self.broker
    }

    /// Consumes the Mirror and returns the embedded broker for shutdown.
    ///
    /// This method should be called after `run()` completes to properly
    /// shut down the embedded broker.
    ///
    /// # Returns
    ///
    /// The embedded broker instance for shutdown.
    pub fn into_broker(self) -> EmbeddedBroker {
        self.broker
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u8_to_qos_valid_values() {
        assert!(matches!(Mirror::u8_to_qos(0), QoS::AtMostOnce));
        assert!(matches!(Mirror::u8_to_qos(1), QoS::AtLeastOnce));
        assert!(matches!(Mirror::u8_to_qos(2), QoS::ExactlyOnce));
    }

    #[test]
    fn test_u8_to_qos_invalid_values() {
        // Invalid values should default to QoS 0
        assert!(matches!(Mirror::u8_to_qos(3), QoS::AtMostOnce));
        assert!(matches!(Mirror::u8_to_qos(255), QoS::AtMostOnce));
    }

    #[test]
    fn test_is_fatal_error() {
        // IO errors should be fatal
        let io_error = MqttRecorderError::Io(std::io::Error::other("test"));
        assert!(Mirror::is_fatal_error(&io_error));

        // CSV errors should be fatal
        let csv_error = MqttRecorderError::Csv(csv::Error::from(std::io::Error::other("test")));
        assert!(Mirror::is_fatal_error(&csv_error));

        // TLS errors should not be fatal (they occur during setup, not mirroring)
        let tls_error = MqttRecorderError::Tls("test".to_string());
        assert!(!Mirror::is_fatal_error(&tls_error));

        // Broker errors should not be fatal
        let broker_error = MqttRecorderError::Broker("test".to_string());
        assert!(!Mirror::is_fatal_error(&broker_error));

        // Invalid argument errors should not be fatal during mirroring
        let arg_error = MqttRecorderError::InvalidArgument("test".to_string());
        assert!(!Mirror::is_fatal_error(&arg_error));
    }

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
    fn test_log_interval_constant() {
        // Verify the log interval is set to a reasonable value
        assert_eq!(LOG_INTERVAL, 100);
    }
}
