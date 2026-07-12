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

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;
use tokio::time::Duration;
use tracing::{error, info};

use crate::csv_handler::{CsvReader, MessageRecordBytes};
use crate::error::MqttRecorderError;
use crate::mqtt::AnyMqttClient;
use crate::tui::TuiState;

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
/// * `default_speed` - Playback speed when no TuiState is available (from CLI --speed)
///
/// # Requirements
///
/// - **5.1**: Read messages from CSV and publish to broker
/// - **5.5**: Maintain relative timing between messages
/// - **5.7**: Support loop replay
/// - **5.11**: Handle interrupt signals for graceful shutdown
pub struct Replayer {
    /// The MQTT client for broker communication.
    client: AnyMqttClient,
    /// The CSV reader for reading recorded messages.
    reader: CsvReader,
    /// Whether to loop replay continuously.
    loop_replay: bool,
    /// Playback speed for non-TUI mode (from CLI --speed).
    default_speed: f32,
}

impl Replayer {
    /// Creates a new Replayer with the given components.
    ///
    /// # Arguments
    ///
    /// * `client` - An MQTT client connected to the broker
    /// * `reader` - A CSV reader for the input file
    /// * `loop_replay` - Whether to loop replay continuously
    /// * `speed` - Playback speed multiplier (0 = max speed, 1.0 = real-time)
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
    /// let replayer = Replayer::new(client, reader, false, 1.0);
    /// ```
    pub fn new(client: AnyMqttClient, reader: CsvReader, loop_replay: bool, speed: f32) -> Self {
        Self {
            client,
            reader,
            loop_replay,
            default_speed: speed,
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
    /// * `tui_state` - Optional TUI state for interactive mode
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
    ///     replayer.run(shutdown_rx, None).await
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
        tui_state: Option<Arc<TuiState>>,
    ) -> Result<u64, MqttRecorderError> {
        let mut message_count: u64 = 0;
        let mut previous_timestamp: Option<DateTime<Utc>> = None;

        info!(
            "Starting replay (loop: {})",
            if self.loop_replay {
                "enabled"
            } else {
                "disabled"
            }
        );

        // Spawn background task to drive the MQTT event loop continuously.
        // This handles sending queued publishes over the network, receiving
        // acks, and keepalive — preventing channel backpressure from blocking
        // publish() in the main loop.
        let poll_handle = self.client.spawn_poll_task();

        // Wait briefly for connection to establish
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(ref state) = tui_state {
            state.set_source_connected(true);
        }
        info!("Connected to MQTT broker");

        loop {
            // Check if TUI requested quit
            if let Some(ref state) = tui_state {
                if state.is_quit_requested() {
                    break;
                }
            }

            // Read as raw bytes so binary payloads are republished exactly as
            // recorded (the String-based path is lossy for non-UTF-8 data).
            let record_result = self.reader.read_next_bytes();

            match record_result {
                Some(Ok(record)) => {
                    // Read current speed: TuiState (runtime adjustable) or CLI default
                    let speed = tui_state
                        .as_ref()
                        .map(|s| s.get_playback_speed())
                        .unwrap_or(self.default_speed);

                    // Calculate delay based on timestamp difference (Requirement 5.5)
                    if let Some(prev_ts) = previous_timestamp {
                        let delay = Self::calculate_delay(&prev_ts, &record.timestamp, speed);
                        if delay > Duration::ZERO {
                            // Sleep for the delay; background task keeps connection alive
                            tokio::select! {
                                _ = shutdown.recv() => {
                                    info!("Shutdown signal received during delay, stopping replayer...");
                                    // Fall through to the common epilogue so the
                                    // MQTT client still disconnects gracefully.
                                    break;
                                }
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                    }

                    previous_timestamp = Some(record.timestamp);

                    // Publish the message (Requirements 5.2, 5.3, 5.4)
                    match self.publish_message(&record).await {
                        Ok(()) => {
                            message_count += 1;
                            if let Some(ref state) = tui_state {
                                state.increment_replayed();
                                state.increment_published();
                            }
                        }
                        Err(e) => {
                            error!("Error publishing message: {}", e);
                            if crate::util::is_fatal_error(&e, false) {
                                poll_handle.abort();
                                return Err(e);
                            }
                        }
                    }

                    // Periodically yield and check shutdown
                    if message_count.is_multiple_of(crate::util::FLUSH_INTERVAL) {
                        tokio::task::yield_now().await;

                        match shutdown.try_recv() {
                            Ok(_) => {
                                info!("Shutdown signal received, stopping replayer...");
                                break;
                            }
                            Err(broadcast::error::TryRecvError::Empty) => {}
                            Err(broadcast::error::TryRecvError::Closed) => {
                                info!("Shutdown channel closed, stopping replayer...");
                                break;
                            }
                            Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                        }
                    }
                }
                Some(Err(e)) => {
                    error!("Error reading CSV record: {}", e);
                    poll_handle.abort();
                    return Err(e);
                }
                None => {
                    let looping = tui_state
                        .as_ref()
                        .map(|s| s.is_playback_looping())
                        .unwrap_or(self.loop_replay);
                    if looping {
                        info!(
                            "End of file reached, restarting replay... ({} messages so far)",
                            message_count
                        );
                        self.reader.reset()?;
                        previous_timestamp = None;
                    } else if tui_state.is_some() {
                        // Wait for user to enable looping or quit
                        if let Some(ref state) = tui_state {
                            state
                                .playback_finished
                                .store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        loop {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            if let Some(ref state) = tui_state {
                                if state.is_quit_requested() {
                                    break;
                                }
                                if state.is_playback_looping() {
                                    state
                                        .playback_finished
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    self.reader.reset()?;
                                    previous_timestamp = None;
                                    break;
                                }
                            }
                            if shutdown.try_recv().is_ok() {
                                break;
                            }
                        }
                        // If quit was requested, exit the outer loop
                        if tui_state
                            .as_ref()
                            .map(|s| s.is_quit_requested())
                            .unwrap_or(false)
                        {
                            break;
                        }
                        // If looping wasn't enabled (shutdown), exit
                        if !tui_state
                            .as_ref()
                            .map(|s| s.is_playback_looping())
                            .unwrap_or(false)
                        {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        // Give background poll task a moment to flush remaining publishes
        tokio::time::sleep(Duration::from_millis(crate::util::POLL_BATCH_INTERVAL_MS)).await;
        poll_handle.abort();

        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(crate::util::DISCONNECT_TIMEOUT_SECS),
            self.client.disconnect(),
        )
        .await;

        info!("Replay complete. {} messages replayed.", message_count);

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
    async fn publish_message(&self, record: &MessageRecordBytes) -> Result<(), MqttRecorderError> {
        // Convert QoS u8 to rumqttc QoS enum (Requirement 5.3)
        let qos = crate::util::u8_to_qos(record.qos);

        // Publish with preserved topic, payload, QoS, and retain flag
        // (Requirements 5.2, 5.3, 5.4)
        // Note: Base64 decoding is handled by CsvReader if configured (Requirement 5.6)
        self.client
            .publish(&record.topic, &record.payload, qos, record.retain)
            .await
    }

    /// Calculates the delay between two timestamps, adjusted by playback speed.
    ///
    /// # Arguments
    ///
    /// * `previous` - The timestamp of the previous message
    /// * `current` - The timestamp of the current message
    /// * `speed` - Playback speed multiplier (0 = no delay, 1.0 = real-time, 2.0 = 2x faster)
    ///
    /// # Returns
    ///
    /// Returns the duration to wait before publishing the current message.
    /// Returns `Duration::ZERO` if speed is 0 or timestamps are non-increasing.
    ///
    /// # Requirements
    ///
    /// - **5.5**: Maintain relative timing between messages based on recorded timestamps
    fn calculate_delay(previous: &DateTime<Utc>, current: &DateTime<Utc>, speed: f32) -> Duration {
        if speed == 0.0 {
            return Duration::ZERO;
        }
        let diff = *current - *previous;
        if diff.num_milliseconds() > 0 {
            Duration::from_millis((diff.num_milliseconds() as f64 / speed as f64) as u64)
        } else {
            Duration::ZERO
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

        let delay = Replayer::calculate_delay(&ts1, &ts2, 1.0);
        assert_eq!(delay, Duration::from_secs(1));
    }

    #[test]
    fn test_calculate_delay_milliseconds() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let ts2 = ts1
            .checked_add_signed(chrono::Duration::milliseconds(500))
            .unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2, 1.0);
        assert_eq!(delay, Duration::from_millis(500));
    }

    #[test]
    fn test_calculate_delay_zero_for_same_timestamp() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let delay = Replayer::calculate_delay(&ts, &ts, 1.0);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_calculate_delay_zero_for_earlier_timestamp() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 1).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2, 1.0);
        assert_eq!(delay, Duration::ZERO);
    }

    #[test]
    fn test_calculate_delay_large_difference() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 11, 0, 0).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2, 1.0);
        assert_eq!(delay, Duration::from_secs(3600)); // 1 hour
    }

    #[test]
    fn test_calculate_delay_speed_2x() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 2).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2, 2.0);
        assert_eq!(delay, Duration::from_secs(1));
    }

    #[test]
    fn test_calculate_delay_speed_zero_max() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 5).unwrap();

        let delay = Replayer::calculate_delay(&ts1, &ts2, 0.0);
        assert_eq!(delay, Duration::ZERO);
    }
}
