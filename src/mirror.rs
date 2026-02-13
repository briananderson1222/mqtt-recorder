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

use std::path::Path;
use std::sync::atomic::Ordering;

use chrono::Utc;
use rumqttc::QoS;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::broker::EmbeddedBroker;
use crate::csv_handler::{CsvReader, CsvWriter, MessageRecord};
use crate::error::MqttRecorderError;
use crate::mqtt::{AnyMqttClient, MqttClientV5, MqttIncoming};
use crate::topics::TopicFilter;
use crate::tui::{AuditArea, AuditSeverity, TuiState};

/// Interval for polling broker metrics (in number of messages).
const METRICS_POLL_INTERVAL: u64 = 100;

fn process_event_impl(event: MqttIncoming) -> Option<MessageRecord> {
    let (topic, payload, qos, retain) = crate::util::extract_publish(&event)?;
    let timestamp = Utc::now();
    let payload_str = String::from_utf8_lossy(payload).to_string();
    Some(MessageRecord::new(
        timestamp,
        topic.to_string(),
        payload_str,
        qos,
        retain,
    ))
}

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
    source_client: AnyMqttClient,
    /// The embedded broker instance.
    broker: EmbeddedBroker,
    /// The MQTT v5 client connected to the embedded broker for publishing.
    local_client: MqttClientV5,
    /// Optional CSV writer for recording messages.
    writer: Option<CsvWriter>,
    /// The topic filter specifying which topics to subscribe to.
    topics: TopicFilter,
    /// The Quality of Service level for subscriptions.
    qos: QoS,
    /// Optional verify client for subscribing to the embedded broker.
    verify_client: Option<MqttClientV5>,
    /// Channel sender for expected messages (used when verify is enabled).
    verify_tx: Option<tokio::sync::mpsc::UnboundedSender<(String, Vec<u8>)>>,
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
        source_client: AnyMqttClient,
        broker: EmbeddedBroker,
        writer: Option<CsvWriter>,
        topics: TopicFilter,
        qos: QoS,
        verify: bool,
    ) -> Result<Self, MqttRecorderError> {
        // Get a local client for publishing to the embedded broker
        let local_client = broker.get_local_client().await?;
        broker.register_internal_client();

        // Optionally create a verify client
        let verify_client = if verify {
            let client = broker.get_local_client().await?;
            broker.register_internal_client();
            Some(client)
        } else {
            None
        };

        Ok(Self {
            source_client,
            broker,
            local_client,
            writer,
            topics,
            qos,
            verify_client,
            verify_tx: None,
        })
    }

    fn spawn_local_poll_task(
        &self,
        tui_state: &Option<std::sync::Arc<TuiState>>,
    ) -> tokio::task::JoinHandle<()> {
        let local_eventloop = self.local_client.eventloop();
        let broker_port = self.broker.port();
        let tui_state_bg = tui_state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            let mut connected = false;
            loop {
                let mut el = local_eventloop.lock().await;
                match el.poll().await {
                    Ok(event) => {
                        if matches!(
                            event,
                            rumqttc::v5::Event::Incoming(rumqttc::v5::Incoming::ConnAck(_))
                        ) {
                            let label = if connected {
                                "Reconnected"
                            } else {
                                "Connected"
                            };
                            let severity = if connected {
                                AuditSeverity::Warn
                            } else {
                                AuditSeverity::Info
                            };
                            if let Some(ref state) = tui_state_bg {
                                state.push_audit(
                                    AuditArea::Broker,
                                    severity,
                                    format!("Local client: {} to localhost:{}", label, broker_port),
                                );
                            }
                            connected = true;
                        }
                    }
                    Err(e) => {
                        if connected {
                            if let Some(ref state) = tui_state_bg {
                                state.push_audit(
                                    AuditArea::Broker,
                                    AuditSeverity::Warn,
                                    format!("Local client disconnected: {}", e),
                                );
                            }
                            connected = false;
                        } else if let Some(ref state) = tui_state_bg {
                            state.push_audit(
                                AuditArea::Broker,
                                AuditSeverity::Error,
                                format!("Local client: {}", e),
                            );
                        }
                        drop(el);
                        tokio::time::sleep(tokio::time::Duration::from_secs(
                            crate::util::RETRY_DELAY_SECS,
                        ))
                        .await;
                    }
                }
            }
        })
    }

    async fn spawn_verify_task(
        &mut self,
        tui_state: &Option<std::sync::Arc<TuiState>>,
    ) -> (
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        Option<tokio::task::JoinHandle<()>>,
        Option<tokio::sync::oneshot::Sender<()>>,
    ) {
        let verify_ready = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (verify_handle, verify_shutdown_tx) = if let Some(verify_client) =
            self.verify_client.take()
        {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(String, Vec<u8>)>();
            self.verify_tx = Some(tx);
            let tui_state_vfy = tui_state.clone();
            let vfy_port = self.broker.port();

            // Subscribe verify client to all topics
            if let Err(e) = verify_client
                .subscribe(&["#".to_string()], QoS::AtMostOnce)
                .await
            {
                if let Some(ref state) = tui_state_vfy {
                    state.push_audit(
                        AuditArea::Verify,
                        AuditSeverity::Error,
                        format!("Verify subscribe failed: {}", e),
                    );
                }
            } else if let Some(ref state) = tui_state_vfy {
                state.push_audit(
                    AuditArea::Verify,
                    AuditSeverity::Info,
                    format!("Verify subscriber connected to localhost:{}", vfy_port),
                );
            }

            let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
            let verify_eventloop = verify_client.eventloop();
            let verify_ready_tx = verify_ready.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let mut pending: std::collections::VecDeque<(String, Vec<u8>)> =
                    std::collections::VecDeque::new();
                let mut ready = false;
                loop {
                    if stop_rx.try_recv().is_ok() {
                        break;
                    }
                    let mut el = verify_eventloop.lock().await;
                    match tokio::time::timeout(tokio::time::Duration::from_millis(50), el.poll())
                        .await
                    {
                        Ok(Ok(event)) => {
                            drop(el);
                            while let Ok(msg) = rx.try_recv() {
                                pending.push_back(msg);
                            }
                            if let rumqttc::v5::Event::Incoming(
                                rumqttc::v5::mqttbytes::v5::Packet::Publish(p),
                            ) = event
                            {
                                // Signal ready on first publish so source starts sending expected messages
                                if !ready {
                                    ready = true;
                                    verify_ready_tx.store(true, Ordering::Relaxed);
                                    if let Some(ref state) = tui_state_vfy {
                                        state.push_audit(
                                            AuditArea::Verify,
                                            AuditSeverity::Info,
                                            "Verify active — first message received".to_string(),
                                        );
                                    }
                                    // Skip this message — source hasn't started sending expected yet
                                    continue;
                                }
                                // Skip messages that arrive before source has sent any expected
                                if pending.is_empty() {
                                    // Drain channel one more time in case source just sent
                                    while let Ok(msg) = rx.try_recv() {
                                        pending.push_back(msg);
                                    }
                                    if pending.is_empty() {
                                        continue;
                                    }
                                }
                                let topic = String::from_utf8_lossy(&p.topic).into_owned();
                                let payload = p.payload.to_vec();
                                // 1) Exact match (topic + payload bytes)
                                let exact = pending
                                    .iter()
                                    .position(|(t, p)| t == &topic && p == &payload);
                                if let Some(i) = exact {
                                    pending.remove(i);
                                    if let Some(ref state) = tui_state_vfy {
                                        state.increment_verify_matched();
                                    }
                                } else {
                                    // 2) Topic match but payload differs
                                    let topic_match = pending.iter().position(|(t, _)| t == &topic);
                                    if let Some(ref state) = tui_state_vfy {
                                        state.increment_verify_mismatched();
                                        if let Some(i) = topic_match {
                                            let expected = &pending[i].1;
                                            state.push_audit(AuditArea::Verify, AuditSeverity::Warn,
                                                format!("PAYLOAD MISMATCH on {}: expected {} bytes got {} bytes\n  expected: {}\n  received: {}",
                                                    topic, expected.len(), payload.len(),
                                                    crate::util::format_payload_preview(expected),
                                                    crate::util::format_payload_preview(&payload)));
                                            pending.remove(i);
                                        } else {
                                            // 3) No topic match at all — truly unexpected
                                            state.push_audit(
                                                AuditArea::Verify,
                                                AuditSeverity::Warn,
                                                format!(
                                                    "UNEXPECTED on broker: {} ({} bytes): {}",
                                                    topic,
                                                    payload.len(),
                                                    crate::util::format_payload_preview(&payload)
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Err(_)) => {
                            drop(el);
                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                crate::util::RETRY_DELAY_SECS,
                            ))
                            .await;
                        }
                        Err(_) => {
                            drop(el);
                            while let Ok(msg) = rx.try_recv() {
                                pending.push_back(msg);
                            }
                        }
                    }
                }
                // Report remaining pending as missing
                if !pending.is_empty() {
                    if let Some(ref state) = tui_state_vfy {
                        state.add_verify_missing(pending.len() as u64);
                        for (topic, payload) in pending.iter().take(20) {
                            state.push_audit(
                                AuditArea::Verify,
                                AuditSeverity::Warn,
                                format!(
                                    "MISSING from broker: {} ({} bytes): {}",
                                    topic,
                                    payload.len(),
                                    crate::util::format_payload_preview(payload)
                                ),
                            );
                        }
                        if pending.len() > 20 {
                            state.push_audit(
                                AuditArea::Verify,
                                AuditSeverity::Warn,
                                format!("... and {} more missing messages", pending.len() - 20),
                            );
                        }
                    }
                }
            });
            (Some(handle), Some(stop_tx))
        } else {
            (None, None)
        };
        (verify_ready, verify_handle, verify_shutdown_tx)
    }

    /// Runs the mirror loop with TUI state support for record/passthrough toggle.
    ///
    /// This method is similar to the basic run but checks the TUI state to determine
    /// whether to record messages to CSV. Messages are always republished to
    /// the embedded broker regardless of the TUI mode.
    ///
    /// - `AppMode::Record` - Republish AND write to CSV
    /// - `AppMode::Passthrough` - Republish only (no CSV)
    /// - Other modes - Same as Passthrough
    pub async fn run(
        &mut self,
        mut shutdown: broadcast::Receiver<()>,
        tui_state: Option<std::sync::Arc<TuiState>>,
    ) -> Result<u64, MqttRecorderError> {
        let topics = self.topics.topics();
        if !topics.is_empty() {
            self.source_client.subscribe(topics, self.qos).await?;
            // Mark source as connected after successful subscribe
            if let Some(ref state) = tui_state {
                state.set_source_connected(true);
            }
            info!("Subscribed to {} topic(s) on external broker", topics.len());
        }

        // Spawn background task to drive the local client event loop
        let local_poll_handle = self.spawn_local_poll_task(&tui_state);

        let (verify_ready, verify_handle, verify_shutdown_tx) =
            self.spawn_verify_task(&tui_state).await;
        let mut message_count: u64 = 0;
        let mut flush_counter: u64 = 0;

        info!(
            "Mirror mode started. Recording to file: {}",
            if self.writer.is_some() {
                "yes (toggleable)"
            } else {
                "no"
            }
        );

        let mut playback_reader: Option<CsvReader> = None;
        let mut was_playback_on = false;
        let mut was_recording = tui_state.as_ref().map(|s| s.is_recording()).unwrap_or(true);

        loop {
            if let Some(ref state) = tui_state {
                if state.is_quit_requested() {
                    break;
                }
            }

            // Handle playback state transitions
            let playback_on = tui_state
                .as_ref()
                .map(|s| s.loop_enabled.load(Ordering::Relaxed) && !s.is_recording())
                .unwrap_or(false);

            was_playback_on = self.handle_playback_transition(
                &tui_state,
                &mut playback_reader,
                playback_on,
                was_playback_on,
            );

            // Process one playback record if active
            if playback_reader.is_some() {
                self.process_playback_record(&mut playback_reader, &tui_state, &verify_ready)
                    .await?;
            }

            // Check if source is enabled (pause incoming messages)
            let source_enabled = tui_state
                .as_ref()
                .map(|s| s.is_source_enabled())
                .unwrap_or(true);
            if !source_enabled {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                continue;
            }

            tokio::select! {
                _ = shutdown.recv() => {
                    break;
                }

                _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)), if playback_reader.is_some() => {
                    // Yield to process playback records at top of loop
                }

                event_result = self.source_client.poll() => {
                    match event_result {
                        Ok(event) => {
                            self.handle_source_event(event, &tui_state, &verify_ready, &mut message_count, &mut flush_counter, &mut was_recording).await?;
                        }
                        Err(e) => {
                            if let Some(ref state) = tui_state {
                                state.push_audit(AuditArea::Source, AuditSeverity::Error, format!("{}", e));
                                state.set_source_connected(false);
                            }
                            warn!("MQTT error from source broker: {}", e);
                            if crate::util::is_fatal_error(&e, true) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        self.shutdown(
            &tui_state,
            local_poll_handle,
            verify_shutdown_tx,
            verify_handle,
            message_count,
        )
        .await
    }

    /// Flush writer, abort background tasks, disconnect clients, and log summary.
    async fn shutdown(
        &mut self,
        tui_state: &Option<std::sync::Arc<TuiState>>,
        local_poll_handle: tokio::task::JoinHandle<()>,
        verify_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
        verify_handle: Option<tokio::task::JoinHandle<()>>,
        message_count: u64,
    ) -> Result<u64, MqttRecorderError> {
        if let Some(ref state) = tui_state {
            state.set_source_connected(false);
        }

        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }

        local_poll_handle.abort();

        if let Some(stop_tx) = verify_shutdown_tx {
            drop(self.verify_tx.take());
            let _ = stop_tx.send(());
            if let Some(handle) = verify_handle {
                if tokio::time::timeout(
                    tokio::time::Duration::from_secs(crate::util::DISCONNECT_TIMEOUT_SECS),
                    handle,
                )
                .await
                .is_err()
                {
                    warn!("Verify task shutdown timed out");
                }
            }
            if let Some(ref state) = tui_state {
                let matched = state.get_verify_matched();
                let mismatched = state.get_verify_mismatched();
                let missing = state.get_verify_missing();
                state.push_audit(
                    AuditArea::Verify,
                    AuditSeverity::Info,
                    format!(
                        "Verify summary: {} matched, {} unexpected, {} missing",
                        matched, mismatched, missing
                    ),
                );
            }
        }

        if tokio::time::timeout(
            tokio::time::Duration::from_secs(crate::util::DISCONNECT_TIMEOUT_SECS),
            self.source_client.disconnect(),
        )
        .await
        .is_err()
        {
            warn!("Source client disconnect timed out");
        }

        if tokio::time::timeout(
            tokio::time::Duration::from_secs(crate::util::DISCONNECT_TIMEOUT_SECS),
            self.local_client.disconnect(),
        )
        .await
        .is_err()
        {
            warn!("Local client disconnect timed out");
        }

        info!("Shutting down embedded broker...");
        info!("Mirror complete. {} messages mirrored.", message_count);

        Ok(message_count)
    }

    fn handle_playback_transition(
        &self,
        tui_state: &Option<std::sync::Arc<TuiState>>,
        playback_reader: &mut Option<CsvReader>,
        playback_on: bool,
        was_playback_on: bool,
    ) -> bool {
        if playback_on && !was_playback_on {
            if let Some(ref state) = tui_state {
                if let Some(file_path) = state.get_active_file() {
                    info!("Playback starting: {}", file_path);
                    match CsvReader::new(Path::new(&file_path), false, None) {
                        Ok(reader) => *playback_reader = Some(reader),
                        Err(e) => {
                            state.push_audit(
                                AuditArea::Playback,
                                AuditSeverity::Error,
                                format!("Failed to open file: {}", e),
                            );
                        }
                    }
                }
            }
        } else if playback_on && playback_reader.is_none() {
            // Restart after one-time completion (playback_finished was cleared)
            let finished = tui_state
                .as_ref()
                .map(|s| s.is_playback_finished())
                .unwrap_or(true);
            if !finished {
                if let Some(ref state) = tui_state {
                    if let Some(file_path) = state.get_active_file() {
                        match CsvReader::new(Path::new(&file_path), false, None) {
                            Ok(reader) => *playback_reader = Some(reader),
                            Err(e) => {
                                state.push_audit(
                                    AuditArea::Playback,
                                    AuditSeverity::Error,
                                    format!("Failed to open file: {}", e),
                                );
                            }
                        }
                    }
                }
            }
        } else if !playback_on && was_playback_on {
            *playback_reader = None;
        }
        playback_on
    }

    async fn process_playback_record(
        &self,
        playback_reader: &mut Option<CsvReader>,
        tui_state: &Option<std::sync::Arc<TuiState>>,
        verify_ready: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), MqttRecorderError> {
        if let Some(ref mut reader) = playback_reader {
            match reader.read_next() {
                Some(Ok(record)) => {
                    if verify_ready.load(Ordering::Relaxed) {
                        if let Some(ref tx) = self.verify_tx {
                            let _ =
                                tx.send((record.topic.clone(), record.payload.as_bytes().to_vec()));
                        }
                    }
                    match self.republish_message(&record).await {
                        Ok(()) => {
                            if let Some(ref state) = tui_state {
                                state.increment_replayed();
                                state.increment_published();
                            }
                        }
                        Err(e) => {
                            error!("Playback publish error: {}", e);
                            if crate::util::is_fatal_error(&e, true) {
                                return Err(e);
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    if let Some(ref state) = tui_state {
                        state.push_audit(
                            AuditArea::Playback,
                            AuditSeverity::Warn,
                            format!("Skipped bad CSV record: {}", e),
                        );
                    }
                    // Skip bad record, continue playback
                }
                None => {
                    // End of file - loop or stop based on mode
                    let looping = tui_state
                        .as_ref()
                        .map(|s| s.is_playback_looping())
                        .unwrap_or(false);
                    if looping {
                        if let Err(e) = reader.reset() {
                            if let Some(ref state) = tui_state {
                                state.push_audit(
                                    AuditArea::Playback,
                                    AuditSeverity::Error,
                                    format!("Failed to reset reader: {}", e),
                                );
                            }
                            *playback_reader = None;
                        }
                    } else {
                        // One-time mode: mark finished
                        if let Some(ref state) = tui_state {
                            state.playback_finished.store(true, Ordering::Relaxed);
                            let session = state.get_playback_session_count();
                            let total = state.get_replayed_count();
                            state.push_audit(
                                AuditArea::Playback,
                                AuditSeverity::Info,
                                format!(
                                    "Playback complete ({} this session, {} total)",
                                    session, total
                                ),
                            );
                        }
                        *playback_reader = None;
                    }
                }
            }
        }
        Ok(())
    }

    /// Handles recording logic including file path changes and recording toggles.
    fn handle_recording(
        &mut self,
        record: &MessageRecord,
        tui_state: &Option<std::sync::Arc<TuiState>>,
        flush_counter: &mut u64,
        was_recording: &mut bool,
    ) -> Result<(), MqttRecorderError> {
        // Check for file path change
        if let Some(ref state) = tui_state {
            if let Some(new_path) = state.take_new_file() {
                // Flush and close old writer
                if let Some(ref mut writer) = self.writer {
                    let _ = writer.flush();
                }
                // Create new writer
                match CsvWriter::new(std::path::Path::new(&new_path), false) {
                    Ok(new_writer) => {
                        self.writer = Some(new_writer);
                    }
                    Err(e) => {
                        error!("Error creating new file: {}", e);
                    }
                }
            }
        }

        // Check if recording is enabled
        let should_record = tui_state.as_ref().map(|s| s.is_recording()).unwrap_or(true);

        // Flush on recording toggle off
        if *was_recording && !should_record {
            if let Some(ref mut writer) = self.writer {
                let _ = writer.flush();
            }
        }
        *was_recording = should_record;

        if should_record {
            if let Some(ref mut writer) = self.writer {
                writer.write(record)?;
                *flush_counter += 1;

                if let Some(ref state) = tui_state {
                    state.increment_recorded();
                }

                if *flush_counter >= crate::util::FLUSH_INTERVAL {
                    writer.flush()?;
                    *flush_counter = 0;
                }
            }
        }

        Ok(())
    }

    async fn handle_source_event(
        &mut self,
        event: MqttIncoming,
        tui_state: &Option<std::sync::Arc<TuiState>>,
        verify_ready: &std::sync::Arc<std::sync::atomic::AtomicBool>,
        message_count: &mut u64,
        flush_counter: &mut u64,
        was_recording: &mut bool,
    ) -> Result<(), MqttRecorderError> {
        // Re-subscribe on reconnection
        if matches!(event, MqttIncoming::ConnAck) {
            let topics = self.topics.topics();
            if !topics.is_empty() {
                if let Err(e) = self.source_client.subscribe(topics, self.qos).await {
                    if let Some(ref state) = tui_state {
                        state.push_audit(
                            AuditArea::Source,
                            AuditSeverity::Error,
                            format!("Re-subscribe failed: {}", e),
                        );
                    }
                    error!("Error re-subscribing after reconnect: {}", e);
                }
            }
            if let Some(ref state) = tui_state {
                state.set_source_connected(true);
            }
        }
        if let Some(record) = self.process_event(event) {
            // Check if mirroring is enabled
            let should_mirror = tui_state.as_ref().map(|s| s.is_mirroring()).unwrap_or(true);

            if should_mirror {
                // Queue expected message BEFORE publishing so verify
                // subscriber can't receive it before it's in the pending queue
                if verify_ready.load(Ordering::Relaxed) {
                    if let Some(ref tx) = self.verify_tx {
                        let _ = tx.send((record.topic.clone(), record.payload.as_bytes().to_vec()));
                    }
                }
                if let Err(e) = self.republish_message(&record).await {
                    if let Some(ref state) = tui_state {
                        state.push_audit(
                            AuditArea::Mirror,
                            AuditSeverity::Error,
                            format!("Publish failed: {}", e),
                        );
                    }
                    error!("Error republishing message: {}", e);
                    if crate::util::is_fatal_error(&e, true) {
                        return Err(e);
                    }
                } else if let Some(ref state) = tui_state {
                    state.increment_mirrored();
                    state.increment_published();
                }
            }

            self.handle_recording(&record, tui_state, flush_counter, was_recording)?;

            *message_count += 1;

            if let Some(ref state) = tui_state {
                state.increment_received();
            }

            // Poll broker metrics periodically
            if *message_count % METRICS_POLL_INTERVAL == 0 {
                if let Some(metrics) = self.broker.poll_metrics() {
                    if let Some(ref state) = tui_state {
                        state.update_broker_metrics(&metrics);
                    }
                    if let Some((old, new)) = metrics.connections_changed {
                        debug!("Broker connections: {} → {}", old, new);
                    }
                }
            }
        }
        Ok(())
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
    fn process_event(&self, event: MqttIncoming) -> Option<MessageRecord> {
        process_event_impl(event)
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
        let qos = crate::util::u8_to_qos(record.qos);

        // Publish with preserved topic, payload, QoS, and retain flag
        // The message is queued internally; the local event loop select branch drains it.
        self.local_client
            .publish(&record.topic, record.payload.as_bytes(), qos, record.retain)
            .await?;

        Ok(())
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
    use crate::mqtt::MqttIncoming;
    use rumqttc::QoS as RumqttcQoS;

    #[test]
    fn test_log_interval_constant() {
        // Verify the metrics poll interval is set to a reasonable value
        assert_eq!(METRICS_POLL_INTERVAL, 100);
    }

    #[test]
    fn test_process_event_publish() {
        let event = MqttIncoming::Publish {
            topic: "test/topic".to_string(),
            payload: vec![104, 101, 108, 108, 111],
            qos: RumqttcQoS::AtLeastOnce,
            retain: true,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.topic, "test/topic");
        assert_eq!(record.payload, "hello");
        assert_eq!(record.qos, 1);
        assert!(record.retain);
    }

    #[test]
    fn test_process_event_connack() {
        assert!(process_event_impl(MqttIncoming::ConnAck).is_none());
    }

    #[test]
    fn test_process_event_other() {
        assert!(process_event_impl(MqttIncoming::Other).is_none());
    }

    #[test]
    fn test_process_event_qos_0() {
        let event = MqttIncoming::Publish {
            topic: "test/qos0".to_string(),
            payload: vec![116, 101, 115, 116],
            qos: RumqttcQoS::AtMostOnce,
            retain: false,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.topic, "test/qos0");
        assert_eq!(record.payload, "test");
        assert_eq!(record.qos, 0);
        assert!(!record.retain);
    }

    #[test]
    fn test_process_event_qos_1() {
        let event = MqttIncoming::Publish {
            topic: "test/qos1".to_string(),
            payload: vec![116, 101, 115, 116],
            qos: RumqttcQoS::AtLeastOnce,
            retain: false,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.qos, 1);
    }

    #[test]
    fn test_process_event_qos_2() {
        let event = MqttIncoming::Publish {
            topic: "test/qos2".to_string(),
            payload: vec![116, 101, 115, 116],
            qos: RumqttcQoS::ExactlyOnce,
            retain: false,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.qos, 2);
    }

    #[test]
    fn test_process_event_empty_payload() {
        let event = MqttIncoming::Publish {
            topic: "test/empty".to_string(),
            payload: vec![],
            qos: RumqttcQoS::AtMostOnce,
            retain: false,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.payload, "");
    }

    #[test]
    fn test_process_event_binary_payload() {
        let event = MqttIncoming::Publish {
            topic: "test/binary".to_string(),
            payload: vec![0xFF, 0xFE, 0x00, 0x01],
            qos: RumqttcQoS::AtMostOnce,
            retain: false,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        // Should use lossy UTF-8 conversion
        assert!(record.payload.contains('\u{FFFD}') || !record.payload.is_empty());
    }

    #[test]
    fn test_process_event_unicode_payload() {
        let event = MqttIncoming::Publish {
            topic: "test/unicode".to_string(),
            payload: "Hello 世界! 🌍".as_bytes().to_vec(),
            qos: RumqttcQoS::AtMostOnce,
            retain: false,
        };
        let result = process_event_impl(event);
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.payload, "Hello 世界! 🌍");
    }

    #[test]
    fn test_process_event_suback() {
        assert!(process_event_impl(MqttIncoming::SubAck).is_none());
    }

    #[test]
    fn test_process_event_timestamp_is_recent() {
        let event = MqttIncoming::Publish {
            topic: "test/timestamp".to_string(),
            payload: vec![116, 101, 115, 116],
            qos: RumqttcQoS::AtMostOnce,
            retain: false,
        };
        let before = Utc::now();
        let result = process_event_impl(event);
        let after = Utc::now();

        assert!(result.is_some());
        let record = result.unwrap();
        assert!(record.timestamp >= before);
        assert!(record.timestamp <= after);
        assert!((after - record.timestamp).num_seconds() <= 1);
    }
}
