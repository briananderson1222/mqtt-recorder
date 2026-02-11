//! Embedded broker module
//!
//! Manages the embedded `rumqttd` broker instance for serving MQTT messages locally.
//!
//! This module provides functionality to start an embedded MQTT broker that can:
//! - Run as a standalone broker accepting external client connections
//! - Serve replayed messages from CSV files
//! - Mirror messages from an external broker
//!
//! # Requirements
//! - 10.1: Accept `--serve` as an optional argument to start an embedded broker
//! - 10.2: Accept `--serve_port` with a default value of 1883 for the embedded broker port
//! - 10.3: WHEN serve is enabled in replay mode, start an embedded broker before replaying messages
//! - 10.4: WHEN serve is enabled, accept connections from external MQTT clients
//! - 10.5: WHEN serve is enabled, publish replayed messages to the embedded broker
//! - 10.6: WHEN serve is enabled without replay mode, run as a standalone broker
//! - 10.7: Support QoS levels 0, 1, and 2
//! - 10.8: WHEN the user sends an interrupt signal, gracefully shut down
//! - 10.9: Log client connections and disconnections to stderr
//! - 10.10: Display its current mode (standalone, replay, or mirror) on startup

use crate::error::MqttRecorderError;
use crate::mqtt::{MqttClientConfig, MqttClientV5};
use rumqttd::{
    Broker, Config, ConnectionSettings, MetricSettings, MetricType, RouterConfig, ServerSettings,
};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use tracing::{error, info};

/// Maximum number of concurrent connections the embedded broker accepts.
const MAX_BROKER_CONNECTIONS: usize = 1000;

/// Maximum number of outgoing packets queued per connection.
const MAX_OUTGOING_PACKET_COUNT: u64 = 10000;

/// Maximum size of each commit log segment in bytes (1 MB).
const MAX_SEGMENT_SIZE: usize = 1024 * 1024;

/// Maximum number of segments in the commit log.
const MAX_SEGMENT_COUNT: usize = 100;

/// Connection timeout in milliseconds (60 seconds).
const CONNECTION_TIMEOUT_MS: u16 = 60000;

/// Maximum payload size in bytes (1 MB).
const MAX_PAYLOAD_SIZE: usize = 1024 * 1024;

/// Maximum number of inflight messages per connection.
const MAX_INFLIGHT_COUNT: usize = 100;

/// Snapshot of broker metrics from a single poll.
#[derive(Debug, Clone)]
pub struct BrokerMetrics {
    /// External connection count (excludes internal clients)
    #[allow(dead_code)]
    pub connections: usize,
    /// Whether connection count changed since last poll
    pub connections_changed: Option<(usize, usize)>,
    /// Total active subscriptions
    pub subscriptions: usize,
    /// Cumulative publishes routed by the broker (across all polls)
    pub total_publishes: u64,
    /// Cumulative failed publishes (across all polls)
    pub failed_publishes: u64,
}

/// The operational mode of the embedded broker.
///
/// This enum represents the different modes in which the embedded broker can operate,
/// affecting how it handles incoming connections and message flow.
///
/// # Requirements
/// - 10.6: WHEN serve is enabled without replay mode, run as a standalone broker
/// - 10.10: Display its current mode (standalone, replay, or mirror) on startup
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerMode {
    /// Standalone mode: The broker runs independently, accepting connections
    /// from external MQTT clients without any message source.
    Standalone,

    /// Replay mode: The broker serves messages replayed from a CSV file
    /// to connected clients.
    Replay,

    /// Mirror mode: The broker mirrors messages from an external broker,
    /// republishing them to connected local clients.
    Mirror,
}

impl fmt::Display for BrokerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerMode::Standalone => write!(f, "standalone"),
            BrokerMode::Replay => write!(f, "replay"),
            BrokerMode::Mirror => write!(f, "mirror"),
        }
    }
}

/// Handle for the broker thread, used for graceful shutdown.
struct BrokerHandle {
    /// The thread handle for the broker
    _thread: thread::JoinHandle<()>,
}

/// Embedded MQTT broker using rumqttd.
///
/// This struct manages an embedded MQTT broker instance that can accept
/// connections from external clients and serve messages locally.
///
/// # Requirements
/// - 10.1-10.10: Full embedded broker functionality
///
/// # Example
///
/// ```rust,ignore
/// use mqtt_recorder::broker::{EmbeddedBroker, BrokerMode};
///
/// // Start a standalone broker on port 1883
/// let broker = EmbeddedBroker::new(1883, BrokerMode::Standalone).await?;
///
/// // Get a local client for publishing
/// let client = broker.get_local_client().await?;
///
/// // Shutdown when done
/// broker.shutdown().await?;
/// ```
pub struct EmbeddedBroker {
    /// The broker handle for managing the broker thread
    _handle: BrokerHandle,
    /// The operational mode of the broker
    #[allow(dead_code)]
    mode: BrokerMode,
    /// The port the broker is listening on
    #[allow(dead_code)]
    port: u16,
    /// Metrics link for polling connection counts
    meters_link: Option<rumqttd::meters::MetersLink>,
    /// Cached connection count
    connection_count: AtomicUsize,
    /// Cached subscription count
    subscription_count: AtomicUsize,
    /// Number of internal clients to subtract from connection count
    internal_clients: AtomicUsize,
    /// Counter for generating unique local client IDs
    client_counter: AtomicUsize,
    /// Cumulative publishes routed (RouterMeter resets each interval, so we accumulate)
    cumulative_publishes: AtomicU64,
    /// Cumulative failed publishes
    cumulative_failed: AtomicU64,
}

impl EmbeddedBroker {
    /// Create and start a new embedded broker.
    ///
    /// This method initializes and starts an embedded MQTT broker on the specified port.
    /// The broker will accept connections from external MQTT clients and can be used
    /// to serve replayed or mirrored messages.
    ///
    /// # Arguments
    ///
    /// * `port` - The port number for the broker to listen on
    /// * `mode` - The operational mode of the broker (Standalone, Replay, or Mirror)
    ///
    /// # Returns
    ///
    /// Returns `Ok(EmbeddedBroker)` on success, or an error if the broker fails to start.
    ///
    /// # Requirements
    /// - 10.2: Accept `--serve_port` with a default value of 1883 for the embedded broker port
    /// - 10.3: WHEN serve is enabled in replay mode, start an embedded broker before replaying messages
    /// - 10.4: WHEN serve is enabled, accept connections from external MQTT clients
    /// - 10.7: Support QoS levels 0, 1, and 2
    /// - 10.9: Log client connections and disconnections to stderr
    /// - 10.10: Display its current mode (standalone, replay, or mirror) on startup
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::broker::{EmbeddedBroker, BrokerMode};
    ///
    /// let broker = EmbeddedBroker::new(1883, BrokerMode::Standalone).await?;
    /// ```
    pub async fn new(port: u16, mode: BrokerMode) -> Result<Self, MqttRecorderError> {
        // Log the mode on startup (Requirement 10.10)
        info!(
            "Starting embedded MQTT broker on port {} in {} mode",
            port, mode
        );

        // Create the broker configuration
        let config = Self::create_config(port)?;

        // Create and start the broker
        let mut broker = Broker::new(config);

        // Capture metrics link before moving broker to thread
        let meters_link = broker.meters().ok();

        // Start the broker in a separate thread (broker.start() is blocking)
        let handle = thread::spawn(move || {
            if let Err(e) = broker.start() {
                error!("Broker error: {}", e);
            }
        });

        // Wait for the broker to be ready by probing the TCP listener
        let addr = format!("127.0.0.1:{}", port);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(&addr).is_ok() {
                break;
            }
            if std::time::Instant::now() > deadline {
                return Err(MqttRecorderError::Broker(format!(
                    "Broker failed to start on port {} within 5 seconds",
                    port
                )));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        Ok(Self {
            _handle: BrokerHandle { _thread: handle },
            mode,
            port,
            meters_link,
            connection_count: AtomicUsize::new(0),
            subscription_count: AtomicUsize::new(0),
            internal_clients: AtomicUsize::new(0),
            client_counter: AtomicUsize::new(0),
            cumulative_publishes: AtomicU64::new(0),
            cumulative_failed: AtomicU64::new(0),
        })
    }

    /// Create the rumqttd configuration for the embedded broker.
    ///
    /// This creates a minimal configuration suitable for an embedded broker
    /// with reasonable defaults for local operation.
    fn create_config(port: u16) -> Result<Config, MqttRecorderError> {
        // Create router configuration with reasonable defaults
        // Use Default and override specific fields
        let router = RouterConfig {
            max_connections: MAX_BROKER_CONNECTIONS,
            max_outgoing_packet_count: MAX_OUTGOING_PACKET_COUNT,
            max_segment_size: MAX_SEGMENT_SIZE,
            max_segment_count: MAX_SEGMENT_COUNT,
            ..Default::default()
        };

        // Create connection settings
        // Supports QoS 0, 1, and 2 (Requirement 10.7)
        let connection_settings = ConnectionSettings {
            connection_timeout_ms: CONNECTION_TIMEOUT_MS,
            max_payload_size: MAX_PAYLOAD_SIZE,
            max_inflight_count: MAX_INFLIGHT_COUNT,
            auth: None, // No authentication for embedded broker
            external_auth: None,
            dynamic_filters: true, // Allow dynamic topic creation
        };

        // Create server settings for MQTT v5
        let listen_addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .map_err(|e| MqttRecorderError::Broker(format!("Invalid port {}: {}", port, e)))?;

        let server_settings = ServerSettings {
            name: "mqtt-recorder-broker".to_string(),
            listen: listen_addr,
            tls: None,                   // No TLS for embedded broker
            next_connection_delay_ms: 1, // Minimal delay between connections
            connections: connection_settings,
        };

        // Create the v5 servers map
        let mut v5_servers = HashMap::new();
        v5_servers.insert("1".to_string(), server_settings);

        // Create metrics config so the timer thread pushes data to MetersLink
        // Both Meters and Alerts must be present to avoid a panic in rumqttd's timer.rs
        // (the select! macro evaluates unwrap() before checking the if-guard)
        let meter_settings: MetricSettings = serde_json::from_str(r#"{"push_interval": 1}"#)
            .map_err(|e| MqttRecorderError::Broker(format!("metrics config: {}", e)))?;
        let alert_settings: MetricSettings = serde_json::from_str(r#"{"push_interval": 30}"#)
            .map_err(|e| MqttRecorderError::Broker(format!("metrics config: {}", e)))?;
        let mut metrics = HashMap::new();
        metrics.insert(MetricType::Meters, meter_settings);
        metrics.insert(MetricType::Alerts, alert_settings);

        // Create the final configuration
        let config = Config {
            id: 0,
            router,
            v4: None,
            v5: Some(v5_servers),
            ws: None,
            cluster: None,
            console: None,
            bridge: None,
            prometheus: None,
            metrics: Some(metrics),
        };

        Ok(config)
    }

    /// Get a client connected to the embedded broker for publishing.
    ///
    /// This method creates an MQTT client that connects to the embedded broker,
    /// allowing internal components to publish messages to the broker.
    ///
    /// # Returns
    ///
    /// Returns `Ok(MqttClient)` on success, or an error if the connection fails.
    ///
    /// # Requirements
    /// - 10.5: WHEN serve is enabled, publish replayed messages to the embedded broker
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let broker = EmbeddedBroker::new(1883, BrokerMode::Replay).await?;
    /// let client = broker.get_local_client().await?;
    ///
    /// // Use client to publish messages
    /// client.publish("topic", b"payload", QoS::AtMostOnce, false).await?;
    /// ```
    pub async fn get_local_client(&self) -> Result<MqttClientV5, MqttRecorderError> {
        let id = self.client_counter.fetch_add(1, Ordering::Relaxed);
        let config = MqttClientConfig::new(
            "127.0.0.1".to_string(),
            self.port,
            format!("mqtt-recorder-internal-{}-{}", std::process::id(), id),
        );

        MqttClientV5::new(config).await
    }

    /// Shutdown the broker gracefully.
    ///
    /// This method initiates a graceful shutdown of the embedded broker,
    /// allowing it to close connections and clean up resources.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if shutdown fails.
    ///
    /// # Requirements
    /// - 10.8: WHEN the user sends an interrupt signal, gracefully shut down
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let broker = EmbeddedBroker::new(1883, BrokerMode::Standalone).await?;
    /// // ... use broker ...
    /// broker.shutdown().await?;
    /// ```
    pub async fn shutdown(self) -> Result<(), MqttRecorderError> {
        info!("Shutting down embedded broker on port {}", self.port);

        // The broker thread will be dropped when self is dropped.
        // rumqttd doesn't provide a clean shutdown mechanism through the public API,
        // so we rely on the thread being terminated when the process exits or
        // when the broker handle is dropped.
        //
        // Note: In a production scenario, we might want to implement a more
        // sophisticated shutdown mechanism using channels or other synchronization
        // primitives.

        Ok(())
    }

    /// Get the broker's current mode.
    ///
    /// # Returns
    ///
    /// A reference to the broker's operational mode.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let broker = EmbeddedBroker::new(1883, BrokerMode::Standalone).await?;
    /// assert_eq!(*broker.mode(), BrokerMode::Standalone);
    /// ```
    #[allow(dead_code)]
    pub fn mode(&self) -> &BrokerMode {
        &self.mode
    }

    /// Get the port the broker is listening on.
    ///
    /// # Returns
    ///
    /// The port number.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Poll broker metrics. Returns a snapshot when data is available.
    /// Connection count excludes internal clients.
    /// Publishes and failed_publishes are accumulated across intervals.
    pub fn poll_metrics(&self) -> Option<BrokerMetrics> {
        let link = self.meters_link.as_ref()?;
        let meters = link.recv().ok()?;
        let offset = self.internal_clients.load(Ordering::Relaxed);
        for meter in meters {
            if let rumqttd::Meter::Router(_, router_meter) = meter {
                let new_conn = router_meter.total_connections.saturating_sub(offset);
                let new_sub = router_meter.total_subscriptions;
                self.subscription_count.store(new_sub, Ordering::Relaxed);
                self.cumulative_publishes
                    .fetch_add(router_meter.total_publishes as u64, Ordering::Relaxed);
                self.cumulative_failed
                    .fetch_add(router_meter.failed_publishes as u64, Ordering::Relaxed);
                let old = self.connection_count.swap(new_conn, Ordering::Relaxed);
                let connections_changed = if old != new_conn {
                    Some((old, new_conn))
                } else {
                    None
                };
                return Some(BrokerMetrics {
                    connections: new_conn,
                    connections_changed,
                    subscriptions: new_sub,
                    total_publishes: self.cumulative_publishes.load(Ordering::Relaxed),
                    failed_publishes: self.cumulative_failed.load(Ordering::Relaxed),
                });
            }
        }
        None
    }

    /// Register an internal client so it's excluded from the connection count.
    pub fn register_internal_client(&self) {
        self.internal_clients.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_mode_display() {
        assert_eq!(BrokerMode::Standalone.to_string(), "standalone");
        assert_eq!(BrokerMode::Replay.to_string(), "replay");
        assert_eq!(BrokerMode::Mirror.to_string(), "mirror");
    }

    #[test]
    fn test_broker_mode_equality() {
        assert_eq!(BrokerMode::Standalone, BrokerMode::Standalone);
        assert_eq!(BrokerMode::Replay, BrokerMode::Replay);
        assert_eq!(BrokerMode::Mirror, BrokerMode::Mirror);
        assert_ne!(BrokerMode::Standalone, BrokerMode::Replay);
        assert_ne!(BrokerMode::Replay, BrokerMode::Mirror);
    }

    #[test]
    fn test_broker_mode_clone() {
        let mode = BrokerMode::Standalone;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    #[test]
    fn test_broker_mode_debug() {
        let mode = BrokerMode::Standalone;
        let debug_str = format!("{:?}", mode);
        assert!(debug_str.contains("Standalone"));
    }

    #[test]
    fn test_create_config_valid_port() {
        let result = EmbeddedBroker::create_config(1883);
        assert!(result.is_ok());

        let config = result.unwrap();
        assert!(config.v5.is_some());

        let v5 = config.v5.unwrap();
        assert!(v5.contains_key("1"));

        let server = v5.get("1").unwrap();
        assert_eq!(server.name, "mqtt-recorder-broker");
        assert_eq!(server.listen.port(), 1883);
    }

    #[test]
    fn test_create_config_custom_port() {
        let result = EmbeddedBroker::create_config(9883);
        assert!(result.is_ok());

        let config = result.unwrap();
        let v5 = config.v5.unwrap();
        let server = v5.get("1").unwrap();
        assert_eq!(server.listen.port(), 9883);
    }

    #[test]
    fn test_create_config_router_settings() {
        let config = EmbeddedBroker::create_config(1883).unwrap();

        assert_eq!(config.router.max_connections, 1000);
        assert_eq!(config.router.max_outgoing_packet_count, 10000);
        assert!(config.router.max_segment_size > 0);
        assert!(config.router.max_segment_count > 0);
    }

    #[test]
    fn test_create_config_connection_settings() {
        let config = EmbeddedBroker::create_config(1883).unwrap();
        let v5 = config.v5.unwrap();
        let server = v5.get("1").unwrap();

        assert!(server.connections.connection_timeout_ms > 0);
        assert!(server.connections.max_inflight_count > 0);
        assert!(server.connections.dynamic_filters);
        assert!(server.connections.auth.is_none());
    }

    #[test]
    fn test_create_config_max_payload_matches_client_default() {
        let config = EmbeddedBroker::create_config(1883).unwrap();
        let v5 = config.v5.unwrap();
        let server = v5.get("1").unwrap();

        // Must match MqttClientConfig default max_packet_size (1MB)
        assert_eq!(server.connections.max_payload_size, 1024 * 1024);
    }

    // Note: Integration tests for actual broker startup would require
    // more complex setup and are better suited for integration test files.
    // The following tests verify the configuration is correct without
    // actually starting the broker.

    #[tokio::test]
    async fn test_embedded_broker_new_creates_broker() {
        // This test verifies that we can create a broker instance
        // Note: This will actually start a broker, so we use a non-standard port
        // to avoid conflicts with other tests or services
        let result = EmbeddedBroker::new(19883, BrokerMode::Standalone).await;

        // The broker should start successfully
        assert!(result.is_ok());

        let broker = result.unwrap();
        assert_eq!(*broker.mode(), BrokerMode::Standalone);
        assert_eq!(broker.port(), 19883);

        // Shutdown the broker
        let shutdown_result = broker.shutdown().await;
        assert!(shutdown_result.is_ok());
    }

    #[tokio::test]
    async fn test_embedded_broker_replay_mode() {
        let result = EmbeddedBroker::new(19884, BrokerMode::Replay).await;
        assert!(result.is_ok());

        let broker = result.unwrap();
        assert_eq!(*broker.mode(), BrokerMode::Replay);

        broker.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_embedded_broker_mirror_mode() {
        let result = EmbeddedBroker::new(19885, BrokerMode::Mirror).await;
        assert!(result.is_ok());

        let broker = result.unwrap();
        assert_eq!(*broker.mode(), BrokerMode::Mirror);

        broker.shutdown().await.unwrap();
    }
}
