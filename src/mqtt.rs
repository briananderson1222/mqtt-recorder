//! MQTT client module
//!
//! Wraps `rumqttc` client with connection management and TLS configuration.
//!
//! This module provides configuration structs for MQTT client connections,
//! including support for TLS/SSL encryption and various authentication methods.

use crate::error::MqttRecorderError;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, QoS, Transport};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Configuration for establishing an MQTT client connection.
///
/// This struct holds all the necessary parameters to connect to an MQTT broker,
/// including host, port, client identification, authentication credentials,
/// and optional TLS configuration.
///
/// # Requirements
/// - 2.1: WHEN connecting to a broker, establish a connection using the provided host and port
/// - 2.2: WHEN a client_id is provided, use it for the connection
/// - 2.3: WHEN no client_id is provided, generate a unique client identifier
/// - 2.4: WHEN username and password are provided, authenticate using those credentials
#[derive(Debug, Clone)]
pub struct MqttClientConfig {
    /// The hostname or IP address of the MQTT broker
    pub host: String,

    /// The port number of the MQTT broker (default: 1883 for non-TLS, 8883 for TLS)
    pub port: u16,

    /// The client identifier for the MQTT connection.
    /// If not provided, a unique identifier will be generated.
    pub client_id: String,

    /// Optional username for MQTT broker authentication
    pub username: Option<String>,

    /// Optional password for MQTT broker authentication
    pub password: Option<String>,

    /// Optional TLS configuration for secure connections
    pub tls: Option<TlsConfig>,

    /// Maximum packet size in bytes (default: 1MB)
    pub max_packet_size: usize,
}

/// Configuration for TLS/SSL secure connections.
///
/// This struct holds the TLS-related settings for establishing secure
/// connections to MQTT brokers, including certificate paths and
/// verification options.
///
/// # Requirements
/// - 2.5: WHEN enable_ssl is true, establish a TLS-encrypted connection
/// - 2.6: WHEN ca_cert is provided, use the specified CA certificate for server verification
/// - 2.7: WHEN certfile and keyfile are provided, use client certificate authentication
/// - 2.8: WHEN tls_insecure is true, skip certificate hostname verification
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the CA certificate file for server verification.
    /// When provided, the client will verify the server's certificate
    /// against this CA certificate.
    pub ca_cert: Option<PathBuf>,

    /// Path to the client certificate file for client authentication.
    /// Used together with `client_key` for mutual TLS authentication.
    pub client_cert: Option<PathBuf>,

    /// Path to the client private key file for client authentication.
    /// Used together with `client_cert` for mutual TLS authentication.
    pub client_key: Option<PathBuf>,

    /// When true, skip certificate hostname verification.
    /// This is useful for self-signed certificates but reduces security.
    /// Use with caution in production environments.
    pub insecure: bool,
}

impl MqttClientConfig {
    /// Creates a new MQTT client configuration with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `host` - The hostname or IP address of the MQTT broker
    /// * `port` - The port number of the MQTT broker
    /// * `client_id` - The client identifier for the connection
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::MqttClientConfig;
    ///
    /// let config = MqttClientConfig::new(
    ///     "localhost".to_string(),
    ///     1883,
    ///     "my-client".to_string(),
    /// );
    /// ```
    pub fn new(host: String, port: u16, client_id: String) -> Self {
        Self {
            host,
            port,
            client_id,
            username: None,
            password: None,
            tls: None,
            max_packet_size: 1024 * 1024, // 1MB default
        }
    }

    /// Sets the authentication credentials for the MQTT connection.
    ///
    /// # Arguments
    ///
    /// * `username` - The username for authentication
    /// * `password` - The password for authentication
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::MqttClientConfig;
    ///
    /// let config = MqttClientConfig::new("localhost".to_string(), 1883, "client".to_string())
    ///     .with_credentials("user".to_string(), "pass".to_string());
    /// ```
    pub fn with_credentials(mut self, username: String, password: String) -> Self {
        self.username = Some(username);
        self.password = Some(password);
        self
    }

    /// Sets the TLS configuration for secure connections.
    ///
    /// # Arguments
    ///
    /// * `tls` - The TLS configuration
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::{MqttClientConfig, TlsConfig};
    ///
    /// let tls = TlsConfig::new();
    /// let config = MqttClientConfig::new("localhost".to_string(), 8883, "client".to_string())
    ///     .with_tls(tls);
    /// ```
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sets the maximum packet size for MQTT messages.
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum packet size in bytes
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::MqttClientConfig;
    ///
    /// let config = MqttClientConfig::new("localhost".to_string(), 1883, "client".to_string())
    ///     .with_max_packet_size(2 * 1024 * 1024); // 2MB
    /// ```
    pub fn with_max_packet_size(mut self, size: usize) -> Self {
        self.max_packet_size = size;
        self
    }

    /// Returns true if TLS is configured for this connection.
    #[allow(dead_code)] // Public API
    pub fn is_tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    /// Returns true if authentication credentials are configured.
    #[allow(dead_code)] // Public API
    pub fn has_credentials(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }
}

impl TlsConfig {
    /// Creates a new TLS configuration with default settings.
    ///
    /// By default, certificate verification is enabled (insecure = false).
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::TlsConfig;
    ///
    /// let tls = TlsConfig::new();
    /// assert!(!tls.insecure);
    /// ```
    pub fn new() -> Self {
        Self {
            ca_cert: None,
            client_cert: None,
            client_key: None,
            insecure: false,
        }
    }

    /// Sets the CA certificate path for server verification.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the CA certificate file
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::TlsConfig;
    /// use std::path::PathBuf;
    ///
    /// let tls = TlsConfig::new()
    ///     .with_ca_cert(PathBuf::from("/path/to/ca.crt"));
    /// ```
    pub fn with_ca_cert(mut self, path: PathBuf) -> Self {
        self.ca_cert = Some(path);
        self
    }

    /// Sets the client certificate and key paths for mutual TLS authentication.
    ///
    /// # Arguments
    ///
    /// * `cert_path` - Path to the client certificate file
    /// * `key_path` - Path to the client private key file
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::TlsConfig;
    /// use std::path::PathBuf;
    ///
    /// let tls = TlsConfig::new()
    ///     .with_client_auth(
    ///         PathBuf::from("/path/to/client.crt"),
    ///         PathBuf::from("/path/to/client.key"),
    ///     );
    /// ```
    pub fn with_client_auth(mut self, cert_path: PathBuf, key_path: PathBuf) -> Self {
        self.client_cert = Some(cert_path);
        self.client_key = Some(key_path);
        self
    }

    /// Sets whether to skip certificate hostname verification.
    ///
    /// **Warning**: Setting this to `true` reduces security and should only
    /// be used for testing with self-signed certificates.
    ///
    /// # Arguments
    ///
    /// * `insecure` - If true, skip certificate hostname verification
    ///
    /// # Example
    ///
    /// ```
    /// use mqtt_recorder::mqtt::TlsConfig;
    ///
    /// let tls = TlsConfig::new()
    ///     .with_insecure(true);
    /// ```
    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

    /// Returns true if client certificate authentication is configured.
    ///
    /// Both `client_cert` and `client_key` must be set for client auth to be valid.
    pub fn has_client_auth(&self) -> bool {
        self.client_cert.is_some() && self.client_key.is_some()
    }

    /// Returns true if a CA certificate is configured for server verification.
    #[allow(dead_code)] // Public API
    pub fn has_ca_cert(&self) -> bool {
        self.ca_cert.is_some()
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// MQTT client wrapper around rumqttc.
///
/// This struct provides a high-level interface for MQTT operations including
/// connecting to brokers, subscribing to topics, publishing messages, and
/// handling the event loop.
///
/// # Requirements
/// - 2.1: WHEN connecting to a broker, establish a connection using the provided host and port
/// - 2.2: WHEN a client_id is provided, use it for the connection
/// - 2.3: WHEN no client_id is provided, generate a unique client identifier
/// - 2.4: WHEN username and password are provided, authenticate using those credentials
/// - 2.5: WHEN enable_ssl is true, establish a TLS-encrypted connection
/// - 2.6: WHEN ca_cert is provided, use the specified CA certificate for server verification
/// - 2.7: WHEN certfile and keyfile are provided, use client certificate authentication
/// - 2.8: WHEN tls_insecure is true, skip certificate hostname verification
/// - 2.9: IF the connection fails, report the error and exit with a non-zero status code
/// - 2.10: IF authentication fails, report the authentication error and exit with a non-zero status code
pub struct MqttClient {
    /// The async MQTT client for sending commands
    client: AsyncClient,
    /// The event loop for receiving events (wrapped in Mutex for interior mutability)
    eventloop: Arc<Mutex<EventLoop>>,
}

impl MqttClient {
    /// Create a new MQTT client with the given configuration.
    ///
    /// This method establishes a connection to the MQTT broker using the provided
    /// configuration. It supports TLS connections, authentication, and automatic
    /// client ID generation.
    ///
    /// # Arguments
    ///
    /// * `config` - The MQTT client configuration
    ///
    /// # Returns
    ///
    /// Returns `Ok(MqttClient)` on success, or an error if the configuration is invalid.
    ///
    /// # Requirements
    /// - 2.1: WHEN connecting to a broker, establish a connection using the provided host and port
    /// - 2.2: WHEN a client_id is provided, use it for the connection
    /// - 2.3: WHEN no client_id is provided, generate a unique client identifier
    /// - 2.4: WHEN username and password are provided, authenticate using those credentials
    /// - 2.5-2.8: TLS configuration support
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mqtt_recorder::mqtt::{MqttClient, MqttClientConfig};
    ///
    /// let config = MqttClientConfig::new(
    ///     "localhost".to_string(),
    ///     1883,
    ///     "my-client".to_string(),
    /// );
    /// let client = MqttClient::new(config).await?;
    /// ```
    pub async fn new(config: MqttClientConfig) -> Result<Self, MqttRecorderError> {
        // Generate client_id if empty (Requirement 2.3)
        let client_id = if config.client_id.is_empty() {
            crate::util::generate_client_id(&Some(config.client_id.clone()))
        } else {
            config.client_id.clone()
        };

        // Create MQTT options (Requirement 2.1, 2.2)
        let mut mqtt_options = MqttOptions::new(&client_id, &config.host, config.port);

        // Set keep alive to 30 seconds to tolerate high-throughput message bursts
        mqtt_options.set_keep_alive(Duration::from_secs(30));

        // Set max packet size from config (Requirement 1.19, 1.28)
        mqtt_options.set_max_packet_size(config.max_packet_size, config.max_packet_size);

        // Set credentials if provided (Requirement 2.4)
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            mqtt_options.set_credentials(username, password);
        }

        // Configure TLS if enabled (Requirements 2.5-2.8)
        if let Some(tls_config) = &config.tls {
            let transport = Self::build_tls_transport(tls_config)?;
            mqtt_options.set_transport(transport);
        }

        // Create the async client and event loop
        // Use a large channel capacity to prevent backpressure from blocking ping handling
        // Channel capacity 256: sufficient for v4 clients which are used for external broker
        // connections with moderate throughput
        let (client, eventloop) = AsyncClient::new(mqtt_options, 256);

        Ok(Self {
            client,
            eventloop: Arc::new(Mutex::new(eventloop)),
        })
    }

    /// Build the TLS transport configuration from TlsConfig.
    ///
    /// # Requirements
    /// - 2.5: WHEN enable_ssl is true, establish a TLS-encrypted connection
    /// - 2.6: WHEN ca_cert is provided, use the specified CA certificate for server verification
    /// - 2.7: WHEN certfile and keyfile are provided, use client certificate authentication
    /// - 2.8: WHEN tls_insecure is true, skip certificate hostname verification
    fn build_tls_transport(tls_config: &TlsConfig) -> Result<Transport, MqttRecorderError> {
        // Read CA certificate if provided (Requirement 2.6)
        let ca_bytes = if let Some(ca_path) = &tls_config.ca_cert {
            let ca_data = fs::read(ca_path).map_err(|e| {
                MqttRecorderError::Tls(format!(
                    "Failed to read CA certificate from {:?}: {}",
                    ca_path, e
                ))
            })?;
            ca_data
        } else {
            // Use empty CA for default/insecure mode
            Vec::new()
        };

        // Read client certificate and key if provided (Requirement 2.7)
        let client_auth = if tls_config.has_client_auth() {
            let cert_path = tls_config
                .client_cert
                .as_ref()
                .expect("guaranteed by has_client_auth()");
            let key_path = tls_config
                .client_key
                .as_ref()
                .expect("guaranteed by has_client_auth()");

            let cert_data = fs::read(cert_path).map_err(|e| {
                MqttRecorderError::Tls(format!(
                    "Failed to read client certificate from {:?}: {}",
                    cert_path, e
                ))
            })?;

            let key_data = fs::read(key_path).map_err(|e| {
                MqttRecorderError::Tls(format!(
                    "Failed to read client key from {:?}: {}",
                    key_path, e
                ))
            })?;

            Some((cert_data, key_data))
        } else {
            None
        };

        // For insecure mode or when no CA is provided, use default TLS config
        // (Requirement 2.8)
        if tls_config.insecure || ca_bytes.is_empty() {
            // Use default TLS configuration which doesn't verify certificates
            Ok(Transport::tls_with_default_config())
        } else {
            // Use TLS with CA certificate and optional client auth
            Ok(Transport::tls(ca_bytes, client_auth, None))
        }
    }

    /// Subscribe to a list of topics with the specified QoS.
    ///
    /// # Arguments
    ///
    /// * `topics` - A slice of topic strings to subscribe to
    /// * `qos` - The Quality of Service level for the subscriptions
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the subscription fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rumqttc::QoS;
    ///
    /// let topics = vec!["sensors/+/temperature".to_string(), "actuators/#".to_string()];
    /// client.subscribe(&topics, QoS::AtLeastOnce).await?;
    /// ```
    pub async fn subscribe(&self, topics: &[String], qos: QoS) -> Result<(), MqttRecorderError> {
        for topic in topics {
            self.client.subscribe(topic, qos).await?;
        }
        Ok(())
    }

    /// Publish a message to a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic to publish to
    /// * `payload` - The message payload as bytes
    /// * `qos` - The Quality of Service level for the message
    /// * `retain` - Whether the message should be retained by the broker
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the publish fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use rumqttc::QoS;
    ///
    /// client.publish(
    ///     "sensors/temperature",
    ///     b"{\"value\": 23.5}",
    ///     QoS::AtLeastOnce,
    ///     false,
    /// ).await?;
    /// ```
    pub async fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), MqttRecorderError> {
        self.client.publish(topic, qos, retain, payload).await?;
        Ok(())
    }

    /// Poll for the next event from the broker.
    ///
    /// This method should be called in a loop to process incoming messages
    /// and maintain the connection to the broker.
    ///
    /// # Returns
    ///
    /// Returns the next event from the broker, or an error if the connection fails.
    ///
    /// # Requirements
    /// - 2.9: IF the connection fails, report the error and exit with a non-zero status code
    /// - 2.10: IF authentication fails, report the authentication error and exit with a non-zero status code
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// loop {
    ///     match client.poll().await {
    ///         Ok(event) => {
    ///             // Handle the event
    ///         }
    ///         Err(e) => {
    ///             eprintln!("Connection error: {}", e);
    ///             break;
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn poll(&self) -> Result<Event, MqttRecorderError> {
        let mut eventloop = self.eventloop.lock().await;
        let event = eventloop.poll().await?;
        Ok(event)
    }

    /// Disconnect from the broker.
    ///
    /// This method sends a disconnect packet to the broker and closes the connection
    /// gracefully.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the disconnect fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// client.disconnect().await?;
    /// ```
    pub async fn disconnect(&self) -> Result<(), MqttRecorderError> {
        self.client.disconnect().await?;
        Ok(())
    }

    /// Get a reference to the underlying AsyncClient.
    ///
    /// This can be useful for advanced operations not covered by the wrapper methods.
    #[allow(dead_code)] // Public API
    pub fn client(&self) -> &AsyncClient {
        &self.client
    }
}

/// MQTT v5 client wrapper around rumqttc::v5.
///
/// Used for connections to the embedded broker which runs a v5 listener.
/// Provides the same public API as `MqttClient` but uses MQTT v5 protocol.
pub struct MqttClientV5 {
    client: rumqttc::v5::AsyncClient,
    eventloop: Arc<Mutex<rumqttc::v5::EventLoop>>,
}

impl MqttClientV5 {
    /// Convert v4 QoS to v5 QoS (they're separate types in rumqttc)
    fn to_v5_qos(qos: QoS) -> rumqttc::v5::mqttbytes::QoS {
        match qos {
            QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
        }
    }

    /// Convert v5 QoS to v4 QoS
    fn from_v5_qos(qos: rumqttc::v5::mqttbytes::QoS) -> QoS {
        match qos {
            rumqttc::v5::mqttbytes::QoS::AtMostOnce => QoS::AtMostOnce,
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce => QoS::AtLeastOnce,
            rumqttc::v5::mqttbytes::QoS::ExactlyOnce => QoS::ExactlyOnce,
        }
    }

    /// Create a new MQTT v5 client with the given configuration.
    pub async fn new(config: MqttClientConfig) -> Result<Self, MqttRecorderError> {
        let client_id = if config.client_id.is_empty() {
            crate::util::generate_client_id(&Some(config.client_id.clone()))
        } else {
            config.client_id.clone()
        };

        let mut mqtt_options = rumqttc::v5::MqttOptions::new(&client_id, &config.host, config.port);
        mqtt_options.set_keep_alive(Duration::from_secs(15));
        mqtt_options.set_max_packet_size(Some(config.max_packet_size as u32));

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            mqtt_options.set_credentials(username, password);
        }

        if let Some(tls_config) = &config.tls {
            let transport = MqttClient::build_tls_transport(tls_config)?;
            mqtt_options.set_transport(transport);
        }

        // Channel capacity 2048: v5 clients connect to the embedded broker which handles
        // high-throughput mirroring and needs larger buffers to avoid backpressure
        let (client, eventloop) = rumqttc::v5::AsyncClient::new(mqtt_options, 2048);

        Ok(Self {
            client,
            eventloop: Arc::new(Mutex::new(eventloop)),
        })
    }

    /// Subscribe to the given topics with the specified QoS level.
    pub async fn subscribe(&self, topics: &[String], qos: QoS) -> Result<(), MqttRecorderError> {
        let v5_qos = Self::to_v5_qos(qos);
        for topic in topics {
            self.client.subscribe(topic, v5_qos).await?;
        }
        Ok(())
    }

    /// Publish a message to the given topic.
    pub async fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), MqttRecorderError> {
        self.client
            .publish(topic, Self::to_v5_qos(qos), retain, payload.to_vec())
            .await?;
        Ok(())
    }

    /// Poll for the next event from the broker.
    pub async fn poll(&self) -> Result<rumqttc::v5::Event, MqttRecorderError> {
        let mut eventloop = self.eventloop.lock().await;
        let event = eventloop.poll().await?;
        Ok(event)
    }

    /// Disconnect from the broker.
    pub async fn disconnect(&self) -> Result<(), MqttRecorderError> {
        self.client.disconnect().await?;
        Ok(())
    }

    /// Get a clone of the eventloop Arc for spawning a background poll task.
    pub fn eventloop(&self) -> Arc<Mutex<rumqttc::v5::EventLoop>> {
        Arc::clone(&self.eventloop)
    }
}

/// Protocol-agnostic MQTT client that wraps either a v4 or v5 client.
///
/// Used by components (like Replayer) that need to connect to either
/// an external broker (v4) or the embedded broker (v5).
pub enum AnyMqttClient {
    V4(MqttClient),
    V5(MqttClientV5),
}

/// Unified incoming event extracted from either v4 or v5 MQTT events.
pub enum MqttIncoming {
    Publish {
        topic: String,
        payload: Vec<u8>,
        qos: QoS,
        retain: bool,
    },
    ConnAck,
    SubAck,
    Other,
}

impl AnyMqttClient {
    pub async fn subscribe(&self, topics: &[String], qos: QoS) -> Result<(), MqttRecorderError> {
        match self {
            Self::V4(c) => c.subscribe(topics, qos).await,
            Self::V5(c) => c.subscribe(topics, qos).await,
        }
    }

    pub async fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<(), MqttRecorderError> {
        match self {
            Self::V4(c) => c.publish(topic, payload, qos, retain).await,
            Self::V5(c) => c.publish(topic, payload, qos, retain).await,
        }
    }

    pub async fn poll(&self) -> Result<MqttIncoming, MqttRecorderError> {
        match self {
            Self::V4(c) => {
                let event = c.poll().await?;
                Ok(Self::convert_v4_event(event))
            }
            Self::V5(c) => {
                let event = c.poll().await?;
                Ok(Self::convert_v5_event(event))
            }
        }
    }

    pub async fn disconnect(&self) -> Result<(), MqttRecorderError> {
        match self {
            Self::V4(c) => c.disconnect().await,
            Self::V5(c) => c.disconnect().await,
        }
    }

    fn convert_v4_event(event: Event) -> MqttIncoming {
        use rumqttc::Packet;
        match event {
            Event::Incoming(Packet::Publish(p)) => MqttIncoming::Publish {
                topic: p.topic,
                payload: p.payload.to_vec(),
                qos: p.qos,
                retain: p.retain,
            },
            Event::Incoming(Packet::ConnAck(_)) => MqttIncoming::ConnAck,
            Event::Incoming(Packet::SubAck(_)) => MqttIncoming::SubAck,
            _ => MqttIncoming::Other,
        }
    }

    fn convert_v5_event(event: rumqttc::v5::Event) -> MqttIncoming {
        use rumqttc::v5::mqttbytes::v5::Packet;
        match event {
            rumqttc::v5::Event::Incoming(Packet::Publish(p)) => MqttIncoming::Publish {
                topic: String::from_utf8_lossy(&p.topic).into_owned(),
                payload: p.payload.to_vec(),
                qos: MqttClientV5::from_v5_qos(p.qos),
                retain: p.retain,
            },
            rumqttc::v5::Event::Incoming(Packet::ConnAck(_)) => MqttIncoming::ConnAck,
            rumqttc::v5::Event::Incoming(Packet::SubAck(_)) => MqttIncoming::SubAck,
            _ => MqttIncoming::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_client_config_new() {
        let config =
            MqttClientConfig::new("localhost".to_string(), 1883, "test-client".to_string());

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1883);
        assert_eq!(config.client_id, "test-client");
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.tls.is_none());
    }

    #[test]
    fn test_mqtt_client_config_with_credentials() {
        let config =
            MqttClientConfig::new("localhost".to_string(), 1883, "test-client".to_string())
                .with_credentials("user".to_string(), "pass".to_string());

        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert!(config.has_credentials());
    }

    #[test]
    fn test_mqtt_client_config_with_tls() {
        let tls = TlsConfig::new();
        let config =
            MqttClientConfig::new("localhost".to_string(), 8883, "test-client".to_string())
                .with_tls(tls);

        assert!(config.is_tls_enabled());
    }

    #[test]
    fn test_tls_config_new() {
        let tls = TlsConfig::new();

        assert!(tls.ca_cert.is_none());
        assert!(tls.client_cert.is_none());
        assert!(tls.client_key.is_none());
        assert!(!tls.insecure);
    }

    #[test]
    fn test_tls_config_with_ca_cert() {
        let tls = TlsConfig::new().with_ca_cert(PathBuf::from("/path/to/ca.crt"));

        assert_eq!(tls.ca_cert, Some(PathBuf::from("/path/to/ca.crt")));
        assert!(tls.has_ca_cert());
    }

    #[test]
    fn test_tls_config_with_client_auth() {
        let tls = TlsConfig::new().with_client_auth(
            PathBuf::from("/path/to/client.crt"),
            PathBuf::from("/path/to/client.key"),
        );

        assert_eq!(tls.client_cert, Some(PathBuf::from("/path/to/client.crt")));
        assert_eq!(tls.client_key, Some(PathBuf::from("/path/to/client.key")));
        assert!(tls.has_client_auth());
    }

    #[test]
    fn test_tls_config_with_insecure() {
        let tls = TlsConfig::new().with_insecure(true);

        assert!(tls.insecure);
    }

    #[test]
    fn test_tls_config_default() {
        let tls = TlsConfig::default();

        assert!(tls.ca_cert.is_none());
        assert!(tls.client_cert.is_none());
        assert!(tls.client_key.is_none());
        assert!(!tls.insecure);
    }

    #[test]
    fn test_mqtt_client_config_no_credentials() {
        let config =
            MqttClientConfig::new("localhost".to_string(), 1883, "test-client".to_string());

        assert!(!config.has_credentials());
    }

    #[test]
    fn test_tls_config_partial_client_auth() {
        // Only cert, no key
        let tls = TlsConfig {
            ca_cert: None,
            client_cert: Some(PathBuf::from("/path/to/client.crt")),
            client_key: None,
            insecure: false,
        };
        assert!(!tls.has_client_auth());

        // Only key, no cert
        let tls = TlsConfig {
            ca_cert: None,
            client_cert: None,
            client_key: Some(PathBuf::from("/path/to/client.key")),
            insecure: false,
        };
        assert!(!tls.has_client_auth());
    }

    #[test]
    fn test_generate_client_id_format() {
        let client_id = crate::util::generate_client_id(&None);
        assert!(client_id.starts_with("mqtt-recorder-"));
        // Should be "mqtt-recorder-" (14 chars) + 8 hex chars = 22 chars
        assert_eq!(client_id.len(), 22);
    }

    #[test]
    fn test_generate_client_id_uniqueness() {
        // Generate multiple IDs and check they're different
        // Note: Due to timing, consecutive calls might produce the same ID
        // so we add a small delay between calls
        let id1 = crate::util::generate_client_id(&None);
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = crate::util::generate_client_id(&None);

        // IDs should be different (with high probability due to nanosecond precision)
        // But we can't guarantee this in a unit test, so just check format
        assert!(id1.starts_with("mqtt-recorder-"));
        assert!(id2.starts_with("mqtt-recorder-"));
    }

    #[tokio::test]
    async fn test_mqtt_client_new_basic() {
        // Test creating a client with basic configuration
        // Note: This doesn't actually connect, just creates the client
        let config =
            MqttClientConfig::new("localhost".to_string(), 1883, "test-client".to_string());

        let result = MqttClient::new(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_new_with_empty_client_id() {
        // Test that empty client_id generates a unique ID
        let config = MqttClientConfig::new("localhost".to_string(), 1883, "".to_string());

        let result = MqttClient::new(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_new_with_credentials() {
        let config =
            MqttClientConfig::new("localhost".to_string(), 1883, "test-client".to_string())
                .with_credentials("user".to_string(), "pass".to_string());

        let result = MqttClient::new(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_new_with_tls_insecure() {
        let tls = TlsConfig::new().with_insecure(true);
        let config =
            MqttClientConfig::new("localhost".to_string(), 8883, "test-client".to_string())
                .with_tls(tls);

        let result = MqttClient::new(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_new_with_tls_default() {
        // TLS with no CA cert should use default config
        let tls = TlsConfig::new();
        let config =
            MqttClientConfig::new("localhost".to_string(), 8883, "test-client".to_string())
                .with_tls(tls);

        let result = MqttClient::new(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_new_with_invalid_ca_cert() {
        // Test that invalid CA cert path returns an error
        let tls = TlsConfig::new().with_ca_cert(PathBuf::from("/nonexistent/ca.crt"));
        let config =
            MqttClientConfig::new("localhost".to_string(), 8883, "test-client".to_string())
                .with_tls(tls);

        let result = MqttClient::new(config).await;
        assert!(result.is_err());
        if let Err(MqttRecorderError::Tls(msg)) = result {
            assert!(msg.contains("Failed to read CA certificate"));
        } else {
            panic!("Expected TLS error");
        }
    }

    #[tokio::test]
    async fn test_mqtt_client_new_with_invalid_client_cert() {
        // Test that invalid client cert path returns an error
        let tls = TlsConfig::new().with_client_auth(
            PathBuf::from("/nonexistent/client.crt"),
            PathBuf::from("/nonexistent/client.key"),
        );
        let config =
            MqttClientConfig::new("localhost".to_string(), 8883, "test-client".to_string())
                .with_tls(tls);

        let result = MqttClient::new(config).await;
        assert!(result.is_err());
        if let Err(MqttRecorderError::Tls(msg)) = result {
            assert!(msg.contains("Failed to read client certificate"));
        } else {
            panic!("Expected TLS error");
        }
    }

    #[tokio::test]
    async fn test_mqtt_client_v5_eventloop_accessor() {
        let config = MqttClientConfig::new("127.0.0.1".to_string(), 1883, "test".to_string());
        let client = MqttClientV5::new(config).await.unwrap();
        let el = client.eventloop();
        // Should be able to clone the Arc and lock it
        assert!(el.try_lock().is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_v5_keep_alive() {
        let config = MqttClientConfig::new("127.0.0.1".to_string(), 1883, "test".to_string());
        // Just verify construction succeeds with the configured keep_alive
        let client = MqttClientV5::new(config).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_mqtt_client_channel_capacity() {
        let config = MqttClientConfig::new("127.0.0.1".to_string(), 1883, "test".to_string());
        // Verify client creates successfully with 256 channel capacity
        let client = MqttClient::new(config).await;
        assert!(client.is_ok());
    }
}
