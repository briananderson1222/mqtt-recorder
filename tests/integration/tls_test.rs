//! Integration tests for the client TLS paths against a TLS-enabled rumqttd
//! broker with a self-signed certificate (generated in-process via rcgen).
//!
//! Covers the three verification modes of `build_tls_transport`:
//! - `--tls-insecure`: accept the self-signed certificate
//! - default verification (no CA): reject the self-signed certificate
//! - `--ca-cert` pinning: accept the certificate it is pinned to

use mqtt_recorder::mqtt::{MqttClientConfig, MqttClientV5, TlsConfig};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

fn get_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Generate a self-signed certificate valid for localhost/127.0.0.1 and
/// write the PEM cert + key into the temp dir.
fn write_self_signed_cert(dir: &TempDir) -> (PathBuf, PathBuf) {
    let certified = rcgen::generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("cert generation");
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
    std::fs::write(&key_path, certified.signing_key.serialize_pem()).expect("write key");
    (cert_path, key_path)
}

/// Start a TLS-only rumqttd v5 broker on the given port (mirrors the shape of
/// EmbeddedBroker::create_config, plus TLS). The thread leaks like the other
/// integration-test brokers; rumqttd has no shutdown API.
fn start_tls_broker(port: u16, cert: &Path, key: &Path) {
    let router = RouterConfig {
        max_connections: 100,
        max_outgoing_packet_count: 200,
        max_segment_size: 10 * 1024 * 1024,
        max_segment_count: 10,
        ..Default::default()
    };
    let connections = ConnectionSettings {
        connection_timeout_ms: 60000,
        max_payload_size: 1024 * 1024,
        max_inflight_count: 100,
        auth: None,
        external_auth: None,
        dynamic_filters: true,
    };
    let server = ServerSettings {
        name: "tls-test".to_string(),
        listen: SocketAddr::new([127, 0, 0, 1].into(), port),
        tls: Some(rumqttd::TlsConfig::Rustls {
            capath: None,
            certpath: cert.display().to_string(),
            keypath: key.display().to_string(),
        }),
        next_connection_delay_ms: 1,
        connections,
    };
    let mut v5 = HashMap::new();
    v5.insert("1".to_string(), server);
    let config = Config {
        id: 0,
        router,
        v4: None,
        v5: Some(v5),
        ws: None,
        cluster: None,
        console: None,
        bridge: None,
        prometheus: None,
        metrics: None,
    };
    std::thread::spawn(move || {
        let mut broker = Broker::new(config);
        let _ = broker.start();
    });
}

async fn wait_for_port(port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("TLS broker did not become ready on port {}", port);
}

/// Connect with the given TLS config and drive the event loop until CONNACK
/// or the first error.
async fn try_connect(port: u16, id: &str, tls: TlsConfig) -> Result<(), String> {
    let config =
        MqttClientConfig::new("127.0.0.1".to_string(), port, id.to_string()).with_tls(tls);
    let client = MqttClientV5::new(config).await.map_err(|e| e.to_string())?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        match tokio::time::timeout_at(deadline, client.poll()).await {
            Ok(Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(
                _,
            )))) => return Ok(()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => return Err("timed out waiting for CONNACK".to_string()),
        }
    }
}

/// `--tls-insecure` must accept a self-signed certificate the system does
/// not trust (the connection stays encrypted; only verification is skipped).
#[tokio::test]
async fn test_tls_insecure_accepts_self_signed_cert() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = write_self_signed_cert(&dir);
    let port = get_free_port();
    start_tls_broker(port, &cert, &key);
    wait_for_port(port).await;

    let result = try_connect(port, "tls-insecure", TlsConfig::new().with_insecure(true)).await;
    assert!(
        result.is_ok(),
        "--tls-insecure should accept a self-signed cert, got: {:?}",
        result
    );
}

/// Default verification (TLS enabled, no CA, not insecure) must REJECT a
/// self-signed certificate: this is the safety property that makes
/// --tls-insecure meaningful.
#[tokio::test]
async fn test_tls_default_verification_rejects_self_signed_cert() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = write_self_signed_cert(&dir);
    let port = get_free_port();
    start_tls_broker(port, &cert, &key);
    wait_for_port(port).await;

    let result = try_connect(port, "tls-strict", TlsConfig::new()).await;
    assert!(
        result.is_err(),
        "default verification must reject a self-signed cert, but connected"
    );
}

/// `--ca-cert` pinned to the self-signed certificate must connect: the cert
/// acts as its own CA.
#[tokio::test]
async fn test_tls_ca_cert_pinning_accepts_matching_cert() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key) = write_self_signed_cert(&dir);
    let port = get_free_port();
    start_tls_broker(port, &cert, &key);
    wait_for_port(port).await;

    let result = try_connect(
        port,
        "tls-pinned",
        TlsConfig::new().with_ca_cert(cert.clone()),
    )
    .await;
    assert!(
        result.is_ok(),
        "CA-pinned connection should succeed, got: {:?}",
        result
    );
}
