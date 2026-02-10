//! Integration tests for recorder mode using embedded broker

use mqtt_recorder::broker::{BrokerMode, EmbeddedBroker};
use mqtt_recorder::csv_handler::{CsvReader, CsvWriter};
use mqtt_recorder::mqtt::{MqttClient, MqttClientConfig};
use mqtt_recorder::recorder::Recorder;
use mqtt_recorder::topics::TopicFilter;
use rumqttc::QoS;

use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::broadcast;
use tokio::time::timeout;

/// Helper: create an MQTT client connected to the given port
async fn make_client(port: u16, id: &str) -> MqttClient {
    MqttClient::new(MqttClientConfig::new(
        "127.0.0.1".to_string(),
        port,
        id.to_string(),
    ))
    .await
    .expect("Failed to create client")
}

/// Helper: publish a text message and poll to flush
async fn publish(client: &MqttClient, topic: &str, payload: &[u8], qos: QoS, retain: bool) {
    client
        .publish(topic, payload, qos, retain)
        .await
        .expect("Failed to publish");
    // Poll multiple times to ensure message is sent
    for _ in 0..3 {
        let _ = client.poll().await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Test that recorder captures published messages into CSV.
///
/// 1. Start embedded broker on port 18850
/// 2. Create recorder subscribing to all topics, writing to temp CSV
/// 3. Publish 3 messages to the broker
/// 4. Shutdown recorder
/// 5. Verify CSV contains 3 records with correct topic and payload
#[tokio::test]
async fn test_recorder_captures_published_messages() {
    let _broker = EmbeddedBroker::new(18850, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("output.csv");

    let recorder_client = make_client(18850, "recorder").await;
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let topics = TopicFilter::wildcard();
    let mut recorder = Recorder::new(recorder_client, writer, topics, QoS::AtMostOnce).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { recorder.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher = make_client(18850, "publisher").await;
    for i in 0..3 {
        publish(
            &publisher,
            &format!("test/topic/{}", i),
            format!("payload-{}", i).as_bytes(),
            QoS::AtMostOnce,
            false,
        )
        .await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = shutdown_tx.send(());

    let count = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Recorder timed out")
        .expect("Recorder task panicked")
        .expect("Recorder returned error");

    assert_eq!(count, 3, "Should have recorded 3 messages");

    // Verify CSV content
    let mut reader = CsvReader::new(&csv_path, false, None).expect("Failed to open CSV");
    let mut records = Vec::new();
    while let Some(Ok(record)) = reader.next() {
        records.push(record);
    }
    assert_eq!(records.len(), 3);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.topic, format!("test/topic/{}", i));
        assert_eq!(record.payload, format!("payload-{}", i));
    }
}

/// Test that recorder preserves message fields (topic, payload) in CSV records.
///
/// Note: QoS and retain values in CSV reflect the *delivery* QoS/retain from the
/// broker, not the original publish values. The embedded broker may modify these.
///
/// 1. Start broker on port 18851
/// 2. Publish messages with distinct topics and payloads
/// 3. Verify CSV records have matching topic and payload values
#[tokio::test]
async fn test_recorder_preserves_message_fields() {
    let _broker = EmbeddedBroker::new(18851, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("output.csv");

    let recorder_client = make_client(18851, "recorder").await;
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let topics = TopicFilter::wildcard();
    let mut recorder = Recorder::new(recorder_client, writer, topics, QoS::AtMostOnce).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { recorder.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher = make_client(18851, "publisher").await;
    publish(&publisher, "sensors/temp", b"22.5", QoS::AtMostOnce, false).await;
    publish(&publisher, "sensors/humidity", b"65", QoS::AtMostOnce, false).await;

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = shutdown_tx.send(());

    let count = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked")
        .expect("Error");

    assert_eq!(count, 2);

    let mut reader = CsvReader::new(&csv_path, false, None).expect("Failed to open CSV");
    let mut records = Vec::new();
    while let Some(Ok(record)) = reader.next() {
        records.push(record);
    }
    assert_eq!(records.len(), 2);

    let rec_temp = records.iter().find(|r| r.topic == "sensors/temp").expect("Should have temp");
    let rec_hum = records.iter().find(|r| r.topic == "sensors/humidity").expect("Should have humidity");

    assert_eq!(rec_temp.payload, "22.5");
    assert_eq!(rec_hum.payload, "65");

    // Verify timestamp is present and reasonable (within last minute)
    let now = chrono::Utc::now();
    for record in &records {
        let age = now - record.timestamp;
        assert!(age.num_seconds() < 60, "Timestamp should be recent");
    }
}

/// Test that recorder handles graceful shutdown correctly.
///
/// 1. Start recorder, publish messages
/// 2. Send shutdown signal
/// 3. Verify recorder returns Ok with correct count
/// 4. Verify CSV file is flushed (readable and complete)
#[tokio::test]
async fn test_recorder_handles_graceful_shutdown() {
    let _broker = EmbeddedBroker::new(18852, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("output.csv");

    let recorder_client = make_client(18852, "recorder").await;
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let topics = TopicFilter::wildcard();
    let mut recorder = Recorder::new(recorder_client, writer, topics, QoS::AtMostOnce).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { recorder.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher = make_client(18852, "publisher").await;
    publish(&publisher, "test/shutdown", b"before-shutdown", QoS::AtMostOnce, false).await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = shutdown_tx.send(());

    let result = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked");

    assert!(result.is_ok(), "Recorder should return Ok on shutdown");
    assert_eq!(result.unwrap(), 1);

    // Verify CSV is readable (flushed properly)
    let mut reader = CsvReader::new(&csv_path, false, None).expect("CSV should be readable");
    let record = reader.next().expect("Should have a record").expect("Should parse");
    assert_eq!(record.topic, "test/shutdown");
    assert_eq!(record.payload, "before-shutdown");
}

/// Test that recorder auto-encodes binary payloads with b64: prefix.
///
/// 1. Publish a binary (non-UTF8) payload and a text payload
/// 2. Verify binary payload has b64: prefix in CSV, text is stored as-is
#[tokio::test]
async fn test_recorder_auto_encodes_binary_payloads() {
    let _broker = EmbeddedBroker::new(18853, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("output.csv");

    let recorder_client = make_client(18853, "recorder").await;
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let topics = TopicFilter::wildcard();
    let mut recorder = Recorder::new(recorder_client, writer, topics, QoS::AtMostOnce).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { recorder.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher = make_client(18853, "publisher").await;
    // Text payload
    publish(&publisher, "test/text", b"hello world", QoS::AtMostOnce, false).await;
    // Binary payload (invalid UTF-8)
    publish(
        &publisher,
        "test/binary",
        &[0x00, 0x01, 0xFF, 0xFE],
        QoS::AtMostOnce,
        false,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = shutdown_tx.send(());

    let count = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked")
        .expect("Error");

    assert_eq!(count, 2);

    // Read raw CSV to check encoding
    let content = std::fs::read_to_string(&csv_path).expect("Failed to read CSV");
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 3, "Header + 2 records");

    // Text payload should be stored as-is
    assert!(
        lines[1].contains("hello world"),
        "Text payload should be stored as-is"
    );
    // Binary payload should have b64: prefix
    assert!(
        lines[2].contains("b64:"),
        "Binary payload should have b64: prefix"
    );
}

/// Test that recorder respects topic filter.
///
/// 1. Subscribe to "sensors/#" only
/// 2. Publish to "sensors/temp" and "actuators/fan"
/// 3. Verify only the sensors message is recorded
#[tokio::test]
async fn test_recorder_topic_filter() {
    let _broker = EmbeddedBroker::new(18854, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("output.csv");

    let recorder_client = make_client(18854, "recorder").await;
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let topics = TopicFilter::from_single("sensors/#".to_string());
    let mut recorder = Recorder::new(recorder_client, writer, topics, QoS::AtMostOnce).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { recorder.run(shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(300)).await;

    let publisher = make_client(18854, "publisher").await;
    publish(
        &publisher,
        "sensors/temp",
        b"22.5",
        QoS::AtMostOnce,
        false,
    )
    .await;
    publish(
        &publisher,
        "actuators/fan",
        b"on",
        QoS::AtMostOnce,
        false,
    )
    .await;
    publish(
        &publisher,
        "sensors/humidity",
        b"65",
        QoS::AtMostOnce,
        false,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = shutdown_tx.send(());

    let count = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked")
        .expect("Error");

    assert_eq!(count, 2, "Should only record sensors/# messages");

    let mut reader = CsvReader::new(&csv_path, false, None).expect("Failed to open CSV");
    let mut records = Vec::new();
    while let Some(Ok(record)) = reader.next() {
        records.push(record);
    }
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.topic.starts_with("sensors/")));
}
