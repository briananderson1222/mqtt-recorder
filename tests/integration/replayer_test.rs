//! Integration tests for replayer mode using embedded broker

use mqtt_recorder::broker::{BrokerMode, EmbeddedBroker};
use mqtt_recorder::csv_handler::{CsvReader, CsvWriter, MessageRecord};
use mqtt_recorder::mqtt::{MqttClient, MqttClientConfig};
use mqtt_recorder::replayer::Replayer;
use rumqttc::{Event, Packet, QoS};

use chrono::Utc;
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

/// Helper: create a CSV file with test messages (same timestamp = no delay)
fn create_test_csv(path: &std::path::Path, messages: &[(&str, &str, u8, bool)]) {
    let mut writer = CsvWriter::new(path, false).expect("Failed to create writer");
    let ts = Utc::now();
    for (topic, payload, qos, retain) in messages.iter() {
        let record = MessageRecord::new(
            ts,
            topic.to_string(),
            payload.to_string(),
            *qos,
            *retain,
        );
        writer.write(&record).expect("Failed to write record");
    }
    writer.flush().expect("Failed to flush");
}

/// Helper: subscribe and collect messages from broker
async fn collect_messages(
    client: &MqttClient,
    topic: &str,
    expected: usize,
    timeout_ms: u64,
) -> Vec<(String, Vec<u8>)> {
    client
        .subscribe(&[topic.to_string()], QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");
    // Poll to process subscription
    let _ = client.poll().await;

    let mut messages = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);

    while messages.len() < expected && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), client.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(p)))) => {
                messages.push((p.topic.clone(), p.payload.to_vec()));
            }
            _ => {}
        }
    }
    messages
}

/// Test that replayer publishes CSV messages to broker.
///
/// 1. Create CSV with 3 messages
/// 2. Start embedded broker and replayer
/// 3. Connect subscriber
/// 4. Verify all 3 messages received with correct topic/payload
#[tokio::test]
async fn test_replayer_publishes_csv_messages() {
    let _broker = EmbeddedBroker::new(18860, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("input.csv");
    create_test_csv(
        &csv_path,
        &[
            ("test/a", "payload-a", 0, false),
            ("test/b", "payload-b", 0, false),
            ("test/c", "payload-c", 0, false),
        ],
    );

    // Connect subscriber FIRST so it's ready before replay starts
    let subscriber = make_client(18860, "subscriber").await;
    subscriber
        .subscribe(&["test/#".to_string()], QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");
    // Poll to establish connection and subscription
    let _ = subscriber.poll().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let replayer_client = make_client(18860, "replayer").await;
    let reader = CsvReader::new(&csv_path, false, None).expect("Failed to create reader");
    let mut replayer = Replayer::new(replayer_client, reader, false).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { replayer.run(shutdown_rx).await });

    // Collect messages from subscriber
    let messages = collect_messages(&subscriber, "test/#", 3, 5000).await;

    // Shutdown replayer (it may have already exited)
    let _ = shutdown_tx.send(());
    let count = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked")
        .expect("Error");

    assert_eq!(count, 3, "Replayer should have published 3 messages");
    assert_eq!(messages.len(), 3, "Subscriber should have received 3 messages");

    // Verify topics and payloads
    let topics: Vec<&str> = messages.iter().map(|(t, _)| t.as_str()).collect();
    assert!(topics.contains(&"test/a"));
    assert!(topics.contains(&"test/b"));
    assert!(topics.contains(&"test/c"));
}

/// Test that replayer preserves message fields through replay.
///
/// 1. Create CSV with messages having specific topics and payloads
/// 2. Replay and verify subscriber receives matching fields
#[tokio::test]
async fn test_replayer_preserves_message_fields() {
    let _broker = EmbeddedBroker::new(18861, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("input.csv");
    create_test_csv(
        &csv_path,
        &[
            ("sensors/temperature", "22.5", 0, false),
            ("sensors/humidity", "65", 0, false),
        ],
    );

    let subscriber = make_client(18861, "subscriber").await;
    subscriber
        .subscribe(&["sensors/#".to_string()], QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");
    let _ = subscriber.poll().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let replayer_client = make_client(18861, "replayer").await;
    let reader = CsvReader::new(&csv_path, false, None).expect("Failed to create reader");
    let mut replayer = Replayer::new(replayer_client, reader, false).await;

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { replayer.run(shutdown_rx).await });

    let messages = collect_messages(&subscriber, "sensors/#", 2, 5000).await;

    let _ = timeout(Duration::from_secs(5), handle).await;

    assert_eq!(messages.len(), 2);

    // Find by topic
    let temp = messages
        .iter()
        .find(|(t, _)| t == "sensors/temperature")
        .expect("Should have temperature");
    let hum = messages
        .iter()
        .find(|(t, _)| t == "sensors/humidity")
        .expect("Should have humidity");

    assert_eq!(std::str::from_utf8(&temp.1).unwrap(), "22.5");
    assert_eq!(std::str::from_utf8(&hum.1).unwrap(), "65");
}

/// Test that replayer exits after all messages when loop=false.
///
/// 1. Create CSV with 2 messages
/// 2. Run replayer with loop=false
/// 3. Verify it exits naturally and returns correct count
#[tokio::test]
async fn test_replayer_exits_after_all_messages() {
    let _broker = EmbeddedBroker::new(18862, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("input.csv");
    create_test_csv(
        &csv_path,
        &[
            ("test/1", "msg-1", 0, false),
            ("test/2", "msg-2", 0, false),
        ],
    );

    let replayer_client = make_client(18862, "replayer").await;
    let reader = CsvReader::new(&csv_path, false, None).expect("Failed to create reader");
    let mut replayer = Replayer::new(replayer_client, reader, false).await;

    let (_shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Replayer should exit on its own (no shutdown signal needed)
    let result = timeout(Duration::from_secs(10), replayer.run(shutdown_rx))
        .await
        .expect("Replayer should exit within timeout");

    let count = result.expect("Replayer should return Ok");
    assert_eq!(count, 2, "Should have replayed exactly 2 messages");
}

/// Test that replayer loops continuously when loop=true.
///
/// 1. Create CSV with 2 messages
/// 2. Run replayer with loop=true
/// 3. Wait until subscriber receives >2 messages (proving loop happened)
/// 4. Shutdown and verify count > 2
#[tokio::test]
async fn test_replayer_loops_continuously() {
    let _broker = EmbeddedBroker::new(18863, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("input.csv");
    // Use 0ms-spaced timestamps so replay is fast
    let mut writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let ts = Utc::now();
    writer.write(&MessageRecord::new(ts, "loop/msg".into(), "data-1".into(), 0, false)).unwrap();
    writer.write(&MessageRecord::new(ts, "loop/msg".into(), "data-2".into(), 0, false)).unwrap();
    writer.flush().unwrap();

    let subscriber = make_client(18863, "subscriber").await;
    subscriber
        .subscribe(&["loop/#".to_string()], QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");
    let _ = subscriber.poll().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let replayer_client = make_client(18863, "replayer").await;
    let reader = CsvReader::new(&csv_path, false, None).expect("Failed to create reader");
    let mut replayer = Replayer::new(replayer_client, reader, true).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { replayer.run(shutdown_rx).await });

    // Wait for more than 2 messages (proving loop happened)
    let messages = collect_messages(&subscriber, "loop/#", 4, 10000).await;

    let _ = shutdown_tx.send(());
    let count = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked")
        .expect("Error");

    assert!(
        messages.len() >= 4,
        "Should receive >=4 messages (at least 2 loops), got {}",
        messages.len()
    );
    assert!(
        count >= 4,
        "Replayer count should be >=4, got {}",
        count
    );
}

/// Test that replayer handles shutdown during replay gracefully.
///
/// 1. Create CSV with messages spaced 2 seconds apart
/// 2. Start replayer
/// 3. Send shutdown after first message but before all complete
/// 4. Verify graceful stop with partial count
#[tokio::test]
async fn test_replayer_handles_shutdown_during_replay() {
    let _broker = EmbeddedBroker::new(18864, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("input.csv");
    // Messages spaced 2 seconds apart so we can shutdown mid-replay
    let mut writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let base = Utc::now();
    writer.write(&MessageRecord::new(base, "test/1".into(), "first".into(), 0, false)).unwrap();
    writer.write(&MessageRecord::new(base + chrono::Duration::seconds(2), "test/2".into(), "second".into(), 0, false)).unwrap();
    writer.write(&MessageRecord::new(base + chrono::Duration::seconds(4), "test/3".into(), "third".into(), 0, false)).unwrap();
    writer.flush().unwrap();

    let replayer_client = make_client(18864, "replayer").await;
    let reader = CsvReader::new(&csv_path, false, None).expect("Failed to create reader");
    let mut replayer = Replayer::new(replayer_client, reader, false).await;

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let handle = tokio::spawn(async move { replayer.run(shutdown_rx).await });

    // Wait for first message to be sent, then shutdown during delay before second
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = shutdown_tx.send(());

    let result = timeout(Duration::from_secs(5), handle)
        .await
        .expect("Timed out")
        .expect("Panicked");

    let count = result.expect("Replayer should return Ok on shutdown");
    assert!(
        count < 3,
        "Should have stopped before all 3 messages, got {}",
        count
    );
}

/// Test full record → replay roundtrip.
///
/// 1. Start broker A (18870), publish messages
/// 2. Record those messages to CSV
/// 3. Start broker B (18871), replay CSV to it
/// 4. Verify subscriber on broker B receives the same messages
#[tokio::test]
async fn test_record_then_replay_roundtrip() {
    use mqtt_recorder::recorder::Recorder;
    use mqtt_recorder::topics::TopicFilter;

    // === Phase 1: Record ===
    let _broker_a = EmbeddedBroker::new(18870, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker A");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = tempdir().unwrap();
    let csv_path = dir.path().join("roundtrip.csv");

    let recorder_client = MqttClient::new(MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18870,
        "recorder".to_string(),
    ))
    .await
    .expect("Failed to create recorder client");

    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create writer");
    let topics = TopicFilter::wildcard();
    let mut recorder = Recorder::new(recorder_client, writer, topics, QoS::AtMostOnce).await;

    let (rec_shutdown_tx, rec_shutdown_rx) = broadcast::channel::<()>(1);
    let rec_handle = tokio::spawn(async move { recorder.run(rec_shutdown_rx).await });

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish test messages
    let publisher = make_client(18870, "publisher").await;
    for (topic, payload) in [
        ("roundtrip/a", "alpha"),
        ("roundtrip/b", "beta"),
        ("roundtrip/c", "gamma"),
    ] {
        publisher
            .publish(topic, payload.as_bytes(), QoS::AtMostOnce, false)
            .await
            .unwrap();
        for _ in 0..3 {
            let _ = publisher.poll().await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;
    let _ = rec_shutdown_tx.send(());

    let rec_count = timeout(Duration::from_secs(5), rec_handle)
        .await
        .expect("Recorder timed out")
        .expect("Recorder panicked")
        .expect("Recorder error");

    assert_eq!(rec_count, 3, "Should have recorded 3 messages");

    // Rewrite CSV with same timestamps so replay has no delays
    // (the recorded timestamps are real-time spaced, which causes delay loop issues)
    {
        let mut reader = CsvReader::new(&csv_path, false, None).unwrap();
        let mut records = Vec::new();
        while let Some(Ok(r)) = reader.next() {
            records.push(r);
        }
        let rewrite_path = dir.path().join("roundtrip_nodelay.csv");
        let mut writer = CsvWriter::new(&rewrite_path, false).unwrap();
        let ts = records[0].timestamp;
        for r in &records {
            writer
                .write(&MessageRecord::new(
                    ts,
                    r.topic.clone(),
                    r.payload.clone(),
                    r.qos,
                    r.retain,
                ))
                .unwrap();
        }
        writer.flush().unwrap();
        std::fs::rename(&rewrite_path, &csv_path).unwrap();
    }

    // === Phase 2: Replay ===
    let _broker_b = EmbeddedBroker::new(18871, BrokerMode::Standalone)
        .await
        .expect("Failed to start broker B");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Subscribe on broker B first
    let subscriber = make_client(18871, "subscriber").await;
    subscriber
        .subscribe(&["roundtrip/#".to_string()], QoS::AtMostOnce)
        .await
        .unwrap();
    let _ = subscriber.poll().await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let replayer_client = make_client(18871, "replayer").await;
    let reader = CsvReader::new(&csv_path, false, None).expect("Failed to create reader");
    let mut replayer = Replayer::new(replayer_client, reader, false).await;

    let (rep_shutdown_tx, rep_shutdown_rx) = broadcast::channel::<()>(1);
    let rep_handle = tokio::spawn(async move { replayer.run(rep_shutdown_rx).await });

    // Collect replayed messages
    let messages = collect_messages(&subscriber, "roundtrip/#", 3, 5000).await;

    let _ = rep_shutdown_tx.send(());
    let rep_count = timeout(Duration::from_secs(5), rep_handle)
        .await
        .expect("Replayer timed out")
        .expect("Replayer panicked")
        .expect("Replayer error");

    assert_eq!(rep_count, 3, "Should have replayed 3 messages");
    assert_eq!(messages.len(), 3, "Subscriber should receive 3 messages");

    // Verify the same topics and payloads survived the roundtrip
    let mut received: Vec<(String, String)> = messages
        .iter()
        .map(|(t, p)| (t.clone(), String::from_utf8_lossy(p).to_string()))
        .collect();
    received.sort();

    assert_eq!(
        received,
        vec![
            ("roundtrip/a".to_string(), "alpha".to_string()),
            ("roundtrip/b".to_string(), "beta".to_string()),
            ("roundtrip/c".to_string(), "gamma".to_string()),
        ]
    );
}
