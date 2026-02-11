//! Integration tests for --verify mode across mirror, playback, and combined scenarios

use mqtt_recorder::broker::{BrokerMode, EmbeddedBroker};
use mqtt_recorder::csv_handler::{CsvWriter, MessageRecord};
use mqtt_recorder::mirror::Mirror;
use mqtt_recorder::mqtt::{AnyMqttClient, MqttClientConfig, MqttClientV5};
use mqtt_recorder::topics::TopicFilter;
use mqtt_recorder::tui::{AppMode, TuiState};
use rumqttc::QoS;

use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::broadcast;
use tokio::time::timeout;

/// Helper: create a TuiState for verify tests
fn make_tui_state(broker_port: u16, file_path: Option<String>) -> Arc<TuiState> {
    Arc::new(TuiState::new(
        AppMode::Mirror,
        broker_port,
        file_path,
        Some("127.0.0.1".to_string()),
        0,
        None,
        true,
        vec![],
        false,
        0,
    ))
}

/// Helper: publish N messages to a broker and poll to flush
async fn publish_messages(client: &MqttClientV5, topic: &str, count: usize) {
    for i in 0..count {
        client
            .publish(
                topic,
                format!("msg-{}", i).as_bytes(),
                QoS::AtMostOnce,
                false,
            )
            .await
            .expect("publish failed");
        // Poll after each publish to actually send it
        let _ = client.poll().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Helper: send a warmup message and wait for verify to become active
async fn wait_for_verify_ready(publisher: &MqttClientV5, _tui_state: &Arc<TuiState>, topic: &str) {
    // Publish warmup messages to trigger verify_ready flag on both sides
    for _ in 0..5 {
        publisher
            .publish(topic, b"warmup", QoS::AtMostOnce, false)
            .await
            .expect("warmup publish");
        let _ = publisher.poll().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Give time for verify subscriber to process and source to sync
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// Helper: write a CSV file with N test messages
fn write_test_csv(path: &std::path::Path, topic: &str, count: usize) {
    let mut writer = CsvWriter::new(path, false).expect("csv writer");
    for i in 0..count {
        let record = MessageRecord::new(
            chrono::Utc::now(),
            topic.to_string(),
            format!("csv-{}", i),
            0,
            false,
        );
        writer.write(&record).expect("csv write");
    }
    writer.flush().expect("csv flush");
}

/// Test 1: Source mirroring with verify — messages from source broker arrive on embedded broker
#[tokio::test]
async fn test_verify_source_mirroring() {
    let source_port = 18870;
    let mirror_port = 18871;

    let _source_broker = EmbeddedBroker::new(source_port, BrokerMode::Standalone)
        .await
        .expect("source broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mirror_broker = EmbeddedBroker::new(mirror_port, BrokerMode::Mirror)
        .await
        .expect("mirror broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let source_config =
        MqttClientConfig::new("127.0.0.1".to_string(), source_port, "vfy-src".to_string());
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("source client");

    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        None,
        topics,
        QoS::AtMostOnce,
        true, // verify enabled
    )
    .await
    .expect("mirror");

    let tui_state = make_tui_state(mirror_port, None);
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let state_clone = tui_state.clone();

    let mirror_handle =
        tokio::spawn(async move { mirror.run(shutdown_rx, Some(state_clone)).await });

    // Wait for mirror + verify subscriber to be ready
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Publish messages to source broker
    let pub_config =
        MqttClientConfig::new("127.0.0.1".to_string(), source_port, "vfy-pub".to_string());
    let publisher = MqttClientV5::new(pub_config).await.expect("publisher");
    let _ = publisher.poll().await;

    // Warmup: ensure verify subscriber is active
    wait_for_verify_ready(&publisher, &tui_state, "test/verify/warmup").await;

    let msg_count = 20;
    publish_messages(&publisher, "test/verify/source", msg_count).await;

    // Wait for messages to flow through
    tokio::time::sleep(Duration::from_secs(3)).await;

    let _ = shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(5), mirror_handle).await;

    let matched = tui_state.get_verify_matched();
    let mismatched = tui_state.get_verify_mismatched();
    let missing = tui_state.get_verify_missing();

    eprintln!(
        "Source mirror verify: matched={}, unexpected={}, missing={}",
        matched, mismatched, missing
    );

    assert!(
        matched >= (msg_count as u64) * 8 / 10,
        "expected >=80% matched, got {}/{}",
        matched,
        msg_count
    );
    assert_eq!(missing, 0, "expected 0 missing, got {}", missing);
}

/// Test 2: Playback with verify — CSV messages replayed arrive on embedded broker
#[tokio::test]
async fn test_verify_playback() {
    let source_port = 18872;
    let mirror_port = 18873;

    let _source_broker = EmbeddedBroker::new(source_port, BrokerMode::Standalone)
        .await
        .expect("source broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mirror_broker = EmbeddedBroker::new(mirror_port, BrokerMode::Mirror)
        .await
        .expect("mirror broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create CSV file for playback
    let temp_dir = tempdir().expect("temp dir");
    let csv_path = temp_dir.path().join("playback.csv");
    let msg_count = 20;
    write_test_csv(&csv_path, "test/verify/playback", msg_count);

    let source_config =
        MqttClientConfig::new("127.0.0.1".to_string(), source_port, "vfy-src2".to_string());
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("source client");

    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        None,
        topics,
        QoS::AtMostOnce,
        true,
    )
    .await
    .expect("mirror");

    let tui_state = make_tui_state(mirror_port, Some(csv_path.display().to_string()));
    // Configure for playback: enable loop mode, disable recording
    {
        tui_state
            .loop_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tui_state
            .playback_looping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tui_state
            .recording_enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let state_clone = tui_state.clone();

    let mirror_handle =
        tokio::spawn(async move { mirror.run(shutdown_rx, Some(state_clone)).await });

    // Wait for playback to complete
    tokio::time::sleep(Duration::from_secs(4)).await;

    let _ = shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(5), mirror_handle).await;

    let matched = tui_state.get_verify_matched();
    let mismatched = tui_state.get_verify_mismatched();
    let missing = tui_state.get_verify_missing();
    let replayed = tui_state
        .replayed_count
        .load(std::sync::atomic::Ordering::Relaxed);

    eprintln!(
        "Playback verify: replayed={}, matched={}, unexpected={}, missing={}",
        replayed, matched, mismatched, missing
    );

    assert!(
        replayed >= msg_count as u64,
        "expected all messages replayed, got {}",
        replayed
    );
    assert!(matched > 0, "expected some matched messages, got 0");
    // After warmup, most messages should match
    assert!(
        matched > mismatched,
        "expected more matched than unexpected: matched={}, unexpected={}",
        matched,
        mismatched
    );
}

/// Test 3: Combined source mirroring + playback with verify
#[tokio::test]
async fn test_verify_combined_source_and_playback() {
    let source_port = 18874;
    let mirror_port = 18875;

    let _source_broker = EmbeddedBroker::new(source_port, BrokerMode::Standalone)
        .await
        .expect("source broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mirror_broker = EmbeddedBroker::new(mirror_port, BrokerMode::Mirror)
        .await
        .expect("mirror broker");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create CSV for playback
    let temp_dir = tempdir().expect("temp dir");
    let csv_path = temp_dir.path().join("combined.csv");
    let csv_count = 10;
    write_test_csv(&csv_path, "test/verify/csv", csv_count);

    let source_config =
        MqttClientConfig::new("127.0.0.1".to_string(), source_port, "vfy-src3".to_string());
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("source client");

    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        None,
        topics,
        QoS::AtMostOnce,
        true,
    )
    .await
    .expect("mirror");

    let tui_state = make_tui_state(mirror_port, Some(csv_path.display().to_string()));
    // Enable playback
    {
        tui_state
            .loop_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tui_state
            .playback_looping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tui_state
            .recording_enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    let state_clone = tui_state.clone();

    let mirror_handle =
        tokio::spawn(async move { mirror.run(shutdown_rx, Some(state_clone)).await });

    // Wait for verify subscriber + playback to start
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Also publish live messages to source broker
    let pub_config =
        MqttClientConfig::new("127.0.0.1".to_string(), source_port, "vfy-pub3".to_string());
    let publisher = MqttClientV5::new(pub_config).await.expect("publisher");
    let _ = publisher.poll().await;

    // Warmup: wait for verify to be active (playback will trigger it)
    tokio::time::sleep(Duration::from_secs(2)).await;

    let live_count = 10;
    publish_messages(&publisher, "test/verify/live", live_count).await;

    // Wait for everything to flow through verify
    tokio::time::sleep(Duration::from_secs(4)).await;

    let _ = shutdown_tx.send(());
    let _ = timeout(Duration::from_secs(5), mirror_handle).await;

    let matched = tui_state.get_verify_matched();
    let mismatched = tui_state.get_verify_mismatched();
    let missing = tui_state.get_verify_missing();
    let replayed = tui_state
        .replayed_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let mirrored = tui_state
        .mirrored_count
        .load(std::sync::atomic::Ordering::Relaxed);

    eprintln!(
        "Combined verify: mirrored={}, replayed={}, matched={}, unexpected={}, missing={}",
        mirrored, replayed, matched, mismatched, missing
    );

    // Both sources should contribute to matched count
    assert!(matched > 0, "expected some matched messages, got 0");
    assert_eq!(missing, 0, "expected 0 missing, got {}", missing);
}
