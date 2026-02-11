//! Integration tests for mirror mode broker-to-broker republishing

use mqtt_recorder::broker::{BrokerMode, EmbeddedBroker};
use mqtt_recorder::csv_handler::CsvWriter;
use mqtt_recorder::mirror::Mirror;
use mqtt_recorder::mqtt::{AnyMqttClient, MqttClientConfig, MqttClientV5};
use mqtt_recorder::topics::TopicFilter;
use rumqttc::QoS;

use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::broadcast;
use tokio::time::timeout;

/// Test that mirror mode republishes messages from source broker to embedded broker.
///
/// Setup:
/// 1. Start source broker on port 18830
/// 2. Start mirror with embedded broker on port 18831, connected to source
/// 3. Connect test client to embedded broker (18831)
/// 4. Publish message to source broker (18830)
/// 5. Verify message arrives on test client via embedded broker
#[tokio::test]
async fn test_mirror_republishes_to_embedded_broker() {
    // Start source broker
    let _source_broker = EmbeddedBroker::new(18830, BrokerMode::Standalone)
        .await
        .expect("Failed to start source broker");

    // Give broker time to fully start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start mirror's embedded broker
    let mirror_broker = EmbeddedBroker::new(18831, BrokerMode::Mirror)
        .await
        .expect("Failed to start mirror broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create source client for mirror (connects to source broker)
    let source_config =
        MqttClientConfig::new("127.0.0.1".to_string(), 18830, "mirror-source".to_string());
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("Failed to create source client");

    // Connect publisher to source broker first
    let publisher_config =
        MqttClientConfig::new("127.0.0.1".to_string(), 18830, "test-publisher".to_string());
    let publisher = MqttClientV5::new(publisher_config)
        .await
        .expect("Failed to create publisher");

    // Poll to establish connection
    let _ = publisher.poll().await;

    // Create mirror without CSV recording
    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        None,
        topics,
        QoS::AtMostOnce,
        false,
    )
    .await
    .expect("Failed to create mirror");

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Run mirror in background task
    let mirror_handle = tokio::spawn(async move { mirror.run(shutdown_rx, None).await });

    // Give mirror time to subscribe
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect test subscriber to mirror's embedded broker BEFORE publishing
    let subscriber_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18831,
        "test-subscriber".to_string(),
    );
    let subscriber = MqttClientV5::new(subscriber_config)
        .await
        .expect("Failed to create subscriber");

    // Subscribe to test topic
    subscriber
        .subscribe(&["test/mirror".to_string()], QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");

    // Poll subscriber to establish connection and subscription
    loop {
        match subscriber.poll().await {
            Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::SubAck(_))) => {
                break
            }
            Ok(_) => continue,
            Err(e) => panic!("Subscriber setup error: {}", e),
        }
    }

    // Give subscription time to propagate
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish test message to source broker
    publisher
        .publish("test/mirror", b"hello from source", QoS::AtMostOnce, false)
        .await
        .expect("Failed to publish");

    // Poll publisher to actually send the message
    let _ = publisher.poll().await;

    // Wait for message to arrive on subscriber (via mirror)
    let received = timeout(Duration::from_secs(5), async {
        loop {
            match subscriber.poll().await {
                Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(
                    publish,
                ))) => {
                    return Some((
                        String::from_utf8_lossy(&publish.topic).into_owned(),
                        publish.payload.to_vec(),
                    ));
                }
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    })
    .await;

    // Shutdown mirror
    let _ = shutdown_tx.send(());
    let _ = mirror_handle.await;

    // Verify message was received
    match received {
        Ok(Some((topic, payload))) => {
            assert_eq!(topic, "test/mirror");
            assert_eq!(payload, b"hello from source");
        }
        Ok(None) => panic!("No message received - mirror republishing failed"),
        Err(_) => panic!("Timeout waiting for message - mirror republishing failed"),
    }
}

/// Test that mirror mode records messages to CSV when writer is provided.
#[tokio::test]
async fn test_mirror_records_to_csv() {
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("mirror_output.csv");

    // Start source broker
    let _source_broker = EmbeddedBroker::new(18832, BrokerMode::Standalone)
        .await
        .expect("Failed to start source broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start mirror's embedded broker
    let mirror_broker = EmbeddedBroker::new(18833, BrokerMode::Mirror)
        .await
        .expect("Failed to start mirror broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect publisher to source broker first
    let publisher_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18832,
        "test-publisher-2".to_string(),
    );
    let publisher = MqttClientV5::new(publisher_config)
        .await
        .expect("Failed to create publisher");

    // Poll to establish connection
    let _ = publisher.poll().await;

    // Create source client for mirror
    let source_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18832,
        "mirror-source-2".to_string(),
    );
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("Failed to create source client");

    // Create CSV writer
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create CSV writer");

    // Create mirror with CSV recording
    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        Some(writer),
        topics,
        QoS::AtMostOnce,
        false,
    )
    .await
    .expect("Failed to create mirror");

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    let mirror_handle = tokio::spawn(async move { mirror.run(shutdown_rx, None).await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish test message to source broker
    publisher
        .publish("test/record", b"recorded message", QoS::AtMostOnce, false)
        .await
        .expect("Failed to publish");

    // Poll to actually send the message
    let _ = publisher.poll().await;

    // Give time for message to be processed and written
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown mirror
    let _ = shutdown_tx.send(());
    let _ = mirror_handle.await;

    // Verify CSV file was created and contains the message
    let csv_content = std::fs::read_to_string(&csv_path).expect("Failed to read CSV");
    assert!(
        csv_content.contains("test/record"),
        "CSV should contain topic"
    );
    assert!(
        csv_content.contains("recorded message"),
        "CSV should contain payload"
    );
}

/// Test that mirror mode respects TUI state for record/passthrough toggle.
/// When in Record mode, messages should be written to CSV.
/// When in Passthrough mode, messages should NOT be written to CSV.
/// In both cases, messages should always be republished to the embedded broker.
#[tokio::test]
async fn test_mirror_tui_record_toggle() {
    use mqtt_recorder::tui::{AppMode, TuiState};
    use std::sync::Arc;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("toggle_output.csv");

    // Start source broker
    let _source_broker = EmbeddedBroker::new(18834, BrokerMode::Standalone)
        .await
        .expect("Failed to start source broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start mirror's embedded broker
    let mirror_broker = EmbeddedBroker::new(18835, BrokerMode::Mirror)
        .await
        .expect("Failed to start mirror broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect publisher first
    let publisher_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18834,
        "test-publisher-3".to_string(),
    );
    let publisher = MqttClientV5::new(publisher_config)
        .await
        .expect("Failed to create publisher");
    let _ = publisher.poll().await;

    // Create source client for mirror
    let source_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18834,
        "mirror-source-3".to_string(),
    );
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("Failed to create source client");

    // Create CSV writer
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create CSV writer");

    // Create TUI state - recording starts OFF (will toggle ON later)
    let tui_state = Arc::new(TuiState::new(
        AppMode::Mirror,
        18835,
        Some(csv_path.to_string_lossy().to_string()),
        None,
        1883,
        None,
        true,
        vec![],
        true,
        60,
    ));
    tui_state.set_recording(false); // Start with recording OFF

    // Create mirror with TUI state
    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        Some(writer),
        topics,
        QoS::AtMostOnce,
        false,
    )
    .await
    .expect("Failed to create mirror");

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Clone tui_state for the mirror task
    let mirror_tui_state = tui_state.clone();
    let mirror_handle =
        tokio::spawn(async move { mirror.run(shutdown_rx, Some(mirror_tui_state)).await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish message while recording is OFF - should NOT be recorded
    publisher
        .publish("test/passthrough", b"not recorded", QoS::AtMostOnce, false)
        .await
        .expect("Failed to publish");
    let _ = publisher.poll().await;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Enable recording
    tui_state.set_recording(true);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish message while in Record mode - should be recorded
    publisher
        .publish("test/record", b"recorded", QoS::AtMostOnce, false)
        .await
        .expect("Failed to publish");
    let _ = publisher.poll().await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shutdown mirror
    let _ = shutdown_tx.send(());
    let _ = mirror_handle.await;

    // Verify CSV contains only the recorded message
    let csv_content = std::fs::read_to_string(&csv_path).expect("Failed to read CSV");
    assert!(
        !csv_content.contains("not recorded"),
        "Passthrough message should NOT be in CSV"
    );
    assert!(
        csv_content.contains("recorded"),
        "Record mode message should be in CSV"
    );
}

/// Test that disabling mirror stops republishing but continues recording.
/// When mirror is OFF:
/// - Messages should NOT be republished to the embedded broker
/// - Messages should still be recorded to CSV (if recording is enabled)
#[tokio::test]
async fn test_mirror_toggle_stops_republish_but_continues_recording() {
    use mqtt_recorder::tui::{AppMode, TuiState};
    use std::sync::Arc;

    let temp_dir = tempdir().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("mirror_toggle_test.csv");

    // Start source broker
    let _source_broker = EmbeddedBroker::new(18836, BrokerMode::Standalone)
        .await
        .expect("Failed to start source broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start mirror's embedded broker
    let mirror_broker = EmbeddedBroker::new(18837, BrokerMode::Mirror)
        .await
        .expect("Failed to start mirror broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect publisher
    let publisher_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18836,
        "test-publisher-mirror".to_string(),
    );
    let publisher = MqttClientV5::new(publisher_config)
        .await
        .expect("Failed to create publisher");
    let _ = publisher.poll().await;

    // Create source client for mirror
    let source_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18836,
        "mirror-source-toggle".to_string(),
    );
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("Failed to create source client");

    // Create CSV writer
    let writer = CsvWriter::new(&csv_path, false).expect("Failed to create CSV writer");

    // Create TUI state - recording ON, mirroring ON initially
    let tui_state = Arc::new(TuiState::new(
        AppMode::Mirror,
        18837,
        Some(csv_path.to_string_lossy().to_string()),
        None,
        1883,
        None,
        true,
        vec![],
        true,
        60,
    ));
    tui_state.set_recording(true);

    // Create mirror
    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        Some(writer),
        topics,
        QoS::AtMostOnce,
        false,
    )
    .await
    .expect("Failed to create mirror");

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    let mirror_tui_state = tui_state.clone();
    let mirror_handle =
        tokio::spawn(async move { mirror.run(shutdown_rx, Some(mirror_tui_state)).await });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect subscriber to embedded broker
    let subscriber_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18837,
        "test-subscriber-mirror".to_string(),
    );
    let subscriber = MqttClientV5::new(subscriber_config)
        .await
        .expect("Failed to create subscriber");

    subscriber
        .subscribe(&["test/#".to_string()], QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");

    // Wait for subscription
    loop {
        match subscriber.poll().await {
            Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::SubAck(_))) => {
                break
            }
            Ok(_) => continue,
            Err(e) => panic!("Subscriber setup error: {}", e),
        }
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish while mirroring ON - should be republished AND recorded
    publisher
        .publish(
            "test/mirrored",
            b"republished and recorded",
            QoS::AtMostOnce,
            false,
        )
        .await
        .expect("Failed to publish");
    let _ = publisher.poll().await;

    // Wait for message on subscriber
    let received_mirrored = timeout(Duration::from_secs(2), async {
        loop {
            match subscriber.poll().await {
                Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(
                    p,
                ))) => return Some(String::from_utf8_lossy(&p.topic).into_owned()),
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    })
    .await;

    assert!(
        received_mirrored.is_ok(),
        "Should receive message when mirroring ON"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Turn mirroring OFF
    tui_state.set_mirroring(false);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish while mirroring OFF - should be recorded but NOT republished
    publisher
        .publish(
            "test/not-mirrored",
            b"recorded but not republished",
            QoS::AtMostOnce,
            false,
        )
        .await
        .expect("Failed to publish");
    let _ = publisher.poll().await;

    // Should NOT receive on subscriber (timeout expected)
    let received_not_mirrored = timeout(Duration::from_millis(500), async {
        loop {
            match subscriber.poll().await {
                Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(
                    p,
                ))) => return Some(String::from_utf8_lossy(&p.topic).into_owned()),
                Ok(_) => continue,
                Err(_) => return None,
            }
        }
    })
    .await;

    // Timeout is expected - message should NOT be republished
    assert!(
        received_not_mirrored.is_err() || received_not_mirrored.unwrap().is_none(),
        "Should NOT receive message when mirroring OFF"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Shutdown
    let _ = shutdown_tx.send(());
    let _ = mirror_handle.await;

    // Verify CSV contains BOTH messages (recording continues when mirroring off)
    let csv_content = std::fs::read_to_string(&csv_path).expect("Failed to read CSV");
    assert!(
        csv_content.contains("mirrored"),
        "Mirrored message should be recorded"
    );
    assert!(
        csv_content.contains("not-mirrored"),
        "Non-mirrored message should still be recorded"
    );
    assert!(
        csv_content.contains("recorded but not republished"),
        "Non-mirrored payload should be in CSV"
    );
}

/// Test that mirror mode handles source broker disconnection gracefully.
///
/// NOTE: This test documents current behavior. The MQTT client (rumqttc) has
/// automatic reconnection built in, which means it may not immediately report
/// connection errors. The keepalive mechanism (15s) should eventually detect
/// dead connections, but the exact timing depends on network conditions.
///
/// This test verifies that:
/// 1. The mirror doesn't panic when the source broker shuts down
/// 2. The error tracking infrastructure is in place
#[tokio::test]
async fn test_mirror_handles_source_broker_disconnect() {
    use mqtt_recorder::tui::{AppMode, TuiState};
    use std::sync::Arc;

    // Start source broker (we'll shut it down later)
    let source_broker = EmbeddedBroker::new(18840, BrokerMode::Standalone)
        .await
        .expect("Failed to start source broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start mirror's embedded broker
    let mirror_broker = EmbeddedBroker::new(18841, BrokerMode::Mirror)
        .await
        .expect("Failed to start mirror broker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create source client for mirror
    let source_config = MqttClientConfig::new(
        "127.0.0.1".to_string(),
        18840,
        "mirror-source-disconnect".to_string(),
    );
    let source_client = MqttClientV5::new(source_config)
        .await
        .expect("Failed to create source client");

    // Create TUI state to capture errors
    let tui_state = Arc::new(TuiState::new(
        AppMode::Mirror,
        18841,
        None,
        Some("127.0.0.1".to_string()),
        18840,
        None,
        true,
        vec![],
        true,
        60,
    ));

    // Create mirror
    let topics = TopicFilter::wildcard();
    let mut mirror = Mirror::new(
        AnyMqttClient::V5(source_client),
        mirror_broker,
        None,
        topics,
        QoS::AtMostOnce,
        false,
    )
    .await
    .expect("Failed to create mirror");

    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    let mirror_tui_state = tui_state.clone();
    let mirror_handle =
        tokio::spawn(async move { mirror.run(shutdown_rx, Some(mirror_tui_state)).await });

    // Let mirror establish connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify no error initially
    assert!(
        tui_state.get_error().is_none(),
        "Should have no error initially"
    );

    // Shutdown the source broker to simulate disconnection
    source_broker
        .shutdown()
        .await
        .expect("Failed to shutdown source broker");

    // Wait a bit for any immediate error detection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Shutdown mirror gracefully
    let _ = shutdown_tx.send(());
    let result = mirror_handle.await;

    // The test passes if the mirror task completed without panicking
    assert!(result.is_ok(), "Mirror task should not panic");

    // Log what happened for debugging
    if let Some(err) = tui_state.get_error() {
        println!("TUI captured error: {}", err);
    }

    let task_result = result.unwrap();
    println!("Mirror task result: {:?}", task_result.is_ok());
}
