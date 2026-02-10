//! MQTT Recorder - Record and replay MQTT messages
//!
//! This CLI tool provides four operational modes:
//! - **Record**: Subscribe to topics on an MQTT broker and persist messages to a CSV file
//! - **Replay**: Read messages from a CSV file and publish them to a broker with timing preservation
//! - **Mirror**: Subscribe to an external broker and republish messages to an embedded broker
//! - **Serve**: Run a standalone embedded MQTT broker
//!
//! # Exit Codes
//!
//! | Code | Meaning |
//! |------|---------|
//! | 0 | Success (including graceful shutdown) |
//! | 1 | Configuration/argument error |
//! | 2 | Connection/authentication error |
//! | 3 | File I/O error |
//! | 4 | Runtime error (unrecoverable) |
//!
//! # Requirements
//!
//! - 1.19-1.26: CLI argument handling and validation
//! - 8.1-8.5: Error handling and logging
//! - 9.1-9.5: Graceful shutdown handling

mod broker;
mod cli;
mod csv_handler;
mod error;
mod fixer;
mod mirror;
mod mqtt;
mod recorder;
mod replayer;
mod topics;
mod tui;
mod validator;

use clap::Parser;
use std::process::ExitCode;
use tokio::sync::broadcast;

use broker::{BrokerMode, EmbeddedBroker};
use cli::{Args, Mode};
use csv_handler::{CsvReader, CsvWriter};
use error::MqttRecorderError;
use fixer::CsvFixer;
use mirror::Mirror;
use mqtt::{AnyMqttClient, MqttClient, MqttClientConfig, MqttClientV5, TlsConfig};
use recorder::Recorder;
use replayer::Replayer;
use topics::TopicFilter;
use tui::{should_enable_interactive, AppMode, AuditArea, AuditSeverity, TuiState};
use validator::CsvValidator;

/// Exit code for success (including graceful shutdown)
const EXIT_SUCCESS: u8 = 0;
/// Exit code for configuration/argument errors
const EXIT_CONFIG_ERROR: u8 = 1;
/// Exit code for connection/authentication errors
const EXIT_CONNECTION_ERROR: u8 = 2;
/// Exit code for file I/O errors
const EXIT_IO_ERROR: u8 = 3;
/// Exit code for validation failure (same as IO error per design spec)
const EXIT_VALIDATION_FAILURE: u8 = 3;
/// Exit code for runtime errors (unrecoverable)
const EXIT_RUNTIME_ERROR: u8 = 4;

#[tokio::main]
async fn main() -> ExitCode {
    // Parse CLI arguments (Requirements 1.1-1.18)
    let mut args = Args::parse();
    args.apply_defaults();

    // Validate argument combinations (Requirements 1.19-1.26)
    if let Err(e) = args.validate() {
        eprintln!("Error: Configuration error: {}", e);
        eprintln!("  Hint: Use --help for usage information");
        return ExitCode::from(EXIT_CONFIG_ERROR);
    }

    // Run the application and handle errors
    match run(args).await {
        Ok(()) => ExitCode::from(EXIT_SUCCESS),
        Err(e) => {
            let exit_code = error_to_exit_code(&e);
            // Error message already printed in run() or by the error handler
            ExitCode::from(exit_code)
        }
    }
}

/// Main application logic.
///
/// This function handles:
/// 1. Setting up signal handlers for graceful shutdown (Requirements 9.1-9.2)
/// 2. Dispatching to the appropriate mode handler
/// 3. Coordinating graceful shutdown (Requirements 9.3-9.5)
async fn run(args: Args) -> Result<(), MqttRecorderError> {
    // Check for validate mode before other mode dispatch
    // Requirements 4.9, 4.10, 6.1-6.6
    if args.validate {
        return run_validate_mode(&args);
    }

    // Check for fix mode before other mode dispatch
    // Requirements 8.2, 8.5
    if args.fix {
        return run_fix_mode(&args);
    }

    // Set up shutdown signal handling (Requirements 9.1, 9.2)
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Clone the sender for the signal handler
    let shutdown_tx_signal = shutdown_tx.clone();

    // Spawn a task to handle shutdown signals
    tokio::spawn(async move {
        if let Err(e) = wait_for_shutdown_signal().await {
            eprintln!("Error setting up signal handler: {}", e);
        }
        // Send shutdown signal to all receivers
        let _ = shutdown_tx_signal.send(());
    });

    // Determine if TUI should be enabled (only for long-running serve modes)
    let enable_tui = args.serve && should_enable_interactive(args.no_interactive);

    // Create TUI state if enabled
    let tui_state = if enable_tui {
        let initial_mode = match args.mode {
            Some(Mode::Record) => AppMode::Record,
            Some(Mode::Replay) => AppMode::Replay,
            Some(Mode::Mirror) => AppMode::Mirror,
            None => AppMode::Passthrough,
        };
        let file_path = args.file.as_ref().map(|p| p.display().to_string());
        let playlist: Vec<String> = args
            .playlist
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        let tui = std::sync::Arc::new(TuiState::new(
            initial_mode,
            args.serve_port,
            file_path,
            args.host.clone(),
            args.port,
            args.record,
            args.mirror,
            playlist,
            args.audit,
        ));
        if let Some(ref path) = args.audit_log {
            tui.set_audit_file_path(path.display().to_string());
            tui.enable_audit_file();
        }
        Some(tui)
    } else {
        None
    };

    // Spawn TUI task if enabled
    let tui_handle = if let Some(ref state) = tui_state {
        let state_clone = state.clone();
        let shutdown_tx_clone = shutdown_tx.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            if let Err(e) = tui::run_tui(state_clone, shutdown_tx_clone, shutdown_rx).await {
                eprintln!("TUI error: {}", e);
            }
        }))
    } else {
        None
    };

    // Dispatch to the appropriate mode handler
    let result = if args.is_standalone_broker() {
        // Standalone broker mode (Requirement 1.26)
        run_standalone_broker(&args, shutdown_tx.subscribe(), tui_state.clone()).await
    } else {
        match args.mode.as_ref().unwrap() {
            Mode::Record => {
                run_record_mode(&args, shutdown_tx.subscribe(), tui_state.clone()).await
            }
            Mode::Replay => run_replay_mode(&args, shutdown_tx.subscribe(), tui_state.clone()).await,
            Mode::Mirror => {
                run_mirror_mode(&args, shutdown_tx.subscribe(), tui_state.clone()).await
            }
        }
    };

    // Wait for TUI to finish if it was running
    if let Some(handle) = tui_handle {
        let _ = handle.await;
    }

    result
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM signals.
///
/// # Requirements
/// - 9.1: Handle SIGINT (Ctrl+C) for graceful shutdown
/// - 9.2: Handle SIGTERM for graceful shutdown
async fn wait_for_shutdown_signal() -> Result<(), MqttRecorderError> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigint = signal(SignalKind::interrupt()).map_err(MqttRecorderError::Io)?;
        let mut sigterm = signal(SignalKind::terminate()).map_err(MqttRecorderError::Io)?;

        tokio::select! {
            _ = sigint.recv() => {
                eprintln!("\nReceived SIGINT (Ctrl+C), initiating graceful shutdown...");
            }
            _ = sigterm.recv() => {
                eprintln!("\nReceived SIGTERM, initiating graceful shutdown...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        // On non-Unix platforms, only handle Ctrl+C
        signal::ctrl_c().await.map_err(MqttRecorderError::Io)?;
        eprintln!("\nReceived Ctrl+C, initiating graceful shutdown...");
    }

    Ok(())
}

/// Run in validate mode.
///
/// Validates a CSV file without replaying, checking for format errors,
/// invalid field values, and encoding issues.
///
/// # Requirements
/// - 4.9: WHEN validation succeeds, report total valid records and exit with code 0
/// - 4.10: WHEN validation fails, report line number and error details, exit with code 3
/// - 6.1: Report total number of records processed
/// - 6.2: Report number of text payloads
/// - 6.3: Report number of binary payloads (auto-encoded with "b64:" prefix)
/// - 6.4: Report number of base64 payloads (when decode_b64 is true)
/// - 6.5: Report number of invalid records (if any)
/// - 6.6: Report largest payload size encountered
fn run_validate_mode(args: &Args) -> Result<(), MqttRecorderError> {
    eprintln!("Starting CSV validation mode...");

    // Get the file path (validated by Args::validate())
    let file_path = args.file.as_ref().unwrap();

    eprintln!("Validating file: {:?}", file_path);

    // Create validator with appropriate settings
    // decode_b64: when true, all payloads are expected to be base64 encoded
    // field_size_limit: optional maximum field size
    let validator = CsvValidator::new(args.encode_b64, args.csv_field_size_limit);

    // Run validation
    let stats = validator.validate(file_path).map_err(|e| {
        eprintln!("Error: Failed to validate file {:?}: {}", file_path, e);
        e
    })?;

    // Print the validation report (Requirements 6.1-6.6)
    // The Display impl for ValidationStats formats the report
    println!("{}", stats);

    // Exit with appropriate code (Requirements 4.9, 4.10)
    if stats.is_valid() {
        eprintln!(
            "Validation complete. All {} records are valid.",
            stats.valid_records
        );
        Ok(())
    } else {
        eprintln!(
            "Validation failed. {} of {} records are invalid.",
            stats.invalid_records, stats.total_records
        );
        // Return ValidationFailed error to trigger exit code 3
        Err(MqttRecorderError::ValidationFailed(format!(
            "{} invalid records found",
            stats.invalid_records
        )))
    }
}

/// Run in fix mode.
///
/// Repairs a corrupted CSV file by detecting and re-encoding binary payloads.
///
/// # Requirements
/// - 8.2: WHEN fix mode is active, read the input file and write a repaired version to a new file
/// - 8.5: WHEN fix mode is active, report the number of records repaired
fn run_fix_mode(args: &Args) -> Result<(), MqttRecorderError> {
    eprintln!("Starting CSV fix mode...");

    // Get the file paths (validated by Args::validate())
    let input_path = args.file.as_ref().unwrap();
    let output_path = args.output.as_ref().unwrap();

    eprintln!("Input file: {:?}", input_path);
    eprintln!("Output file: {:?}", output_path);

    // Create fixer with appropriate settings
    // encode_b64: when true, all payloads in output will be base64 encoded
    let fixer = CsvFixer::new(args.encode_b64);

    // Run repair
    let stats = fixer.repair(input_path, output_path).map_err(|e| {
        eprintln!("Error: Failed to repair file {:?}: {}", input_path, e);
        e
    })?;

    // Print the repair report (Requirement 8.5)
    // The Display impl for RepairStats formats the report
    println!("{}", stats);

    // Exit with appropriate code
    // Exit code 0 on success, 3 on failure (when records were skipped)
    if stats.is_success() {
        eprintln!(
            "Repair complete. {} records processed, {} repaired.",
            stats.total_records, stats.repaired_records
        );
        Ok(())
    } else {
        eprintln!(
            "Repair completed with issues. {} records could not be repaired.",
            stats.skipped_records
        );
        // Return ValidationFailed error to trigger exit code 3
        Err(MqttRecorderError::ValidationFailed(format!(
            "{} records could not be repaired",
            stats.skipped_records
        )))
    }
}

/// Run in standalone broker mode.
///
/// Starts an embedded MQTT broker without any recording, replaying, or mirroring.
///
/// # Requirements
/// - 1.26: WHEN --serve is enabled without a mode, run the embedded broker in standalone mode
/// - 10.6: WHEN serve is enabled without replay mode, run as a standalone broker
async fn run_standalone_broker(
    args: &Args,
    mut shutdown: broadcast::Receiver<()>,
    tui_state: Option<std::sync::Arc<TuiState>>,
) -> Result<(), MqttRecorderError> {
    eprintln!("Starting standalone MQTT broker mode...");

    // Start the embedded broker
    let broker = EmbeddedBroker::new(args.serve_port, BrokerMode::Standalone).await?;

    let tui_active = tui_state.is_some();
    eprintln!(
        "Embedded broker running on port {}. Press Ctrl+C to stop.",
        args.serve_port
    );

    // Poll metrics while waiting for shutdown
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(2)) => {
                if let Some((old, new)) = broker.poll_metrics() {
                    if let Some(ref state) = tui_state {
                        state.set_broker_connections(new);
                    }
                    if !tui_active {
                        eprintln!("Broker connections: {} → {}", old, new);
                    }
                }
            }
        }
    }

    // Graceful shutdown (Requirement 9.5)
    if let Some(ref state) = tui_state {
        state.push_audit(AuditArea::Broker, AuditSeverity::Info, "Broker shutting down".into());
    }
    broker.shutdown().await?;
    if let Some(ref state) = tui_state {
        state.push_audit(AuditArea::Broker, AuditSeverity::Info, "Broker shutdown complete".into());
    }

    eprintln!("Standalone broker shutdown complete.");
    Ok(())
}

/// Run in record mode.
///
/// Subscribes to topics on an MQTT broker and records messages to a CSV file.
///
/// # Requirements
/// - 4.1-4.9: Message recording functionality
/// - 8.3: Log connection status changes to stderr
/// - 8.4: Log the number of messages recorded upon completion
async fn run_record_mode(
    args: &Args,
    shutdown: broadcast::Receiver<()>,
    tui_state: Option<std::sync::Arc<TuiState>>,
) -> Result<(), MqttRecorderError> {
    eprintln!("Starting record mode...");

    // Optionally start embedded broker for passthrough
    let _broker = if args.serve {
        eprintln!("Starting embedded broker on port {}...", args.serve_port);
        Some(EmbeddedBroker::new(args.serve_port, BrokerMode::Replay).await?)
    } else {
        None
    };

    // Create MQTT client configuration
    let client_config = create_mqtt_client_config(args)?;

    // Log connection attempt (Requirement 8.3)
    eprintln!(
        "Connecting to MQTT broker at {}:{} (MQTT v{})...",
        client_config.host, client_config.port, args.mqtt_version
    );

    // Create MQTT client (v5 by default, v4 if specified)
    let client = if args.use_mqtt_v5() {
        AnyMqttClient::V5(MqttClientV5::new(client_config).await.map_err(|e| {
            eprintln!("Error: Connection failed: {}", e);
            eprintln!("  Hint: Verify the broker is running and the host/port are correct");
            e
        })?)
    } else {
        AnyMqttClient::V4(MqttClient::new(client_config).await.map_err(|e| {
            eprintln!("Error: Connection failed: {}", e);
            eprintln!("  Hint: Verify the broker is running and the host/port are correct");
            e
        })?)
    };

    // Create CSV writer (Requirement 4.5: writes header row)
    let file_path = args.file.clone().unwrap_or_else(|| {
        std::path::PathBuf::from(crate::tui::generate_default_filename(args.host.as_deref()))
    });
    let writer = CsvWriter::new(&file_path, args.encode_b64).map_err(|e| {
        eprintln!("Error: Failed to create output file {:?}: {}", file_path, e);
        e
    })?;

    // Create topic filter
    let topics = create_topic_filter(args)?;

    // Create and run recorder
    let mut recorder = Recorder::new(client, writer, topics, args.get_qos()).await;

    // Run the recording loop (Requirements 4.1-4.9)
    let message_count = recorder.run_with_tui(shutdown, tui_state).await?;

    // Log completion (Requirement 8.4)
    eprintln!("Recording complete. {} messages recorded.", message_count);

    Ok(())
}

/// Run in replay mode.
///
/// Reads messages from a CSV file and publishes them to a broker.
/// Optionally starts an embedded broker if --serve is enabled.
///
/// # Requirements
/// - 5.1-5.11: Message replay functionality
/// - 1.24: WHEN --serve is enabled in replay mode with --host provided, publish to both
/// - 8.3: Log connection status changes to stderr
/// - 8.4: Log the number of messages replayed upon completion
async fn run_replay_mode(
    args: &Args,
    shutdown: broadcast::Receiver<()>,
    tui_state: Option<std::sync::Arc<TuiState>>,
) -> Result<(), MqttRecorderError> {
    let tui_active = tui_state.is_some();
    if !tui_active {
        eprintln!("Starting replay mode...");
    }

    // Optionally start embedded broker (Requirement 10.3)
    let broker = if args.serve {
        if !tui_active {
            eprintln!("Starting embedded broker on port {}...", args.serve_port);
        }
        Some(EmbeddedBroker::new(args.serve_port, BrokerMode::Replay).await?)
    } else {
        None
    };

    // Determine which client to use for publishing
    let client = if let Some(host) = &args.host {
        // Connect to external broker (v5 by default, v4 if specified)
        let client_config = create_mqtt_client_config(args)?;
        if !tui_active {
            eprintln!(
                "Connecting to MQTT broker at {}:{} (MQTT v{})...",
                host, args.port, args.mqtt_version
            );
        }
        if args.use_mqtt_v5() {
            AnyMqttClient::V5(MqttClientV5::new(client_config).await.map_err(|e| {
                eprintln!("Error: Connection failed: {}", e);
                eprintln!("  Hint: Verify the broker is running and the host/port are correct");
                e
            })?)
        } else {
            AnyMqttClient::V4(MqttClient::new(client_config).await.map_err(|e| {
                eprintln!("Error: Connection failed: {}", e);
                eprintln!("  Hint: Verify the broker is running and the host/port are correct");
                e
            })?)
        }
    } else if args.serve {
        // Connect to embedded broker via v5 (Requirement 1.22: host optional with --serve)
        let config = MqttClientConfig::new(
            "127.0.0.1".to_string(),
            args.serve_port,
            generate_client_id(&args.client_id),
        );
        if !tui_active {
            eprintln!(
                "Connecting to embedded broker on port {}...",
                args.serve_port
            );
        }
        let c = MqttClientV5::new(config).await?;
        if let Some(ref b) = broker {
            b.register_internal_client();
        }
        AnyMqttClient::V5(c)
    } else {
        // This shouldn't happen due to validation, but handle it anyway
        return Err(MqttRecorderError::InvalidArgument(
            "No broker specified for replay mode".to_string(),
        ));
    };

    // Create CSV reader
    let file_path = args.file.as_ref().unwrap();
    let reader =
        CsvReader::new(file_path, args.encode_b64, args.csv_field_size_limit).map_err(|e| {
            eprintln!("Error: Failed to open input file {:?}: {}", file_path, e);
            e
        })?;

    // Create and run replayer
    let mut replayer = Replayer::new(client, reader, args.loop_replay).await;

    // Run the replay loop (Requirements 5.1-5.11)
    let tui_ref = tui_state.clone();
    let message_count = replayer.run_with_tui(shutdown, tui_state).await?;

    // Log completion (Requirement 8.4)
    if !tui_active {
        eprintln!("Replay complete. {} messages replayed.", message_count);
    }

    // Shutdown embedded broker if started
    if let Some(broker) = broker {
        if let Some(ref state) = tui_ref {
            state.push_audit(AuditArea::Broker, AuditSeverity::Info, "Broker shutting down".into());
        }
        broker.shutdown().await?;
        if let Some(ref state) = tui_ref {
            state.push_audit(AuditArea::Broker, AuditSeverity::Info, "Broker shutdown complete".into());
        }
    }

    Ok(())
}

/// Run in mirror mode.
///
/// Subscribes to an external broker and republishes messages to an embedded broker.
/// Optionally records messages to a CSV file.
///
/// # Requirements
/// - 11.1-11.9: Mirror mode functionality
/// - 8.3: Log connection status changes to stderr
/// - 8.4: Log the number of messages mirrored upon completion
async fn run_mirror_mode(
    args: &Args,
    shutdown: broadcast::Receiver<()>,
    tui_state: Option<std::sync::Arc<TuiState>>,
) -> Result<(), MqttRecorderError> {
    let tui_active = tui_state.is_some();
    if !tui_active {
        eprintln!("Starting mirror mode...");
    }

    // Start embedded broker (Requirement 11.2: mirror mode requires --serve)
    let broker = EmbeddedBroker::new(args.serve_port, BrokerMode::Mirror).await?;

    // Create source client configuration (connect to external broker)
    let source_config = create_mqtt_client_config(args)?;

    // Log connection attempt (Requirement 8.3)
    if !tui_active {
        eprintln!(
            "Connecting to source MQTT broker at {}:{} (MQTT v{})...",
            source_config.host, source_config.port, args.mqtt_version
        );
    }

    // Create source MQTT client (v5 by default, v4 if specified)
    let source_client = if args.use_mqtt_v5() {
        AnyMqttClient::V5(MqttClientV5::new(source_config).await.map_err(|e| {
            eprintln!("Error: Connection to source broker failed: {}", e);
            eprintln!("  Hint: Verify the broker is running and the host/port are correct");
            e
        })?)
    } else {
        AnyMqttClient::V4(MqttClient::new(source_config).await.map_err(|e| {
            eprintln!("Error: Connection to source broker failed: {}", e);
            eprintln!("  Hint: Verify the broker is running and the host/port are correct");
            e
        })?)
    };

    // Optionally create CSV writer (Requirement 11.5)
    let writer = if let Some(file_path) = &args.file {
        Some(CsvWriter::new(file_path, args.encode_b64).map_err(|e| {
            eprintln!("Error: Failed to create output file {:?}: {}", file_path, e);
            e
        })?)
    } else {
        None
    };

    // Create topic filter
    let topics = create_topic_filter(args)?;

    // Create and run mirror
    let mut mirror = Mirror::new(source_client, broker, writer, topics, args.get_qos()).await?;

    // Run the mirror loop with TUI support (Requirements 11.3-11.9)
    let tui_ref = tui_state.clone();
    let message_count = if tui_active {
        mirror.run_with_tui(shutdown, tui_state).await?
    } else {
        mirror.run(shutdown).await?
    };

    // Log completion (Requirement 8.4)
    if !tui_active {
        eprintln!("Mirror complete. {} messages mirrored.", message_count);
    }

    // Shutdown embedded broker
    if let Some(ref state) = tui_ref {
        state.push_audit(AuditArea::Broker, AuditSeverity::Info, "Broker shutting down".into());
    }
    let broker = mirror.into_broker();
    broker.shutdown().await?;
    if let Some(ref state) = tui_ref {
        state.push_audit(AuditArea::Broker, AuditSeverity::Info, "Broker shutdown complete".into());
    }

    Ok(())
}

/// Create an MQTT client configuration from CLI arguments.
///
/// # Requirements
/// - 2.1-2.8: MQTT broker connection configuration
/// - 1.25: WHEN --serve is enabled, ignore TLS options for the embedded broker connection
fn create_mqtt_client_config(args: &Args) -> Result<MqttClientConfig, MqttRecorderError> {
    let host = args
        .host
        .as_ref()
        .ok_or_else(|| MqttRecorderError::InvalidArgument("Host is required".to_string()))?;

    let client_id = generate_client_id(&args.client_id);

    let mut config = MqttClientConfig::new(host.clone(), args.port, client_id)
        .with_max_packet_size(args.max_packet_size);

    // Set credentials if provided (Requirement 2.4)
    if let (Some(username), Some(password)) = (&args.username, &args.password) {
        config = config.with_credentials(username.clone(), password.clone());
    }

    // Configure TLS if enabled (Requirements 2.5-2.8)
    // Note: TLS options are ignored for embedded broker connections (Requirement 1.25)
    if args.enable_ssl {
        let mut tls_config = TlsConfig::new();

        if let Some(ca_cert) = &args.ca_cert {
            tls_config = tls_config.with_ca_cert(ca_cert.clone());
        }

        if let (Some(certfile), Some(keyfile)) = (&args.certfile, &args.keyfile) {
            tls_config = tls_config.with_client_auth(certfile.clone(), keyfile.clone());
        }

        if args.tls_insecure {
            tls_config = tls_config.with_insecure(true);
        }

        config = config.with_tls(tls_config);
    }

    Ok(config)
}

/// Create a topic filter from CLI arguments.
///
/// # Requirements
/// - 3.1: WHEN no topic arguments are provided, subscribe to wildcard topic `#`
/// - 3.2: WHEN a single topic is provided via `-t` or `--topic`, subscribe only to that topic
/// - 3.3: WHEN a topics JSON file is provided, parse the file and subscribe to all listed topics
fn create_topic_filter(args: &Args) -> Result<TopicFilter, MqttRecorderError> {
    if let Some(topic) = &args.topic {
        // Single topic from CLI (Requirement 3.2)
        Ok(TopicFilter::from_single(topic.clone()))
    } else if let Some(topics_file) = &args.topics {
        // Topics from JSON file (Requirement 3.3)
        TopicFilter::from_json_file(topics_file).map_err(|e| {
            eprintln!(
                "Error: Failed to parse topics file {:?}: {}",
                topics_file, e
            );
            e
        })
    } else {
        // Default wildcard subscription (Requirement 3.1)
        Ok(TopicFilter::wildcard())
    }
}

/// Generate a client ID, using the provided one or generating a unique one.
///
/// # Requirement
/// - 2.3: WHEN no client_id is provided, generate a unique client identifier
fn generate_client_id(client_id: &Option<String>) -> String {
    client_id.clone().unwrap_or_else(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let hash = timestamp ^ (timestamp >> 32);
        format!("mqtt-recorder-{:08x}", hash as u32)
    })
}

/// Convert an error to the appropriate exit code.
///
/// # Requirement
/// - 8.2: Use appropriate exit codes
fn error_to_exit_code(error: &MqttRecorderError) -> u8 {
    match error {
        MqttRecorderError::InvalidArgument(_) => EXIT_CONFIG_ERROR,
        MqttRecorderError::ValidationFailed(_) => EXIT_VALIDATION_FAILURE,
        MqttRecorderError::Connection(_) => EXIT_CONNECTION_ERROR,
        MqttRecorderError::Client(_) => EXIT_CONNECTION_ERROR,
        MqttRecorderError::Tls(_) => EXIT_CONNECTION_ERROR,
        MqttRecorderError::Io(_) => EXIT_IO_ERROR,
        MqttRecorderError::Csv(_) => EXIT_IO_ERROR,
        MqttRecorderError::Json(_) => EXIT_IO_ERROR,
        MqttRecorderError::Broker(_) => EXIT_RUNTIME_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create Args with defaults for testing
    fn default_args() -> Args {
        Args {
            host: None,
            port: 1883,
            client_id: None,
            mode: None,
            file: None,
            loop_replay: false,
            qos: 0,
            topics: None,
            topic: None,
            enable_ssl: false,
            tls_insecure: false,
            ca_cert: None,
            certfile: None,
            keyfile: None,
            username: None,
            password: None,
            encode_b64: false,
            csv_field_size_limit: None,
            max_packet_size: 1048576,
            serve: false,
            serve_port: 1883,
            validate: false,
            fix: false,
            output: None,
            no_interactive: false,
            record: None,
            mirror: true,
            playlist: vec![],
            audit: true,
            audit_log: None,
            mqtt_version: "5".to_string(),
        }
    }

    #[test]
    fn test_error_to_exit_code_config_error() {
        let error = MqttRecorderError::InvalidArgument("test".to_string());
        assert_eq!(error_to_exit_code(&error), EXIT_CONFIG_ERROR);
    }

    #[test]
    fn test_error_to_exit_code_validation_failure() {
        let error = MqttRecorderError::ValidationFailed("5 invalid records".to_string());
        assert_eq!(error_to_exit_code(&error), EXIT_VALIDATION_FAILURE);
    }

    #[test]
    fn test_error_to_exit_code_connection_error() {
        let error = MqttRecorderError::Tls("test".to_string());
        assert_eq!(error_to_exit_code(&error), EXIT_CONNECTION_ERROR);
    }

    #[test]
    fn test_error_to_exit_code_io_error() {
        let error =
            MqttRecorderError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "test"));
        assert_eq!(error_to_exit_code(&error), EXIT_IO_ERROR);

        let error = MqttRecorderError::Json(serde_json::from_str::<()>("invalid").unwrap_err());
        assert_eq!(error_to_exit_code(&error), EXIT_IO_ERROR);
    }

    #[test]
    fn test_error_to_exit_code_runtime_error() {
        let error = MqttRecorderError::Broker("test".to_string());
        assert_eq!(error_to_exit_code(&error), EXIT_RUNTIME_ERROR);
    }

    #[test]
    fn test_generate_client_id_with_provided_id() {
        let client_id = Some("my-client".to_string());
        let result = generate_client_id(&client_id);
        assert_eq!(result, "my-client");
    }

    #[test]
    fn test_generate_client_id_without_provided_id() {
        let client_id = None;
        let result = generate_client_id(&client_id);
        assert!(result.starts_with("mqtt-recorder-"));
        assert_eq!(result.len(), 22); // "mqtt-recorder-" (14) + 8 hex chars
    }

    #[test]
    fn test_create_topic_filter_single_topic() {
        let mut args = default_args();
        args.topic = Some("test/topic".to_string());

        let filter = create_topic_filter(&args).unwrap();
        assert_eq!(filter.topics(), &["test/topic"]);
    }

    #[test]
    fn test_create_topic_filter_wildcard() {
        let args = default_args();

        let filter = create_topic_filter(&args).unwrap();
        assert_eq!(filter.topics(), &["#"]);
    }

    #[test]
    fn test_create_mqtt_client_config_basic() {
        let mut args = default_args();
        args.host = Some("localhost".to_string());
        args.port = 1883;
        args.client_id = Some("test-client".to_string());

        let config = create_mqtt_client_config(&args).unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1883);
        assert_eq!(config.client_id, "test-client");
        assert!(!config.is_tls_enabled());
        assert!(!config.has_credentials());
    }

    #[test]
    fn test_create_mqtt_client_config_with_credentials() {
        let mut args = default_args();
        args.host = Some("localhost".to_string());
        args.username = Some("user".to_string());
        args.password = Some("pass".to_string());

        let config = create_mqtt_client_config(&args).unwrap();
        assert!(config.has_credentials());
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
    }

    #[test]
    fn test_create_mqtt_client_config_with_tls() {
        let mut args = default_args();
        args.host = Some("localhost".to_string());
        args.enable_ssl = true;
        args.tls_insecure = true;

        let config = create_mqtt_client_config(&args).unwrap();
        assert!(config.is_tls_enabled());
    }

    #[test]
    fn test_create_mqtt_client_config_missing_host() {
        let args = default_args();

        let result = create_mqtt_client_config(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_exit_codes_values() {
        assert_eq!(EXIT_SUCCESS, 0);
        assert_eq!(EXIT_CONFIG_ERROR, 1);
        assert_eq!(EXIT_CONNECTION_ERROR, 2);
        assert_eq!(EXIT_IO_ERROR, 3);
        assert_eq!(EXIT_RUNTIME_ERROR, 4);
    }
}
