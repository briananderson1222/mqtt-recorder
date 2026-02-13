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
mod util;
mod validator;

use clap::Parser;
use std::process::ExitCode;
use tokio::sync::broadcast;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;

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
use tui::{should_enable_interactive, AuditArea, AuditSeverity, TuiConfig, TuiState};
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
async fn run(mut args: Args) -> Result<(), MqttRecorderError> {
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
            error!("Error setting up signal handler: {}", e);
        }
        // Send shutdown signal to all receivers
        let _ = shutdown_tx_signal.send(());
    });

    // Determine if TUI should be enabled (only for long-running serve modes)
    let enable_tui = args.serve && should_enable_interactive(args.no_interactive);

    // Resolve default filename for record mode when --file not provided
    if matches!(args.mode.as_ref(), Some(Mode::Record)) && args.file.is_none() {
        args.file = Some(std::path::PathBuf::from(
            crate::tui::generate_default_filename(args.host.as_deref()),
        ));
    }

    // Initialize tracing subscriber only when TUI is not active
    if !enable_tui {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("mqtt_recorder=info")),
            )
            .with_writer(std::io::stderr)
            .try_init();
    }

    // Create TUI state for serve modes (tracks counters, audit, verify even without TUI)
    let tui_state = create_tui_state(&args);

    let tui_handle = spawn_tui_task(enable_tui, &tui_state, &shutdown_tx);

    // Dispatch to the appropriate mode handler
    // When --serve + --host are both present, always use mirror event loop
    // so all TUI toggles (mirror/record/playback) actually work.
    // --mode just sets initial toggle states.
    let result = if args.is_standalone_broker() {
        run_standalone_broker(&args, shutdown_tx.subscribe(), tui_state.clone()).await
    } else if args.serve && args.host.is_some() {
        // Unified serve path: mirror event loop handles all modes
        run_mirror_mode(&args, shutdown_tx.subscribe(), tui_state.clone()).await
    } else {
        match args
            .mode
            .as_ref()
            .expect("mode validated by Args::validate()")
        {
            Mode::Record => {
                run_record_mode(&args, shutdown_tx.subscribe(), tui_state.clone()).await
            }
            Mode::Replay => {
                run_replay_mode(&args, shutdown_tx.subscribe(), tui_state.clone()).await
            }
            Mode::Mirror => {
                // Mirror requires --serve + --host, handled above
                unreachable!("mirror mode requires --serve and --host")
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
                info!("Received SIGINT (Ctrl+C), initiating graceful shutdown...");
            }
            _ = sigterm.recv() => {
                info!("Received SIGTERM, initiating graceful shutdown...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        use tokio::signal;
        // On non-Unix platforms, only handle Ctrl+C
        signal::ctrl_c().await.map_err(MqttRecorderError::Io)?;
        info!("Received Ctrl+C, initiating graceful shutdown...");
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
    info!("Starting CSV validation mode...");

    // Get the file path (validated by Args::validate())
    let file_path = args
        .file
        .as_ref()
        .expect("--file validated by Args::validate()");

    info!("Validating file: {:?}", file_path);

    // Create validator with appropriate settings
    // decode_b64: when true, all payloads are expected to be base64 encoded
    // field_size_limit: optional maximum field size
    let validator = CsvValidator::new(args.encode_b64, args.csv_field_size_limit);

    // Run validation
    let stats = validator.validate(file_path).map_err(|e| {
        error!("Failed to validate file {:?}: {}", file_path, e);
        e
    })?;

    // Print the validation report (Requirements 6.1-6.6)
    // The Display impl for ValidationStats formats the report
    println!("{}", stats);

    // Exit with appropriate code (Requirements 4.9, 4.10)
    if stats.is_valid() {
        info!(
            "Validation complete. All {} records are valid.",
            stats.valid_records
        );
        Ok(())
    } else {
        error!(
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
    info!("Starting CSV fix mode...");

    // Get the file paths (validated by Args::validate())
    let input_path = args
        .file
        .as_ref()
        .expect("--file validated by Args::validate()");
    let output_path = args
        .output
        .as_ref()
        .expect("--output validated by Args::validate()");

    info!("Input file: {:?}", input_path);
    info!("Output file: {:?}", output_path);

    // Create fixer with appropriate settings
    // encode_b64: when true, all payloads in output will be base64 encoded
    let fixer = CsvFixer::new(args.encode_b64);

    // Run repair
    let stats = fixer.repair(input_path, output_path).map_err(|e| {
        error!("Failed to repair file {:?}: {}", input_path, e);
        e
    })?;

    // Print the repair report (Requirement 8.5)
    // The Display impl for RepairStats formats the report
    println!("{}", stats);

    // Exit with appropriate code
    // Exit code 0 on success, 3 on failure (when records were skipped)
    if stats.is_success() {
        info!(
            "Repair complete. {} records processed, {} repaired.",
            stats.total_records, stats.repaired_records
        );
        Ok(())
    } else {
        error!(
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
    info!("Starting standalone MQTT broker mode...");

    // Start the embedded broker
    let broker = EmbeddedBroker::new(args.serve_port, BrokerMode::Standalone).await?;

    info!(
        "Embedded broker running on port {}. Press Ctrl+C to stop.",
        args.serve_port
    );

    // Poll metrics while waiting for shutdown
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(crate::util::BROKER_SHUTDOWN_TIMEOUT_SECS)) => {
                if let Some(metrics) = broker.poll_metrics() {
                    if let Some(ref state) = tui_state {
                        state.update_broker_metrics(&metrics);
                    }
                    if let Some((old, new)) = metrics.connections_changed {
                        debug!("Broker connections: {} → {}", old, new);
                    }
                }
            }
        }
    }

    // Graceful shutdown (Requirement 9.5)
    shutdown_broker_with_audit(broker, &tui_state).await?;

    info!("Standalone broker shutdown complete.");
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
    info!("Starting record mode...");

    // Create MQTT client configuration
    let client_config = create_mqtt_client_config(args)?;

    // Log connection attempt (Requirement 8.3)
    info!(
        "Connecting to MQTT broker at {}:{} (MQTT v{})...",
        client_config.host, client_config.port, args.mqtt_version
    );

    // Create MQTT client (v5 by default, v4 if specified)
    let client = create_any_mqtt_client(client_config, args.use_mqtt_v5())
        .await
        .map_err(|e| {
            error!("Connection failed: {}", e);
            info!("Hint: Verify the broker is running and the host/port are correct");
            e
        })?;

    // Create CSV writer (Requirement 4.5: writes header row)
    // args.file is already resolved (default filename generated earlier if needed)
    let file_path = args
        .file
        .as_ref()
        .expect("--file resolved by run() for record mode");
    let writer = CsvWriter::new(file_path, args.encode_b64).map_err(|e| {
        error!("Failed to create output file {:?}: {}", file_path, e);
        e
    })?;

    // Create topic filter
    let topics = create_topic_filter(args)?;

    // Create and run recorder
    let mut recorder = Recorder::new(client, writer, topics, args.get_qos());

    // Run the recording loop (Requirements 4.1-4.9)
    let message_count = recorder.run(shutdown, tui_state).await?;

    // Log completion (Requirement 8.4)
    info!("Recording complete. {} messages recorded.", message_count);

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
    info!("Starting replay mode...");

    // Optionally start embedded broker (Requirement 10.3)
    let broker = if args.serve {
        info!("Starting embedded broker on port {}...", args.serve_port);
        Some(EmbeddedBroker::new(args.serve_port, BrokerMode::Replay).await?)
    } else {
        None
    };

    // Determine which client to use for publishing
    let client = if let Some(host) = &args.host {
        // Connect to external broker (v5 by default, v4 if specified)
        let client_config = create_mqtt_client_config(args)?;
        info!(
            "Connecting to MQTT broker at {}:{} (MQTT v{})...",
            host, args.port, args.mqtt_version
        );
        create_any_mqtt_client(client_config, args.use_mqtt_v5())
            .await
            .map_err(|e| {
                error!("Connection failed: {}", e);
                info!("Hint: Verify the broker is running and the host/port are correct");
                e
            })?
    } else if args.serve {
        // Connect to embedded broker via v5 (Requirement 1.22: host optional with --serve)
        let config = MqttClientConfig::new(
            "127.0.0.1".to_string(),
            args.serve_port,
            crate::util::generate_client_id(&args.client_id),
        );
        info!(
            "Connecting to embedded broker on port {}...",
            args.serve_port
        );
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
    let file_path = args
        .file
        .as_ref()
        .expect("--file validated by Args::validate()");
    let reader =
        CsvReader::new(file_path, args.encode_b64, args.csv_field_size_limit).map_err(|e| {
            error!("Failed to open input file {:?}: {}", file_path, e);
            e
        })?;

    // Create and run replayer
    let mut replayer = Replayer::new(client, reader, args.loop_replay);

    // Run the replay loop (Requirements 5.1-5.11)
    let tui_ref = tui_state.clone();
    let message_count = replayer.run(shutdown, tui_state).await?;

    // Log completion (Requirement 8.4)
    info!("Replay complete. {} messages replayed.", message_count);

    // Shutdown embedded broker if started
    if let Some(broker) = broker {
        shutdown_broker_with_audit(broker, &tui_ref).await?;
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
    info!("Starting mirror mode...");

    // Start embedded broker (Requirement 11.2: mirror mode requires --serve)
    let broker = EmbeddedBroker::new(args.serve_port, BrokerMode::Mirror).await?;

    // Create source client configuration (connect to external broker)
    let source_config = create_mqtt_client_config(args)?;

    // Log connection attempt (Requirement 8.3)
    info!(
        "Connecting to source MQTT broker at {}:{} (MQTT v{})...",
        source_config.host, source_config.port, args.mqtt_version
    );

    // Create source MQTT client (v5 by default, v4 if specified)
    let source_client = create_any_mqtt_client(source_config, args.use_mqtt_v5())
        .await
        .map_err(|e| {
            error!("Connection to source broker failed: {}", e);
            info!("Hint: Verify the broker is running and the host/port are correct");
            e
        })?;

    // Create CSV writer - use args.file, or fall back to TUI state's file path
    // (which may have been generated as a default for record mode)
    let resolved_file: Option<std::path::PathBuf> = args.file.clone().or_else(|| {
        tui_state
            .as_ref()
            .and_then(|s| s.get_file_path())
            .map(std::path::PathBuf::from)
    });
    let writer = if let Some(ref file_path) = resolved_file {
        Some(CsvWriter::new(file_path, args.encode_b64).map_err(|e| {
            error!("Failed to create output file {:?}: {}", file_path, e);
            e
        })?)
    } else {
        None
    };

    // Create topic filter
    let topics = create_topic_filter(args)?;

    // Create and run mirror
    let mut mirror = Mirror::new(
        source_client,
        broker,
        writer,
        topics,
        args.get_qos(),
        args.verify,
    )
    .await?;

    // Run the mirror loop with TUI support (Requirements 11.3-11.9)
    let tui_ref = tui_state.clone();
    let message_count = mirror.run(shutdown, tui_state).await?;

    // Log completion (Requirement 8.4)
    info!("Mirror complete. {} messages mirrored.", message_count);

    // Print verify summary if active
    if args.verify {
        if let Some(ref state) = tui_ref {
            let matched = state.get_verify_matched();
            let mismatched = state.get_verify_mismatched();
            let missing = state.get_verify_missing();
            info!(
                "Verify: {} matched, {} unexpected, {} missing",
                matched, mismatched, missing
            );
        }
    }

    // Shutdown embedded broker
    let broker = mirror.into_broker();
    shutdown_broker_with_audit(broker, &tui_ref).await?;

    Ok(())
}

/// Shut down an embedded broker with audit logging.
async fn shutdown_broker_with_audit(
    broker: EmbeddedBroker,
    tui_state: &Option<std::sync::Arc<TuiState>>,
) -> Result<(), MqttRecorderError> {
    if let Some(ref state) = tui_state {
        state.push_audit(
            AuditArea::Broker,
            AuditSeverity::Info,
            "Broker shutting down".into(),
        );
    }
    broker.shutdown().await?;
    if let Some(ref state) = tui_state {
        state.push_audit(
            AuditArea::Broker,
            AuditSeverity::Info,
            "Broker shutdown complete".into(),
        );
    }
    Ok(())
}

/// Spawn the TUI rendering task if interactive mode is enabled and TUI state exists.
fn spawn_tui_task(
    enable_tui: bool,
    tui_state: &Option<std::sync::Arc<TuiState>>,
    shutdown_tx: &broadcast::Sender<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !enable_tui {
        return None;
    }
    let state = tui_state.as_ref()?;
    let state_clone = state.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    let shutdown_rx = shutdown_tx.subscribe();
    Some(tokio::spawn(async move {
        if let Err(e) = tui::run_tui(state_clone, shutdown_tx_clone, shutdown_rx).await {
            error!("TUI error: {}", e);
        }
    }))
}

/// Create TUI state for serve modes (tracks counters, audit, verify even without TUI).
///
/// Returns `None` when `--serve` is not enabled.
fn create_tui_state(args: &Args) -> Option<std::sync::Arc<TuiState>> {
    if !args.serve {
        return None;
    }

    let mode = args.mode.as_ref();
    let file_path = args.file.as_ref().map(|p| p.display().to_string());

    // Set initial toggle states based on --mode
    let (initial_record, initial_mirror) = match mode {
        Some(Mode::Record) => (args.record.or(Some(true)), false),
        Some(Mode::Replay) => (Some(false), false),
        Some(Mode::Mirror) => (args.record, args.mirror),
        None => (Some(false), false), // standalone broker
    };

    let playlist: Vec<String> = args
        .playlist
        .iter()
        .map(|p| p.display().to_string())
        .collect();
    let tui = std::sync::Arc::new(TuiState::new(TuiConfig {
        broker_port: args.serve_port,
        file_path,
        source_host: args.host.clone(),
        source_port: args.port,
        initial_record,
        initial_mirror,
        playlist,
        audit_enabled: args.audit,
        health_check_interval: args.health_check,
    }));
    if let Some(ref path) = args.audit_log {
        tui.set_audit_file_path(path.display().to_string());
        tui.enable_audit_file();
    }
    // Set playback active for replay mode, or when --loop is explicitly provided
    let is_replay = matches!(mode, Some(Mode::Replay));
    let has_playback_file = tui.get_file_path().is_some() || !args.playlist.is_empty();
    if (is_replay || args.loop_replay) && has_playback_file {
        tui.loop_enabled
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if args.loop_replay {
            tui.playback_looping
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        tui.recording_enabled
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
    Some(tui)
}

/// Build an [`MqttClientConfig`] from CLI arguments for connecting to an external broker.
///
/// # Requirements
/// - 2.1-2.8: MQTT broker connection configuration
/// - 1.25: WHEN --serve is enabled, ignore TLS options for the embedded broker connection
fn create_mqtt_client_config(args: &Args) -> Result<MqttClientConfig, MqttRecorderError> {
    let host = args
        .host
        .as_ref()
        .ok_or_else(|| MqttRecorderError::InvalidArgument("Host is required".to_string()))?;

    let client_id = crate::util::generate_client_id(&args.client_id);

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

/// Create an MQTT client from configuration, using v5 or v4 based on the flag.
async fn create_any_mqtt_client(
    config: MqttClientConfig,
    use_v5: bool,
) -> Result<AnyMqttClient, MqttRecorderError> {
    let client = if use_v5 {
        AnyMqttClient::V5(MqttClientV5::new(config).await?)
    } else {
        AnyMqttClient::V4(MqttClient::new(config).await?)
    };
    Ok(client)
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
            error!("Failed to parse topics file {:?}: {}", topics_file, e);
            e
        })
    } else {
        // Default wildcard subscription (Requirement 3.1)
        Ok(TopicFilter::wildcard())
    }
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
        MqttRecorderError::V5Connection(_) => EXIT_CONNECTION_ERROR,
        MqttRecorderError::V5Client(_) => EXIT_CONNECTION_ERROR,
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
    fn test_create_topic_filter_single_topic() {
        let args = Args {
            topic: Some("test/topic".to_string()),
            ..Default::default()
        };

        let filter = create_topic_filter(&args).unwrap();
        assert_eq!(filter.topics(), &["test/topic"]);
    }

    #[test]
    fn test_create_topic_filter_wildcard() {
        let args = Args::default();

        let filter = create_topic_filter(&args).unwrap();
        assert_eq!(filter.topics(), &["#"]);
    }

    #[test]
    fn test_create_mqtt_client_config_basic() {
        let args = Args {
            host: Some("localhost".to_string()),
            port: 1883,
            client_id: Some("test-client".to_string()),
            ..Default::default()
        };

        let config = create_mqtt_client_config(&args).unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1883);
        assert_eq!(config.client_id, "test-client");
        assert!(!config.is_tls_enabled());
        assert!(!config.has_credentials());
    }

    #[test]
    fn test_create_mqtt_client_config_with_credentials() {
        let args = Args {
            host: Some("localhost".to_string()),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            ..Default::default()
        };

        let config = create_mqtt_client_config(&args).unwrap();
        assert!(config.has_credentials());
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
    }

    #[test]
    fn test_create_mqtt_client_config_with_tls() {
        let args = Args {
            host: Some("localhost".to_string()),
            enable_ssl: true,
            tls_insecure: true,
            ..Default::default()
        };

        let config = create_mqtt_client_config(&args).unwrap();
        assert!(config.is_tls_enabled());
    }

    #[test]
    fn test_create_mqtt_client_config_missing_host() {
        let args = Args::default();

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
