//! CLI argument parsing module
//!
//! Handles command-line argument parsing using `clap` derive macros.
//! This module defines the `Mode` enum for operation modes and the `Args` struct
//! containing all CLI arguments with validation logic.

use clap::{Parser, ValueEnum};
use rumqttc::QoS;
use std::path::PathBuf;

/// Operation mode for the MQTT Recorder.
///
/// Defines the three main operational modes:
/// - **Record**: Subscribe to topics and save messages to a CSV file
/// - **Replay**: Read messages from a CSV file and publish them to a broker
/// - **Mirror**: Subscribe to an external broker and republish to an embedded broker
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// Record MQTT messages to a CSV file
    Record,
    /// Replay MQTT messages from a CSV file
    Replay,
    /// Mirror messages from external broker to embedded broker
    Mirror,
}

/// Command-line arguments for the MQTT Recorder.
///
/// This struct defines all CLI arguments using clap derive macros.
/// Use the `validate()` method after parsing to ensure argument combinations are valid.
///
/// # Example
///
/// ```rust,ignore
/// use clap::Parser;
/// use mqtt_recorder::cli::Args;
///
/// let args = Args::parse();
/// args.validate()?;
/// ```
#[derive(Parser, Debug)]
#[command(name = "mqtt-recorder")]
#[command(about = "Record and replay MQTT messages")]
#[command(version)]
pub struct Args {
    /// MQTT broker address (required unless --serve in replay mode)
    #[arg(long)]
    pub host: Option<String>,

    /// MQTT broker port
    #[arg(long, default_value = "1883")]
    pub port: u16,

    /// MQTT client ID
    #[arg(long)]
    pub client_id: Option<String>,

    /// Operation mode: record, replay, or mirror (optional if --serve alone)
    #[arg(long, value_enum)]
    pub mode: Option<Mode>,

    /// CSV file path for recording/replaying (required for record/replay, optional for mirror)
    #[arg(long)]
    pub file: Option<PathBuf>,

    /// Loop replay continuously
    #[arg(long = "loop", default_value = "false")]
    pub loop_replay: bool,

    /// QoS level for subscriptions (0, 1, or 2)
    #[arg(long, default_value = "0")]
    pub qos: u8,

    /// JSON file containing topics to subscribe
    #[arg(long)]
    pub topics: Option<PathBuf>,

    /// Single topic to subscribe
    #[arg(short = 't', long)]
    pub topic: Option<String>,

    /// Enable TLS/SSL
    #[arg(long, default_value = "false")]
    pub enable_ssl: bool,

    /// Skip TLS certificate verification
    #[arg(long, default_value = "false")]
    pub tls_insecure: bool,

    /// Path to CA certificate
    #[arg(long)]
    pub ca_cert: Option<PathBuf>,

    /// Path to client certificate
    #[arg(long)]
    pub certfile: Option<PathBuf>,

    /// Path to client private key
    #[arg(long)]
    pub keyfile: Option<PathBuf>,

    /// MQTT username
    #[arg(long)]
    pub username: Option<String>,

    /// MQTT password
    #[arg(long)]
    pub password: Option<String>,

    /// Encode payloads as base64
    #[arg(long, default_value = "false")]
    pub encode_b64: bool,

    /// CSV field size limit
    #[arg(long)]
    pub csv_field_size_limit: Option<usize>,

    /// Maximum MQTT packet size in bytes (default: 1MB)
    #[arg(long, default_value = "1048576")]
    pub max_packet_size: usize,

    /// Start embedded MQTT broker
    #[arg(long, default_value = "false")]
    pub serve: bool,

    /// Embedded broker port
    #[arg(long, default_value = "1883")]
    pub serve_port: u16,

    /// Validate CSV file without replaying
    #[arg(long, default_value = "false")]
    pub validate: bool,

    /// Repair corrupted CSV file
    #[arg(long, default_value = "false")]
    pub fix: bool,

    /// Output file path for --fix mode
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Disable interactive TUI mode
    #[arg(long, default_value = "false")]
    pub no_interactive: bool,

    /// Start with recording enabled (default: true if --file provided)
    #[arg(long)]
    pub record: Option<bool>,

    /// Start with mirroring enabled (default: true)
    #[arg(long, default_value = "true")]
    pub mirror: bool,

    /// Additional CSV files for playback selection (can be specified multiple times)
    #[arg(long = "playlist")]
    pub playlist: Vec<PathBuf>,

    /// Enable audit logging in TUI (default: true)
    #[arg(long, default_value = "true")]
    pub audit: bool,

    /// Path to write audit log file (auto-enables file writing)
    #[arg(long)]
    pub audit_log: Option<PathBuf>,

    /// MQTT protocol version for external broker connections (3.1.1 or 5)
    #[arg(long, default_value = "5")]
    pub mqtt_version: String,
}

impl Args {
    /// Validate argument combinations.
    ///
    /// This method checks that the provided arguments form a valid configuration:
    /// - `--host` is required for record/mirror modes
    /// - `--host` is required for replay mode unless `--serve` is enabled
    /// - Mirror mode requires `--serve` to be enabled
    /// - `--mode` is required unless `--serve` is used alone (standalone broker)
    /// - `--file` is required for record/replay modes, optional for mirror
    /// - QoS must be 0, 1, or 2
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the argument combination is valid
    /// - `Err(String)` with a descriptive error message if validation fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let args = Args::parse();
    /// if let Err(e) = args.validate() {
    ///     eprintln!("Error: {}", e);
    ///     std::process::exit(1);
    /// }
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        // Validate MQTT version
        if self.mqtt_version != "3.1.1" && self.mqtt_version != "5" {
            return Err(format!(
                "Invalid MQTT version: {}. Must be '3.1.1' or '5'.",
                self.mqtt_version
            ));
        }

        // Validate QoS level
        if self.qos > 2 {
            return Err(format!(
                "Invalid QoS level: {}. Must be 0, 1, or 2.",
                self.qos
            ));
        }

        // Validate mode requires a file
        // Requirement 4.1: --validate requires --file
        if self.validate && self.file.is_none() {
            return Err("--validate requires --file".to_string());
        }

        // Validate mode is incompatible with other modes
        // Requirement 4.1: --validate cannot be used with --mode
        if self.validate && self.mode.is_some() {
            return Err("--validate cannot be used with --mode".to_string());
        }

        // Fix mode requires file and output
        // Requirement 8.1, 8.8: --fix requires --file and --output
        if self.fix && self.file.is_none() {
            return Err("--fix requires --file".to_string());
        }

        if self.fix && self.output.is_none() {
            return Err("--fix requires --output".to_string());
        }

        // Fix mode is incompatible with other modes
        // Requirement 8.1: --fix cannot be used with --mode
        if self.fix && self.mode.is_some() {
            return Err("--fix cannot be used with --mode".to_string());
        }

        // Cannot combine --fix and --validate
        // Requirement 8.1: --fix cannot be used with --validate
        if self.fix && self.validate {
            return Err("--fix cannot be used with --validate".to_string());
        }

        // If --validate or --fix is enabled, skip mode-specific validation
        if self.validate || self.fix {
            return Ok(());
        }

        // If --serve is enabled without a mode, it's standalone broker mode (valid)
        // Requirement 1.26: WHEN --serve is enabled without a mode, run embedded broker in standalone mode
        if self.serve && self.mode.is_none() {
            return Ok(());
        }

        // Mode is required unless --serve is used alone
        let mode = match &self.mode {
            Some(m) => m,
            None => {
                return Err(
                    "--mode is required unless --serve is used alone for standalone broker"
                        .to_string(),
                );
            }
        };

        match mode {
            Mode::Record => {
                // Requirement 1.21: IF mode is "record" and --host is not provided, display error
                if self.host.is_none() {
                    return Err("--host is required for record mode".to_string());
                }
                // File is required for record mode
                if self.file.is_none() {
                    return Err("--file is required for record mode".to_string());
                }
            }
            Mode::Replay => {
                // Requirement 1.22: IF mode is "replay" without --serve and --host is not provided, display error
                if !self.serve && self.host.is_none() {
                    return Err(
                        "--host is required for replay mode unless --serve is enabled".to_string(),
                    );
                }
                // File is required for replay mode
                if self.file.is_none() {
                    return Err("--file is required for replay mode".to_string());
                }
            }
            Mode::Mirror => {
                // Requirement 1.21: IF mode is "mirror" and --host is not provided, display error
                if self.host.is_none() {
                    return Err("--host is required for mirror mode".to_string());
                }
                // Requirement 11.2: WHEN mode is "mirror", --serve is required
                if !self.serve {
                    return Err("--serve is required for mirror mode".to_string());
                }
                // File is optional for mirror mode (for optional recording)
            }
        }

        Ok(())
    }

    /// Get QoS as rumqttc QoS enum.
    ///
    /// Converts the u8 QoS value to the corresponding `rumqttc::QoS` enum variant.
    /// This method assumes the QoS value has been validated (0, 1, or 2).
    ///
    /// # Returns
    ///
    /// The corresponding `rumqttc::QoS` variant:
    /// - `QoS::AtMostOnce` for QoS 0
    /// - `QoS::AtLeastOnce` for QoS 1
    /// - `QoS::ExactlyOnce` for QoS 2
    ///
    /// # Panics
    ///
    /// Panics if QoS is not 0, 1, or 2. Call `validate()` first to ensure valid QoS.
    pub fn get_qos(&self) -> QoS {
        match self.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => panic!("Invalid QoS level: {}. Call validate() first.", self.qos),
        }
    }

    /// Check if MQTT v5 should be used for external broker connections.
    ///
    /// Returns `true` if `--mqtt-version 5` (default), `false` if `--mqtt-version 3.1.1`.
    pub fn use_mqtt_v5(&self) -> bool {
        self.mqtt_version == "5"
    }

    /// Check if running in standalone broker mode.
    ///
    /// Standalone broker mode is when `--serve` is enabled without specifying a mode.
    /// In this mode, the embedded broker runs without recording, replaying, or mirroring.
    ///
    /// # Returns
    ///
    /// `true` if running in standalone broker mode, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let args = Args::parse();
    /// if args.is_standalone_broker() {
    ///     println!("Running in standalone broker mode");
    /// }
    /// ```
    pub fn is_standalone_broker(&self) -> bool {
        self.serve && self.mode.is_none()
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
    fn test_record_mode_requires_host() {
        let mut args = default_args();
        args.mode = Some(Mode::Record);
        args.file = Some(PathBuf::from("test.csv"));

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--host is required for record mode"));
    }

    #[test]
    fn test_record_mode_requires_file() {
        let mut args = default_args();
        args.mode = Some(Mode::Record);
        args.host = Some("localhost".to_string());

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--file is required for record mode"));
    }

    #[test]
    fn test_record_mode_valid() {
        let mut args = default_args();
        args.mode = Some(Mode::Record);
        args.host = Some("localhost".to_string());
        args.file = Some(PathBuf::from("test.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_replay_mode_requires_host_without_serve() {
        let mut args = default_args();
        args.mode = Some(Mode::Replay);
        args.file = Some(PathBuf::from("test.csv"));

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--host is required for replay mode unless --serve is enabled"));
    }

    #[test]
    fn test_replay_mode_requires_file() {
        let mut args = default_args();
        args.mode = Some(Mode::Replay);
        args.host = Some("localhost".to_string());

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--file is required for replay mode"));
    }

    #[test]
    fn test_replay_mode_valid_with_host() {
        let mut args = default_args();
        args.mode = Some(Mode::Replay);
        args.host = Some("localhost".to_string());
        args.file = Some(PathBuf::from("test.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_replay_mode_valid_with_serve_no_host() {
        let mut args = default_args();
        args.mode = Some(Mode::Replay);
        args.serve = true;
        args.file = Some(PathBuf::from("test.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_replay_mode_valid_with_serve_and_host() {
        // Requirement 1.24: replay with --serve and --host publishes to both
        let mut args = default_args();
        args.mode = Some(Mode::Replay);
        args.serve = true;
        args.host = Some("localhost".to_string());
        args.file = Some(PathBuf::from("test.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_mirror_mode_requires_host() {
        let mut args = default_args();
        args.mode = Some(Mode::Mirror);
        args.serve = true;

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--host is required for mirror mode"));
    }

    #[test]
    fn test_mirror_mode_requires_serve() {
        let mut args = default_args();
        args.mode = Some(Mode::Mirror);
        args.host = Some("localhost".to_string());

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--serve is required for mirror mode"));
    }

    #[test]
    fn test_mirror_mode_valid() {
        let mut args = default_args();
        args.mode = Some(Mode::Mirror);
        args.host = Some("localhost".to_string());
        args.serve = true;

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_mirror_mode_valid_with_file() {
        // File is optional for mirror mode (for recording while mirroring)
        let mut args = default_args();
        args.mode = Some(Mode::Mirror);
        args.host = Some("localhost".to_string());
        args.serve = true;
        args.file = Some(PathBuf::from("test.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_standalone_broker_mode() {
        let mut args = default_args();
        args.serve = true;
        // No mode specified

        assert!(args.validate().is_ok());
        assert!(args.is_standalone_broker());
    }

    #[test]
    fn test_not_standalone_broker_with_mode() {
        let mut args = default_args();
        args.serve = true;
        args.mode = Some(Mode::Replay);
        args.file = Some(PathBuf::from("test.csv"));

        assert!(!args.is_standalone_broker());
    }

    #[test]
    fn test_mode_required_without_serve() {
        let args = default_args();
        // No mode, no serve

        let result = args.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--mode is required"));
    }

    #[test]
    fn test_invalid_qos() {
        let mut args = default_args();
        args.mode = Some(Mode::Record);
        args.host = Some("localhost".to_string());
        args.file = Some(PathBuf::from("test.csv"));
        args.qos = 3;

        let result = args.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid QoS level"));
    }

    #[test]
    fn test_get_qos_at_most_once() {
        let mut args = default_args();
        args.qos = 0;
        assert_eq!(args.get_qos(), QoS::AtMostOnce);
    }

    #[test]
    fn test_get_qos_at_least_once() {
        let mut args = default_args();
        args.qos = 1;
        assert_eq!(args.get_qos(), QoS::AtLeastOnce);
    }

    #[test]
    fn test_get_qos_exactly_once() {
        let mut args = default_args();
        args.qos = 2;
        assert_eq!(args.get_qos(), QoS::ExactlyOnce);
    }

    #[test]
    #[should_panic(expected = "Invalid QoS level")]
    fn test_get_qos_invalid_panics() {
        let mut args = default_args();
        args.qos = 3;
        args.get_qos();
    }

    #[test]
    fn test_mode_enum_values() {
        // Ensure Mode enum has the expected variants
        let record = Mode::Record;
        let replay = Mode::Replay;
        let mirror = Mode::Mirror;

        assert_eq!(record, Mode::Record);
        assert_eq!(replay, Mode::Replay);
        assert_eq!(mirror, Mode::Mirror);
    }

    #[test]
    fn test_mode_clone() {
        let mode = Mode::Record;
        let cloned = mode;
        assert_eq!(mode, cloned);
    }

    #[test]
    fn test_mode_debug() {
        let mode = Mode::Record;
        let debug_str = format!("{:?}", mode);
        assert!(debug_str.contains("Record"));
    }

    #[test]
    fn test_args_debug() {
        let args = default_args();
        let debug_str = format!("{:?}", args);
        assert!(debug_str.contains("Args"));
    }

    #[test]
    fn test_validate_flag_default_false() {
        let args = default_args();
        assert!(!args.validate);
    }

    #[test]
    fn test_validate_flag_can_be_set() {
        let mut args = default_args();
        args.validate = true;
        assert!(args.validate);
    }

    #[test]
    fn test_fix_flag_default_false() {
        let args = default_args();
        assert!(!args.fix);
    }

    #[test]
    fn test_fix_flag_can_be_set() {
        let mut args = default_args();
        args.fix = true;
        assert!(args.fix);
    }

    #[test]
    fn test_output_default_none() {
        let args = default_args();
        assert!(args.output.is_none());
    }

    #[test]
    fn test_output_can_be_set() {
        let mut args = default_args();
        args.output = Some(PathBuf::from("repaired.csv"));
        assert_eq!(args.output, Some(PathBuf::from("repaired.csv")));
    }

    // Tests for --validate flag validation rules (Requirement 4.1)

    #[test]
    fn test_validate_requires_file() {
        let mut args = default_args();
        args.validate = true;
        // No file specified

        let result = args.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--validate requires --file"));
    }

    #[test]
    fn test_validate_with_file_is_valid() {
        let mut args = default_args();
        args.validate = true;
        args.file = Some(PathBuf::from("test.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_cannot_be_used_with_mode() {
        let mut args = default_args();
        args.validate = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.mode = Some(Mode::Record);

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--validate cannot be used with --mode"));
    }

    #[test]
    fn test_validate_cannot_be_used_with_replay_mode() {
        let mut args = default_args();
        args.validate = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.mode = Some(Mode::Replay);

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--validate cannot be used with --mode"));
    }

    #[test]
    fn test_validate_cannot_be_used_with_mirror_mode() {
        let mut args = default_args();
        args.validate = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.mode = Some(Mode::Mirror);

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--validate cannot be used with --mode"));
    }

    // Tests for --fix flag validation rules (Requirements 8.1, 8.8)

    #[test]
    fn test_fix_requires_file() {
        let mut args = default_args();
        args.fix = true;
        args.output = Some(PathBuf::from("repaired.csv"));
        // No file specified

        let result = args.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--fix requires --file"));
    }

    #[test]
    fn test_fix_requires_output() {
        let mut args = default_args();
        args.fix = true;
        args.file = Some(PathBuf::from("test.csv"));
        // No output specified

        let result = args.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--fix requires --output"));
    }

    #[test]
    fn test_fix_with_file_and_output_is_valid() {
        let mut args = default_args();
        args.fix = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.output = Some(PathBuf::from("repaired.csv"));

        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_fix_cannot_be_used_with_mode() {
        let mut args = default_args();
        args.fix = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.output = Some(PathBuf::from("repaired.csv"));
        args.mode = Some(Mode::Record);

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--fix cannot be used with --mode"));
    }

    #[test]
    fn test_fix_cannot_be_used_with_replay_mode() {
        let mut args = default_args();
        args.fix = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.output = Some(PathBuf::from("repaired.csv"));
        args.mode = Some(Mode::Replay);

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--fix cannot be used with --mode"));
    }

    #[test]
    fn test_fix_cannot_be_used_with_mirror_mode() {
        let mut args = default_args();
        args.fix = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.output = Some(PathBuf::from("repaired.csv"));
        args.mode = Some(Mode::Mirror);

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--fix cannot be used with --mode"));
    }

    // Tests for --fix and --validate mutual exclusion

    #[test]
    fn test_fix_cannot_be_used_with_validate() {
        let mut args = default_args();
        args.fix = true;
        args.validate = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.output = Some(PathBuf::from("repaired.csv"));

        let result = args.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("--fix cannot be used with --validate"));
    }

    #[test]
    fn test_validate_cannot_be_used_with_fix() {
        // Same test as above but from validate perspective
        let mut args = default_args();
        args.validate = true;
        args.fix = true;
        args.file = Some(PathBuf::from("test.csv"));
        args.output = Some(PathBuf::from("repaired.csv"));

        let result = args.validate();
        assert!(result.is_err());
        // The error message will be about --fix since that check comes after --validate checks
        assert!(result
            .unwrap_err()
            .contains("--fix cannot be used with --validate"));
    }
}
