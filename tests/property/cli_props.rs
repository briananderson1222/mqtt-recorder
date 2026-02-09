//! Property-based tests for CLI argument parsing
//!
//! This module contains property tests that validate the CLI argument parsing
//! behavior as specified in the design document.
//!
//! **Validates: Requirements 1.1-1.22**

use clap::Parser;
use proptest::prelude::*;
use std::path::PathBuf;

// Import the CLI module from the main crate
use mqtt_recorder::cli::{Args, Mode};

/// Strategy for generating valid host strings
fn valid_host_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple hostnames
        "[a-z][a-z0-9]{0,10}".prop_map(|s| s),
        // Hostnames with dots
        "[a-z][a-z0-9]{0,5}\\.[a-z]{2,4}".prop_map(|s| s),
        // IP addresses
        (1u8..=254, 0u8..=255, 0u8..=255, 1u8..=254)
            .prop_map(|(a, b, c, d)| format!("{}.{}.{}.{}", a, b, c, d)),
        // localhost
        Just("localhost".to_string()),
    ]
}

/// Strategy for generating valid port numbers
fn valid_port_strategy() -> impl Strategy<Value = u16> {
    1u16..=65535
}

/// Strategy for generating valid client IDs
fn valid_client_id_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_-]{0,22}".prop_map(|s| s)
}

/// Strategy for generating valid QoS values (0, 1, or 2)
fn valid_qos_strategy() -> impl Strategy<Value = u8> {
    0u8..=2
}

/// Strategy for generating valid topic strings
fn valid_topic_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Simple topics
        "[a-z][a-z0-9]{0,10}".prop_map(|s| s),
        // Topics with slashes
        "[a-z][a-z0-9]{0,5}/[a-z][a-z0-9]{0,5}".prop_map(|s| s),
        // Topics with wildcards
        "[a-z]+/\\+".prop_map(|s| s),
        "[a-z]+/#".prop_map(|s| s),
    ]
}

/// Strategy for generating valid file paths
fn valid_file_path_strategy() -> impl Strategy<Value = PathBuf> {
    "[a-z][a-z0-9]{0,10}\\.csv".prop_map(PathBuf::from)
}

/// Strategy for generating valid mode strings
fn valid_mode_string_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("record".to_string()),
        Just("replay".to_string()),
        Just("mirror".to_string()),
    ]
}

/// Strategy for generating invalid mode strings
fn invalid_mode_string_strategy() -> impl Strategy<Value = String> {
    "[a-z]{1,10}".prop_filter("Must not be a valid mode", |s| {
        s != "record" && s != "replay" && s != "mirror"
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // *For any* valid combination of CLI arguments with explicit values, parsing those
    // arguments and extracting the values SHALL produce the same values that were provided.
    // **Validates: Requirements 1.1-1.18**
    #[test]
    fn property_1_cli_argument_parsing_preserves_host(
        host in valid_host_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            host.clone(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.host, Some(host));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.2**
    #[test]
    fn property_1_cli_argument_parsing_preserves_port(
        port in valid_port_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--port".to_string(),
            port.to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.port, port);
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.3**
    #[test]
    fn property_1_cli_argument_parsing_preserves_client_id(
        client_id in valid_client_id_strategy()
    ) {
        // Note: clap converts snake_case to kebab-case for CLI args
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--client-id".to_string(),
            client_id.clone(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.client_id, Some(client_id));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.4**
    #[test]
    fn property_1_cli_argument_parsing_preserves_mode(
        mode_str in valid_mode_string_strategy()
    ) {
        let (host_required, serve_required, file_required) = match mode_str.as_str() {
            "record" => (true, false, true),
            "replay" => (true, false, true),
            "mirror" => (true, true, false),
            _ => unreachable!(),
        };

        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
            "--mode".to_string(),
            mode_str.clone(),
        ];

        if host_required {
            args_vec.push("--host".to_string());
            args_vec.push("localhost".to_string());
        }

        if serve_required {
            args_vec.push("--serve".to_string());
        }

        if file_required {
            args_vec.push("--file".to_string());
            args_vec.push("test.csv".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");

        let expected_mode = match mode_str.as_str() {
            "record" => Mode::Record,
            "replay" => Mode::Replay,
            "mirror" => Mode::Mirror,
            _ => unreachable!(),
        };

        prop_assert_eq!(args.mode, Some(expected_mode));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.5**
    #[test]
    fn property_1_cli_argument_parsing_preserves_file(
        file_path in valid_file_path_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.file, Some(file_path));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.6**
    #[test]
    fn property_1_cli_argument_parsing_preserves_loop(
        loop_replay in any::<bool>()
    ) {
        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        if loop_replay {
            args_vec.push("--loop".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.loop_replay, loop_replay);
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.7**
    #[test]
    fn property_1_cli_argument_parsing_preserves_qos(
        qos in valid_qos_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
            "--qos".to_string(),
            qos.to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.qos, qos);
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.9**
    #[test]
    fn property_1_cli_argument_parsing_preserves_topic(
        topic in valid_topic_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
            "-t".to_string(),
            topic.clone(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.topic, Some(topic));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.10**
    #[test]
    fn property_1_cli_argument_parsing_preserves_enable_ssl(
        enable_ssl in any::<bool>()
    ) {
        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        if enable_ssl {
            args_vec.push("--enable-ssl".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.enable_ssl, enable_ssl);
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.11**
    #[test]
    fn property_1_cli_argument_parsing_preserves_tls_insecure(
        tls_insecure in any::<bool>()
    ) {
        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        if tls_insecure {
            args_vec.push("--tls-insecure".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.tls_insecure, tls_insecure);
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.15**
    #[test]
    fn property_1_cli_argument_parsing_preserves_username(
        username in "[a-zA-Z][a-zA-Z0-9]{0,15}"
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
            "--username".to_string(),
            username.clone(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.username, Some(username));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.16**
    #[test]
    fn property_1_cli_argument_parsing_preserves_password(
        password in "[a-zA-Z0-9!@#$%]{1,20}"
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
            "--password".to_string(),
            password.clone(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.password, Some(password));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.17**
    #[test]
    fn property_1_cli_argument_parsing_preserves_encode_b64(
        encode_b64 in any::<bool>()
    ) {
        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        if encode_b64 {
            args_vec.push("--encode-b64".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.encode_b64, encode_b64);
    }


    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.18**
    #[test]
    fn property_1_cli_argument_parsing_preserves_csv_field_size_limit(
        limit in 1usize..1_000_000
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
            "--csv-field-size-limit".to_string(),
            limit.to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.csv_field_size_limit, Some(limit));
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.1, 1.2 (serve and serve_port)**
    #[test]
    fn property_1_cli_argument_parsing_preserves_serve_options(
        serve in any::<bool>(),
        serve_port in valid_port_strategy()
    ) {
        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
        ];

        if serve {
            args_vec.push("--serve".to_string());
            args_vec.push("--serve-port".to_string());
            args_vec.push(serve_port.to_string());
            // Standalone broker mode - no mode needed
        } else {
            // Need mode, host, and file for non-serve mode
            args_vec.push("--host".to_string());
            args_vec.push("localhost".to_string());
            args_vec.push("--mode".to_string());
            args_vec.push("record".to_string());
            args_vec.push("--file".to_string());
            args_vec.push("test.csv".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.serve, serve);
        if serve {
            prop_assert_eq!(args.serve_port, serve_port);
        }
    }

    // Feature: mqtt-recorder, Property 2: Invalid Mode Rejection
    // *For any* string that is not "record", "replay", or "mirror", providing it as
    // the `--mode` argument SHALL result in a parsing error.
    // **Validates: Requirements 1.19**
    #[test]
    fn property_2_invalid_mode_rejection(
        invalid_mode in invalid_mode_string_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            invalid_mode,
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        let result = Args::try_parse_from(&args_vec);
        prop_assert!(result.is_err(), "Invalid mode should result in parsing error");
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // *For any* argument set where mode is "record", `--host` SHALL be required.
    // **Validates: Requirements 1.1, 1.21**
    #[test]
    fn property_3_record_mode_requires_host(
        file_path in valid_file_path_strategy()
    ) {
        // Record mode without host should fail validation
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_err(), "Record mode without host should fail validation");
        prop_assert!(validation_result.unwrap_err().contains("--host is required"));
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // *For any* argument set where mode is "mirror", `--host` SHALL be required.
    // **Validates: Requirements 1.1, 1.21**
    #[test]
    fn property_3_mirror_mode_requires_host(
        _dummy in Just(())
    ) {
        // Mirror mode without host should fail validation
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--mode".to_string(),
            "mirror".to_string(),
            "--serve".to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_err(), "Mirror mode without host should fail validation");
        prop_assert!(validation_result.unwrap_err().contains("--host is required"));
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // *For any* argument set where mode is "replay" and `--serve` is not enabled,
    // `--host` SHALL be required.
    // **Validates: Requirements 1.1, 1.22**
    #[test]
    fn property_3_replay_mode_without_serve_requires_host(
        file_path in valid_file_path_strategy()
    ) {
        // Replay mode without serve and without host should fail validation
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--mode".to_string(),
            "replay".to_string(),
            "--file".to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_err(), "Replay mode without serve and without host should fail validation");
        prop_assert!(validation_result.unwrap_err().contains("--host is required"));
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // *For any* argument set where mode is "replay" and `--serve` is enabled,
    // `--host` SHALL be optional.
    // **Validates: Requirements 1.1, 1.22**
    #[test]
    fn property_3_replay_mode_with_serve_host_optional(
        file_path in valid_file_path_strategy(),
        has_host in any::<bool>()
    ) {
        // Replay mode with serve should work with or without host
        let mut args_vec = vec![
            "mqtt-recorder".to_string(),
            "--mode".to_string(),
            "replay".to_string(),
            "--serve".to_string(),
            "--file".to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        if has_host {
            args_vec.push("--host".to_string());
            args_vec.push("localhost".to_string());
        }

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_ok(), "Replay mode with serve should accept optional host");
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // Combined test: Record mode with host should pass validation
    // **Validates: Requirements 1.1, 1.21**
    #[test]
    fn property_3_record_mode_with_host_passes(
        host in valid_host_strategy(),
        file_path in valid_file_path_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            host,
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_ok(), "Record mode with host should pass validation");
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // Combined test: Mirror mode with host and serve should pass validation
    // **Validates: Requirements 1.1, 1.21**
    #[test]
    fn property_3_mirror_mode_with_host_and_serve_passes(
        host in valid_host_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            host,
            "--mode".to_string(),
            "mirror".to_string(),
            "--serve".to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_ok(), "Mirror mode with host and serve should pass validation");
    }

    // Feature: mqtt-recorder, Property 3: Conditional Host Requirement
    // Combined test: Replay mode without serve but with host should pass validation
    // **Validates: Requirements 1.1, 1.22**
    #[test]
    fn property_3_replay_mode_without_serve_with_host_passes(
        host in valid_host_strategy(),
        file_path in valid_file_path_strategy()
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            host,
            "--mode".to_string(),
            "replay".to_string(),
            "--file".to_string(),
            file_path.to_string_lossy().to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse args");
        let validation_result = args.validate();
        prop_assert!(validation_result.is_ok(), "Replay mode without serve but with host should pass validation");
    }

    // Feature: mqtt-recorder, Property 1: CLI Argument Parsing Preserves Values
    // **Validates: Requirements 1.19**
    #[test]
    fn property_1_cli_argument_parsing_preserves_max_packet_size(
        max_packet_size in 1024usize..10_000_000
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
            "--max-packet-size".to_string(),
            max_packet_size.to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.max_packet_size, max_packet_size);
    }

    // Feature: mqtt-recorder, Property 12: Max Packet Size Configuration
    // *For any* valid max_packet_size value, the CLI SHALL accept and preserve it.
    // **Validates: Requirements 1.19, 1.28**
    #[test]
    fn property_12_max_packet_size_default_is_1mb(
        _dummy in Just(())
    ) {
        let args_vec = vec![
            "mqtt-recorder".to_string(),
            "--host".to_string(),
            "localhost".to_string(),
            "--mode".to_string(),
            "record".to_string(),
            "--file".to_string(),
            "test.csv".to_string(),
        ];

        let args = Args::try_parse_from(&args_vec).expect("Should parse valid args");
        prop_assert_eq!(args.max_packet_size, 1048576, "Default max_packet_size should be 1MB (1048576 bytes)");
    }
}
