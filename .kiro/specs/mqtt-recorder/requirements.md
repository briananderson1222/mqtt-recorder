# Requirements Document

## Introduction

This document specifies the requirements for an MQTT Recorder CLI tool written in Rust. The tool enables users to record MQTT messages from a broker to a CSV file and replay those recorded messages to a broker. It is inspired by Python-based MQTT recorder tools and follows Rust best practices for CLI applications.

## Glossary

- **MQTT_Recorder**: The main CLI application that records and replays MQTT messages
- **MQTT_Client**: The underlying MQTT client component that handles broker connections
- **CSV_Handler**: The component responsible for reading and writing CSV files containing recorded messages
- **Topic_Filter**: The component that manages topic subscription patterns
- **TLS_Config**: The configuration component for SSL/TLS secure connections
- **Message_Record**: A single recorded MQTT message containing topic, payload, QoS, timestamp, and retain flag
- **Broker**: An MQTT message broker that the tool connects to
- **QoS**: Quality of Service level (0, 1, or 2) for MQTT message delivery guarantees
- **Wildcard_Topic**: MQTT topic patterns using `+` (single-level) or `#` (multi-level) wildcards
- **Embedded_Broker**: An optional built-in MQTT broker that can serve messages locally without requiring an external broker
- **Mirror_Mode**: A mode where messages from an external broker are republished to the embedded broker in real-time

## Requirements

### Requirement 1: CLI Argument Parsing

**User Story:** As a user, I want to provide configuration options via command-line arguments, so that I can control the tool's behavior without modifying configuration files.

#### Acceptance Criteria

1. THE MQTT_Recorder SHALL accept `--host` as an argument specifying the MQTT broker address (required unless `--serve` is enabled in replay mode)
2. THE MQTT_Recorder SHALL accept `--port` with a default value of 1883 for the MQTT broker port
3. THE MQTT_Recorder SHALL accept `--client_id` as an optional argument for the MQTT client identifier
4. THE MQTT_Recorder SHALL accept `--mode` as an argument with values "record", "replay", or "mirror" (required unless `--serve` is used alone for standalone broker)
5. THE MQTT_Recorder SHALL accept `--file` as an argument specifying the CSV file path (required for record/replay modes, optional for mirror mode)
6. THE MQTT_Recorder SHALL accept `--loop` with a default value of false for looping replay
7. THE MQTT_Recorder SHALL accept `--qos` with a default value of 0 for subscription Quality of Service
8. THE MQTT_Recorder SHALL accept `--topics` as an optional argument for a JSON file containing topic subscriptions
9. THE MQTT_Recorder SHALL accept `-t` or `--topic` as an optional argument for a single topic subscription
10. THE MQTT_Recorder SHALL accept `--enable_ssl` with a default value of false for TLS support
11. THE MQTT_Recorder SHALL accept `--tls_insecure` with a default value of false for self-signed certificates
12. THE MQTT_Recorder SHALL accept `--ca_cert` as an optional path to CA certificate files
13. THE MQTT_Recorder SHALL accept `--certfile` as an optional path to client certificate
14. THE MQTT_Recorder SHALL accept `--keyfile` as an optional path to client private key
15. THE MQTT_Recorder SHALL accept `--username` as an optional MQTT broker username
16. THE MQTT_Recorder SHALL accept `--password` as an optional MQTT broker password
17. THE MQTT_Recorder SHALL accept `--encode_b64` with a default value of false for base64 payload encoding
18. THE MQTT_Recorder SHALL accept `--csv_field_size_limit` as an optional CSV field size limit
19. THE MQTT_Recorder SHALL accept `--max_packet_size` with a default value of 1048576 (1MB) for maximum MQTT packet size
19. IF an invalid mode value is provided, THEN THE MQTT_Recorder SHALL display an error message and exit with a non-zero status code
20. IF required arguments are missing, THEN THE MQTT_Recorder SHALL display usage help and exit with a non-zero status code
21. IF mode is "record" or "mirror" and `--host` is not provided, THEN THE MQTT_Recorder SHALL display an error and exit with a non-zero status code
22. IF mode is "replay" without `--serve` and `--host` is not provided, THEN THE MQTT_Recorder SHALL display an error and exit with a non-zero status code
23. WHEN `--serve` is enabled, THE MQTT_Recorder SHALL ignore `--port` for the embedded broker and use `--serve_port` instead
24. WHEN `--serve` is enabled in replay mode with `--host` provided, THE MQTT_Recorder SHALL publish to the external broker AND the embedded broker
25. WHEN `--serve` is enabled, THE MQTT_Recorder SHALL ignore TLS options (`--enable_ssl`, `--tls_insecure`, `--ca_cert`, `--certfile`, `--keyfile`) for the embedded broker connection
26. WHEN `--serve` is enabled without a mode, THE MQTT_Recorder SHALL run the embedded broker in standalone mode
27. WHEN connecting to an MQTT broker, THE MQTT_Client SHALL use the configured max_packet_size for both incoming and outgoing packets

### Requirement 2: MQTT Broker Connection

**User Story:** As a user, I want to connect to MQTT brokers with various authentication methods, so that I can record and replay messages from secured and unsecured brokers.

#### Acceptance Criteria

1. WHEN connecting to a broker, THE MQTT_Client SHALL establish a connection using the provided host and port
2. WHEN a client_id is provided, THE MQTT_Client SHALL use it for the connection
3. WHEN no client_id is provided, THE MQTT_Client SHALL generate a unique client identifier
4. WHEN username and password are provided, THE MQTT_Client SHALL authenticate using those credentials
5. WHEN enable_ssl is true, THE MQTT_Client SHALL establish a TLS-encrypted connection
6. WHEN ca_cert is provided, THE MQTT_Client SHALL use the specified CA certificate for server verification
7. WHEN certfile and keyfile are provided, THE MQTT_Client SHALL use client certificate authentication
8. WHEN tls_insecure is true, THE MQTT_Client SHALL skip certificate hostname verification
9. IF the connection fails, THEN THE MQTT_Client SHALL report the error and exit with a non-zero status code
10. IF authentication fails, THEN THE MQTT_Client SHALL report the authentication error and exit with a non-zero status code

### Requirement 3: Topic Subscription Management

**User Story:** As a user, I want to specify which topics to subscribe to, so that I can record only relevant messages.

#### Acceptance Criteria

1. WHEN no topic arguments are provided, THE Topic_Filter SHALL subscribe to the wildcard topic `#` to receive all messages
2. WHEN a single topic is provided via `-t` or `--topic`, THE Topic_Filter SHALL subscribe only to that topic
3. WHEN a topics JSON file is provided, THE Topic_Filter SHALL parse the file and subscribe to all listed topics
4. THE Topic_Filter SHALL support MQTT wildcard patterns (`+` and `#`) in topic subscriptions
5. IF the topics JSON file cannot be read, THEN THE MQTT_Recorder SHALL report the error and exit with a non-zero status code
6. IF the topics JSON file contains invalid JSON, THEN THE MQTT_Recorder SHALL report a parsing error and exit with a non-zero status code
7. THE Topic_Filter SHALL expect the JSON file format: `{"topics": ["topic1", "topic2"]}`

### Requirement 4: Message Recording

**User Story:** As a user, I want to record MQTT messages to a CSV file, so that I can replay them later or analyze the message traffic.

#### Acceptance Criteria

1. WHEN mode is "record", THE MQTT_Recorder SHALL subscribe to the specified topics and write received messages to the CSV file
2. WHEN a message is received, THE CSV_Handler SHALL write a record containing: timestamp, topic, payload, QoS, and retain flag
3. WHEN encode_b64 is true, THE CSV_Handler SHALL store message payloads as base64-encoded strings
4. WHEN encode_b64 is false, THE CSV_Handler SHALL store message payloads as raw strings
5. THE CSV_Handler SHALL write a header row as the first line of the CSV file
6. THE CSV_Handler SHALL properly escape CSV special characters in payloads
7. THE CSV_Handler SHALL flush writes to ensure data persistence
8. IF the output file cannot be created or written, THEN THE MQTT_Recorder SHALL report the error and exit with a non-zero status code
9. WHEN the user sends an interrupt signal (Ctrl+C), THE MQTT_Recorder SHALL gracefully close the file and disconnect from the broker

### Requirement 5: Message Replay

**User Story:** As a user, I want to replay recorded messages to a broker, so that I can simulate message traffic or restore a previous state.

#### Acceptance Criteria

1. WHEN mode is "replay", THE MQTT_Recorder SHALL read messages from the CSV file and publish them to the broker
2. WHEN replaying messages, THE MQTT_Recorder SHALL preserve the original topic for each message
3. WHEN replaying messages, THE MQTT_Recorder SHALL preserve the original QoS level for each message
4. WHEN replaying messages, THE MQTT_Recorder SHALL preserve the original retain flag for each message
5. WHEN replaying messages, THE MQTT_Recorder SHALL maintain the relative timing between messages based on recorded timestamps
6. WHEN encode_b64 was used during recording, THE MQTT_Recorder SHALL decode base64 payloads before publishing
7. WHEN loop is true, THE MQTT_Recorder SHALL restart replay from the beginning after reaching the end of the file
8. WHEN loop is false, THE MQTT_Recorder SHALL exit after replaying all messages
9. IF the input file cannot be read, THEN THE MQTT_Recorder SHALL report the error and exit with a non-zero status code
10. IF the CSV file contains invalid data, THEN THE MQTT_Recorder SHALL report the parsing error and exit with a non-zero status code
11. WHEN the user sends an interrupt signal (Ctrl+C), THE MQTT_Recorder SHALL gracefully disconnect from the broker

### Requirement 6: CSV File Format

**User Story:** As a user, I want a well-defined CSV format, so that I can inspect, edit, or process recorded messages with other tools.

#### Acceptance Criteria

1. THE CSV_Handler SHALL use the following column order: timestamp, topic, payload, qos, retain
2. THE CSV_Handler SHALL use RFC 4180 compliant CSV formatting
3. THE CSV_Handler SHALL use double quotes to escape fields containing commas, quotes, or newlines
4. WHEN csv_field_size_limit is specified, THE CSV_Handler SHALL enforce the maximum field size during reading
5. THE CSV_Handler SHALL store timestamps in ISO 8601 format with millisecond precision
6. THE CSV_Handler SHALL store QoS as an integer (0, 1, or 2)
7. THE CSV_Handler SHALL store retain flag as a boolean string ("true" or "false")

### Requirement 7: Topics JSON File Parsing

**User Story:** As a user, I want to specify multiple topics in a JSON file, so that I can easily manage complex topic subscriptions.

#### Acceptance Criteria

1. THE Topic_Filter SHALL parse JSON files with the structure: `{"topics": ["topic1", "topic2", ...]}`
2. THE Topic_Filter SHALL validate that the "topics" field is an array of strings
3. IF the JSON structure is invalid, THEN THE Topic_Filter SHALL report a descriptive error message
4. THE Topic_Filter SHALL support empty topic arrays (resulting in no subscriptions)

### Requirement 8: Error Handling and Logging

**User Story:** As a user, I want clear error messages and logging, so that I can diagnose issues and monitor the tool's operation.

#### Acceptance Criteria

1. WHEN an error occurs, THE MQTT_Recorder SHALL output a descriptive error message to stderr
2. THE MQTT_Recorder SHALL use appropriate exit codes: 0 for success, non-zero for errors
3. THE MQTT_Recorder SHALL log connection status changes to stderr
4. THE MQTT_Recorder SHALL log the number of messages recorded or replayed upon completion
5. IF a recoverable error occurs during recording, THEN THE MQTT_Recorder SHALL log the error and continue operation

### Requirement 9: Graceful Shutdown

**User Story:** As a user, I want the tool to handle interrupts gracefully, so that I don't lose data or leave connections open.

#### Acceptance Criteria

1. WHEN receiving SIGINT (Ctrl+C), THE MQTT_Recorder SHALL initiate graceful shutdown
2. WHEN receiving SIGTERM, THE MQTT_Recorder SHALL initiate graceful shutdown
3. DURING graceful shutdown in record mode, THE CSV_Handler SHALL flush and close the output file
4. DURING graceful shutdown, THE MQTT_Client SHALL disconnect cleanly from the broker
5. THE MQTT_Recorder SHALL exit with status code 0 after successful graceful shutdown

### Requirement 10: Embedded Broker Mode

**User Story:** As a user, I want to optionally start an embedded MQTT broker, so that I can replay messages without needing an external broker or create a local mirror for testing.

#### Acceptance Criteria

1. THE MQTT_Recorder SHALL accept `--serve` as an optional argument to start an embedded broker
2. THE MQTT_Recorder SHALL accept `--serve_port` with a default value of 1883 for the embedded broker port
3. WHEN serve is enabled in replay mode, THE MQTT_Recorder SHALL start an embedded broker before replaying messages
4. WHEN serve is enabled, THE Embedded_Broker SHALL accept connections from external MQTT clients
5. WHEN serve is enabled, THE MQTT_Recorder SHALL publish replayed messages to the embedded broker
6. WHEN serve is enabled without replay mode, THE Embedded_Broker SHALL run as a standalone broker
7. THE Embedded_Broker SHALL support QoS levels 0, 1, and 2
8. WHEN the user sends an interrupt signal, THE Embedded_Broker SHALL gracefully shut down
9. THE Embedded_Broker SHALL log client connections and disconnections to stderr
10. THE Embedded_Broker SHALL display its current mode (standalone, replay, or mirror) on startup

### Requirement 11: Mirror/Passthrough Mode

**User Story:** As a user, I want to mirror messages from an external broker to the embedded broker, so that I can create a local copy of traffic for testing or debugging without affecting the source broker.

#### Acceptance Criteria

1. THE MQTT_Recorder SHALL accept `--mode mirror` as a valid mode option
2. WHEN mode is "mirror", THE MQTT_Recorder SHALL require `--serve` to be enabled
3. WHEN mode is "mirror", THE MQTT_Recorder SHALL subscribe to topics on the external broker specified by `--host`
4. WHEN mode is "mirror", THE MQTT_Recorder SHALL republish received messages to the embedded broker
5. WHEN mode is "mirror" and `--file` is provided, THE MQTT_Recorder SHALL also record messages to the CSV file
6. WHEN mode is "mirror", THE MQTT_Recorder SHALL preserve message topic, payload, QoS, and retain flag during republishing
7. THE Mirror_Mode SHALL support all topic filtering options (--topic, --topics JSON file, or wildcard #)
8. THE Mirror_Mode SHALL log the number of messages mirrored periodically to stderr
9. WHEN the user sends an interrupt signal in mirror mode, THE MQTT_Recorder SHALL gracefully disconnect from the external broker and shut down the embedded broker

### Requirement 12: CI/CD Pipeline

**User Story:** As a developer, I want automated CI/CD pipelines, so that code quality is maintained and releases are automated.

#### Acceptance Criteria

1. THE CI_Pipeline SHALL run on every push and pull request to the main branch
2. THE CI_Pipeline SHALL execute `cargo fmt --check` to verify code formatting
3. THE CI_Pipeline SHALL execute `cargo clippy` to check for linting issues
4. THE CI_Pipeline SHALL execute `cargo test` to run all unit and integration tests
5. THE CI_Pipeline SHALL build the project for Linux (x86_64), macOS (x86_64, aarch64), and Windows (x86_64)
6. WHEN a version tag is pushed, THE Release_Pipeline SHALL create a GitHub release with compiled binaries
7. THE Release_Pipeline SHALL generate checksums for all release artifacts
8. THE CI_Pipeline SHALL cache Cargo dependencies to improve build times

### Requirement 13: Project Documentation

**User Story:** As a developer or user, I want comprehensive documentation, so that I can understand how to use and contribute to the project.

#### Acceptance Criteria

1. THE Project SHALL include a README.md with installation instructions, usage examples, and configuration options
2. THE Project SHALL include an AGENTS.md file with guidance for AI assistants interacting with the repository
3. THE Project SHALL include a LICENSE file with the Apache 2.0 license text
4. THE Project SHALL include a CONTRIBUTING.md file with contribution guidelines
5. THE README.md SHALL include examples for recording and replaying messages
6. THE README.md SHALL document all CLI arguments with descriptions and defaults

### Requirement 14: Rust Best Practices

**User Story:** As a developer, I want the codebase to follow Rust best practices, so that the code is maintainable, safe, and performant.

#### Acceptance Criteria

1. THE Project SHALL use the `clap` crate with derive macros for CLI argument parsing
2. THE Project SHALL use the `rumqttc` crate for MQTT client functionality
3. THE Project SHALL use the `rumqttd` crate for embedded broker functionality
4. THE Project SHALL use the `tokio` runtime for async operations
5. THE Project SHALL use the `serde` and `serde_json` crates for JSON parsing
6. THE Project SHALL use the `csv` crate for CSV file handling
7. THE Project SHALL use the `base64` crate for base64 encoding/decoding
8. THE Project SHALL use the `chrono` crate for timestamp handling
9. THE Project SHALL use `thiserror` for custom error types
10. THE Project SHALL use `anyhow` for error propagation in the main application
11. THE Project SHALL organize code into modules: cli, mqtt, csv_handler, topics, broker, error
12. THE Project SHALL include comprehensive documentation comments for public APIs
13. THE Project SHALL pass `cargo clippy` without warnings
14. THE Project SHALL be formatted according to `rustfmt` defaults
