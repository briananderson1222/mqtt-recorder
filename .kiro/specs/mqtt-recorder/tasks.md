# Implementation Plan: MQTT Recorder

## Overview

This implementation plan breaks down the MQTT Recorder CLI tool into incremental coding tasks. Each task builds on previous work, with property-based tests validating correctness properties from the design document. The implementation uses Rust with async/await (Tokio), clap for CLI, rumqttc for MQTT client, and rumqttd for the embedded broker.

## Tasks

- [x] 1. Project setup and core infrastructure
  - [x] 1.1 Initialize Cargo project with workspace structure
    - Create `Cargo.toml` with dependencies: clap, rumqttc, rumqttd, tokio, serde, serde_json, csv, base64, chrono, thiserror, anyhow, proptest
    - Configure edition 2021, resolver 2
    - Set up binary target `mqtt-recorder`
    - _Requirements: 14.1-14.10_

  - [x] 1.2 Create error module with custom error types
    - Define `MqttRecorderError` enum with variants for connection, client, CSV, IO, JSON, argument, TLS, and broker errors
    - Implement `From` traits for underlying error types
    - _Requirements: 8.1, 8.2_

  - [x] 1.3 Create CLI module with argument parsing
    - Define `Mode` enum (Record, Replay, Mirror)
    - Define `Args` struct with all CLI arguments using clap derive macros
    - Implement `validate()` method for argument combination validation
    - Implement `get_qos()` and `is_standalone_broker()` helper methods
    - _Requirements: 1.1-1.26_

  - [x] 1.4 Write property tests for CLI argument parsing
    - **Property 1: CLI Argument Parsing Preserves Values**
    - **Property 2: Invalid Mode Rejection**
    - **Property 3: Conditional Host Requirement**
    - **Validates: Requirements 1.1-1.22**

- [x] 2. Topic filtering and JSON parsing
  - [x] 2.1 Create topics module with TopicFilter struct
    - Implement `from_single()` for single topic
    - Implement `from_json_file()` for JSON file parsing
    - Implement `wildcard()` for default `#` subscription
    - Implement `topics()` getter and `is_empty()` check
    - _Requirements: 3.1-3.7, 7.1-7.4_

  - [x] 2.2 Write property tests for topic filtering
    - **Property 4: Single Topic Filter Construction**
    - **Property 5: JSON Topics File Parsing**
    - **Property 6: Wildcard Topic Preservation**
    - **Validates: Requirements 3.2-3.4, 7.1-7.2**

- [x] 3. CSV handling module
  - [x] 3.1 Create csv_handler module with MessageRecord struct
    - Define `MessageRecord` with timestamp, topic, payload, qos, retain fields
    - Implement serde Serialize/Deserialize
    - _Requirements: 4.2, 6.1, 6.5-6.7_

  - [x] 3.2 Implement CsvWriter for recording messages
    - Create writer with header row output
    - Implement base64 encoding option for payloads
    - Implement proper CSV escaping for special characters
    - Implement flush() method
    - _Requirements: 4.3-4.7, 6.2-6.3_

  - [x] 3.3 Implement CsvReader for replaying messages
    - Create reader with optional field size limit
    - Implement base64 decoding option for payloads
    - Implement Iterator trait for sequential reading
    - Implement reset() for loop replay support
    - _Requirements: 5.1-5.6, 6.4_

  - [x] 3.4 Write property tests for CSV handling
    - **Property 7: Base64 Encoding Round-Trip**
    - **Property 8: CSV Special Character Escaping**
    - **Property 9: CSV Message Record Round-Trip**
    - **Property 10: Timestamp Difference Calculation**
    - **Property 11: CSV Field Size Limit Enforcement**
    - **Validates: Requirements 4.3, 4.6, 5.2-5.6, 6.2-6.7**

- [x] 4. Checkpoint - Core modules complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. MQTT client module
  - [x] 5.1 Create mqtt module with client configuration
    - Define `MqttClientConfig` struct with host, port, client_id, credentials
    - Define `TlsConfig` struct for SSL/TLS options
    - _Requirements: 2.1-2.8_

  - [x] 5.2 Implement MqttClient wrapper around rumqttc
    - Implement `new()` with TLS configuration support
    - Implement `subscribe()` for topic subscriptions
    - Implement `publish()` for message publishing
    - Implement `poll()` for event loop processing
    - Implement `disconnect()` for clean shutdown
    - Generate unique client_id if not provided
    - _Requirements: 2.1-2.10_

- [x] 6. Embedded broker module
  - [x] 6.1 Create broker module with EmbeddedBroker struct
    - Define `BrokerMode` enum (Standalone, Replay, Mirror)
    - Implement `new()` to start rumqttd broker on specified port
    - Implement `get_local_client()` for internal publishing
    - Implement `shutdown()` for graceful termination
    - Log mode on startup
    - _Requirements: 10.1-10.10_

- [x] 7. Mode handlers
  - [x] 7.1 Implement Recorder mode handler
    - Create `Recorder` struct with client, writer, topics, qos
    - Implement `run()` loop: subscribe, receive messages, write to CSV
    - Handle shutdown signal for graceful termination
    - Log message count on completion
    - _Requirements: 4.1-4.9_

  - [x] 7.2 Implement Replayer mode handler
    - Create `Replayer` struct with client, reader, loop flag
    - Implement `run()` loop: read CSV, calculate delays, publish messages
    - Support loop replay with reader reset
    - Handle shutdown signal for graceful termination
    - Support optional embedded broker publishing
    - Log message count on completion
    - _Requirements: 5.1-5.11_

  - [x] 7.3 Implement Mirror mode handler
    - Create `Mirror` struct with source client, broker, local client, optional writer
    - Implement `run()` loop: receive from source, republish to embedded, optionally record
    - Handle shutdown signal for graceful termination
    - Log mirrored message count periodically
    - _Requirements: 11.1-11.9_

- [x] 8. Main application wiring
  - [x] 8.1 Implement main.rs with mode dispatch
    - Parse CLI arguments
    - Validate argument combinations
    - Set up signal handlers for SIGINT/SIGTERM
    - Dispatch to appropriate mode handler based on args
    - Handle standalone broker mode
    - Implement graceful shutdown coordination
    - _Requirements: 1.19-1.26, 8.1-8.5, 9.1-9.5_

- [x] 9. Checkpoint - Core functionality complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. CI/CD and documentation
  - [x] 10.1 Create GitHub Actions CI workflow
    - Configure triggers for push/PR to main
    - Add cargo fmt check step
    - Add cargo clippy step
    - Add cargo test step (unit, property, integration)
    - Configure dependency caching
    - _Requirements: 12.1-12.4, 12.8_

  - [x] 10.2 Create GitHub Actions release workflow
    - Configure trigger on version tags (v*)
    - Build binaries for Linux x86_64, macOS x86_64/aarch64, Windows x86_64
    - Generate SHA256 checksums
    - Create GitHub release with artifacts
    - _Requirements: 12.5-12.7_

  - [x] 10.3 Create project documentation
    - Write README.md with installation, usage examples, CLI reference
    - Write AGENTS.md with AI assistant guidance
    - Write CONTRIBUTING.md with contribution guidelines
    - Add Apache 2.0 LICENSE file
    - _Requirements: 13.1-13.6_

- [x] 11. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.
  - Verify `cargo fmt --check` passes
  - Verify `cargo clippy` passes with no warnings
  - Verify all documentation is complete

## Notes

- All tasks including property-based tests are required
- Each task references specific requirements for traceability
- Property tests use the `proptest` crate with minimum 100 iterations
- Integration tests require Docker for ephemeral MQTT broker (testcontainers)
- The implementation follows async Rust patterns with Tokio runtime
