# AI Assistant Guidance for mqtt-recorder

This document provides guidance for AI assistants (such as GitHub Copilot, Claude, ChatGPT, etc.) when working with this repository.

## About This File

This file serves as persistent context/memory for AI agents. Update it when:
- Adding new modules, features, or architectural changes
- The user repeatedly corrects the same behavior (capture the lesson here)
- Important patterns or conventions are established
- You learn something unique about this repository through trial and error

**Do not** reference this file in user-facing documentation (README, doc comments, etc.) unless explicitly requested.

## Project Overview

**mqtt-recorder** is a Rust CLI tool for recording and replaying MQTT messages. It provides four operational modes:

1. **Record**: Subscribe to MQTT topics and save messages to CSV
2. **Replay**: Read messages from CSV and publish to a broker with timing preservation
3. **Mirror**: Subscribe to an external broker and republish to an embedded broker
4. **Standalone Broker**: Run an embedded MQTT broker

Additionally, it provides utility modes for CSV file management:

5. **Validate**: Check CSV files for format correctness and data integrity
6. **Fix**: Repair corrupted CSV files by re-encoding binary payloads

## Architecture

```
src/
├── main.rs          # Application entry point, mode dispatch, signal handling
├── lib.rs           # Library exports
├── cli.rs           # CLI argument parsing (clap)
├── mqtt.rs          # MQTT client wrapper (rumqttc)
├── broker.rs        # Embedded broker (rumqttd)
├── csv_handler.rs   # CSV reading/writing, binary detection, auto-encoding
├── validator.rs     # CSV validation logic
├── fixer.rs         # CSV repair logic
├── topics.rs        # Topic filter and JSON parsing
├── recorder.rs      # Record mode handler
├── replayer.rs      # Replay mode handler
├── mirror.rs        # Mirror mode handler
└── error.rs         # Custom error types (thiserror)
```

## Key Dependencies

- **clap**: CLI argument parsing with derive macros
- **rumqttc**: MQTT client library
- **rumqttd**: Embedded MQTT broker
- **tokio**: Async runtime
- **serde/serde_json**: JSON serialization
- **csv**: CSV file handling
- **base64**: Payload encoding
- **chrono**: Timestamp handling
- **thiserror/anyhow**: Error handling

## Code Conventions

### Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/) format:
- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `test:` adding/updating tests
- `refactor:` code changes that neither fix bugs nor add features
- `chore:` maintenance tasks

### Error Handling

- Use `thiserror` for defining error types in `src/error.rs`
- Use `anyhow` for error propagation in application code
- Return `Result<T, MqttRecorderError>` from library functions
- Provide descriptive error messages with context

### Async Patterns

- Use `tokio` runtime for all async operations
- Use `tokio::sync::broadcast` for shutdown signaling
- Handle graceful shutdown via signal handlers

### Testing

- Unit tests are co-located in source files (`#[cfg(test)]` modules)
- Property-based tests use `proptest` crate in `tests/property/`
- Integration tests in `tests/integration/` (require Docker for MQTT broker)

### Documentation

- All public APIs have doc comments
- Use `///` for item documentation
- Use `//!` for module-level documentation
- Include examples in doc comments where helpful

## Common Tasks

### Adding a New CLI Argument

1. Add the field to `Args` struct in `src/cli.rs`
2. Add validation logic in `Args::validate()` if needed
3. Update the mode handlers to use the new argument
4. Add tests for the new argument
5. Update README.md CLI reference

### Adding a New Mode

1. Create a new mode handler in `src/<mode>.rs`
2. Add the mode variant to `Mode` enum in `src/cli.rs`
3. Add validation rules in `Args::validate()`
4. Add dispatch logic in `src/main.rs`
5. Add tests and documentation

### Modifying CSV Format

1. Update `MessageRecord` struct in `src/csv_handler.rs`
2. Update `CsvWriter` and `CsvReader` implementations
3. Update property tests in `tests/property/csv_props.rs`
4. Update README.md CSV format documentation

## Testing Guidelines

### Development Approach: Test-Driven Development (TDD)

This project follows strict TDD practices with property-based testing at its core:

1. **Write properties first**: Before implementing a feature, define the properties it must satisfy
2. **Add property tests**: Create property-based tests in `tests/property/` that encode these properties
3. **Implement the feature**: Write the minimal code to make properties pass
4. **Add integration tests**: For features involving external systems (MQTT broker, file I/O)
5. **Refactor**: Clean up while keeping all tests green

### Property-Based Testing (Required)

Property tests are the primary verification mechanism. When adding new functionality:

1. Identify invariants and properties the code must maintain
2. Look at existing property tests for patterns (e.g., `tests/property/csv_props.rs`)
3. Use `proptest` crate with appropriate strategies
4. Test edge cases through property generation, not manual enumeration

Example workflow for a new CSV feature:
```rust
// 1. Define the property
proptest! {
    #[test]
    fn roundtrip_preserves_data(record in any_message_record()) {
        // Write then read should return identical data
        let written = write_record(&record);
        let read = read_record(&written);
        prop_assert_eq!(record, read);
    }
}

// 2. Then implement the feature to satisfy this property
```

### Running Tests

```bash
# All tests
cargo test

# Unit tests only
cargo test --lib

# Property tests only
cargo test --test '*_props'

# Specific test
cargo test test_name
```

### Property-Based Tests

Property tests verify correctness properties from the design document:

- **Property 1-3**: CLI argument parsing (tests/property/cli_props.rs)
- **Property 4-6**: Topic filtering (tests/property/topics_props.rs)
- **Property 7-11**: CSV handling (tests/property/csv_props.rs)
- **Property 1-7**: CSV validation and binary handling (tests/property/csv_validation_props.rs)

When modifying core logic, ensure property tests still pass.

## Important Files

| File | Purpose |
|------|---------|
| `Cargo.toml` | Dependencies and project configuration |
| `src/cli.rs` | All CLI arguments and validation |
| `src/error.rs` | Error type definitions |
| `src/csv_handler.rs` | CSV format, binary detection, auto-encoding |
| `src/validator.rs` | CSV validation logic |
| `src/fixer.rs` | CSV repair logic |
| `.github/workflows/ci.yml` | CI pipeline configuration |
| `.github/workflows/release.yml` | Release automation |

## Design Decisions

### Why rumqttc + rumqttd?

- `rumqttc` is a mature, async MQTT client for Rust
- `rumqttd` provides an embedded broker for mirror/replay modes
- Both are from the same project, ensuring compatibility

### MQTT v4/v5 Split

- The embedded broker runs an MQTT v5 listener (rumqttd v5 config)
- Internal clients connecting to the embedded broker use `MqttClientV5` (rumqttc v5 module)
- External-facing clients (record mode, mirror source) use `MqttClient` (rumqttc v4) for maximum compatibility with third-party brokers
- `AnyMqttClient` enum wraps either client type for components that connect to both (Replayer, Recorder, Mirror source)
- `MqttIncoming` provides a unified event type so consumers don't depend on v4/v5 packet types directly
- QoS types differ between v4 and v5 in rumqttc; `MqttClientV5` converts internally

### Why CSV for Storage?

- Human-readable and editable
- Compatible with spreadsheet tools
- Simple to parse and generate
- Supports streaming writes (no need to buffer entire file)

### Why Base64 Encoding Option?

- MQTT payloads can be binary
- CSV doesn't handle binary data well
- Base64 ensures safe storage of any payload

### Automatic Binary Detection

- When `--encode_b64` is not set, payloads are automatically analyzed
- Binary payloads (non-UTF8 or control characters) are auto-encoded with `b64:` prefix
- Text payloads are stored as-is for human readability
- The `b64:` prefix allows distinguishing auto-encoded from literal content

## Troubleshooting

### Common Issues

1. **Connection refused**: Check broker host/port, ensure broker is running
2. **TLS errors**: Verify certificate paths and permissions
3. **CSV parsing errors**: Check for proper escaping, verify encoding matches
4. **Binary payload corruption**: Use `--validate` to check file, `--fix` to repair

### Debug Tips

- Use `RUST_LOG=debug` for verbose logging
- Check exit codes for error category
- Verify argument combinations with `--help`

## Contact

For questions about this codebase, refer to:
- [README.md](README.md) for usage documentation
- [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines

## Lessons Learned

Capture corrections and unique patterns discovered while working in this repo:

### TUI and Logging

- When TUI is active (`--serve` without `--no-interactive`), suppress all `eprintln!` logging
- Use `tui_state.is_some()` or a `tui_active` boolean to conditionally log
- Logging interferes with ratatui terminal rendering and corrupts the display
- Keep logging enabled for `--no-interactive` and CI modes

### Verify Mode (`--verify` on mirror)

- `--verify` spawns an independent MqttClientV5 subscriber on the embedded broker that subscribes to `#`
- Compares raw `(topic, payload_bytes)` from the source against what actually arrives on the embedded broker — completely bypasses CSV
- Three mismatch categories: **PAYLOAD MISMATCH** (same topic, different bytes), **UNEXPECTED** (message on broker not in expected queue), **MISSING** (expected but never received)
- All mismatches log the actual payload content (text or hex) to the audit log for debugging
- Use `--verify` as the primary debugging tool when investigating data integrity issues between source and embedded broker — if verify shows clean matches but CSV output differs, the bug is in the CSV layer
- The verify subscriber is registered as an internal client so it doesn't inflate the broker connection count
