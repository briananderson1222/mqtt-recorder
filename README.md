# mqtt-recorder

A command-line tool for recording and replaying MQTT messages, written in Rust.

[![CI](https://github.com/YOUR_USERNAME/mqtt-recorder/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/mqtt-recorder/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Features

- **Record Mode**: Subscribe to MQTT topics and save messages to a CSV file
- **Replay Mode**: Read messages from a CSV file and publish them to a broker with timing preservation
- **Mirror Mode**: Subscribe to an external broker and republish messages to an embedded broker in real-time
- **Standalone Broker**: Run an embedded MQTT broker without any recording or replaying
- **TLS/SSL Support**: Connect to secured brokers with certificate authentication
- **Base64 Encoding**: Optionally encode binary payloads as base64 for safe CSV storage
- **Automatic Binary Detection**: Automatically detects and encodes binary payloads with a `b64:` prefix
- **CSV Validation**: Validate CSV files for format correctness and data integrity
- **CSV Repair**: Repair corrupted CSV files by re-encoding binary payloads
- **Flexible Topic Filtering**: Subscribe to specific topics, use wildcards, or load topics from a JSON file

## Installation

### From Source

Ensure you have Rust installed (1.70 or later recommended), then:

```bash
git clone https://github.com/YOUR_USERNAME/mqtt-recorder.git
cd mqtt-recorder
cargo build --release
```

The binary will be available at `target/release/mqtt-recorder`.

### From Releases

Download pre-built binaries from the [Releases](https://github.com/YOUR_USERNAME/mqtt-recorder/releases) page for:
- Linux (x86_64)
- macOS (x86_64, aarch64)
- Windows (x86_64)

## Quick Start

### Record Messages

Record all messages from a broker to a CSV file:

```bash
mqtt-recorder --mode record --host localhost --file messages.csv
```

Record messages from specific topics:

```bash
mqtt-recorder --mode record --host localhost --file messages.csv -t "sensors/temperature"
```

### Replay Messages

Replay recorded messages to a broker:

```bash
mqtt-recorder --mode replay --host localhost --file messages.csv
```

Replay messages in a continuous loop:

```bash
mqtt-recorder --mode replay --host localhost --file messages.csv --loop
```

### Mirror Mode

Mirror messages from an external broker to a local embedded broker:

```bash
mqtt-recorder --mode mirror --host external-broker.example.com --serve --serve_port 1884
```

Mirror and record simultaneously:

```bash
mqtt-recorder --mode mirror --host external-broker.example.com --serve --file backup.csv
```

### Standalone Broker

Run an embedded MQTT broker:

```bash
mqtt-recorder --serve --serve_port 1883
```

### Validate CSV File

Check a CSV file for format errors:

```bash
mqtt-recorder --validate --file messages.csv
```

Validate with base64 decoding enabled:

```bash
mqtt-recorder --validate --file messages.csv --encode_b64
```

### Repair CSV File

Repair a corrupted CSV file by re-encoding binary payloads:

```bash
mqtt-recorder --fix --file corrupted.csv --output repaired.csv
```

## Usage Examples

### Recording with Authentication

```bash
mqtt-recorder --mode record \
  --host broker.example.com \
  --username myuser \
  --password mypassword \
  --file messages.csv
```

### Recording with TLS

```bash
mqtt-recorder --mode record \
  --host secure-broker.example.com \
  --port 8883 \
  --enable_ssl \
  --ca_cert /path/to/ca.crt \
  --file messages.csv
```

### Recording with Client Certificates

```bash
mqtt-recorder --mode record \
  --host secure-broker.example.com \
  --port 8883 \
  --enable_ssl \
  --ca_cert /path/to/ca.crt \
  --certfile /path/to/client.crt \
  --keyfile /path/to/client.key \
  --file messages.csv
```

### Recording with Base64 Encoding

For binary payloads, use base64 encoding to encode all payloads:

```bash
mqtt-recorder --mode record \
  --host localhost \
  --file messages.csv \
  --encode_b64
```

Without `--encode_b64`, binary payloads are automatically detected and encoded with a `b64:` prefix, while text payloads are stored as-is. This provides the best of both worlds: human-readable text and safe binary storage.

### Subscribing to Multiple Topics

Using a JSON file:

```bash
# Create topics.json
echo '{"topics": ["sensors/+/temperature", "actuators/#", "home/livingroom/light"]}' > topics.json

# Record from those topics
mqtt-recorder --mode record --host localhost --file messages.csv --topics topics.json
```

### Replay to Embedded Broker

Replay messages to an embedded broker (no external broker needed):

```bash
mqtt-recorder --mode replay --serve --serve_port 1884 --file messages.csv
```

### Replay to Both External and Embedded Broker

```bash
mqtt-recorder --mode replay \
  --host external-broker.example.com \
  --serve \
  --serve_port 1884 \
  --file messages.csv
```

## CLI Reference

### Global Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--host` | MQTT broker address | Required (unless `--serve` in replay mode) |
| `--port` | MQTT broker port | `1883` |
| `--client_id` | MQTT client identifier | Auto-generated |
| `--mode` | Operation mode: `record`, `replay`, or `mirror` | Required (unless `--serve` alone) |
| `--file` | CSV file path for recording/replaying | Required for record/replay |

### Topic Options

| Argument | Description | Default |
|----------|-------------|---------|
| `-t`, `--topic` | Single topic to subscribe | Subscribe to all (`#`) |
| `--topics` | JSON file containing topics to subscribe | None |
| `--qos` | QoS level for subscriptions (0, 1, or 2) | `0` |

### Replay Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--loop` | Loop replay continuously | `false` |

### TLS/SSL Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--enable_ssl` | Enable TLS/SSL connection | `false` |
| `--tls_insecure` | Skip TLS certificate verification | `false` |
| `--ca_cert` | Path to CA certificate file | None |
| `--certfile` | Path to client certificate file | None |
| `--keyfile` | Path to client private key file | None |

### Authentication Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--username` | MQTT broker username | None |
| `--password` | MQTT broker password | None |

### Encoding Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--encode_b64` | Encode all payloads as base64 | `false` |
| `--csv_field_size_limit` | Maximum CSV field size in bytes | None |

### Validation and Repair Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--validate` | Validate CSV file format and integrity | `false` |
| `--fix` | Repair corrupted CSV file | `false` |
| `--output` | Output path for repaired CSV file | Required with `--fix` |

### Advanced Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--max_packet_size` | Maximum MQTT packet size in bytes | `1048576` (1MB) |

### Embedded Broker Options

| Argument | Description | Default |
|----------|-------------|---------|
| `--serve` | Start embedded MQTT broker | `false` |
| `--serve_port` | Embedded broker port | `1883` |

## CSV File Format

Messages are stored in RFC 4180 compliant CSV format with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | ISO 8601 | Message timestamp with millisecond precision |
| `topic` | String | MQTT topic |
| `payload` | String | Message payload (raw, auto-encoded, or base64 encoded) |
| `qos` | Integer | Quality of Service level (0, 1, or 2) |
| `retain` | Boolean | Retain flag (`true` or `false`) |

### Payload Encoding

When `--encode_b64` is **not** set (default):
- **Text payloads** (valid UTF-8 without control characters) are stored as-is
- **Binary payloads** (non-UTF-8 or containing control characters) are automatically base64 encoded and prefixed with `b64:`

When `--encode_b64` is set:
- **All payloads** are base64 encoded without any prefix

The `b64:` prefix marker allows the reader to distinguish between:
- Intentionally stored text that happens to look like base64
- Automatically encoded binary data

Example:

```csv
timestamp,topic,payload,qos,retain
2024-01-15T10:30:00.123Z,sensors/temperature,{"value": 23.5},0,false
2024-01-15T10:30:01.456Z,sensors/humidity,{"value": 65},1,true
2024-01-15T10:30:02.789Z,binary/data,b64:CAoSGA==,0,false
```

## Topics JSON Format

When using `--topics`, provide a JSON file with the following structure:

```json
{
  "topics": [
    "sensors/+/temperature",
    "actuators/#",
    "home/livingroom/light"
  ]
}
```

MQTT wildcards are supported:
- `+` matches a single level (e.g., `sensors/+/temperature` matches `sensors/kitchen/temperature`)
- `#` matches multiple levels (e.g., `actuators/#` matches `actuators/fan/speed`)

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (including graceful shutdown via Ctrl+C) |
| 1 | Configuration/argument error |
| 2 | Connection/authentication error |
| 3 | File I/O error or validation/repair failure |
| 4 | Runtime error (unrecoverable) |

## Signal Handling

The tool handles the following signals gracefully:
- **SIGINT** (Ctrl+C): Initiates graceful shutdown
- **SIGTERM**: Initiates graceful shutdown

During shutdown:
- Recording mode: Flushes and closes the CSV file
- All modes: Disconnects cleanly from the broker
- Embedded broker: Shuts down gracefully

## Building from Source

### Prerequisites

- Rust 1.70 or later
- Cargo

### Build Commands

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run with clippy lints
cargo clippy

# Format code
cargo fmt
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
