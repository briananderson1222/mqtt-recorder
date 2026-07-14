# mqtt-recorder

Record MQTT traffic to CSV files and replay it later — with an embedded broker,
live mirroring, and an interactive terminal dashboard.

[![CI](https://github.com/briananderson1222/mqtt-recorder/actions/workflows/ci.yml/badge.svg)](https://github.com/briananderson1222/mqtt-recorder/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

`mqtt-recorder` has four modes:

- **Record** — subscribe to topics on a broker and save every message to a CSV file
- **Replay** — publish a recorded CSV back to a broker, preserving the original timing
- **Mirror** — relay messages from an external broker onto a local embedded broker in real time
- **Serve** — run a standalone embedded MQTT broker (v5), no recording required

Binary payloads survive round-trips byte-for-byte (automatic base64 encoding),
timing is preserved and speed-adjustable on replay, and long-running modes get
a live TUI with toggles for everything.

## Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [The Interactive TUI](#the-interactive-tui)
- [Common Recipes](#common-recipes)
- [Security Notes](#security-notes)
- [Reference](#reference) — [CLI options](#cli-options) · [CSV format](#csv-file-format) · [Exit codes](#exit-codes) · [Signals](#signal-handling)
- [Development](#development)

## Installation

### Pre-built binaries (recommended)

Download from the [Releases](https://github.com/briananderson1222/mqtt-recorder/releases) page:
Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64), with
SHA-256 checksums.

### From source

Requires Rust 1.88 or later:

```bash
git clone https://github.com/briananderson1222/mqtt-recorder.git
cd mqtt-recorder
cargo build --release
# binary at target/release/mqtt-recorder
```

## Quick Start

Record everything a broker publishes:

```bash
mqtt-recorder --mode record --host localhost --file messages.csv
```

Record only specific topics:

```bash
mqtt-recorder --mode record --host localhost --file messages.csv -t "sensors/temperature"
```

Replay a recording (original timing preserved; add `--loop` to repeat, `--speed 2.0` for 2x):

```bash
mqtt-recorder --mode replay --host localhost --file messages.csv
```

Mirror an external broker onto a local embedded one:

```bash
mqtt-recorder --mode mirror --host external-broker.example.com --serve --serve-port 1884
```

Run a standalone local broker:

```bash
mqtt-recorder --serve --serve-port 1883
```

Check or repair a CSV recording:

```bash
mqtt-recorder --validate --file messages.csv
mqtt-recorder --fix --file corrupted.csv --output repaired.csv
```

## The Interactive TUI

Any mode that runs the embedded broker (`--serve`) gets a live dashboard by
default when run in a terminal: source/mirror/broker panels with message
rates, recording and playback controls, and a scrollable audit log.

| Key | Action |
|-----|--------|
| `q` / `Ctrl+C` | Quit (graceful shutdown) |
| `m` | Toggle mirroring on/off |
| `r` | Toggle recording on/off |
| `s` | Pause/resume the source broker connection |
| `p` | Toggle playback on/off |
| `l` | Toggle loop mode for playback |
| `+` / `-` | Increase / decrease playback speed |
| `f` | Choose a recording/playback file (playlist selection) |
| `↑` / `↓`, `Enter` | Navigate and confirm file selection |
| `a` | Toggle the audit log panel |
| `A` | Set an audit log file path |
| `j` / `k` | Scroll the audit log down / up |

Disable the TUI for scripting or CI:

```bash
mqtt-recorder --mode mirror --host broker.example.com --serve --no-interactive
```

## Common Recipes

### Authentication

```bash
# Environment variables (preferred: flags are visible in `ps` and shell history)
MQTT_PASSWORD=mypassword mqtt-recorder --mode record \
  --host broker.example.com --username myuser --file messages.csv

# Or flags
mqtt-recorder --mode record --host broker.example.com \
  --username myuser --password mypassword --file messages.csv
```

`MQTT_USERNAME` works the same way for the username.

### TLS

```bash
# Verify against a CA certificate
mqtt-recorder --mode record --host secure-broker.example.com --port 8883 \
  --enable-ssl --ca-cert /path/to/ca.crt --file messages.csv

# Mutual TLS with client certificates
mqtt-recorder --mode record --host secure-broker.example.com --port 8883 \
  --enable-ssl --ca-cert /path/to/ca.crt \
  --certfile /path/to/client.crt --keyfile /path/to/client.key \
  --file messages.csv

# Self-signed certs in test environments: skip server verification entirely.
# The connection stays encrypted but the peer is NOT authenticated.
mqtt-recorder --mode record --host test-broker.local --port 8883 \
  --enable-ssl --tls-insecure --file messages.csv
```

With `--enable-ssl` and no `--ca-cert`, the server is verified against the
system root store.

### Topic filtering

```bash
# Single topic (repeatable via a JSON file below); default is everything (#)
mqtt-recorder --mode record --host localhost --file messages.csv -t "sensors/+/temperature"

# Multiple topics from a JSON file
echo '{"topics": ["sensors/+/temperature", "actuators/#", "home/livingroom/light"]}' > topics.json
mqtt-recorder --mode record --host localhost --file messages.csv --topics topics.json
```

MQTT wildcards are supported: `+` matches one level, `#` matches all remaining
levels.

### Binary payloads

Nothing to configure: non-UTF-8 payloads are automatically base64 encoded with
a `b64:` prefix in the CSV and decoded on replay, byte-for-byte. To base64
encode *all* payloads instead (no prefix), pass `--encode-b64` when recording
and replaying. Details in [CSV file format](#csv-file-format).

### Replay targets, speed, and playlists

```bash
# Replay into an embedded broker (no external broker needed)
mqtt-recorder --mode replay --serve --serve-port 1884 --file messages.csv

# Replay to an external broker AND an embedded one simultaneously
mqtt-recorder --mode replay --host external-broker.example.com \
  --serve --serve-port 1884 --file messages.csv

# Continuous loop at 4x speed (0 = as fast as possible)
mqtt-recorder --mode replay --host localhost --file messages.csv --loop --speed 4.0

# Load extra files for playback selection in the TUI (repeatable)
mqtt-recorder --mode replay --serve --file main.csv \
  --playlist extra1.csv --playlist extra2.csv
```

### Mirroring and verification

```bash
# Mirror and record at the same time
mqtt-recorder --mode mirror --host external-broker.example.com --serve --file backup.csv

# Independently verify that the embedded broker delivers exactly what the
# source sent (reports matched / unexpected / missing)
mqtt-recorder --mode mirror --host broker.example.com --serve --serve-port 1884 --verify
```

### Audit logging

Structured audit events (connections, toggles, health checks) are shown in the
TUI by default (`--no-audit` disables). Write them to a file too:

```bash
mqtt-recorder --mode mirror --host broker.example.com --serve --audit-log /tmp/audit.log
```

### Shell completions

```bash
# zsh
mqtt-recorder --completions zsh > ~/.zfunc/_mqtt-recorder
# bash
mqtt-recorder --completions bash > /usr/local/etc/bash_completion.d/mqtt-recorder
```

Also available: `fish`, `elvish`, `powershell`.

## Security Notes

- **The embedded broker has no authentication and no TLS.** It binds to
  loopback (`127.0.0.1`) by default so only local processes can reach it.
  Pass `--bind-addr 0.0.0.0` (or a specific interface) only on networks where
  every reachable host is trusted — anyone who can reach the port can read
  all mirrored/replayed traffic and publish arbitrary messages.
- **Recordings and audit logs may contain sensitive payloads.** They are
  created with mode `0600` on Unix.
- **`--tls-insecure` disables server identity verification.** Encrypted but
  unauthenticated — never use it where man-in-the-middle is a concern.
- Prefer `MQTT_PASSWORD`/`MQTT_USERNAME` over the credential flags.

See [SECURITY.md](SECURITY.md) for the full policy and private vulnerability
reporting.

## Reference

### CLI options

#### Connection

| Argument | Description | Default |
|----------|-------------|---------|
| `--host` | MQTT broker address | Required (unless `--serve` in replay mode) |
| `--port` | MQTT broker port | `1883` |
| `--client-id` | MQTT client identifier | Auto-generated |
| `--mode` | Operation mode: `record`, `replay`, or `mirror` | Required (unless `--serve` alone) |
| `--file` | CSV file path for recording/replaying | Required for record/replay |
| `--mqtt-version` | MQTT protocol version for external brokers (`3.1.1` or `5`) | `5` |
| `--max-packet-size` | Maximum MQTT packet size in bytes | `1048576` (1MB) |
| `--health-check` | Health check interval in seconds (0 to disable) | `60` |

#### Topics

| Argument | Description | Default |
|----------|-------------|---------|
| `-t`, `--topic` | Single topic to subscribe | Subscribe to all (`#`) |
| `--topics` | JSON file containing topics to subscribe | None |
| `--qos` | QoS level for subscriptions (0, 1, or 2) | `0` |

#### Authentication and TLS

| Argument | Description | Default |
|----------|-------------|---------|
| `--username` | MQTT broker username (or `MQTT_USERNAME` env var) | None |
| `--password` | MQTT broker password (or `MQTT_PASSWORD` env var) | None |
| `--enable-ssl` | Enable TLS/SSL connection | `false` |
| `--tls-insecure` | Skip all server certificate verification (self-signed certs; connection stays encrypted but the peer is unauthenticated) | `false` |
| `--ca-cert` | Path to CA certificate file | None |
| `--certfile` | Path to client certificate file | None |
| `--keyfile` | Path to client private key file | None |

#### Replay

| Argument | Description | Default |
|----------|-------------|---------|
| `--loop` | Loop replay continuously | `false` |
| `--speed` | Playback speed multiplier (`0` = max speed, `1.0` = real-time, `2.0` = 2x faster) | `1.0` |
| `--playlist` | Additional CSV files for playback selection (repeatable) | None |

#### Mirror

| Argument | Description | Default |
|----------|-------------|---------|
| `--mirror` | Start with mirroring enabled | `true` |
| `--no-mirror` | Start with mirroring disabled | `false` |
| `--verify` | Verify mirrored messages against embedded broker output | `false` |

#### Embedded broker

| Argument | Description | Default |
|----------|-------------|---------|
| `--serve` | Start embedded MQTT broker (MQTT v5) | `false` |
| `--serve-port` | Embedded broker port | `1883` |
| `--bind-addr` | Bind address for the embedded broker | `127.0.0.1` |

#### TUI and audit

| Argument | Description | Default |
|----------|-------------|---------|
| `--no-interactive` | Disable interactive TUI mode | `false` |
| `--record` | Start with recording enabled | `true` if `--file` provided |
| `--audit` | Enable audit logging in TUI | `true` |
| `--no-audit` | Disable audit logging in the TUI | `false` |
| `--audit-log` | Path to write audit log file (auto-enables file writing) | None |

#### Encoding, validation, and repair

| Argument | Description | Default |
|----------|-------------|---------|
| `--encode-b64` | Encode all payloads as base64 | `false` |
| `--csv-field-size-limit` | Maximum CSV field size in bytes | None |
| `--validate` | Validate CSV file format and integrity | `false` |
| `--fix` | Repair corrupted CSV file | `false` |
| `--output` | Output path for repaired CSV file | Required with `--fix` |

#### Misc

| Argument | Description |
|----------|-------------|
| `--completions <SHELL>` | Print shell completions and exit |
| `--version` | Print version (includes git commit hash) |

### CSV file format

Messages are stored in RFC 4180 compliant CSV with the column order:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | ISO 8601 | Message timestamp with millisecond precision |
| `topic` | String | MQTT topic |
| `payload` | String | Message payload (raw, auto-encoded, or base64 encoded) |
| `qos` | Integer | Quality of Service level (0, 1, or 2) |
| `retain` | Boolean | Retain flag (`true` or `false`) |

**Payload encoding.** By default (no `--encode-b64`):

- Text payloads (valid UTF-8 without control characters) are stored as-is
- Binary payloads are automatically base64 encoded and prefixed with `b64:`

With `--encode-b64`, *all* payloads are base64 encoded without a prefix. The
`b64:` marker lets the reader distinguish intentionally-stored text that
happens to look like base64 from automatically encoded binary data.

```csv
timestamp,topic,payload,qos,retain
2024-01-15T10:30:00.123Z,sensors/temperature,{"value": 23.5},0,false
2024-01-15T10:30:01.456Z,sensors/humidity,{"value": 65},1,true
2024-01-15T10:30:02.789Z,binary/data,b64:CAoSGA==,0,false
```

### Topics JSON format

```json
{
  "topics": [
    "sensors/+/temperature",
    "actuators/#",
    "home/livingroom/light"
  ]
}
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success (including graceful shutdown via Ctrl+C) |
| 1 | Configuration/argument error |
| 2 | Connection/authentication error |
| 3 | File I/O error or validation/repair failure |
| 4 | Runtime error (unrecoverable) |

### Signal handling

**SIGINT** (Ctrl+C) and **SIGTERM** both trigger graceful shutdown: recording
flushes and closes the CSV file, brokers disconnect cleanly, and the embedded
broker is released.

## Development

Requires Rust 1.88 or later. The Makefile targets are what CI runs:

```bash
make build          # Debug build
make release        # Release build
make test           # Run all tests
make test-unit      # Unit tests only
make test-property  # Property-based tests
make clippy         # Lints (CI fails on warnings)
make fmt            # Format code
make coverage       # Coverage summary (requires cargo-llvm-cov)
make coverage-html  # HTML coverage report
```

For coverage, install [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov)
(`cargo install cargo-llvm-cov`). On macOS with Homebrew Rust you may need
`brew install llvm`.

Project layout, conventions, and the release process are documented in
[CONTRIBUTING.md](CONTRIBUTING.md); the changelog is in
[CHANGELOG.md](CHANGELOG.md).

### The living-spec hook

This project was built spec-first with [Kiro](https://kiro.dev) — the specs in
[`.kiro/specs/`](.kiro/specs/) generated much of the code. But spec-driven
development is usually a one-way street: specs write the code, then rot as the
code evolves.

The [`living-spec` hook](.kiro/hooks/living-spec.json) closes the loop. Every
time a file in `src/` is saved in Kiro, an agent:

1. Diffs the change and finds the spec that owns the modified behavior
2. **Repairs drift** — updates requirements/design sections to match the code
   as-built, or flags a `:warning: DRIFT` marker for a human when the change
   contradicts a recorded design decision
3. **Fills gaps** — if the module was never spec'd (code that grew past the
   original specs), it scaffolds a `:memo: DRAFT` spec reverse-engineered from the
   code for human review
4. Keeps the [CLI options](#cli-options) tables in sync with the `clap`
   definitions in `src/cli.rs`
5. Adds a [Keep a Changelog](https://keepachangelog.com) entry under
   `[Unreleased]`

The hook never commits — it leaves everything in the working tree for review.
Specs wrote the code; the code keeps the specs honest.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for
guidelines.

## License

This project is licensed under the Apache License 2.0 — see the
[LICENSE](LICENSE) file for details.
