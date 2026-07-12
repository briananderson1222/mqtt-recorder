# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Replay mode now exits with code 2 when the broker never accepts the
  connection, instead of "replaying" into a locally-queued void and
  reporting success. The replayer waits for CONNACK (10s timeout) before
  publishing anything.

## [0.3.0] - 2026-07-12

### Fixed
- **Binary payloads are no longer corrupted by mirror and replay.** Both paths
  converted payloads through a lossy UTF-8 string, mangling non-UTF-8 data
  (protobuf, compressed payloads) before republishing and recording. Payloads
  now stay raw bytes end-to-end.
- Long gaps between recorded messages no longer block Ctrl+C, MQTT keepalives,
  or TUI toggles during mirror playback; replay shutdown mid-delay now
  disconnects from the broker gracefully.
- `--tls-insecure` now actually skips certificate verification (it previously
  fell through to full verification against system roots — a documented no-op).
- Every multi-word CLI flag in the README was documented in snake_case that the
  binary rejects; all corrected to the real kebab-case spellings.
- Replay connection failures are logged instead of silently retried forever.

### Added
- `--bind-addr` for the embedded broker (binds `127.0.0.1` by default — see
  Security below), `--no-mirror`, `--no-audit`, `--completions <shell>`, and
  `MQTT_USERNAME`/`MQTT_PASSWORD` environment variables for credentials.
- `--version` now includes the git commit hash.
- CI: `cargo audit` job with monthly scheduled run, Dependabot (monthly,
  grouped), all GitHub Actions pinned to commit SHAs.
- Test coverage for binary payload round-trips (property + integration) and
  `--speed` wall-clock timing.

### Changed
- **Security:** the embedded broker now binds to loopback by default instead
  of `0.0.0.0`. The broker has no authentication; exposing it on a network
  interface requires an explicit `--bind-addr` and logs a warning.
- Recorded CSVs and audit log files are created with mode `0600` on Unix.
- `--csv-field-size-limit` now bounds memory during parsing instead of
  checking after the full record is buffered.
- Dependencies: rumqttc 0.25, rumqttd 0.20, ratatui 0.30, crossterm 0.29,
  thiserror 2. The MQTT client TLS path now runs on a patched rustls-webpki.
- Minimum supported Rust version is 1.88 (the previous 1.70 claim was stale).

## [0.2.1] - 2026-04-07

### Fixed
- Aligned the crate version in Cargo.toml with the git tag so
  `mqtt-recorder --version` reports correctly. No functional changes.

## [0.2.0] - 2026-03-16

### Added
- Playlist support: load multiple CSV files for playback selection in replay
  mode (`--playlist`, repeatable).
- Periodic broker health checks reporting connections, subscriptions, and
  publish metrics (`--health-check`).
- Structured audit log with area/severity, viewable in the TUI and optionally
  written to a file (`--audit-log`).
- Replay improvements, including runtime-adjustable playback speed.

## [0.1.0] - 2026-02-12

### Added
- Initial release: record, replay, and mirror MQTT messages via CSV files.
- Embedded MQTT broker (rumqttd), interactive ratatui TUI, verify mode for
  mirrors, CSV validation and repair, MQTT v3.1.1/v5 support, TLS support,
  automatic base64 encoding for binary payloads.

[Unreleased]: https://github.com/briananderson1222/mqtt-recorder/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/briananderson1222/mqtt-recorder/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/briananderson1222/mqtt-recorder/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/briananderson1222/mqtt-recorder/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/briananderson1222/mqtt-recorder/releases/tag/v0.1.0
