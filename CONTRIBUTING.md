# Contributing to mqtt-recorder

Thank you for your interest in contributing to mqtt-recorder! This document provides guidelines and instructions for contributing.

## Code of Conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful
and constructive; we welcome contributors of all experience levels.

## Getting Started

### Prerequisites

- Rust 1.88 or later
- Cargo
- Git

No Docker or external broker needed: integration tests run against the
embedded rumqttd broker on dynamically allocated localhost ports.

### Setting Up the Development Environment

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/mqtt-recorder.git
   cd mqtt-recorder
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/briananderson1222/mqtt-recorder.git
   ```
4. Build the project:
   ```bash
   cargo build
   ```
5. Run tests to verify setup:
   ```bash
   cargo test
   ```

## Development Workflow

### Creating a Branch

Create a feature branch from `main`:

```bash
git checkout main
git pull upstream main
git checkout -b feature/your-feature-name
```

Use descriptive branch names:
- `feature/add-websocket-support`
- `fix/csv-parsing-error`
- `docs/update-readme`

### Making Changes

1. Make your changes in small, focused commits
2. Follow the code style guidelines (see below)
3. Add or update tests as needed
4. Update documentation if applicable

### Running Tests

```bash
# Run all tests (or: make test)
cargo test

# Run unit tests only (or: make test-unit)
cargo test --lib

# Run property-based tests (or: make test-property)
cargo test --test '*_props'

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

CI runs `make fmt-check`, `make clippy`, `make test-unit`, `make test-property`,
`make test`, `make release`, and `cargo audit` — the Makefile targets are the
source of truth for what must pass.

### Code Quality Checks

Before submitting, ensure your code passes all checks:

```bash
# Format code
cargo fmt

# Check formatting (CI will fail if not formatted)
cargo fmt --check

# Run clippy lints
cargo clippy -- -D warnings

# Build in release mode
cargo build --release
```

### Committing Changes

Use [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>: <description>

[optional body]
```

Types:
- `feat:` new features
- `fix:` bug fixes
- `docs:` documentation changes
- `test:` adding/updating tests
- `refactor:` code changes that neither fix bugs nor add features
- `chore:` maintenance tasks

Examples:
```
feat: add WebSocket connection support

- Implement WebSocket transport in mqtt.rs
- Add --websocket CLI flag
- Update documentation with WebSocket examples
```

```
fix: handle empty payloads in CSV reader
```

Guidelines:
- Use the imperative mood ("Add feature" not "Added feature")
- Keep the first line under 72 characters
- Add a blank line before the body
- Explain what and why, not how

### Submitting a Pull Request

1. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```
2. Open a Pull Request on GitHub
3. Fill out the PR template with:
   - Description of changes
   - Related issue numbers
   - Testing performed
4. Wait for CI checks to pass
5. Address any review feedback

If your change alters flags or user-visible behavior, update README.md and
add an entry under `Unreleased` in CHANGELOG.md as part of the same PR.

## Code Style Guidelines

### Rust Style

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `rustfmt` for formatting (default settings)
- Address all `clippy` warnings
- Use meaningful variable and function names

### Documentation

- Add doc comments (`///`) to all public items
- Include examples in doc comments where helpful
- Use `//!` for module-level documentation
- Keep comments up-to-date with code changes

### Error Handling

- Use `thiserror` for defining error types (`src/error.rs` is the single
  error enum; there is no `anyhow` in this codebase)
- Provide descriptive error messages
- Include context in error messages (file paths, values, etc.)

### Testing

- Write unit tests for new functionality
- Add property-based tests for core logic
- Test edge cases and error conditions
- Don't use mocks unless absolutely necessary

## Project Structure

```
mqtt-recorder/
├── src/
│   ├── main.rs          # Entry point, mode dispatch, signal handling
│   ├── lib.rs           # Library exports
│   ├── cli.rs           # CLI argument parsing (clap)
│   ├── mqtt.rs          # MQTT client wrapper (v4/v5)
│   ├── broker.rs        # Embedded MQTT broker (rumqttd)
│   ├── csv_handler/     # CSV handling
│   │   ├── mod.rs       # Module exports
│   │   ├── reader.rs    # CSV reading and decoding
│   │   ├── writer.rs    # CSV writing and encoding
│   │   ├── record.rs    # MessageRecord struct
│   │   └── encoding.rs  # Binary detection and base64 encoding
│   ├── tui/             # Interactive terminal UI (ratatui)
│   │   ├── mod.rs       # Module exports
│   │   ├── state.rs     # TUI state management
│   │   ├── render.rs    # UI rendering
│   │   ├── input.rs     # Keyboard input handling
│   │   └── types.rs     # TUI type definitions
│   ├── topics.rs        # Topic filtering and JSON parsing
│   ├── recorder.rs      # Record mode handler
│   ├── replayer.rs      # Replay mode handler
│   ├── mirror.rs        # Mirror mode handler
│   ├── validator.rs     # CSV validation logic
│   ├── fixer.rs         # CSV repair logic
│   ├── util.rs          # Shared utilities
│   └── error.rs         # Error types (thiserror)
├── tests/
│   ├── property/        # Property-based tests
│   └── integration/     # Integration tests
├── .github/
│   └── workflows/       # CI/CD pipelines
├── build.rs             # Embeds the git hash in --version
├── Cargo.toml
├── Makefile             # Canonical build/test/lint targets (used by CI)
├── README.md
├── CHANGELOG.md
├── CONTRIBUTING.md
├── SECURITY.md
├── AGENTS.md
└── LICENSE
```

## Types of Contributions

### Bug Reports

When reporting bugs, please include:
- mqtt-recorder version
- Operating system and version
- Steps to reproduce
- Expected behavior
- Actual behavior
- Error messages or logs

### Feature Requests

When requesting features, please include:
- Use case description
- Proposed solution (if any)
- Alternatives considered

### Documentation

Documentation improvements are always welcome:
- Fix typos or unclear explanations
- Add examples
- Improve README or CLI help text
- Add inline code comments

### Code Contributions

Code contributions should:
- Address a single concern
- Include tests
- Update documentation
- Pass all CI checks

## Review Process

1. A maintainer will review your PR
2. They may request changes or ask questions
3. Address feedback and push updates
4. Once approved, a maintainer will merge your PR

## Release Process

Releases are automated via GitHub Actions:
1. Move the `Unreleased` section of CHANGELOG.md under the new version and
   bump `version` in Cargo.toml (one PR)
2. Version tags (e.g., `v1.0.0`) trigger the release workflow
3. Binaries are built for Linux, macOS, and Windows
4. A GitHub Release is created with artifacts and checksums

Security issues: see [SECURITY.md](SECURITY.md) — report privately, not via
public issues.

## Getting Help

- Check existing issues and PRs
- Read the documentation in README.md
- Review the design document in `.kiro/specs/mqtt-recorder/design.md`
- Open an issue for questions

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

Thank you for contributing to mqtt-recorder!
