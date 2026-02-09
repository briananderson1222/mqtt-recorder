# Contributing to mqtt-recorder

Thank you for your interest in contributing to mqtt-recorder! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please be respectful and constructive in all interactions. We welcome contributors of all experience levels.

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Cargo
- Git
- Docker (for integration tests)

### Setting Up the Development Environment

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/mqtt-recorder.git
   cd mqtt-recorder
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/mqtt-recorder.git
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
# Run all tests
cargo test

# Run unit tests only
cargo test --lib

# Run property-based tests
cargo test --test '*_props'

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

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

- Use `thiserror` for defining error types
- Provide descriptive error messages
- Include context in error messages (file paths, values, etc.)
- Use `anyhow` for error propagation in application code

### Testing

- Write unit tests for new functionality
- Add property-based tests for core logic
- Test edge cases and error conditions
- Don't use mocks unless absolutely necessary

## Project Structure

```
mqtt-recorder/
├── src/
│   ├── main.rs          # Entry point
│   ├── lib.rs           # Library exports
│   ├── cli.rs           # CLI argument parsing
│   ├── mqtt.rs          # MQTT client
│   ├── broker.rs        # Embedded broker
│   ├── csv_handler.rs   # CSV handling
│   ├── topics.rs        # Topic filtering
│   ├── recorder.rs      # Record mode
│   ├── replayer.rs      # Replay mode
│   ├── mirror.rs        # Mirror mode
│   └── error.rs         # Error types
├── tests/
│   ├── property/        # Property-based tests
│   └── integration/     # Integration tests
├── .github/
│   └── workflows/       # CI/CD pipelines
├── Cargo.toml
├── README.md
├── CONTRIBUTING.md
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
1. Version tags (e.g., `v1.0.0`) trigger the release workflow
2. Binaries are built for Linux, macOS, and Windows
3. A GitHub Release is created with artifacts and checksums

## Getting Help

- Check existing issues and PRs
- Read the documentation in README.md
- Review the design document in `.kiro/specs/mqtt-recorder/design.md`
- Open an issue for questions

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.

Thank you for contributing to mqtt-recorder!
