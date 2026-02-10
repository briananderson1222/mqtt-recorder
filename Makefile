.PHONY: build test test-unit test-integration test-property clippy fmt coverage coverage-html clean run

# Build
build:
	cargo build

release:
	cargo build --release

# Run (builds first, pass args with: make run ARGS="--mode record --host localhost")
run:
	cargo run -- $(ARGS)

# Invoke without building (uses existing debug binary)
invoke:
	./target/debug/mqtt-recorder $(ARGS)

# Testing
test:
	cargo test

test-unit:
	cargo test --lib

test-integration:
	cargo test --test mirror_test

test-property:
	cargo test --test cli_props --test topics_props --test csv_props --test csv_validation_props --test tui_props -- --test-threads=1

# Linting
clippy:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

# Coverage (requires cargo-llvm-cov: cargo install cargo-llvm-cov)
# Auto-detects Homebrew LLVM on macOS if rustup llvm-tools-preview is not available
LLVM_PREFIX := $(shell brew --prefix llvm 2>/dev/null)
ifdef LLVM_PREFIX
  export LLVM_COV := $(LLVM_PREFIX)/bin/llvm-cov
  export LLVM_PROFDATA := $(LLVM_PREFIX)/bin/llvm-profdata
endif

coverage:
	cargo llvm-cov --lib --summary-only

coverage-html:
	cargo llvm-cov --lib --html
	@echo "Coverage report: target/llvm-cov/html/index.html"

coverage-all:
	cargo llvm-cov --summary-only

# Clean
clean:
	cargo clean
