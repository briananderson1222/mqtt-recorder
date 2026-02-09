# Implementation Plan: CSV Validation and Binary Payload Handling

## Overview

This implementation adds automatic binary payload detection and encoding, a CSV validation mode, a CSV repair mode, and ensures round-trip integrity for all MQTT message types. The work is organized to build foundational components first, then integrate them into the existing CSV handler, and finally add the validation and repair modes.

## Tasks

- [x] 1. Implement binary payload detection
  - [x] 1.1 Add `is_binary_payload` function to `src/csv_handler.rs`
    - Implement UTF8 validation check
    - Implement control character detection (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F)
    - Allow tab (0x09), newline (0x0A), carriage return (0x0D)
    - Add `AUTO_ENCODE_MARKER` constant ("b64:")
    - _Requirements: 1.1, 1.2, 1.3, 1.5_
  
  - [x] 1.2 Write property test for binary classification
    - **Property 1: Binary Payload Classification**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.5**

- [x] 2. Enhance CSV writer with auto-encoding
  - [x] 2.1 Add `WriteStats` struct to track encoding statistics
    - Track total_records, text_payloads, auto_encoded_payloads, largest_payload
    - _Requirements: 6.1, 6.2, 6.3, 6.6_
  
  - [x] 2.2 Add `write_bytes` method to `CsvWriter`
    - Accept payload as `&[u8]` instead of `&str`
    - Call `is_binary_payload` to determine encoding strategy
    - When binary and encode_b64 is false: prefix with "b64:" and base64 encode
    - When text and encode_b64 is false: write as-is
    - When encode_b64 is true: base64 encode without prefix (existing behavior)
    - Update statistics
    - _Requirements: 2.1, 2.2, 2.3_
  
  - [x] 2.3 Update existing `write` method to use `write_bytes` internally
    - Convert string payload to bytes
    - Delegate to `write_bytes`
    - _Requirements: 2.1, 2.2, 2.3_
  
  - [x] 2.4 Write property test for encoding strategy
    - **Property 2: Encoding Strategy Correctness**
    - **Validates: Requirements 2.1, 2.2, 2.3**

- [x] 3. Enhance CSV reader with auto-decoding
  - [x] 3.1 Add `MessageRecordBytes` struct for binary payload support
    - Same fields as `MessageRecord` but payload is `Vec<u8>`
    - _Requirements: 3.1_
  
  - [x] 3.2 Add `read_next_bytes` method to `CsvReader`
    - Check for "b64:" prefix when decode_b64 is false
    - Strip prefix and decode if present
    - Return payload as-is if no prefix
    - When decode_b64 is true: decode all payloads (existing behavior)
    - Return descriptive error for invalid base64
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  
  - [x] 3.3 Update existing `read_next` method to handle auto-encoded payloads
    - Use same logic as `read_next_bytes`
    - Convert decoded bytes to UTF8 string (lossy for binary)
    - _Requirements: 3.1, 3.2, 3.3_
  
  - [x] 3.4 Write property test for decoding strategy
    - **Property 3: Decoding Strategy Correctness**
    - **Validates: Requirements 3.1, 3.2, 3.3**

- [x] 4. Checkpoint - Ensure encoding/decoding tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement round-trip integrity
  - [x] 5.1 Handle marker collision edge case
    - When text payload starts with "b64:", ensure it round-trips correctly
    - Consider escaping strategy or always encoding such payloads
    - _Requirements: 5.4_
  
  - [x] 5.2 Write property test for round-trip integrity
    - **Property 5: Round-Trip Integrity**
    - **Validates: Requirements 5.1, 5.2, 5.3**
  
  - [x] 5.3 Write property test for marker collision handling
    - **Property 6: Marker Collision Handling**
    - **Validates: Requirements 5.4**

- [x] 6. Implement CSV validator
  - [x] 6.1 Create `src/validator.rs` module
    - Add `ValidationResult` enum for different error types
    - Add `ValidationStats` struct for statistics
    - Add `ValidationError` struct with line number
    - _Requirements: 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_
  
  - [x] 6.2 Implement `CsvValidator` struct
    - Constructor with decode_b64 and field_size_limit options
    - Implement `validate` method to process entire file
    - Implement `validate_record` for single record validation
    - Continue processing after errors to provide complete report
    - _Requirements: 4.2, 4.11_
  
  - [x] 6.3 Implement field validation logic
    - Validate field count (exactly 5)
    - Validate timestamp format (ISO 8601)
    - Validate QoS (0, 1, or 2)
    - Validate retain ("true" or "false")
    - Validate base64 when applicable
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_
  
  - [x] 6.4 Write property test for validation error detection
    - **Property 4: Validation Error Detection**
    - **Validates: Requirements 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.11**

- [x] 7. Implement CSV fixer
  - [x] 7.1 Create `src/fixer.rs` module
    - Add `RepairStats` struct for tracking repair statistics
    - _Requirements: 8.5_
  
  - [x] 7.2 Implement `CsvFixer` struct
    - Constructor with encode_b64 option
    - Implement `repair` method to process entire file
    - Implement `try_repair_record` for single record repair
    - _Requirements: 8.2, 8.3, 8.4_
  
  - [x] 7.3 Implement repair logic
    - Detect corrupted records (unencoded binary payloads)
    - Re-encode binary payloads with proper encoding
    - Preserve valid records unchanged
    - Skip unrecoverable records and report line numbers
    - _Requirements: 8.3, 8.4, 8.6, 8.7_
  
  - [x] 7.4 Write property test for repair mode
    - **Property 7: Repair Mode Correctness**
    - **Validates: Requirements 8.2, 8.3, 8.4, 8.5, 8.6, 8.7**

- [x] 8. Add CLI support for validation and fix modes
  - [x] 8.1 Add `--validate` argument to `Args` struct in `src/cli.rs`
    - Boolean flag, default false
    - _Requirements: 4.1_
  
  - [x] 8.2 Add `--fix` and `--output` arguments to `Args` struct
    - `--fix` boolean flag, default false
    - `--output` optional PathBuf for repaired file
    - _Requirements: 8.1, 8.8_
  
  - [x] 8.3 Add validation rules for new flags
    - Require `--file` when `--validate` is used
    - Require `--file` and `--output` when `--fix` is used
    - Disallow `--mode` when `--validate` or `--fix` is used
    - Disallow combining `--validate` and `--fix`
    - _Requirements: 4.1, 8.1, 8.8_
  
  - [x] 8.4 Add validation mode dispatch in `src/main.rs`
    - Check for validate flag before mode dispatch
    - Create CsvValidator and run validation
    - Print statistics report
    - Exit with code 0 on success, 3 on failure
    - _Requirements: 4.9, 4.10, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_
  
  - [x] 8.5 Add fix mode dispatch in `src/main.rs`
    - Check for fix flag before mode dispatch
    - Create CsvFixer and run repair
    - Print repair statistics
    - Exit with code 0 on success, 3 on failure
    - _Requirements: 8.2, 8.5_

- [x] 9. Update recorder to use binary-aware writing
  - [x] 9.1 Update `src/recorder.rs` to use `write_bytes`
    - Pass raw MQTT payload bytes to writer
    - Let writer handle encoding decision
    - _Requirements: 2.1, 2.2_

- [x] 10. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Update documentation
  - [x] 11.1 Update README.md with new features
    - Document `--validate` flag
    - Document `--fix` and `--output` flags
    - Document automatic binary encoding behavior
    - Document "b64:" prefix marker
    - Add examples for validation and fix modes
    - _Requirements: 4.1, 2.1, 8.1_
  
  - [x] 11.2 Add doc comments to new public APIs
    - Document `is_binary_payload` function
    - Document `write_bytes` method
    - Document `read_next_bytes` method
    - Document `CsvValidator` struct and methods
    - Document `CsvFixer` struct and methods
    - _Requirements: All_

- [x] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All tasks including property tests are required for comprehensive coverage
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The marker collision edge case (5.1) may require careful design - consider always encoding payloads that start with "b64:" to avoid ambiguity
