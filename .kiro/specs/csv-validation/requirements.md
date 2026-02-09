# Requirements Document

## Introduction

This document specifies requirements for CSV validation and binary payload handling enhancements to the mqtt-recorder tool. The current implementation has issues when recording MQTT messages containing binary data (protobuf, raw bytes, etc.) without base64 encoding enabled. Binary payloads with control characters corrupt the CSV structure, making files unreadable or causing data loss during replay.

## Glossary

- **CSV_Handler**: The component responsible for reading and writing CSV files containing recorded messages
- **Binary_Payload**: An MQTT message payload containing non-UTF8 bytes or control characters (bytes 0x00-0x1F excluding tab, newline, carriage return)
- **Control_Character**: ASCII characters 0x00-0x1F that can disrupt CSV parsing (excluding 0x09 tab, 0x0A newline, 0x0D carriage return which are handled by CSV quoting)
- **Auto_Encode_Marker**: A prefix marker (`b64:`) indicating that a payload was automatically base64 encoded due to binary content detection
- **Payload_Validator**: The component that detects binary content in payloads and determines encoding strategy
- **CSV_Validator**: The component that validates CSV file integrity and readability
- **Round_Trip**: The process of writing a message to CSV and reading it back, which should produce identical data

## Requirements

### Requirement 1: Binary Payload Detection

**User Story:** As a user, I want the recorder to automatically detect binary payloads, so that I don't have to manually enable base64 encoding for mixed traffic containing both text and binary messages.

#### Acceptance Criteria

1. WHEN encode_b64 is false and a payload contains non-UTF8 bytes, THE Payload_Validator SHALL classify the payload as binary
2. WHEN encode_b64 is false and a payload contains control characters (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F), THE Payload_Validator SHALL classify the payload as binary
3. WHEN a payload contains only valid UTF8 with no control characters, THE Payload_Validator SHALL classify the payload as text
4. WHEN a payload is empty, THE Payload_Validator SHALL classify the payload as text
5. THE Payload_Validator SHALL allow tab (0x09), newline (0x0A), and carriage return (0x0D) in text payloads since CSV quoting handles these

### Requirement 2: Automatic Binary Encoding

**User Story:** As a user, I want binary payloads to be automatically encoded when base64 is disabled, so that the CSV file remains valid and parseable.

#### Acceptance Criteria

1. WHEN encode_b64 is false and a payload is classified as binary, THE CSV_Handler SHALL base64 encode the payload and prefix it with "b64:"
2. WHEN encode_b64 is false and a payload is classified as text, THE CSV_Handler SHALL write the payload as-is without any prefix
3. WHEN encode_b64 is true, THE CSV_Handler SHALL base64 encode all payloads without any prefix (existing behavior)
4. THE Auto_Encode_Marker "b64:" SHALL be used only when encode_b64 is false and automatic encoding is applied

### Requirement 3: Automatic Binary Decoding

**User Story:** As a user, I want the replayer to automatically detect and decode auto-encoded binary payloads, so that replay works correctly regardless of how payloads were encoded.

#### Acceptance Criteria

1. WHEN decode_b64 is false and a payload starts with "b64:", THE CSV_Handler SHALL strip the prefix and base64 decode the remaining content
2. WHEN decode_b64 is false and a payload does not start with "b64:", THE CSV_Handler SHALL use the payload as-is
3. WHEN decode_b64 is true, THE CSV_Handler SHALL base64 decode all payloads without checking for prefix (existing behavior)
4. IF a payload starts with "b64:" but the remaining content is not valid base64, THEN THE CSV_Handler SHALL return an error with a descriptive message

### Requirement 4: CSV Validation Mode

**User Story:** As a user, I want to validate a CSV file before attempting replay, so that I can identify and fix issues without affecting the broker.

#### Acceptance Criteria

1. THE MQTT_Recorder SHALL accept `--validate` as an argument to validate a CSV file without replaying
2. WHEN validate mode is active, THE CSV_Validator SHALL read all records from the CSV file
3. WHEN validate mode is active, THE CSV_Validator SHALL verify each record has exactly 5 fields (timestamp, topic, payload, qos, retain)
4. WHEN validate mode is active, THE CSV_Validator SHALL verify timestamps are valid ISO 8601 format
5. WHEN validate mode is active, THE CSV_Validator SHALL verify QoS values are 0, 1, or 2
6. WHEN validate mode is active, THE CSV_Validator SHALL verify retain values are "true" or "false"
7. WHEN validate mode is active and decode_b64 is true, THE CSV_Validator SHALL verify all payloads are valid base64
8. WHEN validate mode is active and decode_b64 is false, THE CSV_Validator SHALL verify payloads with "b64:" prefix contain valid base64
9. WHEN validation succeeds, THE CSV_Validator SHALL report the total number of valid records and exit with code 0
10. WHEN validation fails, THE CSV_Validator SHALL report the line number and error details for each invalid record and exit with code 3
11. THE CSV_Validator SHALL continue checking all records even after finding errors to provide a complete report

### Requirement 5: Round-Trip Integrity

**User Story:** As a user, I want assurance that any message I record can be replayed with identical content, so that I can trust the recording/replay process.

#### Acceptance Criteria

1. FOR ALL valid MQTT payloads (text or binary), writing to CSV and reading back SHALL produce the identical byte sequence
2. FOR ALL MessageRecords, the topic, QoS, and retain flag SHALL be preserved exactly through CSV round-trip
3. FOR ALL MessageRecords, the timestamp SHALL be preserved with millisecond precision through CSV round-trip
4. WHEN a payload contains the literal string "b64:" at the start, THE CSV_Handler SHALL handle it correctly without misinterpreting it as an auto-encode marker

### Requirement 6: Validation Statistics

**User Story:** As a user, I want detailed statistics from validation, so that I can understand the composition of my recorded data.

#### Acceptance Criteria

1. WHEN validation completes, THE CSV_Validator SHALL report the total number of records processed
2. WHEN validation completes, THE CSV_Validator SHALL report the number of text payloads
3. WHEN validation completes, THE CSV_Validator SHALL report the number of binary payloads (auto-encoded with "b64:" prefix)
4. WHEN validation completes, THE CSV_Validator SHALL report the number of base64 payloads (when decode_b64 is true)
5. WHEN validation completes, THE CSV_Validator SHALL report the number of invalid records (if any)
6. WHEN validation completes, THE CSV_Validator SHALL report the largest payload size encountered

### Requirement 7: Error Handling for Binary Detection

**User Story:** As a user, I want clear error messages when binary payload handling fails, so that I can diagnose and fix issues.

#### Acceptance Criteria

1. IF base64 decoding fails for an auto-encoded payload, THEN THE CSV_Handler SHALL report the line number and the invalid base64 content
2. IF a CSV record has an incorrect number of fields, THEN THE CSV_Validator SHALL report the line number and actual field count
3. IF a timestamp cannot be parsed, THEN THE CSV_Validator SHALL report the line number and the invalid timestamp string
4. IF a QoS value is invalid, THEN THE CSV_Validator SHALL report the line number and the invalid value
5. THE error messages SHALL include suggestions for resolution when applicable

### Requirement 8: CSV Repair Mode

**User Story:** As a user, I want to repair corrupted CSV files, so that I can recover data from recordings that were made before binary detection was implemented.

#### Acceptance Criteria

1. THE MQTT_Recorder SHALL accept `--fix` as an argument to repair a CSV file
2. WHEN fix mode is active, THE CSV_Handler SHALL read the input file and write a repaired version to a new file
3. WHEN fix mode is active, THE CSV_Handler SHALL detect corrupted records caused by unencoded binary payloads
4. WHEN fix mode is active and a corrupted record is detected, THE CSV_Handler SHALL attempt to recover the record by re-encoding the payload
5. WHEN fix mode is active, THE CSV_Handler SHALL report the number of records repaired
6. WHEN fix mode is active, THE CSV_Handler SHALL preserve valid records unchanged
7. IF a record cannot be repaired, THEN THE CSV_Handler SHALL skip it and report the line number
8. THE fix mode SHALL require `--file` for input and `--output` for the repaired file path

