# Implementation Plan: Agent Room

## Overview

This implementation adds a multiplayer agent room on top of mqtt-recorder's existing embedded broker: small Python clients in `room/` (shared plumbing, human REPL, Kiro agent wrapper, PRD generator) plus three Kiro agent persona configs in `.kiro/agents/`. The Rust core is untouched — transport, recording, validation, and replay all reuse stock mqtt-recorder features. The work is organized foundations-first: shared plumbing and its property tests land before the clients that depend on it, then personas, clients, the PRD generator, a transport smoke test, and a final live end-to-end rehearsal of the demo. Property-test subtasks marked `(stretch)` cover logic beyond the pure functions in `room/common.py` and can be deferred if the deadline demands; the core path is plumbing + Properties 1–3, personas, both clients, the PRD generator, the smoke test, and the live E2E.

## Tasks

- [x] 1. Implement shared plumbing in `room/common.py`
  - [x] 1.1 Create `Message` dataclass and encode/decode functions
    - Define `Message` with `sender`, `role`, `text`, `ts`, and `id` (fresh UUID4 generated on every publish)
    - Implement `encode_message` producing JSON with all required Room_Message fields
    - Implement `decode_message` with tolerant parsing: invalid JSON or a missing required field returns no message instead of raising
    - Define Presence_Event encoding with `sender`, `role`, `event` ("join"/"leave"), `ts`
    - _Requirements: 1.5, 1.6, 1.9_

  - [x] 1.2 Implement display helper pure functions
    - Implement `should_display(msg, published_ids)`: suppress if and only if `msg.id` is in the session's published-id set — never by sender-name matching
    - Implement `render_line(msg)` producing `[sender] text` output containing both sender name and message text
    - _Requirements: 2.3, 2.4, 7.2, 7.3_

  - [x] 1.3 Implement `RoomClient` paho-mqtt wrapper
    - Wrap paho-mqtt v2 API: connect, subscribe to `room/chat`, background network loop
    - Add publish helpers for chat messages and presence events, all at QoS 1
    - Route incoming payloads through `decode_message`; drop malformed payloads and continue operating
    - Log unexpected errors during incoming-payload processing and keep running
    - Define topic constants (`room/chat`, `room/presence`) and default host/port (`127.0.0.1:1884`)
    - _Requirements: 1.1, 1.3, 1.4, 1.7, 1.9, 1.10_

  - [x] 1.4 Write property test for message serialization round-trip
    - **Property 1: Message serialization round-trip**
    - **Validates: Requirements 1.5, 1.6**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 1: <property_text>` in `room/tests/test_props.py`

  - [x] 1.5 Write property test for malformed payload rejection
    - **Property 2: Malformed payload rejection**
    - **Validates: Requirements 1.9**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 2: <property_text>`

  - [x] 1.6 Write property test for session-based self-suppression
    - **Property 3: Session-based self-suppression**
    - **Validates: Requirements 2.4, 7.2, 7.3**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 3: <property_text>`

  - [x] 1.7 Write property test for rendered lines
    - **Property 4: Rendered lines carry sender and text**
    - **Validates: Requirements 2.3**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 4: <property_text>`

- [x] 2. Create agent persona configs in `.kiro/agents/`
  - [x] 2.1 Create `room-pm.json` persona
    - Product manager voice: sharpens problem statements, asks who the user is, drives toward a decision
    - `prompt` field with brevity instruction (2–4 sentences), Mention convention (`@<persona-name>`), and roster of the other room personas
    - `"tools": []` — no file-writing or shell tools
    - _Requirements: 4.1, 4.2, 4.5, 4.6, 4.7_

  - [x] 2.2 Create `room-architect.json` persona
    - Skeptical architect voice: probes feasibility, surfaces risks, challenges scope, concedes when convinced
    - `prompt` field with brevity instruction, Mention convention, and roster of other personas
    - `"tools": []` — no file-writing or shell tools
    - _Requirements: 4.1, 4.3, 4.5, 4.6, 4.7_

  - [x] 2.3 Create `room-prd-writer.json` persona
    - Technical writer voice: outputs only a Markdown PRD with fixed sections — Problem, Goals, Non-Goals, Requirements, Open Questions
    - _Requirements: 4.1, 4.4, 6.5_

  - [x] 2.4 Write persona config smoke checks
    - Assert the three `room-*.json` files exist in `.kiro/agents/`
    - Assert chat personas (`room-pm`, `room-architect`) have `"tools": []`
    - Assert chat prompts contain the brevity instruction, the Mention convention, and the roster of other personas
    - _Requirements: 4.1, 4.5, 4.6, 4.7_

- [x] 3. Implement human client in `room/human.py`
  - [x] 3.1 Implement REPL loop and presence lifecycle
    - On start: connect to the Room_Broker via `RoomClient`, subscribe to `room/chat`, publish a join Presence_Event
    - Main thread runs `input()` loop; paho network loop runs on its background thread
    - Each entered line publishes a Room_Message with `role: "human"` at QoS 1
    - `/quit` or EOF publishes a leave Presence_Event and disconnects cleanly
    - _Requirements: 2.1, 2.2, 2.5_

  - [x] 3.2 Wire id-based self-suppression and rendering
    - Track the `id` of every message this client publishes in an in-memory session set
    - Display incoming messages via `render_line` unless suppressed by `should_display` (id-based only)
    - Replayed messages flow through the same render path as live messages and display regardless of sender field
    - _Requirements: 2.3, 2.4, 7.2, 7.3_

- [x] 4. Implement agent client in `room/agent.py`
  - [x] 4.1 Implement pure `should_reply` decision function
    - Signature: `should_reply(msg, own_persona, replies_sent, max_replies)`
    - Never reply when `msg.sender` equals the agent's own persona name
    - Always consider messages with `role == "human"` from another sender
    - Reply to `role == "agent"` messages only when the text contains `@<own-persona-name>`
    - Once `replies_sent >= max_replies`, never reply again this session
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.7_

  - [x] 4.2 Implement agent lifecycle and serial reply worker
    - Args: `--persona <name>` (matches `.kiro/agents/room-<name>.json`), `--max-replies` (default 6), `--history` (default 12), `--timeout` (default 120)
    - On start: connect, subscribe to `room/chat`, publish a join Presence_Event identifying the persona
    - Serial worker queue: replies generated one at a time, never more than one kiro-cli call in flight
    - _Requirements: 3.1_

  - [x] 4.3 Implement kiro-cli invocation and prompt building
    - Build prompt from the persona's standing instruction plus the most recent `--history` messages as `sender: text` lines, in original order
    - Invoke `kiro-cli chat --no-interactive --agent room-<persona> <prompt>` as a subprocess with the configured timeout
    - Strip ANSI escape codes from output and publish the reply as a Room_Message at QoS 1
    - _Requirements: 3.2, 3.6_

  - [x] 4.4 Implement error notice and reply-limit announcement paths
    - On kiro-cli non-zero exit or timeout: publish a brief error notice as a Room_Message and keep running
    - On reaching the Reply_Limit: publish the limit announcement exactly once and stop generating replies for the session
    - _Requirements: 3.7, 3.8_

  - [x] 4.5 Write property test for never self-reply (stretch)
    - **Property 5: Never self-reply**
    - **Validates: Requirements 3.5**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 5: <property_text>`

  - [x] 4.6 Write property test for human messages always trigger (stretch)
    - **Property 6: Human messages always trigger**
    - **Validates: Requirements 3.2**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 6: <property_text>`

  - [x] 4.7 Write property test for mention-gated agent replies (stretch)
    - **Property 7: Agent messages trigger if and only if mentioned**
    - **Validates: Requirements 3.3, 3.4**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 7: <property_text>`

  - [x] 4.8 Write property test for reply limit monotonicity (stretch)
    - **Property 8: Reply limit is monotonic and announced once**
    - **Validates: Requirements 3.7**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 8: <property_text>`

  - [x] 4.9 Write property test for bounded, ordered history window (stretch)
    - **Property 9: History window is bounded and ordered**
    - **Validates: Requirements 3.6**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 9: <property_text>`

  - [x] 4.10 Write example tests for agent wiring with stubbed kiro-cli
    - Stub `kiro-cli` with a shell script placed first on PATH — no LLM cost
    - Reply-policy wiring: scripted human message + non-mentioning agent message → assert exactly one kiro-cli invocation
    - Failing stub (non-zero exit) → assert error notice published and agent keeps running
    - Sleeping stub (exceeds timeout) → assert timeout handled, error notice published, agent keeps running
    - Tests live in `room/tests/test_agent.py`
    - _Requirements: 3.2, 3.4, 3.8_

- [x] 5. Implement PRD generator in `room/prd.py`
  - [x] 5.1 Implement transcript reconstruction from Session_CSV
    - Parse the CSV with Python's `csv` module using the recorded format: timestamp, topic, payload, qos, retain
    - Filter to `topic == "room/chat"` rows only
    - Base64-decode any payload bearing the `b64:` Auto_Encode_Marker before including it
    - Parse payload JSON and sort messages by timestamp
    - Render the transcript as `sender (role): text` lines
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 5.2 Implement kiro-cli invocation with atomic buffered output
    - Invoke `kiro-cli chat --no-interactive --agent room-prd-writer` with the reconstructed transcript and a configured timeout
    - Capture output to an in-memory buffer; write `PRD.md` only after the subprocess exits 0 within the timeout
    - On kiro-cli failure or timeout: exit non-zero with a descriptive stderr message and leave the filesystem untouched — never a partial `PRD.md`
    - _Requirements: 6.4, 6.5, 6.7_

  - [x] 5.3 Implement empty-transcript and unreadable-file exit paths
    - No `room/chat` messages in the CSV → exit non-zero AND emit a descriptive error message (both required)
    - Missing or unreadable Session_CSV → exit non-zero with descriptive stderr message
    - _Requirements: 6.6_

  - [x] 5.4 Write property test for transcript reconstruction (stretch)
    - **Property 10: Transcript reconstruction**
    - **Validates: Requirements 6.1, 6.2, 6.3**
    - hypothesis, `max_examples=100`, tag `# Feature: agent-room, Property 10: <property_text>`

  - [x] 5.5 Write fixture-based example tests for the PRD pipeline
    - Fixed CSV fixture → `prd.py` with stubbed kiro-cli → assert the transcript passed to the stub and `PRD.md` written
    - Empty fixture → assert non-zero exit, descriptive stderr, and no `PRD.md`
    - Failing stub → assert non-zero exit and no `PRD.md` on disk
    - Tests live in `room/tests/test_prd.py`
    - _Requirements: 6.1, 6.4, 6.6, 6.7_

- [x] 6. Write transport smoke test (integration)
  - [x] 6.1 Create `room/tests/test_smoke.sh`
    - Start the broker (`mqtt-recorder --serve --serve-port 1884 --no-interactive`) and the flight recorder (`--mode record --qos 1 --file session.csv -t 'room/#' --no-interactive`)
    - Run two scripted publishers sending a known number of Room_Messages and Presence_Events at QoS 1
    - Assert the CSV row count matches the published message count
    - Assert `mqtt-recorder --validate --file session.csv` exits 0 with zero errors
    - Tear down all processes cleanly
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.7, 1.8, 5.1, 5.2, 5.3, 5.4, 5.5_

- [x] 7. Checkpoint — live end-to-end demo rehearsal (manual)
  - Full session per the runbook: broker → flight recorder → `agent.py --persona pm` + `agent.py --persona architect` → `human.py`
  - Brainstorm exchange with both live agents, then quit; run `python room/prd.py session.csv` and confirm `PRD.md` contains problem statement, goals, non-goals, requirements, and open questions
  - Disconnect agents, then replay: `mqtt-recorder --mode replay --file session.csv` watched from a fresh `human.py` (empty suppress set) — all messages display with original timing, including the human's own
  - Ensure all automated tests pass; ask the user if questions arise.
  - _Requirements: 6.5, 7.1, 7.2, 7.3_

## Notes

- Subtasks marked `(stretch)` are property tests beyond the pure functions in `room/common.py`; defer them if the deadline demands, but do not defer Properties 1–3
- Core path for tonight: task 1 (plumbing + Properties 1–3) → task 2 (personas) → tasks 3–4 (clients) → task 5 (PRD generator) → task 6 (smoke test) → task 7 (live E2E)
- Each task references specific requirements for traceability
- Property tests use hypothesis with `max_examples=100` and the tag format `# Feature: agent-room, Property N: <property_text>`
- Example and smoke tests stub `kiro-cli` on PATH — no LLM cost during automated testing
- The Rust core is untouched; the existing Rust test suite stands as-is
