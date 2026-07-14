# Design Document: Agent Room

## Overview

The Agent Room turns mqtt-recorder's embedded broker into a team room with a flight recorder. Humans and Kiro agents are peer MQTT clients on `room/chat`; a stock record-mode process captures everything to CSV; a PRD generator consumes the transcript and emits `PRD.md`. The design principle is **thin new code, thick reuse**: transport, recording, validation, and replay are all existing mqtt-recorder features (Req 1.2, 5.3). The only new code is ~4 small Python files and 3 Kiro agent persona configs.

All room traffic is published at QoS 1 (Req 1.7) and recorded at QoS 1 (Req 5.2), so a momentary client hiccup does not silently drop a message from the flight recording.

## Architecture

```
                        ┌──────────────────────────────┐
                        │  mqtt-recorder --serve       │
                        │  (embedded rumqttd broker,   │
                        │   127.0.0.1:1884)            │
                        └──────┬───────────┬───────────┘
               room/chat       │           │        room/# (QoS 1)
        ┌──────────┬───────────┼───────┐   │
        │          │           │       │   │
  ┌─────┴────┐ ┌───┴─────┐ ┌───┴────┐  │ ┌─┴──────────────────────┐
  │ human.py │ │ agent.py│ │agent.py│  │ │ mqtt-recorder          │
  │  (REPL)  │ │  (PM)   │ │ (arch.)│  │ │ --mode record --qos 1  │
  └──────────┘ └────┬────┘ └───┬────┘  │ │ --file session.csv     │
                    │          │       │ └───────────┬────────────┘
              kiro-cli chat  kiro-cli  │             │
              --agent room-pm  chat    │             ▼
                                       │      session.csv ──► prd.py ──► PRD.md
                                       │                      (kiro-cli chat
                                       │                       --agent room-prd-writer)
```

### Process Topology

Four kinds of processes, all independent OS processes connected over localhost MQTT:

1. **Room broker** — `mqtt-recorder --serve --serve-port 1884 --no-interactive`. Port 1884 avoids colliding with any local mosquitto on 1883. The broker binds to 127.0.0.1 by default (Req 1.8); see Operational Notes for the security posture.
2. **Flight recorder** — `mqtt-recorder --host 127.0.0.1 --port 1884 --mode record --qos 1 --file session.csv -t 'room/#' --no-interactive`. Runs `run_record_mode` (simple CLI recording — no TUI, no serve), which is exactly the stock recording path, subscribed at QoS 1 (Req 5.1, 5.2).
3. **Participants** — `room/human.py` and one `room/agent.py` per persona.
4. **PRD generator** — `room/prd.py session.csv`, run after the session.

Two mqtt-recorder processes (broker + recorder) rather than one unified serve process keeps each in its simplest, best-tested code path and makes the "flight recorder" a visibly separate character in the demo.


## Components and Interfaces

### `room/common.py` — shared plumbing

- `RoomClient`: thin paho-mqtt (v2 API) wrapper — connect, subscribe to `room/chat`, JSON encode/decode with tolerant parsing (payloads that are not valid JSON or are missing a required field are dropped, Req 1.9; an unexpected error while processing an incoming payload is logged and the client continues, Req 1.10), publish helpers for chat and presence. All publishes use QoS 1 (Req 1.7).
- `Message` dataclass mirroring the Room_Message schema, including the `id` field (a fresh UUID4 generated on every publish).
- Pure functions used by the property tests: `encode_message` / `decode_message`, `render_line(msg)`, and `should_display(msg, published_ids)`.
- Constants: topics (`room/chat`, `room/presence`), default host/port (`127.0.0.1:1884`).

### `room/human.py` — human REPL client

- Main thread runs `input()` loop; paho network loop runs on its background thread.
- Incoming messages print as `[sender] text` (Req 2.3).
- **Session-based self-suppression** (Req 2.4): the client keeps an in-memory set of the `id` values it generated for its own publishes this session. An incoming message is suppressed if and only if its `id` is in that set — never by sender-name matching. This is what makes replay work (Req 7.2, 7.3): a replayed session watched from a fresh client has an empty suppress set, so every replayed message displays, including the human's own past messages, and replayed messages flow through exactly the same render path as live ones.
- On each line entered, publish a Room_Message with `role: "human"` (Req 2.2).
- `/quit` or EOF → leave Presence_Event → clean disconnect (Req 2.5).

### `room/agent.py` — Kiro agent participant

- Args: `--persona <name>` (matches `.kiro/agents/room-<name>.json`), `--max-replies N` (default 6), `--history N` (default 12), `--timeout SECS` (default 120).
- **Reply policy** (loop prevention, Req 3.2–3.5, 3.7) — implemented as a pure decision function `should_reply(msg, own_persona, replies_sent, max_replies)` so it can be property-tested in isolation:
  - never reply to a message whose `sender` is the agent's own persona (Req 3.5);
  - always consider messages with `role == "human"` (Req 3.2);
  - reply to another agent only when the text contains a Mention of this persona, `@<persona-name>` (Req 3.3, 3.4);
  - once `replies_sent >= max_replies`, never reply again this session; announce the limit exactly once (Req 3.7).
- **Generation**: on each triggering message, build a prompt containing the persona's standing instruction, the last `--history` messages as `sender: text` lines (Req 3.6), and invoke:
  `kiro-cli chat --no-interactive --agent room-<persona> <prompt>`
  with the configured subprocess timeout. Output is stripped of ANSI escape codes and published as the reply at QoS 1.
- Replies are generated serially on a worker queue so an agent never talks over itself and never has more than one kiro-cli call in flight.
- kiro-cli non-zero exit or timeout → publish a short error notice as a Room_Message and keep running (Req 3.8).

### `room/prd.py` — PRD generator

- Reads the Session_CSV with Python's `csv` module using the recorded format: timestamp, topic, payload, qos, retain (Req 6.1).
- Filters `topic == "room/chat"`, decodes payloads carrying the Auto_Encode_Marker (`b64:` prefix, mirroring the recorder's auto-encode convention, Req 6.3), parses JSON, sorts by timestamp (Req 6.2).
- Renders the transcript as `sender (role): text` lines and invokes `kiro-cli chat --no-interactive --agent room-prd-writer` with the transcript (Req 6.4).
- **Atomic output** (Req 6.7): the kiro-cli result is captured to an in-memory buffer; `PRD.md` is written only after the subprocess exits 0 within the timeout. On failure or timeout, exit non-zero with a descriptive message on stderr and leave the filesystem untouched — a partial `PRD.md` is never produced.
- Empty transcript (no `room/chat` messages) → exit non-zero **and** emit a descriptive error message (Req 6.6).


### Personas — `.kiro/agents/room-*.json`

Same config shape as the existing `mqtt-dev.json`, plus a `prompt` field carrying the persona (Req 4.1). Chat personas get `"tools": []` — they are voices, not actors; no file-writing or shell tools (Req 4.7). Every room-chat prompt includes:

- a brevity instruction — "2–4 sentences, this is a chat room, not a design review" (Req 4.5);
- the Mention convention (`@<persona-name>`) and a roster of the other room Personas, so agents can address one another (Req 4.6).

Personas provided:

- **room-pm** — sharpens the problem, asks who the user is, pushes to a decision (Req 4.2).
- **room-architect** — skeptical; probes feasibility, risk, and scope creep; concedes when convinced (Req 4.3).
- **room-prd-writer** — outputs only Markdown PRD with fixed sections: Problem, Goals, Non-Goals, Requirements, Open Questions (Req 4.4, 6.5).

## Data Models

### Room_Message (`room/chat`)

```json
{
  "id": "6f1c2ab8-6f0e-4d9a-9d5b-2a41d7f3c001",
  "sender": "brian",
  "role": "human",
  "text": "let's spec the idea",
  "ts": "2026-07-14T21:04:05.123Z"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `sender` | string | Participant name (required, Req 1.5) |
| `role` | string | `"human"` or `"agent"` (required, Req 1.5) |
| `text` | string | Message body; may contain Mentions (required, Req 1.5) |
| `ts` | string | ISO 8601 timestamp (required, Req 1.5) |
| `id` | string | UUID4 generated per publish; additional field beyond the required set, used for session-based self-suppression (Req 2.4) |

### Presence_Event (`room/presence`)

```json
{"sender": "room-pm", "role": "agent", "event": "join", "ts": "2026-07-14T21:03:58.001Z"}
```

Fields: `sender` (string), `role` (`"human"` | `"agent"`), `event` (`"join"` | `"leave"`), `ts` (ISO 8601 string) — per Req 1.6. Published at QoS 1 like all room traffic (Req 1.7).


### Topic and Encoding Choices

JSON-over-one-topic (rather than `room/chat/<sender>`) keeps payloads self-describing in the CSV, so the PRD generator needs only the payload column. Payloads are plain UTF-8 text, so the recorder's automatic binary detection leaves them human-readable in the CSV — the flight recording is greppable. If a payload ever trips binary detection, the recorder writes it with the `b64:` Auto_Encode_Marker, which `prd.py` transparently decodes (Req 6.3).

### Session_CSV

The stock recorded format, untouched (Req 5.1, 5.4):

```csv
timestamp,topic,payload,qos,retain
2026-07-14T21:04:05.123Z,room/chat,"{""id"":""6f1c…"",""sender"":""brian"",…}",1,false
2026-07-14T21:03:58.001Z,room/presence,"{""sender"":""room-pm"",""event"":""join"",…}",1,false
```

## Loop & Cost Safety

Two agents that both answer everything would ping-pong forever. Three stacked guards:

1. agents reply to other agents **only when mentioned** (Req 3.3, 3.4);
2. hard `--max-replies` per agent per session (Req 3.7);
3. serial reply queue (one in-flight kiro-cli call per agent).

Worst case cost is bounded at `max_replies × number_of_agents` LLM calls per session.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

The Python clients isolate their decision logic in pure functions (`encode_message`/`decode_message`, `should_display`, `should_reply`, prompt builder, transcript reconstruction) precisely so these properties can be tested without a broker or an LLM.

### Property 1: Message serialization round-trip

*For any* valid Room_Message or Presence_Event value, encoding to JSON and decoding back SHALL produce an equivalent value, and the encoded JSON SHALL contain every required field for its type.

**Validates: Requirements 1.5, 1.6**

### Property 2: Malformed payload rejection

*For any* payload that is not valid JSON, or that is valid JSON missing at least one required Room_Message field, the decoder SHALL reject the payload (returning no message) without raising an unhandled exception.

**Validates: Requirements 1.9**

### Property 3: Session-based self-suppression

*For any* set of published message ids S and any incoming Room_Message m, the Human_Client SHALL suppress m if and only if `m.id ∈ S`. In particular, when S is empty (a fresh session watching a replay), every message SHALL display — including messages whose sender equals the viewing human's name — and a message with a matching sender but an id not in S SHALL display.

**Validates: Requirements 2.4, 7.2, 7.3**


### Property 4: Rendered lines carry sender and text

*For any* Room_Message, the rendered display line SHALL contain both the sender's name and the message text.

**Validates: Requirements 2.3**

### Property 5: Never self-reply

*For any* Room_Message whose `sender` equals the Agent_Client's own Persona name, the reply-policy function SHALL decide "ignore", regardless of role, Mentions, or reply count.

**Validates: Requirements 3.5**

### Property 6: Human messages always trigger

*For any* Room_Message with `role == "human"` from another sender, while the Agent_Client is below its Reply_Limit, the reply-policy function SHALL decide "reply".

**Validates: Requirements 3.2**

### Property 7: Agent messages trigger if and only if mentioned

*For any* Room_Message with `role == "agent"` from another sender, while below the Reply_Limit, the reply-policy function SHALL decide "reply" if and only if the message text contains `@<own-persona-name>`.

**Validates: Requirements 3.3, 3.4**

### Property 8: Reply limit is monotonic and announced once

*For any* sequence of triggering Room_Messages, once the number of replies sent reaches the Reply_Limit, the reply-policy function SHALL never again decide "reply" for the remainder of the session, and the limit announcement SHALL be published exactly once.

**Validates: Requirements 3.7**

### Property 9: History window is bounded and ordered

*For any* transcript and configured history size N, the prompt built for kiro-cli SHALL contain at most the N most recent Room_Messages, in their original order.

**Validates: Requirements 3.6**

### Property 10: Transcript reconstruction

*For any* list of recorded CSV rows with mixed topics and shuffled timestamps, the PRD_Generator's reconstruction SHALL contain exactly the `room/chat` messages, ordered by timestamp, with any payload bearing the `b64:` Auto_Encode_Marker base64-decoded to its original text (encode-then-reconstruct round-trips).

**Validates: Requirements 6.1, 6.2, 6.3**


## Error Handling

| Category | Condition | Handling Strategy |
|----------|-----------|-------------------|
| Transport | Payload on `room/chat` is not valid JSON or missing a required field | Drop silently, keep running (Req 1.9) — broker traffic may include replays or foreign publishers |
| Transport | Unexpected exception while processing an incoming payload | Log the error, keep running (Req 1.10) |
| Agent | kiro-cli non-zero exit or timeout | Publish `⚠ <persona> hit an error, continuing…` as a Room_Message; stay in the room (Req 3.8) |
| Agent | Reply_Limit reached | Publish a one-time announcement; ignore further triggers (Req 3.7) |
| PRD | Session_CSV missing or unreadable | Exit non-zero with descriptive stderr message |
| PRD | No `room/chat` messages in Session_CSV | Exit non-zero AND emit descriptive error — both required (Req 6.6) |
| PRD | kiro-cli failure or timeout | Exit non-zero, descriptive stderr message, `PRD.md` not written — output is buffered and only flushed on success, so a partial `PRD.md` is impossible (Req 6.7) |
| Human | `/quit` or EOF | Publish leave Presence_Event, disconnect cleanly (Req 2.5) |

## Testing Strategy

### Dual Testing Approach

The Rust core is untouched, so the existing suite stands. The new Python code uses both property-based tests (for the pure decision/serialization functions) and example/integration tests (for wiring and error paths), matching the project's evidence-first discipline.

### Property-Based Testing Configuration

- **Library**: `hypothesis` for Python (do not hand-roll generators/shrinkers)
- **Minimum iterations**: 100 per property test (`max_examples=100`)
- **Tag format**: `# Feature: agent-room, Property N: <property_text>`
- Each correctness property above is implemented by a single hypothesis test.

### Test Organization

```
room/
├── common.py            # pure functions under property test
├── human.py
├── agent.py
├── prd.py
└── tests/
    ├── test_props.py    # Properties 1–10 (hypothesis)
    ├── test_agent.py    # reply-policy wiring, stubbed kiro-cli
    ├── test_prd.py      # PRD pipeline with fixtures, stubbed kiro-cli
    └── test_smoke.sh    # transport smoke test (broker + recorder + publishers)
```


### Key Test Scenarios

1. **Transport smoke test** (integration): start broker + flight recorder (QoS 1) + two scripted publishers; assert CSV row count matches published count and `mqtt-recorder --validate --file session.csv` exits 0 (Req 1.3, 1.4, 5.1–5.5).
2. **Reply-policy test** (example): run `agent.py` against a scripted human message and a non-mentioning agent message; assert exactly one kiro-cli invocation. `kiro-cli` is stubbed with a shell script placed first on PATH — no LLM cost. A failing stub verifies the error-notice path (Req 3.8); a sleeping stub verifies the timeout path.
3. **PRD pipeline test** (example + edge cases): fixed CSV fixture → `prd.py` with stubbed kiro-cli → assert the transcript passed to the stub and `PRD.md` written. Empty fixture asserts non-zero exit and stderr message with no `PRD.md` (Req 6.6); failing stub asserts non-zero exit and no `PRD.md` (Req 6.7).
4. **Persona config checks** (smoke): assert the three `room-*.json` files exist, chat personas have `"tools": []` (Req 4.7), and prompts contain the brevity instruction, the Mention convention, and the roster of other personas (Req 4.5, 4.6).
5. **Live E2E** (manual, pre-demo): real session with two live agents, real PRD generation, replay of the CSV — the demo script itself is the acceptance test.

## Operational Notes (Runbook)

- **Startup order**: broker → flight recorder → agents → human. Suggested commands:
  ```bash
  mqtt-recorder --serve --serve-port 1884 --no-interactive
  mqtt-recorder --host 127.0.0.1 --port 1884 --mode record --qos 1 --file session.csv -t 'room/#' --no-interactive
  python room/agent.py --persona pm
  python room/agent.py --persona architect
  python room/human.py
  ```
- **Port**: use `--serve-port 1884` to avoid colliding with any local broker already on 1883.
- **Security posture** (Req 1.8): every command above uses the default 127.0.0.1 bind. The embedded broker has **no authentication**, so binding to a non-loopback interface must be an explicit, documented operator decision — never a default. The room is a local, single-machine demo surface.
- **Replay hygiene** (Req 7.1–7.3): **disconnect the Agent_Clients before running a replay.** Replayed messages carry `role: "human"`, so a live agent would treat them as fresh prompts and generate new replies; the Reply_Limit bounds the damage, but the clean demo flow is a human-only replay: broker + `human.py` (fresh session, empty suppress set) + `mqtt-recorder --host 127.0.0.1 --port 1884 --mode replay --file session.csv`.
- **PRD generation** runs after the session: `python room/prd.py session.csv` → `PRD.md`.

## Design Decisions

- **Why Python for the clients?** The room clients are demo-critical glue, not product code. paho-mqtt is the fastest credible path, and keeping them out of the Rust crate makes clear the product is unchanged (Req 1.2).
- **Why stateless kiro-cli calls with transcript replay?** `--no-interactive` gives one-shot calls; passing the last N messages (Req 3.6) is simpler and more robust than managing per-agent resume state, and N=12 is ample for a brainstorm.
- **Why not the unified serve mode with `--host` pointing at itself?** Self-mirroring is an untested corner (mirror republish loop risk). The two-process topology uses only well-trodden code paths.
- **Why an `id` field instead of sender-name suppression?** Name matching breaks replay: a fresh client watching a replay shares the original human's sender name, and name-based suppression would hide exactly the messages the demo needs to show. Per-publish UUIDs make suppression a session property, not an identity property (Req 2.4, 7.3). The requirements enumerate required fields; `id` is an additional, permitted field.
- **Why QoS 1 everywhere?** At QoS 0 a briefly wedged recorder drops messages and the Session_CSV silently diverges from the conversation; QoS 1 (Req 1.7, 5.2) makes the flight recording trustworthy at the cost of possible duplicates, which are harmless in a chat transcript.
