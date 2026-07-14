# Design Document: Agent Room

## Overview

The Agent Room turns mqtt-recorder's embedded broker into a team room with a flight recorder. Humans and Kiro agents are peer MQTT clients on `room/chat`; a stock record-mode process captures everything to CSV; a PRD generator consumes the transcript and emits `PRD.md`. The design principle is **thin new code, thick reuse**: transport, recording, validation, and replay are all existing mqtt-recorder features. The only new code is ~4 small Python files and 3 Kiro agent persona configs.

```
                        ┌──────────────────────────────┐
                        │  mqtt-recorder --serve       │
                        │  (embedded rumqttd broker)   │
                        └──────┬───────────┬───────────┘
               room/chat       │           │        room/#
        ┌──────────┬───────────┼───────┐   │
        │          │           │       │   │
  ┌─────┴────┐ ┌───┴─────┐ ┌───┴────┐  │ ┌─┴──────────────────────┐
  │ human.py │ │ agent.py│ │agent.py│  │ │ mqtt-recorder          │
  │  (REPL)  │ │  (PM)   │ │ (arch.)│  │ │ --mode record          │
  └──────────┘ └────┬────┘ └───┬────┘  │ │ --file session.csv     │
                    │          │       │ └───────────┬────────────┘
              kiro-cli chat  kiro-cli  │             │
              --agent room-pm  chat    │             ▼
                                       │      session.csv ──► prd.py ──► PRD.md
                                       │                      (kiro-cli chat
                                       │                       --agent room-prd-writer)
```

## Process Topology

Four kinds of processes, all independent OS processes connected over localhost MQTT:

1. **Room broker** — `mqtt-recorder --serve --serve-port 1884 --no-interactive`. Port 1884 avoids colliding with any local mosquitto on 1883.
2. **Flight recorder** — `mqtt-recorder --host 127.0.0.1 --port 1884 --mode record --file session.csv -t 'room/#' --no-interactive`. Runs `run_record_mode` (simple CLI recording — no TUI, no serve), which is exactly the stock recording path.
3. **Participants** — `room/human.py` and one `room/agent.py` per persona.
4. **PRD generator** — `room/prd.py session.csv`, run after the session.

Two mqtt-recorder processes (broker + recorder) rather than one unified serve process keeps each in its simplest, best-tested code path and makes the "flight recorder" a visibly separate character in the demo.

## Message Schema

Single topic `room/chat`, JSON payload:

```json
{"sender": "brian", "role": "human", "text": "let's spec the idea", "ts": "2026-07-14T21:04:05.123Z"}
```

Presence events on `room/presence`:

```json
{"sender": "room-pm", "role": "agent", "event": "join", "ts": "..."}
```

JSON-over-one-topic (rather than `room/chat/<sender>`) keeps payloads self-describing in the CSV, so the PRD generator needs only the payload column. Payloads are plain UTF-8 text, so the recorder's automatic binary detection leaves them human-readable in the CSV — the flight recording is greppable.

## Components

### `room/common.py` — shared plumbing

- `RoomClient`: thin paho-mqtt (v2 API) wrapper — connect, subscribe to `room/chat`, JSON encode/decode with tolerant parsing (malformed payloads are dropped, per Req 1.5), publish helpers for chat and presence.
- `Message` dataclass mirroring the schema.
- Constants: topics, default host/port (127.0.0.1:1884).

### `room/human.py` — human REPL client

- Main thread runs `input()` loop; paho network loop runs on its background thread.
- Incoming messages print as `[sender] text` (own messages suppressed, Req 2.4).
- `/quit` or EOF → leave event → clean disconnect.

### `room/agent.py` — Kiro agent participant

- Args: `--persona <name>` (matches `.kiro/agents/room-<name>.json`), `--max-replies N` (default 6), `--history N` (default 12).
- **Reply policy** (loop prevention, Req 3.3/3.7):
  - never reply to self;
  - always consider messages with `role == "human"`;
  - reply to another agent only when mentioned (`@<persona>` in text);
  - stop after `max-replies`, announcing the limit once.
- **Generation**: on each triggering message, build a prompt containing the persona's standing instruction ("you are in a team chat…"), the last `history` messages as `sender: text` lines, and invoke:
  `kiro-cli chat --no-interactive --agent room-<persona> <prompt>`
  with a subprocess timeout (default 120 s). Output is stripped of ANSI codes and published as the reply.
- Replies are generated serially on a worker queue so an agent never talks over itself.
- kiro-cli failure/timeout → publish short error notice, keep running (Req 3.6).

### `room/prd.py` — PRD generator

- Reads the session CSV with Python's `csv` module (columns: timestamp, topic, payload, qos, retain).
- Filters `topic == "room/chat"`, decodes `b64:`-prefixed payloads (mirrors the recorder's auto-encode marker), parses JSON, sorts by timestamp.
- Renders transcript as `sender (role): text` lines, invokes `kiro-cli chat --no-interactive --agent room-prd-writer` with the transcript and writes stdout to `PRD.md`.
- Empty transcript → exit 1 with a descriptive error (Req 6.5).

### Personas — `.kiro/agents/room-*.json`

Same config shape as the existing `mqtt-dev.json`, plus a `prompt` field carrying the persona. Chat personas get **no tools** (`"tools": []`) — they are voices, not actors (Req 4.6). Prompts enforce brevity: "2–4 sentences, this is a chat room, not a design review."

- **room-pm** — sharpens the problem, asks who the user is, pushes to a decision.
- **room-architect** — skeptical; probes feasibility, risk, and scope creep; concedes when convinced.
- **room-prd-writer** — outputs only Markdown PRD with fixed sections: Problem, Goals, Non-Goals, Requirements, Open Questions (Req 6.4).

## Loop & Cost Safety

Two agents that both answer everything would ping-pong forever. Three stacked guards:

1. agents reply to other agents **only when mentioned**;
2. hard `--max-replies` per agent per session;
3. serial reply queue (one in-flight kiro-cli call per agent).

Worst case cost is bounded at `max_replies × number_of_agents` LLM calls per session.

## Error Handling

- Malformed JSON on `room/chat`: dropped silently by clients (broker traffic may include replays or foreign publishers).
- kiro-cli non-zero exit / timeout: agent publishes `⚠ <persona> hit an error, continuing…` and stays in the room.
- PRD generator errors (missing file, no chat messages, kiro-cli failure) exit non-zero with a message on stderr; it never writes a partial `PRD.md`.

## Testing Strategy

The Rust core is untouched, so the existing suite stands. New-code verification is end-to-end, matching the project's evidence-first discipline:

1. **Transport smoke test**: broker + recorder + two scripted publishers; assert CSV row count and `--validate` exit 0 (Req 5.3).
2. **Reply-policy test**: run `agent.py` against a scripted human message and a non-mentioning agent message; assert exactly one kiro-cli invocation (stub `kiro-cli` with a shell script on PATH — no LLM cost).
3. **PRD pipeline test**: fixed CSV fixture → `prd.py` with stubbed kiro-cli → assert transcript passed to the stub and `PRD.md` written; empty-fixture case asserts exit 1.
4. **Live E2E** (manual, pre-demo): real session with two live agents, real PRD generation, replay of the CSV — the demo script itself is the acceptance test.

## Design Decisions

- **Why Python for the clients?** The room clients are demo-critical glue, not product code. paho-mqtt is the fastest credible path, and keeping them out of the Rust crate makes clear the product is unchanged.
- **Why stateless kiro-cli calls with transcript replay?** `--no-interactive` gives one-shot calls; passing the last N messages is simpler and more robust than managing per-agent resume state, and N=12 is ample for a brainstorm.
- **Why not the unified serve mode with `--host` pointing at itself?** Self-mirroring is an untested corner (mirror republish loop risk). The two-process topology uses only well-trodden code paths.
