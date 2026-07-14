# Requirements Document

## Introduction

This document specifies requirements for the **Agent Room**: a multiplayer workspace where humans and AI agents collaborate as peer MQTT clients on the embedded broker that mqtt-recorder already ships. A human joins the room from a terminal, brings in one or more Kiro agent teammates (each with a distinct persona), works through an idea in shared chat, and leaves with a useful final output — a PRD generated from the session transcript. mqtt-recorder itself acts as the room's flight recorder: every message is captured to CSV using the existing record mode, and the brainstorm can even be replayed with original timing using the existing replay mode.

The room is deliberately thin: all transport, recording, and replay capability comes from mqtt-recorder's existing features. The new code is a small set of Python clients and Kiro agent personas.

## Glossary

- **Room**: A shared MQTT topic namespace (`room/#`) on the embedded broker where participants exchange messages
- **Room_Broker**: The embedded rumqttd broker started with `mqtt-recorder --serve`
- **Flight_Recorder**: An `mqtt-recorder --mode record` client subscribed to the room topics, persisting all traffic to CSV
- **Participant**: Any MQTT client publishing to the room — a Human_Client or an Agent_Client
- **Human_Client**: A terminal REPL client that lets a person read and send room messages
- **Agent_Client**: A client that wraps `kiro-cli chat --no-interactive` with a persona, turning an LLM agent into a room participant
- **Persona**: A Kiro agent config (`.kiro/agents/*.json`) whose prompt defines an agent's role, voice, and behavior in the room
- **Room_Message**: A JSON payload published to `room/chat` with fields `sender`, `role`, `text`, and `ts`
- **PRD_Generator**: A client that reads the Flight_Recorder's CSV transcript and produces `PRD.md` via a Kiro agent
- **Mention**: The literal string `@<sender>` inside a Room_Message's text, addressing a specific participant

## Requirements

### Requirement 1: Room Transport

**User Story:** As a participant, I want all room communication to flow over the embedded MQTT broker, so that humans and agents interact as equal peers using a proven protocol.

#### Acceptance Criteria

1. THE Room SHALL use the Room_Broker (embedded broker from `--serve`) as its only transport; no new server code SHALL be written
2. THE Participants SHALL exchange chat messages on the `room/chat` topic
3. THE Participants SHALL announce join and leave events on the `room/presence` topic
4. THE Room_Message SHALL be a JSON object containing `sender` (string), `role` (string: "human" | "agent"), `text` (string), and `ts` (ISO 8601 string)
5. WHEN a Room_Message payload is not valid JSON or is missing required fields, THE receiving Participant SHALL ignore the message without crashing

### Requirement 2: Human Client

**User Story:** As a human, I want a simple terminal client to join the room, so that I can read the conversation and contribute ideas.

#### Acceptance Criteria

1. WHEN started, THE Human_Client SHALL connect to the Room_Broker, subscribe to `room/chat`, and publish a join event to `room/presence`
2. WHEN a line is entered at the prompt, THE Human_Client SHALL publish it as a Room_Message with `role` set to "human"
3. WHEN a Room_Message arrives from another Participant, THE Human_Client SHALL print it with the sender's name
4. THE Human_Client SHALL NOT print the human's own messages back (no echo)
5. WHEN the human types `/quit` or sends EOF, THE Human_Client SHALL publish a leave event and disconnect cleanly

### Requirement 3: Agent Client

**User Story:** As a human, I want to bring Kiro agents into the room as teammates with distinct personas, so that I get genuinely different perspectives while working through an idea.

#### Acceptance Criteria

1. WHEN started with a persona name, THE Agent_Client SHALL connect to the Room_Broker, subscribe to `room/chat`, and publish a join event identifying the persona
2. WHEN a Room_Message from a human arrives, THE Agent_Client SHALL generate a reply by invoking `kiro-cli chat --no-interactive` with the persona's agent config
3. WHEN a Room_Message from another agent arrives, THE Agent_Client SHALL reply only if the message Mentions the Agent_Client's persona name
4. THE Agent_Client SHALL include recent conversation history (bounded to the last N messages) in each kiro-cli invocation so replies have context
5. THE Agent_Client SHALL enforce a per-session reply limit, after which it stops replying and announces it has hit the limit
6. WHEN the kiro-cli invocation fails or times out, THE Agent_Client SHALL publish a brief error notice to the room and continue running
7. THE Agent_Client SHALL never reply to its own messages

### Requirement 4: Agent Personas

**User Story:** As a human, I want the agent teammates to be defined as Kiro agent configs with steering-style prompts, so that their behavior is version-controlled alongside the project.

#### Acceptance Criteria

1. THE Personas SHALL be defined as Kiro agent configs in `.kiro/agents/`
2. THE project SHALL provide a "room-pm" Persona: a product manager who sharpens problem statements, asks about users, and drives toward a decision
3. THE project SHALL provide a "room-architect" Persona: a skeptical architect who probes feasibility, surfaces risks, and challenges scope
4. THE project SHALL provide a "room-prd-writer" Persona: a technical writer who turns a transcript into a structured PRD
5. THE Personas SHALL keep replies short (a few sentences) so the room reads like a chat, not essays
6. THE Personas used for room chat SHALL have no file-writing or shell tools enabled

### Requirement 5: Flight Recorder Integration

**User Story:** As a participant, I want the whole session recorded automatically, so that nothing said in the room is lost and the output can be generated from the actual conversation.

#### Acceptance Criteria

1. THE Flight_Recorder SHALL be a stock `mqtt-recorder --mode record` process subscribed to `room/#`; no recording code SHALL be added
2. THE session CSV SHALL capture every Room_Message with its timestamp, topic, and payload
3. WHEN the session ends, THE CSV SHALL be readable by the existing `--validate` mode with zero errors

### Requirement 6: PRD Generation

**User Story:** As a human, I want to leave the room with a PRD generated from the recorded transcript, so that the session produces a useful, durable artifact.

#### Acceptance Criteria

1. WHEN invoked with a session CSV path, THE PRD_Generator SHALL parse the CSV using the recorded format (timestamp, topic, payload, qos, retain)
2. THE PRD_Generator SHALL reconstruct the conversation transcript from `room/chat` messages in timestamp order, decoding any `b64:`-prefixed payloads
3. THE PRD_Generator SHALL invoke `kiro-cli chat --no-interactive` with the "room-prd-writer" Persona and the transcript, and write the result to `PRD.md`
4. THE generated PRD SHALL contain, at minimum: problem statement, goals, non-goals, requirements, and open questions
5. IF the CSV contains no `room/chat` messages, THEN THE PRD_Generator SHALL exit with a non-zero code and a descriptive error

### Requirement 7: Session Replay

**User Story:** As a human, I want to replay the brainstorm with its original timing, so that I can review how the idea evolved (and show it off).

#### Acceptance Criteria

1. THE session CSV SHALL be replayable with the existing `mqtt-recorder --mode replay` against the Room_Broker with no transformation
2. WHEN a replay is running, THE Human_Client SHALL render the replayed messages exactly as it renders live messages
