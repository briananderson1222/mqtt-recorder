# Requirements Document

## Introduction

This document specifies requirements for the **Agent Room**: a multiplayer workspace where humans and Kiro AI agents collaborate as peer MQTT clients on the embedded broker that mqtt-recorder already ships (`--serve`). A human joins the room from a terminal REPL, brings in one or more Kiro agent teammates (each a persona wrapping `kiro-cli chat --no-interactive`), works through an idea in shared chat on the `room/chat` topic, and leaves with a PRD generated from the session transcript.

mqtt-recorder itself acts as the room's flight recorder: a stock `--mode record` process captures all room traffic to CSV, and the session can be replayed with its original timing using the existing replay mode. The new code is deliberately thin — small Python clients (paho-mqtt) in `room/` plus Kiro agent persona configs in `.kiro/agents/`. The Rust core remains untouched.

## Glossary

- **Room**: A shared MQTT topic namespace (`room/#`) on the embedded broker where Participants exchange messages
- **Room_Broker**: The embedded rumqttd broker started with `mqtt-recorder --serve`
- **Flight_Recorder**: A stock `mqtt-recorder --mode record` process subscribed to the Room topics, persisting all traffic to a Session_CSV
- **Session_CSV**: The CSV file produced by the Flight_Recorder for one Room session, in the existing recorded format (timestamp, topic, payload, qos, retain)
- **Participant**: Any MQTT client publishing to the Room — a Human_Client or an Agent_Client
- **Human_Client**: A terminal REPL client that lets a person read and send Room messages
- **Agent_Client**: A client that wraps `kiro-cli chat --no-interactive` with a Persona, turning a Kiro agent into a Room Participant
- **Persona**: A Kiro agent config in `.kiro/agents/` whose prompt defines an agent's role, voice, and behavior in the Room
- **Room_Message**: A JSON payload published to `room/chat` with fields `sender` (string), `role` (string), `text` (string), and `ts` (ISO 8601 string)
- **Presence_Event**: A payload published to `room/presence` announcing that a Participant joined or left the Room
- **Mention**: The literal string `@<persona-name>` inside a Room_Message's `text` field, addressing a specific Participant
- **Reply_Limit**: A configurable maximum number of replies an Agent_Client may publish during one session
- **PRD_Generator**: A client that reads a Session_CSV, reconstructs the transcript, and produces `PRD.md` via the room-prd-writer Persona
- **Auto_Encode_Marker**: The `b64:` prefix used by the existing CSV handler to mark automatically base64-encoded payloads

## Requirements

### Requirement 1: Room Transport

**User Story:** As a participant, I want all room communication to flow over the embedded MQTT broker using a small, well-defined message schema, so that humans and agents interact as equal peers over a proven protocol.

#### Acceptance Criteria

1. THE Room SHALL use the Room_Broker as its only transport
2. THE Agent Room feature SHALL introduce no new server-side code; all broker capability SHALL come from the existing embedded broker
3. THE Participants SHALL exchange chat messages on the `room/chat` topic
4. THE Participants SHALL publish Presence_Events on the `room/presence` topic
5. THE Room_Message SHALL be a JSON object containing `sender` (string), `role` (string with value "human" or "agent"), `text` (string), and `ts` (ISO 8601 timestamp string)
6. THE Presence_Event SHALL be a JSON object containing `sender` (string), `role` (string with value "human" or "agent"), `event` (string with value "join" or "leave"), and `ts` (ISO 8601 timestamp string)
7. THE Participants SHALL publish Room_Messages and Presence_Events at QoS 1
8. THE Room_Broker SHALL bind to the loopback interface (127.0.0.1) by default; binding to any non-loopback interface SHALL require explicit operator action
9. WHEN a received payload on `room/chat` is not valid JSON or is missing a required Room_Message field, THE receiving Participant SHALL discard the payload and continue operating
10. IF an unexpected error occurs while discarding or processing an incoming payload, THEN THE receiving Participant SHALL log the error and continue operating

### Requirement 2: Human Client

**User Story:** As a human, I want a simple terminal client to join the room, so that I can read the conversation and contribute ideas.

#### Acceptance Criteria

1. WHEN the Human_Client starts, THE Human_Client SHALL connect to the Room_Broker, subscribe to `room/chat`, and publish a join Presence_Event to `room/presence`
2. WHEN the human enters a line at the prompt, THE Human_Client SHALL publish the line as a Room_Message with `role` set to "human"
3. WHEN a Room_Message from another Participant arrives, THE Human_Client SHALL print the message text together with the sender's name
4. THE Human_Client SHALL suppress from display only Room_Messages that it itself published during the current session; suppression SHALL NOT be based on sender-name matching alone
5. WHEN the human enters `/quit` or the input stream reaches end-of-file, THE Human_Client SHALL publish a leave Presence_Event and disconnect cleanly from the Room_Broker

### Requirement 3: Agent Client

**User Story:** As a human, I want to bring Kiro agents into the room as teammates with distinct personas, so that I get genuinely different perspectives while working through an idea.

#### Acceptance Criteria

1. WHEN the Agent_Client starts with a Persona name, THE Agent_Client SHALL connect to the Room_Broker, subscribe to `room/chat`, and publish a join Presence_Event identifying the Persona
2. WHEN a Room_Message with `role` "human" arrives, THE Agent_Client SHALL generate a reply by invoking `kiro-cli chat --no-interactive` with the Persona's agent config
3. WHEN a Room_Message with `role` "agent" arrives and the message text contains a Mention of the Agent_Client's Persona name, THE Agent_Client SHALL generate a reply
4. WHEN a Room_Message with `role` "agent" arrives and the message text does not contain a Mention of the Agent_Client's Persona name, THE Agent_Client SHALL ignore the message
5. WHEN a Room_Message whose `sender` matches the Agent_Client's own Persona name arrives, THE Agent_Client SHALL ignore the message
6. WHEN invoking `kiro-cli chat --no-interactive`, THE Agent_Client SHALL include the conversation history bounded to the most recent N Room_Messages, where N is configurable
7. WHEN the Agent_Client reaches its Reply_Limit, THE Agent_Client SHALL publish a Room_Message announcing that the limit was reached and SHALL stop generating further replies for the session
8. IF a `kiro-cli` invocation fails or exceeds a configured timeout, THEN THE Agent_Client SHALL publish a brief error notice as a Room_Message and continue running

### Requirement 4: Agent Personas

**User Story:** As a human, I want the agent teammates defined as version-controlled Kiro agent configs, so that their behavior lives alongside the project and reads like a chat rather than essays.

#### Acceptance Criteria

1. THE Personas SHALL be defined as Kiro agent configs in `.kiro/agents/`
2. THE project SHALL provide a "room-pm" Persona: a product manager who sharpens problem statements, asks about users, and drives toward a decision
3. THE project SHALL provide a "room-architect" Persona: a skeptical architect who probes feasibility, surfaces risks, and challenges scope
4. THE project SHALL provide a "room-prd-writer" Persona: a technical writer who turns a transcript into a structured PRD
5. THE Persona prompts for room chat SHALL instruct the agent to keep each reply to a few sentences
6. THE Persona prompts for room chat SHALL state the Mention convention (`@<persona-name>`) and list the other room Personas, so that agents can address one another
7. THE Personas used for room chat SHALL be configured with no file-writing tools and no shell tools

### Requirement 5: Flight Recorder Integration

**User Story:** As a participant, I want the whole session recorded automatically by mqtt-recorder's existing record mode, so that nothing said in the room is lost and the output can be generated from the actual conversation.

#### Acceptance Criteria

1. THE Flight_Recorder SHALL be a stock `mqtt-recorder --mode record` process subscribed to `room/#`
2. THE Flight_Recorder SHALL subscribe to `room/#` at QoS 1
3. THE Agent Room feature SHALL reuse the existing record mode without adding new recording code
4. THE Session_CSV SHALL capture every Room_Message and Presence_Event delivered to the Flight_Recorder, with its timestamp, topic, and payload
5. WHEN the session ends, THE Session_CSV SHALL pass the existing `--validate` mode with exactly zero errors; a validation with any error SHALL be treated as failure

### Requirement 6: PRD Generation

**User Story:** As a human, I want to leave the room with a PRD generated from the recorded transcript, so that the session produces a useful, durable artifact.

#### Acceptance Criteria

1. WHEN invoked with a Session_CSV path, THE PRD_Generator SHALL parse the file using the recorded format (timestamp, topic, payload, qos, retain)
2. WHEN reconstructing the transcript, THE PRD_Generator SHALL include only `room/chat` messages, ordered by timestamp
3. WHEN a recorded payload begins with the Auto_Encode_Marker, THE PRD_Generator SHALL base64-decode the payload before including it in the transcript
4. THE PRD_Generator SHALL invoke `kiro-cli chat --no-interactive` with the "room-prd-writer" Persona and the reconstructed transcript, and SHALL write the result to `PRD.md`
5. THE generated PRD SHALL contain, at minimum: problem statement, goals, non-goals, requirements, and open questions
6. IF the Session_CSV contains no `room/chat` messages, THEN THE PRD_Generator SHALL exit with a non-zero exit code and SHALL emit a descriptive error message; neither alone SHALL be considered sufficient
7. IF the `kiro-cli` invocation fails or exceeds a configured timeout, THEN THE PRD_Generator SHALL exit with a non-zero exit code, SHALL emit a descriptive error message, and SHALL NOT write a partial `PRD.md`

### Requirement 7: Session Replay

**User Story:** As a human, I want to replay the brainstorm with its original timing, so that I can review how the idea evolved and demonstrate the session.

#### Acceptance Criteria

1. THE Session_CSV SHALL be replayable with the existing `mqtt-recorder --mode replay` against the Room_Broker with no transformation of the file
2. WHILE a replay is running, THE Human_Client SHALL render replayed Room_Messages exactly as it renders live Room_Messages
3. WHEN replayed Room_Messages arrive, THE Human_Client SHALL display them regardless of their sender field, including messages originally sent by the viewing human
