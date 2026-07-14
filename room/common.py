"""Shared plumbing for the Agent Room.

Defines the Room_Message schema (`Message`), tolerant JSON encode/decode,
pure display helpers, presence-event encoding, and the `RoomClient`
paho-mqtt wrapper used by every room participant.

The pure functions here (`encode_message`, `decode_message`,
`should_display`, `render_line`, `encode_presence`) are property-tested in
`room/tests/test_props.py` without a broker.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, field

import paho.mqtt.client as mqtt

# Topic and connection constants (Req 1.1, 1.3, 1.4, 1.8).
CHAT_TOPIC = "room/chat"
PRESENCE_TOPIC = "room/presence"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1884

# All room traffic is published at QoS 1 (Req 1.7).
QOS = 1

# Required fields of a Room_Message payload (Req 1.5). `id` is an
# additional field used for session-based self-suppression (Req 2.4).
REQUIRED_MESSAGE_FIELDS = ("sender", "role", "text", "ts")

logger = logging.getLogger(__name__)

# Escape sequences emitted by kiro-cli, stripped from its output.
#
# Order of alternation matters: OSC sequences must match FIRST. kiro-cli
# emits `ESC ] 9 ; Response complete <BEL>` (an OSC sequence, terminated by
# BEL \x07 or ST ESC-backslash). A regex whose two-char-escape branch can
# match `ESC ]` consumes only those two bytes and lets the OSC payload
# ("9;Response complete") leak through as plain text — observed live as
# literal "9;Response complete" embedded mid-word in published replies.
ANSI_RE = re.compile(
    r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\|$)"  # OSC: ESC ] ... (BEL | ST | EOF)
    r"|\x1b\[[0-9;?]*[ -/]*[@-~]"  # CSI: ESC [ params intermediates final
    r"|\x1b."  # remaining two-char escapes
)


def strip_ansi(text: str) -> str:
    """Remove ANSI/OSC escape sequences and stray BEL characters."""
    return ANSI_RE.sub("", text).replace("\x07", "")


def clean_output(text: str) -> str:
    """Clean raw kiro-cli stdout into plain reply text.

    Strips escape sequences (OSC, CSI, two-char; see ANSI_RE), then removes
    the "> " prompt-echo prefix, then trims surrounding whitespace.

    Prefix rule: kiro-cli prefixes its response with a colored "> " once, at
    the very start of the output only — verified against real output
    (`kiro-cli chat --no-interactive ... | cat -v` produced
    `^[[38;5;141m> ^[[0mhello ^[]9;Response complete^Gworld`). Subsequent
    lines are NOT prefixed, so only a single leading "> " is removed here.
    This deliberately leaves "> " on later lines untouched: agent replies
    and PRDs are Markdown, where "> " is legitimate blockquote syntax.
    """
    text = strip_ansi(text)
    if text.startswith("> "):
        text = text[2:]
    return text.strip()


@dataclass
class Message:
    """A Room_Message as exchanged on `room/chat` (Req 1.5).

    `id` is a fresh UUID4 generated per publish; it enables
    session-based self-suppression rather than sender-name matching.
    """

    sender: str
    role: str
    text: str
    ts: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


def encode_message(msg: Message) -> str:
    """Encode a Message to its JSON wire format with all required fields."""
    return json.dumps(
        {
            "id": msg.id,
            "sender": msg.sender,
            "role": msg.role,
            "text": msg.text,
            "ts": msg.ts,
        }
    )


def decode_message(payload: str | bytes) -> Message | None:
    """Decode a `room/chat` payload into a Message, tolerantly.

    Returns None (never raises) when the payload is not valid JSON, is not
    a JSON object, or is missing any required Room_Message field
    (Req 1.9). A missing `id` is tolerated: foreign publishers need only
    the required fields; a fresh UUID4 is assigned in that case.
    """
    try:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        data = json.loads(payload)
        if not isinstance(data, dict):
            return None
        for fname in REQUIRED_MESSAGE_FIELDS:
            if fname not in data or not isinstance(data[fname], str):
                return None
        msg_id = data.get("id")
        if not isinstance(msg_id, str):
            msg_id = str(uuid.uuid4())
        return Message(
            sender=data["sender"],
            role=data["role"],
            text=data["text"],
            ts=data["ts"],
            id=msg_id,
        )
    except Exception:
        return None


def encode_presence(sender: str, role: str, event: str, ts: str) -> str:
    """Encode a Presence_Event for `room/presence` (Req 1.6).

    `event` is "join" or "leave".
    """
    return json.dumps({"sender": sender, "role": role, "event": event, "ts": ts})


def should_display(msg: Message, published_ids: set[str]) -> bool:
    """Decide whether an incoming message should be displayed.

    Suppress if and only if `msg.id` is in the session's published-id set —
    never by sender-name matching (Req 2.4). This makes replay work: a
    fresh session has an empty set, so every replayed message displays,
    including the viewer's own past messages (Req 7.2, 7.3).
    """
    return msg.id not in published_ids


def render_line(msg: Message) -> str:
    """Render a message for terminal display as `[sender] text` (Req 2.3)."""
    return f"[{msg.sender}] {msg.text}"


class RoomClient:
    """Thin paho-mqtt (v2 API) wrapper for room participants.

    Connects to the Room_Broker, subscribes to `room/chat`, runs the
    network loop on a background thread, and routes incoming payloads
    through `decode_message`. Malformed payloads are dropped (Req 1.9);
    unexpected processing errors are logged and swallowed so the client
    keeps running (Req 1.10).
    """

    def __init__(
        self,
        client_id: str,
        on_message,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
    ):
        """Create a client. `on_message` is called with each decoded Message."""
        self._on_message = on_message
        self._host = host
        self._port = port
        # The embedded rumqttd broker exposes an MQTT v5 listener only, so
        # the client must speak MQTT v5 (paho defaults to v3.1.1, whose
        # CONNECT the broker never acknowledges).
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv5,
        )
        self._client.on_message = self._handle_message

    def connect(self) -> None:
        """Connect to the broker, subscribe to `room/chat`, start the loop."""
        self._client.connect(self._host, self._port)
        self._client.subscribe(CHAT_TOPIC, qos=QOS)
        self._client.loop_start()

    def disconnect(self) -> None:
        """Stop the network loop and disconnect cleanly."""
        self._client.disconnect()
        self._client.loop_stop()

    def publish_chat(self, msg: Message) -> None:
        """Publish a Room_Message to `room/chat` at QoS 1 (Req 1.3, 1.7)."""
        self._client.publish(CHAT_TOPIC, encode_message(msg), qos=QOS)

    def publish_presence(self, sender: str, role: str, event: str, ts: str) -> None:
        """Publish a Presence_Event to `room/presence` at QoS 1 (Req 1.4, 1.7)."""
        self._client.publish(PRESENCE_TOPIC, encode_presence(sender, role, event, ts), qos=QOS)

    def _handle_message(self, client, userdata, mqtt_msg) -> None:
        """paho on_message callback: decode, drop malformed, log-and-continue."""
        try:
            msg = decode_message(mqtt_msg.payload)
            if msg is None:
                return  # malformed payload dropped silently (Req 1.9)
            self._on_message(msg)
        except Exception:  # Req 1.10: log unexpected errors, keep running
            logger.exception("error processing incoming payload")
