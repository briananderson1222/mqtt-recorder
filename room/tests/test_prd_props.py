"""Property-based test for PRD transcript reconstruction.

Covers Property 10 from the agent-room design document, exercising
`reconstruct_transcript` from `room/prd.py` with hypothesis
(`max_examples=100`). Generates recorded CSV files with interleaved chat,
presence, malformed, and foreign-topic rows; a random subset of chat
payloads carry the `b64:` Auto_Encode_Marker. The expected transcript is
computed independently with the same stable timestamp sort and compared
as a full string.
"""

import base64
import csv
import json
import os
import tempfile

from hypothesis import given, settings
from hypothesis import strategies as st

from room.prd import reconstruct_transcript

MAX_EXAMPLES = 100

# JSON-embedded fields: no surrogates (json.dumps escapes control chars).
field_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), max_size=100
)
# Raw CSV fields (timestamps, foreign topics, malformed payloads): also no
# NUL, which the csv module rejects on read.
raw_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00"),
    max_size=60,
)
roles = st.sampled_from(["human", "agent"])


def _is_valid_chat_payload(payload: str) -> bool:
    """Mirror prd's acceptance check: JSON object with string sender/role/text."""
    try:
        data = json.loads(payload)
    except ValueError:
        return False
    return isinstance(data, dict) and all(
        isinstance(data.get(f), str) for f in ("sender", "role", "text")
    )


# Row kinds, each tagged so the test can build rows and the expected
# transcript from the same generated data.
chat_rows = st.tuples(
    st.just("chat"), raw_text, field_text, roles, field_text, st.booleans()
)  # (kind, ts, sender, role, text, b64-encode?)
presence_rows = st.tuples(st.just("presence"), raw_text, field_text, roles)
malformed_rows = st.tuples(
    st.just("malformed"),
    raw_text,
    raw_text.filter(
        lambda p: not _is_valid_chat_payload(p) and not p.startswith("b64:")
    ),
)
foreign_rows = st.tuples(
    st.just("foreign"), raw_text, raw_text.filter(lambda t: t != "room/chat")
)

row_lists = st.lists(
    st.one_of(chat_rows, presence_rows, malformed_rows, foreign_rows), max_size=20
)


# Feature: agent-room, Property 10: For any list of recorded CSV rows with mixed topics and shuffled timestamps, the PRD_Generator's reconstruction SHALL contain exactly the room/chat messages, ordered by timestamp, with any payload bearing the b64: Auto_Encode_Marker base64-decoded to its original text (encode-then-reconstruct round-trips).
# **Validates: Requirements 6.1, 6.2, 6.3**
@settings(max_examples=MAX_EXAMPLES, deadline=None)
@given(entries=row_lists)
def test_property_10_transcript_reconstruction(entries):
    rows = [["timestamp", "topic", "payload", "qos", "retain"]]
    chat_msgs = []  # (ts, sender, role, text) in file order

    for entry in entries:
        kind = entry[0]
        if kind == "chat":
            _, ts, sender, role, text, use_b64 = entry
            payload = json.dumps(
                {"sender": sender, "role": role, "text": text, "ts": ts}
            )
            if use_b64:
                payload = "b64:" + base64.b64encode(
                    payload.encode("utf-8")
                ).decode("ascii")
            rows.append([ts, "room/chat", payload, "1", "false"])
            chat_msgs.append((ts, sender, role, text))
        elif kind == "presence":
            _, ts, sender, role = entry
            payload = json.dumps(
                {"sender": sender, "role": role, "event": "join", "ts": ts}
            )
            rows.append([ts, "room/presence", payload, "1", "false"])
        elif kind == "malformed":
            _, ts, payload = entry
            rows.append([ts, "room/chat", payload, "1", "false"])
        else:  # foreign topic carrying an otherwise-valid chat payload
            _, ts, topic = entry
            payload = json.dumps(
                {"sender": "x", "role": "human", "text": "foreign", "ts": ts}
            )
            rows.append([ts, topic, payload, "1", "false"])

    # Expected transcript, computed independently: valid chat messages only,
    # stable-sorted by timestamp (identical to prd's list.sort stability),
    # rendered exactly as prd renders them.
    ordered = sorted(chat_msgs, key=lambda m: m[0])
    expected = "\n".join(f"{s} ({r}): {t}" for _, s, r, t in ordered)

    fd, path = tempfile.mkstemp(suffix=".csv", prefix="prd-props-")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
        assert reconstruct_transcript(path) == expected
    finally:
        os.unlink(path)
