"""Property-based tests for the Agent Room shared plumbing.

Covers Properties 1-4 from the agent-room design document, exercising the
pure functions in `room/common.py` with hypothesis (`max_examples=100`).
"""

import json

from hypothesis import given, settings
from hypothesis import strategies as st

from room.common import (
    Message,
    decode_message,
    encode_message,
    encode_presence,
    render_line,
    should_display,
)

MAX_EXAMPLES = 100

# Generators: message field text avoids surrogates (not JSON-encodable byte-safely).
field_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), max_size=200
)
roles = st.sampled_from(["human", "agent"])
events = st.sampled_from(["join", "leave"])
uuids = st.uuids(version=4).map(str)

messages = st.builds(
    Message,
    sender=field_text,
    role=roles,
    text=field_text,
    ts=field_text,
    id=uuids,
)


# Feature: agent-room, Property 1: For any valid Room_Message or Presence_Event value, encoding to JSON and decoding back SHALL produce an equivalent value, and the encoded JSON SHALL contain every required field for its type.
# **Validates: Requirements 1.5, 1.6**
@settings(max_examples=MAX_EXAMPLES)
@given(msg=messages)
def test_property_1_message_round_trip(msg):
    encoded = encode_message(msg)
    data = json.loads(encoded)
    for fname in ("sender", "role", "text", "ts"):
        assert fname in data
    decoded = decode_message(encoded)
    assert decoded == msg


# Feature: agent-room, Property 1: For any valid Room_Message or Presence_Event value, encoding to JSON and decoding back SHALL produce an equivalent value, and the encoded JSON SHALL contain every required field for its type.
# **Validates: Requirements 1.5, 1.6**
@settings(max_examples=MAX_EXAMPLES)
@given(sender=field_text, role=roles, event=events, ts=field_text)
def test_property_1_presence_encoding(sender, role, event, ts):
    encoded = encode_presence(sender, role, event, ts)
    data = json.loads(encoded)
    assert data == {"sender": sender, "role": role, "event": event, "ts": ts}


# Feature: agent-room, Property 2: For any payload that is not valid JSON, or that is valid JSON missing at least one required Room_Message field, the decoder SHALL reject the payload (returning no message) without raising an unhandled exception.
# **Validates: Requirements 1.9**
@settings(max_examples=MAX_EXAMPLES)
@given(payload=st.one_of(st.text(max_size=200), st.binary(max_size=200)))
def test_property_2_invalid_json_rejected(payload):
    try:
        data = json.loads(payload)
        is_complete_message = isinstance(data, dict) and all(
            isinstance(data.get(f), str) for f in ("sender", "role", "text", "ts")
        )
    except Exception:
        is_complete_message = False
    result = decode_message(payload)
    if is_complete_message:
        assert result is not None
    else:
        assert result is None


# Feature: agent-room, Property 2: For any payload that is not valid JSON, or that is valid JSON missing at least one required Room_Message field, the decoder SHALL reject the payload (returning no message) without raising an unhandled exception.
# **Validates: Requirements 1.9**
@settings(max_examples=MAX_EXAMPLES)
@given(
    msg=messages,
    missing=st.sampled_from(["sender", "role", "text", "ts"]),
)
def test_property_2_missing_required_field_rejected(msg, missing):
    data = json.loads(encode_message(msg))
    del data[missing]
    assert decode_message(json.dumps(data)) is None


# Feature: agent-room, Property 3: For any set of published message ids S and any incoming Room_Message m, the Human_Client SHALL suppress m if and only if m.id is in S. In particular, when S is empty (a fresh session watching a replay), every message SHALL display — including messages whose sender equals the viewing human's name — and a message with a matching sender but an id not in S SHALL display.
# **Validates: Requirements 2.4, 7.2, 7.3**
@settings(max_examples=MAX_EXAMPLES)
@given(
    msg=messages,
    other_ids=st.sets(uuids, max_size=10),
    include_own=st.booleans(),
)
def test_property_3_session_based_self_suppression(msg, other_ids, include_own):
    published_ids = set(other_ids)
    published_ids.discard(msg.id)
    if include_own:
        published_ids.add(msg.id)
    # Suppress iff msg.id is in the set.
    assert should_display(msg, published_ids) == (msg.id not in published_ids)

    # Replay case: an empty set displays everything, including a message
    # whose sender equals the viewing human's own name.
    assert should_display(msg, set()) is True

    # Matching sender but unknown id displays: only ids matter, not names.
    ids_without_own = set(other_ids)
    ids_without_own.discard(msg.id)
    assert should_display(msg, ids_without_own) is True


# Feature: agent-room, Property 4: For any Room_Message, the rendered display line SHALL contain both the sender's name and the message text.
# **Validates: Requirements 2.3**
@settings(max_examples=MAX_EXAMPLES)
@given(msg=messages)
def test_property_4_rendered_lines_carry_sender_and_text(msg):
    line = render_line(msg)
    assert msg.sender in line
    assert msg.text in line
