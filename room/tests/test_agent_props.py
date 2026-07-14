"""Property-based tests for the Agent Room agent client.

Covers Properties 5-9 from the agent-room design document, exercising
`should_reply`, `build_prompt`, and the `Agent` core from `room/agent.py`
with hypothesis (`max_examples=100`). Property 8 drives an `Agent` with a
cheap echo stub for kiro-cli (absolute path, no PATH manipulation, no LLM
cost), draining the pending queue serially exactly as the worker does.
"""

import tempfile
from collections import deque
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from room.agent import TRIGGER_FLAG, Agent, build_prompt, should_reply
from room.common import Message

MAX_EXAMPLES = 100
TS = "2026-07-14T21:04:05.123Z"

# Generators: no surrogates (not JSON-encodable); prompt tests additionally
# need newline-free senders/texts because prompts are compared line-by-line.
field_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), max_size=200
)
line_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    max_size=100,
)
personas = st.text(
    alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
    min_size=1,
    max_size=20,
).map(lambda s: f"room-{s}")
any_roles = st.sampled_from(["human", "agent", "system", ""])

# (max_replies, replies_sent) pairs with replies_sent strictly below the limit.
below_limit = st.integers(min_value=1, max_value=20).flatmap(
    lambda m: st.tuples(st.just(m), st.integers(min_value=0, max_value=m - 1))
)


@pytest.fixture(scope="module")
def echo_stub():
    """A kiro-cli stub that instantly echoes a fixed reply (module-scoped)."""
    d = tempfile.mkdtemp(prefix="agent-props-")
    stub = Path(d) / "kiro-stub"
    stub.write_text("#!/bin/sh\necho reply\n")
    stub.chmod(0o755)
    return str(stub)


# Feature: agent-room, Property 5: For any Room_Message whose sender equals the Agent_Client's own Persona name, the reply-policy function SHALL decide "ignore", regardless of role, Mentions, or reply count.
# **Validates: Requirements 3.5**
@settings(max_examples=MAX_EXAMPLES)
@given(
    own=personas,
    role=any_roles,
    text=field_text,
    add_mention=st.booleans(),
    replies_sent=st.integers(min_value=0, max_value=20),
    max_replies=st.integers(min_value=0, max_value=20),
)
def test_property_5_never_self_reply(own, role, text, add_mention, replies_sent, max_replies):
    if add_mention:
        text = f"{text} @{own}"
    msg = Message(sender=own, role=role, text=text, ts=TS)
    assert should_reply(msg, own, replies_sent, max_replies) is False


# Feature: agent-room, Property 6: For any Room_Message with role == "human" from another sender, while the Agent_Client is below its Reply_Limit, the reply-policy function SHALL decide "reply".
# **Validates: Requirements 3.2**
@settings(max_examples=MAX_EXAMPLES)
@given(own=personas, sender=field_text, text=field_text, limits=below_limit)
def test_property_6_human_messages_always_trigger(own, sender, text, limits):
    max_replies, replies_sent = limits
    if sender == own:
        sender = sender + "-other"
    msg = Message(sender=sender, role="human", text=text, ts=TS)
    assert should_reply(msg, own, replies_sent, max_replies) is True


# Feature: agent-room, Property 7: For any Room_Message with role == "agent" from another sender, while below the Reply_Limit, the reply-policy function SHALL decide "reply" if and only if the message text contains @<own-persona-name>.
# **Validates: Requirements 3.3, 3.4**
@settings(max_examples=MAX_EXAMPLES)
@given(
    own=personas,
    sender=field_text,
    text=field_text,
    add_mention=st.booleans(),
    limits=below_limit,
)
def test_property_7_agent_reply_iff_mentioned(own, sender, text, add_mention, limits):
    max_replies, replies_sent = limits
    if sender == own:
        sender = sender + "-other"
    if add_mention:
        text = f"{text} @{own}"
    msg = Message(sender=sender, role="agent", text=text, ts=TS)
    expected = f"@{own}" in text  # the iff, computed independently
    assert should_reply(msg, own, replies_sent, max_replies) is expected


# Feature: agent-room, Property 8: For any sequence of triggering Room_Messages, once the number of replies sent reaches the Reply_Limit, the reply-policy function SHALL never again decide "reply" for the remainder of the session, and the limit announcement SHALL be published exactly once.
# **Validates: Requirements 3.7**
@settings(max_examples=MAX_EXAMPLES, deadline=None)
@given(max_replies=st.integers(min_value=1, max_value=3), n=st.integers(min_value=0, max_value=6))
def test_property_8_reply_limit_monotonic_announced_once(echo_stub, max_replies, n):
    sent = []
    agent = Agent(
        persona="pm",
        publish=sent.append,
        kiro_cmd=echo_stub,
        max_replies=max_replies,
        timeout=10,
    )

    def drain():
        while not agent.pending.empty():
            agent.process(agent.pending.get())

    for i in range(n):
        agent.on_message(Message(sender="brian", role="human", text=f"msg {i}", ts=TS))
    drain()

    # Feed more triggers after the first batch: once the limit is reached,
    # no further replies may ever be sent (monotonic).
    extra = 3
    for i in range(extra):
        agent.on_message(Message(sender="brian", role="human", text=f"late {i}", ts=TS))
    drain()

    total = n + extra
    replies = [m for m in sent if m.text == "reply"]
    announcements = [m for m in sent if "reached its reply limit" in m.text]
    assert len(replies) == min(total, max_replies)
    assert agent.replies_sent <= max_replies
    assert len(announcements) == (1 if total >= max_replies else 0)


# Feature: agent-room, Property 9: For any transcript and configured history size N, the prompt built for kiro-cli SHALL contain at most the N most recent Room_Messages, in their original order.
# **Validates: Requirements 3.6**
@settings(max_examples=MAX_EXAMPLES)
@given(
    entries=st.lists(st.tuples(line_text, line_text), max_size=15),
    n=st.integers(min_value=1, max_value=8),
)
def test_property_9_history_window_bounded_and_ordered(entries, n):
    # Unique per-message marker so evicted messages cannot alias window ones.
    msgs = [
        Message(sender=sender, role="human", text=f"{text} #{i}", ts=TS)
        for i, (sender, text) in enumerate(entries)
    ]
    window = deque(maxlen=n)  # built exactly as the Agent builds its history
    for m in msgs:
        window.append(m)
    trigger = msgs[-1] if msgs else Message(sender="brian", role="human", text="go", ts=TS)

    prompt = build_prompt("room-pm", list(window), trigger)
    # Split on "\n" exactly as build_prompt joins; splitlines() would also
    # split on exotic line boundaries (e.g. U+001E) inside message text.
    lines = prompt.split("\n")
    assert lines[2] == "Recent conversation (oldest first):"
    body = lines[3:]

    stripped = [line.removesuffix(TRIGGER_FLAG) for line in body]
    expected = [f"{m.sender}: {m.text}" for m in msgs[-n:]]
    if not msgs:
        # Empty transcript: only the appended trigger line is present.
        expected = [f"{trigger.sender}: {trigger.text}"]
    assert stripped == expected
    assert len(body) <= n + 1  # at most N history lines (+ appended trigger)
    assert sum(1 for line in body if line.endswith(TRIGGER_FLAG)) == 1
