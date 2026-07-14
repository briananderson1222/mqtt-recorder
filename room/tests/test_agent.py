"""Example tests for the agent wiring with a stubbed kiro-cli.

The stub is a shell script created in tmp_path and passed to the Agent by
absolute path via the kiro_cmd parameter (mirroring --kiro-cmd) — no PATH
manipulation and no LLM cost. These tests run WITHOUT a broker: the Agent
core takes an injected publish callable, and the tests drain the pending
queue serially exactly as the worker thread does.

Covers task 4.10: _Requirements: 3.2, 3.4, 3.8_
"""

import pytest

from room.agent import (
    Agent,
    KiroInvocationError,
    build_prompt,
    invoke_kiro,
    should_reply,
    strip_ansi,
)
from room.common import Message, clean_output

TS = "2026-07-14T21:04:05.123Z"


def make_msg(sender: str, role: str, text: str) -> Message:
    return Message(sender=sender, role=role, text=text, ts=TS)


def make_stub(tmp_path, body: str) -> str:
    """Create an executable shell-script stub for kiro-cli in tmp_path."""
    stub = tmp_path / "kiro-stub"
    stub.write_text("#!/bin/sh\n" + body)
    stub.chmod(0o755)
    return str(stub)


def drain(agent: Agent) -> None:
    """Consume the pending queue serially, as the worker thread does."""
    while not agent.pending.empty():
        agent.process(agent.pending.get())


class TestShouldReply:
    """Unit examples for the pure reply-policy function."""

    def test_never_self_reply(self):
        msg = make_msg("room-pm", "human", "@room-pm hello")
        assert should_reply(msg, "room-pm", 0, 6) is False

    def test_human_message_triggers(self):
        assert should_reply(make_msg("brian", "human", "hi"), "room-pm", 0, 6) is True

    def test_agent_message_with_mention_triggers(self):
        msg = make_msg("room-architect", "agent", "what do you think @room-pm?")
        assert should_reply(msg, "room-pm", 0, 6) is True

    def test_agent_message_without_mention_ignored(self):
        msg = make_msg("room-architect", "agent", "just musing aloud")
        assert should_reply(msg, "room-pm", 0, 6) is False

    def test_reply_limit_blocks(self):
        assert should_reply(make_msg("brian", "human", "hi"), "room-pm", 6, 6) is False

    def test_unknown_role_ignored(self):
        assert should_reply(make_msg("x", "system", "hi"), "room-pm", 0, 6) is False


class TestBuildPrompt:
    def test_prompt_contains_history_and_flags_trigger(self):
        m1 = make_msg("brian", "human", "first")
        m2 = make_msg("room-architect", "agent", "second")
        prompt = build_prompt("room-pm", [m1, m2], m2)
        assert "You are room-pm in a team chat room." in prompt
        assert prompt.index("brian: first") < prompt.index("room-architect: second")
        assert "respond to this message" in prompt.splitlines()[-1]

    def test_evicted_trigger_appended(self):
        history = [make_msg("brian", "human", "recent")]
        trigger = make_msg("brian", "human", "older, already evicted")
        prompt = build_prompt("room-pm", history, trigger)
        assert "older, already evicted" in prompt
        assert "respond to this message" in prompt.splitlines()[-1]


def test_wiring_exactly_one_invocation(tmp_path):
    """Human message + non-mentioning agent message → exactly one kiro-cli call."""
    counter = tmp_path / "calls"
    stub = make_stub(tmp_path, f'echo call >> "{counter}"\necho "stub reply"\n')
    sent = []
    agent = Agent(persona="pm", publish=sent.append, kiro_cmd=stub, timeout=10)

    agent.on_message(make_msg("brian", "human", "let's spec the idea"))
    agent.on_message(make_msg("room-architect", "agent", "no mention of anyone"))
    drain(agent)

    assert counter.read_text().count("call") == 1
    assert len(sent) == 1
    assert sent[0].text == "stub reply"
    assert sent[0].sender == "room-pm"
    assert sent[0].role == "agent"
    assert agent.replies_sent == 1


def test_failing_stub_publishes_error_notice_and_continues(tmp_path):
    """Non-zero exit → error notice published, agent keeps running (Req 3.8)."""
    failing = make_stub(tmp_path, "exit 1\n")
    sent = []
    agent = Agent(persona="pm", publish=sent.append, kiro_cmd=failing, timeout=10)

    agent.on_message(make_msg("brian", "human", "hello?"))
    drain(agent)

    assert len(sent) == 1
    assert "hit an error" in sent[0].text
    assert agent.replies_sent == 0  # error notices don't count toward the limit

    # Agent keeps running: a later trigger with a healthy stub still replies.
    agent.kiro_cmd = make_stub(tmp_path, "echo recovered\n")
    agent.on_message(make_msg("brian", "human", "still there?"))
    drain(agent)
    assert sent[-1].text == "recovered"
    assert agent.replies_sent == 1


def test_sleeping_stub_times_out_and_continues(tmp_path):
    """Stub exceeding the timeout → handled as failure, agent continues (Req 3.8)."""
    sleeper = make_stub(tmp_path, "sleep 5\necho too late\n")
    sent = []
    agent = Agent(persona="pm", publish=sent.append, kiro_cmd=sleeper, timeout=1)

    agent.on_message(make_msg("brian", "human", "quick question"))
    drain(agent)

    assert len(sent) == 1
    assert "hit an error" in sent[0].text
    assert agent.replies_sent == 0


def test_own_echo_is_ignored(tmp_path):
    """Broker echo of the agent's own publish is never processed (belt: id check)."""
    stub = make_stub(tmp_path, "echo reply\n")
    sent = []
    agent = Agent(persona="pm", publish=sent.append, kiro_cmd=stub, timeout=10)

    agent.on_message(make_msg("brian", "human", "hi"))
    drain(agent)
    assert len(sent) == 1

    agent.on_message(sent[0])  # broker echoes our own reply back
    assert agent.pending.empty()


def test_limit_announcement_published_exactly_once(tmp_path):
    """Reaching max-replies announces once; further triggers are ignored (Req 3.7)."""
    stub = make_stub(tmp_path, "echo reply\n")
    sent = []
    agent = Agent(persona="pm", publish=sent.append, kiro_cmd=stub,
                  max_replies=1, timeout=10)

    agent.on_message(make_msg("brian", "human", "one"))
    agent.on_message(make_msg("brian", "human", "two"))
    agent.on_message(make_msg("brian", "human", "three"))
    drain(agent)

    announcements = [m for m in sent if "reached its reply limit" in m.text]
    replies = [m for m in sent if m.text == "reply"]
    assert len(replies) == 1
    assert len(announcements) == 1
    assert agent.limit_announced is True


class TestInvokeKiro:
    def test_empty_output_is_failure(self, tmp_path):
        stub = make_stub(tmp_path, "printf '\\033[2J   \\n'\n")  # ANSI + whitespace only
        with pytest.raises(KiroInvocationError, match="empty"):
            invoke_kiro(stub, "room-pm", "prompt", timeout=10)

    def test_ansi_stripped_from_reply(self, tmp_path):
        stub = make_stub(tmp_path, "printf '\\033[1;32mgreen reply\\033[0m\\n'\n")
        assert invoke_kiro(stub, "room-pm", "prompt", timeout=10) == "green reply"

    def test_osc_response_complete_stripped(self, tmp_path):
        """Regression: OSC `ESC ] 9;Response complete BEL` must not leak.

        The old regex treated ESC ] as a two-char escape, letting the
        payload "9;Response complete" leak into published replies.
        """
        stub = make_stub(
            tmp_path, "printf 'reply text\\033]9;Response complete\\007\\n'\n"
        )
        assert invoke_kiro(stub, "room-pm", "prompt", timeout=10) == "reply text"

    def test_leading_prompt_prefix_removed(self, tmp_path):
        """Regression: kiro-cli's leading "> " prompt echo is stripped."""
        stub = make_stub(tmp_path, "printf '> prefixed reply\\n'\n")
        assert invoke_kiro(stub, "room-pm", "prompt", timeout=10) == "prefixed reply"

    def test_unterminated_osc_at_end_stripped(self, tmp_path):
        """Regression: an OSC left unterminated at EOF is still stripped."""
        stub = make_stub(tmp_path, "printf 'reply\\033]9;Response complete'\n")
        assert invoke_kiro(stub, "room-pm", "prompt", timeout=10) == "reply"


class TestCleanOutput:
    def test_real_kiro_output_shape(self):
        """The exact escape shapes observed live via `cat -v`."""
        raw = "\x1b[38;5;141m> \x1b[0mhello \x1b]9;Response complete\x07world\n"
        assert clean_output(raw) == "hello world"

    def test_prefix_removed_from_first_line_only(self):
        # "> " on later lines is Markdown blockquote content — preserved.
        raw = "> first line\n> quoted line\nplain line\n"
        assert clean_output(raw) == "first line\n> quoted line\nplain line"

    def test_stray_bel_removed(self):
        assert clean_output("ding\x07dong") == "dingdong"

    def test_osc_with_st_terminator(self):
        assert clean_output("a\x1b]9;Response complete\x1b\\b") == "ab"


def test_strip_ansi():
    assert strip_ansi("\x1b[1;32mhello\x1b[0m world") == "hello world"
    assert strip_ansi("plain") == "plain"
    # OSC sequences (BEL- and ST-terminated) are stripped whole, payload included.
    assert strip_ansi("a\x1b]9;Response complete\x07b") == "ab"
    assert strip_ansi("a\x1b]0;title\x1b\\b") == "ab"
