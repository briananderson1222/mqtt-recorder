"""Fixture-based example tests for the PRD pipeline (room/prd.py).

Covers Req 6.1, 6.4, 6.6, 6.7: transcript reconstruction from a recorded
CSV (out-of-order timestamps, b64-encoded payloads, malformed rows,
foreign topics), atomic PRD.md output via a stubbed kiro-cli, and the
error exit paths (empty transcript, failing generator).

kiro-cli is stubbed with a shell script — no LLM cost.
"""

from __future__ import annotations

import base64
import csv
import json
import os
import stat
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from room.prd import main, reconstruct_transcript  # noqa: E402

FAKE_PRD = """# PRD
## Problem
p
## Goals
g
## Non-Goals
n
## Requirements
r
## Open Questions
q"""


def _chat_payload(sender: str, role: str, text: str, ts: str) -> str:
    return json.dumps(
        {"id": f"id-{sender}-{ts}", "sender": sender, "role": role, "text": text, "ts": ts}
    )


def write_fixture_csv(path):
    """Fixture: 2 out-of-order chat rows, 1 presence, 1 b64 chat, 1 malformed chat."""
    later = _chat_payload("brian", "human", "second message", "2026-07-14T21:05:00.000Z")
    earlier = _chat_payload("room-pm", "agent", "first message", "2026-07-14T21:04:00.000Z")
    b64_inner = _chat_payload("room-architect", "agent", "encoded message", "2026-07-14T21:06:00.000Z")
    b64_payload = "b64:" + base64.b64encode(b64_inner.encode("utf-8")).decode("ascii")
    rows = [
        ["timestamp", "topic", "payload", "qos", "retain"],
        ["2026-07-14T21:05:00.000Z", "room/chat", later, "1", "false"],
        ["2026-07-14T21:03:58.000Z", "room/presence",
         '{"sender":"room-pm","role":"agent","event":"join","ts":"2026-07-14T21:03:58.000Z"}',
         "1", "false"],
        ["2026-07-14T21:04:00.000Z", "room/chat", earlier, "1", "false"],
        ["2026-07-14T21:06:00.000Z", "room/chat", b64_payload, "1", "false"],
        ["2026-07-14T21:07:00.000Z", "room/chat", "{not valid json", "1", "false"],
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)


def make_stub(tmp_path, argv_file, exit_code=0):
    """Echo-stub kiro-cli: records argv to a file, prints a fake PRD."""
    stub = tmp_path / "kiro-stub"
    lines = ["#!/bin/sh", f'printf \'%s\\n\' "$@" > "{argv_file}"']
    if exit_code == 0:
        lines.append(f"cat <<'EOF'\n{FAKE_PRD}\nEOF")
    else:
        lines.append(f"exit {exit_code}")
    stub.write_text("\n".join(lines) + "\n")
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return str(stub)


def test_pipeline_success(tmp_path):
    """Valid fixture → correct transcript passed to stub, PRD.md written."""
    csv_path = tmp_path / "session.csv"
    write_fixture_csv(csv_path)
    argv_file = tmp_path / "argv.txt"
    stub = make_stub(tmp_path, argv_file)
    output = tmp_path / "PRD.md"

    rc = main([str(csv_path), "--output", str(output), "--kiro-cmd", stub])

    assert rc == 0
    argv_lines = argv_file.read_text().splitlines()
    assert argv_lines[:4] == ["chat", "--no-interactive", "--agent", "room-prd-writer"]
    prompt = "\n".join(argv_lines[4:])
    # All three valid messages present, in timestamp order, b64 one decoded.
    lines = [
        "room-pm (agent): first message",
        "brian (human): second message",
        "room-architect (agent): encoded message",
    ]
    positions = [prompt.find(line) for line in lines]
    assert all(p >= 0 for p in positions), prompt
    assert positions == sorted(positions)
    # Malformed row skipped, presence row excluded.
    assert "not valid json" not in prompt
    assert "join" not in prompt
    # PRD written with the stub's content.
    prd = output.read_text()
    for heading in ("Problem", "Goals", "Non-Goals", "Requirements", "Open Questions"):
        assert heading in prd


def test_reconstruct_transcript_headerless(tmp_path):
    """A headerless CSV (first row is data) is parsed correctly too."""
    csv_path = tmp_path / "session.csv"
    payload = _chat_payload("brian", "human", "hello", "2026-07-14T21:04:00.000Z")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(
            ["2026-07-14T21:04:00.000Z", "room/chat", payload, "1", "false"]
        )
    assert reconstruct_transcript(str(csv_path)) == "brian (human): hello"


def test_empty_transcript_exits_nonzero(tmp_path, capsys):
    """No room/chat messages → exit 1, descriptive stderr, no PRD.md (Req 6.6)."""
    csv_path = tmp_path / "session.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "topic", "payload", "qos", "retain"])
        writer.writerow(
            ["2026-07-14T21:03:58.000Z", "room/presence", '{"event":"join"}', "1", "false"]
        )
    output = tmp_path / "PRD.md"

    rc = main([str(csv_path), "--output", str(output), "--kiro-cmd", "/nonexistent"])

    assert rc == 1
    assert "no room/chat messages found" in capsys.readouterr().err
    assert not output.exists()


def test_missing_csv_exits_nonzero(tmp_path, capsys):
    """Missing CSV → exit 1 with descriptive stderr, no PRD.md."""
    output = tmp_path / "PRD.md"
    rc = main([str(tmp_path / "nope.csv"), "--output", str(output)])
    assert rc == 1
    assert "cannot read" in capsys.readouterr().err
    assert not output.exists()


def test_failing_stub_no_prd(tmp_path, capsys):
    """kiro-cli exits non-zero → exit 1, no PRD.md on disk (Req 6.7)."""
    csv_path = tmp_path / "session.csv"
    write_fixture_csv(csv_path)
    argv_file = tmp_path / "argv.txt"
    stub = make_stub(tmp_path, argv_file, exit_code=1)
    output = tmp_path / "PRD.md"

    rc = main([str(csv_path), "--output", str(output), "--kiro-cmd", stub])

    assert rc == 1
    assert "PRD generation failed" in capsys.readouterr().err
    assert not output.exists()
