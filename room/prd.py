"""PRD generator for the Agent Room.

Reads a Session_CSV produced by the mqtt-recorder flight recorder,
reconstructs the `room/chat` transcript, and invokes the room-prd-writer
Kiro persona to produce a Markdown PRD (Req 6.1-6.7).

The two stages are importable pure-ish functions so they can be tested
without a broker or an LLM:

- `reconstruct_transcript(csv_path)` -> transcript string (Req 6.1-6.3)
- `generate_prd(transcript, ...)` -> PRD Markdown text (Req 6.4, 6.5, 6.7)

Output is atomic: the PRD is buffered in memory and written via a temp
file + `os.replace` only on success, so a partial `PRD.md` is impossible
(Req 6.7).
"""

from __future__ import annotations

import argparse
import base64
import binascii
import csv
import json
import os
import subprocess
import sys
import tempfile

try:
    from room.common import clean_output
except ImportError:  # running as `python3 room/prd.py`
    from common import clean_output

# Topic carrying chat messages (Req 6.2).
CHAT_TOPIC = "room/chat"

# Prefix marking payloads the recorder auto-base64-encoded (Req 6.3).
AUTO_ENCODE_MARKER = "b64:"

# Fields a payload must carry to appear in the transcript.
REQUIRED_FIELDS = ("sender", "role", "text")

# Fixed instruction prepended to the transcript for the PRD writer (Req 6.4, 6.5).
PRD_INSTRUCTION = (
    "Write a PRD in Markdown with exactly these sections: Problem, Goals, "
    "Non-Goals, Requirements, Open Questions, based on this team chat "
    "transcript:"
)

DEFAULT_OUTPUT = "PRD.md"
DEFAULT_TIMEOUT = 300
DEFAULT_KIRO_CMD = "kiro-cli"


def _decode_payload(payload: str) -> dict | None:
    """Decode one recorded payload into a Room_Message dict, tolerantly.

    Strips the Auto_Encode_Marker and base64-decodes when present
    (Req 6.3). Returns None for anything that is not a JSON object with
    string `sender`, `role`, and `text` fields — presence events and
    foreign traffic must never crash the generator.
    """
    try:
        if payload.startswith(AUTO_ENCODE_MARKER):
            payload = base64.b64decode(
                payload[len(AUTO_ENCODE_MARKER):], validate=True
            ).decode("utf-8")
        data = json.loads(payload)
    except (ValueError, binascii.Error, UnicodeDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    for fname in REQUIRED_FIELDS:
        if not isinstance(data.get(fname), str):
            return None
    return data


def reconstruct_transcript(csv_path: str) -> str:
    """Rebuild the chat transcript from a Session_CSV (Req 6.1-6.3).

    Reads the recorded format (timestamp, topic, payload, qos, retain).
    The real recorder always writes a header row (see CsvWriter::new in
    src/csv_handler/writer.rs), but headerless files are tolerated: the
    first row is skipped only if its first column is literally
    "timestamp". Keeps only valid `room/chat` messages, sorted by the
    CSV timestamp column, rendered as `sender (role): text` lines.

    Raises OSError if the file is missing or unreadable. Returns an
    empty string when no valid chat messages are found.
    """
    entries: list[tuple[str, str]] = []  # (timestamp, rendered line)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if len(row) < 3:
                continue
            if i == 0 and row[0] == "timestamp":
                continue  # header row written by the recorder
            timestamp, topic, payload = row[0], row[1], row[2]
            if topic != CHAT_TOPIC:
                continue
            msg = _decode_payload(payload)
            if msg is None:
                continue
            line = f"{msg['sender']} ({msg['role']}): {msg['text']}"
            entries.append((timestamp, line))
    entries.sort(key=lambda e: e[0])  # ISO 8601 sorts lexicographically (Req 6.2)
    return "\n".join(line for _, line in entries)


def generate_prd(
    transcript: str,
    kiro_cmd: str = DEFAULT_KIRO_CMD,
    timeout: int = DEFAULT_TIMEOUT,
) -> str:
    """Generate PRD Markdown from a transcript via kiro-cli (Req 6.4).

    Invokes the room-prd-writer persona non-interactively, captures
    stdout, and returns it cleaned via `room.common.clean_output` (escape
    sequences — including the OSC "Response complete" marker — stripped,
    leading "> " prompt echo removed, whitespace trimmed). Raises
    RuntimeError on non-zero exit, timeout, or empty output — callers
    must not write any file in those cases (Req 6.7).
    """
    prompt = f"{PRD_INSTRUCTION}\n\n{transcript}"
    argv = [kiro_cmd, "chat", "--no-interactive", "--agent", "room-prd-writer", prompt]
    try:
        result = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"kiro-cli timed out after {timeout}s")
    except OSError as exc:
        raise RuntimeError(f"failed to run {kiro_cmd}: {exc}")
    if result.returncode != 0:
        raise RuntimeError(
            f"kiro-cli exited with code {result.returncode}: "
            f"{result.stderr.strip()}"
        )
    text = clean_output(result.stdout)
    if not text:
        raise RuntimeError("kiro-cli produced no output")
    return text


def _write_atomic(path: str, text: str) -> None:
    """Write text to path atomically via a temp file in the same directory."""
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".prd-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: csv_path -> PRD.md, with strict error exits."""
    parser = argparse.ArgumentParser(
        description="Generate a PRD from a recorded Agent Room session CSV."
    )
    parser.add_argument("csv_path", help="path to the recorded session CSV")
    parser.add_argument(
        "--output", default=DEFAULT_OUTPUT, help="output file (default: PRD.md)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"kiro-cli timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--kiro-cmd",
        default=DEFAULT_KIRO_CMD,
        help="kiro-cli executable to invoke (default: kiro-cli)",
    )
    args = parser.parse_args(argv)

    try:
        transcript = reconstruct_transcript(args.csv_path)
    except OSError as exc:
        print(f"error: cannot read {args.csv_path}: {exc}", file=sys.stderr)
        return 1

    if not transcript:
        # Req 6.6: non-zero exit AND a descriptive error, no output file.
        print(
            f"error: no room/chat messages found in {args.csv_path}",
            file=sys.stderr,
        )
        return 1

    try:
        prd_text = generate_prd(transcript, kiro_cmd=args.kiro_cmd, timeout=args.timeout)
    except RuntimeError as exc:
        print(f"error: PRD generation failed: {exc}", file=sys.stderr)
        return 1

    _write_atomic(args.output, prd_text)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
