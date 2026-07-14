"""Human REPL client for the Agent Room.

Connects to the Room_Broker, announces presence, and runs a terminal
input loop: each entered line is published to `room/chat` as a
Room_Message with role "human" at QoS 1 (Req 2.1, 2.2). Incoming
messages print as `[sender] text`, suppressed only when their id is in
this session's published-id set (Req 2.3, 2.4) — so replayed sessions
watched from a fresh client display everything (Req 7.2, 7.3).

Usage: python3 room/human.py [--name NAME] [--host HOST] [--port PORT]
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
import time
from datetime import datetime, timezone

if __package__ in (None, ""):  # allow `python3 room/human.py` from repo root
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from room.common import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    Message,
    RoomClient,
    render_line,
    should_display,
)

PROMPT = "> "


def now_ts() -> str:
    """Return the current UTC time as an ISO 8601 string (e.g. ...Z)."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_args(argv=None) -> argparse.Namespace:
    """Parse CLI arguments; defaults come from $USER and room constants."""
    parser = argparse.ArgumentParser(description="Agent Room human REPL client")
    parser.add_argument(
        "--name",
        default=os.environ.get("USER") or getpass.getuser(),
        help="participant name (default: $USER)",
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="broker host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="broker port")
    return parser.parse_args(argv)


def make_on_message(published_ids: set[str]):
    """Build the incoming-message callback bound to this session's id set.

    Prints with a leading carriage return so agent replies do not mangle
    the input prompt, then reprints the prompt.
    """

    def on_message(msg: Message) -> None:
        if should_display(msg, published_ids):
            print("\r" + render_line(msg))
            print(PROMPT, end="", flush=True)

    return on_message


def main(argv=None) -> int:
    """Run the REPL: join, chat until /quit or EOF, then leave cleanly."""
    args = parse_args(argv)
    published_ids: set[str] = set()
    client = RoomClient(
        client_id=f"human-{args.name}",
        on_message=make_on_message(published_ids),
        host=args.host,
        port=args.port,
    )
    client.connect()
    client.publish_presence(args.name, "human", "join", now_ts())
    print(f"joined the room as {args.name} — type /quit to leave")
    try:
        while True:
            line = input(PROMPT)
            if line.strip() == "/quit":
                break
            if not line.strip():
                continue
            msg = Message(sender=args.name, role="human", text=line, ts=now_ts())
            published_ids.add(msg.id)  # add BEFORE publishing (Req 2.4)
            client.publish_chat(msg)
    except (EOFError, KeyboardInterrupt):
        print()
    finally:
        client.publish_presence(args.name, "human", "leave", now_ts())
        time.sleep(0.2)  # let the QoS 1 leave event flush before disconnect
        client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
