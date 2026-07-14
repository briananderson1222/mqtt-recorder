"""Kiro agent participant for the Agent Room.

Wraps `kiro-cli chat --no-interactive` behind a Persona and joins the room
as a peer MQTT client (Req 3.1). The decision logic (`should_reply`), prompt
building (`build_prompt`), and subprocess invocation (`invoke_kiro`) are
pure/isolated units so they can be tested without a broker or an LLM
(see `room/tests/test_agent.py`); all MQTT wiring lives in `main()`.

Loop-safety guards (see design doc "Loop & Cost Safety"):
1. agents reply to other agents only when mentioned (Req 3.3, 3.4);
2. a hard `--max-replies` cap per session (Req 3.7);
3. a serial worker queue — never more than one kiro-cli call in flight.
"""

from __future__ import annotations

import argparse
import logging
import queue
import signal
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone

try:
    from room.common import (
        DEFAULT_HOST,
        DEFAULT_PORT,
        Message,
        RoomClient,
        clean_output,
        strip_ansi,
    )
except ImportError:  # running as `python3 room/agent.py`
    from common import (
        DEFAULT_HOST,
        DEFAULT_PORT,
        Message,
        RoomClient,
        clean_output,
        strip_ansi,
    )

# Defaults for the CLI arguments (Req 3.6, 3.7, 3.8).
DEFAULT_MAX_REPLIES = 6
DEFAULT_HISTORY = 12
DEFAULT_TIMEOUT = 120
# The kiro-cli executable. Exposed as --kiro-cmd so tests can substitute a
# stub executable by absolute path — no PATH manipulation needed.
DEFAULT_KIRO_CMD = "kiro-cli"

# Standing instruction prepended to every prompt (Req 3.2, 3.6).
STANDING_INSTRUCTION = (
    "You are {name} in a team chat room. Reply with your next message only."
)
# Marker appended to the triggering message inside the prompt.
TRIGGER_FLAG = "  <-- respond to this message"

logger = logging.getLogger(__name__)


class KiroInvocationError(Exception):
    """kiro-cli failed: non-zero exit, timeout, or empty output (Req 3.8)."""


def should_reply(msg: Message, own_persona: str, replies_sent: int, max_replies: int) -> bool:
    """Pure reply-policy decision (Req 3.2–3.5, 3.7).

    `own_persona` is the full sender name as it appears on the wire,
    e.g. "room-pm". Rules, in order:
    - never reply to our own messages (Req 3.5);
    - never reply once the Reply_Limit is reached (Req 3.7);
    - always reply to human messages from another sender (Req 3.2);
    - reply to agent messages only when Mentioned via `@<own_persona>`
      (Req 3.3, 3.4);
    - any other role: ignore.
    """
    if msg.sender == own_persona:
        return False
    if replies_sent >= max_replies:
        return False
    if msg.role == "human":
        return True
    if msg.role == "agent":
        return ("@" + own_persona) in msg.text
    return False


def build_prompt(own_persona: str, history: list[Message], trigger: Message) -> str:
    """Build the kiro-cli prompt: standing instruction + bounded history (Req 3.6).

    History lines are rendered as `sender: text` in original order; the
    triggering message is flagged. If the trigger has already been evicted
    from the bounded history window, it is appended as the final line.
    """
    lines = [STANDING_INSTRUCTION.format(name=own_persona), "", "Recent conversation (oldest first):"]
    flagged = False
    for m in history:
        line = f"{m.sender}: {m.text}"
        if not flagged and m.id == trigger.id:
            line += TRIGGER_FLAG
            flagged = True
        lines.append(line)
    if not flagged:
        lines.append(f"{trigger.sender}: {trigger.text}{TRIGGER_FLAG}")
    return "\n".join(lines)


def invoke_kiro(kiro_cmd: str, agent_name: str, prompt: str, timeout: float) -> str:
    """Invoke kiro-cli once and return the cleaned reply text.

    Output is cleaned via `room.common.clean_output`: escape sequences
    (including the OSC "Response complete" marker) are stripped and the
    leading "> " prompt echo is removed. Raises KiroInvocationError on
    non-zero exit, timeout, or a reply that is empty after cleaning (all
    failure per Req 3.8).
    """
    argv = [kiro_cmd, "chat", "--no-interactive", "--agent", agent_name, prompt]
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise KiroInvocationError(f"kiro-cli timed out after {timeout}s") from exc
    if result.returncode != 0:
        raise KiroInvocationError(f"kiro-cli exited with status {result.returncode}")
    reply = clean_output(result.stdout)
    if not reply:
        raise KiroInvocationError("kiro-cli produced an empty reply")
    return reply


def _now_iso() -> str:
    """Current UTC time as an ISO 8601 string with a Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Agent:
    """Broker-independent agent core: history, reply queue, invocation.

    `publish` is a callable taking a Message — in production it is
    `RoomClient.publish_chat`; tests inject a list-appender. Incoming
    messages are fed to `on_message` (thread-safe: paho callback thread
    only appends/enqueues); a single worker consumes `pending` serially
    via `process`, so at most one kiro-cli call is ever in flight.
    """

    def __init__(
        self,
        persona: str,
        publish,
        kiro_cmd: str = DEFAULT_KIRO_CMD,
        max_replies: int = DEFAULT_MAX_REPLIES,
        history_size: int = DEFAULT_HISTORY,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        self.name = f"room-{persona}"
        self.kiro_cmd = kiro_cmd
        self.max_replies = max_replies
        self.timeout = timeout
        self._publish = publish
        self.history: deque[Message] = deque(maxlen=history_size)
        self.pending: queue.Queue[Message] = queue.Queue()
        self.published_ids: set[str] = set()
        self.replies_sent = 0
        self.limit_announced = False

    def on_message(self, msg: Message) -> None:
        """Handle a decoded incoming Room_Message.

        Belt: skip broker echoes of our own publishes by id (they were
        already added to history at publish time). Suspenders: even if the
        id check missed, `should_reply` rejects by sender (Req 3.5).
        """
        if msg.id in self.published_ids:
            return
        self.history.append(msg)
        if should_reply(msg, self.name, self.replies_sent, self.max_replies):
            self.pending.put(msg)

    def process(self, msg: Message) -> None:
        """Generate and publish one reply for a queued trigger (worker side)."""
        # Re-check the limit at dequeue time: earlier queue items may have
        # consumed the remaining budget since this trigger was enqueued.
        if self.replies_sent >= self.max_replies:
            return
        prompt = build_prompt(self.name, list(self.history), msg)
        try:
            reply = invoke_kiro(self.kiro_cmd, self.name, prompt, self.timeout)
        except KiroInvocationError as exc:
            logger.warning("kiro-cli invocation failed: %s", exc)
            # Error notices do NOT count toward max-replies and cannot
            # recurse: they are role "agent" with no Mention, so other
            # agents ignore them per the reply policy (Req 3.3, 3.4, 3.8).
            self._send(f"\u26a0 {self.name} hit an error, continuing\u2026")
            return
        self._send(reply)
        self.replies_sent += 1
        if self.replies_sent >= self.max_replies and not self.limit_announced:
            # Announce the Reply_Limit exactly once (Req 3.7).
            self.limit_announced = True
            self._send(f"{self.name} has reached its reply limit for this session")

    def _send(self, text: str) -> None:
        """Publish a Room_Message as this persona, tracking its id."""
        msg = Message(sender=self.name, role="agent", text=text, ts=_now_iso())
        self.published_ids.add(msg.id)
        self.history.append(msg)
        self._publish(msg)


def parse_args(argv=None) -> argparse.Namespace:
    """Parse CLI arguments (Req 3.1, 3.6, 3.7, 3.8)."""
    parser = argparse.ArgumentParser(
        description="Kiro agent participant for the Agent Room. "
        "Sender name is room-<persona>; agent config is .kiro/agents/room-<persona>.json."
    )
    parser.add_argument("--persona", required=True, help="persona name, e.g. 'pm' for room-pm")
    parser.add_argument("--max-replies", type=int, default=DEFAULT_MAX_REPLIES,
                        help="max replies per session (default %(default)s)")
    parser.add_argument("--history", type=int, default=DEFAULT_HISTORY,
                        help="history window size for prompts (default %(default)s)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help="kiro-cli subprocess timeout in seconds (default %(default)s)")
    parser.add_argument("--host", default=DEFAULT_HOST, help="broker host (default %(default)s)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="broker port (default %(default)s)")
    parser.add_argument("--kiro-cmd", default=DEFAULT_KIRO_CMD,
                        help="kiro-cli executable; tests pass a stub path here (default %(default)s)")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    """MQTT wiring: connect, join, run the serial worker until SIGINT."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    agent = Agent(
        persona=args.persona,
        publish=lambda m: client.publish_chat(m),
        kiro_cmd=args.kiro_cmd,
        max_replies=args.max_replies,
        history_size=args.history,
        timeout=args.timeout,
    )
    client = RoomClient(client_id=agent.name, on_message=agent.on_message,
                        host=args.host, port=args.port)
    client.connect()
    client.publish_presence(agent.name, "agent", "join", _now_iso())
    logger.info("%s joined the room at %s:%s", agent.name, args.host, args.port)

    stop = threading.Event()

    def worker() -> None:
        while not stop.is_set():
            try:
                msg = agent.pending.get(timeout=0.5)
            except queue.Empty:
                continue
            agent.process(msg)

    threading.Thread(target=worker, name=f"{agent.name}-worker", daemon=True).start()

    # Scripted/background agents are stopped with SIGTERM (a non-interactive
    # shell cannot deliver SIGINT). Route SIGTERM through the same clean
    # shutdown path as Ctrl-C so the leave Presence_Event is still published
    # (the agent analog of Req 2.5).
    def _on_sigterm(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        # SIGINT (Ctrl-C) raises KeyboardInterrupt here → clean leave path.
        # SIGTERM is translated to KeyboardInterrupt above → same path.
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        client.publish_presence(agent.name, "agent", "leave", _now_iso())
        client.disconnect()
        logger.info("%s left the room", agent.name)


if __name__ == "__main__":
    main()
