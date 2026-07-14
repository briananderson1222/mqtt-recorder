#!/usr/bin/env bash
# Transport smoke test for the Agent Room (spec: agent-room, task 6.1).
#
# End-to-end flow over the real embedded broker and stock flight recorder:
#   broker (--serve) -> recorder (--mode record, room/#) -> two scripted
#   RoomClient publishers -> SIGINT recorder -> assert CSV row count ->
#   mqtt-recorder --validate exits 0.
#
# Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.7, 1.8, 5.1, 5.2, 5.3, 5.4, 5.5
#
# Expected traffic: publisher A sends 1 join presence + 3 chat messages,
# publisher B sends 1 join presence + 2 chat messages -> 7 data rows in the
# Session_CSV (plus the header row the recorder writes).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
export PYTHONPATH="$REPO_ROOT"

BIN="./target/debug/mqtt-recorder"
PORT=1884
EXPECTED_ROWS=7

BROKER_PID=""
REC_PID=""
TMPDIR_SMOKE="$(mktemp -d)"
CSV="$TMPDIR_SMOKE/session.csv"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

teardown() {
    # Kill children if still alive; never leak processes, even on failure.
    for pid in "$REC_PID" "$BROKER_PID"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$TMPDIR_SMOKE"
}
trap teardown EXIT

# --- Build the debug binary if missing -------------------------------------
if [[ ! -x "$BIN" ]]; then
    echo "debug binary missing; running cargo build..."
    BUILD_LOG="$TMPDIR_SMOKE/cargo-build.log"
    if ! cargo build >"$BUILD_LOG" 2>&1; then
        tail -n 20 "$BUILD_LOG" >&2
        fail "cargo build failed (log tail above)"
    fi
    tail -n 3 "$BUILD_LOG"
fi
[[ -x "$BIN" ]] || fail "debug binary still missing after cargo build"

# --- Preflight: port must be free ------------------------------------------
if (echo >"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
    fail "port $PORT is already in use; is another broker running?"
fi

# --- Start the Room_Broker (Req 1.1, 1.2, 1.8) ------------------------------
"$BIN" --serve --serve-port "$PORT" --no-interactive \
    >"$TMPDIR_SMOKE/broker.log" 2>&1 &
BROKER_PID=$!

# Wait for the broker to accept connections (~10s timeout).
port_up=0
for _ in $(seq 1 100); do
    if (echo >"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        port_up=1
        break
    fi
    kill -0 "$BROKER_PID" 2>/dev/null || fail "broker exited early: $(tail -n 5 "$TMPDIR_SMOKE/broker.log")"
    sleep 0.1
done
[[ "$port_up" -eq 1 ]] || fail "broker did not open port $PORT within 10s"
echo "broker up on 127.0.0.1:$PORT (pid $BROKER_PID)"

# --- Start the Flight_Recorder (Req 5.1, 5.2, 5.3) --------------------------
"$BIN" --host 127.0.0.1 --port "$PORT" --mode record --qos 1 \
    --file "$CSV" -t 'room/#' --no-interactive \
    >"$TMPDIR_SMOKE/recorder.log" 2>&1 &
REC_PID=$!

# Give the recorder time to connect and subscribe before publishing.
sleep 2
kill -0 "$REC_PID" 2>/dev/null || fail "recorder exited early: $(tail -n 5 "$TMPDIR_SMOKE/recorder.log")"
echo "flight recorder running (pid $REC_PID)"

# --- Scripted publishers via RoomClient (Req 1.3, 1.4, 1.7) ------------------
python3 - "$PORT" <<'PYEOF'
import sys
import time
from datetime import datetime, timezone

from room.common import Message, RoomClient

port = int(sys.argv[1])


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_publisher(name: str, role: str, texts: list[str]) -> None:
    client = RoomClient(client_id=f"smoke-{name}", on_message=lambda m: None, port=port)
    client.connect()
    time.sleep(0.5)  # let CONNACK/SUBACK settle
    client.publish_presence(name, role, "join", now())
    for text in texts:
        client.publish_chat(Message(sender=name, role=role, text=text, ts=now()))
    time.sleep(1.0)  # allow QoS 1 publishes to complete before disconnect
    client.disconnect()


run_publisher("alice", "human", ["hello room", "let us brainstorm", "third idea"])
run_publisher("bob", "agent", ["bob joining in", "second thought from bob"])
print("publishers done: 2 presence + 5 chat messages sent at QoS 1")
PYEOF

# Let the recorder drain everything to the CSV.
sleep 2

# --- Stop the recorder with SIGINT (it flushes the CSV on shutdown) ---------
kill -INT "$REC_PID"
wait "$REC_PID" || true
REC_PID=""

# --- Assert CSV row count (Req 5.4) ------------------------------------------
[[ -f "$CSV" ]] || fail "recorder produced no CSV at $CSV"
total_lines=$(awk 'END{print NR}' "$CSV")
data_rows=$((total_lines - 1))  # first line is the header the recorder writes
echo "CSV: $total_lines lines ($data_rows data rows, expected $EXPECTED_ROWS)"
if [[ "$data_rows" -ne "$EXPECTED_ROWS" ]]; then
    echo "--- CSV contents ---" >&2
    cat "$CSV" >&2
    fail "expected $EXPECTED_ROWS data rows, got $data_rows"
fi

# --- Validate the Session_CSV (Req 5.5) --------------------------------------
if "$BIN" --validate --file "$CSV" >"$TMPDIR_SMOKE/validate.log" 2>&1; then
    validate_exit=0
else
    validate_exit=$?
fi
echo "validate exit code: $validate_exit"
if [[ "$validate_exit" -ne 0 ]]; then
    tail -n 20 "$TMPDIR_SMOKE/validate.log" >&2
    fail "--validate exited non-zero"
fi

# --- Tear down the broker -----------------------------------------------------
kill "$BROKER_PID" 2>/dev/null || true
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo "PASS: smoke test — $data_rows data rows recorded (5 chat + 2 presence), validate exit 0"
