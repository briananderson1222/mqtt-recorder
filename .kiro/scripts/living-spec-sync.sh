#!/usr/bin/env bash
# Living-spec bridge for kiro-cli 2.x: fired by a postToolUse hook (matcher: fs_write).
# Daemonizes a kiro-cli run of the living-spec prompt (fork + setsid) so the hook
# returns instantly; a lock prevents overlapping syncs.
# Native equivalents: .kiro/hooks/living-spec.json (Kiro IDE 1.0 / CLI 3.0 PostFileSave agent action).
exec 0</dev/null
REPO_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LOG="${TMPDIR:-/tmp}/living-spec-sync.log"
LOCK="${TMPDIR:-/tmp}/living-spec-sync.lock"
PROMPT_FILE="$REPO_DIR/.kiro/scripts/living-spec-prompt.txt"

if ! mkdir "$LOCK" 2>/dev/null; then
  # a sync is already running; treat locks older than 15 minutes as stale
  if [ -n "$(find "$LOCK" -maxdepth 0 -mmin +15 2>/dev/null)" ]; then
    rmdir "$LOCK" 2>/dev/null
    mkdir "$LOCK" 2>/dev/null || { echo "living-spec: sync already running, skipping"; exit 0; }
  else
    echo "living-spec: sync already running, skipping"
    exit 0
  fi
fi

REPO_DIR="$REPO_DIR" LOG="$LOG" LOCK="$LOCK" PROMPT_FILE="$PROMPT_FILE" python3 - <<'PY'
import os, sys, subprocess
if os.fork() > 0:
    sys.exit(0)          # parent returns to the hook runner immediately
os.setsid()              # new session: fully detach from the hook runner
env = os.environ
log = open(env["LOG"], "a")
log.write("=== living-spec sync triggered ===\n")
prompt = open(env["PROMPT_FILE"]).read()
# run the sync agent, then release the lock
subprocess.Popen(
    ["bash", "-c", 'kiro-cli chat --no-interactive --trust-all-tools "$1"; rmdir "$2" 2>/dev/null', "_", prompt, env["LOCK"]],
    cwd=env["REPO_DIR"], stdin=subprocess.DEVNULL, stdout=log, stderr=log,
)
PY
echo "living-spec: spec sync started in background (log: $LOG)"
