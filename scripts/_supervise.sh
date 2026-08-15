#!/bin/bash
# Inner auto-restart loop used by run_bot.sh (not meant to be called directly).
# Keeps the bot alive on hosts without systemd: restart 5s after any exit —
# unless bot_logs/.stop exists, which means a graceful /stop was requested.
APP_DIR="$1"
cd "$APP_DIR" || exit 1
PY="$APP_DIR/.venv/bin/python"
STOP_MARKER="$APP_DIR/bot_logs/.stop"
LOG="$APP_DIR/bot_logs/supervisor.log"
mkdir -p "$(dirname "$LOG")"
while true; do
  if [ -f "$STOP_MARKER" ]; then
    echo "[$(date '+%F %T')] supervisor: .stop marker found — staying stopped" >> "$LOG"
    exit 0
  fi
  echo "[$(date '+%F %T')] supervisor: starting bot" >> "$LOG"
  "$PY" "$APP_DIR/main.py" trade >> "$LOG" 2>&1
  rc=$?
  if [ -f "$STOP_MARKER" ]; then
    echo "[$(date '+%F %T')] supervisor: graceful stop (rc=$rc) — staying stopped" >> "$LOG"
    exit 0
  fi
  echo "[$(date '+%F %T')] supervisor: bot exited rc=$rc — restarting in 5s" >> "$LOG"
  sleep 5
done