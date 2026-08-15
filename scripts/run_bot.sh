#!/bin/bash
# Simple supervisor for hosts WITHOUT systemd.
# Usage: bash scripts/run_bot.sh {start|stop|status|restart}
# The bot runs `main.py trade` under the uv-managed venv. A `/stop` command
# (or SIGTERM) makes the bot shut down gracefully (gate closes, in-flight
# trades finish, exit 0) and writes bot_logs/.stop so the supervisor honors
# the stop instead of restarting.
set -u
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$APP_DIR/.ave-super.pid"
STOP_MARKER="$APP_DIR/bot_logs/.stop"
PY="$APP_DIR/.venv/bin/python"

pid_alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

bot_alive() { pgrep -f "$APP_DIR/main.py trade" >/dev/null 2>&1; }

start() {
  if pid_alive; then echo "already running (supervisor pid $(cat "$PIDFILE"))"; return 0; fi
  if [ ! -x "$PY" ]; then
    echo "ERROR: $APP_DIR/.venv missing — build it first:"
    echo "  uv sync   # installs everything from pyproject.toml"
    return 1
  fi
  rm -f "$STOP_MARKER"
  nohup bash "$APP_DIR/scripts/_supervise.sh" "$APP_DIR" >/dev/null 2>&1 &
  echo $! > "$PIDFILE"
  sleep 2
  if pid_alive; then
    echo "started (supervisor pid $(cat "$PIDFILE")) — logs: bot_logs/{bot.log,supervisor.log}"
  else
    echo "start FAILED — see bot_logs/supervisor.log"
  fi
}

stop() {
  if ! pid_alive && ! bot_alive; then echo "not running"; rm -f "$PIDFILE"; return 0; fi
  echo "stopping..."
  touch "$STOP_MARKER"
  pkill -TERM -f "$APP_DIR/main.py trade" 2>/dev/null
  sleep 5
  kill "$(cat "$PIDFILE")" 2>/dev/null
  rm -f "$PIDFILE"
  echo "stopped (exit 0)"
}

status() {
  if pid_alive; then
    echo "running (supervisor pid $(cat "$PIDFILE"))"
    pgrep -af "$APP_DIR/main.py trade" || echo "  (bot process not found — restarting soon)"
  else
    echo "not running"
  fi
}

case "${1:-}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; sleep 1; start ;;
  status)  status ;;
  *) echo "usage: $0 {start|stop|status|restart}"; exit 1 ;;
esac