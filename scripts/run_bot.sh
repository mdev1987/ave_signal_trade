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

# NOTE: the `[m]` bracket avoids pgrep matching this script's own cmdline.
bot_alive() { pgrep -f "[m]ain.py trade" >/dev/null 2>&1; }

pid_alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

# If the systemd unit exists, defer to it entirely (even when currently
# stopped) — never run a nohup instance alongside it. This keeps a
# `systemctl stop` genuinely stopped: no supervisor resurrects the bot.
systemd_manages() {
  command -v systemctl >/dev/null 2>&1 \
    && [ -e /etc/systemd/system/ave-signal-trade.service ]
}

kill_bot() {
  # Gracefully stop any existing bot (supervisor or orphaned) so we never run two.
  if ! bot_alive; then return 0; fi
  echo "stopping existing bot instance(s)..."
  touch "$STOP_MARKER"
  pkill -TERM -f "[m]ain.py trade" 2>/dev/null
  for _ in 1 2 3 4 5 6; do
    bot_alive || break
    sleep 1
  done
  if bot_alive; then
    echo "force-killing after grace period..."
    pkill -KILL -f "[m]ain.py trade" 2>/dev/null
  fi
  rm -f "$STOP_MARKER"
}

start() {
  if systemd_manages; then
    echo "systemd is managing the bot (ave-signal-trade.service) — use: systemctl status|restart|stop ave-signal-trade"
    return 0
  fi
  # Never run two instances: kill any existing bot first, then start fresh.
  if pid_alive && bot_alive; then echo "already running (supervisor pid $(cat "$PIDFILE"))"; return 0; fi
  kill_bot
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
  if systemd_manages; then
    echo "systemd is managing the bot — use: systemctl stop ave-signal-trade"
    return 0
  fi
  if ! pid_alive && ! bot_alive; then echo "not running"; rm -f "$PIDFILE"; return 0; fi
  echo "stopping..."
  touch "$STOP_MARKER"
  pkill -TERM -f "[m]ain.py trade" 2>/dev/null
  # Graceful shutdown can take up to SHUTDOWN_GRACE_S (~60s) for in-flight
  # trades + sending the final card; wait that long so the "Bot Stopped" card
  # is sent before force-killing anything still alive (matches TimeoutStopSec).
  for _ in $(seq 1 120); do
    bot_alive || break
    sleep 1
  done
  if bot_alive; then
    echo "force-killing remaining bot process(es)..."
    pkill -KILL -f "[m]ain.py trade" 2>/dev/null
  fi
  if [ -f "$PIDFILE" ]; then
    kill "$(cat "$PIDFILE")" 2>/dev/null
  fi
  rm -f "$PIDFILE"
  echo "stopped (exit 0)"
}

status() {
  if systemd_manages; then
    echo "running under systemd (ave-signal-trade.service) — use: systemctl status ave-signal-trade"
    return 0
  fi
  if pid_alive; then
    echo "running (supervisor pid $(cat "$PIDFILE"))"
    pgrep -af "[m]ain.py trade" || echo "  (bot process not found — restarting soon)"
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