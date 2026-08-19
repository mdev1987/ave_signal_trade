#!/bin/bash
# Health watchdog for the bot, used by the nohup supervisor path AND as a
# systemd backstop (catches a wedged process that Restart=on-failure misses).
#
# Usage:  bash scripts/watchdog.sh [APP_DIR]          (defaults to repo root)
# Crontab:  */5 * * * * /opt/ave-signal-trade/scripts/watchdog.sh /opt/ave-signal-trade
#
# Liveness signal: bot_logs/bot.log mtime. The bot logs a heartbeat line
# every 5 minutes even when idle, so a log silent for WATCHDOG_STALE_MIN is a
# wedged loop (hang in I/O), not a quiet market.
#
# Watches two failure modes that _supervise.sh alone cannot survive:
#   1. the whole supervisor (nohup shell) died — host reboot / OOM / terminal
#      kill. Restart it.
#   2. the supervisor is alive but the bot's log went silent for
#      WATCHDOG_STALE_MIN minutes — the bot is wedged (hang in I/O).
#      Restart it; the graceful-stop path closes the gate and exits cleanly.
#
# On either action a Telegram alert is sent (BOT_TOKEN/CHAT_ID from .env).

set -u
APP_DIR="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
PIDFILE="$APP_DIR/.ave-super.pid"
BOT_LOG="$APP_DIR/bot_logs/bot.log"
STOP_MARKER="$APP_DIR/bot_logs/.stop"
STALE_MIN="${WATCHDOG_STALE_MIN:-10}"

# Telegram alert (optional) — read creds from .env so the crontab line stays bare
BOT_TOKEN="${BOT_TOKEN:-$(grep -E '^BOT_TOKEN=' "$APP_DIR/.env" 2>/dev/null | cut -d= -f2-)}"
CHAT_ID="${CHAT_ID:-$(grep -E '^CHAT_ID=' "$APP_DIR/.env" 2>/dev/null | cut -d= -f2-)}"
alert() {
  [ -z "$BOT_TOKEN" ] || [ -z "$CHAT_ID" ] && return 0
  curl -s --max-time 10 -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
    -d "chat_id=$CHAT_ID" -d "text=$1" >/dev/null 2>&1
}

supervisor_alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

log_stale() {
  [ -f "$BOT_LOG" ] && [ -n "$(find "$BOT_LOG" -mmin +"$STALE_MIN")" ]
}

# If the systemd unit exists, defer to it for start/stop decisions, but still
# recover a *wedged* process: Restart=on-failure never restarts a hung bot.
# A `systemctl stop` (inactive service) must stay stopped, so only restart
# when the unit is still active but the log has gone silent.
if command -v systemctl >/dev/null 2>&1 \
   && [ -e /etc/systemd/system/ave-signal-trade.service ]; then
  if [ -f "$STOP_MARKER" ]; then
    exit 0  # deliberately stopped — nothing to watch
  fi
  if systemctl is-active --quiet ave-signal-trade.service; then
    if log_stale; then
      echo "[$(date '+%F %T')] watchdog: systemd unit active but bot.log silent > ${STALE_MIN}m — restarting"
      alert "⚠️ watchdog: bot wedged (log silent >${STALE_MIN}m) — restarting"
      systemctl restart ave-signal-trade.service
    fi
  fi
  exit 0
fi

if [ -f "$STOP_MARKER" ]; then
  exit 0  # deliberately stopped — nothing to watch
fi

if supervisor_alive; then
  if log_stale; then
    echo "[$(date '+%F %T')] watchdog: bot log silent > ${STALE_MIN}m — restarting"
    alert "⚠️ watchdog: bot log silent >${STALE_MIN}m — restarting"
    bash "$APP_DIR/scripts/run_bot.sh" restart
  fi
else
  echo "[$(date '+%F %T')] watchdog: supervisor not running — starting"
  alert "⚠️ watchdog: bot supervisor not running — restarting"
  bash "$APP_DIR/scripts/run_bot.sh" start
fi