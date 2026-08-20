#!/bin/bash
# One-shot provisioning for a fresh host.
# Usage: bash scripts/deploy_host.sh [/path/to/app]
#   no arg  -> deploy in the repo you are standing in (git clone / tarball)
#   with arg -> deploy at that path (created if missing, e.g. /opt/sol-bot)
#
# Self-contained: installs uv if missing, syncs the venv from pyproject.toml,
# smoke-tests the modules, validates .env, then wires up a supervisor
# (systemd when available, otherwise the nohup run_bot.sh + crontab watchdog).
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="${1:-$(cd "$SCRIPT_DIR/.." && pwd)}"
mkdir -p "$APP_DIR"
cd "$APP_DIR"
if [ ! -f main.py ] || [ ! -d src ]; then
  echo "ERROR: $APP_DIR is not the bot repo (main.py/src missing)."
  echo "Run from the cloned repo:  bash scripts/deploy_host.sh"
  echo "or pass the app dir:       bash scripts/deploy_host.sh /opt/sol-bot"
  exit 1
fi
echo "== deploying in $APP_DIR =="

echo "== [1/5] uv =="
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi
uv --version

echo "== [2/5] venv + deps (from pyproject.toml; .python-version pins 3.14) =="
uv sync

echo "== [3/5] import smoke test =="
PYTHONPATH=src .venv/bin/python - <<'PY'
import importlib, glob, os, sys
mods = ["main"] + [
    os.path.basename(p)[:-3] for p in glob.glob("src/*.py")
    if not p.endswith("__init__.py")
]
bad = []
for m in mods:
    try:
        importlib.import_module(m)
    except Exception as e:
        bad.append(f"{m}: {e!r}")
if bad:
    print("IMPORT FAILURES:", bad); sys.exit(1)
print(f"OK — {len(mods)} modules import on the venv")
PY

echo "== [4/5] .env =="
if [ ! -f .env ]; then
  echo "ERROR: .env missing — it is gitignored, so a clone has none."
  echo "  a) copy your dev .env:   scp .env user@HOST:/tmp/.env && cp /tmp/.env $APP_DIR/.env"
  echo "  b) or bootstrap one:     cp .env.example .env   (then fill in TELEGRAM_API_ID/HASH, BOT_TOKEN, CHAT_ID, JUPITER_API_KEY, ...)"
  exit 1
fi
chmod 600 .env
if grep -qiE '^[[:space:]]*DRY_RUN=[[:space:]]*(0|false|no)[[:space:]]*$' .env; then
  echo "!!! WARNING: DRY_RUN is NOT true — this host will place REAL orders"
else
  echo "DRY_RUN=true (or unset — default) — paper trading (no real orders)"
fi
if ! grep -q '^TELEGRAM_PHONE' .env; then
  echo "NOTE: TELEGRAM_PHONE not set — first 'trade'/'channels' run will prompt once."
fi

echo "== [5/5] process supervisor =="
# NEVER run two instances. Fully decommission ALL existing supervision —
# systemd unit, nohup _supervise.sh, and the watchdog crontab — BEFORE
# installing a single fresh supervisor. A leftover crontab watchdog or nohup
# supervisor is what caused "keep only one websocket connection" (pumpapi) and
# getUpdates 409 / sqlite "database is locked" (two bots, one session).

# Pre-flight: abort BEFORE touching anything if more than one bot instance is
# already running. Two instances polling the same BOT_TOKEN produce the
# getUpdates 409 Conflict (each one steals the other's updates) and double-trade
# in live mode — a second deploy must never paper over that by killing them.
preflight_single_instance() {
  local count
  # `pgrep` exits 1 when nothing matches; under `set -euo pipefail` that would
  # kill the script, so swallow the failure and rely on the count alone.
  count=$(pgrep -f "[m]ain.py trade" 2>/dev/null | wc -l || true)
  if [ "$count" -gt 1 ]; then
    echo "ERROR: $count bot instances are running ('pgrep -f \"main.py trade\"')."
    echo "Resolve the duplicate supervision manually (systemd unit, nohup"
    echo "_supervise.sh, crontab watchdog) so exactly ONE instance remains,"
    echo "then re-run this deploy. Refusing to proceed."
    exit 1
  fi
}

preflight_single_instance
decommission_all() {
  echo "== decommissioning any existing supervision =="
  # systemd unit (stop + disable + remove + reload):
  if command -v systemctl >/dev/null 2>&1; then
    systemctl stop ave-signal-trade.service >/dev/null 2>&1 || true
    systemctl disable ave-signal-trade.service >/dev/null 2>&1 || true
    rm -f /etc/systemd/system/ave-signal-trade.service
    systemctl daemon-reload >/dev/null 2>&1 || true
  fi
  # bot + nohup supervisor processes ([m] bracket avoids self-match):
  pkill -TERM -f "[m]ain.py trade" >/dev/null 2>&1 || true
  sleep 3
  pkill -KILL -f "[m]ain.py trade" >/dev/null 2>&1 || true
  pkill -TERM -f "_supervise.sh" >/dev/null 2>&1 || true
  # watchdog crontab lines for THIS app dir:
  if command -v crontab >/dev/null 2>&1; then
    crontab -l 2>/dev/null | grep -vF "$APP_DIR/scripts/watchdog.sh" | crontab - 2>/dev/null || true
  fi
  rm -f "$APP_DIR/.ave-super.pid" "$APP_DIR/bot_logs/.stop"
  sleep 1
  if pgrep -f "[m]ain.py trade" >/dev/null 2>&1; then
    echo "ERROR: a bot instance is still running — cannot guarantee a single instance."
    exit 1
  fi
  echo "all prior supervision removed — starting fresh"
}

decommission_all

if command -v systemctl >/dev/null 2>&1 && [ -d /etc/systemd/system ] && [ -w /etc/systemd/system ]; then
  echo "systemctl found — installing system service"
  cat > /etc/systemd/system/ave-signal-trade.service <<UNIT
[Unit]
Description=Ave signal trade bot (paper: DRY_RUN=true)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/.venv/bin/python main.py trade
# on-failure: restart on crash (any non-zero exit), but a graceful exit 0
# (telegram /stop or SIGTERM finishing in-flight trades) stays stopped.
Restart=on-failure
RestartSec=5
# Bound the bot's memory so a leak/OOM takes down the service (and is then
# auto-restarted by Restart=on-failure) instead of the whole 4G host.
MemoryMax=2500M
Environment=PYTHONUNBUFFERED=1
# optional: match the local log timestamps
# Environment=TZ=Asia/Tehran
StandardOutput=journal
StandardError=journal
# graceful /stop waits for in-flight trades to finish + sends the stop card
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
  systemctl enable --now ave-signal-trade.service
  sleep 6
  systemctl status ave-signal-trade.service --no-pager | grep -E "Active|Main PID|Memory"
  echo
  echo "--- log tail ---"
  tail -n 4 "$APP_DIR/bot_logs/bot.log" 2>/dev/null || true
  echo
  # Install the wedge-recovery watchdog into crontab even under systemd:
  # Restart=on-failure never restarts a *hung* process, so the watchdog's
  # stale-log check + `systemctl restart` is the safety net for that.
  if command -v crontab >/dev/null 2>&1; then
    ( crontab -l 2>/dev/null; \
      echo "*/5 * * * * $APP_DIR/scripts/watchdog.sh $APP_DIR"; \
      echo "@reboot $APP_DIR/scripts/watchdog.sh $APP_DIR" ) | crontab -
    echo "watchdog installed in crontab (stale-log wedge recovery every 5 min)"
  fi
  echo
  echo "DONE. Commands:  systemctl status|restart|stop ave-signal-trade   |   journalctl -u ave-signal-trade -e"
else
  echo "no systemctl — using simple supervisor (nohup + auto-restart)"
  bash "$APP_DIR/scripts/run_bot.sh" start
  sleep 6
  bash "$APP_DIR/scripts/run_bot.sh" status
  echo
  echo "--- log tail ---"
  tail -n 4 "$APP_DIR/bot_logs/bot.log" 2>/dev/null || true
  echo
  # Install the watchdog into crontab so boot/5-min auto-restart matches this
  # deploy. Idempotent: existing watchdog lines for this app dir are removed
  # first (decommission_all already did), so these are the only ones.
  if command -v crontab >/dev/null 2>&1; then
    ( crontab -l 2>/dev/null; \
      echo "*/5 * * * * $APP_DIR/scripts/watchdog.sh $APP_DIR"; \
      echo "@reboot $APP_DIR/scripts/watchdog.sh $APP_DIR" ) | crontab -
    echo "watchdog installed in crontab (auto-start on boot + every 5 min)"
  fi
  echo
  echo "DONE. Commands:  bash scripts/run_bot.sh {start|stop|status|restart}"
fi
