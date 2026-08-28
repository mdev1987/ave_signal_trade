#!/usr/bin/env bash
# Reliable supervisor: keeps the watcher alive. If it exits (crash, OOM, deploy)
# it restarts after a short backoff. Logs to bot_supervisor.log.
#   ./run_bot.sh            # foreground (Ctrl-C stops the supervisor too)
#   setsid ./run_bot.sh &    # background, survives terminal close
set -u
cd "$(dirname "$0")"
LOG="bot_supervisor.log"
MAX_BACKOFF=60
backoff=2
while true; do
    echo "$(date -u +%FT%TZ) starting watcher" >> "$LOG"
    if uv run main.py watch >> "$LOG" 2>&1; then
        echo "$(date -u +%FT%TZ) watcher exited cleanly" >> "$LOG"
        break
    fi
    echo "$(date -u +%FT%TZ) watcher crashed; restart in ${backoff}s" >> "$LOG"
    sleep "$backoff"
    backoff=$((backoff * 2))
    [ "$backoff" -gt "$MAX_BACKOFF" ] && backoff=$MAX_BACKOFF
done
