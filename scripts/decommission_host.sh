#!/bin/bash
# Fully remove EVERY prior supervisor of the bot on this host:
#   - systemd unit (ave-signal-trade.service)
#   - nohup _supervise.sh / run_bot.sh supervisors
#   - watchdog.sh crontab lines (root + current user, both app dirs)
#   - supervisor pidfiles / .stop markers
# Run as your normal (non-root) user; it uses sudo where needed.
set -u

APP_DIR="${1:-/home/mdev/ave_signal_trade}"
echo "== decommissioning supervision for $APP_DIR =="

echo "-- [1/5] systemd unit --"
sudo systemctl stop ave-signal-trade.service 2>/dev/null; sudo systemctl disable ave-signal-trade.service 2>/dev/null
sudo rm -f /etc/systemd/system/ave-signal-trade.service
sudo systemctl daemon-reload

echo "-- [2/5] kill every bot + supervisor process --"
sudo pkill -KILL -f "main.py trade" 2>/dev/null
sudo pkill -KILL -f "_supervise.sh" 2>/dev/null
sudo pkill -KILL -f "run_bot.sh" 2>/dev/null
sleep 2

echo "-- [3/5] remove watchdog crontab lines --"
sudo crontab -l 2>/dev/null | grep -vE "ave_signal_trade/(scripts/)?watchdog.sh" | sudo crontab - 2>/dev/null || true
crontab -l 2>/dev/null | grep -vE "ave_signal_trade/(scripts/)?watchdog.sh" | crontab - 2>/dev/null || true

echo "-- [4/5] remove pidfiles / stop markers --"
sudo rm -f "$APP_DIR/.ave-super.pid"
sudo rm -f "$APP_DIR/bot_logs/.stop"
sudo rm -f /root/ave_signal_trade/.ave-super.pid 2>/dev/null
sudo rm -f /root/ave_signal_trade/bot_logs/.stop 2>/dev/null

echo "-- [5/5] VERIFY (all three must say NONE) --"
echo "--- remaining bot procs ---"
ps -eo pid,user,cmd | grep -E "[m]ain.py trade|[_]supervise.sh|[r]un_bot.sh" || echo "NONE"
echo "--- remaining unit ---"
sudo systemctl list-unit-files | grep ave-signal-trade || echo "NONE"
echo "--- remaining crontab watchdog lines ---"
crontab -l 2>/dev/null | grep watchdog || echo "NONE"
echo "== done =="