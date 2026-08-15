# ave-signal-trade

Filters "New Solana Pool Launched" signals from the AveSolanaTokenScanner Telegram channel and trades the winners live (paper by default, real with `DRY_RUN=false`).

## Filter (data-backed, 2026-08-13 replay: 108 passed / 59.4% win-to-3x)

- mcap $5K–$20K, DEX = Pumpfunamm, snipes ≥ 3, security score = 0
- Multi-signal tokens deduped to first signal

## Usage

```bash
uv run main.py scan                       # offline filter + win-rate cross-check
uv run main.py trade                      # live trading (paper by default)
uv run main.py channels                   # list visible chats
```

First run prompts for your Telegram phone number and writes `config.ini` + `telegram_session`.

## DRY_RUN / real trading

- `DRY_RUN=true` (default): **paper** — positions are simulated from the live feed; Jupiter is only a quote gate (never signs or executes).
- `DRY_RUN=false`: **live** — real buys/sells via Jupiter `/order` + `/execute`. Requires a base58 `PRIVATE_KEY` in `.env`; the bot fails fast at startup otherwise.

## Telegram control

Send to your bot via `BOT_TOKEN`/`CHAT_ID`:

| Command    | Action                                                             |
| ---------- | ------------------------------------------------------------------ |
| `/start`   | open the trade gate (resume trading)                               |
| `/stop`    | graceful shutdown: gate closes, in-flight trade finishes, exit 0   |
| `/status`  | balance, winrate, realized PnL, active position, quote-gate stats  |
| `/help`    | command list                                                       |

## Running 24/7

```bash
bash scripts/run_bot.sh {start|stop|status|restart}   # nohup supervisor (no systemd)
bash scripts/watchdog.sh                              # crontab health watchdog
```

With systemd: copy `scripts/sol-bot.service` to `/etc/systemd/system/`, fix `WorkingDirectory`/`ExecStart`, then `systemctl enable --now sol-bot`. See `docs/docs/scripts_deply_samples/`.

## Host setup (fresh VPS)

### 1. Install uv + clone

```bash
# uv (if missing): https://docs.astral.sh/uv/
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

cd /opt
git clone <your-repo-url> ave-signal-trade   # or scp/rsync the folder
cd ave-signal-trade
```

The repo layout: `main.py` at the root, all modules under `src/` (the scripts already reference `main.py` from the project root).

### 2. Install dependencies

```bash
uv sync          # creates .venv and installs everything from pyproject.toml
```

Python 3.14+ is required (see `.python-version`).

### 3. Configure `.env`

`.env` is gitignored, so a fresh clone has none — copy it from your dev box or create it:

```bash
scp user@dev-host:/path/to/ave-signal-trade/.env ./
chmod 600 .env
```

Required keys: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` (colon format), `BOT_TOKEN`, `CHAT_ID`, `JUPITER_API_KEY`, `PUMPAPI_WSS`. Trading mode:

```ini
DRY_RUN=true      # paper mode (default; never signs/executes)
# DRY_RUN=false   # live mode — REQUIRES PRIVATE_KEY (base58), else the bot fails fast
# PRIVATE_KEY=    # never use your main wallet
```

### 4. First-run Telegram auth

`trade` and `channels` prompt for your phone number once and write `config.ini` + `telegram_session`:

```bash
.venv/bin/python main.py channels        # verify the session works
```

`telegram_session*` and `config.ini` are gitignored.

### 5. Start the supervisor

```bash
bash scripts/run_bot.sh start             # runs `main.py trade` 24/7, auto-restarts
bash scripts/run_bot.sh status
```

Logs land in `bot_logs/` (`bot.log`, `journal.json`, `trade_log.csv`, `supervisor.log`). A graceful `/stop` (or `run_bot.sh stop`) writes `bot_logs/.stop` so the supervisor stays down instead of restarting.

### 6. Watchdog (recommended, no systemd)

Add both lines to crontab so the bot survives reboots and wedges:

```crontab
*/5 * * * * /opt/ave-signal-trade/scripts/watchdog.sh /opt/ave-signal-trade
@reboot      /opt/ave-signal-trade/scripts/watchdog.sh /opt/ave-signal-trade
```

The watchdog restarts a dead/stalled bot and sends a Telegram alert (from `BOT_TOKEN`/`CHAT_ID`).

### 7. systemd alternative

If systemd is available:

```bash
sudo cp scripts/sol-bot.service /etc/systemd/system/ave-signal-trade.service
sudo sed -i "s|/home/mdev/Programming/ave_signal_trade|/opt/ave-signal-trade|g" \
  /etc/systemd/system/ave-signal-trade.service
sudo systemctl daemon-reload
sudo systemctl enable --now ave-signal-trade
journalctl -u ave-signal-trade -e
```

## Architecture

| Module           | Role                                                                    |
| ---------------- | ----------------------------------------------------------------------- |
| `telegram_feed`  | tgdata: real-time `on_new_message` events + backfill                    |
| `price_feed`     | pumpapi WSS: live trade prices, auto-reconnect, clean stop              |
| `jupiter_swap`   | Jupiter `/order`+`/execute` client; quote gate + DRY_RUN gated execution|
| `filter`         | data-backed rules on mcap/dex/snipes/sec_score                          |
| `paper_trader`   | trade gate; entry on first buy; TP 3x / SL 0.5x / 1h timeout; checkpoint |
| `logs`           | writes `bot_logs/` (bot.log, journal.json, trade_log.csv, .stop)        |
| `notifier`       | Telegram cards + `/start /stop /status /help`; no-op without BOT_TOKEN  |
| `models`/`parser`| `Signal`/`Position` dataclasses + message parsing                       |

Data pipeline: Telegram events → filter → Jupiter quote gate → arm → pumpapi price → position exit (TP/SL/timeout). Paper mode never places real orders.