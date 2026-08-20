# ave-signal-trade

Filters "New Solana Pool Launched" signals from the AveSolanaTokenScanner Telegram channel and trades the winners live (paper by default, real with `DRY_RUN=false`).

## Filter (honest-engine replay, 2026-08-13 feed — `scripts/replay_tune.py`)

- mcap $5K–$20K, DEX = Pumpfunamm, snipes ≥ 3, security score = 0
- Multi-signal tokens deduped to first signal
- Honest-engine results (fresh-quote entries, realized TP/SL/trail/timeout exits, dead-pool writeoffs): **n=105 trades/day, 32.4% realize the 4x take-profit, EV ≈ +101%/trade, win-to-3x ≈ 33%, median exit 1.49x**
- The edge is **fat-tailed, not high-win-rate**: ~19% of positions stop out at −70%, ~40% of pools die mid-hold and are written off at their last mark. Single-day dataset — treat as directional evidence, not proof.
- Earlier "60–64% win-to-3x" figures came from peak-touch simulation and do **not** reproduce once exits are realized.

## Honest engine (paper == live)

The paper engine reproduces live execution behavior so backtests and paper runs predict what real money would have done:

- **Entries** refresh the Jupiter quote at entry time (`force=True`); a failed refresh skips the entry — no fills from stale arm-time quotes.
- **Exits** fill from a real token→SOL quote (`paper_sell_proceeds`), same formula as live proceeds.
- **Failed sells keep the position open** exactly like live: failure counted toward the give-up writeoff, retry backs off, exit markers cleared. There is no tick-mark fallback that books an exit the wallet could not take.
- **Safety gates fail closed**: if DexPaprika or Helius cannot answer, the signal is rejected; the entry-time liquidity re-check rejects on any missing/stale verdict. An unverified pool is not a pool we buy.
- **Dead-pool writeoffs**: after `MAX_SELL_FAILURES` (6) consecutive failed sells a position is written off at its last mark and the slot freed; positions past their timeout window write off after `MAX_SELL_FAILURES_TIMEOUT` (3).
- Paper quotes are deliberately taker-less: a throwaway taker would fail every paper quote with "Insufficient funds" (the paper wallet holds nothing). Dead pools fail identically with or without a taker.

## Usage

```bash
uv run main.py scan                       # offline filter + win-rate cross-check
uv run main.py trade                      # live trading (paper by default)
uv run main.py channels                   # list visible chats
.venv/bin/python scripts/replay_tune.py   # honest-engine replay + filter grid search
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
| `paper_trader`   | trade gate; entry on first buy; TP 4x / SL 0.3x / 1h timeout / trailing stop; fail-closed entry gates; dead-pool writeoffs; checkpoint |
| `pool_check`     | arm-time DexPaprika liquidity + Helius dev-rep gates (fail-closed)      |
| `logs`           | writes `bot_logs/` (bot.log, journal.json, trade_log.csv, .stop)        |
| `notifier`       | Telegram cards + `/start /stop /status /help`; no-op without BOT_TOKEN  |
| `models`/`parser`| `Signal`/`Position` dataclasses + message parsing                       |

Data pipeline: Telegram events → filter → fail-closed pool gates → Jupiter quote gate → arm → fresh entry quote → pumpapi price → position exit (TP / trailing stop / SL / timeout, with dead-pool writeoffs). Paper mode never places real orders.