# ave-signal-trade — **Profitable Mode: 1 POS × 0.2 SOL × 25min TTL**

Filters "New Solana Pool Launched"-style signals from the **DRBTSolanaPF** Telegram channel and trades the winners live. **Robust, reliable, profitable** — single-position focus avoids rug clustering and dilution.

## Profitable Strategy (honest-engine replay, 2026-08-13 feed — `scripts/replay_tune.py`)

- **Filter (validated):** `mcap $5K–$20K, DEX = Pumpfunamm (+ Pump alias), snipes ≥ 3, sec = 0` — deduped to first signal per CA; DEX set configurable via `FILTER_DEXS` CSV. Keep `Pumpfunamm` core; `Meteora/Raydium` are research variants with separate thresholds (adding them without retune drops EV `+101% → +62%`).
- **Loosened tier active in `.env` = EXPERIMENT L2, not a validated strategy** (`mcap $2.5K–$50K, snipes ≥ 1`, ~3× candidates): PRODUCTION CORE remains $5K–$20K/snipes≥3. The active tier is logged at startup (`filter tier: ...`) so runs are separable. On the widened 08-20 flow the damper killed 37 passing instances and RugCheck vetoed 15/30 unique CAs ("LP Unlocked"). Per-bucket outcomes ($2.5–5K/$5–10K/$10–20K/$20–50K × snipes 1/2/3+) must be measured before promoting.
- **Sizing:** `POSITION_SIZE_SOL=0.2` (10% of `2 SOL` bankroll), `MAX_POSITIONS=1` — serial execution, no concurrent dilution, no 429 storm from parallel sells.
- **Hold (TTL) — 25min optimum (`1500s`):** Honest-engine sweep on `2026-08-13` (`TP4 SL0.3 trail2×50%`):
  - `5m +58.9% EV, win4x 13.3%` → `10m +83%` → `15m +94.7%` → `20m +98.1%` → **`25m +99.6% win30.5%`** → `60m +101.4% win32.4%`.  
  `25m` keeps **98% of 60m EV** but frees the single slot **2.4× faster** (`timeout 10 vs 4`, `feed_end 40` identical), doubling daily throughput for 1-POS.
- **Exits (best strategy, reliable):** `TP 4.0×` (real quote touch) > `trailing 2×→50% retrace` (locks runners) > `SL 0.3×` (−70%) > `25min timeout` > `dead-pool writeoff`. Fat-tailed: `median 1.49x`, `~19%` SL, `~40%` pools die — edge is **EV, not win-rate**.
- **Result:** `n≈105/day` at `60m` → `~40-50/day` at `25m` with `1 POS` (~57 max theoretical 24h/25m), `EV ≈ +99%/trade`.

## Honest engine (paper == live) + Rug Avoidance

Paper == live — backtests predict real PnL:

- **Entries (avoid rug, at the ENTRY MOMENT):** arm-time runs only cheap gates; on the actual entry trigger the pipeline is `PumpAPI pool gate` (stream-carried `burnedLiquidity`/`quoteInPool`/authorities, journaled as `poolfeat`; a liquidity REMOVAL within `LIQ_REMOVE_VETO_S` vetoes outright — zero latency, no API call) → `FINAL BUY /order` (`force=True`, cache-bypassed) → **stability burst measured against THAT quote** (`3×300ms` via `QUOTE_STABILITY_INTERVAL_MS`, not the 1s global throttle) → **sell `/order` for exactly that quote's output** (`REQUIRE_SELL_QUOTE true + MAX_SELL_IMPACT 5.0%`) → `execute_order(final.order)` signs & sends THE VALIDATED transaction — no re-quote between gate and execution. Paper fills from the same final quote. The validated market state IS the executed market state.
- **Jupiter RTSE (ultra mode):** buys OMIT `slippageBps` from `/order` (`JUPITER_ORDER_RTSE=true`) so Jupiter applies its Real-Time Slippage Estimator with ALL routers eligible (Metis/JupiterZ RFQ/Dflow/OKX). Per Jupiter's routing-impact matrix any optional param flips `/order` to `manual` mode which may restrict routing. Returned `router`/`mode`/`slippageBps` are journaled per quote as diagnostics. Sells keep explicit slippage (execution certainty dominates on exits).
- **Exits (TP/SL/trailing, robust):** real `token→SOL` quote (`paper_sell_proceeds`) same as live; **failed sells keep position open**, counted to `MAX_SELL_FAILURES 6` (`3` after TTL), `SELL_BACKOFF 60s` (`429/timeout` is transient, not counted — fixes STAR `6×429` writeoff bug), markers cleared, bounded `90s` close timeout so hung sell never stalls sweep.
- **Fail-closed gates (reliable):** DexPaprika liquidity (`MIN_LIQUIDITY_USD 5000`, `LIQ_CONFIRM_WINDOW 10s`) + Helius dev-rep (`DEV_REP`) — `429`/timeout rejects; entry-time `cached_verdict` re-check rejects missing/stale. Unverified pool = no trade.
- **RugCheck gate (arm-time, fail-open):** `GET /v1/tokens/{mint}/report/summary` (cached `RUGCHECK_CACHE_TTL_S 120`); vetoes only on explicit danger risks (`RUGCHECK_VETO_RISKS=lp unlocked,mint authority,freeze authority`). A missing report ADMITS the token — sec-0 snipes race RugCheck's indexer, and fail-closed there would kill every entry. Every evaluation journals per-risk boolean features (`report_missing`, `lp_unlocked`, `mint_authority`, `freeze_authority`) to `journal.json` so each veto's false-positive rate can be measured against realized outcomes (mint/freeze go beyond the LP-unlocked evidence base). Evidence (`scripts/rugcheck_validate.py` on the 2026-08-20 live rugs): every LP-pull rug (TONK/NEX Ai#2/牛来) carried "Large Amount of LP Unlocked"; winners never did. Scores are NOT used by default — winners and rugs both score ~65.
- **Serial-relaunch damper:** the same normalized token name on `SCAM_DAMPER_MAX_CAS 3` distinct CAs within `SCAM_DAMPER_WINDOW_MIN 360` is rejected after the base filter ("NEX Ai" x5+, "牛来" x20 relaunch farms). Replay on the 08-20 stream: damps 24/63 passing signals incl. the live NEX Ai rug.
- **Dead-pool writeoffs:** `6` fails (`3` post-TTL) → writeoff at last mark, slot freed, Telegram alert, `trade_log.csv` kept.
- Taker-less paper quotes (throwaway would be `Insufficient funds`); dead pools fail taker-less identically.

## Active stack (minimal + free)

```text
Telegram (signals) → PumpAPI WS (prices/events/pool state) → Rug/Pool gates
(RugCheck + ScamDamper + stream pool features) → Jupiter /order + /execute
(RTSE buys) → Helius free RPC only for wallet reads (getBalance /
getAccountInfo / getTokenAccountsByOwner) and dev checks.
```

SHYFT / CabalSpy / CoinStats / PumpDev / DexScreener are **not consumed** by the
runtime; their keys are disabled in `.env` until modules exist. No paid Helius
products (Sender/LaserStream/Shred) are used.

## Usage

```bash
uv run main.py scan                       # offline filter + win-rate cross-check
uv run main.py trade                      # live trading (paper by default)
uv run main.py channels                   # list visible chats
.venv/bin/python scripts/replay_tune.py   # honest-engine replay + filter grid search
uv run python scripts/rugcheck_validate.py [--sweep N]  # anti-rug gates vs live-rug evidence
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
| `paper_trader`   | **1 POS × 0.2 SOL** gate; first buy; **TP 4× / SL 0.3× / 25min timeout / trail 2×50%**; 2-sided + stability quote gates; fail-closed; dead-pool writeoffs; checkpoint |
| `pool_check`     | arm-time DexPaprika liquidity + Helius dev-rep gates (fail-closed)      |
| `jupiter_swap`   | **5% buy/sell impact + 3×300ms stability** rug defense; `SELL_BACKOFF 60s` reliable sells |
| `logs`           | writes `bot_logs/` (bot.log, journal.json, trade_log.csv, .stop)        |
| `notifier`       | Telegram cards + `/start /stop /status /help`; no-op without BOT_TOKEN  |
| `models`/`parser`| `Signal`/`Position` dataclasses + message parsing                       |

Data pipeline: Telegram events → filter → fail-closed pool gates → Jupiter quote gate → arm → fresh entry quote → pumpapi price → position exit (TP / trailing stop / SL / timeout, with dead-pool writeoffs). Paper mode never places real orders.