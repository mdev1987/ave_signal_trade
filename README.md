# ave_signal_trade — Solana KOL-consensus trading bot

Track smart-money wallets, filter consensus buys, trade via Jupiter with trailing stops.
Paper mode (`DRY_RUN=true`) shadows every signal into `shadow_book.json` — no live
funds move. Current paper record needs repair: 63 trades, -0.18 SOL, 25% win
(2026-09-12). Sizes are cut to minimum until 30 post-fix trades turn positive.

## How it works

```
CabalSpy / Kolexplorer / MemeTracker / PumpAPI
       |
       v
Consensus engine — weighted wallet buys per token (needs strong wallet)
       |
       v
Gate filter — wallets, score, liq, momentum (m5≥0.5, h1≥2.0), safety stack
(DBotX pair_safety → RugCheck → Helius rugger → Vybe liq/top5 → CabalSpy
holder/bundle → Jupiter audit incl. 5m organic flow)
       |
       v
Adaptive sizing — quality × per-source multiplier
(pumpapi 0.5x, avesignalmonitor 0.6x, cabalspy 1.3x, else ≤1.0x; max 0.06 SOL)
       |
       v
Jupiter Swap — /swap/v2/order + /swap/v2/execute (simulate-first, RTSE buys)
       |
       v
Position tracker — TP ladder, trail, dead-token kill, oracle-fail tagging
       |
       ├── Dead token: peak <1.015 after 8min → force close
       ├── Hard stop: -25% from entry
       ├── Trailing stop: -15% from peak (arms at 1.3x)
       ├── Breakeven lock: arms at 1.15x, stop → entry
       ├── TP ladder: +20% (40%) / +80% (30%) / +200% (30%)
       ├── Early filter: >10% DD with <5% gain in first 30s → kill
       ├── Flat timeout: peak <1.05 after 1h → close
       ├── Max hold: 24h
       └── Oracle fail: both pricers down → close tagged, excluded from stats
```

## Running

```bash
uv run main.py watch                  # KOL consensus tracker (primary)
uv run main.py status                 # Print status card
```

## 24/7 via OxMgr

```bash
oxmgr rm track-wallet                 # full stop + remove (fresh start)
oxmgr apply ./oxfile.toml             # deploy / restart (TOML — daemon rejects YAML)
oxmgr logs track-wallet -f
```

Fresh start = archive `bot_logs/` + reset `shadow_book.json`, then the two
commands above. Health check watches `bot_logs/watcher.log -mmin -3`.

## Docs

`doc/` holds only verified notes: `dexscreener_api.md`, `dbot.llm.md`
(index), `pumpapi_doc.md`, `shyft_*.md`, plus `jupiter_tokens_search.md`
(audit + organic-flow fields) and `helius_transactionSubscribe.md`
(ATA expansion). Empty stubs were deleted 2026-09-12.

## Project structure

```
main.py                     # entry point + position management
src/
  config.py                 # .env parser + Settings dataclass
  jupiter_trade.py          # Jupiter Swap V2 + ResilientRPC + TokenClient
  tg_signal_feed.py         # Telegram @gmgnsignals listener (Telethon)
  dexscreener_oracle.py     # DexScreener REST wrapper
  kolexplorer.py            # Kolexplorer KOL token monitor
  cabalspy.py               # CabalSpy KOL wallet WebSocket
  helius_ws.py              # Helius WebSocket with key rotation
  helius_client.py          # Helius REST API client
  pump_stream.py            # pumpapi.io WebSocket firehose
  vybe.py                   # Vybe token data API
  rugcheck.py               # RugCheck rug detection
  dbotx.py                  # DBotX fail-open rug filter
  wallet_weights.py         # wallet-quality weighting
  pair_perf.py              # adaptive pair-quality multiplier
  wallet_discovery.py       # batch wallet discovery
  notifier.py               # Telegram notifications
  tatum_notify.py           # Tatum push subscriptions
  logs.py                   # logging + journal (JSONL)
scripts/
  discover_wallets.py       # expand watchlist from SolanaTracker
  wallet_perf.py            # rank wallets by PnL / win rate
  recalc_wallet_weights.py  # recalculate wallet scores
  seed_pair_perf.py         # seed pair_performance.json from journal
  dexscreener_kol.py        # scrape KOL wallets from DexScreener
  gen_wallets_from_replay.py # generate wallet candidates from parquet
backtests/
  backtest_consensus.py     # wallet-consensus strategy backtest
  backtest_ideal.py         # upper-bound test with perfect wallet list
  backtest_v2.py            # sweep exit ladders x consensus x wallet-quality
  backtest_excursion.py     # price excursion analysis
  backtest_live.py          # replay live trades
tests/
  test_watcher_core.py      # unit tests
```

## Configuration

All config in `.env` (template: `.env.example`). Key groups:

| Group | Key params |
|-------|-----------|
| **Wallets** | `CONSENSUS_WEIGHT_THRESHOLD`, `OPEN_MIN_WALLETS`, `WALLET_PERF_PATH` |
| **Entry** | `OPEN_MAX_IMPACT_PCT`, `OPEN_MIN_H1_PCT`, `OPEN_MIN_M5_PCT`, `EARLY_FILTER_*` |
| **Sizing** | `ADAPTIVE_SIZING`, `SIZE_SOL`, `SIZE_SOL_MIN/MAX` |
| **Exit** | `TP_LADDER`, `TRAIL_RETRACE_PCT`, `HARD_STOP_PCT`, `FLAT_TIMEOUT_H`, `MAX_HOLD_H` |
| **Risk** | `MAX_OPEN_POSITIONS`, `PER_WALLET_MAX_POSITIONS`, `REENTRY_COOLDOWN_S` |
| **Trading** | `DRY_RUN`, `JUPITER_SLIPPAGE_BPS`, `PRIVATE_KEY` |
| **Data** | `HELIUS_API_KEYS`, `SHYFT_API_KEY`, `CABALSPY_API_KEY` |

## State files

| File | Description |
|------|-------------|
| `shadow_book.json` | Virtual positions + closed trades |
| `wallet_performance.json` | Per-wallet PnL / win rate |
| `smart_money_wallets.json` | Tracked wallet addresses |
| `pair_performance.json` | Adaptive pair-quality store |
| `watcher_state.json` | Per-wallet last-seen signatures |
| `bot_logs/watcher.log` | Runtime log |
| `bot_logs/journal.json` | Structured event journal |

## Key tuning parameters

| Parameter | Current | Effect |
|-----------|---------|--------|
| `CONSENSUS_WEIGHT_THRESHOLD` | 1.8 | Higher = fewer but stronger entries |
| `OPEN_MIN_WALLETS` | 2 | Min wallets before opening |
| `OPEN_MIN_H1_PCT` | 2.0 | Require 1h uptrend (no knife-catching) |
| `OPEN_MIN_M5_PCT` | 0.5 | Require positive 5m momentum |
| `HARD_STOP_PCT` | 0.25 | Max loss per trade |
| `TRAIL_START_MULT` | 1.3 | Trail arms earlier to lock winners |
| `TRAIL_RETRACE_PCT` | 0.15 | Trail sensitivity (tighter = lock gains faster) |
| `EARLY_FILTER_DD_PCT` | 10.0 | Early drawdown kill threshold |
| `FLAT_TIMEOUT_H` | 1 | Free dead slots fast (was 2h tail bleed) |
| `SIZE_SOL` | 0.025 | Minimum until edge proven |
| `ADAPTIVE_SIZING` | true | Quality × per-source multiplier |

## Dependencies

- Python 3.11+
- `solana-rpc-resilient` — Solana RPC with retry + key rotation
- `jupiter-swap-python` — Jupiter DEX swap client
- `dexscreener-python` — DexScreener API wrapper
- `telethon` — Telegram client for signal feeds
- `websockets` — WebSocket clients for CabalSpy, pumpapi
- `requests` — HTTP client for RugCheck, DBotX, Kolexplorer
