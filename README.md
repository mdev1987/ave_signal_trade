# ave_signal_trade — Solana KOL-consensus trading bot

Track smart-money wallets, filter consensus buys, trade via Jupiter with trailing stops.

## How it works

```
CabalSpy / Kolexplorer / MemeTracker
       |
       v
Consensus engine — aggregate wallet buys per token
       |
       v
Gate filter — min wallets, min score, early DD, stable symbol
       |
       v
Adaptive sizing — scale by wallet quality + source boost
       |
       v
Jupiter Swap — quote + execute (simulate-first)
       |
       v
Position tracker — trailing stop, TP ladder, dead-token kill
       |
       ├── Dead token: peak <1.015 after 5min → force close
       ├── Quick bleed: peak <1.03 within 30min → force close
       ├── Hard stop: -30% from entry
       ├── Trailing stop: -15% from peak (activates at 1.4x)
       ├── Breakeven lock: after 1st TP, stop moves to entry
       ├── TP ladder: +20% (40%) / +80% (30%) / +200% (30%)
       ├── Flat timeout: no price update for 2h
       └── Max hold: 24h
```

## Running

```bash
uv run main.py track-wallet          # KOL consensus tracker (primary)
uv run main.py tg-trade              # TG signal trader (disabled)
uv run main.py status                # Print status card
```

## 24/7 via OxMgr

```bash
oxmgr apply ./oxfile.toml           # deploy / restart
oxmgr stop track-wallet
oxmgr logs track-wallet -f
```

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
| `CONSENSUS_WEIGHT_THRESHOLD` | 2.0 | Higher = fewer but stronger entries |
| `OPEN_MIN_WALLETS` | 3 | Min wallets before opening |
| `HARD_STOP_PCT` | 0.30 | Max loss per trade |
| `TRAIL_RETRACE_PCT` | 0.15 | Trail sensitivity (tighter = lock gains faster) |
| `EARLY_FILTER_DD_PCT` | 10.0 | Early drawdown kill threshold |
| `FLAT_TIMEOUT_H` | 2 | Force close if no price updates |
| `ADAPTIVE_SIZING` | true | Scale size by wallet quality |

## Dependencies

- Python 3.11+
- `solana-rpc-resilient` — Solana RPC with retry + key rotation
- `jupiter-swap-python` — Jupiter DEX swap client
- `dexscreener-python` — DexScreener API wrapper
- `telethon` — Telegram client for signal feeds
- `websockets` — WebSocket clients for CabalSpy, pumpapi
- `requests` — HTTP client for RugCheck, DBotX, Kolexplorer
