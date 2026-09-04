# Smart-Watch — Solana trading bot

Two modes:

1. **TG-first** (`tg-trade`) — Listens to @gmgnsignals Telegram channel in real-time, filters signals, buys via Jupiter, tracks positions with trailing stops.
2. **Watch** (`watch`) — Tracks KOL/smart-money wallets, opens positions on consensus buys.

## Architecture (TG-first)

```
@GMGNsignals (Telegram)
      |
      v
TgSignalFeed (Telethon real-time events)
      |
      v
parse_tg_signal() — extract CA, MC, liq, holders
      |
      v
Quality gates — reject if MC < $5K, liq < $1K, holders < 10
      |
      v
DexScreener — fetch live price, liquidity, 1h change
      |
      v
Secondary gates — reject if dumping (-15% h1)
      |
      v
JupiterSwap — quote + execute buy (paper or live)
      |
      v
PositionManager — track prices, enforce exits
      |
      ├── Hard stop: -25% from entry
      ├── Trailing stop: -35% from peak
      ├── TP ladder: +30% / +80% / +200%
      └── Max hold: 24h
```

## Project structure

```
main.py                  # entry point: watch / tg-trade / sim / wallet-new / status
main_tg.py               # TG-first trader (PositionManager, signal handler)
src/
  config.py              # .env parser + Settings dataclass
  tg_signal_feed.py      # Telegram @gmgnsignals listener (real-time events)
  dexscreener.py         # DexScreener REST oracle
  jupiter_swap.py        # Jupiter Swap V2 client
  notifier.py            # Telegram notifications
  watcher.py             # smart-wallet watcher (watch mode)
  wallet_weights.py      # wallet-quality weights (watch mode)
  pair_perf.py           # adaptive pair-quality multiplier (watch mode)
  dbotx.py               # DBotX fail-open rug filter (watch mode)
  pump_stream.py         # pumpapi.io WebSocket firehose (watch mode)
  tatum_notify.py        # Tatum push subscriptions (watch mode)
  wallet_discovery.py    # batch wallet discovery (watch mode)
  soltracker.py          # SolanaTracker API client (watch mode)
  logs.py                # logging + journal (JSONL)
scripts/
  discover_wallets.py    # expand watchlist from SolanaTracker leaderboard
  wallet_perf.py         # rank wallets by PnL / win rate
  seed_pair_perf.py      # seed pair_performance.json from past journal
  dexscreener_kol.py     # headless scrape Top-Gainers for KOL wallets
  gen_wallets_from_replay.py  # generate wallet candidates from parquet
backtests/
  backtest_consensus.py  # wallet-consensus strategy backtest
  backtest_ideal.py      # upper-bound test with perfect wallet list
  backtest_v2.py         # sweep exit ladders x consensus x wallet-quality
tests/
  test_watcher_core.py   # unit tests: Shyft tx parsing + status card
```

## Commands

| Command | What it does |
|---|---|
| `uv run main.py tg-trade` | TG-first trader (real-time signals + position tracking) |
| `uv run main.py watch` | KOL consensus watcher (wallet tracking + shadow book) |
| `uv run main.py sim <CA>` | Jupiter round-trip quote check (paper) |
| `uv run main.py sim <CA> --live --yes` | Execute real buy+sell on throwaway wallet |
| `uv run main.py wallet-new` | Create throwaway trading wallet |
| `uv run main.py wallet-show` | Show throwaway address/balance |
| `uv run main.py status` | Print status card |
| `uv run main.py tatum-setup` | Register push subscriptions (watch mode) |
| `uv run scripts/wallet_perf.py` | Rank wallets by PnL / win rate |

## Configuration

Everything lives in `.env` (template: `.env.example`). Key groups:

- **Telegram signal feed**: `TG_API_ID`, `TG_API_HASH`, `TG_PHONE`, `TG_SESSION_NAME`, `TG_MIN_MC`, `TG_MIN_LIQ`, `TG_MIN_HOLDERS`
- **Position management**: `SIZE_SOL`, `TP_LADDER`, `TRAIL_RETRACE_PCT`, `HARD_STOP_PCT`, `MAX_HOLD_H`
- **Trading mode**: `DRY_RUN=true` (paper) / `DRY_RUN=false` (live)
- **Jupiter**: `JUPITER_API_KEY`, `JUPITER_SLIPPAGE_BPS`, `JUPITER_MAX_IMPACT_PCT`
- **Data providers**: `DEXSCREENER_BASE_URL`, `DEXSCREENER_RPM`, `HELIUS_API_KEYS`
- **Wallet weighting** (watch mode): `CONSENSUS_WEIGHT_THRESHOLD`, `REQUIRE_STRONG_WALLET`
- **Shadow book** (watch mode): `MAX_OPEN_POSITIONS`, `PER_WALLET_MAX_POSITIONS`

## State files

- `tg_positions.json` — open positions (TG-first mode)
- `tg_closed.json` — closed trade history
- `shadow_book.json` — virtual positions/closed trades (watch mode)
- `watcher_state.json` — per-wallet last-seen signatures (watch mode)
- `wallet_performance.json` — wallet PnL/win-rate data
- `pair_performance.json` — adaptive pair-quality multiplier store
- `bot_logs/bot.log` — runtime log
- `bot_logs/journal.jsonl` — structured event journal (JSONL)

## Running 24/7 (OxMgr)

```bash
oxmgr apply ./oxfile.toml        # supervised, auto-restart, health-checked
oxmgr status track-wallet        # or: oxmgr logs track-wallet -f
```
