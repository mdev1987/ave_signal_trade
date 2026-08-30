# Smart-Watch — smart-money follower for Solana

Tracks **quality-filtered smart-money wallets** — KOLs plus top Solana
traders from the SolanaTracker PnL V2 leaderboard — and alerts the moment
any of them buys something new. Every alert opens a **shadow paper
position** that rides a trailing stop, so the strategy proves (or disproves)
itself with real numbers before you ever risk a cent.

```
DeBot tier/rank --> Moralis Solana swaps (launch-window buyers)
        |                     |
        +------ smart_money_wallets.json
                               |
   Tatum push webhooks <-------+--> 45s Shyft poll fallback
         |                                 |
         v                                 v
   Telegram alerts --> shadow paper book (trailing stop + TP ladder)
```

## Project structure

```
main.py                  # entry point: watch / status / sim / wallet-new
src/
  config.py              # .env parser + Settings dataclass
  watcher.py             # smart-wallet watcher (consensus scoring)
  pair_perf.py           # adaptive pair-quality multiplier
  wallet_weights.py      # wallet-quality weights (Bayesian confidence)
  dexscreener.py         # DexScreener REST oracle
  dbotx.py               # DBotX fail-open rug filter
  jupiter_swap.py        # Jupiter Swap V2 client
  notifier.py            # Telegram notifications
  pump_stream.py         # pumpapi.io WebSocket firehose
  wallet_discovery.py    # batch wallet discovery
  logs.py                # logging + journal (JSONL)
  tatum_notify.py        # Tatum push subscriptions
  debot.py               # DeBot.ai community-signal client
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
| `uv run main.py watch` | run the 24/7 watcher (alerts + shadow book + status cards) |
| `uv run main.py tatum-setup` | register push subscriptions for all tracked wallets |
| `uv run main.py status` | print the status card on demand |
| `uv run main.py sim <CA> [--size X]` | Jupiter round-trip quote check (paper) |
| `uv run main.py sim <CA> --size 0.05 --live --yes` | execute real buy+sell on the throwaway wallet |
| `uv run main.py wallet-new` / `wallet-show` | create/inspect throwaway trading wallet |
| `uv run scripts/wallet_perf.py` | rank every tracked wallet by PnL / win rate |
| `uv run scripts/discover_wallets.py` | expand the watchlist from SolanaTracker leaderboard |
| `uv run scripts/seed_pair_perf.py` | seed `pair_performance.json` from past journal entries |

## Strategy

### Weighted consensus

Each wallet is weighted by its **real trading performance** — win rate + total
PnL from SolanaTracker. When several wallets buy the same token, their weights
are summed:

- **weight 0** — low-win-rate noise wallets (<40% win) are pruned entirely
- **weight 1.0-1.5** — proven winners (>=60% win, >$1M PnL) count the most
- A token **fires** only when the summed weight of **>=2 distinct wallets**
  reaches `CONSENSUS_WEIGHT_THRESHOLD` (1.5). `REQUIRE_STRONG_WALLET=true`
  enforces at least one proven winner, so two mediocre wallets can never
  manufacture a signal.

### Open gate pipeline

Every consensus signal passes through a multi-stage gate before opening a
shadow position:

1. **Per-wallet cap** — skip if >= `PER_WALLET_MAX_POSITIONS` open positions
   already share a wallet (kills correlated stacks)
2. **Liquidity check** — minimum pool liquidity in USD
3. **Momentum filters** — 1h uptrend required (`OPEN_MIN_H1_PCT`), no 5m
   dumps (`OPEN_MAX_M5_DUMP_PCT`)
4. **Multi-timeframe alignment** — trend-shaped tokens (all 4 TFs agree)
   get `MTF_ALIGN_BONUS` added to their score
5. **Pair-quality multiplier** — adaptive per-wallet-pair multiplier based
   on rolling trade history. Known-losing pairs (e.g. AgmLJ+kEFiA) get
   0.5x; unknown pairs stay at 1.0
6. **DBotX safety** — fail-open rug filter checking mint/freeze authority
   and top-10 holder concentration
7. **Jupiter impact guard** — skip if buy-side slippage exceeds
   `OPEN_MAX_IMPACT_PCT`

### Shadow book exit logic

- **TP ladder** — scale-out at +30% / +80% / +200% (configurable)
- **Trailing stop** — after peak >= 1.3x, exit on 35% retrace from peak
- **Hard stop** — -25% from entry
- **Early adverse filter** — one-shot at 30s: reject if drawdown >20% AND gain <5%
- **Timeout** — force-close after 24h

## Running 24/7 (OxMgr)

```bash
oxmgr apply ./oxfile.toml        # supervised, auto-restart, health-checked
oxmgr status watcher             # or: oxmgr logs watcher -f
```

Health check restarts the app if `bot_logs/watcher.log` goes stale >6 min.

## Configuration

Everything lives in `.env` (template: `.env.example`). Key groups:

- **Telegram**: `BOT_TOKEN`, `CHAT_ID`
- **Data providers**: `HELIUS_API_KEYS`, `SHYFT_API_KEY`, `TATUM_API_KEY`,
  `SOLTRACKER_API_KEY`
- **Wallet weighting**: `WALLET_WEIGHT_FLOOR_WIN`, `WALLET_WEIGHT_FULL_WIN`,
  `CONSENSUS_WEIGHT_THRESHOLD`, `REQUIRE_STRONG_WALLET`
- **Shadow book**: `SIZE_SOL`, `TP_LADDER`, `TRAIL_*`, `HARD_STOP_PCT`,
  `MAX_OPEN_POSITIONS`, `PER_WALLET_MAX_POSITIONS`
- **Open gates**: `OPEN_MIN_LIQ_USD`, `OPEN_MAX_IMPACT_PCT`, `OPEN_MIN_H1_PCT`,
  `OPEN_MAX_M5_DUMP_PCT`, `MTF_ALIGN_BONUS`
- **Pair quality**: `PAIR_PERF_FILE`, `scripts/seed_pair_perf.py`
- **DBotX safety**: `DBOTX_API_KEY`, `DBOTX_SAFETY`, `DBOTX_TOP10_MAX`
- **Early adverse filter**: `EARLY_FILTER_WINDOW_S`, `EARLY_FILTER_DD_PCT`, `EARLY_FILTER_GAIN_PCT`

## State & logs

- `shadow_book.json` — virtual positions/closed trades (the scorecard)
- `watcher_state.json` — per-wallet last-seen signatures
- `bot_logs/watcher.log` — runtime log
- `bot_logs/journal.jsonl` — structured event journal (JSONL)
- `pair_performance.json` — adaptive pair-quality multiplier store
- `wallet_performance.json` — wallet PnL/win-rate data
