# 🕵️ Smart-Watch — smart-money follower for Solana

Tracks **262 quality-filtered smart-money wallets** — KOLs plus the top
Solana traders pulled from the SolanaTracker PnL V2 leaderboard (ranked by
realised PnL / win rate, arbitrage + one-hit wonders filtered out) — and
alerts the moment any of them buys something new.
Every alert opens a **shadow paper position** that rides a trailing stop, so
the strategy proves (or disproves) itself with real numbers before you ever
risk a cent.

```
DeBot tier/rank ──► Moralis Solana swaps (launch-window buyers)
        │                     │
        └────── smart_money_wallets.json (262 wallets)
                              │
   Tatum push webhooks ◄──────┴──► 45s Helius/Moralis poll fallback
        │                                 │
        ▼                                 ▼
   🕵️/🔥 Telegram alerts ──► shadow paper book (trail 40% · hard −50%)
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
| `uv run wallet_perf.py` | rank every tracked wallet by PnL / win rate (SolanaTracker PnL V2) |
| `uv run discover_wallets.py` | expand the watchlist from the SolanaTracker leaderboard API (quality-filtered top traders) |
| `uv run dexscreener_kol.py --mode gainers --limit 100` | headless scrape Top-Gainers page-1 holders + KOLs |

## Strategy: data-driven weighted consensus

The watcher no longer treats every tracked wallet equally. Each wallet is
weighted by its **real trading performance** — win rate + total PnL, pulled
from SolanaTracker PnL V2 into `wallet_performance.json` via `wallet_perf.py`.
When several wallets buy the same token, their weights are summed:

- **weight 0** — low-win-rate "noise" wallets (<40% win) are pruned from the
  watchlist entirely, so they never generate a signal or log line.
- **weight 1.0–1.5** — proven winners (≥60% win, >$1M PnL) count the most.
- a token **fires** (🔥 consensus + paper open) only once the summed weight of
  **≥2 distinct buying wallets** reaches `CONSENSUS_WEIGHT_THRESHOLD` (default
  `1.5`, ≈ a strong proven winner [≥60% win, weight ≥1.0] + any second tracked
  wallet). `REQUIRE_STRONG_WALLET=true` enforces that at least one contributor
  carries a real edge, so two mediocre wallets can never manufacture a signal on
  their own. A single wallet — even a top winner — can **never** open alone.

The smart wallets buy fresh, low-liquidity pumps, so the paper book opens on
tokens with `OPEN_MIN_LIQ_USD` ≥ $1500 and, when Jupiter has no route for a
brand-new pair, falls back to the DexScreener mark price (flagged) instead of
dropping a genuine consensus signal.

This makes the bot more robust (weak/noise wallets filtered out) and more
profitable (only high-conviction, multi-wallet consensus trades), while keeping
the liquidity and scale-out safeguards from the shadow book.

### Building / refreshing the wallet list

- `uv run discover_wallets.py` — pull quality-filtered top traders from the
  SolanaTracker `/v2/pnl/leaderboard/top` API (win rate / trade-count / one-hit
  filters) and merge them into `smart_money_wallets.json`.
- `uv run wallet_perf.py` — score every wallet in `smart_money_wallets.json`
  (PnL, win rate, ROI, trades) → `wallet_performance.json` +
  `wallet_performance.ranked.md`.
- `uv run dexscreener_kol.py --mode gainers --limit 100` — headless-Chromium
  scrape of the Top-Gainers page-1 tokens, capturing top holders + KOL/Top-Trader
  wallets per token → `gainers_discovery.json`.

## Manual trading flow

```bash
uv run main.py wallet-show                       # throwaway address + balance
# fund it with ~0.1 SOL from your main wallet, then:
uv run main.py sim <CA>                          # instant-exit cost check
uv run main.py sim <CA> --size 0.05 --live --yes # real buy → sell round-trip
```

Live mode is capped at **0.2 SOL** per trade and requires explicit `--yes`.

## Running 24/7 (OxMgr)

```bash
oxmgr apply ./oxfile.toml        # supervised, auto-restart, health-checked
oxmgr status watcher             # or: oxmgr logs watcher -f
```

Health check restarts the app if `bot_logs/watcher.log` goes stale >6 min.

## Configuration

Everything lives in `.env` (template: `.env.example`):

- Telegram notify: `BOT_TOKEN`, `CHAT_ID`
- Data: `HELIUS_API_KEYS`, `TATUM_API_KEY`, `DEXSCREENER_RPM=300`
- Watcher: `WATCH_POLL_S`, `WATCH_MIN_BUY_USD`, `WATCH_CONSENSUS_WALLETS`,
  `WATCH_WEBHOOK_URL` (+ port) for Tatum push
- Wallet-quality weighting: `WALLET_PERF_PATH`, `WALLET_WEIGHT_FLOOR_WIN=0.40`,
  `WALLET_WEIGHT_FULL_WIN=0.60`, `WALLET_PNL_TIER1=1000000`, `WALLET_PNL_TIER2=5000000`,
  `WALLET_WEIGHT_TIER1_MULT=1.25`, `WALLET_WEIGHT_TIER2_MULT=1.5`,
  `WALLET_DEFAULT_WEIGHT=0.5`, `WALLET_WEIGHT_MAX=2.0`, `CONSENSUS_WEIGHT_THRESHOLD=1.5`,
  `REQUIRE_STRONG_WALLET=true`, `OPEN_MIN_LIQ_USD=1500`,
  `OPEN_MIN_WALLETS=2`
- Shadow book: `SIZE_SOL`, `TP_LADDER=1.3:0.4,1.8:0.3,3.0:0.3`,
  `TRAIL_RETRACE_PCT=0.25`, `HARD_STOP_PCT=0.25`, `MAX_HOLD_H=24`,
  `MAX_OPEN_POSITIONS=18`, `PER_WALLET_MAX_POSITIONS=3` (caps correlated bets per wallet),
  `OPEN_MIN_LIQ_USD=1500`, `START_BALANCE_SOL`, `STATUS_EVERY_MIN`,
  `OPEN_MIN_H1_PCT=0.0` (only enter tokens with a 1h uptrend; skip tops/flat),
  `OPEN_MAX_M5_DUMP_PCT=-5.0` (skip tokens dumping >5% in the last 5m at the signal),
  `MTF_ALIGN_BONUS=0.3` (multi-timeframe alignment score modifier: trend-shaped tokens get a bonus, reversing/late ones a discount),
  `DBOTX_SAFETY=1` + `DBOTX_API_KEY` (fail-open rug filter: skips tokens with an active mint/freeze authority or top-10 holding > `DBOTX_TOP10_MAX`)
- Pair-aware penalty: `PAIR_PERF_FILE=pair_performance.json` (adaptive per-wallet-PAIR
  expectancy; `seed_pair_perf.py` seeds it from a past journal so known-losing pairs like
  AgmLJ+kEFiA are blocked from the first run), `OPEN_MAX_IMPACT_PCT=4.0` (don't open when
  Jupiter buy-side impact is too high), `EARLY_EXIT_WINDOW_S=120` + `EARLY_EXIT_DROP_PCT=0.30`
  (fast-rug guard: bail on positions that collapse >30% in the first 2 min)
- SolanaTracker (wallet PnL ranking): `SOLTRACKER_BASE_URL`, `SOLTRACKER_API_KEY`

## State & logs

- `shadow_book.json` — virtual positions/closed trades (the scorecard)
- `watcher_state.json` — per-wallet last-seen signatures
- `watched_tokens.json` — every alerted CA
- `bot_logs/watcher.log`, `journal.jsonl` events: `smart_buy`, `partial_tp`,
  `shadow_close`, …

> Historical note: this repo previously hosted a full signal-channel sniper
> stack (DRBTSolanaPF listener, pumpapi feed, rugcheck/pool gates). That
> strategy is retired; browse git history (< `aae60b2`) if you need it.
