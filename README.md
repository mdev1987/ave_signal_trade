# 🕵️ Smart-Watch — smart-money follower for Solana

Tracks **30 discovered smart-money wallets** (extracted from 18 tier-ranked
pumping tokens) and alerts the moment any of them buys something new.
Every alert opens a **shadow paper position** that rides a trailing stop, so
the strategy proves (or disproves) itself with real numbers before you ever
risk a cent.

```
DeBot tier/rank ──► Moralis Solana swaps (launch-window buyers)
        │                     │
        └────── smart_money_wallets.json (30 wallets)
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
- Data: `HELIUS_API_KEYS`, `MORALIS_API_KEY`, `TATUM_API_KEY`, `DEXSCREENER_RPM=300`
- Watcher: `WATCH_POLL_S`, `WATCH_MIN_BUY_USD`, `WATCH_CONSENSUS_WALLETS`,
  `WATCH_WEBHOOK_URL` (+ port) for Tatum push
- Shadow book: `SIZE_SOL`, `TRAIL_RETRACE_PCT=0.40`, `HARD_STOP_PCT=0.50`,
  `START_BALANCE_SOL`, `STATUS_EVERY_MIN`

## State & logs

- `shadow_book.json` — virtual positions/closed trades (the scorecard)
- `watcher_state.json` — per-wallet last-seen signatures
- `watched_tokens.json` — every alerted CA
- `bot_logs/watcher.log`, `journal.jsonl` events: `smart_buy`, `partial_tp`,
  `shadow_close`, …

> Historical note: this repo previously hosted a full signal-channel sniper
> stack (DRBTSolanaPF listener, pumpapi feed, rugcheck/pool gates). That
> strategy is retired; browse git history (< `aae60b2`) if you need it.
