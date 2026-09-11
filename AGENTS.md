# AGENTS.md — ave_signal_trade

## Project Overview

Solana KOL-consensus trading bot that tracks smart-money wallets, aggregates consensus buy signals, and trades via Jupiter with trailing stops.

## Architecture

```
CabalSpy / Kolexplorer / MemeTracker / PumpAPI
       |
       v
Consensus engine — aggregate wallet buys per token
       |
       v
Gate filter — min wallets, min score, early DD, stable symbol, safety checks
       |
       v
Adaptive sizing — scale by wallet quality + source boost
       |
       v
Jupiter Swap — /swap/v2/order + /swap/v2/execute (managed landing)
       |
       v
Position tracker — trailing stop, TP ladder, dead-token kill
```

## Running Commands

```bash
# Primary: KOL consensus tracker (24/7)
uv run main.py watch

# Deploy/restart via OxMgr
oxmgr apply ./oxfile.toml

# Status card
uv run main.py status

# Manual tests
uv run python -c "from src.jupiter_trade import JupiterSwap; ..."
```

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point, signal routing, open/close logic |
| `src/jupiter_trade.py` | Jupiter Swap V2 client, PumpAPI fallback, token audit |
| `src/cabalspy.py` | CabalSpy KOL wallet WebSocket |
| `src/helius_ws.py` | Helius WebSocket with key rotation |
| `src/pump_stream.py` | PumpAPI WebSocket firehose |
| `src/watcher.py` | Smart wallet watcher |
| `src/dexscreener_oracle.py` | DexScreener REST wrapper |
| `src/dexpaprika.py` | DexPaprika fallback (new) |
| `src/dbotx.py` | DBotX safety + hot/new tokens feeds |
| `src/rugcheck.py` | RugCheck rug detection |
| `src/vybe.py` | Vybe token data API |
| `src/config.py` | .env parser + Settings dataclass |
| `src/wallet_weights.py` | Wallet-quality weighting |
| `src/pair_perf.py` | Adaptive pair-quality multiplier |

## Safety Gates (applied in order)

1. **DBotX** — mint/freeze authority, top-10 concentration, dev position
2. **RugCheck** — rug score, danger flags
3. **Helius** — deployer rugger check, top-10 holder concentration
4. **Vybe** — liquidity, top holder concentration, buy/sell ratio
5. **CabalSpy** — holder concentration, bundle detection
6. **Jupiter Token Audit** — mint/freeze authority, organic score, holder count (NEW)
7. **Multi-timeframe alignment** — momentum + pair quality

## Exit Rules

- Dead token: peak <1.015 after 5min → force close
- Quick bleed: peak <1.03 within 30min → force close
- Hard stop: -30% from entry
- Trailing stop: -15% from peak (activates at 1.4x)
- Breakeven lock: after 1st TP, stop moves to entry
- TP ladder: configurable (default: +20%/+80%/+200%)
- Flat timeout: no price update for 2h
- Max hold: 24h

## Key Config Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `CONSENSUS_WEIGHT_THRESHOLD` | 1.5 | Higher = fewer but stronger entries |
| `OPEN_MIN_WALLETS` | 2 | Min wallets before opening |
| `HARD_STOP_PCT` | 0.30 | Max loss per trade |
| `TRAIL_RETRACE_PCT` | 0.15 | Trail sensitivity |
| `ADAPTIVE_SIZING` | true | Scale size by wallet quality |
| `JUP_AUDIT_ENABLED` | true | Jupiter token audit pre-trade |

## API Keys Required

- **Helius** (4 keys) — RPC + WebSocket
- **CabalSpy** (2 keys) — KOL wallet streams
- **Jupiter** — Swap API + token audit
- **DBotX** — Safety checks + hot/new tokens
- **RugCheck** — Rug detection
- **Vybe** — Token data + liquidity
- **PumpAPI** — Bonding curve fallback + firehose

## Testing

```bash
# Compile check
uv run python -c "import py_compile; py_compile.compile('main.py', doraise=True)"

# Lint
uv run ruff check .

# Run in paper mode (default)
DRY_RUN=true uv run main.py watch
```

## Deployment

```bash
# Via OxMgr (production)
oxmgr apply ./oxfile.toml

# Manual restart
oxmgr restart track-wallet

# Check logs
oxmgr logs track-wallet
tail -f bot_logs/watcher.log
```
