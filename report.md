# Dual-Channel Signal Run Report — 2026-08-24

**Scope:** First 30-minute live run after switching signal sources from `@AveSolanaTokenScanner` (removed) to **@DRBTSolanaPF** (preferred) + **@SOLTRENDING**.
**Mode:** PAPER (`DRY_RUN=true`, no private key). Size 0.05 SOL, max 3 positions, TP 4× / SL 0.3× / TTL 25 min.
**Window:** 05:07:37 – 05:37:37 local (30.0 min). Sources: `bot_logs/bot.log`, `bot_logs/journal.json` (scoped to this run; the journal file is append-across-runs), fresh channel pulls for attribution, DexScreener API for cross-checks.

---

## 1. Headline

| Metric | Value |
|---|---|
| Unique tokens processed | **134** (134 signal events, 0 duplicates → dedup verified) |
| Filter rejects | **51** — 45 serial-relaunch damper, 6 mcap-band |
| Gate skips at offer time | **133** — 102 *no pool found* (DexPaprika, fail-closed), 26 low liq, 5 liq $0 |
| RugCheck evaluations | 1 (clean) |
| **Armed** | **1** — “Rick” via SOLTRENDING |
| Entries / closed trades | 0 (no PumpAPI buy event observed before shutdown) |
| Errors / crashes | 0 (no ERROR/CRITICAL/traceback; no HTTP 429s; graceful SIGTERM exit 0) |

Funnel: **134 signals → 83 rejected/skipped pre-gate → 51 pool/liquidity vetoes → 1 armed → 0 entered.**

---

## 2. Channel comparison

| | @DRBTSolanaPF | @SOLTRENDING |
|---|---|---|
| Nature | New pump.fun launch feed (pre-bonding curve mints) | Buy/momentum alerts on already-trading tokens |
| Msgs in window (attribution pull) | 243 | 473 |
| Unique CAs | 214 (**~1 post per token**) | **20** (~24 reposts per token!) |
| Fields available to parser | name, CA (`Mint:`), nothing else | title symbol, CA (jup.ag Buy href), `Market Cap $` |
| Message-metadata filters that apply | none available → skipped by design; dex inferred `Pumpfunamm` from `…pump` mint suffix | mcap band applies ($150K–$15M ⇒ rejected under current ceiling) |
| Attributed share of bot’s unique signals | ~99% (159/160 matched) | ~1% — “Rick” only |
| Outcome in this run | dominated gate-skips (curve tokens have no external pool yet) | 33 mcap rejections earlier in journal + the run’s single ARM |

**Overlap:** zero CAs were posted by both channels during the window — they are complementary feeds, not redundant ones. Preference ordering (DRBT first) therefore never had to break a tie.

**Key structural finding:** DRBTSolanaPF is the volume source (~7 launches/min) but its tokens are minutes-old curve mints: DexPaprika finds **no external pool** for most, so the fail-closed liquidity gate (MIN_LIQUIDITY_USD=$5000) vetoed 102/133 skips. Under the current calibration DRBTSolanaPF effectively *cannot* trade until a token graduates to PumpSwap with ≥$5K pooled liquidity — which is exactly what happened for the single ARM.

---

## 3. The one ARM — “Rick” (SOLTRENDING)

- CA `Dm5A8Bniqh7jM5eNDHc24C3siuPSLoVasht2YrVTpump`, armed 05:28:40 at message-reported mcap **$44,341** (inside the active L2_EXPERIMENT band $2.5K–$50K from `.env`).
- Passed the full stack: base filter → scam damper → DexPaprika pool check (real pool found) → dev-rep → **RugCheck clean** (no lp-unlocked / mint-authority / freeze-authority).
- Never entered: the trader waits for a PumpAPI buy event on the mint as the entry trigger; none arrived before the 30-min cutoff.
- **DexScreener cross-check (post-run):** PumpSwap pair live, liquidity **$17.5K**, 24 h volume **$615K**, price now ≈ $0.0000516 (≈ $51K mcap, ~+16% above arm-time level). A legitimately tradeable setup — the pipeline surfaced exactly the kind of token it should.
- Note: this proves SOLTRENDING can produce tradable signals *below* the $50K ceiling; its $150K+ alerts remain log-only by your explicit choice (keep current band).

---

## 4. Pipeline behavior verified

- **Multi-channel plumbing:** backfill pulled 200 msgs/channel (“backfill: 400 signals”), realtime streamed both channels concurrently; startup card lists both channels.
- **Parser adaptation:** 100 % parse-to-CA on both formats (DRBT `Name | TICKER` + `Mint:`; SOLTRENDING title + jup.ag href + Market Cap).
- **Filter semantics:** absent metadata (mcap=0/snipes=None/dex="") skips the corresponding rule instead of auto-rejecting — confirmed by DRBT signals reaching offer.
- **Scam damper earned its keep:** 45 rejects were serial-relaunch farms (“The Heroic Father” alone reached 11 CAs sharing a name within the window).
- **Stale-signal guard:** backfilled signals older than `ENTRY_MAX_AGE_S=300` skipped (285 in the startup burst of this run’s log segment).

---

## 5. Reference docs consulted

- **Local `docs/docs`** (symlink to bot_plan): pumpapi stream docs (event types incl. buys/pool state used for entry triggers & vetoes), Jupiter `/order`+`/execute` notes, RugCheck risk model, pump.fun strategy primer (only ~1 % of launches graduate; sniping is highest-risk tier — consistent with our gate-heavy posture).
- **Jupiter MCP:** confirms the bot’s buy path — `/order` without optional params runs **mode “ultra”** with automatic **RTSE** slippage and all routers (Metis/JupiterZ/Dflow/OKX); any optional param flips to “manual” and may restrict routing. Matches `JUPITER_ORDER_RTSE` design; sells correctly keep explicit slippage for execution certainty.
- **Helius MCP:** free-plan wallet reads (`getTokenAccountsByOwner`, `getAccountInfo`) and 429 handling match the bot’s rotation/cooldown config; `getTokenAccountsByOwnerV2` pagination exists if position reconciliation ever needs it.
- **DEX Screener MCP:** `GET /token-pairs/v1/solana/{mint}` returns per-pair liquidity/volume/txns — used here as an independent cross-check of Rick (values above); viable as a future secondary liquidity oracle alongside DexPaprika.

---

## 6. Findings & recommendations

1. **Calibration gap (main blocker):** the $5 000 DexPaprika liquidity floor + fail-closed “no pool” veto excludes virtually all DRBTSolanaPF curve-phase launches. If curve-sniping is desired, add a pump.fun-curve-aware branch (bonding-curve liquidity is mathematical, per local strategy doc) or lower the threshold for `…pump` mints younger than N minutes. Otherwise DRBTSolanaPF functions as a *graduation watchlist*, not a sniper feed.
2. **Journal the source channel:** signals aren’t tagged with their channel; attribution required post-hoc matching. Add `source` to `Signal` + journal rows (small change, big observability win).
3. **SOLTRENDING duplication:** 473 posts → 20 tokens; dedup handled it, but consider tracking repost velocity as a momentum feature rather than discarding it.
4. **Single-arm sample:** 1 arm / 0 trades in 30 min is expected for a 25-min-TTL serial system under tight gates; recommend a 6–24 h soak before judging entry conversion.
5. **Ops:** clean shutdown (exit 0), zero API errors, health watchdog idle — deployment-ready. Run under `setsid`/systemd so shell timeouts can’t kill it (lesson from this session).

---

## Addendum — Strategy v2: "catch the pump, dodge the rug" (implemented 2026-08-24)

Post-run scoring showed the signals were NOT junk — 4/133 skipped tokens did >+20% within the hour, and the two biggest (**WOFI** +~1380x after graduating to PumpSwap in the same minute we skipped it; **creator capital** +~1270x) were vetoed only because DexPaprika hadn't indexed their brand-new pools inside the 10s confirm window. Strategy v2 removes that race while keeping every rug defense:

### Changes
1. **New `src/dexscreener.py`** — rate-limited DexScreener REST oracle (docs: 60 req/min public tier; `DEXSCREENER_RPM` configurable for 300 rpm plans). Sliding-window limiter, best-pair normalization (liq/mcap/vol m5+h1/txns), all failures silent-None.
2. **`pool_check.py` → multi-oracle cascade** (`check_pool`):
   - Oracle 1 DexPaprika (unchanged; explicit liq < min still rejects outright).
   - Red-flag rug vetoes run FIRST in every iteration regardless of source: stream liquidity-removal <=120s, set mint/freeze authorities.
   - Oracle 2 **stream-curve admission**: fresh (<=90s) PumpAPI activity on a `...pump` mint passes — bonding-curve liquidity is mathematical pre-graduation, so an LP-pull rug is impossible at that phase.
   - Oracle 3 **DexScreener admission**: liquidity >= $5K, or curve mint with real activity (mcap + m5/h1 volume/trades).
   - All oracles silent => still FAIL-CLOSED ("liquidity check unavailable"). Non-`pump` mints never get curve fallback. Every decision journaled as `pool_oracle {src, ok, liq}`.
3. **Config knobs** (`config.py`, `.env.example`): `POOL_CURVE_FALLBACK=true`, `CURVE_STREAM_MAX_AGE_S=90`, `DEXSCREENER_ENABLED/BASE_URL/RPM=60`.

### Verification
- Gate unit checks: stream-admit ok, red-flag veto overrides fresh stream, stale-stream + silence fail-closed, non-pump no-fallback.
- Live replay of the run's winners: WOFI + creator capital now **PASS**; The Crypt/Yin-Yang correctly reject on *explicit* post-deflation liq ($2-3K) — a true negative, not a race loss.
- **Live soak (5.5 min):** 17 signals -> 17 oracle decisions (6 dexpaprika-rejects, 8 fail-closed, **3 dexscreener admissions**) -> **3 arms** ~10x the v1 arm rate, RugCheck evaluating every one; clean SIGTERM exit 0.

### Guardrails kept (avoid-the-rug)
Scam damper (45 farms killed in the 30-min run) | RugCheck lp-unlocked/mint/freeze veto | dev-rep gate | entry-time pool re-check (fail-closed in live mode) | PumpAPI drained-pool + unburned-LP vetoes | liq-collapse early exit | chase guard | Jupiter sell-route validation with stability burst.

### Expected PnL mechanics change
v1's edge came from graduated-pool entries only. v2 adds curve-phase entries whose exits ride the graduation pop (TP 4x hits fast on WOFI-class moves) while SL 0.3x + liq-collapse bound the downside; position sizing/max-positions from `.env` remain the risk budget. Recommend a 6-24h paper soak before flipping `DRY_RUN=false`.
