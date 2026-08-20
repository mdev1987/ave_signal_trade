"""Honest-engine replay re-tune (Phase 4).

Replays the 2026-08-13 channel signals through the HONEST paper engine
semantics against the parquet price feed:

- Entry: first traded price >= signal_ts + ENTRY_LATENCY_S. If no trade
  happens within ENTRY_WINDOW_S, the pool is treated as dead/unquotable and
  the signal is skipped (mirrors the fresh-quote entry gate).
- Exits (in priority order, evaluated on every tick):
    tp      : price touches entry*TAKE_PROFIT -> fill at the touch price
              (a real quote at that moment nets approximately this)
    trail   : peak >= entry*TRAIL_ACTIVATE_MULT then price <= peak*(1-retrace)
    sl      : price <= entry*STOP_LOSS -> fill at current price
    timeout : HOLD_S elapsed -> exit at last traded price (writeoff semantics)
- Win = realized exit_mult >= target (4.0). EV = mean(exit_mult) - 1.

Grid-searches filter constants (mcap band, snipes floor) to maximize EV per
trade subject to a minimum sample size, and prints the best configs.

Usage:
    .venv/bin/python scripts/replay_tune.py [--fast]
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import filter as F
from parser import parse_message_dict

# --- engine constants (mirror live config) ----------------------------------
TAKE_PROFIT = 4.0
STOP_LOSS = 0.3
HOLD_S = 3600.0
TRAIL_ACTIVATE_MULT = 2.0
TRAIL_RETRACE_PCT = 0.5
ENTRY_LATENCY_S = 5.0     # arm -> first-trade latency before the buy lands
ENTRY_WINDOW_S = 120.0    # no trade in this window after signal => dead pool

MIN_SAMPLE = 25           # grid candidates below this many trades are ignored


def load_signals() -> dict[str, dict]:
    """Parse the channel export; keep the FIRST signal per CA."""
    with open(ROOT / "docs/channel_signals.json") as fh:
        msgs = json.load(fh)["messages"]
    by_ca: dict[str, dict] = {}
    for m in msgs:
        sig = parse_message_dict(m)
        if not sig.ca or sig.ca in by_ca:
            continue
        by_ca[sig.ca] = {
            "ca": sig.ca,
            "name": sig.name,
            "dex": sig.dex,
            "mcap": sig.mcap_usd,
            "snipes": sig.snipes,
            "sec": sig.sec_score,
            "ts": float(sig.unixtime),
        }
    return by_ca


def load_prices(cas: set[str]) -> dict[str, list[tuple[float, float]]]:
    """Per-CA sorted [(ts_s, price)] from the day's parquet feed.

    Column-projected + mint-filtered per file so this stays fast.
    """
    files = sorted(glob.glob(str(ROOT / "docs/parquet/2026-08-13/*.parquet")))
    out: dict[str, list[tuple[float, float]]] = {}
    for f in files:
        df = pd.read_parquet(f, columns=["timestamp", "action", "mint", "price"])
        df = df[df["action"].isin(["buy", "sell"]) & df["mint"].isin(cas)]
        df = df[df["price"].notna() & (df["price"] > 0)]
        for mint, ts_ms, px in zip(df["mint"], df["timestamp"], df["price"]):
            out.setdefault(mint, []).append((ts_ms / 1000.0, float(px)))
    for series in out.values():
        series.sort()
    return out


def simulate(ticks: list[tuple[float, float]], sig_ts: float) -> dict | None:
    """Run one honest-engine position over the tick series. None = no entry."""
    t0 = sig_ts + ENTRY_LATENCY_S
    start_idx = next((i for i, (t, _) in enumerate(ticks) if t >= t0), None)
    if start_idx is None or ticks[start_idx][0] > t0 + ENTRY_WINDOW_S:
        return None  # dead pool: no executable entry inside the window
    entry_ts, entry_px = ticks[start_idx]
    peak = entry_px
    deadline = entry_ts + HOLD_S
    for ts, px in ticks[start_idx:]:
        peak = max(peak, px)
        mult = px / entry_px
        # TP: touch of entry*tp fills at the touch price.
        if px >= entry_px * TAKE_PROFIT:
            return {"exit_mult": TAKE_PROFIT, "reason": "tp", "hold_s": ts - entry_ts}
        # Trailing stop once activated.
        if (peak >= entry_px * TRAIL_ACTIVATE_MULT
                and px <= peak * (1.0 - TRAIL_RETRACE_PCT)):
            return {"exit_mult": mult, "reason": "trail", "hold_s": ts - entry_ts}
        # Stop-loss fills at the current (worse) price.
        if px <= entry_px * STOP_LOSS:
            return {"exit_mult": mult, "reason": "sl", "hold_s": ts - entry_ts}
        if ts >= deadline:
            return {"exit_mult": mult, "reason": "timeout", "hold_s": HOLD_S}
    # Feed ended before any exit: mark at last known price (honest writeoff).
    last_ts, last_px = ticks[-1]
    return {"exit_mult": last_px / entry_px, "reason": "feed_end",
            "hold_s": last_ts - entry_ts}


def passes(sig: dict, mcap_min: float, mcap_max: float, snipes_min: int,
           sec_max: int) -> bool:
    return (sig["dex"] == "Pumpfunamm"
            and mcap_min <= sig["mcap"] <= mcap_max
            and sig["snipes"] >= snipes_min
            and sig["sec"] <= sec_max)


def evaluate(outcomes: dict[str, dict | None], sigs: dict[str, dict],
             mcap_min: float, mcap_max: float, snipes_min: int,
             sec_max: int) -> dict:
    n = wins = 0
    ev_sum = 0.0
    reasons: dict[str, int] = {}
    for ca, o in outcomes.items():
        s = sigs[ca]
        if not passes(s, mcap_min, mcap_max, snipes_min, sec_max):
            continue
        if o is None:
            reasons["no_entry"] = reasons.get("no_entry", 0) + 1
            continue
        n += 1
        ev_sum += o["exit_mult"]
        reasons[o["reason"]] = reasons.get(o["reason"], 0) + 1
        if o["exit_mult"] >= TAKE_PROFIT:
            wins += 1
    if n == 0:
        return {"n": 0, "win": 0.0, "ev": 0.0}
    return {"n": n, "win": wins / n, "ev": ev_sum / n - 1.0, "reasons": reasons}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fast", action="store_true",
                    help="coarser grid (quicker iteration)")
    args = ap.parse_args()

    print("loading signals…")
    sigs = load_signals()
    print(f"  {len(sigs)} unique CAs")

    print("loading price feed (column-projected)…")
    prices = load_prices(set(sigs))
    print(f"  {len(prices)} CAs with feed data")

    print("simulating honest engine (TP=4.0 SL=0.3 hold=1h trail=2x/50%)…")
    outcomes: dict[str, dict | None] = {}
    for ca, s in sigs.items():
        ticks = prices.get(ca)
        outcomes[ca] = simulate(ticks, s["ts"]) if ticks else None
    entered = sum(1 for o in outcomes.values() if o is not None)
    print(f"  entries possible: {entered}/{len(sigs)} "
          f"({len(sigs) - entered} dead pools skipped)")

    base = F.get_filter()
    cur = evaluate(outcomes, sigs, base["mcap_usd_min"], base["mcap_usd_max"],
                   base["snipes_min"], base["sec_score_max"])
    print(f"\nCURRENT filter (mcap {base['mcap_usd_min']:.0f}-{base['mcap_usd_max']:.0f}, "
          f"snipes>={base['snipes_min']}, sec<={base['sec_score_max']}): "
          f"n={cur['n']} win{TAKE_PROFIT:.0f}x={100 * cur['win']:.1f}% "
          f"EV/trade={100 * cur['ev']:+.1f}%")
    mcaps_min = [5000, 8000, 10000] if args.fast else [4000, 5000, 8000, 10000, 12000]
    mcaps_max = [20000, 30000, 50000]
    snipes = [3, 5, 8, 12] if not args.fast else [3, 5, 10]
    results = []
    for mmin in mcaps_min:
        for mmax in mcaps_max:
            if mmax <= mmin:
                continue
            for sn in snipes:
                r = evaluate(outcomes, sigs, mmin, mmax, sn, 0)
                if r["n"] >= MIN_SAMPLE:
                    results.append((r["ev"], r["win"], r["n"], mmin, mmax, sn))
    results.sort(reverse=True)
    print(f"\nTOP 10 configs by EV/trade (min sample {MIN_SAMPLE}):")
    print("EV/trade   win4x    n    mcap_band       snipes>=")
    for ev, win, n, mmin, mmax, sn in results[:10]:
        print(f"{100 * ev:+7.1f}%  {100 * win:5.1f}%  {n:4d}  "
              f"${mmin:>5.0f}-${mmax:<5.0f}  {sn}")
    if results:
        ev, win, n, mmin, mmax, sn = results[0]
        print(f"\nRECOMMENDED: FILTER_MCAP_USD_MIN={mmin:.0f} "
              f"FILTER_MCAP_USD_MAX={mmax:.0f} FILTER_SNIPES_MIN={sn} "
              f"FILTER_SEC_SCORE_MAX=0  ->  n={n} win4x={100 * win:.1f}% "
              f"EV/trade={100 * ev:+.1f}%")
        # Exit-reason breakdown + win3x comparison for the recommended config.
        r = evaluate(outcomes, sigs, mmin, mmax, sn, 0)
        mults = [outcomes[ca]["exit_mult"] for ca in outcomes
                 if outcomes[ca] is not None
                 and passes(sigs[ca], mmin, mmax, sn, 0)]
        w3 = sum(1 for m in mults if m >= 3.0) / len(mults)
        print(f"  exit reasons: {r.get('reasons', {})}")
        print(f"  win3x (old target) = {100 * w3:.1f}%   "
              f"median mult = {sorted(mults)[len(mults) // 2]:.2f}x")
    else:
        print("\nNO config reached the minimum sample size — the honest engine "
              "finds no tradable edge in this dataset.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())