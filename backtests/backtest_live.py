"""Backtest analyzer for ave_signal_trade.

Two modes:
  1. analyze  — analyze existing closed trades from shadow_book.json
  2. simulate — simulate consensus signals from journal using DexScreener spot prices

Usage:
    uv run python backtests/backtest_live.py analyze
    uv run python backtests/backtest_live.py simulate [--max-signals 20]
    uv run python backtests/backtest_live.py simulate --source cabalspy
"""
from __future__ import annotations

import argparse
import json
import time
from collections import defaultdict
from pathlib import Path

import httpx


# ── analyze mode ─────────────────────────────────────────────────────────────

def analyze_trades(book_path: str = "shadow_book.json"):
    book = json.load(open(book_path))
    closed = book.get("closed", [])
    open_pos = book.get("open", {})
    balance = book.get("balance_sol", 0)

    if not closed:
        print("No closed trades found.")
        return

    total_pnl = sum(t["pnl_sol"] for t in closed)
    wins = [t for t in closed if t["pnl_sol"] > 0]
    losses = [t for t in closed if t["pnl_sol"] <= 0]

    print(f"\n{'='*65}")
    print(f"  TRADE ANALYSIS — {len(closed)} closed trades")
    print(f"{'='*65}")
    print(f"  Balance:       {balance:.4f} SOL")
    print(f"  Open:          {len(open_pos)}")
    print(f"  Total PnL:     {total_pnl:+.4f} SOL")
    print(f"  Win rate:      {len(wins)}/{len(closed)} ({100*len(wins)/len(closed):.1f}%)")
    if wins:
        print(f"  Avg win:       {sum(t['pnl_sol'] for t in wins)/len(wins):+.4f} SOL")
    if losses:
        print(f"  Avg loss:      {sum(t['pnl_sol'] for t in losses)/len(losses):+.4f} SOL")
    if wins and losses:
        pf = abs(sum(t["pnl_sol"] for t in wins) / sum(t["pnl_sol"] for t in losses))
        print(f"  Profit factor: {pf:.2f}")
    avg_hold = sum(t.get("hold_min", 0) for t in closed) / len(closed)
    print(f"  Avg hold:      {avg_hold:.0f} min")
    print(f"{'='*65}")

    # By source
    by_source: dict[str, list] = defaultdict(list)
    for t in closed:
        by_source[t.get("source", "unknown")].append(t)
    print(f"\n  BY SOURCE:")
    for src, trades in sorted(by_source.items(), key=lambda x: -sum(t["pnl_sol"] for t in x[1])):
        pnl = sum(t["pnl_sol"] for t in trades)
        w = [t for t in trades if t["pnl_sol"] > 0]
        avg_h = sum(t.get("hold_min", 0) for t in trades) / len(trades)
        print(f"    {src.ljust(12)}: {len(trades):2d} trades, {len(w):2d} wins "
              f"({100*len(w)/len(trades):.0f}%), PnL={pnl:+.4f}, hold={avg_h:.0f}m")

    # By exit reason
    by_reason: dict[str, list] = defaultdict(list)
    for t in closed:
        by_reason[t.get("reason", "unknown")].append(t)
    print(f"\n  BY EXIT REASON:")
    for reason, trades in sorted(by_reason.items(), key=lambda x: -len(x[1])):
        pnl = sum(t["pnl_sol"] for t in trades)
        print(f"    {reason.ljust(16)}: {len(trades):2d} trades, PnL={pnl:+.4f}")

    # By score (if available in journal)
    journal = _load_journal()
    score_map = {}
    for e in journal:
        if e["event"] == "shadow_open":
            score_map[e["ca"]] = e.get("score", 0)

    scored = [(t, score_map.get(t["ca"], 0)) for t in closed]
    buckets = {"<2.0": [], "2.0-3.0": [], "3.0-5.0": [], "5.0+": []}
    for t, sc in scored:
        if sc < 2.0:
            buckets["<2.0"].append(t)
        elif sc < 3.0:
            buckets["2.0-3.0"].append(t)
        elif sc < 5.0:
            buckets["3.0-5.0"].append(t)
        else:
            buckets["5.0+"].append(t)
    print(f"\n  BY SCORE:")
    for bucket, trades in buckets.items():
        if trades:
            pnl = sum(t["pnl_sol"] for t in trades)
            w = [t for t in trades if t["pnl_sol"] > 0]
            print(f"    {bucket.ljust(10)}: {len(trades):2d} trades, {len(w):2d} wins "
                  f"({100*len(w)/len(trades):.0f}%), PnL={pnl:+.4f}")

    # All trades
    print(f"\n  ALL TRADES:")
    for t in sorted(closed, key=lambda x: x["ts"] if "ts" in x else 0):
        sc = score_map.get(t["ca"], 0)
        print(f"    {t['symbol']:12} {t.get('source','?'):12} "
              f"score={sc:4.1f} mult={t['mult']:.3f} "
              f"pnl={t['pnl_sol']:+.4f} hold={t.get('hold_min',0):5.0f}m "
              f"{t['reason']}")

    # Top wallets
    wallet_pnl: dict[str, float] = defaultdict(float)
    wallet_count: dict[str, int] = defaultdict(int)
    for t in closed:
        for w in t.get("wallets", []):
            wallet_pnl[w[:10]] += t["pnl_sol"] / max(1, len(t.get("wallets", [])))
            wallet_count[w[:10]] += 1
    print(f"\n  TOP WALLETS (by shared PnL):")
    for w, pnl in sorted(wallet_pnl.items(), key=lambda x: -x[1])[:10]:
        print(f"    {w}: {pnl:+.4f} SOL ({wallet_count[w]} trades)")


def _load_journal(path: str = "bot_logs/journal.json") -> list[dict]:
    entries = []
    for line in Path(path).open():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return entries


# ── simulate mode ────────────────────────────────────────────────────────────

TP_LADDER = [
    (1.2, 0.40, 0.30),
    (1.8, 0.30, 0.30),
    (3.0, 0.30, 0.30),
]
HARD_STOP = 0.25
BE_ARM_MULT = 1.15
FLAT_TIMEOUT_H = 6.0
FLAT_TIMEOUT_PEAK = 1.10
MAX_HOLD_H = 24.0
CONSENSUS_WINDOW_S = 600.0
MIN_WALLET_WEIGHT = 0.5
MIN_WALLETS = 2


def build_consensus_signals(entries: list[dict], window_s: float = CONSENSUS_WINDOW_S) -> list[dict]:
    smart_buys = [e for e in entries if e["event"] == "smart_buy_seen"]
    by_ca: dict[str, list[dict]] = defaultdict(list)
    for e in smart_buys:
        by_ca[e["ca"]].append(e)

    signals = []
    for ca, buys in by_ca.items():
        if len(buys) < MIN_WALLETS:
            continue
        buys.sort(key=lambda b: b["ts"])
        best_cluster = []
        for i, start in enumerate(buys):
            cluster = [b for b in buys[i:] if b["ts"] - start["ts"] <= window_s]
            if len(cluster) > len(best_cluster):
                best_cluster = cluster
        if len(best_cluster) < MIN_WALLETS:
            continue

        wallet_best: dict[str, dict] = {}
        for b in best_cluster:
            w = b["wallet"]
            if w not in wallet_best or b["wt"] > wallet_best[w]["wt"]:
                wallet_best[w] = b
        wallets = list(wallet_best.values())
        if len(wallets) < MIN_WALLETS:
            continue

        total_weight = sum(max(b["wt"], MIN_WALLET_WEIGHT) for b in wallets)
        if total_weight < 1.3:
            continue
        if not any(b["wt"] >= 1.0 for b in wallets):
            continue

        signals.append({
            "ca": ca,
            "ts": min(b["ts"] for b in wallets),
            "wallets": [b["wallet"] for b in wallets],
            "n_wallets": len(wallets),
            "score": total_weight,
            "source": "mixed",
        })
    signals.sort(key=lambda s: s["ts"])
    return signals


def simulate_signals(
    journal_path: str = "bot_logs/journal.json",
    size_sol: float = 0.05,
    max_signals: int = 30,
    source_filter: str = "",
):
    entries = _load_journal(journal_path)
    signals = build_consensus_signals(entries)
    print(f"Consensus signals: {len(signals)}")

    # Already-traded CAs (skip)
    book = json.load(open("shadow_book.json"))
    traded_cas = set(book.get("open", {}).keys())
    traded_cas |= {t["ca"] for t in book.get("closed", [])}

    # Filter
    if source_filter:
        signals = [s for s in signals if source_filter in s.get("source", "")]
    signals = [s for s in signals if s["ca"] not in traded_cas]
    if max_signals:
        signals = signals[:max_signals]

    print(f"New signals (not yet traded): {len(signals)}")
    if not signals:
        return

    ds = httpx.Client(timeout=15, headers={"User-Agent": "backtest/1.0"})
    results = []

    for i, sig in enumerate(signals):
        ca = sig["ca"]
        try:
            r = ds.get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
            if r.status_code != 200:
                continue
            pairs = r.json().get("pairs", [])
            if not pairs:
                continue
            pair = pairs[0]
            px = float(pair.get("priceUsd", 0))
            liq = float(pair.get("liquidity", {}).get("usd", 0))
            vol = float(pair.get("volume", {}).get("h1", 0))
            h1_change = float(pair.get("priceChange", {}).get("h1", 0))
            h5m_change = float(pair.get("priceChange", {}).get("m5", 0))

            if px <= 0 or liq < 500:
                continue

            results.append({
                "ca": ca,
                "ts": sig["ts"],
                "score": sig["score"],
                "n_wallets": sig["n_wallets"],
                "price_usd": px,
                "liq_usd": liq,
                "vol_1h": vol,
                "h1_pct": h1_change,
                "m5_pct": h5m_change,
                "wallets": sig["wallets"][:3],
            })
            print(f"  [{i+1}/{len(signals)}] {ca[:10]} score={sig['score']:.1f} "
                  f"px=${px:.8f} liq=${liq:.0f} h1={h1_change:+.1f}%")
            time.sleep(0.3)
        except Exception as e:
            print(f"  [{i+1}/{len(signals)}] {ca[:10]} ERROR: {e}")

    print(f"\n{'='*65}")
    print(f"  SIGNAL QUALITY REPORT — {len(results)} signals with live prices")
    print(f"{'='*65}")

    if not results:
        print("  No signals with live prices found.")
        return

    # Score distribution
    print(f"\n  SCORE DISTRIBUTION:")
    for r in sorted(results, key=lambda x: -x["score"]):
        print(f"    {r['ca'][:10]} score={r['score']:.1f} "
              f"n={r['n_wallets']} liq=${r['liq_usd']:.0f} "
              f"h1={r['h1_pct']:+.1f}% m5={r['m5_pct']:+.1f}%")

    # Liquidity distribution
    liq_buckets = {"<$1k": [], "$1k-5k": [], "$5k-20k": [], "$20k+": []}
    for r in results:
        liq = r["liq_usd"]
        if liq < 1000:
            liq_buckets["<$1k"].append(r)
        elif liq < 5000:
            liq_buckets["$1k-5k"].append(r)
        elif liq < 20000:
            liq_buckets["$5k-20k"].append(r)
        else:
            liq_buckets["$20k+"].append(r)
    print(f"\n  LIQUIDITY DISTRIBUTION:")
    for bucket, trades in liq_buckets.items():
        if trades:
            avg_h1 = sum(t["h1_pct"] for t in trades) / len(trades)
            print(f"    {bucket.ljust(10)}: {len(trades)} signals, avg h1={avg_h1:+.1f}%")

    # Price action summary
    h1_values = [r["h1_pct"] for r in results]
    print(f"\n  PRICE ACTION (1h):")
    print(f"    Mean:   {sum(h1_values)/len(h1_values):+.1f}%")
    print(f"    Median: {sorted(h1_values)[len(h1_values)//2]:+.1f}%")
    print(f"    Min:    {min(h1_values):+.1f}%")
    print(f"    Max:    {max(h1_values):+.1f}%")
    positive = sum(1 for v in h1_values if v > 0)
    print(f"    >0%:    {positive}/{len(h1_values)} ({100*positive/len(h1_values):.0f}%)")


def main():
    parser = argparse.ArgumentParser(description="Backtest analyzer")
    sub = parser.add_subparsers(dest="mode")

    sub.add_parser("analyze", help="Analyze existing trades from shadow_book")

    sim = sub.add_parser("simulate", help="Simulate signals with live DexScreener prices")
    sim.add_argument("--max-signals", type=int, default=30)
    sim.add_argument("--source", type=str, default="")
    sim.add_argument("--size", type=float, default=0.05)

    args = parser.parse_args()
    if args.mode == "analyze":
        analyze_trades()
    elif args.mode == "simulate":
        simulate_signals(
            max_signals=args.max_signals,
            source_filter=args.source,
            size_sol=args.size,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
