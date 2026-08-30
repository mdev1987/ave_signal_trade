"""End-to-end signal quality backtest with excursion analysis.

For every consensus signal, measures:
  1. Entry execution (PumpAPI price + configurable slippage)
  2. Adverse excursion (max drawdown from entry before exit)
  3. Max favorable excursion (peak profit from entry before exit)
  4. Actual exit via production TP ladder + stops
  5. Net PnL in SOL (including fee/slippage model)

This answers: "given a consensus signal, what was the realistic PnL path?"

Production TP ladder: 1.3x:40%, 1.8x:30%, 3.0x:30% (of original size).
Fees: Jupiter taker fee + Solana priority fee (configurable).

Run:
  uv run --with pyarrow python backtests/backtest_excursion.py \
      --data /home/mdev/Programming/new_sol_automate_bot/bot_plan/parquet/2026-08-13 \
      --out /tmp/backtest_excursion.json
"""
from __future__ import annotations

import argparse
import glob
import json
import time
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

# ---- production config (mirror src/config.py) --------------------------------
SIZE_SOL = 0.05
TP_LADDER: list[tuple[float, float]] = [(1.3, 0.4), (1.8, 0.3), (3.0, 0.3)]
TRAIL_START_MULT = 1.30
TRAIL_RETRACE = 0.35
HARD_STOP = 0.25
BE_BUFFER = 0.0
MAX_HOLD_S = 24 * 3600.0
MIN_CONSENSUS = 2
MIN_LIQ_USD = 1500.0
CONSENSUS_WINDOW_S = 600.0
MAX_CANDIDATE_AGE_S = 90 * 60.0
MAX_POSITIONS = 18
SOL_USD = 150.0
ENTRY_SLIPPAGE_BPS = 100    # 1% entry slippage (Jupiter taker worst case)
EXIT_SLIPPAGE_BPS = 300     # 3% exit slippage (escalating ladder starts here)
JUPITER_FEE_BPS = 40        # Jupiter platform fee
PRIORITY_FEE_SOL = 0.001    # Solana priority fee per tx (approx)

COLS = ["action", "mint", "txSigner", "price", "quoteInPool", "timestamp",
        "symbol", "name"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Signal quality backtest with excursion analysis")
    ap.add_argument("--data", required=True, help="parquet folder (HH.parquet)")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--min-consensus", type=int, default=MIN_CONSENSUS)
    ap.add_argument("--min-liq", type=float, default=MIN_LIQ_USD)
    ap.add_argument("--max-positions", type=int, default=MAX_POSITIONS)
    ap.add_argument("--entry-slippage-bps", type=int, default=ENTRY_SLIPPAGE_BPS)
    ap.add_argument("--exit-slippage-bps", type=int, default=EXIT_SLIPPAGE_BPS)
    ap.add_argument("--no-slippage", action="store_true", help="disable fee/slippage model")
    ap.add_argument("--out", default="/tmp/backtest_excursion.json")
    args = ap.parse_args()

    fee_mult = (1.0 + (ENTRY_SLIPPAGE_BPS + EXIT_SLIPPAGE_BPS + JUPITER_FEE_BPS) / 10_000.0) \
        if not args.no_slippage else 1.0

    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet in {args.data}")
    total = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"backtest: {len(files)} files, {total:,} rows, fee_mult={fee_mult:.4f}", flush=True)

    buyers: dict[str, dict[str, float]] = defaultdict(dict)
    first_buy: dict[str, float] = {}
    open_pos: dict[str, dict] = {}
    trades: list[dict] = []
    all_mult: list[float] = []
    all_adverse: list[float] = []    # max drawdown from entry (0..1)
    all_favorable: list[float] = []  # peak gain from entry (mult - 1)
    all_hold_s: list[float] = []
    exit_reasons = Counter()
    stats = Counter()
    t0 = time.time()

    def try_open(mint, price, liq, ts):
        if mint in open_pos:
            return
        if len(open_pos) >= args.max_positions:
            stats["pos_cap"] += 1
            return
        distinct = [s for s, bt in buyers[mint].items()
                    if ts - bt <= CONSENSUS_WINDOW_S]
        if len(distinct) < args.min_consensus:
            return
        if liq < args.min_liq:
            stats["low_liq"] += 1
            return
        # Jupiter entry: apply slippage to entry price (we pay more)
        exec_entry = price * (1.0 + args.entry_slippage_bps / 10_000.0)
        open_pos[mint] = {
            "entry_px": exec_entry, "entry_ts": ts,
            "raw_entry_px": price,
            "peak": exec_entry, "trough": exec_entry,
            "tp_taken": [], "be_armed": False, "banked_pnl": 0.0,
            "remaining": 1.0,
        }
        stats["entries"] += 1

    def close(mint, reason, price, ts):
        p = open_pos.pop(mint)
        # Jupiter exit: apply slippage (we receive less)
        exec_exit = price * (1.0 - args.exit_slippage_bps / 10_000.0)
        mult = exec_exit / p["entry_px"] if p["entry_px"] else 0.0
        # Net PnL includes banked TP + remaining at exit multiple, minus fees
        gross_pnl = p["banked_pnl"] + p["remaining"] * SIZE_SOL * (mult - 1.0)
        fee_cost = SIZE_SOL * (ENTRY_SLIPPAGE_BPS + EXIT_SLIPPAGE_BPS + JUPITER_FEE_BPS) / 10_000.0 \
            if not args.no_slippage else 0.0
        net_pnl = gross_pnl - fee_cost - PRIORITY_FEE_SOL
        # Excursion metrics (measured from exec entry to raw price path)
        adverse = max(0, 1.0 - p["trough"] / p["entry_px"])  # max drawdown
        favorable = max(0, p["peak"] / p["entry_px"] - 1.0)   # max gain
        hold_s = ts - p["entry_ts"]
        all_mult.append(mult)
        all_adverse.append(adverse)
        all_favorable.append(favorable)
        all_hold_s.append(hold_s)
        trades.append({
            "mint": mint[:12], "reason": reason,
            "entry_px": round(p["entry_px"], 8),
            "exit_px": round(exec_exit, 8),
            "raw_entry_px": round(p["raw_entry_px"], 8),
            "mult": round(mult, 3),
            "pnl_sol": round(net_pnl, 5),
            "gross_pnl_sol": round(gross_pnl, 5),
            "adverse_pct": round(adverse * 100, 2),
            "favorable_pct": round(favorable * 100, 2),
            "hold_min": round(hold_s / 60, 1),
            "tp_taken": list(p["tp_taken"]),
        })
        exit_reasons[reason] += 1
        stats["wins" if net_pnl >= 0 else "losses"] += 1

    for fpath in files:
        pf = pq.ParquetFile(fpath)
        for rb in pf.iter_batches(batch_size=500_000, columns=COLS):
            d = rb.to_pydict()
            n = len(d["action"])
            for i in range(n):
                action = d["action"][i]
                mint = d["mint"][i]
                ts_ms = d["timestamp"][i]
                if mint is None or ts_ms is None:
                    continue
                ts = ts_ms / 1000.0
                if action == "create":
                    stats["launches"] += 1
                    continue
                if action not in ("buy", "sell"):
                    continue
                price = d["price"][i]
                liq = (d["quoteInPool"][i] or 0.0) * 2.0 * SOL_USD
                signer = d["txSigner"][i]

                if action == "buy" and signer:
                    if mint not in first_buy:
                        first_buy[mint] = ts
                    if ts - first_buy[mint] <= MAX_CANDIDATE_AGE_S:
                        buyers[mint][signer] = ts

                if action == "buy" and mint not in open_pos:
                    try_open(mint, price, liq, ts)

                # Monitor open position: track price path for excursion
                if mint in open_pos and price:
                    p = open_pos[mint]
                    p["peak"] = max(p["peak"], price)
                    p["trough"] = min(p["trough"], price)
                    peak_mult = p["peak"] / p["entry_px"]
                    mult = price / p["entry_px"]

                    # --- TP ladder (production: 1.3:0.4, 1.8:0.3, 3.0:0.3)
                    for lvl, frac in TP_LADDER:
                        if lvl in p["tp_taken"]:
                            continue
                        if peak_mult >= lvl:
                            exec_at = min(mult, lvl) if mult < lvl else lvl
                            p["tp_taken"].append(lvl)
                            p["banked_pnl"] += frac * SIZE_SOL * (exec_at - 1.0)
                            p["remaining"] = max(0.0, p["remaining"] - frac)
                            if p["remaining"] <= 1e-9:
                                p["remaining"] = 0.0
                    if p["tp_taken"] and not p["be_armed"]:
                        p["be_armed"] = True

                    if p["remaining"] <= 0:
                        close(mint, "tp", price, ts)
                    else:
                        # Early rug guard
                        if (mult <= 1.0 - 0.30 and
                                ts - p["entry_ts"] < 60):
                            close(mint, "early_invalid", price, ts)
                        else:
                            stop_mult = 1.0 - HARD_STOP
                            if p["be_armed"]:
                                stop_mult = max(stop_mult, 1.0 + BE_BUFFER)
                            if mult <= stop_mult:
                                close(mint, "sl", price, ts)
                            elif (peak_mult >= TRAIL_START_MULT and
                                  price <= p["peak"] * (1 - TRAIL_RETRACE)):
                                close(mint, "trail", price, ts)
                            elif ts - p["entry_ts"] > MAX_HOLD_S:
                                close(mint, "max_hold", price, ts)

                if args.max_trades and stats["entries"] >= args.max_trades:
                    break
            if args.max_trades and stats["entries"] >= args.max_trades:
                break
        if args.max_trades and stats["entries"] >= args.max_trades:
            break

    wins = stats["wins"]
    entries = stats["entries"]
    pnl = sum(t["pnl_sol"] for t in trades)
    gross_pnl = sum(t["gross_pnl_sol"] for t in trades)

    def _pct(arr, x):
        s = sorted(arr)
        return round(s[min(len(s) - 1, int(x * len(s)))], 2) if s else 0.0

    # Per-exit-reason breakdown
    reason_stats: dict[str, dict] = {}
    for reason in exit_reasons:
        rts = [t for t in trades if t["reason"] == reason]
        reason_stats[reason] = {
            "count": len(rts),
            "win_rate": round(sum(1 for t in rts if t["pnl_sol"] >= 0) / len(rts) * 100, 1) if rts else 0,
            "avg_pnl": round(sum(t["pnl_sol"] for t in rts) / len(rts), 5) if rts else 0,
            "avg_adverse": round(sum(t["adverse_pct"] for t in rts) / len(rts), 2) if rts else 0,
            "avg_favorable": round(sum(t["favorable_pct"] for t in rts) / len(rts), 2) if rts else 0,
        }

    res = {
        "data": args.data,
        "rows": total,
        "params": {
            "tp_ladder": TP_LADDER, "trail_start": TRAIL_START_MULT,
            "trail_retrace": TRAIL_RETRACE, "hard_stop": HARD_STOP,
            "min_consensus": args.min_consensus, "min_liq": args.min_liq,
            "max_hold_h": MAX_HOLD_S / 3600, "size_sol": SIZE_SOL,
            "entry_slippage_bps": args.entry_slippage_bps,
            "exit_slippage_bps": args.exit_slippage_bps,
            "no_slippage": args.no_slippage,
        },
        "funnel": {k: stats[k] for k in
                   ("launches", "entries", "wins", "losses", "low_liq", "pos_cap")},
        "exit_reasons": dict(exit_reasons),
        "reason_breakdown": reason_stats,
        # Core metrics
        "pnl_sol": round(pnl, 4),
        "gross_pnl_sol": round(gross_pnl, 4),
        "win_rate": round(wins / entries * 100, 1) if entries else 0.0,
        "avg_pnl_per_trade": round(pnl / entries, 5) if entries else 0.0,
        # Multiple stats
        "mult_p10": _pct(all_mult, 0.10),
        "mult_p50": _pct(all_mult, 0.50),
        "mult_p90": _pct(all_mult, 0.90),
        "mult_max": round(max(all_mult), 2) if all_mult else 0,
        # EXCURSION ANALYSIS (the key new metrics)
        "adverse": {
            "p50": _pct(all_adverse, 0.50),   # median max drawdown
            "p90": _pct(all_adverse, 0.90),   # 90th percentile drawdown
            "max": round(max(all_adverse), 4) if all_adverse else 0,
            "avg": round(sum(all_adverse) / len(all_adverse), 4) if all_adverse else 0,
        },
        "favorable": {
            "p50": _pct(all_favorable, 0.50),
            "p90": _pct(all_favorable, 0.90),
            "max": round(max(all_favorable), 4) if all_favorable else 0,
            "avg": round(sum(all_favorable) / len(all_favorable), 4) if all_favorable else 0,
        },
        "hold": {
            "avg_min": round(sum(all_hold_s) / len(all_hold_s) / 60, 1) if all_hold_s else 0,
            "median_min": round(_pct([h/60 for h in all_hold_s], 0.50), 1) if all_hold_s else 0,
            "p90_min": round(_pct([h/60 for h in all_hold_s], 0.90), 1) if all_hold_s else 0,
        },
        "top_trades": sorted(trades, key=lambda x: x["pnl_sol"], reverse=True)[:15],
        "worst_trades": sorted(trades, key=lambda x: x["pnl_sol"])[:10],
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(res, indent=2))
    with open(args.out, "w") as f:
        json.dump(res, f, indent=2)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
