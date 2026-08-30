"""Signal quality backtest: single-pass extraction + post-hoc filter ablation.

Phase 1: Single pass over parquet — detect consensus entries, record price
snapshots at T+5/10/15/30/60/120/300s, run exit logic, produce trade list.

Phase 2: Apply filters as post-processing on the trade list — no re-scan.

This makes the 4-way ablation ~1x instead of ~4x the single-pass cost.

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
ENTRY_SLIPPAGE_BPS = 100
EXIT_SLIPPAGE_BPS = 300
JUPITER_FEE_BPS = 40
PRIORITY_FEE_SOL = 0.001
SNAPSHOT_WINDOWS = [5, 10, 15, 30, 60, 120, 300]
COLS = ["action", "mint", "txSigner", "price", "quoteInPool", "timestamp",
        "symbol", "name"]


def _pct(arr, x):
    s = sorted(arr)
    return round(s[min(len(s) - 1, int(x * len(s)))], 2) if s else 0.0


def _time_features(snaps: dict[int, float], entry_px: float) -> dict:
    if not snaps or entry_px <= 0:
        return {}
    f = {}
    for w in SNAPSHOT_WINDOWS:
        if w in snaps:
            f[f"ret_{w}s"] = round(snaps[w] / entry_px - 1.0, 4)
    p30 = [v for k, v in snaps.items() if k <= 30]
    p60 = [v for k, v in snaps.items() if k <= 60]
    if p30:
        f["early_adv_30"] = round(1.0 - min(p30) / entry_px, 4)
        f["early_fav_30"] = round(max(p30) / entry_px - 1.0, 4)
    if p60:
        f["early_adv_60"] = round(1.0 - min(p60) / entry_px, 4)
        f["early_fav_60"] = round(max(p60) / entry_px - 1.0, 4)
    for lbl, tgt in [("t10", 1.10), ("t20", 1.20), ("t30", 1.30)]:
        t = None
        for w in sorted(snaps):
            if snaps[w] / entry_px >= tgt:
                t = w
                break
        f[f"time_{lbl}"] = t
    for lbl, tgt in [("tm10", 0.90), ("tm20", 0.80), ("tm30", 0.70)]:
        t = None
        for w in sorted(snaps):
            if snaps[w] / entry_px <= tgt:
                t = w
                break
        f[f"time_{lbl}"] = t
    t10 = f.get("time_t10")
    tm20 = f.get("time_tm20")
    if t10 is not None and tm20 is not None:
        f["conf_ratio"] = 1.0 if t10 < tm20 else 0.0
    elif t10 is not None:
        f["conf_ratio"] = 1.0
    elif tm20 is not None:
        f["conf_ratio"] = 0.0
    return f


def _metrics(trades: list[dict]) -> dict:
    entries = len(trades)
    if entries == 0:
        return {"entries": 0, "win_rate": 0, "pnl_sol": 0, "gross_pnl_sol": 0,
                "profit_factor": 0, "avg_pnl": 0}
    wins = sum(1 for t in trades if t["pnl_sol"] >= 0)
    pnl = sum(t["pnl_sol"] for t in trades)
    gross = sum(t["gross_pnl_sol"] for t in trades)
    gw = sum(t["pnl_sol"] for t in trades if t["pnl_sol"] > 0)
    gl = abs(sum(t["pnl_sol"] for t in trades if t["pnl_sol"] < 0))
    return {
        "entries": entries, "wins": wins, "losses": entries - wins,
        "win_rate": round(wins / entries * 100, 1),
        "pnl_sol": round(pnl, 4), "gross_pnl_sol": round(gross, 4),
        "fee_cost": round(pnl - gross, 4),
        "profit_factor": round(gw / gl, 3) if gl > 0 else float("inf"),
        "avg_pnl": round(pnl / entries, 5),
        "adverse_p50": _pct([t["adverse_pct"] for t in trades], 0.50),
        "favorable_p50": _pct([t["favorable_pct"] for t in trades], 0.50),
        "sl_count": sum(1 for t in trades if t["reason"] == "sl"),
        "trail_count": sum(1 for t in trades if t["reason"] == "trail"),
        "tp_count": sum(1 for t in trades if t["reason"] == "tp"),
        "early_invalid_count": sum(1 for t in trades if t["reason"] == "early_invalid"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", default="/tmp/backtest_excursion.json")
    args = ap.parse_args()

    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet in {args.data}")
    total = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"backtest: {len(files)} files, {total:,} rows", flush=True)
    t0 = time.time()

    # ---- Phase 1: single pass ----
    buyers: dict[str, dict[str, float]] = defaultdict(dict)
    first_buy: dict[str, float] = {}
    open_pos: dict[str, dict] = {}
    trades: list[dict] = []
    stats = Counter()

    def try_open(mint, price, liq, ts):
        if mint in open_pos or len(open_pos) >= MAX_POSITIONS:
            return
        distinct = [s for s, bt in buyers[mint].items() if ts - bt <= CONSENSUS_WINDOW_S]
        if len(distinct) < MIN_CONSENSUS:
            return
        if liq < MIN_LIQ_USD:
            stats["low_liq"] += 1
            return
        exec_entry = price * (1.0 + ENTRY_SLIPPAGE_BPS / 10_000.0)
        open_pos[mint] = {
            "entry_px": exec_entry, "entry_ts": ts, "raw_entry_px": price,
            "peak": exec_entry, "trough": exec_entry,
            "tp_taken": [], "be_armed": False, "banked_pnl": 0.0,
            "remaining": 1.0, "snaps": {},
        }
        stats["entries"] += 1

    def close(mint, reason, price, ts):
        p = open_pos.pop(mint)
        exec_exit = price * (1.0 - EXIT_SLIPPAGE_BPS / 10_000.0)
        mult = exec_exit / p["entry_px"] if p["entry_px"] else 0.0
        gross_pnl = p["banked_pnl"] + p["remaining"] * SIZE_SOL * (mult - 1.0)
        fee = SIZE_SOL * (ENTRY_SLIPPAGE_BPS + EXIT_SLIPPAGE_BPS + JUPITER_FEE_BPS) / 10_000.0
        net_pnl = gross_pnl - fee - PRIORITY_FEE_SOL
        adverse = max(0, 1.0 - p["trough"] / p["entry_px"])
        favorable = max(0, p["peak"] / p["entry_px"] - 1.0)
        hold_s = ts - p["entry_ts"]
        tf = _time_features(p["snaps"], p["entry_px"])
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
            **tf,
        })
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
                if mint in open_pos and price:
                    p = open_pos[mint]
                    elapsed = ts - p["entry_ts"]
                    for w in SNAPSHOT_WINDOWS:
                        if w not in p["snaps"] and elapsed >= w:
                            p["snaps"][w] = price
                    p["peak"] = max(p["peak"], price)
                    p["trough"] = min(p["trough"], price)
                    peak_mult = p["peak"] / p["entry_px"]
                    mult = price / p["entry_px"]
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
                        if mult <= 0.70 and elapsed < 60:
                            close(mint, "early_invalid", price, ts)
                        else:
                            stop_mult = 1.0 - HARD_STOP
                            if p["be_armed"]:
                                stop_mult = max(stop_mult, 1.0 + BE_BUFFER)
                            if mult <= stop_mult:
                                close(mint, "sl", price, ts)
                            elif peak_mult >= TRAIL_START_MULT and price <= p["peak"] * (1 - TRAIL_RETRACE):
                                close(mint, "trail", price, ts)
                            elif elapsed > MAX_HOLD_S:
                                close(mint, "max_hold", price, ts)

    for mint in list(open_pos):
        p = open_pos[mint]
        close(mint, "end_of_data", p["peak"], p["entry_ts"] + MAX_HOLD_S)

    print(f"phase 1 done: {len(trades)} trades in {time.time()-t0:.1f}s", flush=True)

    # ---- Phase 2: post-hoc filter ablation ----
    def apply_filters(trades, early_adv_thresh, early_fav_thresh, conf_thresh):
        kept = []
        filtered = Counter()
        for t in trades:
            reject = False
            reason = None
            if early_adv_thresh is not None and early_fav_thresh is not None:
                adv = t.get("early_adv_30")
                fav = t.get("early_fav_30")
                if adv is not None and fav is not None:
                    if adv >= early_adv_thresh and fav < early_fav_thresh:
                        reject = True
                        reason = "early_adverse"
            if conf_thresh is not None and not reject:
                cr = t.get("conf_ratio")
                if cr is not None and cr < conf_thresh:
                    reject = True
                    reason = "no_confirmation"
            if reject:
                filtered[reason] += 1
            else:
                kept.append(t)
        return kept, dict(filtered)

    # A: baseline
    a_m = _metrics(trades)
    print(f"\nA baseline: {a_m['entries']} entries, {a_m['win_rate']}% win, "
          f"gross={a_m['gross_pnl_sol']:.4f} net={a_m['pnl_sol']:.4f} PF={a_m['profit_factor']}")

    # B: early adverse filter (-20% adverse AND < +5% favorable in first 30s)
    b_trades, b_filt = apply_filters(trades, -0.20, 0.05, None)
    b_m = _metrics(b_trades)
    print(f"B early_adv: {b_m['entries']} entries (-{sum(b_filt.values())} filtered), "
          f"{b_m['win_rate']}% win, gross={b_m['gross_pnl_sol']:.4f} net={b_m['pnl_sol']:.4f} PF={b_m['profit_factor']}")

    # C: confirmation race filter (+10% before -20%)
    c_trades, c_filt = apply_filters(trades, None, None, 0.4)
    c_m = _metrics(c_trades)
    print(f"C confirm:   {c_m['entries']} entries (-{sum(c_filt.values())} filtered), "
          f"{c_m['win_rate']}% win, gross={c_m['gross_pnl_sol']:.4f} net={c_m['pnl_sol']:.4f} PF={c_m['profit_factor']}")

    # D: both
    d_trades, d_filt = apply_filters(trades, -0.20, 0.05, 0.4)
    d_m = _metrics(d_trades)
    print(f"D both:      {d_m['entries']} entries (-{sum(d_filt.values())} filtered), "
          f"{d_m['win_rate']}% win, gross={d_m['gross_pnl_sol']:.4f} net={d_m['pnl_sol']:.4f} PF={d_m['profit_factor']}")

    # Per-reason breakdown for baseline
    reason_stats = {}
    for reason in set(t["reason"] for t in trades):
        rts = [t for t in trades if t["reason"] == reason]
        rts_pnl = [t["pnl_sol"] for t in rts]
        reason_stats[reason] = {
            "count": len(rts),
            "win_rate": round(sum(1 for p in rts_pnl if p >= 0) / len(rts) * 100, 1) if rts else 0,
            "avg_pnl": round(sum(rts_pnl) / len(rts), 5) if rts else 0,
            "total_pnl": round(sum(rts_pnl), 5),
            "avg_adverse": round(sum(t["adverse_pct"] for t in rts) / len(rts), 2) if rts else 0,
        }

    # Print comparison table
    print("\n" + "=" * 90)
    print("ABLATION COMPARISON")
    print("=" * 90)
    hdr = f"{'variant':<22} {'ent':>6} {'w%':>6} {'gross':>9} {'net':>9} {'PF':>7} {'adv_p50':>8} {'SL':>5} {'trail':>6}"
    print(hdr)
    print("-" * 90)
    for label, m, filt in [("A_baseline", a_m, {}),
                            ("B_early_adv", b_m, b_filt),
                            ("C_confirm", c_m, c_filt),
                            ("D_both", d_m, d_filt)]:
        filt_str = f"-{sum(filt.values())}" if filt else ""
        print(f"{label:<22} {m['entries']:>6} {m['win_rate']:>5}% "
              f"{m['gross_pnl_sol']:>9.4f} {m['pnl_sol']:>9.4f} "
              f"{m['profit_factor']:>7.3f} {m['adverse_p50']:>7.2f} "
              f"{m['sl_count']:>5} {m['trail_count']:>6}  {filt_str}")
    print("=" * 90)

    # Reason breakdown
    print("\nEXIT REASON BREAKDOWN (baseline):")
    for reason, rs in sorted(reason_stats.items()):
        print(f"  {reason:<16} n={rs['count']:>4}  wr={rs['win_rate']:>5.1f}%  "
              f"avg_pnl={rs['avg_pnl']:>9.5f}  total={rs['total_pnl']:>9.5f}  "
              f"avg_adv={rs['avg_adverse']:>6.2f}%")

    # Write full output
    result = {
        "data": args.data, "rows": total, "seconds": round(time.time() - t0, 1),
        "ablation": {
            "A_baseline": {k: v for k, v in a_m.items()},
            "B_early_adverse": {**{k: v for k, v in b_m.items()}, "filtered": b_filt},
            "C_confirmation": {**{k: v for k, v in c_m.items()}, "filtered": c_filt},
            "D_both": {**{k: v for k, v in d_m.items()}, "filtered": d_filt},
        },
        "reason_breakdown": reason_stats,
        "baseline_trades": trades,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
