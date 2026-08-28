"""Backtest the ave_signal_trade wallet-consensus strategy on PumpAPI replay parquet.

Entry: a mint is "consensus" when >= MIN_CONSENSUS distinct buyer wallets buy it
within CONSENSUS_WINDOW_S and pool liquidity >= MIN_LIQ_USD -> open at that price
(one position per mint; capped at MAX_POSITIONS concurrent).

Exit (identical to main.py ShadowBook, validated in /tmp/test_book.py):
  * bank 50% at TP1_MULT (1.5x), then raise the stop to breakeven (BE_BUFFER)
  * trailing stop after TRAIL_START_MULT (1.3x) peak: TRAIL_RETRACE (35%) retrace
  * hard stop HARD_STOP (40%)
  * max-hold timeout MAX_HOLD_S (72h)

PnL is reported in SOL via the price-ratio method ShadowBook uses (no per-fill
fee/slippage modelled here — noted as a caveat). Run:

  uv run --with pyarrow python backtest_consensus.py \
      --data /home/mdev/Programming/new_sol_automate_bot/bot_plan/parquet/2026-08-13
"""
from __future__ import annotations

import argparse
import glob
import json
import time
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

# ---- mirror ave_signal_trade/src/config.py (the hardened strategy) ----------
SIZE_SOL = 0.05
TP1_MULT = 1.50
TRAIL_START_MULT = 1.30
TRAIL_RETRACE = 0.35
HARD_STOP = 0.40
BE_BUFFER = 0.0
MAX_HOLD_S = 72 * 3600.0
MIN_CONSENSUS = 2
MIN_LIQ_USD = 5000.0
ENTRY_LATENCY_S = 3.0
MAX_CANDIDATE_AGE_S = 90 * 60.0   # buyers considered for consensus this long after first
CONSENSUS_WINDOW_S = 600.0         # distinct buyers within this window -> consensus
MAX_POSITIONS = 10
SOL_USD = 150.0                    # constant for the liquidity gate (caveat)

COLS = ["action", "mint", "txSigner", "price", "quoteInPool", "timestamp",
        "symbol", "name"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="parquet folder (HH.parquet)")
    ap.add_argument("--max-trades", type=int, default=0, help="0 = unlimited")
    ap.add_argument("--min-consensus", type=int, default=MIN_CONSENSUS)
    ap.add_argument("--min-liq", type=float, default=MIN_LIQ_USD)
    ap.add_argument("--max-positions", type=int, default=MAX_POSITIONS)
    ap.add_argument("--out", default="/tmp/backtest_consensus.json")
    args = ap.parse_args()

    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet in {args.data}")
    total = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"backtest: {len(files)} files, {total:,} rows", flush=True)

    # per-mint consensus tracking
    buyers: dict[str, dict[str, float]] = defaultdict(dict)  # mint -> {signer: ts}
    first_buy: dict[str, float] = {}
    open_pos: dict[str, dict] = {}
    trades: list[dict] = []
    all_mult: list[float] = []
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
        open_pos[mint] = {
            "entry_px": price, "entry_ts": ts, "peak": price,
            "tp1_banked": False, "be_armed": False, "banked_pnl": 0.0,
        }
        stats["entries"] += 1

    def close(mint, reason, px, ts):
        p = open_pos.pop(mint)
        mult = px / p["entry_px"] if p["entry_px"] else 0.0
        pnl = p["banked_pnl"] + SIZE_SOL * (mult - 1.0)
        all_mult.append(mult)
        trades.append({"mint": mint[:12], "reason": reason,
                       "entry_px": p["entry_px"], "exit_px": px,
                       "mult": round(mult, 3), "pnl_sol": round(pnl, 5),
                       "held_s": round(ts - p["entry_ts"], 0)})
        exit_reasons[reason] += 1
        stats["wins" if pnl >= 0 else "losses"] += 1

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

                # consensus bookkeeping (only buys add distinct wallets)
                if action == "buy" and signer:
                    if mint not in first_buy:
                        first_buy[mint] = ts
                    if ts - first_buy[mint] <= MAX_CANDIDATE_AGE_S:
                        buyers[mint][signer] = ts

                # entry trigger: consensus reached on a buy
                if action == "buy" and mint not in open_pos:
                    try_open(mint, price, liq, ts)

                # monitor open position for this mint
                if mint in open_pos and price:
                    p = open_pos[mint]
                    p["peak"] = max(p["peak"], price)
                    peak_mult = p["peak"] / p["entry_px"]
                    mult = price / p["entry_px"]
                    if not p["tp1_banked"] and peak_mult >= TP1_MULT:
                        p["tp1_banked"] = True
                        p["banked_pnl"] = SIZE_SOL * 0.5 * (TP1_MULT - 1.0)
                    if p["tp1_banked"] and not p["be_armed"]:
                        p["be_armed"] = True
                    stop_mult = 1.0 - HARD_STOP
                    if p["be_armed"]:
                        stop_mult = max(stop_mult, 1.0 + BE_BUFFER)
                    if mult <= stop_mult:
                        close(mint, "sl", price, ts)
                    elif peak_mult >= TRAIL_START_MULT and price <= p["peak"] * (1 - TRAIL_RETRACE):
                        close(mint, "trail", price, ts)
                    elif ts - p["entry_ts"] > MAX_HOLD_S:
                        close(mint, "max_hold", price, ts)

                if args.max_trades and stats["entries"] >= args.max_trades:
                    break
            if args.max_trades and stats["entries"] >= args.max_trades:
                break
        if args.max_trades and stats["entries"] >= args.max_trades:
            break

    wins = stats["wins"]; losses = stats["losses"]
    entries = stats["entries"]
    pnl = sum(t["pnl_sol"] for t in trades)
    am = sorted(all_mult)
    def pct(x):
        return round(am[min(len(am) - 1, int(x * len(am)))], 2) if am else 0.0
    res = {
        "data": args.data, "rows": total,
        "params": {"tp1": TP1_MULT, "trail_start": TRAIL_START_MULT,
                    "trail_retrace": TRAIL_RETRACE, "hard_stop": HARD_STOP,
                    "min_consensus": args.min_consensus, "min_liq": args.min_liq,
                    "max_hold_h": MAX_HOLD_S / 3600, "size_sol": SIZE_SOL},
        "funnel": {k: stats[k] for k in
                   ("launches", "entries", "wins", "losses", "low_liq", "pos_cap")},
        "exit_reasons": dict(exit_reasons),
        "mult_p10": pct(0.10), "mult_p50": pct(0.50), "mult_p90": pct(0.90),
        "mult_max": round(max(am), 1) if am else 0.0,
        "winrate": round(wins / entries * 100, 1) if entries else 0.0,
        "trades": len(trades), "pnl_sol": round(pnl, 4),
        "avg_pnl_sol": round(pnl / entries, 5) if entries else 0.0,
        "top_trades": sorted(
            [{"mint": t["mint"], "entry_px": t["entry_px"], "exit_px": t["exit_px"],
              "mult": t["mult"], "reason": t["reason"], "pnl_sol": t["pnl_sol"]}
             for t in trades], key=lambda x: x["mult"], reverse=True)[:12],
        "seconds": round(time.time() - t0, 1),
    }
    print(json.dumps(res, indent=2))
    with open(args.out, "w") as f:
        json.dump(res, f, indent=2)


if __name__ == "__main__":
    main()
