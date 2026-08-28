"""Upper-bound test: if we had a PERFECT smart-wallet list, is the strategy
profitable under realistic constraints?

Pass 1 (lookahead / cheating): score every buyer signer by how many distinct
mints they bought that later pumped >= PUMP_MULT (1.5x) within MAX_HOLD_S.
Pick top TOP_K signers.

Pass 2: require >= MIN_CONSENSUS of those TOP_K wallets to buy a mint within
CONSENSUS_WINDOW_S, then run the SAME hardened exits as main.py ShadowBook,
with a realistic position cap.

This tells us the ceiling a good discovery scorer must reach.
"""
from __future__ import annotations
import argparse, glob, json, time
from collections import defaultdict
from pathlib import Path
import pyarrow.parquet as pq

SIZE_SOL = 0.05
TP1_MULT = 1.50
TRAIL_START_MULT = 1.30
TRAIL_RETRACE = 0.35
HARD_STOP = 0.40
BE_BUFFER = 0.0
MAX_HOLD_S = 72 * 3600.0
MIN_LIQ_USD = 5000.0
SOL_USD = 150.0
PUMP_MULT = 1.5
CONSENSUS_WINDOW_S = 600.0
MAX_CANDIDATE_AGE_S = 90 * 60.0
COLS = ["action", "mint", "txSigner", "price", "quoteInPool", "timestamp"]


def pass1_score(files):
    mint_first = {}      # mint -> (first_px, first_ts)
    mint_peak = {}       # mint -> peak_px
    mint_signers = defaultdict(set)
    for f in files:
        pf = pq.ParquetFile(f)
        for rb in pf.iter_batches(batch_size=500_000, columns=COLS):
            d = rb.to_pydict(); n = len(d["action"])
            for i in range(n):
                a = d["action"][i]; m = d["mint"][i]; ts = (d["timestamp"][i] or 0) / 1000.0
                px = d["price"][i] or 0.0
                if a == "create":
                    continue
                if m is None:
                    continue
                if a == "buy":
                    if m not in mint_first:
                        mint_first[m] = (px, ts)
                    mint_peak[m] = max(mint_peak.get(m, 0.0), px)
                    s = d["txSigner"][i]
                    if s:
                        mint_signers[m].add(s)
                else:  # sell still updates peak (price field is the trade price)
                    mint_peak[m] = max(mint_peak.get(m, 0.0), px)
    wins = defaultdict(int); totals = defaultdict(int)
    for m, sigs in mint_signers.items():
        fp = mint_first.get(m, (0.0, 0.0))[0]
        peak = mint_peak.get(m, 0.0)
        pumped = fp > 0 and peak / fp >= PUMP_MULT
        for s in sigs:
            totals[s] += 1
            if pumped:
                wins[s] += 1
    scored = [(wins[s] / max(totals[s], 1), wins[s], totals[s], s) for s in totals]
    scored.sort(reverse=True)
    return scored


def backtest(files, chosen, max_pos, min_consensus):
    buyers = defaultdict(dict)
    first_buy = {}
    open_pos = {}
    trades = []; reasons = defaultdict(int); stats = defaultdict(int)

    def close(m, reason, px, ts):
        p = open_pos.pop(m); mult = px / p["entry_px"]
        pnl = p["banked"] + SIZE_SOL * (mult - 1.0)
        trades.append({"pnl": pnl}); reasons[reason] += 1
        stats["wins" if pnl >= 0 else "losses"] += 1
    for f in files:
        pf = pq.ParquetFile(f)
        for rb in pf.iter_batches(batch_size=500_000, columns=COLS):
            d = rb.to_pydict(); n = len(d["action"])
            for i in range(n):
                a = d["action"][i]; m = d["mint"][i]
                ts = (d["timestamp"][i] or 0) / 1000.0
                if m is None or a not in ("buy", "sell"):
                    continue
                px = d["price"][i] or 0.0
                liq = (d["quoteInPool"][i] or 0.0) * 2 * SOL_USD
                s = d["txSigner"][i]
                if a == "buy" and s in chosen:
                    if m not in first_buy:
                        first_buy[m] = ts
                    if ts - first_buy[m] <= MAX_CANDIDATE_AGE_S:
                        buyers[m][s] = ts
                if a == "buy" and m not in open_pos:
                    if len(open_pos) >= max_pos:
                        stats["pos_cap"] += 1; continue
                    distinct = [x for x, bt in buyers[m].items() if ts - bt <= CONSENSUS_WINDOW_S]
                    if len(distinct) < min_consensus:
                        continue
                    if liq < MIN_LIQ_USD:
                        stats["low_liq"] += 1; continue
                    open_pos[m] = {"entry_px": px, "entry_ts": ts, "peak": px,
                                   "tp1": False, "be": False, "banked": 0.0}
                    stats["entries"] += 1
                if m in open_pos and px:
                    p = open_pos[m]; p["peak"] = max(p["peak"], px)
                    pm = p["peak"] / p["entry_px"]; mult = px / p["entry_px"]
                    if not p["tp1"] and pm >= TP1_MULT:
                        p["tp1"] = True; p["banked"] = SIZE_SOL * 0.5 * (TP1_MULT - 1.0)
                    if p["tp1"]:
                        p["be"] = True
                    stop = 1.0 - HARD_STOP
                    if p["be"]:
                        stop = max(stop, 1.0 + BE_BUFFER)
                    if mult <= stop:
                        close(m, "sl", px, ts)
                    elif pm >= TRAIL_START_MULT and px <= p["peak"] * (1 - TRAIL_RETRACE):
                        close(m, "trail", px, ts)
                    elif ts - p["entry_ts"] > MAX_HOLD_S:
                        close(m, "max_hold", px, ts)
    pnl = sum(t["pnl"] for t in trades)
    e = stats["entries"] or 1
    return {"entries": stats["entries"], "wins": stats["wins"], "losses": stats["losses"],
            "low_liq": stats["low_liq"], "pos_cap": stats["pos_cap"],
            "winrate": round(stats["wins"] / e * 100, 1),
            "pnl_sol": round(pnl, 4), "avg_pnl": round(pnl / e, 5),
            "exit_reasons": dict(reasons)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--top-k", type=int, default=30)
    ap.add_argument("--min-consensus", type=int, default=2)
    ap.add_argument("--max-positions", type=int, default=10)
    ap.add_argument("--out", default="/tmp/bt_ideal.json")
    args = ap.parse_args()
    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    t0 = time.time()
    scored = pass1_score(files)
    chosen = set(s for _, _, _, s in scored[:args.top_k])
    print(f"top-{args.top_k} wallets chosen (winrate/win/total):")
    for wr, w, tot, s in scored[:args.top_k]:
        print(f"  {s[:14]} wr={wr:.2f} wins={w} buys={tot}")
    res = backtest(files, chosen, args.max_positions, args.min_consensus)
    res["seconds"] = round(time.time() - t0, 1)
    res["top_k"] = args.top_k; res["max_positions"] = args.max_positions
    print(json.dumps(res, indent=2))
    json.dump(res, open(args.out, "w"), indent=2)


if __name__ == "__main__":
    main()
