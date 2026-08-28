"""Sweep exit ladders x consensus x wallet-quality on one replay hour to find a
robust, backtest-validated strategy config. Reads the parquet ONCE, then runs
every combo as a single streaming pass (fast).

Exit ladder model (scale-out):
  tps: list[(mult, frac)]  -> when peak >= mult, sell `frac` of remaining size
                             at that mult (banked). Once any TP taken, stop rises
                             to breakeven (+be_buffer) on the remaining size.
  trailing: after peak >= trail_start, close remaining if px <= peak*(1-retr).
  hard: close remaining if px <= (1-hard_stop).
  max_hold: close remaining after max_hold_s.

PnL per trade (SOL) = banked + remaining*size*(exit_mult-1).
"""
from __future__ import annotations
import argparse, glob, json, time, sys
from collections import defaultdict
from pathlib import Path
import pyarrow.parquet as pq

SIZE_SOL = 0.05
SOL_USD = 150.0
MAX_HOLD_S = 72 * 3600.0
MIN_LIQ_USD = 5000.0
CONSENSUS_WINDOW_S = 600.0
MAX_CANDIDATE_AGE_S = 90 * 60.0

# exit ladder configs to sweep
EXITS = {
    "E1_current": dict(tps=[(1.5, 0.5)], trail_start=1.3, trail_retr=0.35,
                       hard=0.40, be_buffer=0.0),
    "E2_aggro":   dict(tps=[(1.25, 0.5), (1.6, 0.3), (2.5, 0.2)],
                       trail_start=1.5, trail_retr=0.30, hard=0.35, be_buffer=0.0),
    "E3_fullspike": dict(tps=[(1.3, 1.0)], trail_start=99, trail_retr=0.0,
                         hard=0.35, be_buffer=0.0),
    "E4_ladder_tight": dict(tps=[(1.3, 0.4), (1.8, 0.3), (3.0, 0.3)],
                            trail_start=1.4, trail_retr=0.25, hard=0.35,
                            be_buffer=0.0),
}
CONSENSUS_LEVELS = [2, 3, 4]
WALLET_MODES = ["any", "ideal30"]


def load_events(files):
    ev = []
    for f in files:
        pf = pq.ParquetFile(f)
        for rb in pf.iter_batches(batch_size=500_000,
                                   columns=["action", "mint", "txSigner",
                                            "price", "quoteInPool", "timestamp"]):
            d = rb.to_pydict(); n = len(d["action"])
            for i in range(n):
                a = d["action"][i]; m = d["mint"][i]
                if m is None or a not in ("buy", "sell", "create"):
                    continue
                ts = (d["timestamp"][i] or 0) / 1000.0
                px = d["price"][i] or 0.0
                liq = (d["quoteInPool"][i] or 0.0) * 2 * SOL_USD
                ev.append((ts, m, a, px, liq, d["txSigner"][i]))
    ev.sort(key=lambda x: x[0])
    return ev


def ideal_wallets(ev, topk=30, pump_mult=1.5):
    first = {}; peak = {}; sigs = defaultdict(set)
    for ts, m, a, px, liq, s in ev:
        if a == "create":
            continue
        if a == "buy":
            if m not in first:
                first[m] = px
            peak[m] = max(peak.get(m, 0.0), px)
            if s:
                sigs[m].add(s)
        else:
            peak[m] = max(peak.get(m, 0.0), px)
    wins = defaultdict(int); tot = defaultdict(int)
    for m, ss in sigs.items():
        pumped = first.get(m, 0) > 0 and peak.get(m, 0) / first[m] >= pump_mult
        for s in ss:
            tot[s] += 1
            if pumped:
                wins[s] += 1
    scored = sorted(tot, key=lambda s: (wins[s] / max(tot[s], 1), wins[s]),
                    reverse=True)
    return set(scored[:topk])


def run_combo(ev, exit_cfg, consensus, wallet_mode, chosen, cap):
    buyers = defaultdict(dict); first_buy = {}
    open_pos = {}; trades = []; pos_cap = 0; low_liq = 0
    tps = exit_cfg["tps"]; trail_start = exit_cfg["trail_start"]
    trail_retr = exit_cfg["trail_retr"]; hard = exit_cfg["hard"]; beb = exit_cfg["be_buffer"]
    for ts, m, a, px, liq, s in ev:
        if a == "buy" and (wallet_mode == "any" or s in chosen):
            if m not in first_buy:
                first_buy[m] = ts
            if ts - first_buy[m] <= MAX_CANDIDATE_AGE_S:
                buyers[m][s] = ts
        # entry trigger
        if a == "buy" and m not in open_pos:
            if len(open_pos) >= cap:
                pos_cap += 1
            else:
                if wallet_mode == "any":
                    distinct = [x for x, bt in buyers[m].items()
                                if ts - bt <= CONSENSUS_WINDOW_S]
                else:
                    distinct = [x for x, bt in buyers[m].items()
                                if x in chosen and ts - bt <= CONSENSUS_WINDOW_S]
                if len(distinct) >= consensus:
                    if liq < MIN_LIQ_USD:
                        low_liq += 1
                    else:
                        open_pos[m] = {"entry": px, "entry_ts": ts, "peak": px,
                                       "taken": set(), "remaining": 1.0,
                                       "banked": 0.0, "be": False}
        # price update + exit check
        if m in open_pos and px > 0:
            p = open_pos[m]; p["peak"] = max(p["peak"], px)
            peak_mult = p["peak"] / p["entry"]; mult = px / p["entry"]
            for lvl, frac in tps:
                if lvl not in p["taken"] and peak_mult >= lvl:
                    p["taken"].add(lvl)
                    p["banked"] += frac * SIZE_SOL * (lvl - 1.0)
                    p["remaining"] -= frac
                    if p["remaining"] <= 1e-9:
                        p["remaining"] = 0.0
            if p["taken"] and not p["be"]:
                p["be"] = True
            exit_r = None
            stop = 1 - hard
            if p["be"]:
                stop = max(stop, 1.0 + beb)
            if p["remaining"] > 0:
                if mult <= stop:
                    exit_r = "sl"
                elif trail_start < 99 and peak_mult >= trail_start and \
                        mult <= peak_mult * (1 - trail_retr):
                    exit_r = "trail"
                elif (ts - p["entry_ts"]) > MAX_HOLD_S:
                    exit_r = "maxhold"
            if exit_r or p["remaining"] <= 0:
                pnl = p["banked"] + p["remaining"] * SIZE_SOL * (mult - 1.0)
                trades.append(pnl)
                del open_pos[m]
    wins = sum(1 for x in trades if x >= 0)
    e = len(trades) or 1
    return dict(entries=len(trades), winrate=round(wins / e * 100, 1),
                pnl_sol=round(sum(trades), 3),
                avg=round(sum(trades) / e, 5), pos_cap=pos_cap, low_liq=low_liq)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--cap", type=int, default=10)
    ap.add_argument("--topk", type=int, default=30)
    ap.add_argument("--only", default="", help="wm:C:exit,wm:C:exit to restrict")
    args = ap.parse_args()
    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    ev = load_events(files)
    chosen = ideal_wallets(ev, args.topk) if any(
        wm.startswith("ideal") for wm in WALLET_MODES) else set()
    only = set()
    for piece in args.only.split(","):
        if piece:
            only.add(tuple(piece.split(":")))
    print(f"events={len(ev):,} cap={args.cap} chosen={len(chosen)} "
          f"files={len(files)}")
    rows = []
    for wm in WALLET_MODES:
        ch = chosen if wm != "any" else set()
        for c in CONSENSUS_LEVELS:
            for name, ec in EXITS.items():
                if only and (wm, str(c), name) not in only:
                    continue
                r = run_combo(ev, ec, c, wm, ch, args.cap)
                rows.append((wm, c, name, r))
    # sort by pnl desc for cap-constrained 'any' (deployment-realistic)
    print(f"{'wallet':9} {'C':>2} {'exit':16} {'ent':>5} {'win%':>5} "
          f"{'pnl':>10} {'avg':>9} {'cap':>8} {'lowliq':>7}")
    for wm, c, name, r in sorted(rows, key=lambda x: -x[3]["pnl_sol"]):
        print(f"{wm:9} {c:>2} {name:16} {r['entries']:>5} {r['winrate']:>5} "
              f"{r['pnl_sol']:>10} {r['avg']:>9} {r['pos_cap']:>8} {r['low_liq']:>7}")


if __name__ == "__main__":
    main()
