"""Generate a pump-hit-rate-ranked smart-wallet candidate list from replay data.

This is the concrete realization of "rank wallets by whether their picks
actually pump" (the discovery scorer's target metric, which live APIs can't
compute directly). It scans the PumpAPI replay parquet, computes per-buyer
pump-hit-rate (mints they bought that later >= PUMP_MULT), and writes the
top-K to smart_money_wallets.candidates.json for the operator to merge.

NOT applied to the live wallet file automatically — the running bot keeps its
current 30 wallets. This is a deliverable to review/merge deliberately.
"""
from __future__ import annotations
import argparse, glob, json, time
from collections import defaultdict
from pathlib import Path
import pyarrow.parquet as pq

SOL_USD = 150.0
PUMP_MULT = 1.5
MIN_DISTINCT = 3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--topk", type=int, default=40)
    ap.add_argument("--out", default="smart_money_wallets.candidates.json")
    args = ap.parse_args()
    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    first = {}; peak = {}; sigs = defaultdict(set)   # mint -> buyers
    started = time.time()
    for f in files:
        pf = pq.ParquetFile(f)
        for rb in pf.iter_batches(batch_size=500_000,
                                   columns=["action", "mint", "txSigner",
                                            "price", "quoteInPool", "timestamp"]):
            d = rb.to_pydict(); n = len(d["action"])
            for i in range(n):
                a = d["action"][i]; m = d["mint"][i]
                if m is None or a not in ("buy", "sell"):
                    continue
                px = d["price"][i] or 0.0
                if a == "buy":
                    if m not in first:
                        first[m] = px
                    peak[m] = max(peak.get(m, 0.0), px)
                    s = d["txSigner"][i]
                    if s:
                        sigs[m].add(s)
                else:
                    peak[m] = max(peak.get(m, 0.0), px)
    # per-signer stats
    distinct = defaultdict(set); pumped = defaultdict(int)
    for m, ss in sigs.items():
        pumped_m = first.get(m, 0) > 0 and peak.get(m, 0) / first[m] >= PUMP_MULT
        for s in ss:
            distinct[s].add(m)
            if pumped_m:
                pumped[s] += 1
    now = time.time()
    rows = []
    for s, mset in distinct.items():
        dt = len(mset)
        if dt < MIN_DISTINCT:
            continue
        ph = pumped[s]
        rows.append({"address": s, "distinct_tokens": dt,
                     "pumped_hits": ph,
                     "pump_hit_rate": round(ph / dt, 3),
                     "score": round(0.55 * (ph / dt) + 0.45 * min(1.0, dt / 20.0), 4)})
    rows.sort(key=lambda r: (r["pump_hit_rate"], r["pumped_hits"]), reverse=True)
    top = rows[: args.topk]
    out = {}
    for st in top:
        out[st["address"]] = {
            "score": st["score"], "distinct_tokens": st["distinct_tokens"],
            "pumped_hits": st["pumped_hits"],
            "pump_hit_rate": st["pump_hit_rate"],
            "source": "replay_pump_hit_rate",
            "added_ts": now, "updated_ts": now,
        }
    Path(args.out).write_text(json.dumps(out, indent=1))
    print(f"scanned {sum(len(v) for v in sigs.values())} buyer-mints "
          f"in {time.time()-started:.0f}s")
    print(f"wrote {len(top)} candidate wallets -> {args.out}")
    for st in top[:10]:
        print(f"  {st['address'][:14]} rate={st['pump_hit_rate']:.2f} "
              f"pumped={st['pumped_hits']} tokens={st['distinct_tokens']}")


if __name__ == "__main__":
    main()
