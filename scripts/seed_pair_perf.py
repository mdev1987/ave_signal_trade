"""Seed pair_performance.json from a past journal so the pair-aware penalty is
active on the very next run (otherwise it would take several more losing trades
to engage). Reads bot_logs/journal.json, joins shadow_open->shadow_close by CA,
and writes the per-pair expectancy store consumed by src/pair_perf.py.

Usage: python seed_pair_perf.py [journal_path] [out_path]
"""
from __future__ import annotations
import json
import sys
from collections import defaultdict
from pathlib import Path

JOURNAL = sys.argv[1] if len(sys.argv) > 1 else "bot_logs/journal.json"
OUT = sys.argv[2] if len(sys.argv) > 2 else "pair_performance.json"


def main() -> None:
    # Use a list of openings per CA to handle multiple trades on the same token.
    opens: dict[str, list[list[str]]] = defaultdict(list)
    closes: list[dict] = []
    with open(JOURNAL) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            e = json.loads(line)
            if e.get("event") == "shadow_open":
                opens[e["ca"]].append(list(e.get("wallets") or []))
            elif e.get("event") == "shadow_close":
                closes.append(e)

    # Match closes to opens by CA, consuming one opening per close.
    perf = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
    for cl in closes:
        ca = cl.get("ca", "")
        if ca not in opens or not opens[ca]:
            continue
        wallets = opens[ca].pop(0)  # consume first unmatched opening
        key = "+".join(sorted(set(wallets)))
        d = perf[key]
        d["trades"] += 1
        pnl = float(cl.get("pnl_sol", 0.0))
        if pnl > 0:
            d["wins"] += 1
        d["pnl"] = round(d["pnl"] + pnl, 5)

    Path(OUT).write_text(json.dumps(dict(perf), indent=2))
    print(f"seeded {len(perf)} pairs -> {OUT}")
    for key, d in sorted(perf.items(), key=lambda x: x[1]["pnl"]):
        if d["trades"] >= 2:
            wr = d["wins"] / d["trades"]
            print(f"  n={d['trades']} wr={wr:.0%} pnl={d['pnl']:+.4f}  {key[:48]}")


if __name__ == "__main__":
    main()
