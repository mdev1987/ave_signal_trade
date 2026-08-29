#!/usr/bin/env python3
"""Expand the smart-money watchlist from the SolanaTracker PnL V2 leaderboard.

The leaderboard endpoints rank real Solana traders by realised PnL / win rate
over a rolling window and return full wallet addresses plus metrics, so they
are a far better source of high-conviction wallets than scraping screenshots
or an ad-hoc CSV.

We query several windows + sort orders, apply quality filters that drop
one-hit wonders and arbitrage, dedupe, and merge the surviving addresses into
``smart_money_wallets.json`` (preserving any wallets already tracked). Run
``wallet_perf.py`` afterwards to (re)score every wallet and rebuild the
weighted-consensus map.

Usage:
    uv run discover_wallets.py [--limit 50] [--min-win 55] [--min-trades 50]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import requests

BASE = os.getenv("SOLTRACKER_BASE_URL", "https://data.solanatracker.io").rstrip("/")
API_KEY = os.getenv("SOLTRACKER_API_KEY", "")
WALLETS_FILE = "smart_money_wallets.json"

# (window_days, sort) combos -> broad coverage of consistently profitable wallets.
QUERIES = [
    (30, "realized"),
    (7, "realized"),
    (90, "realized"),
    (30, "win_percentage"),
    (30, "roi"),
]


def fetch(days: int, sort: str, limit: int, min_win: float,
          min_trades: int, max_single: float) -> list[dict]:
    params = {
        "days": days,
        "sort": sort,
        "direction": "desc",
        "limit": limit,
        "minWinRate": min_win,
        "minTrades": min_trades,
        "minDays": 10,
        "maxSingleTokenPct": max_single,
        "excludeArbitrage": "true",
        "pnlMode": "strict",
    }
    for attempt in range(5):
        try:
            r = requests.get(f"{BASE}/v2/pnl/leaderboard/top",
                             params=params, headers={"x-api-key": API_KEY},
                             timeout=30)
            if r.status_code == 429:
                time.sleep(2 + attempt * 2)
                continue
            r.raise_for_status()
            return r.json().get("traders", [])
        except Exception as e:  # noqa: BLE001
            if attempt == 4:
                print(f"  ! {days}d/{sort} failed: {e}", file=sys.stderr)
                return []
            time.sleep(2 + attempt * 2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--min-win", type=float, default=55.0)
    ap.add_argument("--min-trades", type=int, default=50)
    ap.add_argument("--max-single", type=float, default=40.0)
    args = ap.parse_args()

    if not API_KEY:
        sys.exit("SOLTRACKER_API_KEY not set")

    existing = json.load(open(WALLETS_FILE)) if os.path.exists(WALLETS_FILE) else {}
    seen: dict[str, dict] = {}
    for days, sort in QUERIES:
        rows = fetch(days, sort, args.limit, args.min_win, args.min_trades,
                     args.max_single)
        print(f"  {days}d/{sort}: {len(rows)} traders")
        for t in rows:
            w = t.get("wallet")
            if not w:
                continue
            meta = {
                "source": "soltracker_leaderboard",
                "winRate": round(t.get("winRate", 0.0), 2),
                "realized": round((t.get("period") or {}).get("realized", 0.0), 2),
                "roi": round((t.get("period") or {}).get("roi", 0.0), 2),
                "tokens": (t.get("tokens") or {}).get("closed", 0),
            }
            # Keep the best metrics seen for a wallet across queries.
            if w not in seen or meta["realized"] > seen[w]["realized"]:
                seen[w] = meta

    added = 0
    for w, meta in seen.items():
        if w in existing:
            # Preserve prior tracking; just enrich with discovered metrics.
            existing[w].setdefault("source", meta["source"])
            existing[w].update({k: v for k, v in meta.items() if k != "source"})
            continue
        existing[w] = {"tokens": [], "buys": 0, "usd": 0.0, "tiers": [],
                       "syms": [], **meta}
        added += 1

    json.dump(existing, open(WALLETS_FILE, "w"), indent=1)
    print(f"watchlist: {len(existing)} wallets (+{added} new, "
          f"{len(seen)} discovered)")


if __name__ == "__main__":
    main()
