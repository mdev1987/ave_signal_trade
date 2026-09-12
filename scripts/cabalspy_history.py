"""CabalSpy signal backtest — offline, read-only, ~4 API calls.

Answers: do KOL clusters precede moves, and does our min_win_rate gate
narrow the feed (gated-mode AND check)? Correlates historical signals
with our own shadow closes + DexScreener h24 as a rough post-signal proxy
(valid only for signals <24h old).

Usage:
    uv run python scripts/cabalspy_history.py [--days 7] [--no-prices]
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import json
import statistics
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

import config as cfg  # noqa: E402
from cabalspy_rest import CabalSpyREST, parse_api_date  # noqa: E402


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--blockchain", default="solana")
    ap.add_argument("--no-prices", action="store_true",
                    help="skip DexScreener h24 proxy (faster, no extra deps)")
    ap.add_argument("--book", default="shadow_book.json")
    args = ap.parse_args()

    env = cfg.load_env()
    keys = (cfg.get(env, "CABALSPY_API_KEY", "") or "").split(",")
    rest = CabalSpyREST(api_keys=keys)
    try:
        all_sig: list[dict] = []
        per_type: dict[str, int] = {}
        for wtype in ("kol", "smart"):
            sigs = await rest.signals_history(
                blockchain=args.blockchain, wallet_type=wtype,
                days=args.days, min_wallets=3, limit=100)
            per_type[wtype] = len(sigs)
            for s in sigs:
                s["_type"] = wtype
            all_sig.extend(sigs)
        print(f"history ({args.days}d): kol={per_type.get('kol', 0)} "
              f"smart={per_type.get('smart', 0)}")

        # Gated-mode A/B: same query with min_win_rate=50 (our live gate).
        # A collapse here means the live subscription is narrower than it looks.
        gated = await rest.signals_history(
            blockchain=args.blockchain, wallet_type="kol",
            days=args.days, min_wallets=3, min_win_rate=50, limit=100)
        print(f"gated min_win_rate=50: {len(gated)} "
              f"({len(gated) / max(per_type.get('kol', 1), 1) * 100:.0f}% of ungated kol)")

        if not all_sig:
            print("no signals returned")
            return 1

        wc = [s.get("wallet_count", 0) or 0 for s in all_sig]
        inv = [s.get("total_invested_usd") or s.get("total_invested") or 0
               for s in all_sig]
        strong = sum(1 for s in all_sig if s.get("signal_strength") == "strong")
        print(f"wallet_count: median={statistics.median(wc):.0f} max={max(wc)} | "
              f"strong={strong}/{len(all_sig)}")
        inv_nz = [v for v in inv if v]
        if inv_nz:
            print(f"invested_usd: median={statistics.median(inv_nz):.0f} "
                  f"max={max(inv_nz):.0f}")

        # Overlap with our own shadow closes (book + backup).
        ours: set[str] = set()
        for bp in (args.book, "shadow_book.bak-20260912-0905.json"):
            try:
                book = json.loads(Path(bp).read_text())
                ours.update(t.get("ca", "") for t in book.get("closed", []))
                ours.update(book.get("open", {}).keys())
            except (OSError, json.JSONDecodeError):
                pass
        hit = [s for s in all_sig if s.get("mint") in ours]
        print(f"overlap with our book: {len(hit)}/{len(all_sig)} traded")

        # h24 proxy for fresh signals only.
        if not args.no_prices:
            try:
                from dexscreener_oracle import DexScreenerClient
            except ImportError as e:
                print(f"prices skipped ({e})")
                return 0
            now = time.time()
            fresh = sorted(
                (s for s in all_sig
                 if s.get("triggered_at")
                 and 0 < now - parse_api_date(s["triggered_at"]).timestamp() < 86400),
                key=lambda s: s.get("total_invested_usd") or 0, reverse=True,
            )[:20]
            print(f"h24 proxy for {len(fresh)} fresh signals (<24h, top by invested):")
            ds = DexScreenerClient()
            try:
                ups = downs = 0
                for s in fresh:
                    snap = await ds.token_pairs("solana", s["mint"])
                    pc = ((snap or {}).get("price_change") or {}).get("h24")
                    mark = "?" if pc is None else ("+" if pc > 0 else "-")
                    if pc is not None:
                        ups += pc > 0
                        downs += pc <= 0
                    print(f"  {mark} {s['mint'][:10]} wallets={s.get('wallet_count')} "
                          f"h24={pc}")
                if ups + downs:
                    print(f"  up={ups} down={downs} "
                          f"({ups / (ups + downs) * 100:.0f}% green, rough proxy)")
            finally:
                await ds.close()
    finally:
        await rest.close()
    if rest.last_rate_limit:
        print("rate-limit:", rest.last_rate_limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
