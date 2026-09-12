"""Journal analyzer — one-command review of bot performance.

Reads bot_logs/journal.json (and optional archives) plus shadow_book.json
and prints: trade expectancy (ex-oracle), breakdowns by reason/source,
entry funnel, MadeOnSol overlap hit-rate, wallet concentration.

Usage:
    uv run python scripts/analyze_journal.py
    uv run python scripts/analyze_journal.py --journal bot_logs/journal-20260912-120000.json
    uv run python scripts/analyze_journal.py --book shadow_book.bak-20260912-0905.json
"""

from __future__ import annotations

import argparse
import collections
import datetime
import glob
import json
import statistics
import sys


def load_events(paths: list[str]) -> list[dict]:
    events = []
    for p in paths:
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        except OSError as e:
            print(f"warn: cannot read {p}: {e}", file=sys.stderr)
    return events


def load_closes(book_path: str | None) -> list[dict]:
    """Closed trades, tagged with origin (journal shadow_close vs book)."""
    closes: list[dict] = []
    if book_path:
        try:
            with open(book_path, encoding="utf-8") as f:
                book = json.load(f)
            for t in book.get("closed", []):
                t = dict(t)
                t["_origin"] = "book"
                closes.append(t)
        except (OSError, json.JSONDecodeError) as e:
            print(f"warn: cannot read book {book_path}: {e}", file=sys.stderr)
    return closes


def fmt_ts(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")


def expectancy_row(trades: list[dict], label: str) -> None:
    n = len(trades)
    if not n:
        print(f"  {label}: no trades")
        return
    pnls = [t.get("pnl_sol", 0) or 0 for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    tot = sum(pnls)
    print(f"  {label}: n={n} wins={wins} ({wins / n * 100:.0f}%) "
          f"pnl={tot:+.4f} avg={tot / n:+.5f}/trade")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--journal", default="bot_logs/journal.json")
    ap.add_argument("--archives", action="store_true",
                    help="include bot_logs/journal-*.json archives")
    ap.add_argument("--book", default="shadow_book.json")
    args = ap.parse_args()

    paths = [args.journal]
    if args.archives:
        paths += sorted(glob.glob("bot_logs/journal-*.json"))
    events = load_events(paths)
    if not events:
        print("no journal events found")
        return 1

    ts = [e.get("ts", 0) for e in events if e.get("ts")]
    print(f"range: {fmt_ts(min(ts))} -> {fmt_ts(max(ts))} "
          f"({len(events)} events)")

    by_event = collections.Counter(e.get("event", "?") for e in events)
    print("\nevents:")
    for k, v in by_event.most_common():
        print(f"  {v:6}  {k}")

    # --- closes: journal shadow_close + book (dedupe by ca, prefer book) ---
    jcloses = [e for e in events if e.get("event") == "shadow_close"]
    book_closes = load_closes(args.book)
    seen = {t.get("ca") for t in book_closes}
    closes = list(book_closes) + [t for t in jcloses if t.get("ca") not in seen]
    print(f"\ncloses: {len(closes)} ({len(book_closes)} book + "
          f"{len(closes) - len(book_closes)} journal-only)")

    print("expectancy:")
    expectancy_row(closes, "all")
    real = [t for t in closes if not t.get("oracle_fail")]
    expectancy_row(real, "ex-oracle-fail")

    print("by reason:")
    by_reason: dict[str, list] = collections.defaultdict(list)
    for t in closes:
        by_reason[str(t.get("reason", "?"))].append(t)
    for r, g in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
        tot = sum(t.get("pnl_sol", 0) or 0 for t in g)
        print(f"  {r:>16}: n={len(g):3} pnl={tot:+.4f}")

    print("by source:")
    by_src: dict[str, list] = collections.defaultdict(list)
    for t in closes:
        by_src[str(t.get("source", "?"))].append(t)
    for s_, g in sorted(by_src.items(), key=lambda kv: -len(kv[1])):
        tot = sum(t.get("pnl_sol", 0) or 0 for t in g)
        w = sum(1 for t in g if (t.get("pnl_sol", 0) or 0) > 0)
        print(f"  {s_:>16}: n={len(g):3} wins={w} pnl={tot:+.4f}")

    # --- funnel ---
    mom = [e for e in events if e.get("event") == "open_signal_momentum"]
    liq = [e for e in events if e.get("event") == "open_liq_unchecked"]
    pjo = [e for e in events if e.get("event") == "pumpapi_journal_only"]
    pb = [e for e in events if e.get("event") == "pullback_hold"]
    print(f"\nfunnel: momentum-pass={len(mom)} liq-unchecked={len(liq)} "
          f"pullback-hold={len(pb)} pumpapi-journal-only={len(pjo)} "
          f"opens->closes={len(closes)}")
    if pjo:
        sc = [e.get("score", 0) or 0 for e in pjo]
        print(f"  journal-only scores: min={min(sc):.2f} "
              f"median={statistics.median(sc):.2f} max={max(sc):.2f}")

    # --- MadeOnSol overlap vs outcomes ---
    ov = [e for e in events if e.get("event") == "madeonsol_overlap"]
    by_ca: dict[str, list] = collections.defaultdict(list)
    for e in ov:
        by_ca[e.get("ca", "")].append(e)
    print(f"\nmadeonsol: {len(ov)} overlap events on {len(by_ca)} tokens")
    close_by_ca = {t.get("ca"): t for t in closes}
    hit, hit_pnl, miss = 0, 0.0, 0
    for ca in by_ca:
        t = close_by_ca.get(ca)
        if t is None:
            miss += 1
        else:
            hit += 1
            hit_pnl += t.get("pnl_sol", 0) or 0
    print(f"  overlapped tokens that later closed: {hit} "
          f"(pnl={hit_pnl:+.4f}); never opened: {miss}")
    if hit:
        print(f"  overlap-trade avg: {hit_pnl / hit:+.5f}/trade")

    # --- wallet concentration ---
    buys = [e for e in events if e.get("event") == "smart_buy_seen"]
    if buys:
        wc = collections.Counter(e.get("wallet", "?") for e in buys)
        top, n = wc.most_common(1)[0]
        print(f"\nwallets: {len(buys)} buys from {len(wc)} wallets; "
              f"top={top[:10]} {n} ({n / len(buys) * 100:.0f}% of feed)")
        print("  top 5:")
        for w, c in wc.most_common(5):
            print(f"    {c:6}  {w[:12]}")

    # --- pullback / vybe signal events ---
    for ev in ("pullback_hold", "vybe_sell_pressure"):
        g = [e for e in events if e.get("event") == ev]
        if g:
            print(f"\n{ev}: {len(g)}")

    print("\nrule of thumb: judge only post-fix trades (book reset 2026-09-12); "
          "need 30+ closes before tuning.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
