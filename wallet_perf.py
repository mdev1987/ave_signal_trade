"""Rank smart-money wallets by performance via the SolanaTracker PnL V2 API.

Uses GET /v2/pnl/wallets/{wallet} (Data API key) to pull total/realized/unrealized
PnL, ROI, trade count, and win rate for every tracked wallet, then writes an
incrementally-updated JSON plus a final sorted ranking. Sequential with 429
backoff to respect Free-tier rate limits.

Usage:
    python wallet_perf.py
    python wallet_perf.py --wallets smart_money_wallets.json --out wallet_performance.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # noqa: BLE001
    pass


def _load_env():
    p = Path(".env")
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

BASE = os.getenv("SOLTRACKER_BASE_URL", "https://data.solanatracker.io").rstrip("/")
KEY = os.getenv("SOLTRACKER_API_KEY", "")
WALLETS_PATH = "smart_money_wallets.json"
OUT_PATH = "wallet_performance.json"


def fetch(addr: str, retries: int = 8) -> dict:
    import requests

    rec = {"address": addr, "ok": False}
    backoff = 2.0
    for _ in range(retries):
        try:
            r = requests.get(
                f"{BASE}/v2/pnl/wallets/{addr}",
                headers={"x-api-key": KEY},
                timeout=30,
            )
            if r.status_code == 200:
                d = r.json()
                idn = d.get("identity", {}) or {}
                summ = d.get("summary", {}) or {}
                pnl = summ.get("pnl", {}) or {}
                an = d.get("analysis", {}) or {}
                rec.update({
                    "ok": True,
                    "name": idn.get("name"),
                    "type": idn.get("type"),
                    "tags": idn.get("tags", []),
                    "pnl_total": pnl.get("total"),
                    "pnl_realized": pnl.get("realized"),
                    "pnl_unrealized": pnl.get("unrealized"),
                    "roi": summ.get("roi"),
                    "trades": (summ.get("counts", {}) or {}).get("trades"),
                    "win_rate": an.get("winRate"),
                    "avg_hold_secs": (summ.get("timing", {}) or {}).get("avgHoldTimeSecs"),
                })
                return rec
            if r.status_code == 429:
                time.sleep(backoff)
                backoff = min(backoff * 1.8, 25)
                continue
            rec["error"] = f"HTTP {r.status_code}: {r.text[:160]}"
            return rec
        except Exception as exc:  # noqa: BLE001
            rec["error"] = str(exc)[:200]
            time.sleep(2)
    return rec


def run(wallets_path: str, out_path: str, limit: int, delay: float) -> list[dict]:
    wallets = json.loads(Path(wallets_path).read_text())
    addrs = list(dict.fromkeys(wallets.keys()))[:limit]
    results: list[dict] = []
    if Path(out_path).exists():
        try:
            results = json.loads(Path(out_path).read_text())
        except Exception:
            results = []
    done = {r["address"] for r in results if r.get("ok")}
    todo = [a for a in addrs if a not in done]
    print(f"[perf] {len(addrs)} wallets, {len(done)} cached, {len(todo)} to fetch", flush=True)

    for i, a in enumerate(todo, 1):
        rec = fetch(a)
        if rec.get("ok"):
            results.append(rec)
            Path(out_path).write_text(json.dumps(results, indent=2))
            print(f"  [{i}/{len(todo)}] {a[:8]} pnl={rec['pnl_total']} "
                  f"win={rec['win_rate']} trades={rec['trades']}", flush=True)
        else:
            print(f"  [fail] {a[:8]} {rec.get('error','')[:80]}", flush=True)
        time.sleep(delay)
    return results


def rank(results: list[dict]) -> list[dict]:
    good = [r for r in results if r.get("ok")]
    good.sort(key=lambda r: (r.get("pnl_total") or -1e18, r.get("win_rate") or -1),
              reverse=True)
    return good


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", default=WALLETS_PATH)
    ap.add_argument("--out", default=OUT_PATH)
    ap.add_argument("--limit", type=int, default=10_000)
    ap.add_argument("--delay", type=float, default=1.0)
    args = ap.parse_args()
    if not KEY:
        print("ERROR: SOLTRACKER_API_KEY not set", file=sys.stderr)
        return 2
    results = run(args.wallets, args.out, args.limit, args.delay)
    good = rank(results)
    md = Path(args.out).with_suffix(".ranked.md")
    lines = ["# Smart-money wallet ranking (SolanaTracker PnL V2)", "",
             f"Scored {len(good)} / {len(results)} wallets.", "",
             "| # | wallet | name | PnL $ | win % | ROI % | trades | avg hold s |",
             "|---|--------|------|-------|-------|-------|--------|-----------|"]
    for i, r in enumerate(good, 1):
        lines.append(
            f"| {i} | `{r['address'][:8]}…{r['address'][-4:]}` | {r.get('name') or ''} | "
            f"{r.get('pnl_total')} | {r.get('win_rate')} | {r.get('roi')} | "
            f"{r.get('trades')} | {r.get('avg_hold_secs')} |"
        )
    md.write_text("\n".join(lines))
    print(f"[ok] {len(good)} ranked -> {args.out} + {md}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
