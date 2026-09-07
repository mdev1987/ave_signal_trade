"""Recalculate wallet weights from journal shadow_close events.

Reads bot_logs/journal.json, extracts per-wallet win rates and PnL from
shadow_close events, and merges with existing wallet_performance.json.
Run nightly or manually: ``python scripts/recalc_wallet_weights.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

JOURNAL = Path("bot_logs/journal.json")
PERF_FILE = Path("wallet_performance.json")
OUT_FILE = Path("wallet_performance.json")  # overwrite in place


def recalc() -> None:
    if not JOURNAL.exists():
        print(f"journal not found: {JOURNAL}")
        return

    # Collect per-wallet stats from shadow_close events
    wallet_stats: dict[str, dict] = {}  # addr -> {trades, wins, pnl}
    with open(JOURNAL) as f:
        for line in f:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            if ev.get("event") != "shadow_close":
                continue
            pnl = ev.get("pnl_sol", 0.0)
            for addr in ev.get("wallets", []):
                if addr not in wallet_stats:
                    wallet_stats[addr] = {"trades": 0, "wins": 0, "pnl": 0.0}
                wallet_stats[addr]["trades"] += 1
                wallet_stats[addr]["pnl"] += pnl
                if pnl > 0:
                    wallet_stats[addr]["wins"] += 1

    if not wallet_stats:
        print("no shadow_close events found in journal")
        return

    # Load existing performance data
    existing: dict[str, dict] = {}
    if PERF_FILE.exists():
        try:
            data = json.loads(PERF_FILE.read_text())
            if isinstance(data, list):
                for rec in data:
                    addr = rec.get("address", "")
                    if addr:
                        existing[addr] = rec
            elif isinstance(data, dict):
                existing = data
        except Exception:
            pass

    # Merge: update win_rate, trades, pnl from journal stats
    updated = 0
    for addr, stats in wallet_stats.items():
        if stats["trades"] == 0:
            continue
        wr = (stats["wins"] / stats["trades"]) * 100.0
        if addr in existing:
            rec = existing[addr]
            # Blend with existing SolanaTracker data (weighted average)
            old_trades = rec.get("trades", 0) or rec.get("picks", 0) or 0
            old_wr = rec.get("win_rate", 0.0)
            # Give more weight to local data (4x multiplier for recent trades)
            local_weight = stats["trades"] * 4
            total_weight = old_trades + local_weight
            if total_weight > 0:
                blended_wr = (old_wr * old_trades + wr * local_weight) / total_weight
                rec["win_rate"] = round(blended_wr, 2)
                rec["trades"] = total_weight
                rec["pnl_total"] = (rec.get("pnl_total", 0.0) or 0.0) + stats["pnl"]
                updated += 1
        else:
            # New wallet from journal only
            existing[addr] = {
                "address": addr,
                "ok": True,
                "name": addr[:8],
                "type": "kol",
                "pnl_total": stats["pnl"],
                "win_rate": round(wr, 2),
                "trades": stats["trades"],
            }
            updated += 1

    # Write back as list (canonical format)
    out = [rec for rec in existing.values() if rec.get("address")]
    OUT_FILE.write_text(json.dumps(out, indent=1))
    print(f"updated {updated} wallets from {len(wallet_stats)} journal entries")
    print(f"total wallets in perf: {len(out)}")


if __name__ == "__main__":
    recalc()
