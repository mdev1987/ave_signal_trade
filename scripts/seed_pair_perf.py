"""Seed pair_performance.json from journal shadow_close events.

Reads bot_logs/journal.json and bot_logs/trade_log.csv to build
wallet-pair performance statistics. Run after initial period to give
pair_multiplier meaningful stats faster.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

JOURNAL = Path("bot_logs/journal.json")
TRADE_CSV = Path("bot_logs/trade_log.csv")
PAIR_FILE = Path("pair_performance.json")


def seed() -> None:
    # Load existing pair performance
    pair_data: dict[str, dict] = {}
    if PAIR_FILE.exists():
        try:
            pair_data = json.loads(PAIR_FILE.read_text())
        except Exception:
            pair_data = {}

    # Seed from journal shadow_close events
    journal_pairs = 0
    if JOURNAL.exists():
        with open(JOURNAL) as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if ev.get("event") != "shadow_close":
                    continue
                wallets = ev.get("wallets", [])
                pnl = ev.get("pnl_sol", 0.0)
                if len(wallets) < 2:
                    continue
                # Create sorted pair key from wallet addresses
                wallets_sorted = sorted(wallets)
                pair_key = "+".join(wallets_sorted)
                if pair_key not in pair_data:
                    pair_data[pair_key] = {
                        "trades": 0, "wins": 0, "pnl": 0.0, "history": []
                    }
                pair_data[pair_key]["trades"] += 1
                pair_data[pair_key]["pnl"] += pnl
                if pnl > 0:
                    pair_data[pair_key]["wins"] += 1
                pair_data[pair_key]["history"].append({
                    "pnl": pnl, "ts": ev.get("ts", 0)
                })
                journal_pairs += 1

    # Seed from trade_log.csv if available
    csv_pairs = 0
    if TRADE_CSV.exists():
        try:
            with open(TRADE_CSV) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    wallets_raw = row.get("wallets", "")
                    pnl = float(row.get("pnl_sol", 0) or 0)
                    if not wallets_raw or pnl == 0:
                        continue
                    # Parse wallets from CSV (may be in different formats)
                    try:
                        wallets = json.loads(wallets_raw) if wallets_raw.startswith("[") else [w.strip() for w in wallets_raw.split(",")]
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if len(wallets) < 2:
                        continue
                    pair_key = "+".join(sorted(wallets))
                    if pair_key not in pair_data:
                        pair_data[pair_key] = {
                            "trades": 0, "wins": 0, "pnl": 0.0, "history": []
                        }
                    pair_data[pair_key]["trades"] += 1
                    pair_data[pair_key]["pnl"] += pnl
                    if pnl > 0:
                        pair_data[pair_key]["wins"] += 1
                    pair_data[pair_key]["history"].append({
                        "pnl": pnl, "ts": 0
                    })
                    csv_pairs += 1
        except Exception:
            pass

    PAIR_FILE.write_text(json.dumps(pair_data, indent=1))
    total = len(pair_data)
    print(f"seeded {journal_pairs} journal + {csv_pairs} csv entries")
    print(f"total pairs in perf: {total}")


if __name__ == "__main__":
    seed()
