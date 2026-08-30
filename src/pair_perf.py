"""Pair-aware performance tracking.

The journal proved that specific *wallet pairs* — not individual wallets — are
the real signal. AgmLJ+kEFiA produced 6 trades / 1 win / -0.0558 SOL while the
same wallets paired with *others* won big (AgmLJ+8MaVa -> +48.7%). We track
sorted wallet-pairs and penalise pairs with clearly negative expectancy so they
stop monopolising the book (e.g. the per-wallet cap still let that one pair take
6 of the slots).

Penalty is *adaptive*, not a hard blacklist, to avoid overfitting a tiny sample:
a pair only gets penalised after it has enough trades to be informative.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Tuple

DEFAULT_PATH = "pair_performance.json"


def load(path: str = DEFAULT_PATH) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save(perf: dict, path: str = DEFAULT_PATH) -> None:
    Path(path).write_text(json.dumps(perf, indent=2))


def pair_key(wallets: Iterable[str]) -> str:
    return "+".join(sorted(set(wallets or [])))


def update(perf: dict, wallets: Iterable[str], pnl_sol: float) -> str:
    key = pair_key(wallets)
    d = perf.get(key, {"trades": 0, "wins": 0, "pnl": 0.0})
    d["trades"] = d.get("trades", 0) + 1
    if pnl_sol > 0:
        d["wins"] = d.get("wins", 0) + 1
    d["pnl"] = round(d.get("pnl", 0.0) + pnl_sol, 5)
    perf[key] = d
    return key


def penalty(perf: dict, wallets: Iterable[str]) -> Tuple[float, str]:
    """Return (score_penalty, note).

    The penalty subtracts from the consensus score in the open gate; a large
    enough penalty pushes it below threshold, effectively blocking the pair.
    """
    d = perf.get(pair_key(wallets))
    if not d or d.get("trades", 0) < 3:
        return 0.0, ""
    trades = d["trades"]
    wr = d["wins"] / trades
    pnl = d.get("pnl", 0.0)
    if trades >= 6 and wr < 0.45:
        return 2.0, f"pair_block(t={trades},wr={wr:.0%},pnl={pnl:+.3f})"
    if trades >= 4 and wr < 0.40:
        return 1.5, f"pair_penalty(t={trades},wr={wr:.0%},pnl={pnl:+.3f})"
    if trades >= 3 and pnl < 0:
        return 0.8, f"pair_penalty_small(t={trades},wr={wr:.0%},pnl={pnl:+.3f})"
    return 0.0, ""
