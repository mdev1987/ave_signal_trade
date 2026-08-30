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

    Deprecated: pair quality is now modelled as a MULTIPLIER on the market
    score (see pair_multiplier) rather than an absolute veto, so a weak pair
    can still trade when the market confirms hard. Kept for backwards compat.
    """
    mult, note = pair_multiplier(perf, wallets)
    return (0.0 if mult >= 1.0 else (1.0 - mult) * 2.0), note


def pair_multiplier(perf: dict, wallets: Iterable[str]) -> Tuple[float, str]:
    """Return (score_multiplier, note).

    Pair quality modulates the *market* score instead of vetoing it:
      - normal / currently-profitable pair -> 1.0 (no effect)
      - weak pair (negative expectancy)     -> 0.5 .. 0.7

    A weak pair is therefore down-weighted but may still open when the market
    aligns (e.g. 4/4 uptrend) — exactly the "conditional gate" the review asked
    for — so we never hard-block on a small sample (AgmLJ+kEFiA was only 6
    trades). The multiplier is recomputed from live pair stats every close, so
    a pair that turns around un-restricts itself automatically.
    """
    d = perf.get(pair_key(wallets))
    if not d or d.get("trades", 0) < 3:
        return 1.0, ""
    trades = d["trades"]
    wr = d["wins"] / trades
    pnl = d.get("pnl", 0.0)
    if pnl >= 0:
        return 1.0, ""  # pair currently profitable -> no discount
    if trades >= 6 and wr < 0.45:
        return 0.5, f"pair_weak(t={trades},wr={wr:.0%},pnl={pnl:+.3f})"
    if trades >= 5 and wr < 0.40:
        return 0.6, f"pair_weak(t={trades},wr={wr:.0%},pnl={pnl:+.3f})"
    if trades >= 3:
        return 0.7, f"pair_soft(t={trades},wr={wr:.0%},pnl={pnl:+.3f})"
    return 1.0, ""
