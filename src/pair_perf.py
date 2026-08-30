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
    d = perf.get(key, {"trades": 0, "wins": 0, "pnl": 0.0, "history": []})
    d["trades"] = d.get("trades", 0) + 1
    if pnl_sol > 0:
        d["wins"] = d.get("wins", 0) + 1
    d["pnl"] = round(d.get("pnl", 0.0) + pnl_sol, 5)
    # Rolling history: keep last 10 trades for recent-performance multiplier
    hist = d.get("history", [])
    hist.append({"pnl": round(pnl_sol, 5), "ts": __import__("time").time()})
    d["history"] = hist[-10:]
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

    Uses a **rolling window** (last 10 trades) weighted 70% recent + 30%
    all-time so stale historical data doesn't permanently penalise a pair
    that has recovered. The multiplier is recomputed from live pair stats
    every close, so a pair that turns around un-restricts itself automatically.

    Also computes **expectancy per trade** (pnl / trades) as a first-class
    feature — not just cumulative PnL + win rate — so a pair with 10 trades
    at -0.001/trade is treated differently from one at -0.05/trade.
    """
    d = perf.get(pair_key(wallets))
    if not d or d.get("trades", 0) < 3:
        return 1.0, ""
    trades = d["trades"]
    pnl = d.get("pnl", 0.0)
    expectancy = pnl / trades  # pnl per trade (first-class feature)
    # Rolling window: last 10 trades, weighted 70% recent + 30% all-time
    hist = d.get("history", [])
    if len(hist) >= 3:
        recent_pnl = sum(h.get("pnl", 0.0) for h in hist)
        recent_wr = sum(1 for h in hist if h.get("pnl", 0.0) > 0) / len(hist)
        effective_pnl = recent_pnl * 0.7 + pnl * 0.3
        effective_wr = recent_wr * 0.7 + (d["wins"] / trades) * 0.3
    else:
        effective_pnl = pnl
        effective_wr = d["wins"] / trades
    # Multiplier tiers: based on effective (rolling) stats, not cumulative
    if effective_pnl >= 0:
        return 1.0, ""  # pair currently profitable -> no discount
    # Very weak: 6+ recent trades, low win rate, bad expectancy
    if len(hist) >= 6 and effective_wr < 0.45:
        return 0.5, (f"pair_weak(t={trades},rec={len(hist)},wr={effective_wr:.0%},"
                     f"exp={expectancy:+.4f})")
    if len(hist) >= 5 and effective_wr < 0.40:
        return 0.6, (f"pair_weak(t={trades},rec={len(hist)},wr={effective_wr:.0%},"
                     f"exp={expectancy:+.4f})")
    if len(hist) >= 3:
        return 0.7, (f"pair_soft(t={trades},rec={len(hist)},wr={effective_wr:.0%},"
                     f"exp={expectancy:+.4f})")
    return 1.0, ""
