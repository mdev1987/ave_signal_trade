"""Apply the data-backed filter to parsed signals.

The base rules live in :data:`models.FILTER` and were re-tuned on 2026-08-13
replay data with the HONEST engine semantics (``scripts/replay_tune.py``:
fresh-quote entries, realized exits, dead-pool writeoffs): mcap $5-20K +
Pumpfunamm + snipes>=3 + security score 0 gives n=105 trades/day, 32.4% of
positions realize the 4x take-profit, EV ≈ +101% per trade, win-to-3x ≈ 33%.
The edge is fat-tailed (median exit 1.49x; ~19% stop out at -70%; ~40% of
pools die mid-hold and are written off at their last mark) — it is NOT a
high-win-rate strategy. Single-day dataset: treat as directional evidence,
not proof. The mcap band and snipes/security thresholds can be adjusted per
deployment via ``FILTER_MCAP_USD_MIN``, ``FILTER_MCAP_USD_MAX``,
``FILTER_SNIPES_MIN`` and ``FILTER_SEC_SCORE_MAX`` in ``.env`` (read lazily
by :func:`models.get_filter`).
"""

from __future__ import annotations

from models import REASONS, Signal, get_filter


def check_signal(sig: Signal) -> tuple[bool, list[str]]:
    """Check one signal against the filter rules.

    Args:
        sig: The parsed signal.

    Returns:
        Tuple of (passed, reasons). ``passed`` is True only when the signal
        meets every rule; ``reasons`` lists every violated rule.
    """
    rules = get_filter()
    reasons: list[str] = []
    if not sig.ca:
        return False, [REASONS["no_ca"]]
    # Wildcard dex set (``FILTER_DEXS=*``) admits every dex — the row's real
    # dex name is still journaled and written into trade_log.csv so the best
    # venues can be measured from real trades.
    if "*" not in rules["dexes"] and sig.dex not in rules["dexes"]:
        reasons.append(REASONS["dex"])
    if not (rules["mcap_usd_min"] <= sig.mcap_usd <= rules["mcap_usd_max"]):
        reasons.append(REASONS["mcap"])
    if sig.snipes < rules["snipes_min"]:
        reasons.append(REASONS["snipes"])
    if sig.sec_score > rules["sec_score_max"]:
        reasons.append(REASONS["sec"])
    return not reasons, reasons


def filter_signals(signals: list[Signal]) -> tuple[list[Signal], dict[str, int], dict[str, Signal]]:
    """Dedupe and filter a list of signals, keeping the first per contract.

    Args:
        signals: Signals in chronological order (or any order; they are sorted).

    Returns:
        A tuple of (passed, reject-reason counts, first-signal-per-CA map).
    """
    seen: dict[str, Signal] = {}
    counts = {r: 0 for r in REASONS.values()}
    for sig in sorted(signals, key=lambda s: s.unixtime):
        if not sig.ca or sig.ca in seen:
            continue
        seen[sig.ca] = sig
        ok, reasons = check_signal(sig)
        if not ok:
            for r in reasons:
                counts[r] += 1
    passed = [s for s in seen.values() if check_signal(s)[0]]
    return passed, counts, seen