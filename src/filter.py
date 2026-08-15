"""Apply the data-backed filter to parsed signals.

The rules live in :data:`models.FILTER` and were tuned on 2026-08-13 replay
data (5210 outcomes): mcap $5-20K + Pumpfunamm + snipes>=3 + security score 0
achieved a ~60-64% win rate to a 3x target.
"""

from __future__ import annotations

from models import FILTER, REASONS, Signal


def check_signal(sig: Signal) -> tuple[bool, list[str]]:
    """Check one signal against the filter rules.

    Args:
        sig: The parsed signal.

    Returns:
        Tuple of (passed, reasons). ``passed`` is True only when the signal
        meets every rule; ``reasons`` lists every violated rule.
    """
    reasons: list[str] = []
    if not sig.ca:
        return False, [REASONS["no_ca"]]
    if sig.dex not in FILTER["dexes"]:
        reasons.append(REASONS["dex"])
    if not (FILTER["mcap_usd_min"] <= sig.mcap_usd <= FILTER["mcap_usd_max"]):
        reasons.append(REASONS["mcap"])
    if sig.snipes < FILTER["snipes_min"]:
        reasons.append(REASONS["snipes"])
    if sig.sec_score > FILTER["sec_score_max"]:
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