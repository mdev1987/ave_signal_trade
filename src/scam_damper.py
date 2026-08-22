"""Serial-relaunch damper: veto token names that keep appearing under new CAs.

Scam farms relaunch the same token name on fresh mints all day ("NEX Ai" was
posted with 5+ different contract addresses in ~3 hours, "牛来" with dozens)
until one of the launches catches buyers, then rugged it. A single CA can pass
every per-token filter while the *name* is already a known scam pattern.

The damper records every incoming signal (even ones rejected for other
reasons) and flags a name once it has been seen on ``max_cas`` distinct
contract addresses inside ``window_min``. Name matching is normalized
(lowercase, alphanumeric only) so case/emoji/spam-suffix variants collapse.

It runs AFTER the base filter so only signals that would otherwise be traded
get damped — zero blast radius on already-rejected spam.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict

_NORM = re.compile(r"[^a-z0-9]")


def normalize_name(name: str) -> str:
    """Collapse a token name to its comparable form (lowercase alnum)."""
    return _NORM.sub("", (name or "").lower())


class ScamDamper:
    """Rolling-window name-frequency veto used by the live signal path.

    Semantics: the CURRENT signal is recorded first, so ``max_cas=N`` rejects
    **once the current signal makes the distinct-CA count reach N** — i.e.
    with N=3 it is the *third* distinct CA of a name that gets rejected, not
    the fourth. That is intentional (conservative for relaunch farms).

    Args:
        max_cas: Distinct CAs sharing one normalized name within the window
            before new signals with that name are rejected (count includes
            the current signal).
        window_s: Rolling window length in seconds.
    """

    def __init__(self, max_cas: int = 3, window_s: float = 6 * 3600.0) -> None:
        self.max_cas = max(1, max_cas)
        self.window_s = window_s
        # normalized name -> {ca: last-seen ts}
        self._seen: dict[str, dict[str, float]] = defaultdict(dict)
        self._last_prune = 0.0

    def record(self, ca: str, name: str, ts: float | None = None) -> None:
        """Record one signal occurrence (any CA/name, rejected or not)."""
        if not ca or not name:
            return
        ts = time.time() if ts is None else ts
        key = normalize_name(name)
        if not key:
            return
        cas = self._seen[key]
        prev = cas.get(ca)
        cas[ca] = ts if prev is None else max(prev, ts)
        self._maybe_prune(ts)

    def count(self, name: str, now: float | None = None) -> int:
        """Distinct CAs seen for this name inside the window."""
        now = time.time() if now is None else now
        key = normalize_name(name)
        cas = self._seen.get(key)
        if not cas:
            return 0
        cutoff = now - self.window_s
        return sum(1 for ts in cas.values() if ts >= cutoff)

    def is_serial(self, name: str, now: float | None = None) -> bool:
        """Whether this name has hit the serial-relaunch threshold."""
        return self.count(name, now=now) >= self.max_cas

    def _maybe_prune(self, now: float) -> None:
        """Drop names whose every occurrence aged out of the window."""
        if now - self._last_prune < 300.0:
            return
        self._last_prune = now
        cutoff = now - self.window_s
        stale = []
        for key, cas in self._seen.items():
            fresh = {ca: ts for ca, ts in cas.items() if ts >= cutoff}
            if fresh:
                self._seen[key] = fresh
            else:
                stale.append(key)
        for key in stale:
            self._seen.pop(key, None)
