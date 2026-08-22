"""RugCheck pre-trade gate (arm-time token security veto).

Queries ``api.rugcheck.xyz`` for the token's risk report summary and vetoes
the signal when the report carries one of the configured danger risks.

Evidence base (2026-08-20 live-rug post-mortem, ``docs/replay_zst/live_rugged_logs``):
every hard rug that drained the wallet (TONK, NEX Ai #2, 牛来) showed an explicit
"Large Amount of LP Unlocked" danger on RugCheck — unburned PumpAMM LP lets the
deployer pull liquidity at will. The profitable trades never carried that flag,
which makes it a low-false-positive veto. Raw scores are NOT used by default:
winners and rugs both scored ~65/100 normalised, so a score ceiling would block
the strategy's own edge.

**Fail-open by design**: brand-new pools are sniped at second 0–30 while
RugCheck's indexer has not produced a report yet. A missing/unavailable report
must therefore ADMIT the token (the strict mcap/snipes/dex/quote gates still
apply); otherwise every sec-0 entry would be rejected and the strategy dies.
Set ``RUGCHECK_FAIL_CLOSED=true`` to invert this for conservative deployments.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

import logs

logger = logging.getLogger(__name__)


class RugChecker:
    """Async, cached, single-flight RugCheck summary client.

    Args:
        api_key: RugCheck API key (sent as ``X-API-KEY``; may be empty —
            public endpoints work without auth).
        base_url: API base, e.g. ``https://api.rugcheck.xyz``. A trailing
            ``?key=`` (legacy .env form) is stripped; header auth is used.
        veto_risks: Case-insensitive substrings; a report whose risk list
            contains any of them is vetoed.
        max_score_normalised: Veto when ``score_normalised`` exceeds this.
            0 disables (default) — scores do not separate winners from rugs.
        timeout_s: Per-request timeout.
        cache_ttl_s: How long a fetched summary stays fresh.
        fail_closed: When True, an unavailable report rejects instead of
            admitting the token.
    """

    def __init__(
        self,
        api_key: str = "",
        base_url: str = "https://api.rugcheck.xyz",
        veto_risks: tuple[str, ...] = ("lp unlocked",),
        max_score_normalised: float = 0.0,
        timeout_s: float = 2.0,
        cache_ttl_s: float = 120.0,
        fail_closed: bool = False,
    ) -> None:
        self.api_key = api_key.strip()
        self.base_url = self._normalize_base(base_url)
        self.veto_risks = tuple(v.lower() for v in veto_risks if v.strip())
        self.max_score_normalised = max_score_normalised
        self.timeout_s = timeout_s
        self.cache_ttl_s = cache_ttl_s
        self.fail_closed = fail_closed
        self._client = httpx.AsyncClient(timeout=timeout_s, follow_redirects=True)
        self._cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
        self._inflight: dict[str, asyncio.Future[dict[str, Any] | None]] = {}
        self._cache_max = 2000

    @staticmethod
    def _normalize_base(url: str) -> str:
        """Strip query strings from the base URL.

        The legacy .env value was ``https://api.rugcheck.xyz?key=``; auth now
        goes through the ``X-API-KEY`` header (docs-recommended, keeps the key
        out of logs/referrers), so any ``?...`` suffix is removed.
        """
        return (url or "https://api.rugcheck.xyz").split("?", 1)[0].rstrip("/")

    async def close(self) -> None:
        """Release the HTTP client."""
        await self._client.aclose()

    async def _fetch_summary(self, mint: str) -> dict[str, Any] | None:
        """Fetch ``/v1/tokens/{mint}/report/summary``; None when unavailable."""
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.base_url}/v1/tokens/{mint}/report/summary",
                    headers=headers,
                ),
                timeout=self.timeout_s + 1.0,
            )
            if r.status_code == 404:
                return None  # not indexed yet — the normal case at snipe time
            if r.status_code == 429:
                logger.warning("rugcheck 429 (rate limit) for %s", mint)
                return None
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else None
        except Exception as e:  # noqa: BLE001
            logger.warning("rugcheck summary failed %s: %s", mint, e)
            return None

    async def _summary(self, mint: str) -> dict[str, Any] | None:
        """Cached + single-flight summary fetch."""
        cached = self._cache.get(mint)
        if cached and time.time() - cached[0] < self.cache_ttl_s:
            return cached[1]
        fut = self._inflight.get(mint)
        if fut is not None:
            return await fut
        fut: asyncio.Future[dict[str, Any] | None] = asyncio.get_running_loop().create_future()
        self._inflight[mint] = fut
        try:
            data = await self._fetch_summary(mint)
            self._cache[mint] = (time.time(), data)
            self._prune()
            return data
        finally:
            self._inflight.pop(mint, None)
            if not fut.done():  # cancelled while fetching
                fut.set_result(self._cache.get(mint, (0, None))[1])

    def _prune(self) -> None:
        """Bound the cache size (drop stale first, then oldest)."""
        if len(self._cache) <= self._cache_max:
            return
        now = time.time()
        stale = [k for k, (ts, _) in self._cache.items() if now - ts > self.cache_ttl_s]
        for k in stale:
            self._cache.pop(k, None)
        while len(self._cache) > self._cache_max:
            self._cache.pop(next(iter(self._cache)), None)

    # Feature needles (lowercase substrings) tracked as measurable outcomes.
    _FEATURES = ("lp unlocked", "mint authority", "freeze authority")

    @classmethod
    def features(cls, summary: dict[str, Any] | None) -> dict[str, bool]:
        """Per-risk boolean features for outcome research.

        Returns ``{"report_missing": ..., "lp_unlocked": ...,
        "mint_authority": ..., "freeze_authority": ...}`` so every evaluation
        can be journaled and later correlated with trade results — the
        reviewer's point being that mint/freeze authority vetoes go beyond
        the LP-unlocked evidence and their false-positive rate must be
        measured, not assumed.
        """
        missing = summary is None
        names = " | ".join(
            str(r.get("name") or "").lower() for r in ((summary or {}).get("risks") or [])
        )
        return {
            "report_missing": missing,
            **{
                feat.replace(" ", "_"): (not missing and feat in names)
                for feat in cls._FEATURES
            },
        }

    def evaluate(
        self, mint: str, summary: dict[str, Any] | None
    ) -> tuple[bool, str]:
        """Apply the veto rules to a (possibly missing) summary.

        Returns:
            ``(ok, reason)`` — ok=True admits the token.
        """
        if summary is None:
            if self.fail_closed:
                return False, "report unavailable"
            return True, ""
        risks = summary.get("risks") or []
        for risk in risks:
            name = str(risk.get("name") or "")
            level = str(risk.get("level") or "").lower()
            low = name.lower()
            for needle in self.veto_risks:
                if needle in low and level != "info":
                    return False, f"{name} ({risk.get('value') or level})"
        score = summary.get("score_normalised")
        if (self.max_score_normalised > 0 and isinstance(score, (int, float))
                and score > self.max_score_normalised):
            return False, f"score {score:.0f}>{self.max_score_normalised:.0f}"
        return True, ""

    async def check(self, mint: str) -> tuple[bool, str]:
        """Full gate: fetch (cached) then evaluate, and journal the features.

        Every evaluation is journaled as a ``rugcheck`` event with the
        per-risk boolean features (``report_missing``, ``lp_unlocked``,
        ``mint_authority``, ``freeze_authority``) plus the verdict — so the
        veto list's false-positive rate can be measured against realized
        trades instead of assumed.

        Args:
            mint: Token contract address.

        Returns:
            ``(ok, reason)``; ok=False means DO NOT TRADE this token.
        """
        try:
            summary = await self._summary(mint)
        except Exception as e:  # noqa: BLE001 - gate must never break the trader
            logger.warning("rugcheck gate error %s: %s", mint, e)
            summary = None
            if self.fail_closed:
                ok, reason = False, "gate error"
                flags = self.features(None)
                logs.journal("rugcheck", ca=mint, ok=ok, reason=reason, **flags)
                return ok, reason
        ok, reason = self.evaluate(mint, summary)
        flags = self.features(summary)
        logs.journal("rugcheck", ca=mint, ok=ok,
                     reason=reason or None, veto_risks=list(self.veto_risks), **flags)
        if not ok:
            logger.info("rugcheck VETO %s: %s", mint, reason)
        return ok, reason
