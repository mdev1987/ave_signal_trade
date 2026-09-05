"""RugCheck.xyz client — Solana token rug/safety detection.

Uses the free read-only API (no auth required for GET endpoints).
Falls back to allow on any error (fail-open pattern).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

log = logging.getLogger("rugcheck")

_DEFAULT_BASE = "https://api.rugcheck.xyz"
_DEFAULT_TIMEOUT = 12.0


@dataclass
class RugCheckResult:
    """Parsed RugCheck token report summary."""

    mint: str
    score_normalised: int  # 0-100, higher = riskier
    rugged: bool
    risks: list[dict[str, Any]]
    lp_locked_pct: float
    mint_authority: str | None
    freeze_authority: str | None
    top_holder_pct: float  # max single holder %
    insider_count: int  # number of insider top holders
    total_market_liquidity: float
    has_danger: bool = False

    @property
    def safe(self) -> bool:
        """True if token passes all safety checks."""
        return not self.rugged and not self.has_danger

    def summary(self) -> str:
        parts = [f"score={self.score_normalised}"]
        if self.rugged:
            parts.append("RUGGED")
        if self.has_danger:
            parts.append("DANGER")
        parts.append(f"lp_locked={self.lp_locked_pct:.0f}%")
        if self.mint_authority:
            parts.append("mint_auth=ACTIVE")
        if self.freeze_authority:
            parts.append("freeze_auth=ACTIVE")
        parts.append(f"top_holder={self.top_holder_pct:.1f}%")
        parts.append(f"liq=${self.total_market_liquidity:,.0f}")
        return " | ".join(parts)


class RugCheckClient:
    """Async RugCheck.xyz API client with fail-open pattern.

    Rate limit: 3 req/s, single reports only, cached results.
    """

    def __init__(
        self,
        api_key: str = "",
        base_url: str = _DEFAULT_BASE,
        max_score: int = 30,
        reject_danger: bool = True,
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._max_score = max_score
        self._reject_danger = reject_danger
        self._timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout)
        self._stats = {"checks": 0, "safe": 0, "rejected": 0, "errors": 0}
        # 3 req/s rate limiter
        self._min_interval = 1.0 / 3.0
        self._last_request_ts = 0.0
        self._rate_lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    async def check(self, mint: str) -> RugCheckResult | None:
        """Check a token's safety via RugCheck summary endpoint.

        Returns None on any error (fail-open: callers should treat None as safe).
        """
        self._stats["checks"] += 1
        try:
            # Rate limit: 3 req/s
            async with self._rate_lock:
                now = time.monotonic()
                wait = self._min_interval - (now - self._last_request_ts)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_request_ts = time.monotonic()

            url = f"{self._base}/v1/tokens/{mint}/report/summary"
            params = {}
            if self._api_key:
                params["key"] = self._api_key

            resp = await asyncio.wait_for(
                self._client.get(url, params=params),
                timeout=self._timeout,
            )

            if resp.status_code == 429:
                log.warning("rugcheck rate-limited for %s", mint[:8])
                self._stats["errors"] += 1
                return None

            resp.raise_for_status()
            data = resp.json()

            score = data.get("score_normalised", 100)
            risks = data.get("risks", [])
            has_danger = any(r.get("level") == "danger" for r in risks)
            rugged = data.get("rugged", False)
            lp_locked = data.get("lpLockedPct", 0)

            # Extract detailed fields from the full report if available
            mint_auth = None
            freeze_auth = None
            top_holder_pct = 0.0
            insider_count = 0
            total_liq = data.get("totalMarketLiquidity", 0)

            result = RugCheckResult(
                mint=mint,
                score_normalised=score,
                rugged=rugged,
                risks=risks,
                lp_locked_pct=lp_locked,
                mint_authority=mint_auth,
                freeze_authority=freeze_auth,
                top_holder_pct=top_holder_pct,
                insider_count=insider_count,
                total_market_liquidity=total_liq,
                has_danger=has_danger,
            )

            if result.safe and score <= self._max_score:
                self._stats["safe"] += 1
            else:
                self._stats["rejected"] += 1
                log.info(
                    "rugcheck REJECT %s: %s",
                    mint[:8], result.summary(),
                )

            return result

        except asyncio.TimeoutError:
            log.warning("rugcheck timeout for %s", mint[:8])
            self._stats["errors"] += 1
            return None
        except Exception:
            log.exception("rugcheck error for %s", mint[:8])
            self._stats["errors"] += 1
            return None

    def is_safe(self, result: RugCheckResult | None) -> bool:
        """Evaluate a RugCheck result against configured thresholds.

        Returns True if safe to trade. None result = fail-open (safe).
        """
        if result is None:
            return True  # fail-open
        if result.rugged:
            return False
        if self._reject_danger and result.has_danger:
            return False
        if result.score_normalised > self._max_score:
            return False
        return True
