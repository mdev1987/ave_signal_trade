"""DexScreener REST oracle client (rate-limited).

Docs: https://docs.dexscreener.com/api/reference — all REST endpoints are
rate-limited to **60 requests/minute** on the public API (no keyed tier is
documented; higher plans exist privately — ``rpm`` is configurable so a
300 rpm plan can be honored without code changes).

Used as the fast fallback liquidity/market-cap oracle when DexPaprika has not
indexed a brand-new pool yet (observed live 2026-08-24: WOFI graduated to
PumpSwap and pumped ~1380x within the minute while DexPaprika still returned
no pool — DexScreener had the pair indexed immediately).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class DexScreenerClient:
    """Minimal async DexScreener REST client with a sliding-window limiter.

    Args:
        base_url: REST base (default ``https://api.dexscreener.com``).
        rpm: Requests-per-minute cap (60 public tier; raise via env for
            higher plans).
        timeout_s: Per-request timeout.
    """

    def __init__(
        self,
        base_url: str = "https://api.dexscreener.com",
        rpm: int = 60,
        timeout_s: float = 2.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.rpm = max(1, int(rpm))
        self.timeout_s = timeout_s
        self._client = httpx.AsyncClient(timeout=timeout_s, follow_redirects=True)
        self._sent: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        """Release the HTTP client."""
        await self._client.aclose()

    async def _throttle(self) -> None:
        """Block until a request slot is free under the rpm cap."""
        async with self._lock:
            while True:
                now = time.monotonic()
                while self._sent and now - self._sent[0] > 60.0:
                    self._sent.popleft()
                if len(self._sent) < self.rpm:
                    self._sent.append(now)
                    return
                wait = 60.0 - (now - self._sent[0]) + 0.05
                await asyncio.sleep(min(wait, 5.0))

    @staticmethod
    def _best_pair(pairs: list[dict[str, Any]]) -> dict[str, Any] | None:
        """The most-liquid pair for a token (None input-safe)."""
        if not isinstance(pairs, list) or not pairs:
            return None
        return max(
            pairs,
            key=lambda p: ((p.get("liquidity") or {}).get("usd") or 0),
        )

    @staticmethod
    def normalize(pair: dict[str, Any] | None) -> dict[str, Any] | None:
        """Flatten a Pair object to the fields the pool gate consumes."""
        if not pair:
            return None
        liq = (pair.get("liquidity") or {}).get("usd")
        vol = pair.get("volume") or {}

        def _f(v: Any) -> float | None:
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        txns = pair.get("txns") or {}
        m5 = txns.get("m5") or {}
        return {
            "liq": _f(liq),
            "mcap": _f(pair.get("marketCap") or pair.get("fdv")),
            "price_usd": _f(pair.get("priceUsd")),
            "vol_m5": _f(vol.get("m5")),
            "vol_h1": _f(vol.get("h1")),
            "txns_m5": int((m5.get("buys") or 0) + (m5.get("sells") or 0)),
            "dex_id": pair.get("dexId"),
            "pair_created_ms": pair.get("pairCreatedAt"),
        }

    async def token_pairs(self, chain: str, ca: str) -> dict[str, Any] | None:
        """Normalized best-pair snapshot for one token, or None on failure."""
        await self._throttle()
        try:
            r = await asyncio.wait_for(
                self._client.get(f"{self.base_url}/token-pairs/v1/{chain}/{ca}"),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code == 429:
                logger.warning("dexscreener 429 (rate limit)")
                return None
            r.raise_for_status()
            return self.normalize(self._best_pair(r.json()))
        except Exception as e:  # noqa: BLE001
            logger.warning("dexscreener token-pairs failed %s: %s", ca, e)
            return None
