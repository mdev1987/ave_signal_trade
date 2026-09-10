"""DexScreener REST oracle client.

Wraps ``dexscreener-python`` (pip install dexscreener-python) which provides
adaptive rate limiting, 429 retry with exponential backoff, response caching,
and request deduplication — all critical for production trading.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from dexscreener import DexScreenerClient as _DsClient
from dexscreener import DexPairData

logger = logging.getLogger(__name__)


class DexScreenerClient:
    """Thin wrapper that preserves the old ``token_pairs()`` API.

    All heavy lifting (rate limiting, caching, 429 retry) is handled by
    the underlying ``dexscreener-python`` library.
    """

    def __init__(
        self,
        base_url: str = "https://api.dexscreener.com",
        rpm: int = 300,
        timeout_s: float = 2.5,
    ) -> None:
        # rate_limit = requests/second; rpm/50 → rps, min 1
        rate_limit = max(1.0, rpm / 50.0)
        self._client = _DsClient(rate_limit=rate_limit, cache_ttl=8.0)
        self._started = False

    async def _ensure_started(self) -> None:
        if not self._started:
            await self._client.startup()
            self._started = True

    async def close(self) -> None:
        if self._started:
            await self._client.shutdown()
            self._started = False

    @staticmethod
    def _pair_to_dict(pair: DexPairData) -> dict[str, Any]:
        """Convert a ``DexPairData`` to the normalized dict the bot expects."""
        def _f(v: Any) -> float | None:
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        return {
            "symbol": pair.base_token_symbol or None,
            "liq": _f(pair.liquidity_usd),
            "mcap": _f(pair.market_cap or pair.fdv),
            "price_usd": _f(pair.price_usd),
            "vol_m5": _f(pair.volume_5m),
            "vol_h1": _f(pair.volume_1h),
            "vol_h24": _f(pair.volume_24h),
            "txns_m5": (pair.buys_5m or 0) + (pair.sells_5m or 0),
            "dex_id": pair.dex_id,
            "pair_address": pair.pair_address,
            "pair_created_ms": pair.pair_created_at,
            "price_change": {
                "m5": _f(pair.price_change_5m),
                "h1": _f(pair.price_change_1h),
                "h6": _f(pair.price_change_6h),
                "h24": _f(pair.price_change_24h),
            },
        }

    @staticmethod
    def normalize(pair: dict[str, Any] | None) -> dict[str, Any] | None:
        """Flatten a Pair object to the fields the pool gate consumes."""
        return pair  # already normalized when coming from _pair_to_dict

    async def token_pairs(self, chain: str, ca: str) -> dict[str, Any] | None:
        """Normalized best-pair snapshot for one token, or None on failure.

        Filters for pairs where the requested token is the base (correct price).
        Tokens that are only ever a quote are skipped to avoid mispricing.
        """
        await self._ensure_started()
        try:
            pairs = await asyncio.wait_for(
                self._client.get_token_pairs(chain, ca),
                timeout=12.0,
            )
            if not pairs:
                return None

            # Filter for pairs where our token is the base (correct price)
            ca_l = ca.lower()
            base_pairs = [
                p for p in pairs
                if p.base_token_address.lower() == ca_l
            ]
            pool = base_pairs or pairs

            # Pick most liquid pair
            best = max(pool, key=lambda p: float(p.liquidity_usd or 0))

            if not base_pairs:
                return None  # requested token is only a quote — price wrong

            return self._pair_to_dict(best)

        except asyncio.TimeoutError:
            logger.warning("dexscreener token-pairs timed out %s", ca[:8])
            return None
        except Exception as e:  # noqa: BLE001
            logger.warning("dexscreener token-pairs failed %s: %s %s", ca[:8], type(e).__name__, e)
            return None
