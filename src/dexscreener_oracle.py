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
        # Shared httpx client for the plain-REST endpoints below (boosts,
        # metas, search). Previously each call spun a new AsyncClient,
        # bypassing rate limits and leaking connections.
        import httpx as _httpx
        self._http = _httpx.AsyncClient(
            timeout=timeout_s, headers={"accept": "application/json"})

    async def _ensure_started(self) -> None:
        if not self._started:
            await self._client.startup()
            self._started = True

    async def close(self) -> None:
        try:
            await self._http.aclose()
        except Exception:  # noqa: BLE001
            pass
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

    @staticmethod
    def _dict_to_normalized(pair: dict[str, Any]) -> dict[str, Any]:
        """Normalize a raw DexScreener search-API dict (not a DexPairData).

        ``search_pairs()`` receives plain JSON; the object-based
        ``_pair_to_dict()`` would raise AttributeError on it (previously
        swallowed → silent []). Field names follow doc/dexscreener_api.md.
        """
        def _f(v: Any) -> float | None:
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        base = pair.get("baseToken") or {}
        liq = pair.get("liquidity") or {}
        vol = pair.get("volume") or {}
        txns = pair.get("txns") or {}
        m5 = txns.get("m5") or {}
        pc = pair.get("priceChange") or {}
        return {
            "symbol": base.get("symbol"),
            "liq": _f(liq.get("usd")),
            "mcap": _f(pair.get("marketCap") or pair.get("fdv")),
            "price_usd": _f(pair.get("priceUsd")),
            "vol_m5": _f(vol.get("m5")),
            "vol_h1": _f(vol.get("h1")),
            "vol_h24": _f(vol.get("h24")),
            "txns_m5": int(m5.get("buys") or 0) + int(m5.get("sells") or 0),
            "dex_id": pair.get("dexId"),
            "pair_address": pair.get("pairAddress"),
            "pair_created_ms": pair.get("pairCreatedAt"),
            "price_change": {
                "m5": _f(pc.get("m5")),
                "h1": _f(pc.get("h1")),
                "h6": _f(pc.get("h6")),
                "h24": _f(pc.get("h24")),
            },
        }

    async def token_boosts(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get latest boosted tokens from DexScreener (60 RPM endpoint).

        Returns list of token dicts with address, name, symbol, boost amount, etc.
        Social-signal only — boosts are paid visibility, NOT endorsement
        (per DexScreener boosting terms). Never gate entries on this alone.
        """
        await self._ensure_started()
        try:
            r = await self._http.get(
                "https://api.dexscreener.com/token-boosts/latest/v1",
            )
            if r.status_code != 200:
                return []
            data = r.json()
            if not isinstance(data, list):
                return []
            return data[:limit]
        except Exception as e:  # noqa: BLE001
            logger.debug("dexscreener token-boosts failed: %s", e)
            return []

    async def trending_metas(self) -> list[dict[str, Any]]:
        """Get trending metas/sectors from DexScreener (60 RPM endpoint).

        Returns list of meta dicts with name, slug, volume, etc.
        Sector context only — not an entry signal.
        """
        await self._ensure_started()
        try:
            r = await self._http.get(
                "https://api.dexscreener.com/metas/trending/v1",
            )
            if r.status_code != 200:
                return []
            data = r.json()
            if not isinstance(data, list):
                return []
            return data[:10]
        except Exception as e:  # noqa: BLE001
            logger.debug("dexscreener trending-metas failed: %s", e)
            return []

    async def token_orders(self, chain: str, token_address: str) -> list[dict[str, Any]]:
        """Get paid orders for one token (60 RPM, /orders/v1/{chain}/{token}).

        Per-token boost validation — prefer over the global boosts firehose
        when checking whether a *specific* signal token is promoted.
        Fail-open: [] on any error.
        """
        await self._ensure_started()
        try:
            r = await self._http.get(
                f"https://api.dexscreener.com/orders/v1/{chain}/{token_address}",
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception as e:  # noqa: BLE001
            logger.debug("dexscreener token-orders failed for %s: %s", token_address[:8], e)
            return []

    async def search_pairs(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search for pairs by token name/symbol/address.

        Returns list of normalized pair dicts.
        Useful for validating token symbols from Telegram signals.
        """
        await self._ensure_started()
        try:
            r = await self._http.get(
                "https://api.dexscreener.com/latest/dex/search",
                params={"q": query},
            )
            if r.status_code != 200:
                return []
            data = r.json()
            pairs = data.get("pairs") or []
            return [self._dict_to_normalized(p) for p in pairs[:limit]
                    if isinstance(p, dict)]
        except Exception as e:  # noqa: BLE001
            logger.debug("dexscreener search-pairs failed for %s: %s", query, e)
            return []
