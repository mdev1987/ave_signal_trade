"""DexPaprika client — Solana pool data and token prices.

Used as a fallback when DexScreener is unavailable or rate-limited.
DexPaprika provides richer data for Solana (102K+ pools, 31M+ txns)
with free tier: 15 req/min, 50K credits/month.

Key endpoints used:
  - getNetworkPoolsFilter: find pools by volume/liquidity/price change
  - getPoolDetails: get pool snapshot (price, liquidity, volume)
  - getPoolOHLCV: historical candles (1m, 5m, 1h, etc.)
  - getTokenDetails: token data by contract address
  - getTokenMultiPrices: batch price lookup (up to 10 tokens)
  - search: cross-network token/pool search
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_NETWORK = "solana"
_BASE_URL = "https://api.dexpaprika.com"


class DexPaprikaClient:
    """Async DexPaprika client for Solana pool/token data."""

    def __init__(
        self,
        timeout_s: float = 8.0,
        enabled: bool = True,
    ) -> None:
        self.enabled = enabled
        self._client = httpx.AsyncClient(timeout=timeout_s)
        self._started = False

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict | None = None) -> dict | list | None:
        """Make a GET request to DexPaprika API."""
        if not self.enabled:
            return None
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{_BASE_URL}{path}",
                    params=params or {},
                    headers={"accept": "application/json"},
                ),
                timeout=10.0,
            )
            if r.status_code != 200:
                logger.debug("dexpaprika %s HTTP %s", path, r.status_code)
                return None
            return r.json()
        except asyncio.TimeoutError:
            logger.debug("dexpaprika %s timed out", path)
            return None
        except Exception as exc:  # noqa: BLE001
            logger.debug("dexpaprika %s failed: %s", path, exc)
            return None

    async def get_pool_details(self, pool_address: str) -> dict[str, Any] | None:
        """Get full pool snapshot: price, liquidity, volume, transaction counts.

        Returns normalized dict matching DexScreener format:
          {"symbol", "liq", "mcap", "price_usd", "vol_h24", "pair_address", ...}
        """
        data = await self._get(
            f"/networks/{_NETWORK}/pools/{pool_address}",
        )
        if not data:
            return None
        return self._normalize_pool(data)

    async def search_token(self, query: str) -> dict[str, Any] | None:
        """Search for a token by name/symbol/address.

        Returns first matching token's pool data normalized to DexScreener format.
        """
        data = await self._get(
            "/networks/search",
            params={"query": query, "limit": 5},
        )
        if not data:
            return None
        # Search returns tokens, pools, and dexes — find first Solana pool
        pools = data.get("pools", []) if isinstance(data, dict) else []
        for pool in pools:
            if pool.get("network") == _NETWORK:
                return self._normalize_pool(pool)
        return None

    async def get_token_details(self, token_address: str) -> dict[str, Any] | None:
        """Get token data by contract address on Solana.

        Returns token info with multi-timeframe price/volume metrics.
        """
        data = await self._get(
            f"/networks/{_NETWORK}/tokens/{token_address}",
        )
        if not data:
            return None
        return {
            "available": True,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "price_usd": data.get("price_usd"),
            "fdv": data.get("fdv"),
            "mcap": data.get("mcap"),
            "liquidity": data.get("liquidity"),
            "volume_24h": data.get("volume_usd_24h"),
            "price_change_24h": data.get("price_change_percentage_24h"),
            "holder_count": data.get("holder_count"),
        }

    async def get_token_multi_prices(
        self, token_addresses: list[str]
    ) -> dict[str, float]:
        """Batch price lookup for up to 10 tokens on the same network.

        Returns {token_address: price_usd} dict.
        """
        if not token_addresses or len(token_addresses) > 10:
            return {}
        data = await self._get(
            f"/networks/{_NETWORK}/tokens/multi-prices",
            params={"tokens": ",".join(token_addresses)},
        )
        if not data:
            return {}
        prices = {}
        for item in (data.get("prices") or []):
            addr = item.get("token_address")
            price = item.get("price_usd")
            if addr and price is not None:
                prices[addr] = float(price)
        return prices

    async def get_pool_ohlcv(
        self,
        pool_address: str,
        interval: str = "1h",
        limit: int = 100,
    ) -> list[dict] | None:
        """Get historical OHLCV candles for a pool.

        Intervals: 1m, 5m, 15m, 1h, 6h, 24h
        Returns list of {timestamp, open, high, low, close, volume} dicts.
        """
        data = await self._get(
            f"/networks/{_NETWORK}/pools/{pool_address}/ohlcv",
            params={"interval": interval, "limit": limit},
        )
        if not data:
            return None
        candles = data.get("ohlcv") or data if isinstance(data, list) else []
        return [
            {
                "timestamp": c.get("timestamp"),
                "open": float(c.get("open", 0)),
                "high": float(c.get("high", 0)),
                "low": float(c.get("low", 0)),
                "close": float(c.get("close", 0)),
                "volume": float(c.get("volume", 0)),
            }
            for c in candles
        ]

    async def get_new_pools(
        self,
        min_liquidity: float = 0,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Find newly created pools on Solana.

        Returns list of normalized pool dicts sorted by creation time.
        """
        data = await self._get(
            f"/networks/{_NETWORK}/pools/search",
            params={
                "created_after": "1",  # Unix timestamp placeholder
                "liquidity_usd_min": min_liquidity,
                "limit": limit,
                "sort_by": "created_at",
                "sort_dir": "desc",
            },
        )
        if not data:
            return []
        pools = data.get("results", []) if isinstance(data, dict) else []
        return [self._normalize_pool(p) for p in pools]

    async def get_trending_tokens(
        self,
        min_volume_24h: float = 10000,
        min_liquidity: float = 5000,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Find trending tokens by 24h volume on Solana.

        Returns list of normalized pool dicts sorted by volume.
        """
        data = await self._get(
            f"/networks/{_NETWORK}/pools/search",
            params={
                "volume_24h_min": min_volume_24h,
                "liquidity_usd_min": min_liquidity,
                "limit": limit,
                "sort_by": "volume_usd_24h",
                "sort_dir": "desc",
            },
        )
        if not data:
            return []
        pools = data.get("results", []) if isinstance(data, dict) else []
        return [self._normalize_pool(p) for p in pools]

    @staticmethod
    def _normalize_pool(pool: dict) -> dict[str, Any]:
        """Normalize DexPaprika pool data to match DexScreener format.

        This allows the bot to use either source interchangeably.
        """
        def _f(v: Any) -> float | None:
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        # Extract token info from pool
        tokens = pool.get("tokens") or []
        base_token = tokens[0] if tokens else {}
        quote_token = tokens[1] if len(tokens) > 1 else {}

        return {
            "symbol": base_token.get("symbol") or pool.get("name"),
            "liq": _f(pool.get("liquidity_usd")),
            "mcap": _f(pool.get("fdv_usd")),
            "price_usd": _f(pool.get("price_usd")),
            "vol_m5": None,  # DexPaprika doesn't provide 5m volume in pool details
            "vol_h1": None,
            "vol_h24": _f(pool.get("volume_usd_24h")),
            "txns_m5": None,
            "dex_id": pool.get("dex_name"),
            "pair_address": pool.get("address"),
            "pair_created_ms": None,
            "price_change": {
                "m5": _f(pool.get("price_change_percentage_5m")),
                "h1": _f(pool.get("price_change_percentage_1h")),
                "h6": _f(pool.get("price_change_percentage_6h")),
                "h24": _f(pool.get("price_change_percentage_24h")),
            },
        }
