"""GMGN API client — token safety, wallet scoring, smart money feed.

Async wrapper around https://gmgn.ai/api with:
- Leaky-bucket rate limiter (20 rps, capacity 20)
- TTL-based response cache
- Fail-open: every method returns None/empty on error

Endpoints used:
  GET /v1/token/security   — honeypot, rug_ratio, mint/freeze, tax, top10
  GET /v1/token/info       — wallet_tags_stat (smart_wallets count)
  GET /v1/user/smartmoney  — real-time smart money trades
  GET /v1/user/kol         — real-time KOL trades
  POST /v1/user/wallet_profits — batch wallet PnL (up to 100)
  GET /v1/market/token_kline  — OHLCV for pattern analysis
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

_BASE = "https://gmgn.ai/api"
_TIMEOUT_S = 10.0


class GMGNClient:
    """Async client for the GMGN API."""

    def __init__(
        self,
        api_key: str = "",
        base_url: str = _BASE,
        timeout_s: float = _TIMEOUT_S,
    ) -> None:
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._timeout = timeout_s
        self._client: httpx.AsyncClient | None = None
        # Leaky bucket: rate=20/s, capacity=20
        self._rate = 20.0
        self._bucket = 20.0
        self._last_refill = time.monotonic()
        self._rate_lock = asyncio.Lock()
        # TTL cache
        self._cache: dict[tuple, tuple[float, Any]] = {}
        self._cache_max = 500

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ----------------------------------------------------------- rate limit
    async def _throttle(self, weight: int = 1) -> None:
        async with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._bucket = min(20.0, self._bucket + elapsed * self._rate)
            self._last_refill = now
            while self._bucket < weight:
                wait = (weight - self._bucket) / self._rate
                await asyncio.sleep(wait)
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._bucket = min(20.0, self._bucket + elapsed * self._rate)
                self._last_refill = now
            self._bucket -= weight

    # ---------------------------------------------------------------- cache
    def _cache_get(self, key: tuple, ttl_s: float) -> Any | None:
        entry = self._cache.get(key)
        if entry and entry[0] > time.monotonic():
            return entry[1]
        return None

    def _cache_set(self, key: tuple, value: Any, ttl_s: float) -> None:
        if len(self._cache) >= self._cache_max:
            evict = self._cache_max // 5
            for k in list(self._cache.keys())[:evict]:
                del self._cache[k]
        self._cache[key] = (time.monotonic() + ttl_s, value)

    # ------------------------------------------------------------------ http
    async def _get(
        self, path: str, params: dict | None = None, weight: int = 1
    ) -> dict | list | None:
        if not self._api_key:
            return None
        await self._throttle(weight)
        client = await self._ensure_client()
        try:
            resp = await client.get(
                f"{self._base}{path}",
                params=params or {},
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            if resp.status_code == 429:
                log.warning("gmgn 429 on %s", path)
                return None
            if resp.status_code != 200:
                log.warning("gmgn %d on %s", resp.status_code, path)
                return None
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            log.debug("gmgn %s failed: %s", path, exc)
            return None

    async def _post(
        self, path: str, json_data: dict | None = None, weight: int = 3
    ) -> dict | list | None:
        if not self._api_key:
            return None
        await self._throttle(weight)
        client = await self._ensure_client()
        try:
            resp = await client.post(
                f"{self._base}{path}",
                json=json_data or {},
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            if resp.status_code == 429:
                log.warning("gmgn 429 on %s", path)
                return None
            if resp.status_code != 200:
                log.warning("gmgn %d on %s", resp.status_code, path)
                return None
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            log.debug("gmgn %s failed: %s", path, exc)
            return None

    # -------------------------------------------------------- token security
    async def token_security(self, ca: str, ttl_s: float = 60.0) -> dict | None:
        """Token security audit: honeypot, rug_ratio, mint/freeze, tax, top10.

        Returns the security dict or None on failure.
        """
        cache_key = ("sec", ca)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            "/v1/token/security",
            params={"chain": "sol", "address": ca},
            weight=1,
        )
        if data is None:
            return None
        result = data.get("data") or data
        self._cache_set(cache_key, result, ttl_s)
        return result

    # ---------------------------------------------------------- token info
    async def token_info(self, ca: str, ttl_s: float = 60.0) -> dict | None:
        """Token info including wallet_tags_stat (smart_wallets count)."""
        cache_key = ("info", ca)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            "/v1/token/info",
            params={"chain": "sol", "address": ca},
            weight=1,
        )
        if data is None:
            return None
        result = data.get("data") or data
        self._cache_set(cache_key, result, ttl_s)
        return result

    # ------------------------------------------------------- smart money feed
    async def get_smartmoney_trades(
        self, limit: int = 100, ttl_s: float = 15.0
    ) -> list[dict]:
        """Recent smart money trades on Solana."""
        cache_key = ("sm", limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            "/v1/user/smartmoney",
            params={"chain": "sol", "limit": limit},
            weight=1,
        )
        if data is None:
            return []
        trades = (data.get("data") or data.get("list") or
                  data if isinstance(data, list) else [])
        self._cache_set(cache_key, trades, ttl_s)
        return trades

    async def get_kol_trades(
        self, limit: int = 100, ttl_s: float = 15.0
    ) -> list[dict]:
        """Recent KOL trades on Solana."""
        cache_key = ("kol", limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            "/v1/user/kol",
            params={"chain": "sol", "limit": limit},
            weight=1,
        )
        if data is None:
            return []
        trades = (data.get("data") or data.get("list") or
                  data if isinstance(data, list) else [])
        self._cache_set(cache_key, trades, ttl_s)
        return trades

    # ------------------------------------------------- wallet profits batch
    async def get_wallet_profits(
        self, wallets: list[str], period: str = "all", ttl_s: float = 300.0
    ) -> dict[str, dict]:
        """Batch wallet PnL (up to 100 wallets per call).

        Returns {wallet: {realized_profit, winrate, buy, sell, ...}}.
        """
        if not wallets or not self._api_key:
            return {}
        results: dict[str, dict] = {}
        for i in range(0, len(wallets), 100):
            batch = wallets[i : i + 100]
            cache_key = ("wp", tuple(sorted(batch)), period)
            cached = self._cache_get(cache_key, ttl_s)
            if cached is not None:
                results.update(cached)
                continue
            data = await self._post(
                "/v1/user/wallet_profits",
                json_data={"wallets": batch, "period": period, "chain": "sol"},
                weight=3,
            )
            if data is None:
                continue
            items = (data.get("data") or data.get("list") or
                     data if isinstance(data, list) else [])
            batch_map: dict[str, dict] = {}
            for item in items:
                addr = item.get("wallet_address") or item.get("wallet") or ""
                if addr:
                    batch_map[addr] = item
            self._cache_set(cache_key, batch_map, ttl_s)
            results.update(batch_map)
        return results

    # ---------------------------------------------------------- kline data
    async def get_kline(
        self,
        ca: str,
        resolution: str = "15m",
        limit: int = 100,
        ttl_s: float = 30.0,
    ) -> list[dict]:
        """OHLCV kline data for pattern analysis."""
        cache_key = ("kl", ca, resolution, limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            "/v1/market/token_kline",
            params={
                "chain": "sol",
                "address": ca,
                "resolution": resolution,
                "limit": limit,
            },
            weight=1,
        )
        if data is None:
            return []
        candles = data.get("data") or data if isinstance(data, list) else []
        self._cache_set(cache_key, candles, ttl_s)
        return candles
