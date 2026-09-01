"""SolanaTracker Data API client — wallet scoring, token safety, KOL trades.

Thin async wrapper around https://data.solanatracker.io with:
- Rate limiting (3 req/sec free tier, unlimited on Advanced+)
- TTL-based response cache to avoid redundant calls
- Fail-open design: every method returns None/empty on error

All public methods are safe to call from fire-and-forget tasks.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

_BASE = "https://data.solanatracker.io"
_DEFAULT_TIMEOUT_S = 10.0
_DEFAULT_RATE_LIMIT_S = 1.0 / 3.0  # free tier: 3 req/sec


class SolTrackerClient:
    """Async client for the SolanaTracker Data API."""

    def __init__(
        self,
        api_key: str = "",
        base_url: str = _BASE,
        rate_limit_s: float = _DEFAULT_RATE_LIMIT_S,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
    ) -> None:
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._timeout = timeout_s
        self._client: httpx.AsyncClient | None = None
        # Rate limiter state
        self._rate_limit_s = rate_limit_s
        self._next_req_ts: float = 0.0
        self._rate_lock = asyncio.Lock()
        # TTL cache: (method, key) -> (expires_at, result)
        self._cache: dict[tuple, tuple[float, Any]] = {}
        self._cache_max = 500

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # --------------------------------------------------------------- rate limit
    async def _throttle(self) -> None:
        async with self._rate_lock:
            now = time.monotonic()
            wait = self._next_req_ts - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._next_req_ts = time.monotonic() + self._rate_limit_s

    # ------------------------------------------------------------------- cache
    def _cache_get(self, cache_key: tuple, ttl_s: float) -> Any | None:
        entry = self._cache.get(cache_key)
        if entry and entry[0] > time.monotonic():
            return entry[1]
        return None

    def _cache_set(self, cache_key: tuple, value: Any, ttl_s: float) -> None:
        if len(self._cache) >= self._cache_max:
            # evict oldest 20%
            evict = self._cache_max // 5
            for k in list(self._cache.keys())[:evict]:
                del self._cache[k]
        self._cache[cache_key] = (time.monotonic() + ttl_s, value)

    # ------------------------------------------------------------------ http
    async def _get(self, path: str, params: dict | None = None) -> dict | list | None:
        if not self._api_key:
            return None
        await self._throttle()
        client = await self._ensure_client()
        try:
            resp = await client.get(
                f"{self._base}{path}",
                params=params or {},
                headers={"x-api-key": self._api_key},
            )
            if resp.status_code == 429:
                log.warning("soltracker 429 on %s", path)
                return None
            if resp.status_code != 200:
                log.warning("soltracker %d on %s", resp.status_code, path)
                return None
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            log.debug("soltracker %s failed: %s", path, exc)
            return None

    # --------------------------------------------------------- KOL leaderboard
    async def get_kol_leaderboard(
        self,
        limit: int = 50,
        sort: str = "realized",
        direction: str = "desc",
        ttl_s: float = 3600.0,
    ) -> list[dict]:
        """Top KOL wallets ranked by performance.

        Returns list of dicts with keys: wallet, identity, pnl, winRate, roi,
        trades, tokens, lastTrade.
        """
        cache_key = ("kol_lb", limit, sort, direction)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            "/v2/pnl/leaderboard/kols",
            params={"sort": sort, "direction": direction, "limit": limit},
        )
        if data is None:
            return []
        traders = data.get("traders") or []
        self._cache_set(cache_key, traders, ttl_s)
        return traders

    # ------------------------------------------------------- wallet summary
    async def get_wallet_summary(self, wallet: str, ttl_s: float = 300.0) -> dict | None:
        """Single wallet PnL summary from PnL V2.

        Returns dict with: pnl, winRate, trades, roi, avgHoldTimeSecs, etc.
        """
        cache_key = ("ws", wallet)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(f"/v2/pnl/wallets/{wallet}")
        if data is None:
            return None
        self._cache_set(cache_key, data, ttl_s)
        return data

    async def get_wallets_summary(
        self, wallets: list[str], ttl_s: float = 300.0
    ) -> dict[str, dict]:
        """Batch wallet summaries (up to 100 wallets per call).

        Returns {wallet: summary_dict} for all found wallets.
        """
        if not wallets:
            return {}
        # SolanaTracker batch endpoint accepts POST with wallet list
        if not self._api_key:
            return {}
        await self._throttle()
        client = await self._ensure_client()
        # Split into batches of 100
        results: dict[str, dict] = {}
        for i in range(0, len(wallets), 100):
            batch = wallets[i : i + 100]
            cache_key = ("wbs", tuple(sorted(batch)))
            cached = self._cache_get(cache_key, ttl_s)
            if cached is not None:
                results.update(cached)
                continue
            try:
                resp = await client.post(
                    f"{self._base}/v2/pnl/wallets/batch",
                    json={"wallets": batch},
                    headers={"x-api-key": self._api_key},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    summaries = data.get("results") or []
                    batch_map = {
                        s.get("wallet", ""): s
                        for s in summaries
                        if s.get("wallet")
                    }
                    self._cache_set(cache_key, batch_map, ttl_s)
                    results.update(batch_map)
                else:
                    log.warning("soltracker batch %d on wallets", resp.status_code)
            except Exception as exc:  # noqa: BLE001
                log.debug("soltracker batch wallets failed: %s", exc)
        return results

    # --------------------------------------------------------- token info
    async def get_token_info(self, mint: str, ttl_s: float = 60.0) -> dict | None:
        """Token info including risk score, holders, bundlers, etc.

        Returns the full token object (risk, pools, events, etc.).
        """
        cache_key = ("ti", mint)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(f"/tokens/{mint}")
        if data is None:
            return None
        self._cache_set(cache_key, data, ttl_s)
        return data

    # -------------------------------------------------------- KOL trades
    async def get_kol_trades(
        self,
        min_volume: int = 1000,
        limit: int = 50,
        ttl_s: float = 15.0,
    ) -> list[dict]:
        """Latest KOL spot trades from the shared KOL roster.

        Returns list of trade dicts with: wallet, type, volume, time,
        identity, token, etc.
        """
        cache_key = ("kt", min_volume, limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        params: dict[str, Any] = {"limit": limit}
        if min_volume > 0:
            params["minVolume"] = min_volume
        data = await self._get("/trades/kols", params=params)
        if data is None:
            return []
        trades = data.get("trades") or data if isinstance(data, list) else []
        self._cache_set(cache_key, trades, ttl_s)
        return trades

    # ----------------------------------------------------- first buyers
    async def get_first_buyers(
        self, mint: str, limit: int = 50, ttl_s: float = 300.0
    ) -> list[dict]:
        """Chronological first buyers of a token with PnL and holding state.

        Returns list of trader dicts with: wallet, pnl, position, timing, etc.
        """
        cache_key = ("fb", mint, limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            f"/v2/pnl/tokens/{mint}/first-buyers",
            params={"limit": limit},
        )
        if data is None:
            return []
        traders = data.get("traders") or []
        self._cache_set(cache_key, traders, ttl_s)
        return traders

    # ------------------------------------------------------ token traders
    async def get_token_traders(
        self, mint: str, limit: int = 50, ttl_s: float = 300.0
    ) -> list[dict]:
        """All traders on a token with position data."""
        cache_key = ("tt", mint, limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            f"/v2/pnl/tokens/{mint}/traders",
            params={"limit": limit},
        )
        if data is None:
            return []
        traders = data.get("traders") or []
        self._cache_set(cache_key, traders, ttl_s)
        return traders

    # -------------------------------------------------- wallet positions
    async def get_wallet_positions(
        self, wallet: str, limit: int = 50, ttl_s: float = 300.0
    ) -> list[dict]:
        """Open positions for a wallet."""
        cache_key = ("wp", wallet, limit)
        cached = self._cache_get(cache_key, ttl_s)
        if cached is not None:
            return cached
        data = await self._get(
            f"/v2/pnl/wallets/{wallet}/positions",
            params={"limit": limit},
        )
        if data is None:
            return []
        positions = data.get("positions") or []
        self._cache_set(cache_key, positions, ttl_s)
        return positions
