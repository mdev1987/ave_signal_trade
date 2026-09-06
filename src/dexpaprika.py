"""DexPaprika API client — pool analysis, buy/sell ratios, whale detection.

Uses the free tier (15 req/min).  Fail-open on all errors.

Usage::

    dp = DexPaprikaClient()
    pools = await dp.get_token_pools("solana", token_address)
    details = await dp.get_pool_details("solana", pool_address)
    txns = await dp.get_pool_transactions("solana", pool_address)
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

log = logging.getLogger("dexpaprika")

_BASE = "https://api.dexpaprika.com"
_TIMEOUT = 12.0


class DexPaprikaClient:
    """Async DexPaprika client with fail-open pattern.

    Rate limit: 15 req/min (free tier).  A simple sliding-window limiter
    enforces this across all methods.
    """

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=_TIMEOUT)
        self._stats = {"calls": 0, "errors": 0}
        # Simple rate limiter: 15 req/min = 1 req per 4s
        self._min_interval = 4.0
        self._last_request_ts = 0.0
        self._rate_lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        """Rate-limited GET request."""
        async with self._rate_lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_request_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_ts = time.monotonic()

        try:
            url = f"{_BASE}{path}"
            self._stats["calls"] += 1
            resp = await self._client.get(url, params=params)
            if resp.status_code == 429:
                log.warning("dexpaprika rate limited")
                self._stats["errors"] += 1
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception:
            self._stats["errors"] += 1
            log.debug("dexpaprika GET %s failed", path)
            return None

    # ── Token Pools ─────────────────────────────────────────────────

    async def get_token_pools(
        self, network: str, token_address: str, limit: int = 10
    ) -> list[dict]:
        """Get all pools holding a token, sorted by 24h volume.

        Returns list of pool dicts with: id, dex, volume_usd_24h,
        liquidity_usd, price_usd, txns_24h, etc.
        """
        data = await self._get(
            f"/networks/{network}/tokens/{token_address}/pools",
            params={"limit": limit, "sort_by": "volume_usd_24h", "sort_dir": "desc"},
        )
        if not data:
            return []
        return data.get("results", data.get("pools", []))

    # ── Pool Details ────────────────────────────────────────────────

    async def get_pool_details(
        self, network: str, pool_address: str
    ) -> dict | None:
        """Get full pool snapshot: tokens, reserves, buy/sell ratios.

        Returns dict with: tokens, price_usd, liquidity, volume_24h,
        price_stats, buy_sell_metrics (per timeframe).
        """
        return await self._get(
            f"/networks/{network}/pools/{pool_address}"
        )

    # ── Pool Transactions (Whale Detection) ─────────────────────────

    async def get_pool_transactions(
        self,
        network: str,
        pool_address: str,
        limit: int = 50,
        from_ts: int | None = None,
    ) -> list[dict]:
        """Get recent swap transactions for a pool.

        Each tx has: sender, recipient, amount_0/1, volume_0/1,
        price_0_usd/1_usd, timestamp.
        """
        params: dict[str, Any] = {"limit": limit}
        if from_ts:
            params["from"] = from_ts
        data = await self._get(
            f"/networks/{network}/pools/{pool_address}/transactions",
            params=params,
        )
        if not data:
            return []
        return data.get("transactions", [])

    # ── Composite Analysis ──────────────────────────────────────────

    async def pool_health(self, network: str, pool_address: str) -> dict:
        """Analyze pool health: buy/sell ratio, whale activity, reserves.

        Returns:
            {
                "buy_sell_1h": float,   # >1 = more buying, <1 = more selling
                "buy_usd_1h": float,
                "sell_usd_1h": float,
                "whale_buys": int,      # txns > 50 SOL in last hour
                "whale_sells": int,
                "unique_buyers": int,
                "unique_sellers": int,
                "reserve_ratio": float, # ratio of token reserves (high = one-sided)
                "safe": bool,
            }
        """
        result = {
            "buy_sell_1h": 1.0,
            "buy_usd_1h": 0.0,
            "sell_usd_1h": 0.0,
            "whale_buys": 0,
            "whale_sells": 0,
            "unique_buyers": 0,
            "unique_sellers": 0,
            "reserve_ratio": 0.5,
            "safe": True,
        }

        details = await self.get_pool_details(network, pool_address)
        if not details:
            return result

        # Buy/sell metrics from pool details
        metrics = details.get("buy_sell_metrics", {})
        for window in ["1h", "15m", "5m"]:
            if window in metrics:
                m = metrics[window]
                buy_usd = float(m.get("buy_usd", 0))
                sell_usd = float(m.get("sell_usd", 0))
                if buy_usd + sell_usd > 0:
                    result["buy_usd_1h"] = buy_usd
                    result["sell_usd_1h"] = sell_usd
                    result["buy_sell_1h"] = buy_usd / sell_usd if sell_usd > 0 else 999
                break

        # Whale detection from recent transactions
        now = int(time.time())
        txns = await self.get_pool_transactions(
            network, pool_address, limit=100, from_ts=now - 3600
        )
        if txns:
            buyers = set()
            sellers = set()
            whale_sol_threshold = 50.0
            for tx in txns:
                vol = float(tx.get("volume_0", 0) or tx.get("volume_1", 0) or 0)
                sender = tx.get("sender", "")
                recipient = tx.get("recipient", "")
                is_buy = tx.get("type") == "buy" or vol > 0

                if is_buy:
                    buyers.add(sender)
                    if vol >= whale_sol_threshold:
                        result["whale_buys"] += 1
                else:
                    sellers.add(recipient)
                    if vol >= whale_sol_threshold:
                        result["whale_sells"] += 1

            result["unique_buyers"] = len(buyers)
            result["unique_sellers"] = len(sellers)

        # Reserve ratio (one-sided liquidity detection)
        tokens = details.get("tokens", [])
        if len(tokens) >= 2:
            reserves = [float(t.get("reserve_usd", 0) or 0) for t in tokens[:2]]
            total = sum(reserves)
            if total > 0:
                result["reserve_ratio"] = max(reserves) / total

        # Safety verdict
        if result["whale_sells"] > result["whale_buys"] * 2:
            result["safe"] = False  # whale dumping
        if result["reserve_ratio"] > 0.9:
            result["safe"] = False  # one-sided pool

        return result

    # ── Token Discovery ─────────────────────────────────────────────

    async def find_safe_tokens(
        self,
        network: str,
        min_liq_usd: float = 50_000,
        min_txns_24h: int = 100,
        limit: int = 20,
    ) -> list[dict]:
        """Find tokens with real liquidity and organic activity.

        Returns tokens sorted by 24h volume, filtered by liquidity
        and transaction count minimums.
        """
        data = await self._get(
            f"/networks/{network}/tokens",
            params={
                "limit": limit,
                "sort_by": "volume_usd_24h",
                "sort_dir": "desc",
                "liquidity_usd_min": min_liq_usd,
                "txns_24h_min": min_txns_24h,
            },
        )
        if not data:
            return []
        return data.get("results", [])
