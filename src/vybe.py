"""Vybe Network API client — token data, liquidity, top holders, wallet PnL.

Endpoints used:
  - Token details: /v4/tokens/{mint} — price, volume, market cap
  - Token liquidity: /v4/tokens/{mint}/liquidity — total USD liquidity
  - Top holders: /v4/tokens/{mint}/top-holders — whale/insider tracking
  - Trader activity: /v4/tokens/{mint}/trader-activity — smart money per token
  - Wallet PnL: /v4/wallets/{addr}/pnl — win rate, realized/unrealized PnL
  - Swap quote: /v4/trading/swap-quote — multi-DEX quotes
"""

from __future__ import annotations

import logging
import time
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

_DEFAULT_BASE = "https://api.vybenetwork.xyz"
_DEFAULT_TIMEOUT = 10.0


class VybeClient:
    """Async client for Vybe Network API. Fail-open on all errors."""

    def __init__(
        self,
        api_key: str = "",
        base_url: str = _DEFAULT_BASE,
        timeout: float = _DEFAULT_TIMEOUT,
        # Safety gate config
        enabled: bool = True,
        min_liquidity_usd: float = 500.0,
        max_top_holder_pct: float = 50.0,
        min_buy_sell_ratio: float = 0.2,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.enabled = enabled and bool(api_key)
        self._session: aiohttp.ClientSession | None = None
        # Gate config
        self.min_liquidity_usd = min_liquidity_usd
        self.max_top_holder_pct = max_top_holder_pct
        self.min_buy_sell_ratio = min_buy_sell_ratio
        self._last_call_ts = 0.0
        self._min_interval = 0.5  # 500ms between calls (free tier: generous)
        if self.enabled:
            log.info("vybe: enabled (key=%s…, base=%s)", api_key[:12], base_url)
        else:
            log.info("vybe: disabled (no API key)")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"X-API-KEY": self.api_key} if self.api_key else {},
            )
        return self._session

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call_ts
        if elapsed < self._min_interval:
            import time as _time
            _time.sleep(self._min_interval - elapsed)
        self._last_call_ts = time.monotonic()

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        """GET request. Returns None on any error (fail-open)."""
        if not self.enabled:
            return None
        try:
            self._throttle()
            session = await self._get_session()
            url = f"{self.base_url}{path}"
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429:
                    log.warning("vybe 429 on %s — backing off", path)
                    return None
                else:
                    text = await resp.text()
                    log.debug("vybe %d on %s: %s", resp.status, path, text[:200])
                    return None
        except Exception as exc:
            log.debug("vybe GET %s failed: %s", path, exc)
            return None

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Token data
    # ------------------------------------------------------------------

    async def token_details(self, mint: str) -> dict | None:
        """Get token details: price, volume, market cap, supply."""
        return await self._get(f"/v4/tokens/{mint}")

    async def token_liquidity(self, mint: str) -> float:
        """Get total USD liquidity for a token. Returns 0.0 on error."""
        data = await self._get(f"/v4/tokens/{mint}/liquidity")
        if data and isinstance(data.get("liquidity"), (int, float)):
            return float(data["liquidity"])
        return 0.0

    async def token_top_holders(self, mint: str, limit: int = 20) -> list[dict]:
        """Get top holders. Returns list of {rank, ownerAddress, percentageOfSupplyHeld, valueUsd}."""
        data = await self._get(f"/v4/tokens/{mint}/top-holders", {"limit": limit})
        if data and isinstance(data.get("data"), list):
            return data["data"]
        return []

    async def token_trader_activity(self, mint: str, resolution: str = "1d",
                                     limit: int = 50) -> list[dict]:
        """Get trader activity: buy/sell volumes, PnL per trader."""
        data = await self._get(
            f"/v4/tokens/{mint}/trader-activity",
            {"resolution": resolution, "limit": limit},
        )
        if data and isinstance(data.get("data"), list):
            return data["data"]
        return []

    # ------------------------------------------------------------------
    # Wallet data
    # ------------------------------------------------------------------

    async def wallet_pnl(self, address: str, resolution: str = "30d") -> dict | None:
        """Get wallet PnL: win rate, realized/unrealized PnL, per-token performance."""
        return await self._get(
            f"/v4/wallets/{address}/pnl",
            {"resolution": resolution},
        )

    # ------------------------------------------------------------------
    # Safety gates (for entry pipeline)
    # ------------------------------------------------------------------

    async def check_liquidity(self, mint: str) -> tuple[bool, float, str]:
        """Check token liquidity. Returns (safe, liq_usd, reason)."""
        if not self.enabled:
            return True, 0.0, ""
        liq = await self.token_liquidity(mint)
        if liq <= 0:
            return True, liq, ""  # no data = don't block
        if liq < self.min_liquidity_usd:
            return False, liq, f"vybe_low_liq(${liq:.0f}<${self.min_liquidity_usd:.0f})"
        return True, liq, ""

    async def check_top_holders(self, mint: str) -> tuple[bool, float, str]:
        """Check top holder concentration. Returns (safe, top5_pct, reason)."""
        if not self.enabled:
            return True, 0.0, ""
        holders = await self.token_top_holders(mint, limit=5)
        if not holders:
            return True, 0.0, ""
        top5_pct = sum(h.get("percentageOfSupplyHeld", 0) for h in holders)
        if top5_pct > self.max_top_holder_pct:
            return False, top5_pct, f"vybe_top5_concentration({top5_pct:.1f}%>{self.max_top_holder_pct}%)"
        return True, top5_pct, ""

    async def check_buy_sell_ratio(self, mint: str) -> tuple[bool, float, str]:
        """Check buy/sell ratio from trader activity. Returns (safe, ratio, reason)."""
        if not self.enabled:
            return True, 1.0, ""
        activity = await self.token_trader_activity(mint, resolution="1d", limit=100)
        if not activity:
            return True, 1.0, ""
        total_buy_vol = sum(t.get("buyVolumeUsd", 0) for t in activity)
        total_sell_vol = sum(t.get("sellVolumeUsd", 0) for t in activity)
        if total_sell_vol <= 0:
            return True, 999.0, ""
        ratio = total_buy_vol / total_sell_vol
        if ratio < self.min_buy_sell_ratio:
            return False, ratio, f"vybe_selling_pressure(ratio={ratio:.2f}<{self.min_buy_sell_ratio})"
        return True, ratio, ""

    async def get_token_summary(self, mint: str) -> dict:
        """Get a summary dict for logging: {price, mcap, liq, top5_pct, buy_sell_ratio}."""
        details = await self.token_details(mint)
        liq = await self.token_liquidity(mint)
        holders = await self.token_top_holders(mint, limit=5)
        top5_pct = sum(h.get("percentageOfSupplyHeld", 0) for h in holders) if holders else 0.0

        return {
            "price": details.get("price") if details else 0.0,
            "mcap": details.get("marketCap") if details else 0.0,
            "vol24h": details.get("usdValueVolume24h") if details else 0.0,
            "liq": liq,
            "top5_pct": top5_pct,
            "holders": len(holders),
        }
