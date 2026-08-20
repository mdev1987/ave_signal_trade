"""Arm-time pool checks: DexPaprika liquidity gate + Helius dev reputation.

Both are **fail-open**: if the API is unreachable, rate-limited or times out,
the signal is *not* rejected — we log the failure and let the trade proceed
(the Jupiter quote gate remains the hard filter). A deterministic rejection
only happens when the APIs answer and the data is clearly bad:

- DexPaprika: the token has no indexed pool, or its liquidity is below
  ``min_liquidity_usd``.
- Helius dev-rep (optional): the token's creator account has already created
  more than ``max_creates_24h`` tokens in the last 24h, or the token is
  younger than ``min_age_hours``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class PoolChecker:
    """Async, cached, fail-open gate used by :class:`PaperTrader.offer`.

    Args:
        dex_paprika_key: Bearer key for api.dexpaprika.com (may be empty).
        dex_paprika_base_url: REST base, e.g. ``https://api.dexpaprika.com``.
        helius_api_keys: Comma-separated Helius RPC keys (rotated on 429).
        helius_base_url: Helius RPC base, e.g. ``https://mainnet.helius-rpc.com``.
        min_liquidity_usd: Reject pools indexed below this liquidity.
        dev_rep_enabled: Run the Helius creator-reputation veto.
        dev_rep_max_creates_24h: Max token creations by the creator in 24h.
        dev_rep_min_age_hours: Reject tokens younger than this age.
        timeout_s: Per-request timeout.
    """

    def __init__(
        self,
        dex_paprika_key: str = "",
        dex_paprika_base_url: str = "https://api.dexpaprika.com",
        helius_api_keys: str = "",
        helius_base_url: str = "https://mainnet.helius-rpc.com",
        min_liquidity_usd: float = 4000.0,
        dev_rep_enabled: bool = True,
        dev_rep_max_creates_24h: int = 3,
        dev_rep_min_age_hours: float = 0.0,
        timeout_s: float = 2.5,
    ) -> None:
        self.dex_paprika_key = dex_paprika_key
        self.dex_paprika_base_url = dex_paprika_base_url.rstrip("/")
        self.helius_api_keys = [k.strip() for k in helius_api_keys.split(",") if k.strip()]
        self.helius_base_url = helius_base_url.rstrip("/")
        self.min_liquidity_usd = min_liquidity_usd
        self.dev_rep_enabled = dev_rep_enabled and bool(self.helius_api_keys)
        self.dev_rep_max_creates_24h = dev_rep_max_creates_24h
        self.dev_rep_min_age_hours = dev_rep_min_age_hours
        self.timeout_s = timeout_s
        self._client = httpx.AsyncClient(timeout=timeout_s, follow_redirects=True)
        self._pool_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._dev_cache: dict[str, tuple[float, tuple[bool, str]]] = {}
        self._helius_idx = 0
        # Bounded caches: every unique CA checked would otherwise stay cached
        # forever (the pool cache TTL is 60s, dev-rep TTL 600s, but entries are
        # never evicted). Cap both so long-running live mode stays flat.
        self._cache_max = 2000

    def _prune_cache(self) -> None:
        """Evict old entries from the bounded caches when over capacity."""
        for cache in (self._pool_cache, self._dev_cache):
            if len(cache) <= self._cache_max:
                continue
            # Drop entries older than their TTL first, then the oldest survivors.
            now = time.time()
            stale = [k for k, (ts, _) in cache.items() if now - ts > 600]
            for k in stale:
                cache.pop(k, None)
            while len(cache) > self._cache_max:
                cache.pop(next(iter(cache)), None)

    async def close(self) -> None:
        """Release the HTTP client."""
        await self._client.aclose()

    # ------------------------------------------------------------- dexpaprika
    async def _pool_liq(self, ca: str) -> float | None:
        """Return DexPaprika liquidity_usd for the token, or None if unknown.

        The signal's ``LP`` equals the pool id on PumpSwap, so a direct pool
        lookup is preferred; fall back to a token_address pool search.
        """
        headers = {"User-Agent": "ave-signal-trade/0.1"}
        if self.dex_paprika_key:
            headers["Authorization"] = f"Bearer {self.dex_paprika_key}"
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.dex_paprika_base_url}/networks/solana/pools/search",
                    params={"token_address": ca, "limit": 1},
                    headers=headers,
                ),
                timeout=self.timeout_s + 2.0,
            )
            r.raise_for_status()
            res = r.json().get("results", [])
            if not res:
                return None
            return float(res[0].get("liquidity_usd") or 0.0)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("dexpaprika 429 (rate limit) — fail-open")
            else:
                logger.warning("dexpaprika pool check failed %s: %s", e.response.status_code, ca)
            return None
        except Exception as e:  # noqa: BLE001
            logger.warning("dexpaprika pool check error %s: %s", ca, e)
            return None

    async def check_pool(
        self, ca: str, confirm_window_s: float = 10.0
    ) -> tuple[bool, str]:
        """Veto the signal when DexPaprika proves the pool is dead/empty.

        Retries for up to ``confirm_window_s`` (fresh pools take seconds to be
        indexed). Fail-open on any error — a None liquidity never rejects.

        Returns:
            ``(ok, reason)``; ok=False only on a *confirmed* bad pool.
        """
        cached = self._pool_cache.get(ca)
        if cached and time.time() - cached[0] < 60:
            return self._eval_liq(cached[1])

        deadline = time.time() + confirm_window_s
        last: dict[str, Any] | None = None
        while time.time() < deadline:
            liq = await self._pool_liq(ca)
            last = {"liq": liq, "ts": time.time()}
            if liq is not None:
                break
            await _sleep(0.4)
        self._pool_cache[ca] = (time.time(), last or {})
        self._prune_cache()
        return self._eval_liq(last or {})

    def _eval_liq(self, info: dict[str, Any]) -> tuple[bool, str]:
        liq = info.get("liq")
        if liq is None:
            return True, ""  # API didn't answer — fail open
        if liq < self.min_liquidity_usd:
            return False, f"liquidity ${liq:.0f} < ${self.min_liquidity_usd:.0f}"
        return True, ""

    # ---------------------------------------------------------------- helius
    async def _helius_rpc(self, method: str, params: list) -> dict | None:
        """POST a JSON-RPC call to Helius, rotating API keys on 429."""
        if not self.helius_api_keys:
            return None
        for _ in range(len(self.helius_api_keys)):
            key = self.helius_api_keys[self._helius_idx % len(self.helius_api_keys)]
            self._helius_idx += 1
            try:
                r = await asyncio.wait_for(
                    self._client.post(
                        f"{self.helius_base_url}/?api-key={key}",
                        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    ),
                    timeout=self.timeout_s + 2.0,
                )
                if r.status_code == 429:
                    continue
                r.raise_for_status()
                return r.json().get("result")
            except Exception as e:  # noqa: BLE001
                logger.warning("helius %s failed: %s", method, e)
                return None
        return None

    async def check_dev_rep(self, ca: str, created_ts: float | None = None) -> tuple[bool, str]:
        """Veto freshly-minted tokens (fail-open, conservative).

        Uses Helius ``getAsset`` to read the mint's metadata. The 24h-creates
        heuristic is intentionally NOT implemented: ``getSignaturesForAddress``
        counts every signature (trades included), so it would veto healthy
        devs — a false positive that kills all entries. Only the explicit
        age veto (when ``min_age_hours`` > 0) can reject, and only when Helius
        actually answers.

        Returns:
            ``(ok, reason)``; ok=False only when Helius answered and the
            token is provably too young.
        """
        if not self.dev_rep_enabled:
            return True, ""
        if self.dev_rep_min_age_hours <= 0 or not created_ts:
            return True, ""
        cached = self._dev_cache.get(ca)
        if cached and time.time() - cached[0] < 600:
            return cached[1]

        asset = await self._helius_rpc("getAsset", [ca])
        if asset is None:
            return True, ""  # fail-open
        age_h = (time.time() - created_ts) / 3600.0
        if age_h < self.dev_rep_min_age_hours:
            msg = f"age {age_h:.1f}h < {self.dev_rep_min_age_hours:.1f}h"
            self._dev_cache[ca] = (time.time(), (False, msg))
            self._prune_cache()
            return False, f"token too young (age {age_h:.1f}h)"
        self._dev_cache[ca] = (time.time(), (True, ""))
        self._prune_cache()
        return True, ""


async def _sleep(s: float) -> None:
    """Async sleep helper."""
    await asyncio.sleep(s)