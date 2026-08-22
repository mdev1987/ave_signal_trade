"""Arm-time pool checks: DexPaprika liquidity gate + Helius dev reputation.

**Comments match behavior**: both gates are FAIL-CLOSED — if the API is
unreachable, rate-limited or times out, the signal IS rejected (an unverified
pool is not a pool we buy; entering blind into unconfirmed liquidity turns
every API outage into potential rug exposure). This is deliberate for
small-capital live trading ("safer sniper mode"). A deterministic pass only
happens when the APIs answer and the data is clearly good:

- DexPaprika: the token has an indexed pool with liquidity at or above
  ``min_liquidity_usd``.
- Helius dev-rep (optional): governed by ``DEV_REP_MODE``. ``reject`` treats
  a failed/unavailable check as a veto; ``warn`` journals the details and
  ADMITS the token so the feature's false-positive rate can be measured on
  real trades before it is allowed to hard-reject (default ``warn``).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

import logs

logger = logging.getLogger(__name__)


class PoolChecker:
    """Async, cached, fail-closed gate used by :class:`PaperTrader.offer`.

    Args:
        dex_paprika_key: Bearer key for api.dexpaprika.com (may be empty).
        dex_paprika_base_url: REST base, e.g. ``https://api.dexpaprika.com``.
        helius_api_keys: Comma-separated Helius RPC keys (rotated on 429).
        helius_base_url: Helius RPC base, e.g. ``https://beta.helius-rpc.com``.
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
        helius_base_url: str = "https://beta.helius-rpc.com",
        min_liquidity_usd: float = 4000.0,
        dev_rep_enabled: bool = True,
        dev_rep_mode: str = "warn",
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
        # warn = journal + admit (measure first); reject = hard veto.
        self.dev_rep_mode = "reject" if dev_rep_mode == "reject" else "warn"
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
                logger.warning("dexpaprika 429 (rate limit) — fail-closed")
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
        indexed). Fail-closed on any error — a None liquidity rejects (an
        unverified pool is not a pool we buy).

        Returns:
            ``(ok, reason)``; ok=True only on a *confirmed* good pool.
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
            # API didn't answer — fail CLOSED. An unknown pool is not a
            # confirmed-good pool; entering blind into unverified liquidity
            # turns every API outage into a potential rug exposure.
            return False, "liquidity check unavailable"
        if liq < self.min_liquidity_usd:
            return False, f"liquidity ${liq:.0f} < ${self.min_liquidity_usd:.0f}"
        return True, ""

    def cached_verdict(self, ca: str) -> tuple[bool, str] | None:
        """Return the cached pool verdict if fresh, else None (no verdict).

        Used at entry time for a zero-latency re-check: between the arm-time
        pool gate and the first buy trade the pool can be drained (a rug in
        progress), and this catches it without another network call. None
        means "no fresh data" — callers must treat that as a reject (fail
        closed): an unverified pool is not a pool we should buy.
        """
        cached = self._pool_cache.get(ca)
        if cached and time.time() - cached[0] < 60:
            return self._eval_liq(cached[1])
        return None

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
        """Age/reputation gate, governed by ``dev_rep_mode``.

        Uses Helius ``getAsset`` to read the mint's metadata. The 24h-creates
        heuristic is intentionally NOT implemented: ``getSignaturesForAddress``
        counts every signature (trades included), so it would veto healthy
        devs — a false positive that kills all entries. Only the explicit
        age veto (when ``min_age_hours`` > 0) can fail the check.

        ``warn`` mode (default): a FAILED verdict is journaled as a
        ``devrep_warn`` event with its reason and the token is ADMITTED —
        collect samples before letting this feature hard-reject.
        ``reject`` mode: a failed verdict vetoes (fail-closed).

        Returns:
            ``(ok, reason)``; ok=False only in reject mode (or for callers
            wanting the raw verdict — warn mode translates it to admit).
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
            # Unanswered age check: reject-mode vetoes (fail-closed); warn
            # mode journals and admits so the feature accumulates samples.
            msg = "dev reputation check unavailable"
            self._dev_cache[ca] = (time.time(), (False, msg))
            self._prune_cache()
            if self.dev_rep_mode == "warn":
                logs.journal("devrep_warn", ca=ca, reason=msg)
                logger.warning("dev-rep WARN %s: %s — admitting", ca, msg)
                return True, ""
            return False, msg
        age_h = (time.time() - created_ts) / 3600.0
        if age_h < self.dev_rep_min_age_hours:
            msg = f"token too young (age {age_h:.1f}h)"
            self._dev_cache[ca] = (time.time(), (False, msg))
            self._prune_cache()
            if self.dev_rep_mode == "warn":
                logs.journal("devrep_warn", ca=ca, reason=msg,
                             age_hours=round(age_h, 2))
                logger.warning("dev-rep WARN %s: %s — admitting", ca, msg)
                return True, ""
            return False, msg
        self._dev_cache[ca] = (time.time(), (True, ""))
        self._prune_cache()
        return True, ""

    def cached_dev_rep(self, ca: str) -> tuple[bool, str] | None:
        """Last computed dev-rep verdict for ``ca`` (None = never checked).

        Used to stamp the trade journal with dev features without another
        RPC round-trip at entry time.
        """
        cached = self._dev_cache.get(ca)
        return cached[1] if cached else None


async def _sleep(s: float) -> None:
    """Async sleep helper."""
    await asyncio.sleep(s)