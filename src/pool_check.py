"""Arm-time pool checks: multi-oracle liquidity gate + Helius dev reputation.

Liquidity oracle cascade (first decisive answer wins):

1. **DexPaprika** pool search — the historical gate. An explicit liquidity
   number is decisive (>= min passes, < min rejects).
2. **PumpAPI stream state** — zero-latency, already subscribed. For
   curve-phase pump.fun mints (`...pump`) with no indexed external pool yet,
   fresh stream activity proves the token is trading RIGHT NOW on its
   bonding curve, whose liquidity is mathematical (an LP-pull rug is
   impossible pre-graduation). This catches the graduation-minute race that
   fail-closed DexPaprika used to lose (WOFI, 2026-08-24: skipped at the
   exact minute it graduated, then pumped ~1380x).
3. **DexScreener** `/token-pairs` — fast third-party indexer as the last
   oracle; admits when liquidity >= min OR the pair shows real recent
   trading activity (mcap + m5/h1 volume) on a curve mint.

Red-flag rug vetoes apply in EVERY branch: a liquidity REMOVAL seen within
the veto window, or set mint/freeze authorities known from the stream.

**Comments match behavior**: if every oracle is silent the signal is still
rejected (FAIL-CLOSED — an unverified pool is not a pool we buy). Helius
dev-rep remains governed by ``DEV_REP_MODE`` (``warn`` journals + admits,
``reject`` vetoes fail-closed).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
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
        stream_state_fn: Optional callable ``mint -> dict | None`` returning
            PumpAPI per-mint stream state (oracle 2; wired from PriceFeed).
        dexscreener: Optional :class:`dexscreener.DexScreenerClient`
            (oracle 3).
        curve_fallback_enabled: Admit curve-phase ``...pump`` mints via the
            stream/DexScreener oracles instead of failing closed when
            DexPaprika has no pool yet.
        curve_stream_max_age_s: Max age of stream state for it to count as
            "actively trading" evidence.
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
        stream_state_fn: Callable[[str], dict | None] | None = None,
        dexscreener: Any | None = None,
        curve_fallback_enabled: bool = True,
        curve_stream_max_age_s: float = 90.0,
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
        self.stream_state_fn = stream_state_fn
        self.dexscreener = dexscreener
        self.curve_fallback_enabled = curve_fallback_enabled
        self.curve_stream_max_age_s = curve_stream_max_age_s
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
        """Multi-oracle liquidity verdict for ``ca`` (see module docstring).

        Retries the cascade for up to ``confirm_window_s`` (fresh pools take
        seconds to be indexed). Fail-closed only when EVERY oracle stays
        silent; an explicit low DexPaprika liquidity still rejects outright.

        Returns:
            ``(ok, reason)``; ok=True on a confirmed-good or curve-active
            pool, with the deciding oracle recorded in the journal.
        """
        cached = self._pool_cache.get(ca)
        if cached and time.time() - cached[0] < 60:
            info = cached[1]
            return bool(info.get("ok")), str(info.get("reason") or "")

        deadline = time.time() + confirm_window_s
        decision: dict[str, Any] = {}
        ds_done = False
        while True:
            # Red-flag rug vetoes first — they apply regardless of which
            # oracle would otherwise admit the token.
            flags = self._stream_red_flags(ca)
            if flags:
                decision = {"liq": None, "src": "stream", "ok": False,
                            "reason": flags}
                break
            liq = await self._pool_liq(ca)
            if liq is not None:
                # Explicit answer: decisive either way (>= min passes).
                ok = liq >= self.min_liquidity_usd
                reason = "" if ok else (
                    f"liquidity ${liq:.0f} < ${self.min_liquidity_usd:.0f}")
                decision = {"liq": liq, "src": "dexpaprika", "ok": ok,
                            "reason": reason}
                break
            verdict = self._curve_verdict(ca)
            if verdict is not None:
                decision = {**verdict}
                break
            if not ds_done:
                ds_done = True
                ds = await self._dexscreener_verdict(ca)
                if ds is not None:
                    decision = {**ds}
                    break
            if time.time() >= deadline:
                break
            await _sleep(0.4)
        if not decision:
            decision = {"liq": None, "src": "none", "ok": False,
                        "reason": "liquidity check unavailable"}
        self._pool_cache[ca] = (time.time(), decision)
        self._prune_cache()
        logs.journal("pool_oracle", ca=ca, src=decision.get("src"),
                     ok=decision.get("ok"), liq=decision.get("liq"))
        return bool(decision.get("ok")), str(decision.get("reason") or "")

    def _stream_red_flags(self, ca: str) -> str:
        """Rug red flags visible in the stream state (empty = clean)."""
        st = self.stream_state_fn(ca) if self.stream_state_fn else None
        if not st:
            return ""
        removed_s_ago = (
            time.time() - st["liq_removed_ts"] if st.get("liq_removed_ts") else None
        )
        if removed_s_ago is not None and removed_s_ago <= 120:
            return f"liq_removed:{removed_s_ago:.0f}s_ago"
        if st.get("mint_authority_set"):
            return "mint_authority_set"
        if st.get("freeze_authority_set"):
            return "freeze_authority_set"
        return ""

    def _curve_verdict(self, ca: str) -> dict[str, Any] | None:
        """Curve-phase admission via the stream oracle (None = not decisive).

        Only ``...pump`` mints qualify for the fallback: bonding-curve
        liquidity cannot be pulled pre-graduation, so fresh trading activity
        is sufficient evidence of a tradeable market.
        """
        if not self.curve_fallback_enabled or not ca.endswith("pump"):
            return None
        st = self.stream_state_fn(ca) if self.stream_state_fn else None
        if st:
            age = time.time() - st.get("ts", 0)
            qi = st.get("quote_in_pool")
            if age <= self.curve_stream_max_age_s and (qi is None or qi > 0):
                return {"liq": qi, "src": "stream_curve", "ok": True,
                        "reason": "",
                        "quote_in_pool": qi, "stream_age_s": round(age, 1)}
        return None

    async def _dexscreener_verdict(self, ca: str) -> dict[str, Any] | None:
        """Oracle-3 admission via DexScreener (None = silent/not applicable).

        Admits when liquidity >= min, or when a curve mint shows real recent
        activity on DexScreener (mcap + m5/h1 volume or trades) — the indexer
        that already had WOFI's PumpSwap pair while DexPaprika stayed silent.
        """
        if self.dexscreener is None:
            return None
        snap = await self.dexscreener.token_pairs("solana", ca)
        if snap is None:
            return None
        liq = snap.get("liq")
        mcap = snap.get("mcap") or 0
        vol = (snap.get("vol_m5") or 0) + (snap.get("vol_h1") or 0)
        active = mcap > 0 and (vol > 0 or (snap.get("txns_m5") or 0) > 0)
        curve_ok = self.curve_fallback_enabled and ca.endswith("pump") and active
        if liq is not None and liq >= self.min_liquidity_usd:
            return {"liq": liq, "src": "dexscreener", "ok": True, "reason": "",
                    "mcap": mcap}
        if curve_ok:
            return {"liq": liq, "src": "dexscreener", "ok": True, "reason": "",
                    "mcap": mcap, "vol_m5h1": vol}
        return {
            "liq": liq, "src": "dexscreener", "ok": False,
            "reason": (
                f"dexscreener liq ${liq or 0:.0f} mcap ${mcap:.0f} "
                f"below thresholds"
            ),
        }

    def _eval_liq(self, info: dict[str, Any]) -> tuple[bool, str]:
        """Legacy single-oracle evaluation (kept for cache compatibility)."""
        if "ok" in info:
            return bool(info["ok"]), str(info.get("reason") or "")
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