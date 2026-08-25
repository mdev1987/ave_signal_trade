"""DeBot.ai community-signal client (supplementary oracle).

DeBot aggregates "signal channel" activity: which tokens are being called by
tracked Telegram channels right now, how many channels called each token,
and whether those calls historically pumped (first_price -> max_price_gain).
All endpoints sit behind a Cloudflare JS challenge, so HTTP goes through
``ai-cloudscraper`` (cookie persistence reuses ``cf_clearance`` across calls)
executed in a worker thread — the sync scraper must never block the loop.

This is a SUPPLEMENTARY oracle only: every method returns ``None`` on any
failure and callers must treat that as "no data", never as a veto. Requests
are politeness-throttled (min interval) and TTL-cached (the rank/heatmap
payloads cover ALL recent tokens, so one fetch serves every signal in the
window).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_UA_PREFIX = "Mozilla/5.0 (X11; Linux x86_64)"


class DeBotClient:
    """Best-effort async client for DeBot.ai community signal endpoints.

    Args:
        enabled: Master switch; False makes every method a silent no-op.
        base_url: Site root (endpoints appended under ``/api/...``).
        timeout_s: Per-request timeout.
        min_interval_s: Minimum spacing between network calls (politeness;
            the Cloudflare path resets connections under bursts).
        cache_ttl_s: TTL for cached endpoint payloads. The rank/heatmap
            responses cover all recent tokens at once, so a short TTL lets
            every concurrent signal reuse one fetch.
    """

    def __init__(
        self,
        enabled: bool = True,
        base_url: str = "https://debot.ai",
        timeout_s: float = 12.0,
        min_interval_s: float = 1.5,
        cache_ttl_s: float = 45.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.base_url = base_url.rstrip("/")
        self.timeout_s = float(timeout_s)
        self.min_interval_s = float(min_interval_s)
        self.cache_ttl_s = float(cache_ttl_s)
        self._scraper: Any | None = None
        self._scraper_lock = asyncio.Lock()
        self._net_lock = asyncio.Lock()
        self._last_request_ts = 0.0
        self._cache: dict[str, tuple[float, Any]] = {}
        self._cache_max = 32
        self._fail_streak = 0
        # Circuit breaker: after N consecutive failures stop hammering for a
        # cooldown — Cloudflare escalates against IPs that keep retrying a
        # failed challenge, and each retry deepens the hole (observed live:
        # manual tests worked at 05:2x, were 403-looped by 06:0x).
        self._breaker_until = 0.0
        self.breaker_threshold = 3
        self.breaker_cooldown_s = 900.0

    # ------------------------------------------------------------- internals
    def _get_scraper(self) -> Any:
        """Create the sync scraper lazily (called inside the worker thread)."""
        if self._scraper is None:
            import cloudscraper  # heavy import deferred off the event loop

            try:
                # Cookie persistence DISABLED: a stale persisted cf_clearance
                # (bound to another fingerprint/UA) short-circuits challenge
                # solving and loops 403s forever (observed live 2026-08-25).
                self._scraper = cloudscraper.create_scraper(
                    browser="chrome", enable_cookie_persistence=False
                )
            except TypeError:  # older fork without the kwarg
                self._scraper = cloudscraper.create_scraper(browser="chrome")
        return self._scraper

    def _fetch_sync(self, url: str) -> Any | None:
        """Blocking GET + envelope unwrap (runs in a thread)."""
        s = self._get_scraper()
        r = s.get(url, timeout=self.timeout_s)
        if r.status_code != 200 or r.text.lstrip().startswith("<!DOCTYPE"):
            # Drop the session so the next call starts with a fresh TLS
            # fingerprint + clean cookies and re-solves the challenge instead
            # of retrying against whatever state produced this block.
            self._scraper = None
            raise RuntimeError(f"HTTP {r.status_code} (challenge?)")
        j = r.json()
        if not isinstance(j, dict) or j.get("code") not in (0, "0"):
            raise RuntimeError(f"envelope code={j.get('code') if isinstance(j, dict) else '?'}")
        return j.get("data")

    async def _fetch_json(self, path: str, params: str = "") -> Any | None:
        """Cached, throttled, thread-offloaded GET. None on any failure."""
        if not self.enabled:
            return None
        key = path + (("?" + params) if params else "")
        now = time.monotonic()
        hit = self._cache.get(key)
        if hit and now - hit[0] < self.cache_ttl_s:
            return hit[1]
        if now < self._breaker_until:
            return None  # circuit open — let Cloudflare's memory cool down
        async with self._net_lock:
            wait = self.min_interval_s - (time.monotonic() - self._last_request_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                data = await asyncio.to_thread(
                    self._fetch_sync, f"{self.base_url}{path}{('?' + params) if params else ''}"
                )
                self._last_request_ts = time.monotonic()
                self._fail_streak = 0
            except Exception as e:  # noqa: BLE001 - supplementary oracle only
                self._last_request_ts = time.monotonic()
                self._fail_streak += 1
                if self._fail_streak >= self.breaker_threshold:
                    self._breaker_until = (
                        time.monotonic() + self.breaker_cooldown_s
                    )
                    logger.warning(
                        "debot: %d consecutive failures — circuit open for "
                        "%.0fmin (Cloudflare escalation; will auto-retry)",
                        self._fail_streak,
                        self.breaker_cooldown_s / 60,
                    )
                elif self._fail_streak <= 2 or self._fail_streak % 20 == 0:
                    logger.warning("debot %s failed (%s)", path, e)
                # NEVER cache failures — a poisoned entry would suppress
                # enrichment for the whole TTL.
                return None
        if len(self._cache) >= self._cache_max:
            for k in list(self._cache)[: len(self._cache) - self._cache_max + 1]:
                self._cache.pop(k, None)
        self._cache[key] = (now, data)
        return data

    # ------------------------------------------------------------------ API
    async def activity_rank(
        self, chain: str = "solana", duration: str = "5m", limit: int = 50
    ) -> list[dict[str, Any]] | None:
        """Tokens ranked by signal-channel activity in the window."""
        data = await self._fetch_json(
            "/api/community/signal/channel/activity/rank",
            f"chain={chain}&limit={limit}&duration={duration}",
        )
        return data if isinstance(data, list) else None

    async def heatmap(self, chain: str = "solana") -> dict[str, Any] | None:
        """Per-token call stats keyed by mint (signal_count, max_price_gain…)."""
        data = await self._fetch_json(
            "/api/community/signal/channel/heatmap", f"chain={chain}"
        )
        if isinstance(data, dict):
            meta = data.get("meta") or {}
            sig = meta.get("signals")
            if isinstance(sig, dict):
                return sig
        return None

    async def token_klines(
        self, tokens: list[str], chain: str = "solana"
    ) -> dict[str, Any] | None:
        """Close-price series per token (auto interval by token age)."""
        if not tokens:
            return None
        q = "&".join(f"tokens={t}" for t in tokens[:10])
        data = await self._fetch_json(
            "/api/community/signal/channel/token/kline", f"chain={chain}&{q}"
        )
        return data if isinstance(data, dict) else None

    async def token_buzz(self, ca: str, chain: str = "solana") -> dict[str, Any] | None:
        """Everything DeBot knows about ONE token right now.

        Returns::

            {
              "rank_channels_5m": int,     # channels calling it in the window
              "heat_signal_count": int,    # all-time tracked call count
              "max_price_gain_pct": float, # best gain after first tracked call
              "token_level": str,          # e.g. "silver" (None when absent)
            }
        """
        rank_task = self.activity_rank(chain=chain, duration="5m", limit=50)
        heat_task = self.heatmap(chain=chain)
        rank, heat = await asyncio.gather(rank_task, heat_task)
        out: dict[str, Any] = {}
        if rank:
            hits = [r for r in rank if isinstance(r, dict) and r.get("address") == ca]
            if hits:
                out["rank_channels_5m"] = len(hits)
        if heat and ca in heat:
            h = heat[ca] or {}
            gain = h.get("max_price_gain")
            out["heat_signal_count"] = h.get("signal_count")
            out["max_price_gain_pct"] = (
                round(float(gain) * 100.0, 1) if isinstance(gain, (int, float)) else None
            )
            out["token_level"] = h.get("token_level") or None
        return out or None

    async def warmup(self) -> None:
        """Pre-solve the Cloudflare challenge once at startup.

        The first request pays the JS-challenge cost (seconds); doing it at
        boot keeps the first in-trade enrichment inside its latency budget.
        Result is intentionally ignored.
        """
        try:
            await self._fetch_json("/api/community/signal/channel/heatmap", "chain=solana")
        except Exception:  # noqa: BLE001
            pass

    async def aclose(self) -> None:
        """No persistent resources to release (kept for symmetry)."""
        return None
