"""Birdeye Data API — holder cohorts + smart-money enrichment (journal-only).

Phase 1: measure, don't gate. Every opened position gets one holder-profile
+ one top-traders fetch (TTL-cached, fail-open), journaled as
``birdeye_enrich``. After 1-2 weeks the journal tells us whether bundler /
insider / sniper exposure predicts the trade outcome — only then does any
of this become an entry gate.

Cost discipline: ~50 CU per open (25 + ~25), only on OPENS (a handful per
day), never per signal. TTL cache (15 min) dedupes re-entries. All failures
degrade to "no data", never block the open path.
"""

from __future__ import annotations

import logging
import time

import httpx

log = logging.getLogger(__name__)

BASE_URL = "https://public-api.birdeye.so"
CACHE_TTL_S = 900.0

# Tags Birdeye assigns to holder cohorts.
RISK_TAGS = ("bundler", "sniper", "insider", "dev")
GOOD_TAGS = ("smart_trader", "kol")


def summarize_holder_profile(data: dict) -> dict:
    """Compress a holder-profile payload to gate-relevant features.

    Pure function (unit-tested). All numeric parsing is defensive: any
    missing/malformed field degrades to 0.0, never raises.
    """
    out = {"top10_pct": 0.0, "risk_pct": 0.0, "good_pct": 0.0,
           "insider_pct": 0.0, "bundler_pct": 0.0, "dev_pct": 0.0,
           "sniper_pct": 0.0, "smart_pct": 0.0, "kol_pct": 0.0,
           "labeled_holders": 0}
    if not isinstance(data, dict):
        return out
    try:
        token = data.get("token") or {}
        top10 = token.get("top10_holder") or {}
        out["top10_pct"] = float(top10.get("percent_of_supply") or 0.0)
    except (TypeError, ValueError):
        pass
    try:
        summary = data.get("holder_summary") or {}
        out["labeled_holders"] = int(summary.get("total_holder") or 0)
    except (TypeError, ValueError):
        pass
    for row in data.get("tags") or []:
        if not isinstance(row, dict):
            continue
        tag = row.get("tag")
        try:
            pct = float(row.get("percent_of_supply") or 0.0)
        except (TypeError, ValueError):
            pct = 0.0
        if tag == "insider":
            out["insider_pct"] = pct
        elif tag == "bundler":
            out["bundler_pct"] = pct
        elif tag == "dev":
            out["dev_pct"] = pct
        elif tag == "sniper":
            out["sniper_pct"] = pct
        elif tag == "smart_trader":
            out["smart_pct"] = pct
        elif tag == "kol":
            out["kol_pct"] = pct
    out["risk_pct"] = round(
        out["insider_pct"] + out["bundler_pct"] + out["dev_pct"]
        + out["sniper_pct"], 2)
    out["good_pct"] = round(out["smart_pct"] + out["kol_pct"], 2)
    return out


def summarize_top_traders(items: list) -> dict:
    """Compress top-traders rows to flow features.

    Pure function (unit-tested). Key question answered: are the active
    wallets still holding (conviction) or fully exited (distribution)?
    """
    out = {"n": 0, "exited_frac": 0.0, "tags": {}, "buy_usd": 0.0,
           "sell_usd": 0.0}
    if not items:
        return out
    exited = 0
    tags: dict[str, int] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        out["n"] += 1
        try:
            hold = float(it.get("holdVolumeUsd") or 0.0)
        except (TypeError, ValueError):
            hold = 0.0
        if hold <= 0:
            exited += 1
        for t in it.get("tags") or []:
            tags[t] = tags.get(t, 0) + 1
        try:
            out["buy_usd"] += float(it.get("volumeBuyUSD") or 0.0)
            out["sell_usd"] += float(it.get("volumeSellUSD") or 0.0)
        except (TypeError, ValueError):
            pass
    out["exited_frac"] = round(exited / out["n"], 3) if out["n"] else 0.0
    out["buy_usd"] = round(out["buy_usd"], 2)
    out["sell_usd"] = round(out["sell_usd"], 2)
    out["tags"] = tags
    return out


class BirdeyeClient:
    """Thin Birdeye Data API client (fail-open, TTL-cached, CU-counted)."""

    def __init__(self, api_key: str = "",
                 base_url: str = BASE_URL,
                 enabled: bool = True,
                 cache_ttl_s: float = CACHE_TTL_S,
                 timeout_s: float = 12.0) -> None:
        self._api_key = (api_key or "").strip()
        self._base = base_url.rstrip("/")
        self.enabled = bool(enabled) and bool(self._api_key)
        self._ttl = float(cache_ttl_s)
        self._timeout = float(timeout_s)
        self._client: httpx.AsyncClient | None = None
        self._cache: dict[str, tuple[float, object]] = {}
        self.calls = 0
        self.cached = 0
        self.errors = 0

    @property
    def stats(self) -> dict:
        return {"enabled": self.enabled, "calls": self.calls,
                "cached": self.cached, "errors": self.errors}

    async def _get(self, path: str, params: dict) -> dict | None:
        if not self.enabled:
            return None
        try:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=self._timeout)
            resp = await self._client.get(
                f"{self._base}{path}", params=params,
                headers={"X-API-KEY": self._api_key, "x-chain": "solana",
                         "accept": "application/json"})
        except Exception as exc:
            self.errors += 1
            log.debug("birdeye GET %s failed: %s", path, exc)
            return None
        if resp.status_code == 429:
            self.errors += 1
            log.warning("birdeye rate limited on %s", path)
            return None
        if resp.status_code != 200:
            self.errors += 1
            log.debug("birdeye GET %s HTTP %s", path, resp.status_code)
            return None
        try:
            body = resp.json()
        except ValueError:
            self.errors += 1
            return None
        if not body.get("success"):
            self.errors += 1
            return None
        self.calls += 1
        return body.get("data")

    async def _cached(self, key: str, path: str, params: dict):
        now = time.monotonic()
        hit = self._cache.get(key)
        if hit and now - hit[0] < self._ttl:
            self.cached += 1
            return hit[1]
        data = await self._get(path, params)
        if data is not None:
            self._cache[key] = (now, data)
            if len(self._cache) > 500:
                stale = [k for k, (ts, _) in self._cache.items()
                         if now - ts > self._ttl]
                for k in stale:
                    self._cache.pop(k, None)
        return data

    async def holder_profile(self, mint: str) -> dict | None:
        """Raw holder-profile payload (cached)."""
        return await self._cached(f"hp:{mint}", "/token/v1/holder-profile",
                                  {"token_address": mint, "interval": "1h"})

    async def top_traders(self, mint: str, limit: int = 10) -> list:
        """Raw top-traders rows (cached)."""
        data = await self._cached(
            f"tt:{mint}:{limit}", "/defi/v2/tokens/top_traders",
            {"address": mint, "time_frame": "24h", "sort_by": "volume",
             "sort_type": "desc", "offset": 0, "limit": limit})
        if isinstance(data, dict):
            return data.get("items") or []
        return data or []

    async def smart_money_list(self, interval: str = "1d",
                               trader_style: str = "trenchers",
                               limit: int = 20) -> list:
        """Smart-money ranked token list (uncached discovery call)."""
        data = await self._get(
            "/smart-money/v1/token/list",
            {"interval": interval, "trader_style": trader_style,
             "sort_by": "net_flow", "sort_type": "desc",
             "offset": 0, "limit": limit})
        return data if isinstance(data, list) else []

    async def enrich(self, mint: str) -> dict | None:
        """One-shot enrichment for an opened position (journal payload)."""
        profile = await self.holder_profile(mint)
        traders = await self.top_traders(mint)
        if profile is None and not traders:
            return None
        return {
            "profile": summarize_holder_profile(profile or {}),
            "flow": summarize_top_traders(traders),
        }

    async def close(self) -> None:
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception as exc:
                log.debug("birdeye close failed: %s", exc)
            self._client = None
