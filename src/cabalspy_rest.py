"""CabalSpy REST companion — history backtest, bundle detail, wallet lookup.

The live bot consumes CabalSpy over WebSocket (see cabalspy.py). This
module covers the REST surface the WS lacks, verified 2026-09-12 against
https://docs.cabalspy.xyz plus live probes:

  GET /v1/signals/history?blockchain=&type=&days=[&min_wallets][&limit]
  GET /v1/bundle?blockchain=solana&mint=
  GET /v1/wallets/lookup?address=   (searches every chain + type)

Auth is ``?api_key=`` (the ``X-CabalSpy-Key`` header is NOT accepted —
server answers with the pay-per-call hint). Timestamps in ``data`` are
``"YYYY-MM-DD HH:MM:SS"`` with no timezone: ALWAYS UTC, never parse
them with naive fromisoformat (see parse_api_date).

Fail-open everywhere: any error returns None/[] so callers never block.
Key rotation skips credit-exhausted keys (key1 in .env is dead as of
2026-09-12 — rotation handles it, but remove it when convenient).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.cabalspy.xyz/v1"


def parse_api_date(s: str) -> datetime:
    """Parse a CabalSpy timestamp as UTC (never local time)."""
    s = (s or "").strip()
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.fromtimestamp(0, tz=timezone.utc)


class CabalSpyREST:
    """Minimal async REST client. One shared httpx client, key rotation."""

    def __init__(
        self,
        api_keys: list[str] | str | None = None,
        base_url: str = _BASE_URL,
        timeout_s: float = 10.0,
    ) -> None:
        if isinstance(api_keys, str):
            api_keys = [k.strip() for k in api_keys.split(",") if k.strip()]
        self._keys: list[str] = [k for k in (api_keys or []) if k]
        self._key_idx = 0
        self._dead: set[str] = set()
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout_s)
        self.last_rate_limit: dict[str, Any] = {}

    async def close(self) -> None:
        try:
            await self._client.aclose()
        except Exception:  # noqa: BLE001
            pass

    @property
    def live_keys(self) -> int:
        return len([k for k in self._keys if k not in self._dead])

    async def _get(self, path: str,
                   params: dict[str, Any]) -> dict[str, Any] | None:
        """GET with key rotation + rate-limit capture. None on any failure.

        Tries each live key once: an exhausted key rotates immediately
        instead of failing the call (key1 in .env is dead).
        """
        tried: set[str] = set()
        while True:
            live = [k for k in self._keys if k not in self._dead and k not in tried]
            if not live:
                return None
            key = live[self._key_idx % len(live)]
            tried.add(key)
            try:
                r = await self._client.get(
                    f"{self.base_url}{path}",
                    params={**params, "api_key": key},
                    headers={"accept": "application/json"},
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug("cabalspy REST %s failed: %s", path, exc)
                return None
            # Rate-limit visibility (SDK exposes last_rate_limit; we log it)
            for h in ("x-ratelimit-limit", "x-ratelimit-remaining",
                      "x-ratelimit-reset", "retry-after"):
                if r.headers.get(h) is not None:
                    self.last_rate_limit[h] = r.headers[h]
            if self.last_rate_limit.get("x-ratelimit-remaining") == "0":
                logger.warning("cabalspy REST quota exhausted on key %s…",
                               key[:8])
            if r.status_code != 200:
                # Error bodies are still JSON envelopes — a 403 here is
                # usually insufficient_credits, which must rotate the key
                # instead of failing the call.
                try:
                    data = r.json()
                except Exception:  # noqa: BLE001
                    data = None
                code = ((data or {}).get("error") or {}).get("code", "")
                if code == "insufficient_credits":
                    self._dead.add(key)
                    logger.warning("cabalspy REST: key %s… exhausted, rotating",
                                   key[:8])
                    continue  # retry with next live key
                logger.debug("cabalspy REST %s HTTP %s (%s)",
                             path, r.status_code, code)
                return None
            try:
                data = r.json()
            except Exception:  # noqa: BLE001
                return None
            if isinstance(data, dict) and data.get("success") is False:
                err = (data.get("error") or {})
                code = err.get("code", "")
                if code == "insufficient_credits":
                    self._dead.add(key)
                    logger.warning("cabalspy REST: key %s… exhausted, rotating",
                                   key[:8])
                    continue  # retry with next live key
                logger.debug("cabalspy REST %s error: %s", path, code)
                return None
            return data if isinstance(data, dict) else None

    # ------------------------------------------------------------ endpoints

    async def signals_history(
        self,
        blockchain: str = "solana",
        wallet_type: str = "kol",
        days: int = 7,
        min_wallets: int = 3,
        min_win_rate: float | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Historical cluster signals (backtest input). See signals-history.md."""
        params: dict[str, Any] = {
            "blockchain": blockchain, "type": wallet_type,
            "days": days, "min_wallets": min_wallets, "limit": limit,
        }
        if min_win_rate:
            params["min_win_rate"] = min_win_rate
        data = await self._get("/signals/history", params)
        if not data:
            return []
        return (data.get("data") or {}).get("signals") or []

    async def bundle_get(self, mint: str) -> dict[str, Any] | None:
        """Bundle snapshot for one Solana mint (200 + [] when clean)."""
        data = await self._get(
            "/bundle", {"blockchain": "solana", "mint": mint})
        if not data:
            return None
        return data.get("data")

    async def wallets_lookup(self, address: str) -> dict[str, Any] | None:
        """Identify a wallet across every chain + type (manual tool)."""
        data = await self._get("/wallets/lookup", {"address": address})
        if not data:
            return None
        return data.get("data")
