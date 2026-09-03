"""GMGN API client — thin async wrapper around gmgn_sdk.

The SDK handles auth (X-APIKEY + Ed25519 request signing), rate limiting,
Cloudflare bypass, and structured error handling. This module provides an
async-friendly interface for our event loop via ``asyncio.to_thread``.

All methods return None/empty on failure (fail-open).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cloudflare bypass: use ai-cloudscraper to solve the JS challenge once,
# then feed the clearance cookies into the SDK's httpx.Client.
# ---------------------------------------------------------------------------

_cf_cookies: dict[str, str] = {}
_cf_expiry: float = 0.0
_CF_TTL = 1800  # re-solve every 30 min


def _get_cf_cookies() -> dict[str, str]:
    """Return cached Cloudflare clearance cookies, refreshing if expired."""
    global _cf_expiry  # noqa: PLW0603
    now = time.time()
    if _cf_cookies and now < _cf_expiry:
        return _cf_cookies
    try:
        import cloudscraper

        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "linux", "mobile": False},
        )
        # Hit a lightweight GMGN endpoint to obtain cf_clearance
        resp = scraper.get("https://gmgn.ai/defi/quotation/v1/tokens/sol")
        if resp.status_code == 200:
            _cf_cookies.update(dict(scraper.cookies))
            _cf_expiry = now + _CF_TTL
            log.info("gmgn: cloudflare clearance obtained (%d cookies)", len(_cf_cookies))
        else:
            log.warning("gmgn: cloudscraper got status %d", resp.status_code)
    except Exception as exc:  # noqa: BLE001
        log.warning("gmgn: cloudscraper bypass failed: %s", exc)
    return _cf_cookies


def _build_httpx_client() -> httpx.Client:
    """Build an httpx.Client pre-loaded with Cloudflare clearance cookies."""
    cookies = _get_cf_cookies()
    return httpx.Client(
        timeout=30.0,
        cookies=cookies,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        },
    )


class GMGNClient:
    """Async wrapper around ``gmgn_sdk.GMGNClient``."""

    def __init__(self, api_key: str, private_key: str = "") -> None:
        from gmgn_sdk import GMGNClient as _SyncClient

        self._client = _SyncClient(
            api_key=api_key,
            private_key=private_key,
            http_client=_build_httpx_client(),
        )
        self._closed = False

    async def close(self) -> None:
        if not self._closed:
            self._client.close()
            self._closed = True

    async def _run(self, fn: Any, *args: Any, **kw: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kw)

    # -------------------------------------------------------- token security
    async def token_security(self, ca: str, **_: Any) -> dict | None:
        try:
            return await self._run(self._client.getTokenSecurity, "sol", ca)
        except Exception as exc:  # noqa: BLE001
            log.warning("gmgn token_security failed for %s: %s", ca[:10], exc)
            return None

    # ---------------------------------------------------------- token info
    async def token_info(self, ca: str, **_: Any) -> dict | None:
        try:
            return await self._run(self._client.getTokenInfo, "sol", ca)
        except Exception as exc:  # noqa: BLE001
            log.warning("gmgn token_info failed for %s: %s", ca[:10], exc)
            return None

    # ---------------------------------------------------------- kline data
    async def get_kline(
        self, ca: str, resolution: str = "15m", limit: int = 100, **_: Any
    ) -> list[dict]:
        try:
            data = await self._run(
                self._client.getTokenKline, "sol", ca, resolution
            )
            if isinstance(data, dict):
                return data.get("list", [])
            return data if isinstance(data, list) else []
        except Exception as exc:  # noqa: BLE001
            log.warning("gmgn kline failed for %s: %s", ca[:10], exc)
            return []

    # ------------------------------------------------- smart money / KOL feed
    async def get_smartmoney_trades(self, limit: int = 100, **_: Any) -> list[dict]:
        try:
            data = await self._run(self._client.getSmartMoney, "sol", limit=limit)
            if isinstance(data, dict):
                return data.get("list", [])
            return data if isinstance(data, list) else []
        except Exception as exc:  # noqa: BLE001
            log.warning("gmgn smartmoney failed: %s", exc)
            return []

    async def get_kol_trades(self, limit: int = 100, **_: Any) -> list[dict]:
        try:
            data = await self._run(self._client.getKol, "sol", limit=limit)
            if isinstance(data, dict):
                return data.get("list", [])
            return data if isinstance(data, list) else []
        except Exception as exc:  # noqa: BLE001
            log.warning("gmgn kol failed: %s", exc)
            return []

    # ------------------------------------------------- wallet profits (batch)
    async def get_wallet_profits(
        self, wallets: list[str], period: str = "all", **_: Any
    ) -> dict[str, dict]:
        """Batch wallet PnL.

        The SDK's ``getWalletStats`` does not support batch queries the same
        way the old POST endpoint did, and the old endpoint is Cloudflare-
        blocked.  Return empty dict so the caller falls back to file-based
        weights.
        """
        return {}

    # ------------------------------------------------------------ health
    def health(self) -> dict:
        return {"connected": not self._closed, "sdk": "gmgn-sdk", "cf_cookies": len(_cf_cookies)}
