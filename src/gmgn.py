"""GMGN API client — thin async wrapper around gmgn_sdk.

The SDK handles auth (X-APIKEY + Ed25519 request signing), rate limiting,
Cloudflare bypass, and structured error handling. This module provides an
async-friendly interface for our event loop via ``asyncio.to_thread``.

All methods return None/empty on failure (fail-open).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from gmgn_sdk import GMGNClient as _SyncClient  # noqa: E402

log = logging.getLogger(__name__)


class GMGNClient:
    """Async wrapper around ``gmgn_sdk.GMGNClient``."""

    def __init__(self, api_key: str, private_key: str = "") -> None:
        self._client = _SyncClient(api_key=api_key, private_key=private_key)
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
        return {"connected": not self._closed, "sdk": "gmgn-sdk"}
