"""DBotX data API client — token safety / rug filter (fail-open).

Used only as a cheap pre-trade rug check in the open gate: a token that still
holds an active **mint** or **freeze** authority, or is dangerously top-10
concentrated, is skipped. This is the one gap the bot had — it checked liquidity
and momentum but never whether the token could be minted/frozen away.

The check is strictly fail-open: a missing key, an IP-whitelist 403, a timeout,
or any unexpected response degrades to "allow" so a third-party outage can never
stop the bot from trading. A one-time warning is logged so the operator knows
the safety net is down (e.g. the key's allowed-IP list doesn't include this host).

Endpoint: GET {base_url}/kline/pair_info?chain=solana&pair=<PAIR>&type=safety
Auth: header `x-api-key`. Docs: https://docs.dbotx.com/reference/pair-info-safety
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class DBotXClient:
    """Minimal async DBotX client focused on the Pair Safety endpoint."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api-data-v1.dbotx.com",
        timeout_s: float = 3.0,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._client = httpx.AsyncClient(timeout=timeout_s, follow_redirects=True)
        self._warned = False
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        await self._client.aclose()

    async def hot_tokens(self, chain: str = "solana", limit: int = 20) -> list[dict[str, Any]]:
        """Get hot/trending tokens from DBotX.

        Returns list of token dicts with address, name, symbol, volume, etc.
        Fail-open: returns empty list on any error.
        """
        if not self.api_key:
            return []
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.base_url}/token/hot",
                    params={"chain": chain, "limit": limit},
                    headers={"x-api-key": self.api_key, "accept": "application/json"},
                ),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data.get("res", []) if isinstance(data, dict) else []
        except Exception:  # noqa: BLE001
            return []

    async def new_tokens(self, chain: str = "solana", limit: int = 20) -> list[dict[str, Any]]:
        """Get newly created tokens from DBotX.

        Returns list of token dicts with address, name, symbol, creation time, etc.
        Fail-open: returns empty list on any error.
        """
        if not self.api_key:
            return []
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.base_url}/token/new",
                    params={"chain": chain, "limit": limit},
                    headers={"x-api-key": self.api_key, "accept": "application/json"},
                ),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data.get("res", []) if isinstance(data, dict) else []
        except Exception:  # noqa: BLE001
            return []

    async def meme_tokens(self, chain: str = "solana", category: str = "new",
                          limit: int = 20) -> list[dict[str, Any]]:
        """Get meme tokens by category (new/completing/surging/completed).

        Returns list of token dicts with address, name, symbol, MC, etc.
        Fail-open: returns empty list on any error.
        """
        if not self.api_key:
            return []
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.base_url}/token/meme/{category}",
                    params={"chain": chain, "limit": limit},
                    headers={"x-api-key": self.api_key, "accept": "application/json"},
                ),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data.get("res", []) if isinstance(data, dict) else []
        except Exception:  # noqa: BLE001
            return []

    async def token_holders(self, chain: str, token: str,
                            limit: int = 100) -> list[dict[str, Any]]:
        """Get top holders for a token.

        Returns list of holder dicts with address, balance, percentage, etc.
        Fail-open: returns empty list on any error.
        """
        if not self.api_key:
            return []
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.base_url}/token/holders",
                    params={"chain": chain, "token": token, "limit": limit},
                    headers={"x-api-key": self.api_key, "accept": "application/json"},
                ),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data.get("res", []) if isinstance(data, dict) else []
        except Exception:  # noqa: BLE001
            return []

    async def dev_tokens(self, chain: str, wallet: str,
                         limit: int = 50) -> list[dict[str, Any]]:
        """Get all tokens created by a developer wallet.

        Returns list of token dicts. Useful for detecting serial token creators (rug signal).
        Fail-open: returns empty list on any error.
        """
        if not self.api_key:
            return []
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self.base_url}/token/dev",
                    params={"chain": chain, "wallet": wallet, "limit": limit},
                    headers={"x-api-key": self.api_key, "accept": "application/json"},
                ),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return data.get("res", []) if isinstance(data, dict) else []
        except Exception:  # noqa: BLE001
            return []

    async def pair_safety(self, chain: str, pair: str) -> dict[str, Any]:
        """Return the safety verdict.

        Shape: ``{"available", "safe", "mint_authority", "freeze_authority",
        "dev_position", "top10", "note"}``. On any failure ``available=False``
        and ``safe=True`` (fail-open) so callers never block on this.
        """
        if not self.api_key:
            return {"available": False, "safe": True, "note": "no_key"}
        async with self._lock:
            try:
                r = await asyncio.wait_for(
                    self._client.get(
                        f"{self.base_url}/kline/pair_info",
                        params={"chain": chain, "pair": pair, "type": "safety"},
                        headers={"x-api-key": self.api_key, "accept": "application/json"},
                    ),
                    timeout=self.timeout_s + 2.0,
                )
            except Exception as e:  # noqa: BLE001
                if not self._warned:
                    logger.warning("dbotx safety check unavailable: %s", e)
                    self._warned = True
                return {"available": False, "safe": True, "note": "error"}
            if r.status_code != 200:
                if not self._warned:
                    logger.warning(
                        "dbotx safety HTTP %s (key missing / IP not whitelisted?) "
                        "— rug filter disabled until fixed", r.status_code)
                    self._warned = True
                return {"available": False, "safe": True, "note": f"http{r.status_code}"}
            try:
                data = r.json()
            except Exception:  # noqa: BLE001
                return {"available": False, "safe": True, "note": "bad_json"}
            if data.get("err"):
                return {"available": False, "safe": True, "note": "err_flag"}
            si = (data.get("res") or {}).get("safetyInfo") or {}
            return {
                "available": True,
                "safe": True,  # final verdict is computed by the caller
                "mint_authority": bool(si.get("mintAuthority")),
                "freeze_authority": bool(si.get("freezeAuthority")),
                "dev_position": si.get("devPosition"),
                "top10": float(si.get("top10HolderRate") or 0.0),
                "note": "ok",
            }
