"""Unified Helius API client — DAS token analysis + Wallet Identity + RPC.

Combines Helius REST endpoints (DAS, Wallet Identity) with the existing RPC
rotation logic.  All methods are fail-open: errors return None/empty dict so
the bot never halts on a Helius hiccup.

Usage::

    helius = HeliusClient(api_keys=["key1", "key2"], rpc_url="https://...")
    asset = await helius.get_asset("token_mint...")
    identity = await helius.wallet_identity("deployer_wallet...")
    holders = await helius.get_top_holders("token_mint...")
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

_DAS_TIMEOUT = 10.0
_IDENTITY_TIMEOUT = 8.0
_HOLDER_TIMEOUT = 10.0


class HeliusClient:
    """Unified Helius REST + RPC client with key rotation."""

    def __init__(
        self,
        api_keys: list[str] | None = None,
        rpc_url: str = "https://mainnet.helius-rpc.com",
    ) -> None:
        self._api_keys = [k for k in (api_keys or []) if k]
        self._rpc_url = rpc_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        # Per-key 429 cooldown
        self._cooldown_until: dict[str, float] = {}
        self._cooldown_s = 60.0
        # Stats
        self._calls = 0
        self._errors = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )
        return self._session

    def _get_key(self) -> str | None:
        """Pick the first non-cooled-down API key."""
        now = time.time()
        for key in self._api_keys:
            if self._cooldown_until.get(key, 0) < now:
                return key
        return self._api_keys[0] if self._api_keys else None

    def _mark_cooldown(self, key: str) -> None:
        self._cooldown_until[key] = time.time() + self._cooldown_s

    # ── DAS API ─────────────────────────────────────────────────────

    async def get_asset(self, mint: str) -> dict | None:
        """Get full token metadata via DAS getAsset."""
        key = self._get_key()
        if not key:
            return None
        payload = {
            "jsonrpc": "2.0",
            "id": "das-getAsset",
            "method": "getAsset",
            "params": {"id": mint},
        }
        try:
            session = await self._get_session()
            url = f"{self._rpc_url}/?api-key={key}"
            self._calls += 1
            async with session.post(url, json=payload) as resp:
                if resp.status == 429:
                    self._mark_cooldown(key)
                    log.warning("helius DAS 429 — key cooled down")
                    return None
                if resp.status != 200:
                    self._errors += 1
                    return None
                data = await resp.json()
                return data.get("result")
        except Exception:
            self._errors += 1
            log.debug("helius getAsset failed for %s", mint[:10])
            return None

    async def get_top_holders(
        self, mint: str, limit: int = 20
    ) -> list[dict]:
        """Get top holders via getTokenLargestAccounts.

        Returns list of {address, amount, decimals, uiAmount}.
        """
        key = self._get_key()
        if not key:
            return []
        payload = {
            "jsonrpc": "2.0",
            "id": "das-getTopHolders",
            "method": "getTokenLargestAccounts",
            "params": [mint],
        }
        try:
            session = await self._get_session()
            url = f"{self._rpc_url}/?api-key={key}"
            self._calls += 1
            async with session.post(url, json=payload) as resp:
                if resp.status == 429:
                    self._mark_cooldown(key)
                    return []
                if resp.status != 200:
                    self._errors += 1
                    return []
                data = await resp.json()
                accounts = data.get("result", {}).get("value", [])
                return [
                    {
                        "address": a.get("address", ""),
                        "amount": a.get("amount", "0"),
                        "decimals": a.get("decimals", 0),
                        "uiAmount": a.get("uiAmount", 0),
                    }
                    for a in accounts[:limit]
                ]
        except Exception:
            self._errors += 1
            log.debug("helius getTopHolders failed for %s", mint[:10])
            return []

    async def get_token_supply(self, mint: str) -> float:
        """Get total token supply as float."""
        key = self._get_key()
        if not key:
            return 0.0
        payload = {
            "jsonrpc": "2.0",
            "id": "das-supply",
            "method": "getTokenSupply",
            "params": [mint],
        }
        try:
            session = await self._get_session()
            url = f"{self._rpc_url}/?api-key={key}"
            self._calls += 1
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    self._errors += 1
                    return 0.0
                data = await resp.json()
                value = data.get("result", {}).get("value", {})
                amount = float(value.get("amount", "0"))
                decimals = value.get("decimals", 0)
                return amount / (10 ** decimals) if decimals else amount
        except Exception:
            self._errors += 1
            return 0.0

    # ── Wallet Identity ─────────────────────────────────────────────

    async def wallet_identity(self, wallet: str) -> dict | None:
        """Check wallet identity — returns classification.

        Response includes ``category`` (CEX, DeFi, Rugger, Scammer,
        Bot, Market Maker, etc.), ``labels``, and confidence scores.
        """
        key = self._get_key()
        if not key:
            return None
        try:
            session = await self._get_session()
            url = f"https://api.helius.xyz/v1/wallet/{wallet}/identity?api-key={key}"
            self._calls += 1
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=_IDENTITY_TIMEOUT)
            ) as resp:
                if resp.status == 429:
                    self._mark_cooldown(key)
                    return None
                if resp.status != 200:
                    self._errors += 1
                    return None
                return await resp.json()
        except Exception:
            self._errors += 1
            log.debug("helius wallet_identity failed for %s", wallet[:8])
            return None

    async def batch_wallet_identity(
        self, wallets: list[str]
    ) -> dict[str, dict]:
        """Batch identity lookup for up to 100 wallets.

        Returns {wallet: identity_dict}.
        """
        if not wallets:
            return {}
        key = self._get_key()
        if not key:
            return {}
        try:
            session = await self._get_session()
            url = f"https://api.helius.xyz/v1/wallet/batch-identity?api-key={key}"
            self._calls += 1
            async with session.post(
                url,
                json={"wallets": wallets[:100]},
                timeout=aiohttp.ClientTimeout(total=_IDENTITY_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    self._errors += 1
                    return {}
                data = await resp.json()
                # Response is a list; zip with input wallets
                result = {}
                identities = data if isinstance(data, list) else data.get("identities", [])
                for w, ident in zip(wallets[:len(identities)], identities):
                    if ident:
                        result[w] = ident
                return result
        except Exception:
            self._errors += 1
            return {}

    async def wallet_funded_by(self, wallet: str) -> dict | None:
        """Check original SOL funding source for a wallet.

        Detects exchange, bot, sybil origins.
        """
        key = self._get_key()
        if not key:
            return None
        try:
            session = await self._get_session()
            url = f"https://api.helius.xyz/v1/wallet/{wallet}/funded-by?api-key={key}"
            self._calls += 1
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=_IDENTITY_TIMEOUT)
            ) as resp:
                if resp.status != 200:
                    self._errors += 1
                    return None
                return await resp.json()
        except Exception:
            self._errors += 1
            return None

    # ── Composite Safety Check ──────────────────────────────────────

    async def token_safety(self, mint: str) -> dict:
        """Run full token safety analysis: holders + supply + deployer identity.

        Returns:
            {
                "top_holders": [...],
                "top10_pct": float,  # % of supply held by top 10
                "top1_holder_pct": float,
                "supply": float,
                "deployer_identity": dict | None,  # if deployer is known rugger
                "safe": bool,  # composite verdict
            }
        """
        holders, supply = await asyncio.gather(
            self.get_top_holders(mint),
            self.get_token_supply(mint),
        )

        result: dict[str, Any] = {
            "top_holders": holders,
            "top10_pct": 0.0,
            "top1_holder_pct": 0.0,
            "supply": supply,
            "deployer_identity": None,
            "safe": True,
        }

        if not holders or supply <= 0:
            return result

        # Calculate concentration
        total_held = sum(h.get("uiAmount", 0) for h in holders)
        top10_ui = sum(h.get("uiAmount", 0) for h in holders[:10])
        top1_ui = holders[0].get("uiAmount", 0) if holders else 0
        result["top10_pct"] = (top10_ui / supply * 100) if supply else 0
        result["top1_holder_pct"] = (top1_ui / supply * 100) if supply else 0

        # Check deployer identity (first holder is often the deployer)
        if holders:
            deployer = holders[0].get("address", "")
            if deployer:
                ident = await self.wallet_identity(deployer)
                result["deployer_identity"] = ident
                if ident:
                    cats = [c.lower() for c in (ident.get("categories") or [])]
                    if any(c in cats for c in ["rugger", "scammer", "exploiter"]):
                        result["safe"] = False
                        log.warning(
                            "helius: deployer %s is %s — marking unsafe",
                            deployer[:8], cats,
                        )

        return result

    # ── Lifecycle ───────────────────────────────────────────────────

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def stats(self) -> dict:
        return {"calls": self._calls, "errors": self._errors}
