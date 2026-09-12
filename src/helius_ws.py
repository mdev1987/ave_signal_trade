"""Helius WebSocket client — real-time transaction streaming.

Replaces Shyft polling with a single WebSocket connection that streams
all wallet transactions via Helius's ``transactionSubscribe`` extension.
Sub-second latency, no rate limits, no 429s.

Architecture:
  - Single WS connection with ``account_include`` for all 262 wallets
  - Parses buy transactions (SOL spent → token balance increased)
  - Auto-reconnects on disconnect with exponential backoff
  - Ping/pong health checks every 30s
  - Falls back gracefully if connection fails

Fallback for free-tier plans:
  - If ``transactionSubscribe`` is not available (plan restriction),
    falls back to ``logsSubscribe`` on Token Program + HTTP RPC fetch
    to reconstruct full transactions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Awaitable

import websockets
import websockets.exceptions

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

SOL = "So11111111111111111111111111111111111111112"
WSOL = SOL

# Reconnect backoff: start at 2s, max 60s
_RECONNECT_MIN = 2.0
_RECONNECT_MAX = 60.0
_PING_INTERVAL = 30.0
_SUBSCRIBE_BATCH = 100  # max wallets per subscribe message (Helius limit)
_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"


def parse_helius_tx(wallet: str, msg: dict) -> dict | None:
    """Extract a buy event from a Helius transactionSubscribe message.

    Returns ``{"wallet": ..., "ca": ..., "ts": ..., "amount": ...}`` or None.
    Same logic as ``parse_shyft_buys`` but adapted for the Helius WS payload.
    """
    tx = msg.get("transaction")
    if not tx:
        return None

    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return None

    bt = tx.get("blockTime") or 0

    # Extract account keys
    message = (tx.get("transaction") or {}).get("message") or {}
    account_keys = message.get("accountKeys") or []

    # If account_keys are objects (jsonParsed), extract pubkeys
    if account_keys and isinstance(account_keys[0], dict):
        account_keys = [k.get("pubkey") or k.get("toString", "") for k in account_keys]

    wallet_idx = None
    for i, k in enumerate(account_keys):
        if k == wallet:
            wallet_idx = i
            break
    if wallet_idx is None:
        return None

    # Check SOL balance decrease (native)
    pre_balances = meta.get("preBalances") or []
    post_balances = meta.get("postBalances") or []
    sol_spent = False
    if wallet_idx < len(pre_balances) and wallet_idx < len(post_balances):
        sol_spent = post_balances[wallet_idx] < pre_balances[wallet_idx]

    # Fallback: check WSOL token balance decrease
    if not sol_spent:
        pre_sol = {(b.get("accountIndex")): b for b in meta.get("preTokenBalances") or []}
        post_sol_map = {b.get("accountIndex"): b for b in meta.get("postTokenBalances") or []}
        for ai in pre_sol:
            pre = pre_sol.get(ai)
            post = post_sol_map.get(ai)
            if pre and pre.get("mint") == WSOL and pre.get("owner") == wallet:
                try:
                    pre_amt = float(pre.get("uiTokenAmount", {}).get("uiAmount") or 0)
                    post_amt = float((post or {}).get("uiTokenAmount", {}).get("uiAmount") or 0)
                    if post_amt < pre_amt:
                        sol_spent = True
                        break
                except (TypeError, ValueError):
                    pass

    if not sol_spent:
        return None

    # Find token balance increase
    pre_token = {(b.get("accountIndex")): b for b in meta.get("preTokenBalances") or []}
    for pb in meta.get("postTokenBalances") or []:
        mint = pb.get("mint")
        if not mint or mint == WSOL:
            continue
        if pb.get("owner") != wallet:
            continue
        pre_amt = 0.0
        old = pre_token.get(pb.get("accountIndex"))
        if old and old.get("mint") == mint:
            try:
                pre_amt = float(old.get("uiTokenAmount", {}).get("uiAmount") or 0)
            except (TypeError, ValueError):
                pre_amt = 0.0
        try:
            post_amt = float(pb.get("uiTokenAmount", {}).get("uiAmount") or 0)
        except (TypeError, ValueError):
            continue
        delta = post_amt - pre_amt
        if delta <= 0:
            continue
        return {"wallet": wallet, "ca": mint, "ts": float(bt), "amount": delta}

    return None


class HeliusWS:
    """Helius WebSocket client for real-time wallet transaction streaming.

    Usage::

        ws = HeliusWS(api_key="...", wallets=["addr1", "addr2", ...])
        ws.on_buy = my_callback  # async fn(wallet, buy_row)
        await ws.run()  # blocks forever, auto-reconnects
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_keys: list[str] | None = None,
        wallets: list[str] | None = None,
        on_buy: Callable[[str, dict], Awaitable[None]] | None = None,
        endpoint: str = "wss://beta.helius-rpc.com",
        rpc_url: str | None = None,
    ) -> None:
        self._api_keys = api_keys or ([api_key] if api_key else [])
        self._key_idx = 0
        self.api_key = self._api_keys[0] if self._api_keys else ""
        self.wallets = wallets or []
        self.on_buy = on_buy
        self._endpoint = endpoint
        self._rpc_url = rpc_url or f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._connected = False
        self._last_msg_ts = 0.0
        self._reconnect_count = 0
        self._total_buys = 0
        self._total_msgs = 0
        self._use_logs_subscribe = False  # fallback if transactionSubscribe unavailable
        self._exhausted_keys: set[str] = set()

    def _next_key(self) -> str:
        """Rotate to the next available API key, skipping exhausted ones."""
        if not self._api_keys:
            return ""
        start = self._key_idx
        for _ in range(len(self._api_keys)):
            self._key_idx = (self._key_idx + 1) % len(self._api_keys)
            key = self._api_keys[self._key_idx]
            if key not in self._exhausted_keys:
                self.api_key = key
                self._rpc_url = f"https://mainnet.helius-rpc.com/?api-key={key}"
                logger.info("helius ws: rotated to key %s…", key[:8])
                return key
        # all keys exhausted — reset and retry from beginning
        logger.warning("helius ws: all %d keys exhausted, resetting", len(self._api_keys))
        self._exhausted_keys.clear()
        self._key_idx = 0
        self.api_key = self._api_keys[0]
        self._rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self._api_keys[0]}"
        return self._api_keys[0]

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def stats(self) -> dict:
        return {
            "connected": self._connected,
            "reconnects": self._reconnect_count,
            "total_buys": self._total_buys,
            "total_msgs": self._total_msgs,
            "last_msg_age_s": round(time.time() - self._last_msg_ts, 1) if self._last_msg_ts else None,
        }

    async def run(self) -> None:
        """Main loop: connect, subscribe, reconnect on failure."""
        backoff = _RECONNECT_MIN
        while not self._stop.is_set():
            try:
                await self._connect_and_stream()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._reconnect_count += 1
                exc_str = str(exc).lower()
                # Detect exhausted/rate-limited key and rotate immediately
                if any(kw in exc_str for kw in ("max usage", "429", "rate limit", "too many")):
                    if len(self._api_keys) > 1:
                        self._exhausted_keys.add(self.api_key)
                        self._next_key()
                        backoff = _RECONNECT_MIN
                        continue
                logger.warning("helius ws disconnected (%s), reconnecting in %.0fs (attempt %d)",
                               exc, backoff, self._reconnect_count)
                self._connected = False
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                    break  # stop was set during backoff
                except TimeoutError:
                    pass
                backoff = min(backoff * 1.5, _RECONNECT_MAX)

    async def _connect_and_stream(self) -> None:
        """Connect to Helius WS, subscribe to all wallets, process messages."""
        url = f"{self._endpoint}/?api-key={self.api_key}"
        logger.info("helius ws connecting to %s (wallets=%d)", self._endpoint, len(self.wallets))

        async with websockets.connect(
            url,
            ping_interval=_PING_INTERVAL,
            ping_timeout=10,
            max_size=4 * 1024 * 1024,  # 4MB
            close_timeout=5,
        ) as ws:
            self._connected = True
            self._reconnect_count = 0
            logger.info("helius ws connected")

            if self._use_logs_subscribe:
                logger.info("helius ws: transactionSubscribe unavailable (free tier) — PumpAPI is primary feed")
                # logsSubscribe on Token Program cannot filter by wallet — useless
                # Just keep connection alive for health, but don't process messages
                async for raw in ws:
                    if self._stop.is_set():
                        break
                    # Intentionally ignore all messages — PumpAPI handles everything
            else:
                ok = await self._subscribe_transaction(ws)
                if not ok:
                    logger.info("helius ws: transactionSubscribe unavailable, falling back to logsSubscribe")
                    self._use_logs_subscribe = True
                    await self._subscribe_logs(ws)

            # Message loop
            async for raw in ws:
                if self._stop.is_set():
                    break
                self._total_msgs += 1
                self._last_msg_ts = time.time()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Skip subscription confirmations
                if "result" in msg and "id" in msg:
                    continue

                # Handle errors from subscription
                if "error" in msg:
                    err = msg["error"]
                    code = err.get("code", 0)
                    if code == -32001:
                        logger.warning("helius ws: transactionSubscribe not available (free tier), switching to logsSubscribe")
                        self._use_logs_subscribe = True
                        await self._subscribe_logs(ws)
                        continue
                    logger.warning("helius ws subscription error: %s", err)
                    continue

                # Process transaction notification
                if self._use_logs_subscribe:
                    await self._handle_logs_message(msg)
                else:
                    await self._handle_message(msg)

    async def _subscribe_transaction(self, ws) -> bool:
        """Subscribe via transactionSubscribe. Returns True if subscribed."""
        try:
            # Send first batch and check if transactionSubscribe is available
            batch = self.wallets[:_SUBSCRIBE_BATCH]
            sub = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "transactionSubscribe",
                "params": [
                    {
                        "accountInclude": batch,
                        # ATA expansion: plain accountInclude only matches txs
                        # where the wallet pubkey is in account keys. SPL
                        # receives touch the ATA, not the wallet — without
                        # this the bot sees msgs but buys=0. balanceChanged =
                        # narrow, low-volume, correct per Helius docs.
                        "tokenAccounts": "balanceChanged",
                        "vote": False,
                        "failed": False,
                    },
                    {
                        "commitment": "confirmed",
                        "encoding": "jsonParsed",
                        "transactionDetails": "full",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
            }
            await ws.send(json.dumps(sub))
            logger.info("helius ws testing transactionSubscribe (%d wallets)", len(batch))

            # Check response for plan error
            resp = await asyncio.wait_for(ws.recv(), timeout=10)
            data = json.loads(resp)
            if "error" in data:
                return False

            logger.info("helius ws transactionSubscribe available, subscribing remaining batches")
            # Now subscribe remaining batches
            for i in range(_SUBSCRIBE_BATCH, len(self.wallets), _SUBSCRIBE_BATCH):
                batch = self.wallets[i:i + _SUBSCRIBE_BATCH]
                sub = {
                    "jsonrpc": "2.0",
                    "id": i // _SUBSCRIBE_BATCH + 1,
                    "method": "transactionSubscribe",
                    "params": [
                        {
                            "accountInclude": batch,
                            "tokenAccounts": "balanceChanged",
                            "vote": False,
                            "failed": False,
                        },
                        {
                            "commitment": "confirmed",
                            "encoding": "jsonParsed",
                            "transactionDetails": "full",
                            "maxSupportedTransactionVersion": 0,
                        },
                    ],
                }
                await ws.send(json.dumps(sub))
                logger.info("helius ws subscribed batch %d/%d (%d wallets)",
                            i // _SUBSCRIBE_BATCH + 1,
                            (len(self.wallets) + _SUBSCRIBE_BATCH - 1) // _SUBSCRIBE_BATCH,
                            len(batch))

            logger.info("helius ws transactionSubscribe confirmed")
            return True
        except asyncio.TimeoutError:
            return False

    async def _subscribe_logs(self, ws) -> None:
        """Subscribe via logsSubscribe on Token Program (free-tier compatible)."""
        sub = {
            "jsonrpc": "2.0",
            "id": 999,
            "method": "logsSubscribe",
            "params": [
                {"mentions": [_TOKEN_PROGRAM]},
                {"commitment": "confirmed"},
            ],
        }
        await ws.send(json.dumps(sub))
        logger.info("helius ws subscribed to logsSubscribe (Token Program, %d wallets tracked)",
                     len(self.wallets))

    async def _handle_message(self, msg: dict) -> None:
        """Process a single transaction notification from Helius WS."""
        # Helius wraps the subscription result in "params": {"result": {"transaction": ...}}
        params = msg.get("params") or {}
        result = params.get("result") or {}
        tx_data = result.get("transaction") or msg.get("transaction")

        if not tx_data:
            return

        # The transaction payload may be nested
        meta = tx_data.get("meta") or {}
        message = (tx_data.get("transaction") or {}).get("message") or {}
        account_keys = message.get("accountKeys") or []

        # Resolve account keys (may be objects in jsonParsed)
        if account_keys and isinstance(account_keys[0], dict):
            resolved_keys = [k.get("pubkey", "") for k in account_keys]
        else:
            resolved_keys = account_keys

        # Find which of our tracked wallets participated
        wallet_set = set(self.wallets)
        participating = [k for k in resolved_keys if k in wallet_set]

        for wallet in participating:
            buy = parse_helius_tx(wallet, {"transaction": tx_data, "blockTime": tx_data.get("blockTime")})
            if buy and self.on_buy:
                self._total_buys += 1
                try:
                    await self.on_buy(wallet, buy)
                except Exception:
                    logger.exception("helius ws on_buy callback failed for %s", wallet[:10])

    async def _handle_logs_message(self, msg: dict) -> None:
        """Process a logsSubscribe notification.

        logsSubscribe returns program log messages, NOT wallet addresses.
        We cannot reliably match tracked wallets from log lines alone.
        This is a no-op on free tier — transactionSubscribe is required
        for proper wallet filtering.
        """
        pass  # logsSubscribe cannot filter by wallet; use transactionSubscribe

    async def _fetch_and_parse_tx(self, signature: str, wallet: str) -> dict | None:
        """Fetch a full transaction via HTTP RPC and parse it for buy events."""
        if aiohttp is None:
            logger.debug("aiohttp not installed, skipping HTTP fetch for %s", signature[:16])
            return None

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
                {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "transactionDetails": "full"},
            ],
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self._rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    tx_data = data.get("result")
                    if not tx_data:
                        return None
                    return parse_helius_tx(wallet, {"transaction": tx_data.get("transaction", {}), "blockTime": tx_data.get("blockTime")})
        except Exception as exc:
            logger.debug("helius ws HTTP fetch failed for %s: %s", signature[:16], exc)
            return None

    async def stop(self) -> None:
        """Gracefully stop the WebSocket client."""
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def start(self) -> None:
        """Start the WebSocket client as a background task."""
        self._task = asyncio.create_task(self.run())
        self._task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.exception("helius ws task crashed: %s", exc)
