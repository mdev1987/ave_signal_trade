"""CabalSpy WebSocket client — real-time KOL/SM/Whale data streams.

Provides real-time feeds for:
  - Signal stream: Server-side cluster detection (entry/exit when N wallets buy)
  - TX stream: Every buy/sell with running position, bag%, supply%
  - Holder stream: Live holder table with positions, unrealized PnL
  - Bundle stream: Coordinated KOL bundle detection

Architecture:
  - Single WS connection with multiple stream subscriptions
  - Auto-reconnects on disconnect with exponential backoff
  - Ping/pong health checks every 30s
  - Fail-open on any error (never blocks the bot)

Docs: https://docs.cabalspy.xyz/get-started
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Callable, Awaitable, Any

import websockets
import websockets.exceptions

logger = logging.getLogger(__name__)

# Reconnect backoff: start at 2s, max 60s
_RECONNECT_MIN = 2.0
_RECONNECT_MAX = 60.0
_MAX_RECONNECT_ATTEMPTS = 10   # pause after this many consecutive failures
_RECONNECT_PAUSE_S = 120.0    # 2 min cooldown (was 5 min — too long for transient outages)
_PING_INTERVAL = 30.0

# Stream types
STREAM_SIGNAL = "signal"
STREAM_TX = "tx"
STREAM_HOLDER = "holder"
STREAM_BUNDLE = "bundle"
STREAM_COUNT = "count"
STREAM_BALANCE = "balance"


class CabalSpyClient:
    """CabalSpy WebSocket client for real-time KOL/SM/Whale data.

    Usage::

        client = CabalSpyClient(
            api_key="...",
            on_signal=my_signal_callback,
            on_tx=my_tx_callback,
        )
        client.start()  # runs in background
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_keys: list[str] | None = None,
        on_signal: Callable[[dict], Awaitable[None]] | None = None,
        on_tx: Callable[[dict], Awaitable[None]] | None = None,
        on_holder: Callable[[dict], Awaitable[None]] | None = None,
        on_bundle: Callable[[dict], Awaitable[None]] | None = None,
        on_count: Callable[[dict], Awaitable[None]] | None = None,
        signal_min_buy: float = 0.5,
        signal_entry_at: list[int] | None = None,
        signal_exit_at: list[int] | None = None,
        signal_min_win_rate: float = 50.0,
        signal_token: str = "*",
        tx_types: list[str] | None = None,
        tx_token: str = "*",
        holder_token: str | None = None,
        holder_wallet_types: list[str] | None = None,
        holder_mode: str = "events",
        bundle_token: str | None = None,
        bundle_mode: str = "events",
        count_token: str = "*",
        balance_wallets: list[str] | None = None,
    ) -> None:
        self.api_key = api_key
        self.on_signal = on_signal
        self.on_tx = on_tx
        self.on_holder = on_holder
        self.on_bundle = on_bundle
        self.on_count = on_count

        # Signal stream config
        self.signal_min_buy = signal_min_buy
        self.signal_entry_at = signal_entry_at or [3, 5]
        self.signal_exit_at = signal_exit_at or [1]
        self.signal_min_win_rate = signal_min_win_rate
        self.signal_token = signal_token

        # TX stream config
        self.tx_types = tx_types or ["kol", "smart", "whale"]
        self.tx_token = tx_token

        # Holder stream config
        self.holder_token = holder_token
        self.holder_wallet_types = holder_wallet_types or ["kol", "smart", "whale"]
        self.holder_mode = holder_mode

        # Bundle stream config
        self.bundle_token = bundle_token
        self.bundle_mode = bundle_mode

        # Count stream config
        self.count_token = count_token

        # Balance stream config
        self.balance_wallets = balance_wallets or []

        # Internal state
        self._api_keys = api_keys or ([api_key] if api_key else [])
        self._key_idx = 0
        self.api_key = self._api_keys[0] if self._api_keys else ""
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._connected = False
        self._ws = None  # active WebSocket connection for live subscribe
        self._last_msg_ts = 0.0
        self._reconnect_count = 0
        self._total_signals = 0
        self._total_txs = 0
        self._total_holders = 0
        self._total_bundles = 0
        self._total_counts = 0
        self._exhausted_keys: set[str] = set()

    @property
    def connected(self) -> bool:
        return self._connected

    def _next_key(self) -> str:
        """Rotate to the next available API key, skipping exhausted ones."""
        if not self._api_keys:
            return ""
        for _ in range(len(self._api_keys)):
            self._key_idx = (self._key_idx + 1) % len(self._api_keys)
            key = self._api_keys[self._key_idx]
            if key not in self._exhausted_keys:
                self.api_key = key
                logger.info("cabalspy: rotated to key %s…", key[:8])
                return key
        # all keys exhausted — reset and retry
        logger.warning("cabalspy: all %d keys exhausted, resetting", len(self._api_keys))
        self._exhausted_keys.clear()
        self._key_idx = 0
        self.api_key = self._api_keys[0]
        return self._api_keys[0]

    @property
    def stats(self) -> dict:
        return {
            "connected": self._connected,
            "reconnects": self._reconnect_count,
            "total_signals": self._total_signals,
            "total_txs": self._total_txs,
            "total_holders": self._total_holders,
            "total_bundles": self._total_bundles,
            "total_counts": self._total_counts,
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
                # Rotate key on auth/timeout errors if we have multiple keys
                if any(kw in exc_str for kw in ("401", "403", "unauthorized", "forbidden", "invalid api key")):
                    if len(self._api_keys) > 1:
                        self._exhausted_keys.add(self.api_key)
                        self._next_key()
                        backoff = _RECONNECT_MIN
                        continue
                # Cap: after N consecutive failures, pause 5 min before retrying
                if self._reconnect_count >= _MAX_RECONNECT_ATTEMPTS:
                    logger.warning("cabalspy paused after %d failures — retrying in %.0fs",
                                   self._reconnect_count, _RECONNECT_PAUSE_S)
                    self._connected = False
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=_RECONNECT_PAUSE_S)
                        break
                    except TimeoutError:
                        pass
                    self._reconnect_count = 0
                    backoff = _RECONNECT_MIN
                    continue
                logger.warning("cabalspy ws disconnected (%s), reconnecting in %.0fs (attempt %d)",
                               exc, backoff, self._reconnect_count)
                self._connected = False
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                    break  # stop was set during backoff
                except TimeoutError:
                    pass
                backoff = min(backoff * 1.5, _RECONNECT_MAX)

    async def _connect_and_stream(self) -> None:
        """Connect to CabalSpy WS, subscribe to streams, process messages."""
        url = f"wss://stream.cabalspy.xyz?apiKey={self.api_key}"
        logger.info("cabalspy ws connecting")

        async with websockets.connect(
            url,
            ping_interval=_PING_INTERVAL,
            ping_timeout=10,
            max_size=4 * 1024 * 1024,  # 4MB
            close_timeout=5,
        ) as ws:
            self._connected = True
            self._ws = ws
            self._reconnect_count = 0
            logger.info("cabalspy ws connected")

            # Subscribe to all configured streams
            await self._subscribe_streams(ws)

            # Message loop
            async for raw in ws:
                if self._stop.is_set():
                    break
                self._last_msg_ts = time.time()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Skip pong responses
                if msg.get("op") == "pong":
                    continue

                # Skip welcome/subscribe confirmations
                if msg.get("event") in ("welcome", "subscribed"):
                    continue

                # Route to appropriate handler
                await self._handle_message(msg)

        self._ws = None
        self._connected = False

    async def _subscribe_streams(self, ws) -> None:
        """Subscribe to all configured streams."""
        # Signal stream
        if self.on_signal:
            sub = {
                "op": "subscribe",
                "stream": STREAM_SIGNAL,
                "blockchain": "solana",
                "token": self.signal_token,
                "kol": {
                    "min_buy": self.signal_min_buy,
                    "entry_at": self.signal_entry_at,
                    "exit_at": self.signal_exit_at,
                },
                "min_win_rate": self.signal_min_win_rate,
            }
            await ws.send(json.dumps(sub))
            logger.info("cabalspy subscribed to signal stream (entry_at=%s, min_buy=%.1f)",
                        self.signal_entry_at, self.signal_min_buy)

        # TX stream
        if self.on_tx:
            for tx_type in self.tx_types:
                sub = {
                    "op": "subscribe",
                    "stream": STREAM_TX,
                    "blockchain": "solana",
                    "type": tx_type,
                    "token": self.tx_token,
                }
                await ws.send(json.dumps(sub))
                logger.info("cabalspy subscribed to tx stream (type=%s)", tx_type)

        # Holder stream (single token)
        if self.on_holder and self.holder_token:
            sub = {
                "op": "subscribe",
                "stream": STREAM_HOLDER,
                "blockchain": "solana",
                "token": self.holder_token,
                "wallet_types": self.holder_wallet_types,
                "mode": self.holder_mode,
            }
            await ws.send(json.dumps(sub))
            logger.info("cabalspy subscribed to holder stream (token=%s)", self.holder_token[:12])

        # Bundle stream (single token)
        if self.on_bundle and self.bundle_token:
            sub = {
                "op": "subscribe",
                "stream": STREAM_BUNDLE,
                "blockchain": "solana",
                "token": self.bundle_token,
                "mode": self.bundle_mode,
            }
            await ws.send(json.dumps(sub))
            logger.info("cabalspy subscribed to bundle stream (token=%s)", self.bundle_token[:12])

        # Count stream (all tokens)
        if self.on_count:
            sub = {
                "op": "subscribe",
                "stream": STREAM_COUNT,
                "blockchain": "solana",
                "token": self.count_token,
            }
            await ws.send(json.dumps(sub))
            logger.info("cabalspy subscribed to count stream")

    async def _handle_message(self, msg: dict) -> None:
        """Route message to appropriate handler."""
        event = msg.get("event")
        channel = msg.get("channel", "")

        if event == "signal":
            self._total_signals += 1
            if self.on_signal:
                try:
                    await self.on_signal(msg)
                except Exception:
                    logger.exception("cabalspy on_signal callback failed")

        elif event == "position_update" and "tx" in channel:
            self._total_txs += 1
            if self.on_tx:
                try:
                    await self.on_tx(msg)
                except Exception:
                    logger.exception("cabalspy on_tx callback failed")

        elif event in ("init", "holder_update", "position_update") and "holder" in channel:
            self._total_holders += 1
            if self.on_holder:
                try:
                    await self.on_holder(msg)
                except Exception:
                    logger.exception("cabalspy on_holder callback failed")

        elif event in ("init", "kol_bundle") and "bundle" in channel:
            self._total_bundles += 1
            if self.on_bundle:
                try:
                    await self.on_bundle(msg)
                except Exception:
                    logger.exception("cabalspy on_bundle callback failed")

        elif event == "wallet_count":
            self._total_counts += 1
            if self.on_count:
                try:
                    await self.on_count(msg)
                except Exception:
                    logger.exception("cabalspy on_count callback failed")

    def subscribe_token(self, token: str, streams: list[str] | None = None) -> dict:
        """Generate a subscribe message for a specific token.

        Returns the subscribe dict that can be sent via WebSocket.
        Useful for dynamically subscribing to tokens after connection.
        """
        streams = streams or [STREAM_HOLDER, STREAM_BUNDLE]
        subs = []
        for stream in streams:
            sub = {
                "op": "subscribe",
                "stream": stream,
                "blockchain": "solana",
                "token": token,
            }
            if stream == STREAM_HOLDER:
                sub["wallet_types"] = self.holder_wallet_types
                sub["mode"] = self.holder_mode
            elif stream == STREAM_BUNDLE:
                sub["mode"] = self.bundle_mode
            subs.append(sub)
        return subs

    async def subscribe_token_live(self, token: str, streams: list[str] | None = None) -> None:
        """Dynamically subscribe to holder/bundle streams for a specific token.

        Called after a signal fires or position opens to track holder exits
        and bundle activity for that token. Uses the active WS connection.
        """
        if not self._ws or not self._connected:
            logger.warning("cabalspy subscribe_token_live skipped — not connected")
            return
        subs = self.subscribe_token(token, streams)
        for sub in subs:
            try:
                await self._ws.send(json.dumps(sub))
                logger.info("cabalspy subscribed to %s for %s", sub["stream"], token[:12])
            except Exception:
                logger.exception("cabalspy subscribe %s failed", sub["stream"])

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
            logger.exception("cabalspy ws task crashed: %s", exc)


class HolderCache:
    """In-memory cache for token holder data from CabalSpy holder stream.

    Tracks holder positions, bag percentages, and supply percentages.
    Used for pre-entry safety checks (concentration, KOL exits).
    """

    def __init__(self) -> None:
        self._holders: dict[str, dict[str, dict]] = {}  # mint -> {wallet -> holder_data}
        self._last_update: dict[str, float] = {}  # mint -> timestamp

    def update_from_signal(self, msg: dict) -> None:
        """Update cache from a signal event (includes wallet positions)."""
        data = msg.get("data", {})
        mint = data.get("mint")
        if not mint:
            return

        wallets = data.get("wallets", [])
        if mint not in self._holders:
            self._holders[mint] = {}

        for w in wallets:
            wallet = w.get("wallet")
            if wallet:
                self._holders[mint][wallet] = {
                    "wallet": wallet,
                    "win_rate": w.get("win_rate"),
                    "bag_pct": w.get("bag_pct"),
                    "invested": w.get("invested"),
                    "invested_usd": w.get("invested_usd"),
                    "entry_market_cap": w.get("entry_market_cap"),
                    "unrealized_pnl_sol": w.get("unrealized_pnl_sol"),
                    "unrealized_pnl_pct": w.get("unrealized_pnl_pct"),
                    "sold": w.get("sold", False),
                }

        self._last_update[mint] = time.time()

    def update_from_holder(self, msg: dict) -> None:
        """Update cache from a holder stream event."""
        data = msg.get("data", {})
        mint = data.get("mint")
        if not mint:
            return

        event = msg.get("event")

        if event == "init":
            # Full snapshot
            holders = data.get("holders", [])
            self._holders[mint] = {}
            for h in holders:
                wallet = h.get("wallet")
                if wallet:
                    self._holders[mint][wallet] = {
                        "wallet": wallet,
                        "wallet_type": h.get("wallet_type"),
                        "win_rate": h.get("win_rate"),
                        "position": h.get("position", {}),
                    }
            self._last_update[mint] = time.time()

        elif event == "holder_update":
            # Single holder update
            holder = data.get("holder", {})
            wallet = holder.get("wallet")
            if wallet:
                if mint not in self._holders:
                    self._holders[mint] = {}
                self._holders[mint][wallet] = {
                    "wallet": wallet,
                    "wallet_type": holder.get("wallet_type"),
                    "win_rate": holder.get("win_rate"),
                    "position": holder.get("position", {}),
                }
            self._last_update[mint] = time.time()

    def check_concentration(self, mint: str, max_pct: float = 30.0) -> tuple[bool, float, str]:
        """Check if any single holder owns too much of the supply.

        Returns (safe, max_holder_pct, reason).
        """
        holders = self._holders.get(mint, {})
        if not holders:
            return True, 0.0, "no_data"

        max_pct_found = 0.0
        max_holder = ""

        for wallet, data in holders.items():
            # Try different data shapes
            pos = data.get("position", {})
            supply_pct = pos.get("supply_pct") or data.get("supply_pct") or 0

            if supply_pct > max_pct_found:
                max_pct_found = supply_pct
                max_holder = wallet[:8]

        if max_pct_found > max_pct:
            return False, max_pct_found, f"holder_{max_holder}={max_pct_found:.1f}%>{max_pct}%"

        return True, max_pct_found, "ok"

    def get_holders(self, mint: str) -> dict[str, dict]:
        """Get all cached holders for a token."""
        return self._holders.get(mint, {})

    def get_kol_count(self, mint: str) -> int:
        """Get number of KOL holders for a token."""
        holders = self._holders.get(mint, {})
        return sum(1 for h in holders.values()
                   if h.get("wallet_type") == "kol" or h.get("win_rate", 0) > 50)

    def get_total_invested(self, mint: str) -> float:
        """Get total SOL invested by all tracked holders."""
        holders = self._holders.get(mint, {})
        total = 0.0
        for h in holders.values():
            pos = h.get("position", {})
            invested = pos.get("invested") or h.get("invested") or 0
            total += invested
        return total

    def is_stale(self, mint: str, max_age_s: float = 300.0) -> bool:
        """Check if holder data is stale (older than max_age_s)."""
        last = self._last_update.get(mint, 0)
        return (time.time() - last) > max_age_s

    def cleanup(self, max_mints: int = 1000) -> None:
        """Remove old entries if cache grows too large."""
        if len(self._holders) > max_mints:
            # Remove oldest entries
            sorted_mints = sorted(self._last_update.keys(),
                                  key=lambda m: self._last_update.get(m, 0))
            for mint in sorted_mints[:len(sorted_mints) - max_mints]:
                self._holders.pop(mint, None)
                self._last_update.pop(mint, None)
