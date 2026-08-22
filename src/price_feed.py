"""Live price + pool-state feed over the pumpapi.io WebSocket.

The stream delivers every event for all pools (buys, sells, creates, liquidity
add/remove, migrations, ...). The feed keeps the latest price per mint AND a
bounded per-mint rug-feature state parsed from fields the stream carries on
every event (``burnedLiquidity``, ``quoteInPool``, ``poolCreatedBy``, mint/
freeze authority) — zero-latency pool intelligence that needs no external API.
Callers use :meth:`pool_state` at the entry moment to veto freshly-drained or
unburned-LP pools.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable

import websockets

logger = logging.getLogger(__name__)

EventCallback = Callable[[dict], Awaitable[None]]

# Event actions that signal liquidity leaving a pool (pumpapi stream).
_LIQ_REMOVE_ACTIONS = {"remove", "removeliquidity", "remove_liquidity"}


def _pct_of(raw) -> float | None:
    """Parse ``burnedLiquidity``-style values ("100%", 100, 0.85) to percent."""
    if raw is None:
        return None
    try:
        s = str(raw).strip().rstrip("%")
        v = float(s)
    except (TypeError, ValueError):
        return None
    return v


class PriceFeed:
    """A single WebSocket connection to pumpapi.io with auto-reconnect.

    Args:
        uri: WebSocket endpoint (default from config env ``PUMPAPI_WSS``).
        on_event: Optional async callback invoked for every raw event.
        max_prices: Upper bound on the per-mint price cache (LRU eviction).
            Keeps memory flat on the long-running live feed, where unique
            mints accumulate without limit.

    Attributes:
        prices: Latest per-mint price keyed by contract address (LRU-bounded).
    """

    def __init__(
        self,
        uri: str = "wss://stream.pumpapi.io/",
        on_event: EventCallback | None = None,
        reconnect_s: float = 3.0,
        price_timeout_s: float = 30.0,
        recv_timeout_s: float = 90.0,
        max_prices: int = 20_000,
    ) -> None:
        self.uri = uri
        self.on_event = on_event
        self.reconnect_s = reconnect_s
        self.price_timeout_s = price_timeout_s
        self.recv_timeout_s = recv_timeout_s
        self.max_prices = max_prices
        self.prices: OrderedDict[str, float] = OrderedDict()
        # Per-mint rug-feature state from stream fields (LRU-bounded):
        #   quote_in_pool      actual SOL reserves at last event
        #   burned_pct         LP burn percent ("100%" -> 100.0); None unknown
        #   pool_created_by    e.g. "pump" (trusted migration) when present
        #   mint_authority_set / freeze_authority_set
        #   liq_removed_ts     last liquidity-REMOVAL event time for this mint
        self._pool_state: OrderedDict[str, dict] = OrderedDict()
        self._new_trades: dict[str, asyncio.Event] = {}
        self._stop = asyncio.Event()

    @staticmethod
    def _price_of(event: dict) -> float | None:
        """Extract the price from an event if it carries one."""
        price = event.get("price")
        if price is None:
            return None
        try:
            return float(price)
        except (TypeError, ValueError):
            return None

    def _mint_of(self, event: dict) -> str | None:
        """Return the token mint for any event that carries one."""
        mint = event.get("mint") or (event.get("pool") or {}).get("mint")
        return mint if isinstance(mint, str) and mint else None

    def _update_pool_state(self, event: dict, mint: str) -> None:
        """Fold one raw event's pool/rug fields into per-mint state.

        Every field is best-effort: the stream attaches them to most events
        (buys/sells included), so state converges within the first tick after
        connect — no extra subscription, no API call, no added latency.
        """
        st = self._pool_state.get(mint)
        if st is None:
            if len(self._pool_state) >= self.max_prices:
                self._pool_state.popitem(last=False)
            st = {}
        action = str(event.get("action") or "").lower()
        qi = event.get("quoteInPool")
        if qi is not None:
            try:
                st["quote_in_pool"] = float(qi)
            except (TypeError, ValueError):
                pass
        burned = _pct_of(event.get("burnedLiquidity"))
        if burned is not None:
            st["burned_pct"] = burned
        pcr = event.get("poolCreatedBy")
        if isinstance(pcr, str) and pcr:
            st["pool_created_by"] = pcr
        for key, field in (("mint_authority_set", "mintAuthority"),
                           ("freeze_authority_set", "freezeAuthority")):
            if field in event:
                # Explicit null = authority revoked (good); only a missing
                # key stays unknown.
                st[key] = bool(event[field])
        if action in _LIQ_REMOVE_ACTIONS:
            st["liq_removed_ts"] = time.time()
        st["ts"] = time.time()
        self._pool_state[mint] = st
        self._pool_state.move_to_end(mint)

    def pool_state(self, mint: str) -> dict | None:
        """Latest known pool/rug-feature state for ``mint`` (None if unseen)."""
        st = self._pool_state.get(mint)
        return dict(st) if st else None

    async def _handle(self, event: dict) -> None:
        """Update price state for a single event and wake waiters."""
        if self.on_event is not None:
            try:
                await self.on_event(event)
            except Exception:
                logger.exception("on_event callback failed")
        mint = self._mint_of(event)
        if mint:
            # Pool/rug features ride on most events — fold them in first so
            # state exists even for mints whose price is unusable.
            try:
                self._update_pool_state(event, mint)
            except Exception:
                logger.exception("pool-state update failed")
        price = self._price_of(event)
        if mint and price and price > 0:
            self.prices[mint] = price
            self.prices.move_to_end(mint)
            if self.max_prices > 0:
                while len(self.prices) > self.max_prices:
                    self.prices.popitem(last=False)
            ev = self._new_trades.get(mint)
            if ev is not None:
                ev.set()

    async def run(self) -> None:
        """Consume the feed forever, reconnecting on drops.

        Uses ``recv`` inside a loop that checks the stop flag so :meth:`stop`
        takes effect promptly even while waiting for the next message. The
        socket is closed explicitly rather than via ``async with`` so a
        pending close handshake cannot block shutdown.
        """
        ws = None
        while not self._stop.is_set():
            if ws is None:
                try:
                    ws = await websockets.connect(self.uri)
                    logger.info("connected to %s", self.uri)
                except (websockets.ConnectionClosed, OSError) as e:
                    if self._stop.is_set():
                        break
                    logger.warning("connect failed (%s); retrying in %.0fs", e, self.reconnect_s)
                    await asyncio.sleep(self.reconnect_s)
                    continue
            recv_task = asyncio.create_task(ws.recv())
            stop_task = asyncio.create_task(self._stop.wait())
            timeout_task = asyncio.create_task(asyncio.sleep(self.recv_timeout_s))
            await asyncio.wait(
                {recv_task, stop_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._stop.is_set():
                recv_task.cancel()
                timeout_task.cancel()
                break
            if recv_task.done():
                timeout_task.cancel()
                try:
                    raw = recv_task.result()
                except (websockets.ConnectionClosed, OSError) as e:
                    logger.warning("feed dropped (%s); reconnecting in %.0fs", e, self.reconnect_s)
                    await ws.close()
                    ws = None
                    if not self._stop.is_set():
                        await asyncio.sleep(self.reconnect_s)
                    continue
            else:
                # recv got no data for recv_timeout_s — the socket is likely
                # wedged (half-open). Cancel the pending read and reconnect so
                # the loop can never stall on a dead connection.
                timeout_task.cancel()
                recv_task.cancel()
                logger.warning("feed recv silent >%.0fs; reconnecting in %.0fs",
                               self.recv_timeout_s, self.reconnect_s)
                await ws.close()
                ws = None
                if not self._stop.is_set():
                    await asyncio.sleep(self.reconnect_s)
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await self._handle(event)
        if ws is not None:
            await ws.close()

    def stop(self) -> None:
        """Request the run loop to exit after the current message."""
        self._stop.set()

    async def wait_price(self, mint: str, timeout: float | None = None) -> float | None:
        """Block until a fresh price for ``mint`` arrives.

        Args:
            mint: Token contract address.
            timeout: Max seconds to wait for a new tick (defaults to the
                configured ``PRICE_WAIT_TIMEOUT_S``).

        Returns:
            The latest price, or the current cached price immediately if one is
            already known; None only on timeout with no cached value.
        """
        if timeout is None:
            timeout = self.price_timeout_s
        if mint in self.prices:
            return self.prices[mint]
        ev = self._new_trades.setdefault(mint, asyncio.Event())
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
            return self.prices.get(mint)
        except TimeoutError:
            return self.prices.get(mint)
        finally:
            self._new_trades.pop(mint, None)