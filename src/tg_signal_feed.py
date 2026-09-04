"""Telegram signal feed — listens to @gmgnsignals for token alerts.

Parses GMGN's structured messages (CA, MC, liquidity, holders, price changes)
and feeds qualifying tokens into the entry pipeline.  The channel IS the
consensus — GMGN has already aggregated smart money data — so signals bypass
the wallet-consensus gate but still pass all safety/liquidity/price gates.

Lifecycle::

    feed = TgSignalFeed(on_signal=callback, ...)
    task = asyncio.create_task(feed.run())
    # ...
    stats = feed.health()
    feed.stop()
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Awaitable, Callable

log = logging.getLogger(__name__)

_CA_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

# GMGN message patterns
_PRICE_RE = re.compile(r"[\$]?([\d.]+)")
_MCPCT_RE = re.compile(r"MCP[:\s]+\$?([\d.]+[KMB]?)")
_LIQ_RE = re.compile(r"Liq[:\s]+(?:\d+\.?\d*\s*SOL\s*)?\(\$?([\d.]+[KMB]?)")
_HOLDER_RE = re.compile(r"Holder[s]?[:\s]+([\d,]+)")
_STATUS_RE = re.compile(r"Status进度[:\s]+([\d.]+)%")
_DEV_HOL_RE = re.compile(r"DEV Holding[:\s]+([\d.]+)%\s*->\s*([\d.]+)%")


def _parse_value(val: str) -> float:
    """Parse '1.5K', '2.3M', '100', '50%' → float."""
    val = val.strip().rstrip("%").rstrip("x")
    if not val:
        return 0.0
    mul = 1.0
    if val.endswith("K"):
        mul, val = 1_000, val[:-1]
    elif val.endswith("M"):
        mul, val = 1_000_000, val[:-1]
    elif val.endswith("B"):
        mul, val = 1_000_000_000, val[:-1]
    try:
        return float(val) * mul
    except ValueError:
        return 0.0


def parse_tg_signal(text: str) -> dict | None:
    """Parse a @gmgnsignals message into a token signal dict.

    Returns None if no valid CA found or message is not a token alert.
    """
    if not text:
        return None

    # Extract CA (32-44 base58 chars)
    ca_match = _CA_RE.search(text)
    if not ca_match:
        return None
    ca = ca_match.group(0)

    # Extract token name: look for $TOKEN_NAME(NAME) or $TOKEN pattern
    # Must appear BEFORE the CA (contract address is 32-44 base58 chars)
    name = ""
    sym = ""
    ca_pos = ca_match.start()
    # Try $Symbol(Name) pattern before the CA
    name_match = re.search(r"\$([A-Za-z0-9_]+)\s*\(([^)]+)\)", text[:ca_pos])
    if name_match:
        sym = name_match.group(1)
        name = name_match.group(2)
    else:
        # Try $SYMBOL pattern before the CA
        name_match = re.search(r"\$([A-Za-z0-9_]{2,})", text[:ca_pos])
        if name_match:
            sym = name_match.group(1)

    # Strip Telegram markdown formatting
    clean = text.replace("**", "").replace("*", "").replace("`", "")

    # Extract metrics
    mc = 0.0
    mc_match = _MCPCT_RE.search(clean)
    if mc_match:
        mc = _parse_value(mc_match.group(1))

    liq = 0.0
    liq_match = _LIQ_RE.search(clean)
    if liq_match:
        liq = _parse_value(liq_match.group(1))

    holders = 0
    h_match = _HOLDER_RE.search(clean)
    if h_match:
        holders = int(h_match.group(1).replace(",", ""))

    status_pct = 0.0
    s_match = _STATUS_RE.search(text)
    if s_match:
        status_pct = float(s_match.group(1))

    dev_hold_from = 0.0
    dev_hold_to = 0.0
    d_match = _DEV_HOL_RE.search(text)
    if d_match:
        dev_hold_from = float(d_match.group(1))
        dev_hold_to = float(d_match.group(2))

    # Determine signal type
    signal_type = "unknown"
    text_lower = text.lower()
    if "dev sold" in text_lower or "dev selling" in text_lower:
        signal_type = "dev_sold"
    elif "new pool" in text_lower or "new listing" in text_lower:
        signal_type = "new_pool"
    elif "pump" in text_lower or "surge" in text_lower:
        signal_type = "pump"
    elif "king" in text_lower or "koth" in text_lower:
        signal_type = "koth"
    elif "burn" in text_lower:
        signal_type = "burn"

    # Parse price changes from the 📈 line
    pc_match = re.search(
        r"📈\s*(?:5m\s*\|\s*)?1h\s*\|\s*6h:\s*([-\d.]+)%\s*\|\s*([-\d.]+)%(?:\s*\|\s*([-\d.]+)%)?",
        text,
    )
    pc_1h = float(pc_match.group(2)) if pc_match else 0.0
    pc_6h = float(pc_match.group(3) or pc_match.group(2) or 0) if pc_match else 0.0

    return {
        "ca": ca,
        "symbol": sym,
        "name": name,
        "mc": mc,
        "liq": liq,
        "holders": holders,
        "status_pct": status_pct,
        "dev_hold_from": dev_hold_from,
        "dev_hold_to": dev_hold_to,
        "signal_type": signal_type,
        "pc_1h": pc_1h,
        "pc_6h": pc_6h,
        "raw_text": text[:500],
    }


class TgSignalFeed:
    """Real-time listener for @gmgnsignals Telegram channel.

    Lifecycle::

        feed = TgSignalFeed(on_signal=callback, ...)
        task = asyncio.create_task(feed.run())
        # ...
        feed.stop()
    """

    def __init__(
        self,
        on_signal: Callable[[str, str, str, float, list[str]], Awaitable[None]],
        channel: str = "gmgnsignals",
        api_id: int = 0,
        api_hash: str = "",
        phone: str = "",
        session_name: str = "tg_signal",
        min_mc: float = 5_000.0,
        min_liq: float = 1_000.0,
        min_holders: int = 10,
        dedup_ttl_s: float = 3600.0,
    ) -> None:
        self._on_signal = on_signal
        self._channel = channel
        self._api_id = api_id
        self._api_hash = api_hash
        self._phone = phone
        self._session_name = session_name
        self._min_mc = min_mc
        self._min_liq = min_liq
        self._min_holders = min_holders
        self._dedup_ttl_s = dedup_ttl_s
        self._stop = asyncio.Event()
        self._seen: dict[str, float] = {}  # ca -> first_seen_ts

        # Health
        self._started_at: float = 0.0
        self._messages_received: int = 0
        self._signals_parsed: int = 0
        self._signals_forwarded: int = 0
        self._signals_filtered: int = 0
        self._errors: int = 0
        self._last_event_at: float = 0.0

    async def run(self) -> None:
        """Main listener loop. Runs until ``stop()``."""
        self._started_at = time.time()
        log.info(
            "tg signal feed: started (channel=%s, min_mc=%.0f, min_liq=%.0f)",
            self._channel,
            self._min_mc,
            self._min_liq,
        )

        while not self._stop.is_set():
            try:
                await self._listen()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self._errors += 1
                log.exception("tg signal feed: listener crashed — retrying in 30s")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=30.0)
                except asyncio.TimeoutError:
                    pass

    async def _listen(self) -> None:
        """Poll for new messages via iter_messages.

        Telethon v1.44.0 event handlers don't fire reliably in a threaded/
        background-task context.  iter_messages with ``min_id`` gives us
        sub-5-second latency — essentially real-time for a channel that
        posts every few seconds.
        """
        from telethon import TelegramClient

        client = TelegramClient(
            self._session_name,
            self._api_id,
            self._api_hash,
        )
        await client.start(phone=self._phone)
        log.info("tg signal feed: connected, polling @%s", self._channel)

        entity = await client.get_entity(self._channel)

        # Start from the latest message so we don't re-process history
        last_msg_id = 0
        async for msg in client.iter_messages(entity, limit=1):
            last_msg_id = msg.id
        log.info("tg signal feed: starting from msg_id=%d", last_msg_id)

        poll_interval = 3.0

        while not self._stop.is_set():
            try:
                new_count = 0
                async for msg in client.iter_messages(entity, min_id=last_msg_id):
                    new_count += 1
                    text = msg.text or ""
                    last_msg_id = max(last_msg_id, msg.id)
                    try:
                        await self._handle_message(text)
                    except Exception:  # noqa: BLE001
                        self._errors += 1
                        log.exception("tg signal feed: handle failed")
                if new_count > 0:
                    log.debug("tg signal feed: processed %d new messages", new_count)
            except Exception:  # noqa: BLE001
                self._errors += 1
                log.exception("tg signal feed: poll failed — retrying in 10s")
                await asyncio.sleep(10)
                continue

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=poll_interval)
            except asyncio.TimeoutError:
                pass

        await client.disconnect()

    async def _handle_message(self, text: str) -> None:
        """Parse a message and forward qualifying signals."""
        self._messages_received += 1
        signal = parse_tg_signal(text)
        if not signal:
            return

        self._signals_parsed += 1
        ca = signal["ca"]
        now = time.time()

        # Dedup
        if ca in self._seen:
            return
        self._seen[ca] = now
        self._prune_seen(now)

        # Quality gates — reject unknown (0) or below minimum
        if signal["mc"] <= 0 or signal["mc"] < self._min_mc:
            self._signals_filtered += 1
            log.debug(
                "tg signal: filtered %s (mc=%.0f < %.0f)",
                ca[:8],
                signal["mc"],
                self._min_mc,
            )
            return

        if signal["liq"] <= 0 or signal["liq"] < self._min_liq:
            self._signals_filtered += 1
            log.debug(
                "tg signal: filtered %s (liq=%.0f < %.0f)",
                ca[:8],
                signal["liq"],
                self._min_liq,
            )
            return

        if signal["holders"] <= 0 or signal["holders"] < self._min_holders:
            self._signals_filtered += 1
            log.debug(
                "tg signal: filtered %s (holders=%d < %d)",
                ca[:8],
                signal["holders"],
                self._min_holders,
            )
            return

        self._last_event_at = now
        self._signals_forwarded += 1

        log.info(
            "tg signal: %s %s mc=$%.0f liq=$%.0f holders=%d 1h=%+.1f%%",
            signal["symbol"] or "?",
            ca[:8],
            signal["mc"],
            signal["liq"],
            signal["holders"],
            signal["pc_1h"],
        )

        try:
            # Use MC as the USD value proxy; fallback to 0
            usd = signal["mc"] if signal["mc"] > 0 else 0.0
            await self._on_signal(
                ca,
                signal["symbol"],
                usd,
                3.0,  # high score — channel IS the consensus
                ["tg_signal"],
            )
        except Exception:
            log.exception("tg signal: on_signal failed for %s", ca[:8])

    def _prune_seen(self, now: float) -> None:
        """Drop dedup entries older than TTL."""
        if len(self._seen) > 10_000:
            cutoff = now - self._dedup_ttl_s
            stale = [ca for ca, ts in self._seen.items() if ts < cutoff]
            for ca in stale:
                del self._seen[ca]

    def health(self) -> dict:
        return {
            "connected": self._started_at > 0 and not self._stop.is_set(),
            "uptime_s": (time.time() - self._started_at) if self._started_at else 0,
            "messages": self._messages_received,
            "parsed": self._signals_parsed,
            "forwarded": self._signals_forwarded,
            "filtered": self._signals_filtered,
            "errors": self._errors,
            "last_event_at": self._last_event_at,
        }

    def stop(self) -> None:
        self._stop.set()
