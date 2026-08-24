"""Fetch Telegram channel messages via raw Telethon.

Telethon authenticates a user session (prompting for a login code only when no
valid ``telegram_session.session`` exists) and streams channel events. Two
consumption modes:

- **Realtime (default)**: register an ``on_new_message`` handler so signals
  fire the moment the channel posts them (Telethon event system).
- **Bulk**: one-shot ``fetch_signals`` pull for backfill/scan.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from telethon import TelegramClient, events

from config import TelegramCreds
from models import Signal
from parser import parse_signal

logger = logging.getLogger(__name__)

DEFAULT_CHANNELS: tuple[str, ...] = ("@DRBTSolanaPF", "@SOLTRENDING")
DEFAULT_FETCH_LIMIT = 500

SignalHandler = Callable[[Signal], Awaitable[None]]


def _normalize_channels(channels: str | Sequence[str]) -> tuple[str, ...]:
    """Normalize a channel spec (single name or comma-separated list)."""
    if isinstance(channels, str):
        channels = channels.split(",")
    values = tuple(c.strip() for c in channels if c and c.strip())
    return values or DEFAULT_CHANNELS


class TelegramFeed:
    """Thin wrapper around a raw :class:`telethon.TelegramClient`.

    Args:
        creds: Resolved Telegram credentials + session file (see
            config.resolve_telegram_creds).
        channels: Channel usernames (with ``@``) or numeric ids to monitor;
            a single string may carry comma-separated names. Order is the
            preference order (earlier wins CA ties during backfill).
    """

    def __init__(
        self,
        creds: TelegramCreds,
        channels: str | Sequence[str] = DEFAULT_CHANNELS,
    ) -> None:
        self.creds = creds
        self.channels = _normalize_channels(channels)
        self._client = TelegramClient(
            creds.session_file, creds.api_id, creds.api_hash
        )
        self._connected = False
        self._handlers: list[SignalHandler] = []

    async def _ensure_connected(self) -> None:
        """Connect + authorize once (never prompts when the session is valid)."""
        if self._connected:
            return
        await self._client.connect()
        if not await self._client.is_user_authorized():
            await self._client.start(phone=self.creds.phone)
        self._connected = True
        logger.info("telegram connected (session %s)", self.creds.session_file)

    async def close(self) -> None:
        """Release the persistent Telegram connection."""
        if self._connected:
            await self._client.disconnect()
            self._connected = False

    async def list_channels(self) -> list[dict[str, Any]]:
        """List the channels/groups the session can see.

        Returns:
            Rows shaped like the old groups records.
        """
        await self._ensure_connected()
        rows: list[dict[str, Any]] = []
        async for dialog in self._client.iter_dialogs():
            if not (dialog.is_group or dialog.is_channel):
                continue
            entity = dialog.entity
            username = getattr(entity, "username", None)
            rows.append(
                {
                    "GroupID": entity.id,
                    "Title": getattr(entity, "title", ""),
                    "Username": f"@{username}" if username else None,
                    "Identifier": f"@{username}" if username else str(entity.id),
                    "IsChannel": dialog.is_channel,
                    "IsMegagroup": getattr(entity, "megagroup", False),
                    "ParticipantsCount": getattr(entity, "participants_count", None),
                }
            )
        return rows

    def _msg_to_signal(self, msg: Any) -> Signal | None:
        """Convert one Telethon message into a Signal."""
        text = msg.text
        if not text:
            return None
        date = msg.date or datetime.now(UTC)
        return parse_signal(
            text,
            unixtime=int(date.timestamp()),
            date=date.isoformat(),
            message_id=getattr(msg, "id", 0) or 0,
        )

    async def fetch_signals(
        self,
        limit: int = DEFAULT_FETCH_LIMIT,
        minutes: int | None = None,
    ) -> list[Signal]:
        """Fetch recent messages from every monitored channel and parse them.

        Args:
            limit: Max messages to pull per channel.
            minutes: If given, only messages from the last N minutes.

        Returns:
            Parsed signals in chronological order (stable for equal
            timestamps, so the preferred channel's post wins a tie).
        """
        await self._ensure_connected()
        cutoff = None
        if minutes is not None:
            cutoff = datetime.now(UTC) - timedelta(minutes=minutes)
        signals: list[Signal] = []
        for channel in self.channels:
            async for msg in self._client.iter_messages(channel, limit=limit):
                if cutoff is not None and (msg.date or datetime.now(UTC)) < cutoff:
                    break
                sig = self._msg_to_signal(msg)
                if sig is not None:
                    signals.append(sig)
        signals.sort(key=lambda s: s.unixtime)
        return signals

    def on_signal(self, handler: SignalHandler) -> None:
        """Register an async handler called for every new channel message.

        Args:
            handler: Async callable receiving the parsed :class:`Signal`.
        """
        self._handlers.append(handler)

    async def run_realtime(self) -> None:
        """Stream new messages via Telethon's event loop (blocks until stopped).

        Registers a ``NewMessage`` handler for every monitored channel and
        dispatches each parsed signal to the registered handlers.
        """
        await self._ensure_connected()

        @self._client.on(events.NewMessage(chats=list(self.channels)))
        async def handle(event) -> None:
            msg = event.message
            text = getattr(msg, "text", "") or ""
            if not text:
                return
            date = msg.date or datetime.now(UTC)
            sig = parse_signal(
                text,
                unixtime=int(date.timestamp()),
                date=date.isoformat(),
                message_id=getattr(msg, "id", 0) or 0,
            )
            for h in self._handlers:
                await h(sig)

        await self._client.run_until_disconnected()