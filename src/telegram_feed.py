"""Fetch Telegram channel messages via the ``tgdata`` library.

tgdata authenticates a user session (prompting for a login code on first run)
and returns messages as a pandas DataFrame. Two consumption modes:

- **Realtime (default)**: register an ``on_new_message`` handler so signals
  fire the moment the channel posts them (Telethon event system).
- **Bulk**: one-shot ``fetch_signals`` pull for backfill/scan.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from tgdata import TgData

from models import Signal
from parser import parse_signal

logger = logging.getLogger(__name__)

DEFAULT_CHANNEL = "@AveSolanaTokenScanner"
DEFAULT_FETCH_LIMIT = 500

SignalHandler = Callable[[Signal], Awaitable[None]]


class TelegramFeed:
    """Thin wrapper around :class:`tgdata.TgData`.

    Args:
        config_path: Path to tgdata's ``config.ini`` (see config.resolve_tgdata_config).
        channel: Channel username (with ``@``) or numeric id to monitor.
    """

    def __init__(self, config_path: str, channel: str = DEFAULT_CHANNEL) -> None:
        self.config_path = config_path
        self.channel = channel
        self._tg = TgData(config_path)
        self._handlers: list[SignalHandler] = []

    async def close(self) -> None:
        """Release the persistent Telegram connection."""
        await self._tg.close()

    async def list_channels(self) -> list[dict[str, Any]]:
        """List the channels/groups the session can see.

        Returns:
            Rows of the tgdata groups DataFrame, as dicts.
        """
        df = await self._tg.list_groups()
        return df.to_dict(orient="records")

    def _row_to_signal(self, row: dict[str, Any]) -> Signal | None:
        """Convert one tgdata message row into a Signal."""
        text = row.get("Message")
        if not text:
            return None
        date: datetime = row.get("Date") or datetime.now(UTC)
        return parse_signal(
            text,
            unixtime=int(date.timestamp()),
            date=date.isoformat(),
            message_id=int(row.get("MessageId", 0) or 0),
        )

    async def fetch_signals(
        self,
        limit: int = DEFAULT_FETCH_LIMIT,
        minutes: int | None = None,
    ) -> list[Signal]:
        """Fetch recent channel messages and parse them into signals.

        Args:
            limit: Max messages to pull.
            minutes: If given, only messages from the last N minutes.

        Returns:
            Parsed signals in chronological order.
        """
        kwargs: dict[str, Any] = {"group_id": self.channel, "limit": limit}
        if minutes is not None:
            kwargs["start_date"] = datetime.now(UTC) - timedelta(minutes=minutes)
        df = await self._tg.get_messages(**kwargs)
        if df is None or df.empty:
            return []
        rows = df.to_dict(orient="records")
        signals = [s for s in (self._row_to_signal(r) for r in rows) if s is not None]
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

        Registers an ``on_new_message`` handler for the configured channel and
        dispatches every parsed signal to the registered handlers.
        """
        @self._tg.on_new_message(group_id=self.channel)
        async def handle(event) -> None:
            text = getattr(event.message, "text", "") or ""
            if not text:
                return
            date = event.message.date or datetime.now(UTC)
            sig = parse_signal(
                text,
                unixtime=int(date.timestamp()),
                date=date.isoformat(),
                message_id=getattr(event.message, "id", 0) or 0,
            )
            for h in self._handlers:
                await h(sig)

        await self._tg.run_with_event_loop()