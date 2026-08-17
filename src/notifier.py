"""Telegram bot notifications — markdown trade cards.

This is the only place the bot talks to Telegram. Everything is optional: if
no ``BOT_TOKEN`` / ``CHAT_ID`` are configured every method becomes a silent
no-op, so the bot still runs in console-only mode.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import telegramify_markdown
from telegram import Bot
from telegram import MessageEntity as TGMessageEntity

import config

log = logging.getLogger(__name__)

SEP = "•"

ICONS = {
    "start": "🚀",
    "arm": "🟢",
    "open": "🟢",
    "close": "💰",
    "tp": "🎯",
    "sl": "🛑",
    "timeout": "⏱️",
    "skip": "⚠️",
    "alert": "⚠️",
    "stop": "🏁",
}


class TelegramNotifier:
    """Sends formatted, markdown-friendly messages to a single chat."""

    def __init__(self) -> None:
        s = config.load_settings()
        self._chat_id = s.chat_id
        self._enabled = bool(s.bot_token and s.chat_id)
        self._bot: Bot | None = None
        if self._enabled:
            self._bot = Bot(s.bot_token)

    # ----------------------------------------------------------------- send
    async def _send(self, text: str) -> None:
        """Send one message with resolved markdown entities (best-effort)."""
        if not self._enabled or self._bot is None:
            log.debug("telegram disabled — dropping: %s", text[:80])
            return
        await self.send_to(self._chat_id, text)

    async def send_to(self, chat_id: int | str, text: str) -> None:
        """Send one message to an explicit chat id (used for command replies).

        Args:
            chat_id: Target chat id.
            text: Raw markdown; entities are resolved before sending.
        """
        if not self._enabled or self._bot is None:
            log.debug("telegram disabled — dropping: %s", text[:80])
            return
        try:
            rendered, entities = telegramify_markdown.convert(text, latex_escape=False)
            tg_entities = self._to_tg_entities(entities)
            await self._bot.send_message(
                chat_id=chat_id, text=rendered, entities=tg_entities
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("telegram send failed: %s", exc)

    @staticmethod
    def _to_tg_entities(items) -> list[TGMessageEntity]:
        """Translate telegramify_markdown entities to python-telegram-bot ones."""
        result = []
        for item in items or []:
            kwargs = {"type": item.type, "offset": item.offset, "length": item.length}
            url = getattr(item, "url", None)
            if url:
                kwargs["url"] = url
            result.append(TGMessageEntity(**kwargs))
        return result

    # ------------------------------------------------------------- lifecycle
    async def test(self) -> bool:
        """Verify the bot credentials against the API."""
        if not self._enabled or self._bot is None:
            print("[telegram] disabled")
            return False
        try:
            me = await self._bot.get_me()
            print(f"[telegram] connected as @{me.username}")
            return True
        except Exception as exc:  # noqa: BLE001
            print(f"[telegram] connection failed: {exc}")
            return False

    async def send_startup(self, summary: str = "") -> None:
        """Startup card carrying the active config line."""
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        await self._send(f"{ICONS['start']} **Bot Started**\n{SEP} `{now}`\n{SEP} {summary}")

    async def send_alert(self, title: str, detail: str = "") -> None:
        """Generic warning/health line."""
        body = f"{ICONS['alert']} **{title}**"
        if detail:
            body += f"\n{SEP} {detail}"
        await self._send(body)

    # ----------------------------------------------------------------- trade
    @staticmethod
    def _short(mint: str) -> str:
        """Short mint for display."""
        return f"`{mint[:10]}…`"

    async def send_arm(self, ca: str, name: str, snipes: int, mcap_usd: float) -> None:
        """A signal passed the filter and is armed for entry."""
        await self._send(
            f"{ICONS['arm']} **ARMED** `{name}`\n"
            f"{self._short(ca)}\n"
            f"{SEP} Snipes `{snipes}` {SEP} MCap `${mcap_usd:,.0f}`"
        )

    async def send_open(self, ca: str, name: str, price: float) -> None:
        """Position opened on the first live buy event."""
        await self._send(
            f"{ICONS['open']} **OPEN** `{name}`\n"
            f"{self._short(ca)}\n"
            f"{SEP} Entry `{price:.12g}` SOL"
        )

    async def send_close(
        self,
        ca: str,
        name: str,
        reason: str,
        mult: float,
        pnl_sol: float,
        hold_s: float | None = None,
    ) -> None:
        """Position closed (tp / sl / timeout) with simulated PnL."""
        icon = ICONS.get(reason, "💰")
        card = ICONS["close"] if pnl_sol >= 0 else ICONS["sl"]
        s = "+" if pnl_sol >= 0 else ""
        held = f" {SEP} Held `{hold_s:.0f}s`" if hold_s is not None else ""
        await self._send(
            f"{card} **CLOSE {reason.upper()}** {icon}\n"
            f"`{(ca or '')[:10]}…`\n"
            f"{SEP} Mult `{mult:.2f}x` {SEP} PnL `{s}{pnl_sol:.4f} SOL`{held}"
        )

    async def send_summary(self, summary: dict[str, Any]) -> None:
        """Periodic paper-trading summary."""
        await self._send(
            f"{ICONS['close']} **Summary**\n"
            f"{SEP} Open `{summary['open']}` {SEP} Closed `{summary['closed']}`\n"
            f"{SEP} WinRate `{summary['win_rate']:.1f}%`\n"
            f"{SEP} PnL `{summary['pnl_sol']:+.4f}` SOL"
        )

    async def send_stopped(self, summary: dict[str, Any]) -> None:
        """Shutdown card confirming the bot stopped cleanly."""
        await self._send(
            f"{ICONS['stop']} **Bot Stopped**\n"
            f"{SEP} Open `{summary['open']}` {SEP} Closed `{summary['closed']}`\n"
            f"{SEP} WinRate `{summary['win_rate']:.1f}%`\n"
            f"{SEP} PnL `{summary['pnl_sol']:+.4f}` SOL"
        )

    # ------------------------------------------------------------- commands
    async def poll_commands(self, handler) -> None:
        """Long-poll the bot's update stream and dispatch text commands.

        Runs forever (until the enclosing loop is cancelled). Every incoming
        message whose text starts with ``/`` is passed to ``handler``, which
        returns a reply string (or None to stay silent).

        Args:
            handler: Async ``(message_text, chat_id) -> str | None``.
        """
        if not self._enabled or self._bot is None:
            log.debug("telegram disabled — command polling off")
            return
        offset = 0
        while True:
            try:
                updates = await self._bot.get_updates(
                    offset=offset, timeout=30, allowed_updates=["message"]
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("get_updates failed: %s", exc)
                await asyncio.sleep(5)
                continue
            for update in updates:
                offset = update.update_id + 1
                msg = update.message
                if msg is None or not msg.text or not msg.text.startswith("/"):
                    continue
                try:
                    reply = await handler(msg.text.strip(), msg.chat_id)
                except Exception:
                    log.exception("command handler failed: %s", msg.text)
                    continue
                if reply:
                    await self.send_to(msg.chat_id, reply)