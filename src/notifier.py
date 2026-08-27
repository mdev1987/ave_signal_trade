"""Telegram bot notifications — markdown trade cards.

This is the only place the bot talks to Telegram. Everything is optional: if
no ``BOT_TOKEN`` / ``CHAT_ID`` are configured every method becomes a silent
no-op, so the bot still runs in console-only mode.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

import telegramify_markdown
from telegram import Bot
from telegram import MessageEntity as TGMessageEntity
from telegram.error import RetryAfter, TimedOut

import config

log = logging.getLogger(__name__)

SEP = "•"
# Pace outbound messages so Telegram's flood-control (≈20 msg/min per chat)
# is never tripped. A minimum gap between sends bounds the rate; the per-minute
# cap is a backstop. On a 429 we honour retry_after and back off.
_MIN_GAP_S = 3.5        # => at most ~17 messages/minute
_MAX_PER_MIN = 18       # safety backstop (never hit while pacing above)

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
        self._lock = asyncio.Lock()
        self._last_send = 0.0
        self._sent: list[float] = []

    # ----------------------------------------------------------------- send
    async def _send(self, text: str) -> None:
        """Send one message, paced to avoid Telegram flood control.

        Alerts/opens/closes funnel through here. Messages are serialised and
        spaced by ``_MIN_GAP_S`` so a burst (e.g. 37 consensus hits on a fresh
        start) is spread over ~2 min instead of tripping Telegram's 429.
        """
        if not self._enabled or self._bot is None:
            log.debug("telegram disabled — dropping: %s", text[:80])
            return
        async with self._lock:
            now = time.monotonic()
            # drop timestamps older than 60s
            self._sent = [t for t in self._sent if now - t < 60.0]
            if len(self._sent) >= _MAX_PER_MIN:
                log.warning(
                    "telegram rate limit (%d/min) — dropping message", _MAX_PER_MIN
                )
                return
            # honour the minimum gap between successive sends
            wait = _MIN_GAP_S - (now - self._last_send)
            if wait > 0:
                await asyncio.sleep(wait)
            await self.send_to(self._chat_id, text)
            self._last_send = time.monotonic()
            self._sent.append(self._last_send)

    async def send_to(self, chat_id: int | str, text: str) -> None:
        """Send one message to an explicit chat id (used for command replies).

        Honours Telegram flood-control (``RetryAfter``) and timeouts by backing
        off instead of erroring out. Command replies go here directly and skip
        the per-minute pacing.

        Args:
            chat_id: Target chat id.
            text: Raw markdown; entities are resolved before sending.
        """
        if not self._enabled or self._bot is None:
            log.debug("telegram disabled — dropping: %s", text[:80])
            return
        for attempt in range(5):
            try:
                rendered, entities = telegramify_markdown.convert(text, latex_escape=False)
                tg_entities = self._to_tg_entities(entities)
                await self._bot.send_message(
                    chat_id=chat_id, text=rendered, entities=tg_entities
                )
                return
            except RetryAfter as exc:
                # Telegram says "too many requests" — back off, don't warn-spam
                log.info("telegram flood control — retry after %.0fs", exc.retry_after)
                await asyncio.sleep(exc.retry_after + 1.0)
            except TimedOut:
                await asyncio.sleep(5.0)
            except Exception as exc:  # noqa: BLE001
                log.warning("telegram send failed (attempt %d): %s", attempt + 1, exc)
                await asyncio.sleep(2 * (attempt + 1))
        log.warning("telegram send giving up after retries")

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
    def _fmt_px(px: float) -> str:
        """Fixed-decimal price (no scientific notation): 1.243e-7 -> 0.0000001243."""
        return f"{px:.14f}".rstrip("0").rstrip(".") or "0"

    @staticmethod
    def _fmt_dur(seconds: float) -> str:
        """Human hold duration: `4m 32s` / `58s` / `1h 03m`."""
        s = int(max(seconds, 0))
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        if h:
            return f"{h}h {m:02d}m"
        if m:
            return f"{m}m {sec:02d}s"
        return f"{sec}s"

    @staticmethod
    def _meta_line(dex: str | None, source: str | None) -> str:
        """Shared `🌐 Dex … 📡 channel…` line (omits missing parts)."""
        parts = []
        if dex:
            parts.append(f"🌐 Dex `{dex}`")
        if source:
            parts.append(f"📡 `{source}`")
        return " ".join(parts)

    async def send_arm(self, ca: str, name: str,
                       dex: str | None = None, source: str | None = None,
                       snipes: int | None = None,
                       mcap_usd: float | None = None) -> None:
        """A signal passed the filter and is armed for entry.

        Snipes/mcap stay out of the card — they are journaled on the ``arm``
        event and in bot.log; the Telegram card carries only what a trader
        needs at a glance.
        """
        meta = self._meta_line(dex, source)
        body = (
            f"{ICONS['arm']} **ARMED** `{name}`\n"
            f"📍 `{ca}`"
        )
        if meta:
            body += f"\n{meta}"
        await self._send(body)

    async def send_open(
        self,
        ca: str,
        name: str,
        price: float,
        size_sol: float | None = None,
        balance_before: float | None = None,
        balance_after: float | None = None,
        open_count: int | None = None,
        max_positions: int | None = None,
        dex: str | None = None,
        source: str | None = None,
    ) -> None:
        """Position opened card."""
        lines = [f"{ICONS['open']} **OPENED** `{name}`", f"📍 `{ca}`"]
        meta = self._meta_line(dex, source)
        if meta:
            lines.append(meta)
        entry = f"💵 Entry `{self._fmt_px(price)}` SOL"
        if size_sol is not None:
            entry += f"  💰 Size `{size_sol:g}` SOL"
        lines.append(entry)
        if open_count is not None and max_positions is not None:
            lines.append(f"📊 Positions `{open_count}/{max_positions}`")
        if balance_before is not None and balance_after is not None:
            lines.append(
                f"💼 Balance `{balance_before:.4f}` → `{balance_after:.4f}` SOL"
            )
        elif balance_before is not None:
            lines.append(f"💼 Balance `{balance_before:.4f}` SOL")
        await self._send("\n".join(lines))

    async def send_close(
        self,
        ca: str,
        name: str,
        reason: str,
        mult: float,
        pnl_sol: float,
        hold_s: float | None = None,
        entry_px: float | None = None,
        exit_px: float | None = None,
        size_sol: float | None = None,
        balance_before: float | None = None,
        balance_after: float | None = None,
        open_count: int | None = None,
        max_positions: int | None = None,
        dex: str | None = None,
        source: str | None = None,
    ) -> None:
        """Position closed card (tp / sl / timeout / liq_collapse)."""
        label, r_icon = {
            "tp": ("TAKE PROFIT", "🎯"),
            "sl": ("STOP LOSS", "🛑"),
            "timeout": ("TIMEOUT", "⏱️"),
            "liq_collapse": ("LIQ COLLAPSE", "🚨"),
        }.get(reason, (reason.upper(), ICONS["close"]))
        win = pnl_sol >= 0
        pnl_icon = "✅" if win else "❌"
        sign = "+" if win else ""
        pct = (mult - 1.0) * 100.0
        lines = [
            f"{r_icon} **{label}** `{name}`",
            f"📍 `{ca or ''}`",
        ]
        meta = self._meta_line(dex, source)
        if meta:
            lines.append(meta)
        if entry_px is not None:
            exit_part = (
                f" → 📉 Exit `{self._fmt_px(exit_px)}` SOL"
                if exit_px is not None else ""
            )
            lines.append(f"📈 Entry `{self._fmt_px(entry_px)}` SOL{exit_part}")
        pnl = f"{pnl_icon} PnL `{sign}{pnl_sol:.4f}` SOL (`{sign}{pct:.1f}%`, `{mult:.2f}x`)"
        if size_sol is not None:
            pnl += f"  💰 Size `{size_sol:g}` SOL"
        lines.append(pnl)
        if hold_s is not None:
            lines.append(f"⏱️ Duration `{self._fmt_dur(hold_s)}`")
        if open_count is not None and max_positions is not None:
            lines.append(f"📊 Positions `{open_count}/{max_positions}`")
        if balance_before is not None and balance_after is not None:
            lines.append(
                f"💼 Balance `{balance_before:.4f}` → `{balance_after:.4f}` SOL"
            )
        await self._send("\n".join(lines))

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