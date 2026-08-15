"""Parse Telegram messages into :class:`models.Signal`.

tgdata returns messages as plain text (``Message`` column). The bundled
``docs/channel_signals.json`` export stores structured entities instead, so the
parser flattens either representation to the same plain-text form before
extracting fields.
"""

from __future__ import annotations

import re
from typing import Any

from models import Signal


def flatten(text: Any) -> str:
    """Flatten a Telegram message body to plain text.

    Handles both a plain string and Telethon's entity list (each item is either
    a ``str`` or a dict with ``type``/``text``/``href``). ``text_link`` and
    ``code`` entities keep their text but lose their mark-up, so the regexes
    below work identically on live and exported messages.

    Args:
        text: The message ``text`` field.

    Returns:
        A single flattened string.
    """
    if isinstance(text, str):
        return text
    parts = []
    for p in text:
        if isinstance(p, str):
            parts.append(p)
        else:
            parts.append(p.get("text", ""))
    return "".join(parts)


def parse_amount(raw: str) -> float:
    """Parse a human amount like ``2.40K``, ``1.00M`` or ``5000`` to a float."""
    raw = raw.replace("$", "").replace(",", "").strip()
    if not raw:
        return 0.0
    m = re.fullmatch(r"([0-9.]+)([KMB])?", raw)
    if not m:
        return 0.0
    val = float(m.group(1))
    mult = {"K": 1e3, "M": 1e6, "B": 1e9}.get(m.group(2) or "", 1.0)
    return val * mult


def _first_int(text: str, label: str) -> int | None:
    """Extract the first integer following ``label`` in ``text``."""
    m = re.search(re.escape(label) + r"\s*:\s*(\d+)", text)
    return int(m.group(1)) if m else None


def parse_signal(
    message: str | None,
    unixtime: int = 0,
    date: str = "",
    message_id: int = 0,
) -> Signal:
    """Parse a single channel message into a :class:`Signal`.

    Args:
        message: Flattened message text (or entity list).
        unixtime: Message timestamp as unix seconds.
        date: ISO date string of the message.
        message_id: Telegram message id (for dedup/checkpointing).

    Returns:
        A :class:`Signal`; fields that are absent from the message stay empty.
    """
    text = flatten(message or "")
    sig = Signal(unixtime=unixtime, date=date, message_id=message_id)

    m = re.search(r"Token\s*:\s*(.+)", text)
    if m:
        sig.name = m.group(1).strip()

    m = re.search(r"\bCA\s*:\s*([A-Za-z0-9]{32,44})", text)
    if m:
        sig.ca = m.group(1)

    m = re.search(r"\bLP\s*:\s*([A-Za-z0-9]{32,44})", text)
    if m:
        sig.lp = m.group(1)

    m = re.search(r"Init Price\s*:\s*\$?(\S+)", text)
    if m:
        sig.init_price = m.group(1)

    m = re.search(r"MCap\s*:\s*\$([\d.,KMB]+)", text)
    if m:
        sig.mcap = m.group(1)
        sig.mcap_usd = parse_amount(m.group(1))

    m = re.search(r"Dex\s*:\s*(\S+)", text)
    if m:
        sig.dex = m.group(1)

    m = re.search(r"Liquidity\s*:\s*\$([\d.,KMB]+)", text)
    if m:
        sig.liq = m.group(1)
        sig.liq_usd = parse_amount(m.group(1))

    sec = _first_int(text, "Score")
    if sec is not None:
        sig.sec_score = sec
    if sig.holders is not None:
        pass
    sig.holders = _first_int(text, "Token Holders")
    sig.insiders = _first_int(text, "Insiders") or 0
    sig.snipes = _first_int(text, "SNIPES") or 0
    sig.rushers = _first_int(text, "RUSHERS") or 0

    m = re.search(r"Top10 holdings<30%\s*:\s*(✅|❌)", text)
    if m:
        sig.top10_ok = m.group(1)

    return sig


def parse_message_dict(msg: dict) -> Signal:
    """Parse a message dict from ``docs/channel_signals.json``.

    Args:
        msg: A single message entry from the export.

    Returns:
        A parsed :class:`Signal`.
    """
    try:
        unixtime = int(msg.get("date_unixtime", 0))
    except (TypeError, ValueError):
        unixtime = 0
    return parse_signal(
        msg.get("text", ""),
        unixtime=unixtime,
        date=msg.get("date", ""),
        message_id=int(msg.get("id", 0) or 0),
    )