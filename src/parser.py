"""Parse Telegram messages into :class:`models.Signal`.

Telegram messages carry plain text. The bundled
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
        return _strip_markdown(text)
    parts = []
    for p in text:
        if isinstance(p, str):
            parts.append(p)
        else:
            parts.append(p.get("text", ""))
    return _strip_markdown("".join(parts))


def _strip_markdown(text: str) -> str:
    """Reduce Telethon markdown to the plain shape the regexes expect.

    The live channel wraps every label in markdown (e.g. ``**CA**: `abc```),
    while the offline export used to store plain text. Strip bold/italic/code,
    links and spoilers so both representations parse identically:
    ``**CA**: `G9mY…` `` -> ``CA: G9mY…``.
    """
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)  # [txt](url) -> txt
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)            # **bold**
    text = re.sub(r"\*(.+?)\*", r"\1", text)                # *italic*
    text = re.sub(r"`(.+?)`", r"\1", text)                  # `code`
    text = re.sub(r"~(.+?)~", r"\1", text)                  # ~strike~
    text = re.sub(r"\|\|(.+?)\|\|", r"\1", text)            # ||spoiler||
    return text


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


def parse_price_usd(raw: str) -> float:
    """Parse a token price like ``0.0{5}643`` or ``0.00000643`` to a float.

    The channel writes tiny prices with a ``{n}`` brace notation meaning
    "repeat the previous character n times", e.g. ``0.0{5}643`` expands to
    ``0.0`` + five zeros + ``643`` == ``0.00000643``.
    """
    raw = raw.replace("$", "").replace(",", "").strip()
    if not raw:
        return 0.0
    expanded = re.sub(r"(.)\{(\d+)\}", lambda m: m.group(1) * int(m.group(2)), raw)
    try:
        return float(expanded)
    except ValueError:
        return 0.0


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

    Three message shapes are understood:

    - **Ave** (legacy export/replay): ``Token:``/``CA:``/``MCap:`` labels.
    - **DRBTSolanaPF** new pump.fun launch: ``Name | TICKER`` headline +
      ``Mint: <ca>``; no mcap/liq/snipes metadata.
    - **SOLTRENDING** buy alert: title ``⏺ | Project / SYMBOL``, a
      ``Market Cap $...`` line, and the mint hidden in the ``jup.ag/swap/``
      Buy link href (extracted from the raw markdown before flattening).

    Args:
        message: Raw markdown or entity-list message body.
        unixtime: Message timestamp as unix seconds.
        date: ISO date string of the message.
        message_id: Telegram message id (for dedup/checkpointing).

    Returns:
        A :class:`Signal`; fields that are absent from the message stay
        empty/None so the filter can skip rules the feed can't support.
    """
    raw = message if isinstance(message, str) else ""
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
    sig.snipes = _first_int(text, "SNIPES")
    sig.rushers = _first_int(text, "RUSHERS") or 0

    m = re.search(r"Top10 holdings<30%\s*:\s*(✅|❌)", text)
    if m:
        sig.top10_ok = m.group(1)

    # --- DRBTSolanaPF (new pump.fun launch) ---
    if not sig.ca:
        m = re.search(r"\bMint\s*:\s*([A-Za-z0-9]{32,44})", text)
        if m:
            sig.ca = m.group(1)
    if not sig.name:
        m = re.search(r"^\s*(.+?)\s*\|\s*(.+?)\s*$", text, re.MULTILINE)
        if m and m.group(1).strip() not in ("", "⏺"):
            sig.name = m.group(1).strip()

    # --- SOLTRENDING (buy alert; mint only exists in the Buy link href) ---
    m = re.search(r"/swap/(?:SOL-)?([A-Za-z0-9]{32,44})", raw)
    if m:
        sig.ca = sig.ca or m.group(1)
        if not sig.name:
            t = re.search(r"^\s*⏺\s*\|?\s*(.+?)\s*$", text, re.MULTILINE)
            if t:
                sig.name = t.group(1).split("/")[-1].strip()
    if not sig.mcap_usd:
        m = re.search(r"Market Cap\s*:?\s*\$([\d.,KMB]+)", text)
        if m:
            sig.mcap = m.group(1)
            sig.mcap_usd = parse_amount(m.group(1))

    # pump.fun launches carry no dex label; the canonical `...pump` mint
    # suffix identifies PumpAMM so the dex filter keeps applying to them.
    if not sig.dex and sig.ca.endswith("pump"):
        sig.dex = "Pumpfunamm"

    # --- copycat detection: referenced token addresses ---------------------
    # DRBT launch posts sometimes embed a DIFFERENT pump token in their
    # metadata links (e.g. `https://solscan.io/token/<mint>#metadata`, or a
    # pump-vanity address anywhere in the text). On-chain evidence (2026-08-24
    # GrokBot/WASTED) shows the posted "Mint:" is then a COPYCAT whose
    # metadata points at the referenced ORIGINAL. Collect every distinct
    # candidate so PaperTrader can resolve the mismatch by policy.
    cands: list[str] = []
    for pat in (r"solscan\.io/(?:token|account)/([1-9A-HJ-NP-Za-km-z]{32,44})",
                r"\b([1-9A-HJ-NP-Za-km-z]{39,44}pump)\b"):
        for m2 in re.finditer(pat, message if isinstance(message, str) else raw):
            addr = m2.group(1)
            if addr and addr != sig.ca:
                cands.append(addr)
    sig.alt_cas = tuple(dict.fromkeys(cands))

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