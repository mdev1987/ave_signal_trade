"""Logging + persistence for the bot.

All artifacts land in ``bot_logs/``:

- ``bot.log``        rotating text log (INFO+ from every module).
- ``journal.json``   append-only JSONL of significant events (signal, arm,
                     open, close) for machine-readable history.
- ``trade_log.csv``  per-trade rows appended on position close (for analysis).

``setup_logging`` must be called once at process start; the journal and trade
CSV writers are plain functions so they can be used without re-configuring.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "bot_logs"
BOT_LOG = LOG_DIR / "bot.log"
JOURNAL_LOG = LOG_DIR / "journal.json"
TRADE_CSV = LOG_DIR / "trade_log.csv"
STOP_MARKER = LOG_DIR / ".stop"

TRADE_CSV_FIELDS = [
    # identity + outcome (original schema)
    "ca", "name", "signal_time", "entry_time", "entry_px", "peak_px",
    "exit_time", "exit_px", "exit_reason", "mult", "pnl_sol", "size_sol",
    # rug-classifier feature snapshot (entry-moment, see PaperTrader)
    "filter_profile", "mcap_usd", "dex", "snipes", "liq_usd", "sec_score",
    "burned_pct", "quote_in_pool", "pool_created_by",
    "report_missing", "lp_unlocked", "mint_authority", "freeze_authority",
    "dev_rep_ok", "dev_rep_reason",
    "buy_impact_pct", "sell_impact_pct",
    "router", "mode", "slippage_bps",
    "max_out_drift_pct", "max_impact_drift_pp",
]

# Values redacted from every log line (tokens/keys/secrets must never reach
# bot.log or console). Populated by setup_logging from the active Settings.
_REDACT: tuple[str, ...] = ()


def _redact(message: str) -> str:
    """Replace every known secret with ``<redacted>``.

    Args:
        message: The raw log message.

    Returns:
        The message with each configured secret masked. ``None``-safe because
        ``%(message)s`` is always a string after formatting.
    """
    for secret in _REDACT:
        if secret and secret in message:
            message = message.replace(secret, "<redacted>")
    return message


class _RedactFormatter(logging.Formatter):
    """Scrub known secrets from the fully formatted log line."""

    def format(self, record: logging.LogRecord) -> str:
        return _redact(super().format(record))


def setup_logging(level: int = logging.INFO, log_file: str | None = None) -> None:
    """Configure root logging to console + rotating ``bot_logs/bot.log``.

    Loads the current :class:`config.Settings` to populate the secret list
    that is masked from every emitted record.

    Args:
        level: Minimum log level (default INFO).
    """
    # Quiet chatty library loggers that otherwise flood INFO with every request.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    # Idempotent: setup_logging is invoked from both __main__ and each command
    # entry point; only configure handlers once to avoid duplicate log lines.
    if root.handlers:
        return
    root.setLevel(level)

    try:
        import config

        settings = config.load_settings()
        global _REDACT
        _REDACT = _collect_secrets(settings)
    except Exception:  # noqa: BLE001
        _REDACT = ()

    fmt = _RedactFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    target = Path(BOT_LOG).parent / (log_file or "bot.log")
    file_handler = RotatingFileHandler(
        target, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
    logger = logging.getLogger("logs")
    logger.info("logging to %s", target)


def _collect_secrets(settings) -> tuple[str, ...]:
    """Collect non-empty secrets to redact from every log line.

    Reads secret fields defensively (some may be absent on a given
    :class:`config.Settings` variant) and also scans ``os.environ`` for any
    variable whose name looks like a secret (contains ``KEY``, ``TOKEN``,
    ``SECRET``, ``HASH``, ``PASS`` or ``PRIVATE``) so keys that are not part of
    Settings are still masked.

    The goal is to NEVER let a raw key/token reach ``bot.log`` or the console.
    """
    candidates: list[str] = [
        getattr(settings, name, "") or ""
        for name in (
            "bot_token", "private_key", "jupiter_api_key",
            "dex_paprika_key", "helius_api_keys", "telegram_api_hash",
        )
    ]
    for name, value in os.environ.items():
        upper = name.upper()
        if any(tok in upper for tok in ("KEY", "TOKEN", "SECRET", "HASH", "PASS", "PRIVATE")):
            candidates.append(value)
    return tuple(v for v in candidates if v)


def journal(event: str, **fields) -> None:
    """Append one JSON line to ``bot_logs/journal.json``.

    Args:
        event: Event type, e.g. ``signal``, ``arm``, ``open``, ``close``.
        **fields: Arbitrary key/value metadata for the event.
    """
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        row = {"ts": time.time(), "event": event, **fields}
        with JOURNAL_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
        # Rotate when the journal exceeds ~5 MB so a multi-week live run never
        # fills the disk: keep the newest lines in journal.json, archive the
        # rest once (journal.old), then truncate. Nothing reads the journal at
        # startup, so rotation is safe at any point.
        if JOURNAL_LOG.stat().st_size > 5 * 1024 * 1024:
            old = JOURNAL_LOG.with_suffix(".old")
            if old.exists():
                old.unlink()
            JOURNAL_LOG.rename(old)
            JOURNAL_LOG.write_text("", encoding="utf-8")
    except OSError:
        logging.getLogger("logs").exception("failed to write journal entry")


def log_trade(row: dict) -> None:
    """Append a closed-trade row to ``bot_logs/trade_log.csv``.

    Args:
        row: Position dict (see :meth:`models.Position.to_dict`).
    """
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        # Schema rotation: an older trade_log.csv (pre-features header) would
        # misalign DictWriter rows. Archive it once and start a fresh file
        # so old results stay intact and new rows carry the feature columns.
        if TRADE_CSV.exists():
            with TRADE_CSV.open("r", encoding="utf-8") as f:
                first = f.readline()
            if first.strip() and [c for c in next(csv.reader([first]))] != TRADE_CSV_FIELDS:
                backup = TRADE_CSV.with_name(
                    f"trade_log.csv.bak-{int(time.time())}"
                )
                TRADE_CSV.rename(backup)
                logging.getLogger("logs").info(
                    "trade_log.csv schema changed — archived to %s", backup.name
                )
        write_header = not TRADE_CSV.exists()
        with TRADE_CSV.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_CSV_FIELDS)
            if write_header:
                writer.writeheader()
            # Flatten the Position's nested feature snapshot into columns.
            flat = dict(row)
            feat = flat.pop("features", None)
            if isinstance(feat, dict):
                flat.update(feat)
            writer.writerow({k: flat.get(k) for k in TRADE_CSV_FIELDS})
    except OSError:
        logging.getLogger("logs").exception("failed to write trade row")