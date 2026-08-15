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
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "bot_logs"
BOT_LOG = LOG_DIR / "bot.log"
JOURNAL_LOG = LOG_DIR / "journal.json"
TRADE_CSV = LOG_DIR / "trade_log.csv"
STOP_MARKER = LOG_DIR / ".stop"

TRADE_CSV_FIELDS = [
    "ca", "name", "signal_time", "entry_time", "entry_px", "peak_px",
    "exit_time", "exit_px", "exit_reason", "mult", "pnl_sol", "size_sol",
]


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logging to console + rotating ``bot_logs/bot.log``.

    Args:
        level: Minimum log level (default INFO).
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    file_handler = RotatingFileHandler(
        BOT_LOG, maxBytes=10_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)
    logger = logging.getLogger("logs")
    logger.info("logging to %s", BOT_LOG)


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
    except OSError:
        logging.getLogger("logs").exception("failed to write journal entry")


def log_trade(row: dict) -> None:
    """Append a closed-trade row to ``bot_logs/trade_log.csv``.

    Args:
        row: Position dict (see :meth:`models.Position.to_dict`).
    """
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        write_header = not TRADE_CSV.exists()
        with TRADE_CSV.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_CSV_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow({k: row.get(k) for k in TRADE_CSV_FIELDS})
    except OSError:
        logging.getLogger("logs").exception("failed to write trade row")