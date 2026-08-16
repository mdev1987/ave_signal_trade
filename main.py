"""Ave signal trade — Telegram signal filter + live trader.

Commands:
    uv run main.py scan [--input docs/channel_signals.json]
        Parse + filter an offline export; print the passing signals and
        cross-check win rate against the 2026-08-13 outcomes.
    uv run main.py trade [--channel @AveSolanaTokenScanner]
        Live trading: backfill recent signals, then stream new channel
        messages in real time (Telethon events) + pumpapi trade events. Winning
        signals are validated against Jupiter's quote gate before arming.
        Runs in PAPER mode by default (DRY_RUN=true); set DRY_RUN=false with a
        PRIVATE_KEY in .env to place real orders. Logs land in bot_logs/.
    uv run main.py channels
        List the channels/groups visible to the Telegram session.

Telegram control (via BOT_TOKEN in .env):
    /start   open the trade gate (resume trading)
    /stop    graceful shutdown: gate closes, in-flight trade finishes, exit 0
    /status  balance, winrate, realized PnL, active position, quote-gate stats
    /help    command list

On first run ``trade``/``channels`` prompts for your Telegram phone number
(config.config.resolve_tgdata_config) and writes config.ini + telegram_session.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import config
import filter as filt
import logs
import parser as parser_mod
from filter import filter_signals
from jupiter_swap import JupiterError, JupiterSwap
from models import FILTER
from notifier import SEP, TelegramNotifier
from paper_trader import PaperTrader
from pool_check import PoolChecker
from price_feed import PriceFeed
from telegram_feed import TelegramFeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("main")

HELP_TEXT = (
    "**Ave Signal Trader** — Telegram controls\n"
    f"{SEP} `/start` — open the trade gate (resume trading)\n"
    f"{SEP} `/stop` — graceful shutdown: gate closes, in-flight trade finishes, exit 0\n"
    f"{SEP} `/status` — balance, winrate, realized PnL, active position, quote-gate stats\n"
    f"{SEP} `/help` — this command list"
)


def _load_offline(path: Path) -> list:
    """Load a channel_signals.json export (dict with messages, or a list)."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return data["messages"] if isinstance(data, dict) else data


def cmd_scan(args: argparse.Namespace) -> int:
    """Parse + filter an offline export and report results."""
    msgs = _load_offline(args.input)
    signals = [parser_mod.parse_message_dict(m) for m in msgs]
    passed, counts, seen = filter_signals(signals)

    print(f"messages: {len(msgs)}  unique CAs: {len(seen)}  passed: {len(passed)}")
    for reason, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        if n:
            print(f"  reject ({reason}): {n}")

    if args.test and Path(args.test).exists():
        outcomes = json.loads(Path(args.test).read_text(encoding="utf-8"))
        by_ca = {o["ca"]: o for o in outcomes}
        n = win = 0
        for sig in passed:
            o = by_ca.get(sig.ca)
            if o is None or o.get("sim_mult") is None:
                continue
            n += 1
            win += 1 if o["sim_mult"] >= 3 else 0
        print(f"--test: {n} passed signals with sim data -> win3x = "
              f"{100 * win / max(n, 1):.1f}% ({win}/{n})")

    if args.out:
        filter_out = {k: (list(v) if isinstance(v, set) else v) for k, v in FILTER.items()}
        args.out.write_text(json.dumps({
            "filter": filter_out,
            "signals_total": len(signals),
            "unique_cas": len(seen),
            "passed_count": len(passed),
            "reject_reasons": counts,
            "passed": [s.to_dict() for s in passed],
        }, indent=2), encoding="utf-8")
    else:
        for s in passed:
            print(s.to_dict())
    return 0


async def _handle_new_signal(trader: PaperTrader, seen_cas: set[str], sig, notify: bool = True) -> None:
    """Dedupe a real-time signal, arm it when it passes, or send a reject card.

    Args:
        trader: The active PaperTrader.
        seen_cas: Set of CAs already processed (dedup).
        sig: The parsed signal.
        notify: Whether to send Telegram cards (False during backfill to
            avoid flood control from hundreds of historical rejects).
    """
    if not sig.ca or sig.ca in seen_cas:
        return
    seen_cas.add(sig.ca)
    ok, reasons = filt.check_signal(sig)
    if not ok:
        logs.journal("reject", ca=sig.ca, name=sig.name, reasons=reasons)
        if notify and trader.notifier is not None:
            await trader.notifier.send_reject(
                sig.ca, sig.name, reasons,
                mcap_usd=sig.mcap_usd, liq_usd=sig.liq_usd,
                dex=sig.dex, sec_score=sig.sec_score, snipes=sig.snipes,
            )
        return
    logs.journal("signal", ca=sig.ca, name=sig.name)
    if notify:
        await trader.offer(sig)
    else:
        await trader.offer(sig, quiet=True)


def _format_status(trader: PaperTrader) -> str:
    """Render the /status reply from the trader's current state."""
    s = trader.summary()
    lines = [
        "**Status**",
        f"{SEP} Mode `{s['mode']}` {SEP} Gate `{'OPEN' if s['gate_open'] else 'CLOSED'}`",
        f"{SEP} Balance `{s['balance_sol']:.4f}` SOL",
        f"{SEP} Closed `{s['closed']}` {SEP} WinRate `{s['win_rate']:.1f}%`",
        f"{SEP} Realized PnL `{s['pnl_sol']:+.4f}` SOL",
    ]
    for pos in trader.open.values():
        mult = pos.mult if pos.mult is not None else 0.0
        lines.append(
            f"{SEP} OPEN `{(pos.name or pos.ca)[:10]}…` entry "
            f"`{pos.entry_px:.6g}` mult `{mult:.2f}x`"
        )
    lines.append(f"`{s['quote_gate']}`")
    return "\n".join(lines)


async def _command_handler(
    trader: PaperTrader,
    stop_event: asyncio.Event,
    text: str,
    chat_id,
) -> str | None:
    """Dispatch Telegram control commands; return the reply text (or None)."""
    cmd = text.split()[0].lower()
    logger.info("telegram command %r from chat %s", cmd, chat_id)
    logs.journal("command", command=cmd, chat_id=str(chat_id))
    if cmd == "/start":
        trader.set_gate(True)
        return f"{SEP} trade gate **OPEN** — trading resumed"
    if cmd == "/stop":
        trader.set_gate(False)
        logs.STOP_MARKER.write_text("", encoding="utf-8")
        stop_event.set()
        return f"{SEP} trade gate **CLOSED** — graceful shutdown in progress"
    if cmd == "/status":
        return _format_status(trader)
    if cmd == "/help":
        return HELP_TEXT
    return None


async def _trade_loop(
    tg: TelegramFeed,
    checkpoint: Path,
    notifier: TelegramNotifier,
    jupiter: JupiterSwap | None,
) -> int:
    """Live trading loop (see cmd_trade).

    Args:
        tg: Connected TelegramFeed streaming real-time channel events.
        checkpoint: Path to persist positions.
        notifier: Telegram notifier for trade cards + command replies.
        jupiter: JupiterSwap client (quote gate; real execution when live).

    Returns:
        Exit code (0).
    """
    s = config.load_settings()
    size_sol = s.position_size_sol
    feed = PriceFeed(
        uri=s.pumpapi_wss, reconnect_s=s.pumpapi_reconnect_s,
        price_timeout_s=s.price_wait_timeout_s,
    )
    pool_checker = (
        PoolChecker(
            dex_paprika_key=s.dex_paprika_key,
            dex_paprika_base_url=s.dex_paprika_base_url,
            helius_api_keys=s.helius_api_keys,
            helius_base_url=s.helius_base_url,
            min_liquidity_usd=s.min_liquidity_usd,
            dev_rep_enabled=s.dev_rep_enabled,
            dev_rep_max_creates_24h=s.dev_rep_max_creates_24h,
            dev_rep_min_age_hours=s.dev_rep_min_age_hours,
            timeout_s=s.dev_rep_timeout_s,
        )
        if s.pool_check_enabled
        else None
    )
    trader = PaperTrader(
        feed, size_sol=size_sol, checkpoint=checkpoint,
        jupiter=jupiter, notifier=notifier,
        start_balance_sol=s.start_balance_sol,
        take_profit=s.take_profit, stop_loss=s.stop_loss, timeout_s=s.timeout_s,
        pool_checker=pool_checker,
        entry_latency_s=s.entry_latency_s,
        max_entry_mult=s.max_entry_mult,
        max_entry_peak_pct=s.max_entry_peak_pct,
        liq_confirm_window_s=s.liq_confirm_window_s,
    )
    seen_cas: set[str] = set()
    stop_event = asyncio.Event()
    logger.info("mode=%s gate=OPEN size=%.4f SOL",
                "LIVE" if (jupiter is not None and jupiter.live) else "PAPER", size_sol)

    # Backfill: arm signals already posted so positions can open immediately.
    # quiet=True: don't spam Telegram with hundreds of historical cards.
    try:
        backfill = await tg.fetch_signals(limit=s.backfill_limit)
        for sig in backfill:
            await _handle_new_signal(trader, seen_cas, sig, notify=False)
        logger.info("backfill: %d signals, %d open, %d closed",
                    len(backfill), len(trader.open), len(trader.closed))
    except Exception:
        logger.exception("backfill failed")

    feed.on_event = trader.on_event
    tg.on_signal(lambda sig: _handle_new_signal(trader, seen_cas, sig))

    # SIGTERM/SIGINT -> graceful stop (same path as /stop).
    loop = asyncio.get_running_loop()
    for signame in ("SIGTERM", "SIGINT"):
        try:
            loop.add_signal_handler(getattr(signal, signame), stop_event.set)
        except NotImplementedError:  # Windows
            pass

    async def commands() -> None:
        await notifier.poll_commands(
            lambda text, chat_id: _command_handler(trader, stop_event, text, chat_id)
        )

    feed_task = asyncio.create_task(feed.run())
    tg_task = asyncio.create_task(tg.run_realtime())
    cmd_task = asyncio.create_task(commands())
    sweep_task = asyncio.create_task(trader.run_sweep())

    logger.info("running — send /status or /stop via Telegram")
    await stop_event.wait()

    # Graceful shutdown: gate is already closed by /stop; wait for in-flight
    # trades to finish (bounded), then stop every consumer and exit 0.
    deadline = time.monotonic() + s.shutdown_grace_s
    while trader.open and time.monotonic() < deadline:
        await asyncio.sleep(1.0)
    if trader.open:
        logger.info("shutdown: %d in-flight trade(s) left open; checkpoint saved",
                    len(trader.open))
    else:
        logger.info("shutdown: all in-flight trades finished")

    feed.stop()
    cmd_task.cancel()
    tg_task.cancel()
    sweep_task.cancel()
    for task in (cmd_task, tg_task, feed_task, sweep_task):
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            logger.debug("consumer task %s cancelled", task)
    await tg.close()
    if pool_checker is not None:
        await pool_checker.close()
    trader._save()
    if notifier is not None:
        await notifier.send_summary(trader.summary())
    logger.info("shutdown complete — exit 0")
    return 0


def _build_jupiter() -> JupiterSwap | None:
    """Construct the Jupiter client honoring DRY_RUN/PRIVATE_KEY gating.

    Returns:
        A JupiterSwap client, or None when JUPITER_API_KEY is unset.
    """
    s = config.load_settings()
    if not s.jupiter_api_key:
        logger.info("JUPITER_API_KEY missing — running without jupiter quote gate")
        return None
    logger.info("DRY_RUN=%s", "true" if s.dry_run else "false")
    return JupiterSwap(dry_run=s.dry_run)


def cmd_trade(args: argparse.Namespace) -> int:
    """Stream live signals (event-based), arm winners, track positions."""
    cfg = config.resolve_tgdata_config()
    checkpoint = Path(args.checkpoint)

    async def run() -> int:
        notifier = TelegramNotifier()
        await notifier.send_startup(summary=f"channel `{args.channel}`")
        tg = TelegramFeed(str(cfg), channel=args.channel)
        try:
            jupiter = _build_jupiter()
            # Force the Telegram auth/connection up front so the phone/login
            # prompt happens before the feed starts.
            await tg.list_channels()
            return await _trade_loop(tg, checkpoint, notifier, jupiter)
        finally:
            await tg.close()

    try:
        return asyncio.run(run())
    except JupiterError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nstopped")
        return 0


def cmd_channels(args: argparse.Namespace) -> int:
    """List channels/groups visible to the Telegram session."""
    cfg = config.resolve_tgdata_config()

    async def run() -> int:
        tg = TelegramFeed(str(cfg), channel=args.channel)
        try:
            rows = await tg.list_channels()
            for r in rows:
                print(r)
            return 0
        finally:
            await tg.close()

    return asyncio.run(run())


def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser."""
    s = config.load_settings()
    ap = argparse.ArgumentParser(description="Ave signal filter + paper trader")
    sub = ap.add_subparsers(dest="cmd", required=True)

    scan = sub.add_parser("scan", help="filter an offline channel export")
    scan.add_argument("--input", type=Path, default=Path("docs/channel_signals.json"))
    scan.add_argument("--out", type=Path, default=None)
    scan.add_argument("--test", type=Path, default=None)
    scan.set_defaults(func=cmd_scan)

    trade = sub.add_parser("trade", help="live paper trading (event-based)")
    trade.add_argument("--channel", default=s.channel)
    trade.add_argument("--checkpoint", type=str, default=s.checkpoint_file)
    trade.set_defaults(func=cmd_trade)

    ch = sub.add_parser("channels", help="list visible channels/groups")
    ch.add_argument("--channel", default=s.channel)
    ch.set_defaults(func=cmd_channels)
    return ap


def main() -> int:
    """CLI entrypoint."""
    args = build_parser().parse_args()
    if getattr(args, "cmd", None) != "scan":
        logs.setup_logging()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())