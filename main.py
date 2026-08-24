"""Ave signal trade — Telegram signal filter + live trader.

Commands:
    uv run main.py scan [--input docs/channel_signals.json]
        Parse + filter an offline export; print the passing signals and
        cross-check win rate against the 2026-08-13 outcomes.
    uv run main.py trade [--channels @DRBTSolanaPF,@SOLTRENDING]
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
(config.config.resolve_telegram_creds) and writes the telethon_session.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import config
import filter as filt
import logs
import parser as parser_mod
from dexscreener import DexScreenerClient
from filter import filter_signals
from jupiter_swap import JupiterError, JupiterSwap
from models import FILTER
from notifier import SEP, TelegramNotifier
from paper_trader import PaperTrader
from pool_check import PoolChecker
from price_feed import PriceFeed
from rugcheck import RugChecker
from scam_damper import ScamDamper
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


async def _handle_new_signal(
    trader: PaperTrader,
    seen_cas: set[str],
    sig,
    damper: ScamDamper | None = None,
    notify: bool = True,
) -> None:
    """Dedupe a real-time signal, arm it when it passes, or send a reject card.

    Args:
        trader: The active PaperTrader.
        seen_cas: Set of CAs already processed (dedup).
        sig: The parsed signal.
        damper: Optional serial-relaunch damper; every signal is recorded so
            relaunch farms accumulate evidence even when individual mints are
            rejected for other reasons.
        notify: Whether to send Telegram cards (False during backfill to
            avoid flood control from hundreds of historical rejects).
    """
    if not sig.ca or sig.ca in seen_cas:
        return
    seen_cas.add(sig.ca)
    # Bound the dedup set: with hundreds of signals/day the set grows forever
    # (a slow leak that contributed to live OOM kills). Once over 5k entries,
    # halve it — an old CA being re-posted is a fresh signal anyway, and the
    # trader's own _signals_seen/_prune logic is the real anti-double-open guard.
    if len(seen_cas) > 5000:
        seen_cas.clear()
    if damper is not None:
        damper.record(sig.ca, sig.name, float(sig.unixtime))
    ok, reasons = filt.check_signal(sig)
    # Serial-relaunch damper runs AFTER the base filter so only tradeable
    # signals get damped — zero blast radius on already-rejected spam.
    if ok and damper is not None and damper.is_serial(sig.name):
        reason = (
            f"serial relaunch: {damper.count(sig.name)} CAs share this name"
        )
        logger.info("REJECT %s (%s): %s", sig.ca, sig.name, reason)
        logs.journal("reject", ca=sig.ca, name=sig.name, reasons=[reason])
        trader.note_reject(sig.ca, sig.name, [reason])
        return
    if not ok:
        logs.journal("reject", ca=sig.ca, name=sig.name, reasons=reasons)
        # Count the rejection for /status but don't spam a Telegram card per
        # rejected signal — the details are surfaced via /status instead.
        trader.note_reject(sig.ca, sig.name, reasons)
        return
    logs.journal("signal", ca=sig.ca, name=sig.name, dex=sig.dex or None,
                 mcap_usd=sig.mcap_usd, liq_usd=sig.liq_usd,
                 snipes=sig.snipes, holders=sig.holders)
    if notify:
        await trader.offer(sig)
    else:
        await trader.offer(sig, quiet=True)


def _format_status(trader: PaperTrader) -> str:
    """Render the /status reply from the trader's current state."""
    s = trader.summary()
    bal_line = f"{SEP} Balance `{s['balance_sol']:.4f}` SOL"
    if s.get("allocated_sol"):
        bal_line += f" (alloc `{s['allocated_sol']:.3f}`/`{s['max_positions']}` max)"
    lines = [
        "**Status**",
        f"{SEP} Mode `{s['mode']}` {SEP} Gate `{'OPEN' if s['gate_open'] else 'CLOSED'}`",
        bal_line,
        f"{SEP} Closed `{s['closed']}` {SEP} WinRate `{s['win_rate']:.1f}%`",
        f"{SEP} Realized PnL `{s['pnl_sol']:+.4f}` SOL",
        f"{SEP} Rejected `{s['rejects_total']}`",
    ]
    last = s["last_reject"]
    if last:
        name = (last.get("name") or "")[:14] or (last.get("ca") or "")[:14]
        reasons = ", ".join(last.get("reasons") or [])
        lines.append(f"{SEP} Last rejected `{name}` — {reasons or 'unknown'}")
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
        recv_timeout_s=s.pumpapi_recv_timeout_s,
    )
    pool_checker = (
        PoolChecker(
            dex_paprika_key=s.dex_paprika_key,
            dex_paprika_base_url=s.dex_paprika_base_url,
            helius_api_keys=s.helius_api_keys,
            helius_base_url=s.helius_base_url,
            min_liquidity_usd=s.min_liquidity_usd,
            dev_rep_enabled=s.dev_rep_enabled,
            dev_rep_mode=s.dev_rep_mode,
            dev_rep_max_creates_24h=s.dev_rep_max_creates_24h,
            dev_rep_min_age_hours=s.dev_rep_min_age_hours,
            timeout_s=s.dev_rep_timeout_s,
            stream_state_fn=feed.pool_state,
            dexscreener=(
                DexScreenerClient(
                    base_url=s.dexscreener_base_url, rpm=s.dexscreener_rpm
                )
                if s.dexscreener_enabled
                else None
            ),
            curve_fallback_enabled=s.pool_curve_fallback,
            curve_stream_max_age_s=s.curve_stream_max_age_s,
        )
        if s.pool_check_enabled
        else None
    )
    rug_checker = (
        RugChecker(
            api_key=s.rugcheck_api_key,
            base_url=s.rugcheck_base_url,
            veto_risks=s.rugcheck_veto_risks,
            max_score_normalised=s.rugcheck_max_score,
            timeout_s=s.rugcheck_timeout_s,
            cache_ttl_s=s.rugcheck_cache_ttl_s,
            fail_closed=s.rugcheck_fail_closed,
        )
        if s.rugcheck_enabled
        else None
    )
    if rug_checker is not None:
        logger.info(
            "rugcheck gate ON (veto=%s fail_closed=%s)",
            list(rug_checker.veto_risks), rug_checker.fail_closed,
        )
    # Log the active filter tier so L1 (production core) and L2 (experiment)
    # runs are distinguishable in bot.log/journal when comparing outcomes.
    rules = filt.get_filter()
    logger.info(
        "filter profile=%s: mcap $%s-%s snipes>=%s sec<=%s dex=%s",
        rules.get("profile"), rules["mcap_usd_min"], rules["mcap_usd_max"],
        rules["snipes_min"], rules["sec_score_max"], sorted(rules["dexes"]),
    )
    logger.info("dev-rep mode: %s", s.dev_rep_mode)
    # Health watchdog: if the sweep loop stops making progress (wedged event
    # loop, hung I/O), force-exit so systemd/no-supervisor restarts it. The
    # checkpoint is saved every CHECKPOINT_SAVE_S in run_sweep, so a forced
    # exit loses at most a few minutes of position marks.
    progress: dict[str, float] = {"last": time.monotonic()}

    def _mark_progress() -> None:
        progress["last"] = time.monotonic()

    health_timeout_s = s.health_timeout_s
    if health_timeout_s > 0:
        def _health_watchdog() -> None:
            while True:
                time.sleep(health_timeout_s)
                idle = time.monotonic() - progress["last"]
                if idle >= health_timeout_s:
                    logger.critical(
                        "health watchdog: no sweep progress for %.0fs — forcing exit",
                        idle,
                    )
                    os._exit(1)

        threading.Thread(target=_health_watchdog, daemon=True).start()
        logger.info("health watchdog armed (no-progress threshold %.0fs)",
                    health_timeout_s)

    trader = PaperTrader(
        feed, size_sol=size_sol, checkpoint=checkpoint,
        jupiter=jupiter, notifier=notifier,
        start_balance_sol=s.start_balance_sol,
        max_positions=s.max_positions,
        take_profit=s.take_profit, stop_loss=s.stop_loss, timeout_s=s.timeout_s,
        pool_checker=pool_checker,
        rug_checker=rug_checker,
        entry_latency_s=s.entry_latency_s,
        max_entry_mult=s.max_entry_mult,
        max_entry_peak_pct=s.max_entry_peak_pct,
        liq_confirm_window_s=s.liq_confirm_window_s,
        min_entry_px=s.min_entry_px,
        max_entry_px=s.max_entry_px,
        price_stale_s=s.price_stale_s,
        timeout_stale_grace_s=s.timeout_stale_grace_s,
        max_tick_mult=s.max_tick_mult,
        checkpoint_save_s=s.checkpoint_save_s,
        paper_fill_sim=s.paper_fill_sim,
        sell_slippage_bps=s.jupiter_slippage_bps,
        max_sell_failures=s.max_sell_failures,
        max_sell_failures_timeout=s.max_sell_failures_timeout,
        sell_backoff_s=s.sell_backoff_s,
        trail_activate_mult=s.trail_activate_mult,
        trail_retrace_pct=s.trail_retrace_pct,
        liq_remove_veto_s=s.liq_remove_veto_s,
        min_burned_liq_pct=s.min_burned_liq_pct,
        entry_max_age_s=s.entry_max_age_s,
        liq_collapse_pct=s.liq_collapse_pct,
        liq_collapse_window_s=s.liq_collapse_window_s,
        progress_cb=_mark_progress,
    )
    seen_cas: set[str] = set()
    stop_event = asyncio.Event()
    logger.info("mode=%s gate=OPEN size=%.4f SOL max_positions=%d timeout=%.0fs",
                "LIVE" if (jupiter is not None and jupiter.live) else "PAPER",
                size_sol, s.max_positions, s.timeout_s)

    if trader.open:
        await notifier.send_alert(
            "Recovered open position(s) after restart",
            "\n".join(
                f"{SEP} `{(p.name or p.ca)[:14]}` mult `{p.mult or 0.0:.2f}x` "
                f"ttl `{max(0.0, p.entry_time + p.timeout_s - time.time()):.0f}s`"
                for p in trader.open.values()
            ),
        )

    # Backfill: arm signals already posted so positions can open immediately.
    # quiet=True: don't spam Telegram with hundreds of historical cards.
    damper = (
        ScamDamper(max_cas=s.scam_damper_max_cas, window_s=s.scam_damper_window_min * 60.0)
        if s.scam_damper_enabled
        else None
    )
    try:
        backfill = await tg.fetch_signals(limit=s.backfill_limit)
        for sig in backfill:
            await _handle_new_signal(trader, seen_cas, sig, damper=damper, notify=False)
        logger.info("backfill: %d signals, %d open, %d closed",
                    len(backfill), len(trader.open), len(trader.closed))
    except Exception:
        logger.exception("backfill failed")

    feed.on_event = trader.on_event
    tg.on_signal(lambda sig: _handle_new_signal(trader, seen_cas, sig, damper=damper))

    # Live mode: reconcile restored open positions against the real wallet
    # before the sweep loop starts, so exits never sell a wrong/zero amount.
    await trader._reconcile_token_amounts()

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
    if pool_checker is not None and pool_checker.dexscreener is not None:
        await pool_checker.dexscreener.close()
    if rug_checker is not None:
        await rug_checker.close()
    trader._save()
    if notifier is not None:
        await notifier.send_stopped(trader.summary())
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
    creds = config.resolve_telegram_creds()
    checkpoint = Path(args.checkpoint)

    async def run() -> int:
        notifier = TelegramNotifier()
        tg = TelegramFeed(creds, channels=args.channels)
        await notifier.send_startup(
            summary=f"channels `{', '.join(tg.channels)}`"
        )
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
    creds = config.resolve_telegram_creds()

    async def run() -> int:
        tg = TelegramFeed(creds, channels=args.channels)
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
    trade.add_argument("--channels", "--channel", default=",".join(s.channels),
                       help="comma-separated channel usernames, in preference order")
    trade.add_argument("--checkpoint", type=str, default=s.checkpoint_file)
    trade.set_defaults(func=cmd_trade)

    ch = sub.add_parser("channels", help="list visible channels/groups")
    ch.add_argument("--channels", "--channel", default=",".join(s.channels))
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