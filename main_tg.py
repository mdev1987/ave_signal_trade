"""TG-first trader — listen to @gmgnsignals, filter, trade.

    uv run python main_tg.py          # run in foreground
    uv run main.py tg-trade           # same via main.py entry

Pipeline:
  TG signal feed (@gmgnsignals)
    → parse + quality gates (MC, liq, holders)
    → DexScreener snapshot (price, liquidity, momentum)
    → Jupiter buy (paper or live)
    → track position: poll DexScreener price
    → sell on: hard stop / take profit / trailing stop / max hold time
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path

_SOL_MINT = "So11111111111111111111111111111111111111112"

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import config as cfg  # noqa: E402
from dexscreener import DexScreenerClient  # noqa: E402
from jupiter_swap import JupiterSwap  # noqa: E402
from logs import setup_logging  # noqa: E402
from notifier import TelegramNotifier  # noqa: E402
from tg_signal_feed import TgSignalFeed  # noqa: E402

log = logging.getLogger("tg_trade")

# ──── persistence ──────────────────────────────────────────────────────
POSITIONS_FILE = Path("tg_positions.json")


def _load_positions() -> dict:
    if POSITIONS_FILE.exists():
        return json.loads(POSITIONS_FILE.read_text())
    return {}


def _save_positions(positions: dict) -> None:
    POSITIONS_FILE.write_text(json.dumps(positions, indent=2))


def _load_closed() -> list:
    p = Path("tg_closed.json")
    if p.exists():
        return json.loads(p.read_text())
    return []


def _save_closed(closed: list) -> None:
    # keep last 200
    Path("tg_closed.json").write_text(json.dumps(closed[-200:], indent=2))


# ──── position tracker ─────────────────────────────────────────────────
class PositionManager:
    """Track open positions, poll prices, execute sells."""

    def __init__(
        self,
        ds: DexScreenerClient,
        jupiter: JupiterSwap,
        notifier: TelegramNotifier,
        size_sol: float = 0.1,
        hard_stop_pct: float = -30.0,
        tp_ladder: list[tuple[float, float]] | None = None,
        max_hold_s: float = 3600.0,
        poll_s: float = 5.0,
        trail_retrace_pct: float = -40.0,
    ):
        self.ds = ds
        self.jupiter = jupiter
        self.notifier = notifier
        self.size_sol = size_sol
        self.hard_stop_pct = -abs(hard_stop_pct) * 100  # convert 0.25 → -25%
        self.tp_ladder = tp_ladder or [(1.5, 0.5), (3.0, 0.5)]
        self.max_hold_s = max_hold_s
        self.poll_s = poll_s
        self.trail_retrace_pct = -abs(trail_retrace_pct) * 100  # convert 0.35 → -35%
        self.open: dict = _load_positions()
        self.closed: list = _load_closed()
        self.stats = {"buys": 0, "sells": 0, "wins": 0, "total_pnl": 0.0}

    @property
    def n_open(self) -> int:
        return len(self.open)

    def _position_size_sol(self) -> float:
        """Dynamic sizing: reduce size when many positions are open."""
        if self.n_open == 0:
            return self.size_sol
        if self.n_open >= 5:
            return self.size_sol * 0.5
        return self.size_sol * 0.75

    async def open_position(
        self,
        ca: str,
        sym: str,
        entry_price: float,
        mc: float,
        liq: float,
    ) -> bool:
        """Buy into a position. Returns True if opened."""
        if ca in self.open:
            return False
        if self.n_open >= 8:
            log.info("skip %s — max positions (%d)", ca[:8], self.n_open)
            return False

        size = self._position_size_sol()
        log.info("BUY %s (%s) price=%.8f mc=%.0f size=%.4f SOL", ca[:8], sym, entry_price, mc, size)

        # Execute buy
        buy_amount = int(size * 1e9)  # WSOL has 9 decimals
        quote = await self.jupiter.quote(ca, buy_amount, force=True)

        if not quote or not quote.success:
            log.warning(
                "skip %s — buy quote failed: %s", ca[:8], quote.reason if quote else "exception"
            )
            return False
        if quote.output_amount <= 0:
            log.warning("skip %s — buy quote returned 0 tokens", ca[:8])
            return False

        # Use DexScreener price as entry (already in USD, accurate)
        actual_price = entry_price
        token_amount = quote.output_amount

        if self.jupiter.live:
            # Validate sell-side is quotable before buying
            sell_quote = await self.jupiter.quote_sell(ca, token_amount)
            if not sell_quote or not sell_quote.success:
                log.warning(
                    "skip %s — sell-side quote failed (%s)",
                    ca[:8],
                    sell_quote.reason if sell_quote else "no route",
                )
                return False

            # Execute the buy
            res = await self.jupiter.execute_order(quote.order)
            if not res.success:
                log.warning("buy exec failed for %s: %s", ca[:8], res.error)
                return False
            token_amount = res.output_amount
        else:
            log.info("paper mode — tracking position without executing")

        self.open[ca] = {
            "sym": sym,
            "entry_price": actual_price,
            "entry_mc": mc,
            "entry_liq": liq,
            "entry_usd": actual_price * mc if mc > 0 else 0,
            "size_sol": size,
            "token_amount": token_amount,
            "ts": time.time(),
            "last_price": actual_price,
            "peak_price": actual_price,
            "tp_sold": 0.0,  # fraction already sold via TP
            "banked_pnl": 0.0,
        }
        _save_positions(self.open)
        self.stats["buys"] += 1

        await self.notifier._send(
            f"🟢 **BUY** {sym}\n"
            f"▸ CA: `{ca[:12]}...`\n"
            f"▸ Price: `{actual_price:.10f}`\n"
            f"▸ MC: `${mc:,.0f}`\n"
            f"▸ Size: `{size:.4f} SOL`\n"
            f"▸ Open positions: {self.n_open}"
        )
        return True

    async def refresh_prices(self) -> None:
        """Poll DexScreener for current prices on all open positions."""
        if not self.open:
            return
        for ca, pos in list(self.open.items()):
            try:
                snap = await self.ds.token_pairs("solana", ca)
                if snap and snap.get("price_usd"):
                    price = float(snap["price_usd"])
                    if price > 0:
                        old = pos["last_price"]
                        pos["last_price"] = price
                        pos["peak_price"] = max(pos["peak_price"], price)
                        if old != price:
                            log.debug("price %s: %.10f -> %.10f", pos["sym"], old, price)
            except Exception:
                pass

    async def check_exits(self) -> None:
        """Check all positions for exit conditions."""
        now = time.time()
        for ca, pos in list(self.open.items()):
            entry = pos["entry_price"]
            current = pos["last_price"]
            peak = pos["peak_price"]
            hold_s = now - pos["ts"]

            if entry <= 0 or current <= 0:
                continue

            pnl_pct = (current / entry - 1.0) * 100.0
            from_peak = (current / peak - 1.0) * 100.0 if peak > 0 else 0

            reason = None

            # Hard stop
            if pnl_pct <= self.hard_stop_pct:
                reason = f"hard_stop({pnl_pct:.1f}%)"

            # Trailing stop: if peak was > 50% and retraced > retrace_pct
            elif peak / entry > 1.5 and from_peak <= self.trail_retrace_pct:
                reason = f"trail({from_peak:.1f}% from peak)"

            # Max hold time
            elif hold_s > self.max_hold_s:
                reason = f"max_hold({hold_s / 60:.0f}m)"

            # Take profit ladder
            else:
                for tp_mult, sell_frac in self.tp_ladder:
                    if pnl_pct >= (tp_mult - 1) * 100:
                        already = pos.get("tp_sold", 0.0)
                        if already < sell_frac + 0.01:
                            reason = f"TP{tp_mult}x({pnl_pct:.1f}%)"
                            break

            if reason:
                await self._sell(ca, pos, reason)

    async def _sell(self, ca: str, pos: dict, reason: str) -> None:
        """Sell a position."""
        sym = pos["sym"]
        entry = pos["entry_price"]
        current = pos["last_price"]
        size = pos["size_sol"]
        pnl_pct = (current / entry - 1.0) * 100.0 if entry > 0 else 0
        pnl_sol = size * pnl_pct / 100.0

        log.info(
            "SELL %s (%s) reason=%s pnl=%.1f%% (%.4f SOL)", ca[:8], sym, reason, pnl_pct, pnl_sol
        )

        # Execute sell
        token_amount = pos.get("token_amount", 0)
        if token_amount > 0:
            try:
                if self.jupiter.live:
                    # Validate sell-side quote exists before executing
                    sell_quote = await self.jupiter.quote_sell(ca, token_amount)
                    if not sell_quote or not sell_quote.success:
                        log.warning(
                            "sell-side quote failed for %s (%s) — position held",
                            ca[:8],
                            sell_quote.reason if sell_quote else "no route",
                        )
                        return
                    await self.jupiter.sell(ca, token_amount)
                else:
                    log.info("paper mode — tracking exit without executing")
            except Exception:
                log.exception("sell failed for %s", ca[:8])

        # Record closed trade
        trade = {
            "ca": ca,
            "sym": sym,
            "entry": entry,
            "exit": current,
            "pnl_sol": pnl_sol,
            "pnl_pct": pnl_pct,
            "size_sol": size,
            "hold_s": time.time() - pos["ts"],
            "reason": reason,
            "ts": time.time(),
        }
        self.closed.append(trade)
        _save_closed(self.closed)

        self.stats["sells"] += 1
        self.stats["total_pnl"] += pnl_sol
        if pnl_sol > 0:
            self.stats["wins"] += 1

        # Remove from open
        del self.open[ca]
        _save_positions(self.open)

        icon = "✅" if pnl_sol >= 0 else "❌"
        await self.notifier._send(
            f"{icon} **SELL** {sym} ({reason})\n"
            f"▸ PnL: `{pnl_pct:+.1f}%` (`{pnl_sol:+.4f} SOL`)\n"
            f"▸ Held: `{trade['hold_s'] / 60:.1f}m`\n"
            f"▸ Open positions: {self.n_open}"
        )

    def status(self) -> str:
        """Status card."""
        wins = self.stats["wins"]
        total = self.stats["sells"]
        wr = (wins / total * 100) if total else 0
        lines = [
            f"📊 **TG Trader** · {self.n_open} open",
            f"▸ Wins: {wins}/{total} ({wr:.0f}%)",
            f"▸ PnL: `{self.stats['total_pnl']:+.4f} SOL`",
            "",
        ]
        for ca, pos in self.open.items():
            entry = pos["entry_price"]
            current = pos["last_price"]
            pnl = (current / entry - 1.0) * 100 if entry > 0 else 0
            hold = (time.time() - pos["ts"]) / 60
            lines.append(f"  `{pos['sym']}` {ca[:8]} {pnl:+.1f}% · {hold:.0f}m")
        return "\n".join(lines)


# ──── main ──────────────────────────────────────────────────────────────
async def _run() -> int:
    s = cfg.load_settings()

    setup_logging()
    log.info("tg-trade starting")

    notifier = TelegramNotifier()
    ds = DexScreenerClient(
        base_url=s.dexscreener_base_url,
        rpm=s.dexscreener_rpm,
    )
    jupiter = JupiterSwap(dry_run=s.dry_run)

    pm = PositionManager(
        ds=ds,
        jupiter=jupiter,
        notifier=notifier,
        size_sol=s.size_sol,
        hard_stop_pct=s.hard_stop_pct,
        tp_ladder=s.tp_ladder,
        max_hold_s=s.max_hold_h * 3600,
        trail_retrace_pct=s.trail_retrace_pct,
    )

    log.info("positions loaded: %d open", pm.n_open)

    # ── signal handler ──
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_, stop.set)

    # ── TG signal feed ──
    async def on_signal(ca: str, sym: str, usd: str, score: float, wallets: list[str]):
        """Called by TgSignalFeed for each qualifying signal."""
        # score is always 3.0 from TG feed, use it to fetch fresh DexScreener data
        try:
            snap = await ds.token_pairs("solana", ca)
        except Exception:
            snap = None

        if not snap or not snap.get("price_usd"):
            log.info("signal %s (%s) — no DexScreener data, skipping", ca[:8], sym)
            return

        price = float(snap.get("price_usd") or 0)
        mc = float(snap.get("mcap") or 0)
        liq = float(snap.get("liq") or 0)
        pc = snap.get("price_change") or {}
        h1 = pc.get("h1", 0) or 0

        # Additional gates on live data
        if mc < s.tg_min_mc:
            log.info("signal %s (%s) mc=%.0f < %.0f — skip", ca[:8], sym, mc, s.tg_min_mc)
            return
        if liq < s.tg_min_liq:
            log.info("signal %s (%s) liq=%.0f < %.0f — skip", ca[:8], sym, liq, s.tg_min_liq)
            return

        # Momentum: reject if dumping hard
        if h1 < -15:
            log.info("signal %s (%s) h1=%.1f%% dumping — skip", ca[:8], sym, h1)
            return

        log.info(
            "signal %s (%s) mc=$%.0f liq=$%.0f price=$%.8f h1=%+.1f%% — BUYING",
            ca[:8],
            sym,
            mc,
            liq,
            price,
            h1,
        )

        await pm.open_position(ca, sym, price, mc, liq)

    tg_feed = None
    if s.tg_signal_enabled and s.tg_api_id and s.tg_api_hash:
        tg_feed = TgSignalFeed(
            on_signal=on_signal,
            channel=s.tg_signal_channel,
            api_id=s.tg_api_id,
            api_hash=s.tg_api_hash,
            phone=s.tg_phone,
            session_name=s.tg_session_name,
            min_mc=s.tg_min_mc,
            min_liq=s.tg_min_liq,
            min_holders=s.tg_min_holders,
        )
        tg_task = asyncio.create_task(tg_feed.run())
        tg_task.add_done_callback(_log_task_result)
        log.info("tg signal feed: started (channel=@%s)", s.tg_signal_channel)
    else:
        log.critical("TG signal feed not configured — set TG_API_ID and TG_API_HASH")
        return 2

    # ── status loop ──
    async def status_loop():
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                break
            log.info("\n%s", pm.status())

    status_task = asyncio.create_task(status_loop())
    status_task.add_done_callback(_log_task_result)

    # ── main loop: refresh prices + check exits ──
    try:
        log.info("main: sending startup notification")
        await notifier._send(
            f"🚀 **TG Trader started**\n"
            f"▸ Channel: @{s.tg_signal_channel}\n"
            f"▸ Size: {s.size_sol} SOL\n"
            f"▸ Hard stop: {s.hard_stop_pct}%\n"
            f"▸ TP ladder: {s.tp_ladder}\n"
            f"▸ Positions: {pm.n_open}"
        )

        last_refresh = 0.0
        refresh_interval = 30.0  # refresh prices every 30s, not every tick

        while not stop.is_set():
            await asyncio.sleep(pm.poll_s)
            log.debug(
                "main loop tick — stop=%s tg_alive=%s",
                stop.is_set(),
                tg_task.done() if tg_task else "?",
            )
            if tg_task and tg_task.done() and not stop.is_set():
                log.warning("tg feed task exited unexpectedly — restarting")
                tg_feed = TgSignalFeed(
                    on_signal=on_signal,
                    channel=s.tg_signal_channel,
                    api_id=s.tg_api_id,
                    api_hash=s.tg_api_hash,
                    phone=s.tg_phone,
                    session_name=s.tg_session_name,
                    min_mc=s.tg_min_mc,
                    min_liq=s.tg_min_liq,
                    min_holders=s.tg_min_holders,
                )
                tg_task = asyncio.create_task(tg_feed.run())
                tg_task.add_done_callback(_log_task_result)
            try:
                now = time.time()
                if now - last_refresh >= refresh_interval:
                    await pm.refresh_prices()
                    last_refresh = now
                await pm.check_exits()
            except Exception:
                log.exception("position management error")

    finally:
        status_task.cancel()
        if tg_feed is not None:
            tg_feed.stop()
        await jupiter.close()
        await ds.close()
        log.info("tg-trade stopped. Final: %s", pm.status())

    return 0


def _log_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        log.exception("background task %s failed", task.get_name())


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    sys.exit(main())
