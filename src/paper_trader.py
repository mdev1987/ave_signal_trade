"""Trading engine.

Listens for passing signals from the Telegram channel, opens a position on the
first buy event for the mint, and tracks the live price feed until take-profit
(3x), stop-loss (0.5x) or a 1-hour timeout closes it.

Execution is gated by a **trade gate** (opened by ``/start``, closed by
``/stop``) and by **DRY_RUN**:

- ``DRY_RUN=true`` (default): positions are fully simulated — entry and PnL
  come from the live price feed, and Jupiter is only used as a *quote gate*
  (a throw-away, never-executed route check).
- ``DRY_RUN=false``: the same position model runs, but a real buy is executed
  on entry and a real sell on exit through the :class:`JupiterSwap` client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import logs
from jupiter_swap import JupiterSwap
from models import Position, Signal
from pool_check import PoolChecker
from price_feed import PriceFeed

logger = logging.getLogger(__name__)

EXIT_LABELS = {"tp": "take-profit", "sl": "stop-loss", "timeout": "1h timeout"}


def _safe_float(v: Any) -> float | None:
    """Parse a value to float, tolerating the channel's ``0.0{5}643`` form."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace("$", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        pass
    try:
        from parser import parse_price_usd
        return parse_price_usd(s) or None
    except Exception:  # noqa: BLE001
        return None


class PaperTrader:
    """Coordinates signals, the price feed, and open positions.

    Args:
        feed: The live :class:`PriceFeed`.
        size_sol: Notional size in SOL per position.
        checkpoint: Optional JSON file to persist open + closed positions.
        jupiter: Optional :class:`JupiterSwap` client — quote gate in paper
            mode, real execution when configured with a live wallet.
        notifier: Optional Telegram notifier for arm/open/close cards.
        start_balance_sol: Starting paper balance for /status reporting.
        max_positions: Maximum concurrent open positions (new entries skip).
        pool_checker: Optional :class:`PoolChecker` for arm-time gates.
        entry_latency_s: Wait this many seconds after the signal before a
            first buy may open a position (lets the pool settle + get indexed).
        max_entry_mult: Skip entry if the observed price is already above
            ``max_entry_mult`` x the signal's init price (chase guard).
        max_entry_peak_pct: Skip entry if the observed price is above the
            signal's init price by more than this percent (0 = disabled).
        min_entry_px / max_entry_px: Price sanity band for entries (SOL/token).
        price_stale_s: A ``last_px`` older than this is stale for exits.
        timeout_stale_grace_s: Max extra wait for a fresh tick before forcing
            the timeout exit (prevents stale-price flattery and dead positions).
        max_tick_mult: Ticks beyond this multiple of entry are ignored as noise.
    """

    def __init__(
        self,
        feed: PriceFeed,
        size_sol: float = 0.1,
        checkpoint: Path | None = None,
        jupiter: JupiterSwap | None = None,
        notifier=None,
        start_balance_sol: float = 2.0,
        max_positions: int = 5,
        take_profit: float = 3.0,
        stop_loss: float = 0.5,
        timeout_s: float = 3600.0,
        pool_checker: PoolChecker | None = None,
        entry_latency_s: float = 2.0,
        max_entry_mult: float = 5.0,
        max_entry_peak_pct: float = 0.0,
        liq_confirm_window_s: float = 10.0,
        min_entry_px: float = 1e-11,
        max_entry_px: float = 1e-3,
        price_stale_s: float = 120.0,
        timeout_stale_grace_s: float = 300.0,
        max_tick_mult: float = 1e5,
        checkpoint_save_s: float = 300.0,
        paper_fill_sim: bool = True,
        sell_slippage_bps: int = 500,
        max_sell_failures: int = 6,
        sell_backoff_s: float = 60.0,
        trail_activate_mult: float = 2.0,
        trail_retrace_pct: float = 0.5,
        progress_cb=None,
    ) -> None:
        self.feed = feed
        self.size_sol = size_sol
        self.checkpoint = checkpoint
        self.jupiter = jupiter
        self.notifier = notifier
        self.start_balance_sol = start_balance_sol
        self.max_positions = max_positions
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.timeout_s = timeout_s
        self.pool_checker = pool_checker
        self.entry_latency_s = entry_latency_s
        self.max_entry_mult = max_entry_mult
        self.max_entry_peak_pct = max_entry_peak_pct
        self.liq_confirm_window_s = liq_confirm_window_s
        self.min_entry_px = min_entry_px
        self.max_entry_px = max_entry_px
        self.price_stale_s = price_stale_s
        self.timeout_stale_grace_s = timeout_stale_grace_s
        self.max_tick_mult = max_tick_mult
        self.checkpoint_save_s = checkpoint_save_s
        self._progress_cb = progress_cb
        self.paper_fill_sim = paper_fill_sim
        self.sell_slippage_bps = sell_slippage_bps
        self.max_sell_failures = max_sell_failures
        self.sell_backoff_s = sell_backoff_s
        self.trail_activate_mult = trail_activate_mult
        self.trail_retrace_pct = trail_retrace_pct
        # Hard cap on one close operation (live sell: quote + execute + confirm).
        # Bounded so a hung sell can never stall the whole sweep loop.
        self._close_timeout_s = 90.0
        # Cap on closed positions kept in memory + checkpoint. Full history
        # lives in trade_log.csv; only the last N are needed for /status and
        # PnL rollup, so a multi-day run never grows the checkpoint unboundedly.
        self._closed_keep = 200
        self._closed_dropped_pnl = 0.0  # PnL of truncated positions (kept for /status)
        self.gate_open = True
        self.open: dict[str, Position] = {}
        self.closed: list[Position] = []
        self._signals_seen: set[str] = set()
        self._signals_info: dict[str, Signal] = {}
        self._names: dict[str, str] = {}
        self._token_amounts: dict[str, int] = {}  # live-mode raw token balance
        self._entry_quotes: dict[str, int] = {}  # paper-sim: mint -> raw quote out
        self._live_balance_sol: float | None = None  # RPC wallet balance (live)
        self._rejects_total: int = 0
        self._last_reject: dict[str, Any] | None = None
        self._last_save_s: float = 0.0
        self._heartbeat_s: float = 0.0
        self._load()

    def note_reject(self, ca: str, name: str, reasons: list[str]) -> None:
        """Record a filter rejection for the /status report."""
        self._rejects_total += 1
        self._last_reject = {"ca": ca, "name": name, "reasons": reasons}

    def _load(self) -> None:
        """Restore positions from the checkpoint file, if any."""
        if not self.checkpoint or not self.checkpoint.exists():
            return
        try:
            data = json.loads(self.checkpoint.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("checkpoint %s unreadable; starting fresh", self.checkpoint)
            return
        for p in data.get("closed", []):
            self.closed.append(self._from_dict(p))
        for p in data.get("open", []):
            pos = self._from_dict(p)
            if pos.is_closed:
                # Stale exit markers: the old sell-backoff bug set exit_time on a
                # defer, making the position look closed while it was still in
                # the open list, so it was never retried. Clear them so a
                # restart re-triggers the close attempt (and eventually the
                # give-up writeoff) instead of parking the slot forever.
                logger.warning("RESTORE %s (%s): clearing stale exit markers "
                               "(exit_reason=%s)", pos.ca, pos.name, pos.exit_reason)
                pos.exit_time = None
                pos.exit_px = None
                pos.exit_reason = None
            self.open[pos.ca] = pos
            age = (time.time() - pos.entry_time) if pos.entry_time else 0.0
            ttl = max(0.0, (pos.entry_time + pos.timeout_s) - time.time()) if pos.entry_time else 0.0
            logger.info(
                "RESTORE %s (%s) age=%.0fs ttl=%.0fs entry=%.12g last=%s "
                "tp=%.2fx sl=%.2fx timeout=%.0fs",
                pos.ca, pos.name, age, ttl, pos.entry_px or 0.0,
                f"{pos.last_px:.12g}" if pos.last_px else "n/a",
                pos.take_profit, pos.stop_loss, pos.timeout_s,
            )
        if self.open:
            logger.info("restored %d open position(s) from checkpoint %s",
                        len(self.open), self.checkpoint)

    async def _reconcile_token_amounts(self) -> None:
        """Refresh live token amounts for restored open positions from the RPC.

        After a restart the wallet's real balances are the source of truth:
        the checkpoint may predate the persistence of ``token_amount`` or be
        stale. This runs once at startup in live mode before the sweep loop
        starts, so ``_close_position`` never sells a wrong or zero amount.
        """
        if self.jupiter is None or not self.jupiter.live or not self.open:
            return
        for mint, pos in list(self.open.items()):
            balance = await self.jupiter.token_balance(mint)
            if balance is None:
                logger.warning("token reconcile %s: RPC unavailable — using "
                               "checkpoint amount %d", mint, pos.token_amount)
                continue
            if balance != pos.token_amount:
                logger.info("token reconcile %s: checkpoint=%d wallet=%d",
                            mint, pos.token_amount, balance)
            self._token_amounts[mint] = balance
            pos.token_amount = balance

    def _save(self) -> None:
        """Persist open and closed positions to the checkpoint file."""
        if not self.checkpoint:
            return
        payload = {
            "open": [p.to_dict() for p in self.open.values()],
            "closed": [p.to_dict() for p in self.closed],
        }
        self.checkpoint.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _from_dict(d: dict[str, Any]) -> Position:
        """Rebuild a Position from its serialized dict."""
        p = Position(ca=d["ca"], name=d.get("name", ""), signal_time=d.get("signal_time", 0.0))
        for field in ("entry_time", "entry_px", "peak_px", "last_px", "last_tick_s",
                      "exit_time", "exit_px", "exit_reason", "take_profit", "stop_loss",
                      "timeout_s", "size_sol", "token_amount", "price_stale_s",
                      "timeout_stale_grace_s", "max_tick_mult", "sell_fail_count",
                      "next_sell_retry", "trail_activate_mult", "trail_retrace_pct"):
            if d.get(field) is not None:
                setattr(p, field, d[field])
        return p

    # ------------------------------------------------------------- gate control
    def set_gate(self, open_gate: bool) -> None:
        """Open or close the trade gate.

        When closed, new signals are rejected (no arming, no entries) but
        already-open positions keep being tracked to their exit.

        Args:
            open_gate: True resumes trading, False pauses new entries.
        """
        self.gate_open = open_gate
        logger.info("trade gate %s", "OPEN" if open_gate else "CLOSED")
        logs.journal("gate", open=open_gate)

    # ----------------------------------------------------------- periodic
    async def run_sweep(self, interval_s: float = 15.0) -> None:
        """Periodically advance open positions on wall-clock time.

        Positions only receive price events when the mint trades; without
        this sweep a quiet position would never hit its 1h timeout and would
        never re-price on the last known tick. In live mode it also refreshes
        the wallet balance from the RPC. Runs until cancelled.
        """
        while True:
            await asyncio.sleep(interval_s)
            # Mark progress BEFORE any network I/O: the watchdog's job is to
            # catch a wedged event loop, and run_sweep is the only place that
            # proves liveness. If a hung balance/sell call stalled the loop,
            # marking here first means the watchdog fires on the NEXT check
            # (300s) instead of never — and the waits below are hard-bounded
            # anyway, so a single slow call can't block the whole sweep.
            if self._progress_cb is not None:
                self._progress_cb()
            try:
                await asyncio.wait_for(
                    self._refresh_live_balance(), timeout=15.0
                )
            except (TimeoutError, Exception):  # noqa: BLE001
                logger.warning("sweep: balance refresh timed out — continuing")
            now = time.time()
            self._prune_stale_signal_state(now)
            for mint in list(self.open):
                pos = self.open.get(mint)
                if pos is None:
                    continue
                reason = pos.update(now)  # reuse last_px; price=None
                if reason:
                    try:
                        await asyncio.wait_for(
                            self._close_position(mint, pos, reason),
                            timeout=self._close_timeout_s,
                        )
                    except (TimeoutError, Exception):  # noqa: BLE001
                        logger.error(
                            "sweep: close of %s timed out after %.0fs — retrying "
                            "next sweep", mint, self._close_timeout_s,
                        )
            # Health heartbeat: proves the sweep loop is alive to the watchdog.
            # Logging also keeps bot.log mtime fresh so scripts/watchdog.sh can
            # detect a wedged loop (silent log) and restart the service.
            if self._progress_cb is not None:
                self._progress_cb()
            if now - self._last_save_s >= self.checkpoint_save_s:
                self._save()
                self._last_save_s = now
            if now - self._heartbeat_s >= 300.0:
                self._heartbeat_s = now
                oldest = min(
                    (p.entry_time for p in self.open.values() if p.entry_time),
                    default=0.0,
                )
                logger.info(
                    "heartbeat: %d open, %d closed, balance=%.3f SOL, oldest_age=%.0fs",
                    len(self.open), len(self.closed), self.balance(),
                    (now - oldest) if oldest else 0.0,
                )

    async def _refresh_live_balance(self) -> None:
        """Update the cached real-wallet balance (live mode only; paper no-op)."""
        if self.jupiter is None or not self.jupiter.live:
            return
        bal = await self.jupiter.balance_sol()
        if bal is not None:
            self._live_balance_sol = bal

    async def _close_position(self, mint: str, pos: Position, reason: str) -> None:
        """Close a position (shared by on_event and run_sweep).

        Live mode sells the real tokens FIRST; only when the sell succeeds is
        the position marked closed (a failed sell keeps the position open so
        it is retried on the next sweep instead of silently abandoning tokens).
        """
        balance_before = self.balance()
        if self.jupiter is not None and self.jupiter.live:
            # Sell backoff: after a failed sell, wait ``sell_backoff_s`` before
            # retrying so a dead pool stops being hammered every sweep (the
            # 15s sweep loop used to retry a drained token ~60x/hour).
            if pos.next_sell_retry and time.time() < pos.next_sell_retry:
                # Clear the exit markers: ``pos.update()`` in the sweep sets
                # exit_time/exit_px/exit_reason before _close_position runs, and
                # if we leave them set the position looks closed (is_closed) so
                # the sweep stops retrying it forever. Reset them so the next
                # sweep re-triggers the close attempt once the backoff passes.
                pos.exit_time = None
                pos.exit_px = None
                pos.exit_reason = None
                logger.info("SELL DEFER %s: backoff %.0fs left",
                            mint, pos.next_sell_retry - time.time())
                return
            # Prefer the persisted amount; fall back to the in-memory map (live
            # buy sets both) and finally to the real wallet balance via RPC so
            # a restart can never sell 0 tokens while marking a position closed.
            amount = pos.token_amount or self._token_amounts.pop(mint, 0)
            if amount <= 0:
                amount = await self.jupiter.token_balance(mint) or 0
            if amount <= 0:
                logger.error(
                    "LIVE SELL SKIPPED %s: unknown token amount — keeping "
                    "position open to retry on the next sweep", mint,
                )
                logs.journal("sell_failed", ca=mint, error="unknown token amount")
                pos.sell_fail_count += 1
                pos.next_sell_retry = time.time() + self.sell_backoff_s
                if self.notifier is not None:
                    await self.notifier.send_alert(
                        "Sell skipped", f"`{(mint or '')[:10]}…` — unknown token amount"
                    )
                return
            self._token_amounts[mint] = amount
            swap = await self.jupiter.sell(mint, amount)
            if not swap.success:
                self._token_amounts[mint] = amount  # keep for retry
                pos.exit_time = None
                pos.exit_px = None
                pos.exit_reason = None
                pos.sell_fail_count += 1
                pos.next_sell_retry = time.time() + self.sell_backoff_s
                logs.journal("sell_failed", ca=mint, error=swap.error,
                             sell_fail_count=pos.sell_fail_count)
                logger.warning("LIVE SELL FAILED %s (%d/%d): %s — keeping position",
                               mint, pos.sell_fail_count, self.max_sell_failures,
                               swap.error)
                if pos.sell_fail_count >= self.max_sell_failures:
                    # Give up: the pool is drained or un-quotable. Write the
                    # position off at its last mark (or entry), free the slot
                    # and alert, instead of retrying a dead token forever.
                    logger.error("SELL GIVE-UP %s: %d failures (%s) — writing "
                                 "position off at last mark",
                                 mint, pos.sell_fail_count, swap.error)
                    pos.exit_time = time.time()
                    pos.exit_px = pos.last_px if pos.last_px else pos.entry_px
                    pos.exit_reason = "writeoff"
                    logs.journal("close", ca=mint, reason="writeoff",
                                 exit_px=pos.exit_px, pnl_sol=pos.pnl_sol,
                                 sell_fail_count=pos.sell_fail_count)
                    self.open.pop(mint, None)
                    self._token_amounts.pop(mint, None)
                    self.closed.append(pos)
                    self._prune_closed()
                    if self.notifier is not None:
                        await self.notifier.send_alert(
                            "Sell gave up",
                            f"`{(mint or '')[:10]}…` — {self.max_sell_failures} "
                            f"failed sells, pool drained. Written off at "
                            f"{pos.exit_px:.3g} SOL/token."
                        )
                    self._save()
                    return
                if self.notifier is not None:
                    await self.notifier.send_alert(
                        "Sell failed", f"`{(mint or '')[:10]}…` — {swap.error}"
                    )
                self._save()
                return
            pos.sell_fail_count = 0
            pos.next_sell_retry = 0.0
            logs.journal("sell", ca=mint, sig=swap.signature,
                         output_amount=swap.output_amount)
            logger.info("LIVE SELL %s (sig=%s)", mint, swap.signature[:16])
            proceeds_sol = swap.output_amount / 1e9
            if pos.entry_px and proceeds_sol > 0:
                # Mark the exit so mult/pnl reflect the REAL sold proceeds.
                pos.exit_px = pos.entry_px * (proceeds_sol / pos.size_sol)
        elif self.paper_fill_sim and self.jupiter is not None:
            # Paper sell simulation: quote a real token→SOL swap for the exact
            # token amount the (simulated) buy would have netted, and mark the
            # exit from those proceeds — the same formula live mode uses. Falls
            # back to the tick mark when the amount is unknown or the quote
            # fails, so a transient API issue never corrupts paper PnL.
            amount = pos.token_amount or self._token_amounts.pop(mint, 0)
            if amount > 0:
                proceeds_raw = await self.jupiter.paper_sell_proceeds(
                    mint, amount, self.sell_slippage_bps
                )
                if proceeds_raw and proceeds_raw > 0 and pos.entry_px:
                    pos.exit_px = pos.entry_px * ((proceeds_raw / 1e9) / pos.size_sol)
                    logger.info("PAPER SELL %s proceeds=%d sl=%dbps",
                                mint, proceeds_raw, self.sell_slippage_bps)
                else:
                    logger.info("PAPER SELL %s: quote failed — tick mark", mint)
        self.closed.append(pos)
        self._prune_closed()
        self.open.pop(mint, None)
        self._signals_seen.discard(mint)
        logger.info(
            "CLOSE %s (%s) mult=%.2f pnl=%+.4f SOL",
            mint, EXIT_LABELS.get(reason, reason), pos.mult or 0.0, pos.pnl_sol,
        )
        logs.journal("close", ca=mint, reason=reason, mult=pos.mult, pnl_sol=pos.pnl_sol)
        logs.log_trade(pos.to_dict())
        if self.notifier is not None:
            await self.notifier.send_close(
                mint, self._names.get(mint, ""), reason, pos.mult or 0.0, pos.pnl_sol,
                hold_s=(pos.exit_time or 0) - (pos.entry_time or 0),
                exit_px=pos.exit_px, balance_before=balance_before,
            )
        self._save()

    # ------------------------------------------------------------------- events
    async def on_event(self, event: dict) -> None:
        """Async feed callback: fill entries and update open positions.

        Args:
            event: A raw pumpapi event dict (buy/sell carries a price).
        """
        action = event.get("action")
        mint = event.get("mint")
        price = event.get("price")
        if action not in ("buy", "sell") or not mint or price is None:
            return
        price = float(price)
        now = time.time()

        if mint in self._signals_seen and mint not in self.open:
            sig = self._signals_info.get(mint)
            # Entry latency: wait for the pool to settle + get indexed.
            if sig is not None and now - sig.unixtime < self.entry_latency_s:
                logger.info("DEFER %s entry: %.1fs < latency %.1fs",
                            mint, now - sig.unixtime, self.entry_latency_s)
                return
            # Price sanity band: reject dust/garbage prices that corrupt PnL.
            if not (self.min_entry_px <= price <= self.max_entry_px):
                logger.info("SKIP %s entry: price %.4g outside sane band", mint, price)
                logs.journal("skip_entry", ca=mint, reason="bad_price")
                self._signals_seen.discard(mint)
                return
            # Chase guard: skip if the price already ran far above init.
            init_px = _safe_float(sig.init_price) if sig else 0.0
            if init_px and init_px > 0:
                ratio = price / init_px
                if self.max_entry_mult > 0 and ratio > self.max_entry_mult:
                    logger.info("SKIP %s entry: price %.2fx init (>%.0fx)", mint, ratio, self.max_entry_mult)
                    logs.journal("skip_entry", ca=mint, reason=f"chase:{ratio:.1f}x")
                    self._signals_seen.discard(mint)
                    return
                if self.max_entry_peak_pct > 0 and (ratio - 1) * 100 > self.max_entry_peak_pct:
                    logger.info("SKIP %s entry: price %.2fx init (>%.0f%% peak)", mint, ratio, self.max_entry_peak_pct)
                    logs.journal("skip_entry", ca=mint, reason=f"peak_pct:{(ratio-1)*100:.0f}%")
                    self._signals_seen.discard(mint)
                    return
            if not self.gate_open:
                logger.info("SKIP %s entry: gate closed", mint)
                return  # stays armed — retries once the gate reopens
            if len(self.open) >= self.max_positions:
                logger.info("SKIP %s entry: at max positions (%d)", mint, self.max_positions)
                logs.journal("skip_entry", ca=mint, reason="max_positions")
                return  # stays armed — retries once a slot frees up
            self._signals_seen.discard(mint)
            balance_before = self.balance()
            entry_px = price
            # Entry-time liquidity re-check (zero-latency, cache only): between
            # the arm-time pool gate and the first trade the pool can drain
            # (rug in progress). The cached verdict is fresh (<=60s) so this
            # costs no network call; None means fail-open.
            if (self.pool_checker is not None and self.jupiter is not None
                    and self.jupiter.live):
                verdict = self.pool_checker.cached_verdict(mint)
                if verdict is not None and not verdict[0]:
                    logger.info("SKIP %s entry: pool drained at entry (%s)",
                                mint, verdict[1])
                    logs.journal("skip_entry", ca=mint, reason=f"pool_drained:{verdict[1]}")
                    return
            # Live mode: place the real buy before opening the position.
            if self.jupiter is not None and self.jupiter.live:
                swap = await self.jupiter.buy(mint, self.size_sol)
                if not swap.success:
                    logger.warning("LIVE BUY FAILED %s: %s", mint, swap.error)
                    logs.journal("buy_failed", ca=mint, error=swap.error)
                    return
                self._token_amounts[mint] = swap.output_amount
                logs.journal("buy", ca=mint, sig=swap.signature,
                             output_amount=swap.output_amount)
                # Real fill price (SOL/token) from the executed swap, not the
                # feed tick — decimals come from the RPC mint account.
                decimals = await self.jupiter.token_decimals(mint)
                if decimals is not None and swap.output_amount > 0:
                    token_amt = swap.output_amount / (10**decimals)
                    if token_amt > 0:
                        entry_px = self.size_sol / token_amt
                logger.info("LIVE BUY %s @ %.12g SOL (sig=%s)",
                            mint, entry_px, swap.signature[:16])
            # Paper fill simulation: fill at the quote's expected price (which
            # already bakes in slippage + price impact) rather than the raw
            # feed tick, and bank the matching raw token amount so paper exits
            # can simulate a real sell of exactly those tokens.
            elif self.paper_fill_sim and self.jupiter is not None:
                out = self._entry_quotes.get(mint, 0)
                if out and out > 0:
                    decimals = await self.jupiter.token_decimals(mint)
                    if decimals is None:
                        decimals = 6  # pump.fun tokens are 6-decimals
                    token_amt = out / (10**decimals)
                    if token_amt > 0:
                        entry_px = self.size_sol / token_amt
                        self._token_amounts[mint] = out
                        logger.info("PAPER FILL %s @ %.12g SOL (quote out=%d)",
                                    mint, entry_px, out)
            pos = Position(
                ca=mint,
                name=self._names.get(mint, sig.name if sig else ""),
                signal_time=sig.unixtime if sig else 0.0,
                size_sol=self.size_sol,
                token_amount=self._token_amounts.get(mint, 0),
                take_profit=self.take_profit, stop_loss=self.stop_loss,
                timeout_s=self.timeout_s,
                price_stale_s=self.price_stale_s,
                timeout_stale_grace_s=self.timeout_stale_grace_s,
                max_tick_mult=self.max_tick_mult,
                trail_activate_mult=self.trail_activate_mult,
                trail_retrace_pct=self.trail_retrace_pct,
            )
            pos.entry_time = now
            pos.entry_px = entry_px
            pos.peak_px = entry_px
            pos.last_px = entry_px
            pos.last_tick_s = now
            self.open[mint] = pos
            logger.info("OPEN %s @ %.12g SOL", mint, entry_px)
            logs.journal("open", ca=mint, entry_px=entry_px)
            if self.notifier is not None:
                await self.notifier.send_open(
                    mint, self._names.get(mint, ""), entry_px,
                    balance_before=balance_before,
                )
            self._save()
            return

        pos = self.open.get(mint)
        if pos is None:
            return
        reason = pos.update(now, price)
        if reason:
            await self._close_position(mint, pos, reason)

    async def offer(self, sig: Signal, quiet: bool = False) -> None:
        """Mark a passing signal as eligible for entry on first trade.

        Rejects the signal when the trade gate is closed, or when Jupiter has
        no usable route (quote gate, never executed in paper mode).

        Args:
            sig: A filtered signal that passed all rules.
            quiet: Suppress the Telegram arm card (used during backfill).
        """
        if not self.gate_open:
            logger.info("SKIP %s (%s): gate closed", sig.ca, sig.name)
            logs.journal("skip", ca=sig.ca, name=sig.name, reason="gate_closed")
            return
        if len(self.open) >= self.max_positions:
            logger.info("SKIP %s (%s): at max positions (%d)",
                        sig.ca, sig.name, self.max_positions)
            logs.journal("skip", ca=sig.ca, name=sig.name, reason="max_positions")
            return
        # Arm-time pool gate: reject when DexPaprika *proves* the pool is
        # dead/empty (fail-open on API errors). Also apply the signal's own
        # liquidity snapshot as a cheap first filter.
        if (self.pool_checker is not None and sig.liq_usd > 0
                and sig.liq_usd < self.pool_checker.min_liquidity_usd):
            logger.info("SKIP %s (%s): liq $%.0f < $%.0f",
                        sig.ca, sig.name, sig.liq_usd, self.pool_checker.min_liquidity_usd)
            logs.journal("skip", ca=sig.ca, name=sig.name, reason="low_liquidity")
            return
        if self.pool_checker is not None and sig.liq_usd <= 0:
            ok, reason = await self.pool_checker.check_pool(
                sig.ca, confirm_window_s=self.liq_confirm_window_s
            )
            if not ok:
                logger.info("SKIP %s (%s): pool gate (%s)", sig.ca, sig.name, reason)
                logs.journal("skip", ca=sig.ca, name=sig.name, reason=f"pool_gate:{reason}")
                return
        # Dev-reputation veto (fail-open).
        if self.pool_checker is not None:
            ok, reason = await self.pool_checker.check_dev_rep(sig.ca, sig.unixtime)
            if not ok:
                logger.info("SKIP %s (%s): dev-rep (%s)", sig.ca, sig.name, reason)
                logs.journal("skip", ca=sig.ca, name=sig.name, reason=f"dev_rep:{reason}")
                return
        if self.jupiter is not None:
            route = await self.jupiter.quote(sig.ca, int(self.size_sol * 1e9))
            if route is None or not route.success:
                reason = route.reason if route else "quote_exception"
                logger.info("SKIP %s (%s): no jupiter route (%s)", sig.ca, sig.name, reason)
                logs.journal("skip", ca=sig.ca, name=sig.name, reason=f"no_route:{reason}")
                return
            logs.journal("route", ca=sig.ca, name=sig.name,
                         out_amount=route.output_amount,
                         price_impact_pct=route.price_impact_pct,
                         routes=route.route_count)
            # Paper fill simulation: remember the quote's expected out so the
            # entry fill price reflects real slippage + impact instead of the
            # raw feed tick (a live buy would net roughly this amount).
            self._entry_quotes[sig.ca] = route.output_amount
        self._signals_seen.add(sig.ca)
        self._signals_info[sig.ca] = sig
        self._names[sig.ca] = sig.name
        logger.info("ARMED %s (%s) snipes=%d mcap=$%.0f", sig.ca, sig.name, sig.snipes, sig.mcap_usd)
        logs.journal("arm", ca=sig.ca, name=sig.name, snipes=sig.snipes, mcap_usd=sig.mcap_usd)
        if self.notifier is not None and not quiet:
            await self.notifier.send_arm(sig.ca, sig.name, sig.snipes, sig.mcap_usd)

    # --------------------------------------------------------------- reporting
    def balance(self) -> float:
        """Current available balance (SOL).

        - **Paper mode**: ``start_balance + realized PnL`` minus the SOL still
          committed to open positions (deducted on open, returned on close).
        - **Live mode**: the real wallet SOL balance fetched from the RPC
          (cached; refreshed by :meth:`run_sweep`). No deduction here — the
          wallet already reflects what was spent on buys.
        """
        if self.jupiter is not None and self.jupiter.live:
            if self._live_balance_sol is not None:
                return self._live_balance_sol
            return self.start_balance_sol
        return self.start_balance_sol + self._realized_pnl() - self._allocated()

    def _allocated(self) -> float:
        """SOL committed to currently-open paper positions."""
        return sum(p.size_sol for p in self.open.values())

    def _realized_pnl(self) -> float:
        return self._closed_dropped_pnl + sum(p.pnl_sol for p in self.closed)

    def _prune_closed(self) -> None:
        """Truncate the in-memory closed list, folding dropped PnL into a rollup.

        The full trade history lives in trade_log.csv; keeping the last
        ``_closed_keep`` positions in memory + checkpoint bounds the checkpoint
        file and restart load time on a long-running bot.
        """
        if len(self.closed) <= self._closed_keep:
            return
        drop = self.closed[: len(self.closed) - self._closed_keep]
        self._closed_dropped_pnl += sum(p.pnl_sol for p in drop)
        del self.closed[: len(self.closed) - self._closed_keep]

    def _prune_stale_signal_state(self, now: float) -> None:
        """Drop signal bookkeeping for mints that can never be entered.

        ``_signals_info`` / ``_names`` / ``_entry_quotes`` / ``_signals_seen``
        grow with every armed signal and were never evicted — a real leak on a
        long-running bot. A signal older than ``timeout_s + entry latency`` can
        no longer open a position (the pool will have moved on), so its state is
        safe to drop; entry re-arms on a fresh signal if the token is re-posted.
        """
        if not self._signals_info:
            return
        horizon = self.timeout_s + self.entry_latency_s + 300.0
        stale = [
            mint for mint, sig in self._signals_info.items()
            if now - sig.unixtime > horizon
        ]
        for mint in stale:
            self._signals_info.pop(mint, None)
            self._names.pop(mint, None)
            self._entry_quotes.pop(mint, None)
            self._signals_seen.discard(mint)
        # Safety net: bound absolute size even if signals keep streaming.
        if len(self._signals_info) > 2000:
            for mint in list(self._signals_info)[: len(self._signals_info) - 2000]:
                self._signals_info.pop(mint, None)
                self._names.pop(mint, None)
                self._entry_quotes.pop(mint, None)
                self._signals_seen.discard(mint)

    def summary(self) -> dict[str, Any]:
        """Compute running trading statistics."""
        results = [p.mult for p in self.closed if p.mult is not None]
        wins = sum(1 for m in results if m >= 3.0)
        return {
            "open": len(self.open),
            "closed": len(self.closed),
            "wins": wins,
            "win_rate": (100 * wins / len(results)) if results else 0.0,
            "avg_mult": (sum(results) / len(results)) if results else 0.0,
            "pnl_sol": self._realized_pnl(),
            "balance_sol": self.balance(),
            "allocated_sol": self._allocated(),
            "max_positions": self.max_positions,
            "gate_open": self.gate_open,
            "mode": "LIVE" if (self.jupiter is not None and self.jupiter.live) else "PAPER",
            "quote_gate": self.jupiter.quote_summary() if self.jupiter is not None else "disabled",
            "rejects_total": self._rejects_total,
            "last_reject": self._last_reject,
        }

    def snapshot(self) -> dict[str, Any]:
        """Full serializable state for reporting/persistence."""
        return {
            "summary": self.summary(),
            "open": [p.to_dict() for p in self.open.values()],
            "closed": [p.to_dict() for p in self.closed],
        }