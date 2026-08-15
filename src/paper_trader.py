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

import json
import logging
import time
from pathlib import Path
from typing import Any

import logs
from jupiter_swap import JupiterSwap
from models import Position, Signal
from price_feed import PriceFeed

logger = logging.getLogger(__name__)

EXIT_LABELS = {"tp": "take-profit", "sl": "stop-loss", "timeout": "1h timeout"}


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
    """

    def __init__(
        self,
        feed: PriceFeed,
        size_sol: float = 0.1,
        checkpoint: Path | None = None,
        jupiter: JupiterSwap | None = None,
        notifier=None,
        start_balance_sol: float = 10.0,
        take_profit: float = 3.0,
        stop_loss: float = 0.5,
        timeout_s: float = 3600.0,
    ) -> None:
        self.feed = feed
        self.size_sol = size_sol
        self.checkpoint = checkpoint
        self.jupiter = jupiter
        self.notifier = notifier
        self.start_balance_sol = start_balance_sol
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.timeout_s = timeout_s
        self.gate_open = True
        self.open: dict[str, Position] = {}
        self.closed: list[Position] = []
        self._signals_seen: set[str] = set()
        self._names: dict[str, str] = {}
        self._token_amounts: dict[str, int] = {}  # live-mode raw token balance
        self._load()

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
            self.open[pos.ca] = pos

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
        for field in ("entry_time", "entry_px", "peak_px", "exit_time", "exit_px",
                      "exit_reason", "take_profit", "stop_loss", "timeout_s", "size_sol"):
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
            self._signals_seen.discard(mint)
            if not self.gate_open:
                logger.info("SKIP %s entry: gate closed", mint)
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
                logger.info("LIVE BUY %s @ %.12g SOL (sig=%s)",
                            mint, price, swap.signature[:16])
            pos = Position(
                ca=mint, name="", signal_time=0.0, size_sol=self.size_sol,
                take_profit=self.take_profit, stop_loss=self.stop_loss,
                timeout_s=self.timeout_s,
            )
            pos.entry_time = now
            pos.entry_px = price
            pos.peak_px = price
            self.open[mint] = pos
            logger.info("OPEN %s @ %.12g SOL", mint, price)
            logs.journal("open", ca=mint, entry_px=price)
            if self.notifier is not None:
                await self.notifier.send_open(mint, self._names.get(mint, ""), price)
            self._save()
            return

        pos = self.open.get(mint)
        if pos is None:
            return
        if pos.peak_px is None or price > pos.peak_px:
            pos.peak_px = price
        reason = pos.update(now)
        if reason:
            self.closed.append(pos)
            del self.open[mint]
            # Live mode: place the real sell before recording the close.
            if self.jupiter is not None and self.jupiter.live:
                amount = self._token_amounts.pop(mint, 0)
                if amount > 0:
                    swap = await self.jupiter.sell(mint, amount)
                    if swap.success:
                        logs.journal("sell", ca=mint, sig=swap.signature,
                                     output_amount=swap.output_amount)
                        logger.info("LIVE SELL %s (sig=%s)", mint, swap.signature[:16])
                    else:
                        logs.journal("sell_failed", ca=mint, error=swap.error)
                        logger.warning("LIVE SELL FAILED %s: %s", mint, swap.error)
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
                )
            self._save()

    async def offer(self, sig: Signal) -> None:
        """Mark a passing signal as eligible for entry on first trade.

        Rejects the signal when the trade gate is closed, or when Jupiter has
        no usable route (quote gate, never executed in paper mode).

        Args:
            sig: A filtered signal that passed all rules.
        """
        if not self.gate_open:
            logger.info("SKIP %s (%s): gate closed", sig.ca, sig.name)
            logs.journal("skip", ca=sig.ca, name=sig.name, reason="gate_closed")
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
        self._signals_seen.add(sig.ca)
        self._names[sig.ca] = sig.name
        logger.info("ARMED %s (%s) snipes=%d mcap=$%.0f", sig.ca, sig.name, sig.snipes, sig.mcap_usd)
        logs.journal("arm", ca=sig.ca, name=sig.name, snipes=sig.snipes, mcap_usd=sig.mcap_usd)
        if self.notifier is not None:
            await self.notifier.send_arm(sig.ca, sig.name, sig.snipes, sig.mcap_usd)

    # --------------------------------------------------------------- reporting
    def balance(self) -> float:
        """Current paper balance = starting balance + realized PnL (SOL)."""
        return self.start_balance_sol + self._realized_pnl()

    def _realized_pnl(self) -> float:
        return sum(p.pnl_sol for p in self.closed)

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
            "gate_open": self.gate_open,
            "mode": "LIVE" if (self.jupiter is not None and self.jupiter.live) else "PAPER",
            "quote_gate": self.jupiter.quote_summary() if self.jupiter is not None else "disabled",
        }

    def snapshot(self) -> dict[str, Any]:
        """Full serializable state for reporting/persistence."""
        return {
            "summary": self.summary(),
            "open": [p.to_dict() for p in self.open.values()],
            "closed": [p.to_dict() for p in self.closed],
        }