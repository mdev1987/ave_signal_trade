"""Domain models shared across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

FILTER = {
    "mcap_usd_min": 5000,
    "mcap_usd_max": 20000,
    "dexes": {"Pumpfunamm"},
    "snipes_min": 3,
    "sec_score_max": 0,
}

REASONS = {
    "dex": "dex not in allowed set",
    "mcap": "mcap outside $5K-$20K band",
    "snipes": "snipes below threshold",
    "sec": "security score above threshold",
    "no_ca": "missing contract address",
}


@dataclass
class Signal:
    """A parsed "New Solana Pool Launched" signal from the Telegram channel."""

    unixtime: int
    date: str = ""
    name: str = ""
    ca: str = ""
    lp: str = ""
    init_price: str = ""
    mcap: str = ""
    liq: str = ""
    dex: str = ""
    sec_score: int = 0
    holders: int | None = None
    insiders: int = 0
    snipes: int = 0
    rushers: int = 0
    top10_ok: str = ""
    mcap_usd: float = 0.0
    liq_usd: float = 0.0
    message_id: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict (JSON-friendly)."""
        return {
            "date": self.date,
            "unixtime": self.unixtime,
            "name": self.name,
            "ca": self.ca,
            "lp": self.lp,
            "init_price": self.init_price,
            "mcap": self.mcap,
            "liq": self.liq,
            "dex": self.dex,
            "sec_score": self.sec_score,
            "holders": self.holders,
            "insiders": self.insiders,
            "snipes": self.snipes,
            "rushers": self.rushers,
            "top10_ok": self.top10_ok,
            "mcap_usd": self.mcap_usd,
            "liq_usd": self.liq_usd,
            "message_id": self.message_id,
        }


@dataclass
class Position:
    """A paper-trading position opened on a passing signal.

    Entry happens on the first buy event seen for the mint after the signal;
    the position then tracks the live price feed until one of the exit
    conditions (take-profit, stop-loss, or timeout) fires.
    """

    ca: str
    name: str
    signal_time: float
    entry_time: float | None = None
    entry_px: float | None = None
    peak_px: float | None = None
    last_px: float | None = None
    last_tick_s: float | None = None  # wall-clock time of the last price tick
    exit_time: float | None = None
    exit_px: float | None = None
    exit_reason: str | None = None
    take_profit: float = 3.0
    stop_loss: float = 0.5
    timeout_s: float = 3600.0
    size_sol: float = 0.1
    token_amount: int = 0  # raw token balance held (live mode, from the buy swap)
    price_stale_s: float = 120.0   # last_px older than this is "stale"
    timeout_stale_grace_s: float = 300.0  # max extra wait for a fresh tick
    max_tick_mult: float = 1e5     # ticks beyond this multiple of entry are junk

    @property
    def mult(self) -> float | None:
        """Current multiple of entry price (or None before entry)."""
        if self.entry_px is None or self.entry_px <= 0:
            return None
        # Closed: realized exit price. Open: mark at the LAST observed price
        # (not the peak — the peak is only valid for a TP limit-order fill).
        px = self.exit_px if self.exit_px is not None else self.last_px
        if px is None:
            return None
        return px / self.entry_px

    @property
    def pnl_sol(self) -> float:
        """Realized (or mark-to-market) PnL in SOL."""
        m = self.mult
        if m is None:
            return 0.0
        return self.size_sol * (m - 1.0)

    @property
    def is_closed(self) -> bool:
        """Whether the position has an exit recorded."""
        return self.exit_time is not None

    def update(self, now: float, price: float | None = None) -> str | None:
        """Advance the position against a price snapshot.

        Args:
            now: Current unix time (seconds).
            price: The latest observed price. When omitted, ``last_px`` is
                reused; with no price at all the position is only advanced by
                wall-clock time (timeout check).

        Returns:
            The exit reason if the position closed during this tick, else None.
        """
        if self.entry_px is None or self.is_closed:
            return None
        if price is not None:
            if not self._plausible_tick(price):
                return None  # ignore garbage ticks (absurd spikes/dust)
            self.last_px = price
            self.last_tick_s = now
            if self.peak_px is None or price > self.peak_px:
                self.peak_px = price
        # Take-profit is a limit order: fills when the price *ever* touched
        # entry*tp, so the peak is the right trigger.
        if self.peak_px is not None and self.peak_px >= self.entry_px * self.take_profit:
            self.exit_time = now
            self.exit_px = self.entry_px * self.take_profit  # fills at the limit
            self.exit_reason = "tp"
            return "tp"
        # Stop-loss is a stop-market order: triggers on the CURRENT price
        # (never on the peak — the peak only moves up and can't cross below).
        cur = price if price is not None else self.last_px
        if cur is not None and cur <= self.entry_px * self.stop_loss:
            self.exit_time = now
            self.exit_px = cur  # fills at the current (worse) price
            self.exit_reason = "sl"
            return "sl"
        if now - self.entry_time >= self.timeout_s:
            # A stale last price flatters the paper exit. Prefer a fresh tick;
            # only force the close once the stale-grace window has elapsed so a
            # dead feed can never leave a position open forever.
            stale = (
                self.last_tick_s is not None
                and now - self.last_tick_s > self.price_stale_s
            )
            if stale and now - self.entry_time < self.timeout_s + self.timeout_stale_grace_s:
                return None  # wait for a fresh tick before marking the exit
            self.exit_time = now
            self.exit_px = cur if cur is not None else (self.peak_px or self.entry_px)
            self.exit_reason = "timeout"
            return "timeout"
        return None

    def _plausible_tick(self, price: float) -> bool:
        """Reject absurd price ticks that would corrupt peak/exit marks.

        A valid tick stays within ``max_tick_mult`` of the entry price on
        either side (e.g. 1e5). Out-of-range values (decimal dust, 1e4 SOL
        spikes) are treated as feed noise and ignored.
        """
        if price is None or price <= 0 or self.entry_px is None or self.entry_px <= 0:
            return False
        return price < self.entry_px * self.max_tick_mult and price > self.entry_px / self.max_tick_mult

    def to_dict(self) -> dict[str, Any]:
        """Serialize the position (including running stats)."""
        return {
            "ca": self.ca,
            "name": self.name,
            "signal_time": self.signal_time,
            "entry_time": self.entry_time,
            "entry_px": self.entry_px,
            "peak_px": self.peak_px,
            "last_px": self.last_px,
            "last_tick_s": self.last_tick_s,
            "exit_time": self.exit_time,
            "exit_px": self.exit_px,
            "exit_reason": self.exit_reason,
            "mult": self.mult,
            "pnl_sol": self.pnl_sol,
            "size_sol": self.size_sol,
            "token_amount": self.token_amount,
            "take_profit": self.take_profit,
            "stop_loss": self.stop_loss,
            "timeout_s": self.timeout_s,
            "price_stale_s": self.price_stale_s,
            "timeout_stale_grace_s": self.timeout_stale_grace_s,
            "max_tick_mult": self.max_tick_mult,
        }