"""Domain models shared across the pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

FILTER = {
    "mcap_usd_min": 5000,
    "mcap_usd_max": 20000,
    "dexes": {"Pumpfunamm"},
    "snipes_min": 3,
    "sec_score_max": 0,
}

# Named filter regimes so L1/L2 statistics are never mixed. A profile sets
# the mcap band + snipes floor; individual ``FILTER_MCAP_USD_*`` /
# ``FILTER_SNIPES_MIN`` env values still override it afterwards.
FILTER_PROFILES = {
    "L1_PRODUCTION": {"mcap_usd_min": 5000, "mcap_usd_max": 20000, "snipes_min": 3},
    "L2_EXPERIMENT": {"mcap_usd_min": 2500, "mcap_usd_max": 50000, "snipes_min": 1},
}


def get_filter() -> dict:
    """The live filter, overlaid with env overrides.

    ``.env`` is loaded lazily by :func:`config.load_settings` at runtime, so a
    module-level constant would be frozen before the overrides exist. Reading
    the file here keeps ``FILTER_PROFILE``, ``FILTER_MCAP_USD_MIN`` etc.
    effective on every check.

    Returns:
        The effective rules plus a ``"profile"`` key naming the active regime
        ("custom" when no named profile matches the final values).
    """
    overrides: dict = {}
    profile_name = "custom"
    env = None
    try:
        from config import load_env

        env = load_env()
    except Exception:  # noqa: BLE001 - env loading is best-effort here
        env = None
    if env:
        raw_profile = (env.get("FILTER_PROFILE") or "").strip().upper()
        if raw_profile in FILTER_PROFILES:
            overrides.update(FILTER_PROFILES[raw_profile])
            profile_name = raw_profile
        for key, base in (
            ("mcap_usd_min", "FILTER_MCAP_USD_MIN"),
            ("mcap_usd_max", "FILTER_MCAP_USD_MAX"),
            ("snipes_min", "FILTER_SNIPES_MIN"),
            ("sec_score_max", "FILTER_SEC_SCORE_MAX"),
        ):
            raw = env.get(base)
            if raw is not None:
                try:
                    overrides[key] = int(raw)
                except ValueError:
                    pass
        # Allowed DEX set as CSV (``FILTER_DEXS=Pumpfunamm`` or
        # ``Pumpfunamm,Raydium``). The special value ``*`` (or ``all``)
        # admits EVERY dex — paper-mode research mode: trade_log rows still
        # record each token's real dex so the best venues can be measured.
        dexes_raw = env.get("FILTER_DEXS")
        if dexes_raw is not None and dexes_raw.strip():
            val = dexes_raw.strip()
            overrides["dexes"] = (
                {"*"} if val in ("*", "all", "ANY", "any")
                else {d.strip() for d in val.split(",") if d.strip()}
            )
    rules = {**FILTER, **overrides}
    if profile_name == "custom":
        for name, preset in FILTER_PROFILES.items():
            if all(rules.get(k) == v for k, v in preset.items()):
                profile_name = name
                break
    rules["profile"] = profile_name
    return rules

REASONS = {
    "dex": "dex not in allowed set",
    "mcap": "mcap outside allowed band",
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
    # None = the source feed doesn't report snipes (the snipes filter rule
    # is skipped); an actual count of 0 still fails the threshold.
    snipes: int | None = None
    rushers: int = 0
    top10_ok: str = ""
    mcap_usd: float = 0.0
    liq_usd: float = 0.0
    # Other candidate contract addresses found in the message (e.g. a
    # solscan.io/token/<mint> metadata link). A non-empty value means the
    # posted ``ca`` may be a COPYCAT riding the referenced original's brand;
    # PaperTrader resolves this per CA_MISMATCH_POLICY before arming.
    alt_cas: tuple[str, ...] = ()
    # Source channel username (e.g. "@DRBTSolanaPF") for reporting.
    source: str = ""
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
            "alt_cas": list(self.alt_cas),
            "source": self.source,
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
    # Sell robustness: consecutive failed live sells (e.g. a drained pool that
    # Jupiter can no longer quote). When the count reaches the trader's cap the
    # position is written off so a dead token never holds a slot or hammers
    # the API. ``next_sell_retry`` backs off retries across sweeps.
    sell_fail_count: int = 0
    next_sell_retry: float = 0.0  # wall-clock: don't retry the sell before this
    # Trailing stop: once the peak reaches ``trail_activate_mult`` x entry the
    # stop ratchets to peak * (1 - trail_retrace_pct). 0 disables.
    trail_activate_mult: float = 0.0
    trail_retrace_pct: float = 0.5
    # Entry-moment feature snapshot for the rug-classifier dataset (mcap,
    # dex, snipes, rugcheck flags, pool state, quote diagnostics...). Flat
    # str/float/bool values only — serialized into the checkpoint and every
    # trade_log.csv row.
    features: dict = field(default_factory=dict)

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
        # Trailing stop: once the price has reached trail_activate_mult x entry,
        # ratchet a stop-loss up to (peak - retrace). A winner that reverses
        # after a big run is locked out with gains intact instead of bleeding
        # all the way back to the fixed stop or the timeout exit.
        if (self.trail_activate_mult > 0 and self.peak_px is not None
                and self.peak_px >= self.entry_px * self.trail_activate_mult):
            cur = price if price is not None else self.last_px
            if cur is not None:
                trail_stop = self.peak_px * (1.0 - self.trail_retrace_pct)
                if cur <= trail_stop:
                    self.exit_time = now
                    self.exit_px = cur
                    self.exit_reason = "trail"
                    return "trail"
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
            "sell_fail_count": self.sell_fail_count,
            "next_sell_retry": self.next_sell_retry,
            "trail_activate_mult": self.trail_activate_mult,
            "trail_retrace_pct": self.trail_retrace_pct,
            "features": self.features,
        }