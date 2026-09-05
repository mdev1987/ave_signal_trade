"""TG-first trader — listen to @gmgnsignals, filter, trade.

    uv run main.py tg-trade            # run via main.py entry point

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
from logs import setup_logging, journal  # noqa: E402
from notifier import TelegramNotifier  # noqa: E402
from tg_signal_feed import TgSignalFeed  # noqa: E402
from rugcheck import RugCheckClient  # noqa: E402

# Optional: DexPaprika for enhanced token validation
_DEXPAPRIKA_AVAILABLE = False
try:
    import httpx as _httpx_dex
    _DEXPAPRIKA_AVAILABLE = True
except ImportError:
    pass

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


# ──── DexPaprika enhanced validation ──────────────────────────────────
async def _validate_token_dexpaprika(ca: str) -> dict | None:
    """Fetch token data from DexPaprika for enhanced signal validation.

    Returns token details dict or None if unavailable.
    Used to cross-validate DexScreener data and detect rugs early.
    """
    if not _DEXPAPRIKA_AVAILABLE:
        return None
    try:
        async with _httpx_dex.AsyncClient(timeout=5.0) as client:
            r = await client.get(
                f"https://api.dexpaprika.com/networks/solana/tokens/{ca}",
                headers={"accept": "application/json"},
            )
            if r.status_code == 200:
                data = r.json()
                return data.get("summary") or data
    except Exception:
        log.debug("dexpaprika validation failed for %s", ca[:8])
    return None


def _check_rug_signals(dex_data: dict, dexpaprika_data: dict | None) -> tuple[bool, str]:
    """Check for rug pull signals using DexPaprika data.

    Returns (is_rug, reason) tuple.

    DexPaprika returns liquidity_usd=0 when a token isn't indexed — that is NOT
    a rug signal. We only flag when DexPaprika has *positive* data indicating a
    real problem (e.g. sell-heavy ratio). If the token is simply unknown to DP,
    we let it through (DexScreener already validated liquidity).
    """
    if not dexpaprika_data:
        return False, ""

    # Check if DexPaprika actually has this token indexed
    liq = dexpaprika_data.get("liquidity_usd") or 0
    vol_24h = (dexpaprika_data.get("24h") or {}).get("volume_usd") or 0

    # If DP has no data for this token (liq=0 AND vol=0), it's unindexed — not a rug
    if liq <= 0 and vol_24h <= 0:
        return False, ""

    # Only flag rug when DP *does* have data showing problems
    if liq > 0 and liq < 500:
        return True, f"extremely low liquidity ${liq:.0f}"

    if vol_24h > 0 and vol_24h < 1000:
        return True, f"very low 24h volume ${vol_24h:.0f}"

    # Check buy/sell ratio - heavy selling indicates dump
    buy_usd = (dexpaprika_data.get("24h") or {}).get("buy_usd") or 0
    sell_usd = (dexpaprika_data.get("24h") or {}).get("sell_usd") or 0
    if sell_usd > 0 and buy_usd > 0:
        sell_ratio = sell_usd / (buy_usd + sell_usd)
        if sell_ratio > 0.8:
            return True, f"heavy selling {sell_ratio*100:.0f}% sells"

    return False, ""


# ──── position tracker ─────────────────────────────────────────────────
class PositionManager:
    """Track open positions, poll prices, execute sells."""

    def __init__(
        self,
        ds: DexScreenerClient,
        jupiter: JupiterSwap,
        notifier: TelegramNotifier,
        size_sol: float = 0.1,
        hard_stop_pct: float = 0.25,
        tp_ladder: list[tuple[float, float]] | None = None,
        max_hold_s: float = 3600.0,
        poll_s: float = 5.0,
        trail_retrace_pct: float = 0.25,
        trail_start_mult: float = 1.4,
        be_buffer_pct: float = 0.0,
    ):
        self.ds = ds
        self.jupiter = jupiter
        self.notifier = notifier
        self.size_sol = size_sol
        self.hard_stop_pct = -abs(hard_stop_pct) * 100  # 0.25 → -25%
        self.tp_ladder = tp_ladder or [(1.5, 0.5), (3.0, 0.5)]
        self.max_hold_s = max_hold_s
        self.poll_s = poll_s
        self.trail_retrace_pct = -abs(trail_retrace_pct) * 100  # 0.25 → -25%
        self.trail_start_mult = max(trail_start_mult, 1.0)
        self.be_buffer_pct = be_buffer_pct
        self.open: dict = _load_positions()
        self.closed: list = _load_closed()
        self.stats = {"buys": 0, "sells": 0, "wins": 0, "total_pnl": 0.0}
        # Compute cumulative TP fractions for partial sell tracking
        self._tp_cumulative: list[float] = []
        cum = 0.0
        for _, frac in self.tp_ladder:
            cum += frac
            self._tp_cumulative.append(min(cum, 1.0))
        # Recover positions on startup
        self._recover_positions()

    def _recover_positions(self) -> None:
        """Validate and repair open positions on restart.

        - Verify each position still has tradeable liquidity
        - Update stale prices to avoid false exit triggers
        - Remove positions for tokens that no longer exist
        """
        if not self.open:
            return
        log.info("recovering %d open positions", len(self.open))
        recovered = 0
        removed = 0
        for ca, pos in list(self.open.items()):
            # Ensure required fields exist (backward compat)
            pos.setdefault("initial_token_amount", pos.get("token_amount", 0))
            pos.setdefault("tp_sold", 0.0)
            pos.setdefault("banked_pnl", 0.0)
            pos.setdefault("breakeven_locked", False)
            pos.setdefault("last_price_update", pos.get("ts", time.time()))
            # If token_amount is 0 but we think we have a position, flag it
            if pos.get("token_amount", 0) <= 0 and not self.jupiter.live:
                log.warning("position %s (%s) has 0 tokens — removing", ca[:8], pos.get("sym", "?"))
                del self.open[ca]
                removed += 1
                continue
            recovered += 1
        if removed:
            _save_positions(self.open)
        log.info("recovered %d positions, removed %d", recovered, removed)

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

        # Get balance before buy
        balance_before = await self.jupiter.balance_sol()

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
            sell_quote = await self.jupiter.quote_sell(ca, token_amount)
            if not sell_quote or not sell_quote.success:
                log.warning(
                    "skip %s — paper buy sell-side quote failed (%s)",
                    ca[:8],
                    sell_quote.reason if sell_quote else "no route",
                )
                return False
            log.info("paper mode — tracking position without executing")
            self.jupiter.paper_deduct(size)

        # Get balance after buy
        balance_after = await self.jupiter.balance_sol()

        self.open[ca] = {
            "sym": sym,
            "entry_price": actual_price,
            "entry_mc": mc,
            "entry_liq": liq,
            "entry_usd": actual_price * mc if mc > 0 else 0,
            "size_sol": size,
            "initial_token_amount": token_amount,
            "token_amount": token_amount,
            "ts": time.time(),
            "last_price": actual_price,
            "peak_price": actual_price,
            "tp_sold": 0.0,  # cumulative fraction sold via TP (0.0 - 1.0)
            "banked_pnl": 0.0,
            "breakeven_locked": False,
            "last_price_update": time.time(),
        }
        _save_positions(self.open)
        self.stats["buys"] += 1

        # Win rate for notification
        wins = self.stats["wins"]
        total = self.stats["sells"]
        wr = (wins / total * 100) if total else 0.0

        await self.notifier.send_open(
            ca=ca,
            name=sym,
            price=actual_price,
            size_sol=size,
            balance_before=balance_before,
            balance_after=balance_after,
            open_count=self.n_open,
            max_positions=8,
            win_rate=wr,
            source="gmgnsignals",
        )
        return True

    async def refresh_prices(self) -> None:
        """Poll DexScreener for current prices on all open positions."""
        if not self.open:
            return
        now = time.time()
        for ca, pos in list(self.open.items()):
            try:
                snap = await self.ds.token_pairs("solana", ca)
                if snap and snap.get("price_usd"):
                    price = float(snap["price_usd"])
                    if price > 0:
                        old = pos["last_price"]
                        pos["last_price"] = price
                        pos["peak_price"] = max(pos["peak_price"], price)
                        pos["last_price_update"] = now
                        if old != price:
                            log.debug("price %s: %.10f -> %.10f", pos["sym"], old, price)
            except Exception:
                log.debug("price refresh failed for %s", ca[:8])
        _save_positions(self.open)

    async def check_exits(self) -> None:
        """Check all positions for exit conditions."""
        now = time.time()
        for ca, pos in list(self.open.items()):
            entry = pos["entry_price"]
            current = pos["last_price"]
            peak = pos["peak_price"]
            hold_s = now - pos["ts"]
            last_update = pos.get("last_price_update", pos["ts"])

            if entry <= 0 or current <= 0:
                continue

            # Minimum hold time: don't exit in first 60 seconds
            # (price data may be stale or volatile right after entry)
            if hold_s < 60:
                continue

            pnl_pct = (current / entry - 1.0) * 100.0
            from_peak = (current / peak - 1.0) * 100.0 if peak > 0 else 0

            reason = None

            # Rapid crash detector: >40% drop from peak in <2 minutes = rug
            if hold_s >= 60 and hold_s < 120 and from_peak < -40:
                reason = f"rapid_crash({from_peak:.1f}% in {hold_s:.0f}s)"
                log.warning(
                    "%s (%s) RAPID CRASH: %.1f%% from peak in %.0fs — rug?",
                    ca[:8], pos["sym"], from_peak, hold_s,
                )

            # Price staleness: if no price update in 5 minutes, force close
            stale_s = now - last_update
            if stale_s > 300:
                reason = f"stale_price({stale_s:.0f}s)"
                log.warning("%s (%s) price stale for %.0fs — forcing exit", ca[:8], pos["sym"], stale_s)

            # Hard stop (only after minimum hold)
            if not reason and pnl_pct <= self.hard_stop_pct:
                reason = f"hard_stop({pnl_pct:.1f}%)"

            # Trailing stop: activate after peak reaches trail_start_mult
            elif not reason and peak / entry >= self.trail_start_mult and from_peak <= self.trail_retrace_pct:
                reason = f"trail({from_peak:.1f}% from peak)"

            # Max hold time
            elif not reason and hold_s > self.max_hold_s:
                reason = f"max_hold({hold_s / 60:.0f}m)"

            # Take profit ladder (partial sells)
            elif not reason:
                tp_sold = pos.get("tp_sold", 0.0)
                for i, (tp_mult, sell_frac) in enumerate(self.tp_ladder):
                    if pnl_pct >= (tp_mult - 1) * 100:
                        cum_target = self._tp_cumulative[i]
                        if tp_sold < cum_target - 0.01:
                            # Partial sell: sell the fraction for this level
                            await self._partial_sell(ca, pos, i, reason=f"TP{tp_mult}x({pnl_pct:.1f}%)")
                            break

            if reason:
                await self._sell(ca, pos, reason)

    async def _partial_sell(self, ca: str, pos: dict, level: int, reason: str) -> None:
        """Execute a partial sell for a TP ladder level."""
        sym = pos["sym"]
        entry = pos["entry_price"]
        current = pos["last_price"]
        size = pos["size_sol"]
        tp_mult, sell_frac = self.tp_ladder[level]
        cum_target = self._tp_cumulative[level]
        tp_sold = pos.get("tp_sold", 0.0)

        # Calculate how much to sell at this level
        initial_tokens = pos.get("initial_token_amount", pos.get("token_amount", 0))
        remaining_frac = 1.0 - tp_sold
        sell_frac_of_remaining = sell_frac / remaining_frac if remaining_frac > 0 else sell_frac
        sell_tokens = int(initial_tokens * sell_frac_of_remaining)

        if sell_tokens <= 0:
            log.warning("partial_sell %s level %d: sell_tokens=0, skipping", ca[:8], level)
            return

        pnl_pct = (current / entry - 1.0) * 100.0 if entry > 0 else 0
        pnl_sol = size * sell_frac_of_remaining * pnl_pct / 100.0

        log.info(
            "PARTIAL SELL %s (%s) level=%d reason=%s pnl=%.1f%% sell_frac=%.2f tokens=%d",
            ca[:8], sym, level, reason, pnl_pct, sell_frac_of_remaining, sell_tokens,
        )

        # Execute sell
        if self.jupiter.live:
            try:
                sell_quote = await self.jupiter.quote_sell(ca, sell_tokens)
                if not sell_quote or not sell_quote.success:
                    log.warning(
                        "partial sell quote failed for %s (%s) — holding",
                        ca[:8], sell_quote.reason if sell_quote else "no route",
                    )
                    return
                await self.jupiter.sell(ca, sell_tokens)
            except Exception:
                log.exception("partial sell failed for %s", ca[:8])
                return
        else:
            sell_quote = await self.jupiter.quote_sell(ca, sell_tokens)
            if not sell_quote or not sell_quote.success:
                log.warning(
                    "paper partial sell quote failed for %s (%s) — holding",
                    ca[:8], sell_quote.reason if sell_quote else "no route",
                )
                return
            log.info("paper mode — tracking partial exit without executing")
            self.jupiter.paper_credit(pnl_sol)

        # Update position state
        new_tp_sold = min(tp_sold + sell_frac, 1.0)
        pos["tp_sold"] = new_tp_sold
        pos["token_amount"] = max(pos.get("token_amount", 0) - sell_tokens, 0)
        pos["banked_pnl"] = pos.get("banked_pnl", 0.0) + pnl_sol

        # Breakeven lock: after first TP, raise stop to entry
        if not pos.get("breakeven_locked") and new_tp_sold > 0:
            pos["breakeven_locked"] = True
            log.info("%s (%s) breakeven locked after TP%d", ca[:8], sym, level + 1)

        _save_positions(self.open)

        # Record this partial close
        trade = {
            "ca": ca,
            "sym": sym,
            "entry": entry,
            "exit": current,
            "pnl_sol": pnl_sol,
            "pnl_pct": pnl_pct,
            "size_sol": size * sell_frac_of_remaining,
            "hold_s": time.time() - pos["ts"],
            "reason": f"TP{level+1}_partial",
            "ts": time.time(),
        }
        self.closed.append(trade)
        _save_closed(self.closed)

        self.stats["sells"] += 1
        self.stats["total_pnl"] += pnl_sol
        if pnl_sol > 0:
            self.stats["wins"] += 1

        icon = "✅" if pnl_sol >= 0 else "❌"
        entry = pos.get("entry_price", 0)
        wins = self.stats["wins"]
        total = self.stats["sells"]
        wr = (wins / total * 100) if total else 0.0

        await self.notifier._send(
            f"{icon} **TP{level+1}** `{sym}`\n"
            f"📍 `{ca}`\n"
            f"📈 Entry `{self.notifier._fmt_px(entry)}`  →  📉 Now `{self.notifier._fmt_px(current)}`\n"
            f"💰 Sold `{sell_frac*100:.0f}%` · PnL `{pnl_pct:+.1f}%` (`{pnl_sol:+.4f} SOL`)\n"
            f"🏦 Banked `{pos['banked_pnl']:+.4f} SOL` · Remaining `{100-new_tp_sold*100:.0f}%`\n"
            f"🏆 WinRate `{wr:.0f}%` ({wins}/{total})"
        )

    async def _sell(self, ca: str, pos: dict, reason: str) -> None:
        """Sell remaining position (full close)."""
        sym = pos["sym"]
        entry = pos["entry_price"]
        current = pos["last_price"]
        size = pos["size_sol"]
        token_amount = pos.get("token_amount", 0)
        tp_sold = pos.get("tp_sold", 0.0)
        remaining_frac = 1.0 - tp_sold
        pnl_pct = (current / entry - 1.0) * 100.0 if entry > 0 else 0
        pnl_sol = size * remaining_frac * pnl_pct / 100.0

        log.info(
            "SELL %s (%s) reason=%s pnl=%.1f%% (%.4f SOL) remaining=%.0f%%",
            ca[:8], sym, reason, pnl_pct, pnl_sol, remaining_frac * 100,
        )

        # Get balance before sell
        balance_before = await self.jupiter.balance_sol()

        # Execute sell
        if token_amount > 0:
            try:
                if self.jupiter.live:
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
                    sell_quote = await self.jupiter.quote_sell(ca, token_amount)
                    if not sell_quote or not sell_quote.success:
                        log.warning(
                            "paper sell quote failed for %s (%s) — position held",
                            ca[:8],
                            sell_quote.reason if sell_quote else "no route",
                        )
                        return
                    log.info("paper mode — tracking exit without executing")
                    self.jupiter.paper_credit(size * remaining_frac + pnl_sol)
            except Exception:
                log.exception("sell failed for %s", ca[:8])

        # Get balance after sell
        balance_after = await self.jupiter.balance_sol()

        # Record closed trade
        banked = pos.get("banked_pnl", 0.0)
        total_pnl = banked + pnl_sol
        trade = {
            "ca": ca,
            "sym": sym,
            "entry": entry,
            "exit": current,
            "pnl_sol": total_pnl,
            "pnl_pct": pnl_pct,
            "size_sol": size,
            "hold_s": time.time() - pos["ts"],
            "reason": reason,
            "ts": time.time(),
        }
        self.closed.append(trade)
        _save_closed(self.closed)

        self.stats["sells"] += 1
        self.stats["total_pnl"] += total_pnl
        if total_pnl > 0:
            self.stats["wins"] += 1

        # Win rate
        wins = self.stats["wins"]
        total = self.stats["sells"]
        wr = (wins / total * 100) if total else 0.0

        # Remove from open
        del self.open[ca]
        _save_positions(self.open)

        # Map reason to send_close label
        base_reason = reason.split("(")[0]
        close_reason = {
            "hard_stop": "sl",
            "trail": "trail",
            "max_hold": "timeout",
            "stale_price": "timeout",
            "tp": "tp",
            "rapid_crash": "sl",
        }.get(base_reason, "sl")

        await self.notifier.send_close(
            ca=ca,
            name=sym,
            reason=close_reason,
            mult=current / entry if entry > 0 else 1.0,
            pnl_sol=total_pnl,
            hold_s=trade["hold_s"],
            entry_px=entry,
            exit_px=current,
            size_sol=size,
            balance_before=balance_before,
            balance_after=balance_after,
            open_count=self.n_open,
            max_positions=8,
            win_rate=wr,
            source="gmgnsignals",
        )

    def status(self) -> str:
        """Status card."""
        wins = self.stats["wins"]
        total = self.stats["sells"]
        wr = (wins / total * 100) if total else 0
        pnl = self.stats["total_pnl"]
        pnl_icon = "📈" if pnl >= 0 else "📉"
        wr_icon = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")

        lines = [
            f"━━━━━━━━━━━━━━━━━━━━━━",
            f"🤖 **TG Trader** · {self.n_open} position{'s' if self.n_open != 1 else ''}",
            f"━━━━━━━━━━━━━━━━━━━━━━",
            f"{wr_icon} WinRate `{wr:.0f}%`  ({wins}W / {total-wins}L)",
            f"{pnl_icon} PnL `{pnl:+.4f} SOL`",
            "",
        ]
        for ca, pos in self.open.items():
            entry = pos["entry_price"]
            current = pos["last_price"]
            pnl_pct = (current / entry - 1.0) * 100 if entry > 0 else 0
            hold = (time.time() - pos["ts"]) / 60
            tp_pct = pos.get("tp_sold", 0.0) * 100
            be = " 🔒" if pos.get("breakeven_locked") else ""
            age_s = time.time() - pos.get("last_price_update", pos["ts"])
            stale = " ⚠️" if age_s > 120 else ""
            pnl_color = "🟢" if pnl_pct >= 0 else "🔴"
            lines.append(
                f"  {pnl_color} `{pos['sym']}` · {pnl_pct:+.1f}% · {hold:.0f}m · TP{tp_pct:.0f}%{be}{stale}"
            )
        lines.append("━━━━━━━━━━━━━━━━━━━━━━")
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

    # RugCheck safety filter (fail-open: errors = allow)
    rugcheck_client = None
    if s.rug_check_api_key:
        rugcheck_client = RugCheckClient(
            api_key=s.rug_check_api_key,
            base_url=s.rug_check_base_url,
            max_score=s.rug_check_max_score,
            reject_danger=s.rug_check_reject_danger,
        )
        log.info("rugcheck: enabled (max_score=%d, reject_danger=%s)",
                 s.rug_check_max_score, s.rug_check_reject_danger)
    else:
        log.info("rugcheck: disabled (no API key)")

    pm = PositionManager(
        ds=ds,
        jupiter=jupiter,
        notifier=notifier,
        size_sol=s.size_sol,
        hard_stop_pct=s.hard_stop_pct,
        tp_ladder=s.tp_ladder,
        max_hold_s=s.max_hold_h * 3600,
        trail_retrace_pct=s.trail_retrace_pct,
        trail_start_mult=s.trail_start_mult,
        be_buffer_pct=s.be_buffer_pct,
    )

    log.info("positions loaded: %d open", pm.n_open)
    journal("start", positions=pm.n_open, dry_run=s.dry_run)

    # ── signal handler ──
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_, stop.set)

    # ── health monitoring ──
    health = {
        "started_at": time.time(),
        "signals_received": 0,
        "signals_filtered": 0,
        "buys_attempted": 0,
        "buys_ok": 0,
        "errors": 0,
        "tg_restarts": 0,
        "last_signal_at": None,
    }

    # ── TG signal feed ──
    async def on_signal(ca: str, sym: str, usd: str, score: float, wallets: list[str], tg_liq: float = 0.0):
        """Called by TgSignalFeed for each qualifying signal."""
        health["signals_received"] += 1
        health["last_signal_at"] = time.time()

        # score is always 3.0 from TG feed, use it to fetch fresh DexScreener data
        try:
            snap = await ds.token_pairs("solana", ca)
        except Exception:
            snap = None

        if not snap or not snap.get("price_usd"):
            log.info("signal %s (%s) — no DexScreener data, skipping", ca[:8], sym)
            health["signals_filtered"] += 1
            return

        price = float(snap.get("price_usd") or 0)
        mc = float(snap.get("mcap") or 0)
        liq = float(snap.get("liq") or 0)
        pc = snap.get("price_change") or {}
        h1 = pc.get("h1", 0) or 0
        m5 = pc.get("m5", 0) or 0

        # Use TG signal liquidity as fallback if DexScreener shows 0
        if liq <= 0 and tg_liq > 0:
            liq = tg_liq
            log.info("signal %s (%s) using TG liquidity=$%.0f (DexScreener=0)", ca[:8], sym, tg_liq)

        # Additional gates on live data
        if mc < s.tg_min_mc:
            log.info("signal %s (%s) mc=$%.0f < $%.0f — skip", ca[:8], sym, mc, s.tg_min_mc)
            health["signals_filtered"] += 1
            return
        if liq < s.tg_min_liq:
            log.info("signal %s (%s) liq=$%.0f < $%.0f — skip", ca[:8], sym, liq, s.tg_min_liq)
            health["signals_filtered"] += 1
            return

        # Momentum: reject if dumping hard
        if h1 < -15:
            log.info("signal %s (%s) h1=%.1f%% dumping — skip", ca[:8], sym, h1)
            health["signals_filtered"] += 1
            return

        # Volume confirmation: require some recent activity
        vol_h1 = float(snap.get("vol_h1") or 0)
        if vol_h1 < 100:
            log.info("signal %s (%s) vol_h1=$%.0f too low — skip", ca[:8], sym, vol_h1)
            health["signals_filtered"] += 1
            return

        # DexPaprika cross-validation (optional, best-effort)
        dp_data = await _validate_token_dexpaprika(ca)
        if dp_data:
            is_rug, rug_reason = _check_rug_signals(snap, dp_data)
            if is_rug:
                log.warning("signal %s (%s) DEXPAPRIKA rug detected: %s — skip", ca[:8], sym, rug_reason)
                health["signals_filtered"] += 1
                return

        # RugCheck safety gate (fail-open: errors = allow)
        if rugcheck_client is not None:
            rc_result = await rugcheck_client.check(ca)
            if not rugcheck_client.is_safe(rc_result):
                reason = rc_result.summary() if rc_result else "error"
                log.warning("signal %s (%s) RUGCHECK rejected: %s — skip", ca[:8], sym, reason)
                health["signals_filtered"] += 1
                return

        log.info(
            "signal %s (%s) mc=$%.0f liq=$%.0f price=$%.8f h1=%+.1f%% m5=%+.1f%% — BUYING",
            ca[:8], sym, mc, liq, price, h1, m5,
        )
        journal("signal", ca=ca, sym=sym, mc=mc, liq=liq, price=price, h1=h1, m5=m5)

        health["buys_attempted"] += 1
        success = await pm.open_position(ca, sym, price, mc, liq)
        if success:
            health["buys_ok"] += 1
            journal("buy", ca=ca, sym=sym, price=price, mc=mc, size=pm._position_size_sol())

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

    # ── health/status loop ──
    async def status_loop():
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                break

            # Periodic health report
            uptime_h = (time.time() - health["started_at"]) / 3600
            log.info(
                "\n%s\n uptime=%.1fh signals=%d filtered=%d buys=%d/%d errors=%d tg_restarts=%d",
                pm.status(),
                uptime_h,
                health["signals_received"],
                health["signals_filtered"],
                health["buys_ok"],
                health["buys_attempted"],
                health["errors"],
                health["tg_restarts"],
            )

            # Health check: if no signals in 30 minutes, warn
            if health["last_signal_at"] > 0:
                silence_min = (time.time() - health["last_signal_at"]) / 60
                if silence_min > 30:
                    log.warning("no signals for %.0f minutes — channel may be quiet", silence_min)

    status_task = asyncio.create_task(status_loop())
    status_task.add_done_callback(_log_task_result)

    # ── main loop: refresh prices + check exits ──
    try:
        log.info("main: sending startup notification")
        balance = await jupiter.balance_sol()
        bal_str = f"`{balance:.4f} SOL`" if balance else "`?`"
        await notifier._send(
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 **TG Trader Started**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📡 Channel `@{s.tg_signal_channel}`\n"
            f"💵 Size `{s.size_sol} SOL`\n"
            f"🛑 Hard Stop `{s.hard_stop_pct}%`\n"
            f"🎯 TP Ladder `{s.tp_ladder}`\n"
            f"💼 Balance {bal_str}\n"
            f"📊 Mode `{'LIVE' if not s.dry_run else 'PAPER'}`\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
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
                health["tg_restarts"] += 1
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
                journal("tg_restart", count=health["tg_restarts"])
            try:
                now = time.time()
                if now - last_refresh >= refresh_interval:
                    await pm.refresh_prices()
                    health["last_price_refresh"] = now
                    last_refresh = now
                await pm.check_exits()
            except Exception:
                health["errors"] += 1
                log.exception("position management error")

    finally:
        status_task.cancel()
        if tg_feed is not None:
            tg_feed.stop()
        await jupiter.close()
        await ds.close()

        # Final health report
        uptime_h = (time.time() - health["started_at"]) / 3600
        journal(
            "stop",
            uptime_h=uptime_h,
            signals=health["signals_received"],
            filtered=health["signals_filtered"],
            buys=health["buys_ok"],
            errors=health["errors"],
            tg_restarts=health["tg_restarts"],
        )

        log.info("tg-trade stopped. Final: %s", pm.status())
        log.info(
            "Health: uptime=%.1fh signals=%d filtered=%d buys=%d/%d errors=%d tg_restarts=%d",
            uptime_h,
            health["signals_received"],
            health["signals_filtered"],
            health["buys_ok"],
            health["buys_attempted"],
            health["errors"],
            health["tg_restarts"],
        )

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
