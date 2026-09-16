"""Smart-money watcher — the only strategy.

    uv run main.py watch          # 24/7: alerts + shadow paper book + status
    uv run main.py status         # one-shot status card

Pipeline:
  Helius WS + PumpAPI firehose + CabalSpy streams
    → smart wallet bought something new
      → 🕵️/🔥 Telegram alert
      → shadow paper position opens at Jupiter executable price
        → TP ladder / trail / hard stop managed virtually → paper PnL stats
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

_SOL_MINT = "So11111111111111111111111111111111111111112"  # WSOL

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import os

import base58
from solders.keypair import Keypair

import config as cfg
import logs
from birdeye import BirdeyeClient
from cabalspy import CabalSpyClient, HolderCache
from cabalspy_rest import CabalSpyREST
from dbotx import DBotXClient
from dexpaprika import DexPaprikaClient
from dexscreener_oracle import DexScreenerClient
from helius_ws import HeliusWS
from jupiter_trade import JupiterSwap
from kolexplorer import KolexplorerFeed
from logs import setup_logging
from madeonsol import MadeOnSolClient
from notifier import TelegramNotifier
from pair_perf import load as load_pair_perf
from pair_perf import pair_multiplier
from pair_perf import save as save_pair_perf
from pair_perf import update as update_pair_perf
from pump_stream import PumpApiStream
from rugcheck import RugCheckClient
from tg_signal_feed import (
    TgSignalFeed,
    memetracker_chase_blocked,
    parse_memetracker_signal,
)
from wallet_discovery import WalletDiscovery
from wallet_weights import build_weights
from watcher import SmartWalletWatcher

log = logging.getLogger("main")


def _log_task_result(task: asyncio.Task) -> None:
    """Log (not swallow) any exception from a background task so a crash shows.

    Fire-and-forget ``create_task`` calls lose their exception unless someone
    retrieves ``task.result()``; without this a crashed task is invisible.
    """
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        log.exception("background task %s failed", task.get_name())


# ------------------------------------------------------------------ status --
def build_status(st: dict) -> str:
    """Compact markdown status card from watcher/shadow state dict."""
    up = st.get("uptime_s", 0)
    h, rem = divmod(int(up), 3600)
    m = rem // 60
    uptime = f"{h}h {m:02d}m" if h else f"{m}m"
    open_pos = st.get("open", [])
    closed = st.get("closed", [])
    # oracle_fail closes are infra failures (both pricers down, e.g. Jupiter
    # 429 storm on 2026-09-14: -0.0395 booked as a full loss at mult=0.0).
    # They are not trading losses — exclude from strategy PnL/win-rate.
    strat = [c for c in closed if not c.get("oracle_fail")]
    infra_n = len(closed) - len(strat)
    wins = sum(1 for c in strat if c.get("pnl_sol", 0) > 0)
    # Closed PnL + unrealized PnL on open positions.
    # Open positions store entry_usd/last_usd but not pnl_sol, so compute
    # the unrealized multiple from price change.
    open_pnl = 0.0
    start = st.get("start_balance_sol", 0.0)
    for o in open_pos:
        entry = o.get("entry_usd") or 0.0
        last = o.get("last_usd") or entry
        size = o.get("size_sol") or 0.0
        remaining = o.get("remaining", 1.0)
        banked = o.get("banked_pnl", 0.0)
        if entry > 0 and size > 0:
            open_pnl += banked + remaining * size * (last / entry - 1.0)
    pnl = sum(c.get("pnl_sol", 0) for c in strat) + open_pnl
    pct = (pnl / start * 100.0) if start else 0.0
    icon = "🟢" if pnl >= 0 else "🔴"

    lines = [
        f"🕵️ **Smart-Watch** · {uptime}",
        (f"👁️ {st.get('wallets', 0)} wallets · "
         + f"🚨 {st.get('alerts', 0)} alerts (🔥{st.get('consensus', 0)})"),
        "",
        "📊 **Shadow book**",
        f"▸ Open: {len(open_pos)}",
    ]
    for o in open_pos[:4]:
        entry = o.get("entry_usd") or 0.0
        last = o.get("last_usd") or entry
        mult = last / entry if entry else 1.0
        lines.append(f"   `{o.get('symbol','?')}` {mult:.2f}x "
                     f"({o.get('banked_pnl',0):+.4f})")
    wr = (wins / len(strat) * 100) if strat else 0.0
    lines.append(f"▸ Closed: {len(strat)} · win {wr:.0f}%")
    if infra_n:
        lines[-1] += f" (+{infra_n} infra excluded)"
    lines.append(f"{icon} **PnL `{pnl:+.4f}` SOL ({pct:+.1f}%)**")
    feeds = st.get("feeds") or {}

    def _feed_icon(v) -> str:
        # "degraded" = quota-side Helius 429 ban: feed alive, provider
        # throttling. 🟡 so the status card stops crying 🔴 for hours
        # over something we can't fix from here. "off" = deliberately
        # disabled in config (HELIUS_WS_ENABLED=false) — ⚪, not 🔴.
        if v == "degraded":
            return "🟡"
        if v == "off":
            return "⚪"
        return "🟢" if v else "🔴"

    feed_line = " · ".join(
        f"{_feed_icon(v)} {name}" for name, v in feeds.items())
    if feed_line:
        lines.append(feed_line)
    return "\n".join(lines)


def blind_open_size(adaptive_size: float | None, cap: float, fallback: float) -> float:
    """Cap position size for liq-unchecked (blind) opens.

    Blind entries (no DexScreener/DexPaprika snapshot, pc={}) carried every
    catastrophic paper loss 2026-09-12..15 while scoring no better than
    confirmed entries — so they trade at capped risk, never full adaptive
    size. Non-positive cap disables the cap (returns the adaptive size).
    """
    size = adaptive_size if adaptive_size and adaptive_size > 0 else fallback
    if cap and cap > 0:
        size = min(size, cap)
    return round(size, 4)


async def memetracker_executable(jupiter, ca: str, size_lamports: int,
                                 max_impact_pct: float) -> str | None:
    """Executability guard for signal-price (MemeTracker) entries.

    Returns None when the token can actually be traded at size, else a
    ``skip:...`` reason. Requires a live Jupiter buy quote within impact
    limits plus a working sell quote — the same standard the
    Jupiter-routed path enforces in ``ShadowBook.open_position``.

    Paper 2026-09-12..16: signal-price entries bypassed every Jupiter gate
    and the whole catastrophic left tail of memetracker (-0.14/9, exits at
    0.40-0.89x within 0-4 min) was tokens that were never sellable at
    entry. Unmigrated bonding-curve tokens (no Jupiter route) sit out.
    """
    try:
        bq = await jupiter.quote(ca, size_lamports, force=True)
    except Exception:
        return "skip:no_buy_route(quote_exception)"
    if bq is None or not bq.success:
        return f"skip:no_buy_route({bq.reason if bq else 'quote_exception'})"
    if max_impact_pct > 0 and bq.price_impact_pct > max_impact_pct:
        return (f"skip:impact({bq.price_impact_pct:.2f}%"
                f">{max_impact_pct}%)")
    try:
        sq = await jupiter.quote_sell(ca, bq.output_amount)
    except Exception:
        return "skip:unsellable(quote_exception)"
    if sq is None or not sq.success:
        return f"skip:unsellable({sq.reason if sq else 'quote_exception'})"
    return None


# ------------------------------------------------------------- shadow book --
class ShadowBook:
    """Virtual positions mirroring 'buy what smart money buys'.

    Entries and exits are priced by Jupiter executable quotes (impact +
    slippage included) when available; DexScreener mid is the fallback.
    Peak tracking uses only executable prices so TP/trail can only fire
    at prices the bot could actually exit at.
    """

    def __init__(self, ds: DexScreenerClient, size_sol: float,
                 retrace_pct: float, hard_stop_pct: float,
                 state_file: Path, start_balance_sol: float,
                 jupiter=None, notifier=None, max_positions: int = 12,
                  tp1_mult: float = 1.5, trail_start_mult: float = 1.3,
                   be_buffer: float = 0.0, max_hold_s: float = 0.0,
                   tp_ladder: list | None = None,
                   be_arm_mult: float = 1.15,
                   flat_timeout_s: float = 0.0, flat_timeout_peak: float = 1.10,
                   trail_enabled: bool = False,
                  on_trade_close=None,
                  open_max_impact_pct: float = 4.0,
                 early_filter_window_s: float = 30.0,
                 early_filter_dd_pct: float = 20.0,
                 early_filter_gain_pct: float = 5.0,
                 reentry_cooldown_s: float = 3600.0,
                 dexpaprika=None) -> None:
        self.jupiter = jupiter
        self.notifier = notifier
        self.max_positions = int(max_positions)
        self.ds = ds
        self.size_sol = float(size_sol)
        self.retrace = float(retrace_pct)
        self.hard_stop = float(hard_stop_pct)
        self.tp1_mult = float(tp1_mult)
        self.trail_start_mult = float(trail_start_mult)
        self.be_buffer = float(be_buffer)
        self.max_hold_s = float(max_hold_s)
        # Take-profit ladder: list of (price_multiple, frac_of_original_size). If
        # None, fall back to the legacy single-TP behaviour (tp1_mult bank 50%).
        # Each level banks `frac` of the ORIGINAL position (not remaining).
        self.tp_ladder = tp_ladder or [(1.5, 0.30, 0.15)]
        self.be_arm_mult = float(be_arm_mult)
        self.flat_timeout_s = float(flat_timeout_s)
        self.flat_timeout_peak = float(flat_timeout_peak)
        self.trail_enabled = bool(trail_enabled)
        self.on_trade_close = on_trade_close  # async/normal fn(wallets, win: bool, pnl: float)
        self.open_max_impact_pct = float(open_max_impact_pct)
        self.early_filter_window_s = float(early_filter_window_s)
        self.early_filter_dd = float(early_filter_dd_pct) / 100.0  # store as fraction
        self.early_filter_gain = float(early_filter_gain_pct) / 100.0  # store as fraction
        self.dexpaprika = dexpaprika
        self.state_file = state_file
        self.start_balance_sol = float(start_balance_sol)
        self.balance_sol = float(start_balance_sol)
        self.open: dict[str, dict] = {}
        self.closed: list[dict] = []
        self._cooldown: dict[str, float] = {}  # ca -> expiry timestamp
        self.reentry_cooldown_s = float(reentry_cooldown_s)
        self._lock = asyncio.Lock()
        self._load()

    def _win_rate(self) -> float:
        strat = [c for c in self.closed if not c.get("oracle_fail")]
        if not strat:
            return 0.0
        wins = sum(1 for c in strat if c.get("pnl_sol", 0.0) > 0.0)
        return wins / len(strat) * 100.0

    def _load(self) -> None:
        if not self.state_file.exists():
            return
        try:
            d = json.loads(self.state_file.read_text())
            self.open = d.get("open", {})
            self.closed = d.get("closed", [])
            self.start_balance_sol = d.get("start_balance_sol",
                                           self.start_balance_sol)
            # Restore persisted balance if available (includes realized PnL).
            # Fallback: derive from start_balance - locked (loses realized PnL
            # on upgrade; next save fixes it).
            if "balance_sol" in d:
                self.balance_sol = float(d["balance_sol"])
            else:
                locked = sum(p.get("size_sol", 0.0) for p in self.open.values())
                self.balance_sol = max(0.0, self.start_balance_sol - locked)
            # Seed the re-entry cooldown from persisted closes: _cooldown is
            # in-memory only, so without this a restart would allow instant
            # re-entry into a just-closed token. Fail-safe direction (a
            # token closed long before the restart is held one extra
            # cooldown at most).
            _now = time.time()
            for c in self.closed[-100:]:
                _ca = c.get("ca")
                if _ca and _ca not in self._cooldown:
                    self._cooldown[_ca] = _now + self.reentry_cooldown_s
        except Exception:
            log.exception("shadow book load failed")

    def save(self) -> None:
        try:
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps({
                "open": self.open, "closed": self.closed,
                "start_balance_sol": self.start_balance_sol,
                "balance_sol": self.balance_sol}, indent=1))
            os.replace(str(tmp), str(self.state_file))
        except Exception:
            log.exception("shadow book save failed")

    async def reconcile_balances(self) -> None:
        """Validate restored open positions against on-chain token balances.

        Called once at startup after the Jupiter RPC client is available.
        Positions with tokens_raw == 0 are moved to stuck (can't sell).
        Positions whose on-chain balance is zero are closed as losses.
        """
        if self.jupiter is None:
            return
        async with self._lock:
            pending = list(self.open)
        if not pending:
            return
        # Fetch balances WITHOUT the lock: RPC is slow and holding the lock
        # across it would block the signal path (same fix as refresh_prices).
        balances: dict[str, int | None] = {}
        for ca in pending:
            try:
                balances[ca] = await self.jupiter.token_balance(ca)
            except Exception:
                log.exception("reconcile balance fetch failed %s", ca[:10])
                balances[ca] = None
        async with self._lock:
            removed = []
            for ca in pending:
                if ca not in self.open:
                    continue
                pos = self.open[ca]
                real_balance = balances[ca]
                if real_balance is None:
                    # RPC failed — keep position as-is, will reconcile later
                    continue
                stored = pos.get("tokens_raw", 0)
                if stored == 0 and real_balance > 0:
                    # Fix: we have tokens on-chain but didn't capture amount
                    pos["tokens_raw"] = real_balance
                    log.info("reconcile %s: fixed tokens_raw 0 → %d", ca[:10], real_balance)
                elif real_balance == 0:
                    # Tokens gone — close as loss
                    log.warning("reconcile %s: tokens gone on-chain, closing", ca[:10])
                    removed.append(ca)
            for ca in removed:
                pos = self.open.pop(ca)
                pos["pnl_sol"] = -pos.get("size_sol", 0.0)
                pos["exit_reason"] = "reconcile:sold_offchain"
                self.closed.append(pos)
                if self.on_trade_close:
                    try:
                        self.on_trade_close(pos.get("wallets", []), False, pos["pnl_sol"])
                    except Exception:
                        log.exception("on_trade_close reconcile callback failed")
            if removed:
                self.save()

    async def _sol_usd(self) -> float:
        """Current SOL price in USD, used to derive a USD entry from a SOL quote."""
        try:
            s = await self.ds.token_pairs("solana", _SOL_MINT)
            return float(s.get("price_usd") or 0) if s else 0.0
        except Exception:
            return 0.0

    async def open_position(self, ca: str, symbol: str, usd_entry: float,
                            trigger_usd: float, n_wallets: int,
                            wallets: list[str] | None = None,
                            size_sol: float | None = None,
                            source: str = "pumpapi",
                            mc: float = 0.0,
                            score: float = 0.0,
                            signal_price: float = 0.0) -> None:
        # --- Jupiter executable entry basis (primary) ---
        # When Jupiter is available, derive the actual entry price from the buy
        # quote: size_sol SOL -> tokens_raw, so entry = SOL_per_token * SOL_USD.
        # This is the price the paper book would really pay, including impact +
        # slippage — asymmetric pricing (DexScreener mid in, Jupiter out) was
        # distorting paper PnL.
        tokens_raw = 0
        entry_note = "no_jupiter"
        entry_mode = "mark_only"
        exec_px = 0.0  # Jupiter-derived USD/token (authoritative when available)
        market_px = 0.0  # DexScreener mid (reference only)
        _size = size_sol if size_sol is not None else self.size_sol

        # Signal price bypass: when entry price comes from the signal itself
        # (e.g. MemeTracker TG message), skip Jupiter + DexScreener entirely.
        if signal_price > 0:
            px = signal_price
            snap = None
            entry_note = "signal_price"
            entry_mode = "signal"
        else:
            # DexScreener snapshot: used for market context (liq, price_change) and
            # as fallback when Jupiter is unavailable.
            snap = await self.ds.token_pairs("solana", ca)
            # DexPaprika fallback: richer Solana data when DexScreener fails
            if not snap and self.dexpaprika is not None:
                try:
                    snap = await self.dexpaprika.get_token_details(ca)
                except Exception as exc:
                    log.debug("dexpaprika fallback failed for %s: %s", ca[:10], exc)
            market_px = float(snap.get("price_usd") or 0) if snap else 0.0

            if self.jupiter is not None:
                q = await self.jupiter.quote(ca, int(_size * 1e9),
                                             force=True)
                # PumpAPI bonding-curve entry bypasses every Jupiter-only
                # gate below (impact sampling, stability, sell quote,
                # executable pricing): those need a valid Jupiter quote and
                # previously ran anyway on the failed `q`, causing a second
                # buy_via_pumpapi call + a guaranteed
                # `stability_no_quote:bad_base` skip. No-route PumpAPI
                # entries never opened a position.
                _pump_entry = False
                if q is None or not q.success:
                    reason = q.reason if q else "quote_exception"
                    # PumpAPI entries are priced from the DexScreener mid
                    # (px = market_px below) — without a market price the
                    # fallback buy is wasted and always ends in a
                    # `no_price` skip (observed: misleading "PAPER buy via
                    # pumpapi … shadow skip no price" pairs). Skip it early;
                    # Jupiter executable entries (exec_px) don't need the
                    # mid so they are unaffected — this guard only skips
                    # the PumpAPI attempt.
                    if (signal_price <= 0 and market_px <= 0
                            and self.jupiter._pumpapi_enabled()):
                        logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                     reason=f"no_price:{reason}")
                        log.info("shadow skip %s (%s): no price (%s)",
                                 ca[:10], symbol, reason)
                        return
                    # PumpAPI fallback: try bonding curve buy for non-migrated tokens
                    if self.jupiter._pumpapi_enabled():
                        log.info("Jupiter no route for %s (%s), trying PumpAPI", ca[:10], symbol)
                        p_res = await self.jupiter.buy_via_pumpapi(ca, _size)
                        if p_res.success:
                            tokens_raw = 0  # PumpAPI doesn't return token amount
                            entry_note = "pumpapi_fallback"
                            entry_mode = "pumpapi"
                            px = signal_price if signal_price > 0 else market_px
                            _pump_entry = True
                        else:
                            logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                         reason=f"no_buy_route:{reason}:pumpapi_failed:{p_res.error}")
                            log.info("shadow skip %s (%s): PumpAPI also failed: %s", ca[:10], symbol, p_res.error)
                            return
                    else:
                        logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                     reason=f"no_buy_route:{reason}")
                        log.info("shadow skip %s (%s): no buy route: %s", ca[:10], symbol, reason)
                        return
                if not _pump_entry:
                    tokens_raw = q.output_amount
                    entry_note = f"jup impact={q.price_impact_pct:.2f}%"
                    if tokens_raw <= 0:
                        # Zero-output route (route exists but unusable) — skip
                        # before the stability gate so the reason is explicit
                        # instead of a generic `bad_base`.
                        logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                     reason=f"zero_output:{q.reason}",
                                     impact=round(q.price_impact_pct, 2))
                        log.info("shadow skip %s (%s): zero output (%s)",
                                 ca[:10], symbol, q.reason)
                        return
                    if self.open_max_impact_pct > 0 and q.price_impact_pct > self.open_max_impact_pct:
                        # PumpAPI fallback: try bonding curve when Jupiter impact is too high
                        # (same no-price guard as the no-route branch above:
                        # pump entries need the DexScreener mid for pricing).
                        if signal_price <= 0 and market_px <= 0:
                            logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                         reason=f"no_price:impact{q.price_impact_pct:.2f}%")
                            log.info("shadow skip %s (%s): impact %.2f%%, no price",
                                     ca[:10], symbol, q.price_impact_pct)
                            return
                        if self.jupiter._pumpapi_enabled():
                            log.info("Jupiter impact %.2f%% too high for %s (%s), trying PumpAPI",
                                     q.price_impact_pct, ca[:10], symbol)
                            p_res = await self.jupiter.buy_via_pumpapi(ca, _size)
                            if p_res.success:
                                tokens_raw = 0
                                entry_note = "pumpapi_impact_fallback"
                                entry_mode = "pumpapi"
                                px = signal_price if signal_price > 0 else market_px
                                _pump_entry = True
                            else:
                                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                             reason=f"impact{q.price_impact_pct:.2f}%:pumpapi_failed:{p_res.error}")
                                log.info("shadow skip %s (%s): impact %.2f%% and PumpAPI failed: %s",
                                         ca[:10], symbol, q.price_impact_pct, p_res.error)
                                return
                        else:
                            logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                         reason=f"untradable:impact{q.price_impact_pct:.2f}%")
                            log.info("shadow skip %s (%s): impact %.2f%%",
                                     ca[:10], symbol, q.price_impact_pct)
                            return
                if not _pump_entry:
                    if self.jupiter.quote_stability_checks > 0:
                        buy_slip = None if self.jupiter._buy_rtse else self.jupiter._slippage_bps
                        stable, stab_reason, stab_info = await self.jupiter.check_quote_stability(
                            ca, int(_size * 1e9), base=q, slippage_bps=buy_slip)
                        if not stable:
                            logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                         reason=f"unstable:{stab_reason}", info=stab_info)
                            # Include base diagnostics (now in stab_info) so
                            # `bad_base` lines name the upstream cause.
                            _detail = (f" {stab_info}" if stab_info else "")
                            log.info("shadow skip %s (%s): %s%s", ca[:10], symbol, stab_reason, _detail)
                            return
                    sq = await self.jupiter.quote_sell(ca, tokens_raw)
                    if sq is None or not sq.success:
                        reason = sq.reason if sq else "quote_exception"
                        logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                     reason=f"unsellable:{reason}")
                        log.info("shadow skip %s (%s): unsellable %s", ca[:10], symbol, reason)
                        return
                    entry_mode = "executable"
                    # Derive executable entry from the Jupiter buy quote:
                    # size_sol SOL spent, tokens_raw received, SOL price in USD.
                    dec = await self.jupiter.token_decimals(ca) or 6
                    sol_usd = await self._sol_usd()
                    if sol_usd and tokens_raw:
                        exec_px = (_size * sol_usd) / (tokens_raw / (10 ** dec))
            # Use Jupiter executable price as canonical entry when available;
            # fall back to DexScreener mid only when Jupiter is absent.
            # (PumpAPI entries already set px above; exec_px is 0 there.)
            px = exec_px if exec_px > 0 else market_px
        if px <= 0:
            logs.journal("shadow_skip", ca=ca, symbol=symbol, reason="no_price")
            log.info("shadow skip %s (%s): no price", ca[:10], symbol)
            return
        # Simulated wallet: deploy size_sol on open. Skip if it would
        # over-leverage the tracked balance (can't open what we can't fund).
        # The mutation is locked so an in-flight refresh_prices() (running in
        # the main loop) can never interleave and double-count balance/positions.
        async with self._lock:
            if len(self.open) >= self.max_positions:
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason="max_positions")
                log.info("shadow skip %s (%s): max positions %d reached",
                         ca[:10], symbol, self.max_positions)
                return
            if self.balance_sol < _size:
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason="insufficient_balance")
                log.info("shadow skip %s (%s): insufficient balance %.4f",
                          ca[:10], symbol, self.balance_sol)
                return
            bal_before = self.balance_sol
            self.balance_sol -= _size
            self.open[ca] = {
                "symbol": symbol, "entry_usd": px, "peak_usd": px, "last_usd": px,
                "market_entry_px": market_px, "tokens_raw": tokens_raw,
                "entry_note": entry_note,
                "size_sol": _size, "ts": time.time(),
                "trigger_usd": trigger_usd, "n_wallets": n_wallets,
                "wallets": list(wallets or []),
                "tp_taken": [], "remaining": 1.0, "banked_pnl": 0.0,
                "be_armed": False, "peak_mult": 1.0, "source": source,
                "tp_level": -1,  # index into tp_ladder (-1 = no TP yet)
                "entry_mode": entry_mode,
                # Early adverse filter state (one-shot at early_filter_window_s)
                "early_min_mult": 1.0,  # worst excursion during early window
                "early_max_mult": 1.0,  # best excursion during early window
                "early_checked": False,  # True after filter evaluated
            }
            logs.journal("shadow_entry_px", ca=ca, px=px, note=entry_note)
            logs.journal("shadow_open", ca=ca, symbol=symbol, entry_usd=px,
                         trigger=trigger_usd, n=n_wallets,
                         wallets=list(wallets or []), source=source,
                         mc=mc, score=score)
            self.save()
        if self.notifier is not None:
            try:
                asyncio.get_running_loop().create_task(self.notifier.send_open(
                    ca=ca, name=symbol, price=px, size_sol=_size,
                    balance_before=bal_before, balance_after=self.balance_sol,
                    open_count=len(self.open), max_positions=self.max_positions,
                    n_wallets=n_wallets, trigger_usd=trigger_usd,
                    win_rate=self._win_rate(), wallets=wallets))
            except Exception:
                log.exception("send_open failed")

    async def refresh_prices(self) -> None:
        # Snapshot the scan order under the lock, then release it: quotes
        # are slow (Jupiter/DexScreener RPC) and holding the lock across
        # the whole scan blocked open_position (signal path) behind every
        # quote, risking the 150s hung-signal watchdog during outages.
        # Structural mutations (close/balance/save) stay locked inside
        # _refresh_one; per-position fields are only written by this loop,
        # so touching them unlocked cannot race with open_position (which
        # only adds new keys under the lock).
        async with self._lock:
            # Process fresh positions (< early_filter_window_s) first so the
            # one-shot filter fires with minimal latency.  Mature positions
            # follow; their 5s poll cadence is already generous.
            now = time.time()
            fresh, mature = [], []
            for ca in list(self.open):
                pos = self.open[ca]
                age = now - pos.get("ts", now)
                if age < self.early_filter_window_s and not pos.get("early_checked", False):
                    fresh.append(ca)
                else:
                    mature.append(ca)
            ordered = fresh + mature
        for ca in ordered:
            try:
                await self._refresh_one(ca)
            except Exception:
                log.exception("refresh %s failed this cycle; continuing", ca[:10])
        async with self._lock:
            self.save()

    async def _refresh_one(self, ca: str) -> None:
        """Refresh one position: unlocked fetch/compute, locked close."""
        pos = self.open.get(ca)
        if pos is None:
            return
        entry = pos["entry_usd"]
        # --- price discovery: prefer Jupiter executable sell quote
        # (authoritative for what we'd actually get on exit); fall
        # back to DexScreener mid only when Jupiter is unavailable.
        jup_mult = None
        dex_mult = None
        # Transient Jupiter failures (gateway 429/timeout/5xx) are infra, not
        # dead liquidity: they must NOT advance the dead-quote counter (seen
        # 2026-09-14: a 429 storm force-closed FwEm… at mult=0.0/-100%).
        # Only a genuine no-route counts toward the zombie grace limit.
        _TRANSIENT_QUOTE_REASONS = {
            "quote_rate_limited", "quote_timeout",
            "quote_http_error", "quote_exception",
        }
        if self.jupiter is not None and pos.get("tokens_raw"):
            remaining_raw = int(pos["tokens_raw"] * pos.get("remaining", 1.0))
            if remaining_raw > 0:
                try:
                    sq = await self.jupiter.quote_sell(ca, remaining_raw)
                    if sq is not None and sq.success:
                        jup_mult = (sq.output_amount / 1e9) / \
                            (pos["size_sol"] * pos.get("remaining", 1.0))
                        pos["exit_note"] = f"jup impact={sq.price_impact_pct:.2f}%"
                        pos["_quote_fail_count"] = 0  # reset on success
                        pos["_transient_fail_count"] = 0
                    else:
                        _reason = (sq.reason if sq is not None else "quote_exception")
                        if _reason in _TRANSIENT_QUOTE_REASONS:
                            # Infra hiccup: hold the slot, keep managing via
                            # DexScreener below. Track separately for logs.
                            pos["_transient_fail_count"] = pos.get("_transient_fail_count", 0) + 1
                            _tfc = pos["_transient_fail_count"]
                            # Quiet cadence: refresh runs every 1s on fresh
                            # positions, so every-10 logged x10/x20 within ~20s
                            # during the 2026-09-14 gateway 429 storm. First
                            # hit + every 60 at INFO, the rest at DEBUG.
                            if _tfc == 1 or _tfc % 60 == 0:
                                log.info("jupiter transient %s (%s): %s x%d — holding via DexScreener",
                                         ca[:10], pos["symbol"], _reason,
                                         _tfc)
                            else:
                                log.debug("jupiter transient %s (%s): %s x%d — holding via DexScreener",
                                          ca[:10], pos["symbol"], _reason,
                                          _tfc)
                        else:
                            pos["_quote_fail_count"] = pos.get("_quote_fail_count", 0) + 1
                except Exception:
                    pos["_transient_fail_count"] = pos.get("_transient_fail_count", 0) + 1
                    log.exception("refresh jup quote failed %s", ca[:10])
        else:
            # No Jupiter or no tokens (e.g. PumpAPI bonding-curve entries with
            # tokens_raw==0): Jupiter can never price these by design, so do
            # NOT advance the dead counter here — DexScreener is the pricer.
            # The old code incremented every cycle and killed every PumpAPI
            # position as dead_liquidity after 10 refreshes.
            pass

        snap = await self.ds.token_pairs("solana", ca)
        if snap and snap.get("price_usd"):
            px = float(snap["price_usd"])
            dex_mult = px / entry if entry else 0
            # last_usd drives the status card: prefer the executable Jupiter
            # price (what we'd actually exit at) when available, so the
            # displayed multiple matches the exit logic. DexScreener mid is
            # the fallback.
            if jup_mult is not None and entry:
                pos["last_usd"] = entry * jup_mult
            else:
                pos["last_usd"] = px
        else:
            # DexScreener also dead — no pair found. Keep last_usd in sync
            # with Jupiter when that's all we have.
            dex_mult = None
            if jup_mult is not None and entry:
                pos["last_usd"] = entry * jup_mult

        # --- dead-liquidity force-close ---
        # If both Jupiter AND DexScreener have no route, the token is
        # dead (rug / drained pool).  Force-close after a short grace
        # period so we don't hold zombie slots forever.
        DEAD_QUOTE_LIMIT = 10   # consecutive no-route failures before force-close
        DEAD_LIQ_USD = 10.0     # DexScreener liq below this = dead pool
        is_dead = False
        if jup_mult is None and dex_mult is None:
            qfails = pos.get("_quote_fail_count", 0)
            if qfails >= DEAD_QUOTE_LIMIT:
                is_dead = True
                log.warning("dead liquidity %s (%s): %d consecutive quote failures, force-closing",
                            ca[:10], pos["symbol"], qfails)
        elif dex_mult is not None:
            # DexScreener returned but pool is essentially dead
            liq = (snap or {}).get("liq") or 0
            if 0 < liq < DEAD_LIQ_USD:
                is_dead = True
                log.warning("dead pool %s (%s): liq=$%.0f < $%d, force-closing",
                            ca[:10], pos["symbol"], liq, DEAD_LIQ_USD)
        # NOTE (2026-09-14): the old `jupiter_down_force_close` block was here —
        # any position with >=3 Jupiter misses was killed even with a healthy
        # DexScreener price and no SL/TP hit. During gateway 429 storms that
        # massacred healthy slots. Removed: with Jupiter down we simply manage
        # via dex_mult (mult falls through to DexScreener below); only a real
        # dead_liquidity / timeout / flat / bleed condition closes.

        # --- max_hold timeout check (runs even when pricing fails) ---
        age_s = time.time() - pos["ts"]
        exit_reason = None
        if is_dead:
            exit_reason = "dead_liquidity"
        elif self.max_hold_s > 0 and age_s > self.max_hold_s:
            exit_reason = "timeout"
        elif (self.flat_timeout_s > 0 and age_s > self.flat_timeout_s
                and pos.get("peak_mult", 1.0) < self.flat_timeout_peak
                and not pos.get("tp_taken")):
            # Flat-timeout: held long enough with no TP and never
            # showed life — free the slot instead of slow-bleeding.
            exit_reason = "flat_timeout"
            log.info("flat timeout %s (%s): age=%.1fh peak=%.3f",
                     ca[:10], pos["symbol"], age_s / 3600,
                     pos.get("peak_mult", 1.0))
        elif (pos.get("source") != "memetracker"
                and not pos.get("tp_taken")):
            peak = pos.get("peak_mult", 1.0)
            # Dead-token kill only: no movement at all after 8 min.
            # Tier-2 ("weak", <3% in 8-45m) REMOVED 2026-09-12: it
            # overlapped early_filter + hard_stop + trail and killed
            # 33/63 paper trades, most at hold=0m pre-fix. Let the
            # dedicated exits decide once a position has 8 min of life.
            if age_s > 480 and peak < 1.015:
                exit_reason = "quick_bleed"
                log.info("dead token kill %s (%s): age=%.0fm peak=%.3f",
                         ca[:10], pos["symbol"], age_s / 60, peak)

        # Track peak using ONLY the executable price.
        best_mult = jup_mult if jup_mult is not None else dex_mult
        if best_mult is not None and best_mult > 0:
            pos["peak_usd"] = max(pos["peak_usd"],
                                  pos["entry_usd"] * best_mult)
            pos["peak_mult"] = max(pos.get("peak_mult", 1.0), best_mult)
        # Use Jupiter price as authoritative for exit decisions.
        mult = jup_mult if jup_mult is not None else dex_mult
        if mult is None and not exit_reason:
            return  # can't price, not dead yet — leave open
        peak_mult = pos.get("peak_mult", mult)
        if not is_dead and exit_reason not in ("timeout", "flat_timeout", "quick_bleed"):
            exit_reason = None  # reset; dead_liquidity/timeout already set above
        # ---- early adverse filter (one-shot at early_filter_window_s):
        # Track worst/best excursion during the early window, then
        # evaluate once.  If the position drew down >early_filter_dd
        # AND never gained >early_filter_gain, close immediately.
        # This is the key finding from the 2026-08-13 ablation:
        # rejecting trades with >20% adverse AND <5% favorable in
        # first 30s turns gross PnL from -0.447 to +0.335 SOL.
        age_s = time.time() - pos["ts"]
        if not pos.get("early_checked", False) and mult is not None:
            if age_s < self.early_filter_window_s:
                # Still in early window: track min/max excursion
                pos["early_min_mult"] = min(
                    pos.get("early_min_mult", mult), mult)
                pos["early_max_mult"] = max(
                    pos.get("early_max_mult", mult), mult)
            else:
                # Window expired: evaluate (one-shot)
                early_dd = 1.0 - pos.get("early_min_mult", mult)
                early_gain = pos.get("early_max_mult", mult) - 1.0
                pos["early_checked"] = True
                if (early_dd > self.early_filter_dd
                        and early_gain < self.early_filter_gain):
                    exit_reason = "early_invalid"
                    logs.journal("shadow_early_filter", ca=ca,
                                 symbol=pos["symbol"],
                                 dd_pct=round(early_dd * 100, 2),
                                 gain_pct=round(early_gain * 100, 2),
                                 result="rejected")
                else:
                    logs.journal("shadow_early_filter", ca=ca,
                                 symbol=pos["symbol"],
                                 dd_pct=round(early_dd * 100, 2),
                                 gain_pct=round(early_gain * 100, 2),
                                 result="passed")
        # ---- take-profit ladder (scale-out): when the peak reaches a
        # level, bank that fraction of the ORIGINAL size. Use the
        # EXECUTABLE (Jupiter) price so we only record levels that
        # were actually reachable at fill quality.
        # IMPORTANT: skip all normal exit logic when early_invalid
        # fired — it is terminal (matches the ablation semantics).
        # Also skip when mult is None (dead token, can't price).
        if exit_reason != "early_invalid" and mult is not None:
            for lvl_i, (lvl, frac, trail_pct) in enumerate(self.tp_ladder):
                if lvl in pos["tp_taken"]:
                    continue
                if peak_mult >= lvl:
                    exec_at_level = min(mult, lvl) if mult < lvl else lvl
                    pos["tp_taken"].append(lvl)
                    pos["banked_pnl"] += frac * pos["size_sol"] * (exec_at_level - 1.0)
                    pos["remaining"] = max(0.0, pos["remaining"] - frac)
                    pos["tp_level"] = lvl_i  # track current level for trail
                    logs.journal("shadow_tp", ca=ca, symbol=pos["symbol"],
                                 lvl=lvl, frac=frac, trail_pct=trail_pct,
                                 exec_px=round(exec_at_level, 3))
                    if pos["remaining"] <= 1e-9:
                        pos["remaining"] = 0.0
            if pos["tp_taken"] and not pos["be_armed"]:
                pos["be_armed"] = True
                logs.journal("shadow_be", ca=ca, symbol=pos["symbol"])
            elif (not pos["be_armed"] and self.be_arm_mult > 0
                    and peak_mult >= self.be_arm_mult):
                # Early BE: spike showed +15% but faded before TP1 —
                # lock breakeven instead of riding to the hard stop.
                pos["be_armed"] = True
                logs.journal("shadow_be_early", ca=ca, symbol=pos["symbol"],
                             peak=round(peak_mult, 3))
            if pos["remaining"] <= 0:
                exit_reason = "tp"   # fully scaled out at the spike
            else:
                stop_mult = (1 - self.hard_stop)
                if pos["be_armed"]:
                    stop_mult = max(stop_mult, 1.0 + self.be_buffer)
                if self.hard_stop > 0 and mult <= stop_mult:
                    exit_reason = "sl"
                elif self.trail_enabled:
                    # Tiered trailing stop: use trail_pct from the
                    # highest TP level that has fired. If no TP yet,
                    # use the global retrace_pct as fallback.
                    tp_level = pos.get("tp_level", -1)
                    if tp_level >= 0:
                        # Use the trail_pct from the LAST fired level
                        trail_pct = self.tp_ladder[tp_level][2]
                    else:
                        trail_pct = self.retrace
                    trail_start = self.trail_start_mult if tp_level < 0 else 1.0
                    if peak_mult >= trail_start and \
                            mult <= peak_mult * (1 - trail_pct):
                        exit_reason = "trail"
        if not exit_reason:
            return
        # Structural close runs locked; the fetches above did not hold
        # the lock so signals were never blocked behind slow quotes.
        async with self._lock:
            if ca not in self.open:
                return  # evicted concurrently; nothing to close
            pos = self.open[ca]
            # NOTE (2026-09-14): the old `jupiter_down_force_close` override was
            # here — it re-labelled ANY close during a Jupiter outage, hiding
            # the real reason (sl/trail/timeout evaluated on the DexScreener
            # leg). Removed: keep the genuine exit_reason so stats stay honest.
            # --- PumpAPI sell fallback for bonding curve tokens ---
            # When position was bought via PumpAPI (entry_mode=pumpapi),
            # tokens_raw is 0 and Jupiter can't sell it. Use PumpAPI sell.
            if (pos.get("entry_mode") == "pumpapi"
                    and self.jupiter is not None
                    and self.jupiter._pumpapi_enabled()):
                remaining_pct = int(pos.get("remaining", 1.0) * 100)
                if remaining_pct > 0:
                    log.info("pumpapi sell %s (%s): %d%% remaining",
                             ca[:10], pos["symbol"], remaining_pct)
                    sell_res = await self.jupiter.sell_via_pumpapi(
                        ca, remaining_pct)
                    if sell_res.success:
                        log.info("pumpapi sell OK %s: sig=%s",
                                 ca[:10], sell_res.signature[:16])
                        pos["exit_note"] = f"pumpapi_sell:{sell_res.signature[:12]}"
                    else:
                        log.warning("pumpapi sell failed %s: %s",
                                    ca[:10], sell_res.error)
                        pos["exit_note"] = f"pumpapi_sell_fail:{sell_res.error}"
            # For dead tokens (mult=None), we cannot know the exit price.
            # The old code booked mult=0.0 (full -100% loss), which turned a
            # Jupiter 429 storm into a fake -0.0395 SOL trade (FwEm…, 2026-09-14).
            # Honest fallback: close at the last seen price (usually ~entry, so
            # pnl ≈ 0) and keep oracle_fail=True so status/win-rate exclude it.
            # Banked TP is already counted.
            # oracle_fail=True marks infra failures (both pricers
            # down) so they can be excluded from strategy stats —
            # they are not trading losses.
            oracle_fail = mult is None
            if mult is not None:
                eff_mult = mult
            else:
                _last = pos.get("last_usd") or 0.0
                _entry = pos.get("entry_usd") or 0.0
                eff_mult = (_last / _entry) if _last > 0 and _entry > 0 else 0.0
            pnl = pos.get("banked_pnl", 0.0) + \
                pos["remaining"] * pos["size_sol"] * (eff_mult - 1.0)
            # Trade-level multiple (incl. any banked TP) for honest
            # reporting — the exit-leg `mult` alone misleads when a
            # partial was already banked (e.g. Bear: exit 0.70x but net +).
            trade_mult = (pos["size_sol"] + pnl) / pos["size_sol"]
            rec = {"ca": ca, "symbol": pos["symbol"], "reason": exit_reason,
                     "mult": round(trade_mult, 3), "pnl_sol": round(pnl, 5),
                     "hold_min": int((time.time() - pos["ts"]) / 60),
                     "closed_ts": int(time.time()),
                     "wallets": pos.get("wallets", []),
                     "source": pos.get("source", "pumpapi"),
                     "size_sol": round(pos["size_sol"], 5),
                     "oracle_fail": oracle_fail}
            self.closed.append(rec)
            bal_before = self.balance_sol
            self.balance_sol += pos["size_sol"] + pnl
            del self.open[ca]
            # Time-based cooldown: allow re-entry after cooldown_s
            self._cooldown[ca] = time.time() + self.reentry_cooldown_s
            logs.journal("shadow_close", **rec)
            if self.on_trade_close is not None:
                try:
                    self.on_trade_close(pos.get("wallets", []), pnl > 0, pnl)
                except Exception:
                    log.exception("on_trade_close failed")
            if self.notifier is not None:
                try:
                    asyncio.get_running_loop().create_task(
                    self.notifier.send_close(
                        ca=ca, name=pos["symbol"], reason=exit_reason,
                        mult=trade_mult, pnl_sol=pnl,
                            hold_s=time.time() - pos["ts"],
                            entry_px=pos["entry_usd"], exit_px=pos["last_usd"],
                            size_sol=pos["size_sol"],
                            balance_before=bal_before,
                            balance_after=self.balance_sol,
                             open_count=len(self.open),
                             max_positions=self.max_positions,
                             win_rate=self._win_rate(),
                             wallets=pos.get("wallets", [])))
                except Exception:
                    log.exception("send_close failed")

    # ------------------------------------------------------------- reporting
    def snapshot(self, wallets_n: int, alerts: int, consensus: int,
                 uptime_s: float, feeds: dict) -> dict:
        return {
            "uptime_s": uptime_s, "wallets": wallets_n, "alerts": alerts,
            "consensus": consensus, "open": list(self.open.values()),
            "closed": self.closed, "start_balance_sol": self.start_balance_sol,
            "feeds": feeds,
        }


async def _run_watch(s: cfg.Settings) -> int:
    env = cfg.load_env()
    notifier = TelegramNotifier()
    ds = DexScreenerClient(base_url=s.dexscreener_base_url,
                           rpm=s.dexscreener_rpm)
    # DexPaprika fallback: richer Solana data when DexScreener is unavailable
    dp = DexPaprikaClient(enabled=True)
    # MadeOnSol KOL validator: background /kol/feed overlap journal.
    # Read-only, never gates/skips/sizes — evaluation only.
    madeonsol = None
    if s.madeonsol_enabled:
        try:
            madeonsol = MadeOnSolClient(
                api_key=s.madeonsol_api_key,
                poll_s=s.madeonsol_poll_s,
                window_s=s.madeonsol_window_s)
            madeonsol.start()
            log.info("madeonsol: validator enabled (poll=%.0fs, window=%.0fs)",
                     s.madeonsol_poll_s, s.madeonsol_window_s)
        except Exception:
            log.exception("madeonsol init failed — disabled")
            madeonsol = None
    # Fail-open rug/safety filter (DBotX). Degrades to allow on any error.
    dbx = DBotXClient(api_key=s.dbotx_api_key, base_url=s.dbotx_base_url)

    # RugCheck safety filter (fail-open: errors = allow)
    rugcheck = None
    if s.rug_check_api_key:
        rugcheck = RugCheckClient(
            api_key=s.rug_check_api_key,
            base_url=s.rug_check_base_url,
            max_score=s.rug_check_max_score,
            reject_danger=s.rug_check_reject_danger,
        )
        log.info("rugcheck: enabled (max_score=%d, reject_danger=%s)",
                 s.rug_check_max_score, s.rug_check_reject_danger)
    else:
        log.info("rugcheck: disabled (no API key)")

    # Data-driven wallet quality: weight each KOL by real win rate + PnL so the
    # consensus score reflects conviction, not just head-count.

    # Helius unified client (DAS + Wallet Identity) — uses existing API keys
    helius = None
    try:
        from helius_client import HeliusClient
        _h_keys = [k.strip() for k in s.helius_api_keys.split(",") if k.strip()]
        if _h_keys:
            helius = HeliusClient(api_keys=_h_keys, rpc_url=s.helius_rpc_url)
            log.info("helius: enabled (keys=%d)", len(_h_keys))
        else:
            log.info("helius: disabled (no API keys)")
    except Exception:
        log.exception("helius init failed — disabled")

    # Vybe Network client (token data, liquidity, top holders, wallet PnL)
    vybe = None
    vybe_key = (cfg.get(env, "VYBE_API_KEY") or "").strip()
    if vybe_key:
        try:
            from vybe import VybeClient
            vybe = VybeClient(
                api_key=vybe_key,
                base_url=cfg.get(env, "VYBE_API_URL", "https://api.vybenetwork.xyz"),
                enabled=s.vybe_enabled,
                min_liquidity_usd=s.vybe_min_liquidity_usd,
                max_top_holder_pct=s.vybe_max_top_holder_pct,
                min_buy_sell_ratio=s.vybe_min_buy_sell_ratio,
            )
            if vybe.enabled:
                log.info("vybe: enabled (liq>$%.0f, top5<%.0f%%)",
                         s.vybe_min_liquidity_usd, s.vybe_max_top_holder_pct)
            else:
                vybe = None
        except Exception:
            log.exception("vybe init failed — disabled")

    # Birdeye Data API (holder cohorts + smart money, journal-only Phase 1).
    # Enrichment is journaled per open and never gates entries.
    birdeye = None
    birdeye_key = (cfg.get(env, "BIRDEYE_API_KEY") or "").strip()
    if birdeye_key and s.birdeye_enabled:
        try:
            birdeye = BirdeyeClient(api_key=birdeye_key)
            log.info("birdeye: enabled (journal-only enrichment)")
        except Exception:
            log.exception("birdeye init failed — disabled")
            birdeye = None

    # CabalSpy client (real-time KOL/SM/Whale data streams)
    cabalspy_key = (cfg.get(env, "CABALSPY_API_KEY") or "").strip()
    cabalspy_keys = [k.strip() for k in cabalspy_key.split(",") if k.strip()]
    if cabalspy_keys and s.cabalspy_enabled:
        log.info("cabalspy: enabled (min_buy=%.1f, min_win_rate=%.0f)",
                 s.cabalspy_signal_min_buy, s.cabalspy_signal_min_win_rate)
    # CabalSpy REST companion (history backtest, bundle detail, lookup).
    # Fail-open; rotation skips exhausted keys (key1 dead as of 2026-09-12).
    cabalspy_rest = CabalSpyREST(api_keys=cabalspy_keys) if cabalspy_keys else None
    _bundle_detail_cache: dict[str, float] = {}  # ca -> ts of last bundle_get

    weights, default_weight = build_weights(
        s.wallet_perf_path,
        floor_win=s.wallet_weight_floor_win,
        full_win=s.wallet_weight_full_win,
        pnl_tier1=s.wallet_pnl_tier1, pnl_tier2=s.wallet_pnl_tier2,
        tier1_mult=s.wallet_weight_tier1_mult,
        tier2_mult=s.wallet_weight_tier2_mult,
        default_weight=s.wallet_default_weight,
        max_weight=s.wallet_weight_max,
    )
    log.info("wallet weights: %d scored, %d at 0 (noise), default=%.2f",
             sum(1 for v in weights.values() if v > 0),
             sum(1 for v in weights.values() if v == 0),
             default_weight)

    w = SmartWalletWatcher(
        shyft_key=(cfg.get(env, "SHYFT_API_KEY") or "").strip(),
        shyft_rpc=cfg.get(env, "SHYFT_RPC_URL", "https://rpc.shyft.to"),
        ds=ds,
        notifier=notifier,
        poll_s=s.watch_poll_s,
        min_buy_usd=s.watch_min_buy_usd,
        max_buy_usd=s.watch_max_buy_usd,
        consensus_wallets=s.watch_consensus_wallets,
        consensus_window_s=s.watch_consensus_window_s,
        first_lookback_s=s.watch_first_lookback_s,
        state_file="watcher_state.json",
        wallet_weights=weights,
        wallet_default_weight=default_weight,
        consensus_weight_threshold=s.consensus_weight_threshold,
        require_strong_wallet=s.require_strong_wallet,
    )
    jupiter = JupiterSwap(dry_run=True)
    # Live KOL-buy stream (pumpapi.io): accurate USD pricing for fresh pumps,
    # feeding the same consensus/open pipeline as the Shyft polling fallback.
    async def _on_pump_buy(wallet, ca, sym, usd, amount, tg_liq=0.0, **kw):
        await w._process_buy(wallet, {
            "ca": ca, "amount": amount, "usd": usd,
            "symbol": sym, "ts": time.time(), "tg_liq": tg_liq,
        })
    pump_stream = PumpApiStream(
        wallets=w.wallets, on_buy=_on_pump_buy, http=w._http)
    pump_task = asyncio.create_task(pump_stream.run())
    pump_task.add_done_callback(_log_task_result)

    # Helius WebSocket: real-time transaction streaming (replaces Shyft polling)
    helius_keys = [k.strip() for k in (cfg.get(env, "HELIUS_API_KEYS") or "").split(",") if k.strip()]
    helius_ws = None
    if helius_keys and s.helius_ws_enabled:
        async def _on_helius_buy(wallet: str, buy: dict) -> None:
            await w._process_buy(wallet, buy)
        helius_ws = HeliusWS(
            api_keys=helius_keys,
            wallets=w.wallets,
            on_buy=_on_helius_buy,
        )
        helius_ws.start()
        log.info("helius ws: started (wallets=%d, keys=%d)",
                 len(w.wallets), len(helius_keys))

    # CabalSpy signal stream: server-side cluster detection
    cabalspy_client = None
    if cabalspy_keys and s.cabalspy_enabled:
        try:
            _entry_at = [int(x.strip()) for x in s.cabalspy_signal_entry_at.split(",") if x.strip()]
            _exit_at = [int(x.strip()) for x in s.cabalspy_signal_exit_at.split(",") if x.strip()]
            _tx_types = [x.strip() for x in s.cabalspy_tx_types.split(",") if x.strip()]

            # Holder cache for concentration checks
            holder_cache = HolderCache()

            # Bundle tracking for coordinated rug detection
            _bundle_flags: dict[str, dict] = {}  # mint -> {detected_at, bundles}

            # Feed signal data into holder cache too
            async def _on_cabalspy_signal(msg: dict) -> None:
                """Handle CabalSpy signal events (cluster entry/exit)."""
                # Update holder cache from signal wallets
                holder_cache.update_from_signal(msg)
                data = msg.get("data", {})
                signal_kind = data.get("signal_kind")
                mint = data.get("mint")
                token = data.get("token", {})

                if not mint or signal_kind != "entry":
                    return  # only process entry signals

                sym = token.get("symbol") or "?"
                mc_usd = token.get("market_cap_usd") or 0
                cluster = data.get("cluster", {})
                qualifying_total = cluster.get("qualifying_total") or 0
                total_invested = cluster.get("total_invested") or 0

                # Extract wallet list from signal
                wallets_data = data.get("wallets", [])
                wallet_addresses = [w.get("wallet") for w in wallets_data if w.get("wallet")]

                # Compute weighted score from wallet win rates
                score = 0.0
                for w_data in wallets_data:
                    win_rate = w_data.get("win_rate") or 0
                    # Weight by win rate (0-100 -> 0-1.0)
                    weight = min(1.0, max(0.0, win_rate / 100.0))
                    score += weight

                log.info("cabalspy SIGNAL %s (%s) mc=$%.0f wallets=%d score=%.2f invested=%.2f SOL",
                         mint[:10], sym, mc_usd, qualifying_total, score, total_invested)

                # Dynamically subscribe to holder + bundle streams for this token
                if cabalspy_client:
                    try:
                        await cabalspy_client.subscribe_token_live(mint)
                    except Exception:
                        log.debug("cabalspy subscribe_token_live failed for %s", mint[:10])

                # Route to open gate (same as PumpAPI consensus)
                try:
                    await _on_smart_buy(mint, sym, mc_usd, score, wallet_addresses,
                                        source="cabalspy")
                except asyncio.CancelledError:
                    raise  # shutdown/restart — not a signal failure, don't log
                except Exception:
                    log.exception("cabalspy _on_smart_buy failed for %s", mint[:10])

            async def _on_cabalspy_tx(msg: dict) -> None:
                """Handle CabalSpy TX events (individual wallet trades)."""
                data = msg.get("data", {})
                wallet = data.get("wallet")
                tx = data.get("transaction", {})
                token = data.get("token", {})
                action = tx.get("action")

                if not wallet or action != "buy":
                    return  # only process buys

                mint = token.get("mint")
                sym = token.get("symbol") or "?"
                value = data.get("value", {})
                usd = value.get("amount_usd") or 0
                sol_amount = value.get("amount") or 0

                if not mint or usd < s.watch_min_buy_usd:
                    return
                if usd > s.watch_max_buy_usd:
                    # Same freak-misprice guard as watcher._process_buy: journal
                    # for offline tuning, never feed consensus.
                    logs.journal("smart_buy_outlier", ca=mint, wallet=(wallet or "?")[:10],
                                 usd=round(usd, 2), fresh=True, src="cabalspy_tx")
                    return

                log.debug("cabalspy TX %s buy %s $%.0f (%.2f SOL)",
                          wallet[:8], mint[:8], usd, sol_amount)

                # Route to process_buy (feeds consensus engine)
                try:
                    await w._process_buy(wallet, {
                        "ca": mint, "amount": sol_amount, "usd": usd,
                        "symbol": sym, "ts": time.time(),
                    })
                except Exception:
                    log.exception("cabalspy _process_buy failed for %s", mint[:10])

            async def _on_cabalspy_holder(msg: dict) -> None:
                """Handle CabalSpy holder events (position tracking)."""
                holder_cache.update_from_holder(msg)
                mint = msg.get("data", {}).get("mint", "?")
                event = msg.get("event", "?")
                if event in ("init", "holder_update"):
                    log.debug("cabalspy HOLDER %s %s holders=%d",
                              mint[:10], event, len(holder_cache.get_holders(mint)))

            async def _on_cabalspy_bundle(msg: dict) -> None:
                """Handle CabalSpy bundle events (coordinated KOL bundles)."""
                data = msg.get("data", {})
                mint = data.get("mint")
                if not mint:
                    return
                event = msg.get("event")
                bundles = data.get("bundles", [])
                if bundles:
                    _bundle_flags[mint] = {
                        "detected_at": time.time(),
                        "bundles": bundles,
                    }
                    log.info("cabalspy BUNDLE %s %s bundles=%d total_sol=%.2f",
                             mint[:10], event, len(bundles),
                             sum(b.get("amount_sol", 0) for b in bundles))
                    if s.cabalspy_bundle_block:
                        log.warning("cabalspy BUNDLE BLOCKED %s — coordinated bundle detected",
                                    mint[:10])

            cabalspy_client = CabalSpyClient(
                api_keys=cabalspy_keys,
                on_signal=_on_cabalspy_signal,
                on_tx=_on_cabalspy_tx,
                on_holder=_on_cabalspy_holder,
                on_bundle=_on_cabalspy_bundle,
                signal_min_buy=s.cabalspy_signal_min_buy,
                signal_entry_at=_entry_at,
                signal_exit_at=_exit_at,
                signal_min_win_rate=s.cabalspy_signal_min_win_rate,
                signal_token=s.cabalspy_signal_token,
                tx_types=_tx_types,
                tx_token=s.cabalspy_tx_token,
                holder_mode=s.cabalspy_holder_mode,
                bundle_mode=s.cabalspy_bundle_mode,
                count_token=s.cabalspy_count_token,
            )
            cabalspy_client.start()
            log.info("cabalspy ws: started (signal + tx + holder + bundle streams)")
        except Exception:
            log.exception("cabalspy init failed — disabled")
            cabalspy_client = None

    # Per-CA skip-log throttle (5-min). Initialised before the first signal
    # feed starts: feed callbacks run concurrently once created, so a late
    # init here would NameError on an early signal (memetracker chase guard).
    _skip_log = {}

    # MemeTracker signal feed (@memetrackersol) — fresh pump.fun tokens.
    memetracker_feed = None
    if s.memetracker_enabled and s.tg_api_id and s.tg_api_hash:
        async def _on_memetracker_signal(sig: dict) -> None:
            ca = sig.get("ca", "")
            sym = sig.get("symbol", "")
            mc = sig.get("mc", 0)
            liq = sig.get("liq", 0)
            holders = sig.get("holders", 0)
            vol = sig.get("vol", 0)
            insiders = sig.get("insiders", 0)
            rug = sig.get("rug_score", 0)
            migrated = sig.get("migrated", False)
            price_usd = sig.get("price_usd", 0)
            pc_1h = sig.get("pc_1h", 0) or 0.0
            log.info(
                "memetracker SIGNAL %s (%s) mc=$%.0f liq=$%.0f hold=%d vol=$%.0f ins=%d rug=%d mig=%s px=$%.8f",
                sym or "?", ca[:8], mc, liq, holders, vol, insiders, rug, migrated, price_usd,
            )
            # Vertical-chase guard: a token already up 10x+ in the last hour
            # is a chase, not an entry (paper 2026-09-12..16: 4/5 such opens
            # lost, -0.053 SOL net). Skip before the bypass below — same
            # skip: naming + 5-min per-CA log throttle as the other gates.
            if memetracker_chase_blocked(pc_1h, s.memetracker_max_pc1h_pct):
                reason = (f"skip:vertical_chase(1h={pc_1h:+.1f}%"
                          f">{s.memetracker_max_pc1h_pct:.0f}%)")
                if _skip_log.get(ca, 0) < time.time() - 300:
                    _skip_log[ca] = time.time()
                    log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                return
            # TG signal source bypasses consensus gate — channel IS the signal
            try:
                await _on_smart_buy(ca, sym, mc, 3.0, ["tg_signal"], tg_liq=liq,
                                    source="memetracker", signal_price=price_usd)
            except asyncio.CancelledError:
                raise  # shutdown/restart — not a signal failure, don't log
            except Exception:
                log.exception("memetracker _on_smart_buy failed for %s", ca[:10])

        try:
            memetracker_feed = TgSignalFeed(
                on_signal=_on_memetracker_signal,
                channel=s.memetracker_channel,
                api_id=s.tg_api_id,
                api_hash=s.tg_api_hash,
                phone=s.tg_phone,
                session_name=s.memetracker_session,
                min_mc=s.memetracker_min_mc,
                min_liq=s.memetracker_min_liq,
                min_holders=s.memetracker_min_holders,
                parser=parse_memetracker_signal,
            )
            _mt_feed_task = asyncio.create_task(memetracker_feed.run())
            _mt_feed_task.add_done_callback(_log_task_result)
            log.info("memetracker feed: started (channels=%s)", [s.memetracker_channel])
        except Exception:
            log.exception("memetracker feed init failed")
            memetracker_feed = None

    # Kolexplorer monitor feed — pre-computed KOL consensus tokens.
    kolexplorer_feed = None
    if s.kolexplorer_enabled and s.kolexplorer_cookies:
        # Repeat-signal log quieting: the feed re-emits the same CA every
        # poll (observed: HmJDgky… 15+ INFO lines/day, 1169 kolexplorer OPEN
        # lines in one log window). First sighting per CA per 30 min logs at
        # INFO; repeats stay at DEBUG so the signal path keeps working while
        # watcher.log stays readable. Routing via _on_smart_buy is unaffected.
        _kx_last_log: dict[str, float] = {}

        async def _on_kolexplorer_signal(
            ca: str, sym: str, mc: float, score: float, wallets,
            source: str = "kolexplorer", **kw,
        ) -> None:
            """Route Kolexplorer consensus signal into the open gate."""
            kol_count = kw.get("kol_count", 0)
            total_pnl = kw.get("total_pnl", 0)
            _now = time.time()
            if _now - _kx_last_log.get(ca, 0.0) >= 1800.0:
                _kx_last_log[ca] = _now
                log.info(
                    "kolexplorer OPEN %s (%s) kols=%d score=%.2f mc=$%.0f pnl=$%.0f",
                    ca[:10], sym, kol_count, score, mc, total_pnl,
                )
            else:
                log.debug(
                    "kolexplorer repeat %s (%s) kols=%d score=%.2f",
                    ca[:10], sym, kol_count, score,
                )
            try:
                await _on_smart_buy(ca, sym, mc, score, wallets, source=source)
            except asyncio.CancelledError:
                raise  # shutdown/restart — not a signal failure, don't log
            except Exception:
                log.exception("kolexplorer _on_smart_buy failed for %s", ca[:10])

        kolexplorer_feed = KolexplorerFeed(
            cookies=s.kolexplorer_cookies,
            weights=weights,
            default_weight=default_weight,
            poll_s=s.kolexplorer_poll_s,
            min_kols=s.kolexplorer_min_kols,
            min_score=s.kolexplorer_min_score,
            max_entry_mc=s.kolexplorer_max_entry_mc,
            hours=s.kolexplorer_hours,
            mode=s.kolexplorer_mode,
            heatmap_tf=s.kolexplorer_heatmap_tf,
            on_signal=_on_kolexplorer_signal,
        )
        try:
            await kolexplorer_feed.start()
        except Exception:
            log.exception("kolexplorer init failed — disabled")
            kolexplorer_feed = None

    # Hard cap on concurrent positions: never more than capital allows, and
    # never above the configured max_open_positions (avoids a consensus burst
    # over-leveraging the paper book).
    max_positions = min(max(1, round(s.start_balance_sol / s.size_sol)),
                        s.max_open_positions)
    # Live learning loop: every shadow close updates each triggering wallet's
    # hit-rate (picks vs winners) and the wallet-PAIR expectancy. Pairs with
    # clearly negative expectancy (e.g. AgmLJ+kEFiA: 6 trades / 1 win / -0.0558)
    # get penalised in the open gate so they stop monopolising the book.
    pair_perf = load_pair_perf(s.pair_perf_file)

    def _record_trade(wallets, win, pnl_sol=0.0):
        for addr in (wallets or []):
            perf = w.wallet_perf.setdefault(addr, {"picks": 0, "hits": 0})
            perf["picks"] = perf.get("picks", 0) + 1
            if win:
                perf["hits"] = perf.get("hits", 0) + 1
        pk = update_pair_perf(pair_perf, wallets, pnl_sol)
        save_pair_perf(pair_perf, s.pair_perf_file)
        logs.journal("wallet_perf_update", picks=sum(
            v.get("picks", 0) for v in w.wallet_perf.values()),
            pair=pk, pair_pnl=round(pnl_sol, 5))

    book = ShadowBook(ds, s.size_sol, s.trail_retrace_pct, s.hard_stop_pct,
                      Path(s.shadow_state_file), s.start_balance_sol,
                      jupiter=jupiter, notifier=notifier,
                      max_positions=max_positions,
                      tp1_mult=s.tp1_mult, trail_start_mult=s.trail_start_mult,
                      be_buffer=s.be_buffer_pct, max_hold_s=s.max_hold_h * 3600.0,
                      tp_ladder=s.tp_ladder, be_arm_mult=s.be_arm_mult,
                      flat_timeout_s=s.flat_timeout_h * 3600.0,
                      flat_timeout_peak=s.flat_timeout_peak,
                      trail_enabled=s.trail_enabled,
                      on_trade_close=_record_trade,
                      open_max_impact_pct=s.open_max_impact_pct,
                      early_filter_window_s=s.early_filter_window_s,
                      early_filter_dd_pct=s.early_filter_dd_pct,
                      early_filter_gain_pct=s.early_filter_gain_pct,
                      reentry_cooldown_s=s.reentry_cooldown_s,
                      dexpaprika=dp)
    await book.reconcile_balances()

    # shadow book opens automatically via on_smart_buy callback. During the
    # initial lookback window we only TRACK buys (so consensus alerts still
    # fire) and defer opening, so we never enter late — after a wallet's move
    # has already happened — which would systematically buy high.
    backfill_done = asyncio.Event()
    # Space out opens so a backlog (e.g. post-lookback batch) can't dump a
    # burst of positions at once. Configurable via OPEN_GAP_S.
    last_open = {"t": 0.0, "score": 0.0}
    open_gap_s = s.open_gap_s

    _pullback: dict[str, dict] = {}  # ca -> {t, ref} pending vertical-breakout holds

    def _adaptive_size(settings, effective_score: float, source: str = "") -> float:
        """Scale position size linearly between min/max based on consensus quality.

        Weak consensus (effective ~threshold) -> size_sol_min.
        Strong consensus (effective ~2x threshold) -> size_sol_max.
        Per-source multipliers from paper expectancy (2026-09-16, 67 trades):
          memetracker 1.0x (-0.100/19, worst source — left tail of
          unsellable chase entries; now gated by memetracker_executable),
          kolexplorer 1.0x (-0.021/13), cabalspy 1.0x (-0.077/35; boost cut
          2026-09-16 — 43% wins but small wins vs full-size losses),
          pumpapi 0.5x (journal-only).
        """
        score_min = settings.consensus_weight_threshold
        score_max = score_min * 2.0  # strong signal ~2x threshold
        t = max(0.0, min(1.0, (effective_score - score_min) / (score_max - score_min)))
        size = settings.size_sol_min + t * (settings.size_sol_max - settings.size_sol_min)
        _src_mult = {
            "cabalspy": 1.0,
            "memetracker": 1.0,
            "kolexplorer": 1.0,
            "pumpapi": 0.5,
        }
        size *= _src_mult.get((source or "").lower(), 0.8)
        return round(min(size, settings.size_sol_max), 4)

    _stable_syms = {x.strip().upper() for x in (s.stable_symbols or "").split(",") if x.strip()}

    async def _birdeye_enrich(ca: str, symbol: str) -> None:
        """Journal-only Birdeye enrichment for an opened position.

        Phase 1 measures only: holder cohorts (bundler/insider/sniper/dev
        vs smart_trader/kol supply shares) + top-trader flow (exited
        fraction, tags) are journaled as ``birdeye_enrich`` and never gate
        entries. Promotion to gate comes only with journal evidence.
        """
        if birdeye is None:
            return
        try:
            data = await birdeye.enrich(ca)
        except Exception as exc:
            log.debug("birdeye enrich failed for %s: %s", ca[:10], exc)
            return
        if data:
            logs.journal("birdeye_enrich", ca=ca, symbol=symbol, **data)

    _SIGNAL_TIMEOUT_S = 150.0  # hung-signal watchdog (see below)

    async def _on_smart_buy(ca, sym, usd, score, wallets=None, tg_liq=0.0,
                            source="pumpapi", signal_price=0.0):
        # Watchdog: a gate evaluation must never hang silently (2026-09-12:
        # two GME cabalspy signals vanished with no outcome line, no error).
        # On timeout, dump the stuck frames + lock state, then cancel.
        inner = asyncio.ensure_future(
            _on_smart_buy_inner(ca, sym, usd, score, wallets, tg_liq,
                                source, signal_price))
        try:
            await asyncio.wait_for(asyncio.shield(inner), _SIGNAL_TIMEOUT_S)
        except TimeoutError:
            # Task.get_stack() returns raw frame objects (not
            # traceback.FrameSummary), so use f_code/f_lineno.
            frames = [f"{f.f_code.co_filename.split('/')[-1]}:{f.f_lineno} in {f.f_code.co_name}"
                      for f in inner.get_stack(limit=8)]
            log.error("WATCHDOG hung signal %s (%s) src=%s lock=%s frames=%s",
                      ca[:10], sym, source, book._lock.locked(), " <- ".join(frames))
            logs.journal("signal_watchdog", ca=ca, symbol=sym, source=source,
                         lock_held=book._lock.locked(), frames=frames)
            inner.cancel()
        except asyncio.CancelledError:
            inner.cancel()
            raise

    async def _on_smart_buy_inner(ca, sym, usd, score, wallets=None, tg_liq=0.0,
                                  source="pumpapi", signal_price=0.0):
        last_detection_ts["t"] = time.time()
        # MadeOnSol validator (read-only, journal-only): independent KOL
        # footprint for this mint. In-memory lookup — never blocks, skips,
        # or sizes. Used offline to score the validator before any gating.
        try:
            if madeonsol is not None:
                _fp = madeonsol.kol_footprint(ca)
                if _fp["buys"] > 0:
                    logs.journal("madeonsol_overlap", ca=ca, symbol=sym,
                                 source=source, score=round(score, 2),
                                 kol_buys=_fp["buys"], kol_wallets=_fp["kols"],
                                 max_winrate_7d=_fp["max_winrate_7d"],
                                 top_kol=_fp["top_kol"])
        except Exception as exc:
            log.debug("madeonsol overlap failed for %s: %s", ca[:10], exc)
        n = len(wallets or [])
        # Concentration guard: cap how many open positions may share any one
        # triggering wallet so we don't stack correlated bets and so slots stay
        # free for genuinely different signals.
        overlap = 0
        for w in (wallets or []):
            c = sum(1 for p in book.open.values()
                    if w in (p.get("wallets") or []))
            overlap = max(overlap, c)
        if not backfill_done.is_set():
            reason = "deferred:lookback"
        elif (sym or "").upper() in _stable_syms:
            # Stablecoin/impostor guard: stables can't run the TP ladder and
            # scam mints reuse trusted symbols (fake USDC). -EV either way.
            reason = f"skip:stable_symbol({sym})"
        elif score < s.consensus_weight_threshold:
            # Weighted consensus gate: the summed quality score of distinct
            # buying wallets must clear the threshold. A single proven winner
            # (weight >= 1) is enough; two mid winners sum to ~1; noise wallets
            # (weight ~0) can never manufacture a signal on their own.
            reason = f"skip:score<{s.consensus_weight_threshold}"
        elif n < s.open_min_wallets and wallets != ["tg_signal"]:
            # TG signals bypass min_wallets — the channel IS the consensus
            reason = f"skip:min_wallets<{s.open_min_wallets}"
        elif overlap >= s.per_wallet_max_positions and wallets != ["tg_signal"]:
            # TG signals bypass per_wallet_cap — each signal is a different token
            reason = f"skip:per_wallet_cap>={s.per_wallet_max_positions}"
        elif usd < s.watch_min_buy_usd:
            reason = "skip:below_min_buy"
        elif ca in book.open:
            reason = "skip:already_open"
        elif len(book.open) >= book.max_positions:
            reason = "skip:max_positions"
        elif book.balance_sol < (min(book.size_sol, s.size_sol_min)
                                 if s.adaptive_sizing else book.size_sol):
            # Adaptive sizes can be as small as size_sol_min: block only when
            # even the smallest size is unaffordable (was: default size).
            reason = "skip:insufficient_balance"
        elif time.time() < book._cooldown.get(ca, 0):
            # Time-based re-entry cooldown (seeded from persisted closes
            # on startup so it survives restarts). This is the ONLY
            # re-entry block — the old permanent `recently_closed` check
            # (any CA in the last 100 closes blocked forever) made this
            # dead code and prevented legitimate re-entries.
            reason = "skip:cooldown"
        elif time.time() - last_open["t"] < open_gap_s:
            # Open-spacing override: a much stronger signal (score >= 2.5) can
            # bypass the gap if the last open was weak (score < 2.0).  This
            # prevents a mediocre signal from blocking a genuine multi-wallet
            # consensus that arrives within the cooldown.
            last_score = last_open.get("score", 0)
            if score >= 2.5 and last_score < 2.0:
                log.info("open_spacing override %s (%s) score=%.2f > last %.2f",
                         ca[:10], sym, score, last_score)
                reason = None  # override — will proceed to open
            else:
                reason = "skip:open_spacing"
        elif source == "pumpapi" and s.pumpapi_journal_only:
            # Journal-only mode (worst paper source, -0.11/19): pumpapi sees
            # everything first, so its signals still feed consensus, journal,
            # wallet tracking and the MadeOnSol validator above — but never
            # open positions. Re-enable by setting PUMPAPI_JOURNAL_ONLY=false.
            logs.journal("pumpapi_journal_only", ca=ca, symbol=sym,
                         score=round(score, 2), wallets=n)
            reason = "skip:pumpapi_journal_only"
        elif source == "memetracker":
            # MemeTracker bypass: use signal price directly (no Jupiter/DexScreener needed)
            px = signal_price
            if px <= 0:
                log.info("memetracker skip %s (%s): no price in signal", ca[:10], sym)
                return
            # Executability guard: signal-price entries used to bypass every
            # Jupiter gate (see memetracker_executable). Probe at the actual
            # open size so the impact check reflects the real fill.
            if jupiter is not None:
                _probe = (_adaptive_size(s, score, source)
                          if s.adaptive_sizing else s.size_sol)
                reason = await memetracker_executable(
                    jupiter, ca, int(_probe * 1e9), s.open_max_impact_pct)
                if reason is not None:
                    if _skip_log.get(ca, 0) < time.time() - 300:
                        _skip_log[ca] = time.time()
                        log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                    return
            last_open["t"] = time.time()
            last_open["score"] = score
            logs.journal("open_signal_momentum", ca=ca, symbol=sym,
                         score=score, effective=round(score, 3),
                         pmult=1.0, align=0, price_change={},
                         source=source)
            _open_size = _adaptive_size(s, score, source) if s.adaptive_sizing else None
            await book.open_position(ca, sym, px, px, n, wallets=wallets, size_sol=_open_size,
                                     source=source, mc=usd, score=score,
                                     signal_price=px)
            if birdeye is not None and ca in book.open:
                asyncio.get_running_loop().create_task(
                    _birdeye_enrich(ca, sym)).add_done_callback(_log_task_result)
            return
        else:
            # Fetch the market snapshot once: it drives both the momentum floor
            # and the multi-timeframe alignment score modifier below.
            try:
                snap = await ds.token_pairs("solana", ca)
            except Exception:
                snap = None
            # DexPaprika fallback: richer Solana data when DexScreener fails
            if not snap and dp is not None:
                try:
                    snap = await dp.get_token_details(ca)
                except Exception as exc:
                    log.debug("dexpaprika details failed for %s: %s", ca[:10], exc)
            # Rug/safety gate (DBotX, fail-open): reject tokens that still hold a
            # mint or freeze authority, or are dangerously top-10 concentrated.
            # A 403 / missing key degrades to "allow" so an outage never blocks.
            # NOTE: Pump.fun tokens legitimately have mint authority until graduation.
            # We only block mint/freeze if liquidity is LOW (< $5k) — indicating
            # the token hasn't graduated and is likely a rug risk.
            if s.dbotx_safety:
                pair_addr = (snap or {}).get("pair_address") or ca
                info = await dbx.pair_safety("solana", pair_addr)
                if info.get("available"):
                    _liq = (snap or {}).get("liq") or 0
                    # Use TG-reported liquidity as fallback for fresh pump.fun tokens
                    # not yet indexed by DexScreener (liq=0 from DexScreener)
                    if _liq < 100 and tg_liq > 0:
                        _liq = tg_liq
                    _mint_freeze = info["mint_authority"] or info["freeze_authority"]
                    # Block mint/freeze only if liquidity is below threshold
                    # (pump.fun pre-graduation tokens are high-risk)
                    if _mint_freeze and _liq < s.dbotx_mint_freeze_liq_max:
                        reason = f"skip:unsafe(mint/freeze,liq=${_liq:.0f})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    elif _mint_freeze:
                        log.info("dbotx WARN %s (%s): mint/freeze but liq=$%.0f > $5k — allowing",
                                 ca[:10], sym, _liq)
                    if info["top10"] > s.dbotx_top10_max:
                        # Dynamic top10 threshold: pump.fun tokens naturally
                        # have higher concentration at low MC.  Relax the
                        # gate for tokens under $500k MC so early consensus
                        # signals aren't all rejected.
                        _mc = (snap or {}).get("mcap") or 0
                        if _mc > 0 and _mc < 100_000:
                            _top10_limit = 0.80  # very early, high concentration OK
                        elif _mc > 0 and _mc < 500_000:
                            _top10_limit = 0.60  # pump.fun graduation range
                        else:
                            _top10_limit = s.dbotx_top10_max
                        if info["top10"] > _top10_limit:
                            reason = f"skip:unsafe(top10={info['top10']:.0%}>{_top10_limit:.0%},mc=${_mc:.0f})"
                            if _skip_log.get(ca, 0) < time.time() - 300:
                                _skip_log[ca] = time.time()
                                log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                            return
                        else:
                            log.info("dbotx TOP10 relaxed %s (%s): top10=%.0f%% <= %.0f%% (mc=$%.0f)",
                                     ca[:10], sym, info["top10"] * 100, _top10_limit * 100, _mc)
                    if info["dev_position"] not in (None, "cleared"):
                        reason = f"skip:unsafe(dev={info['dev_position']})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    logs.journal("open_safety_ok", ca=ca, symbol=sym,
                                 safety=info)
            # RugCheck safety gate (fail-open): reject rug/high-risk tokens
            # Skip DANGER filter (mint/freeze) for tokens with MC > threshold
            # OR with high volume (real organic trading = not a rug)
            if rugcheck is not None:
                rc = await rugcheck.check(ca)
                if not rugcheck.is_safe(rc):
                    mc = (snap or {}).get("mcap") or 0
                    vol24 = float((snap or {}).get("vol_h24") or 0)
                    # DANGER on mint/freeze only blocks low-MC, low-volume tokens
                    has_danger = rc.has_danger if rc else False
                    only_danger = has_danger and rc.score_normalised <= s.rug_check_max_score and not rc.rugged
                    if only_danger and (mc > s.rug_check_min_mc_for_danger or vol24 > 100_000):
                        log.info("rugcheck DANGER ignored %s (%s): mc=$%.0f vol=$%.0f",
                                 ca[:10], sym, mc, vol24)
                    else:
                        reason = f"skip:rugcheck({rc.summary() if rc else 'error'})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
            # Helius: deployer rugger check + top-10 holder concentration
            # Quota guard (2026-09-14): while the WS is in 429 circuit-breaker
            # backoff the Helius RPC quota is exhausted too (token_decimals
            # 429s in the same storm). token_safety is 2-3 RPC calls per
            # signal and fail-open anyway — skip the calls entirely while
            # degraded instead of burning quota that delays recovery.
            _helius_degraded = bool(
                helius_ws is not None and getattr(helius_ws, "degraded", False))
            if helius is not None and s.helius_rugger_block and not _helius_degraded:
                try:
                    safety = await helius.token_safety(ca)
                    if not safety.get("safe", True):
                        ident = safety.get("deployer_identity", {})
                        cats = [c.lower() for c in (ident.get("categories") or [])]
                        reason = f"skip:deployer_rugger({cats})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    top10_pct = safety.get("top10_pct", 0)
                    if top10_pct > s.helius_max_top10_pct:
                        reason = f"skip:top10_concentration({top10_pct:.1f}%>{s.helius_max_top10_pct}%)"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                except Exception as exc:
                    log.warning("helius safety check failed for %s: %r", ca[:10], exc)
            # Vybe Network safety gates (fail-open): liquidity, top holder
            # concentration, buy/sell ratio. Provides independent validation
            # alongside DexPaprika and DBotX.
            if vybe is not None and s.vybe_enabled:
                try:
                    # Liquidity check
                    liq_safe, _liq_usd, liq_reason = await vybe.check_liquidity(ca)
                    if not liq_safe:
                        reason = f"skip:{liq_reason}"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    # Top holder concentration check
                    holder_safe, _top5_pct, holder_reason = await vybe.check_top_holders(ca)
                    if not holder_safe:
                        reason = f"skip:{holder_reason}"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    # Buy/sell ratio: journal-only (endpoint UNVERIFIED — see
                    # vybe.py. Never blocks; offline analysis decides if the
                    # ratio predicts anything before it may gate).
                    bs_safe, bs_ratio, bs_reason = await vybe.check_buy_sell_ratio(ca)
                    if not bs_safe:
                        logs.journal("vybe_sell_pressure", ca=ca, symbol=sym,
                                     ratio=round(bs_ratio, 3), note=bs_reason)
                except Exception as exc:
                    log.warning("vybe check failed for %s: %s", ca[:10], exc)
            # CabalSpy holder concentration check: if we have holder data from
            # the signal stream, reject tokens where any single holder owns > max_pct.
            if cabalspy_client is not None and cabalspy_client.connected:
                try:
                    safe, _max_pct, c_reason = holder_cache.check_concentration(
                        ca, s.cabalspy_holder_max_pct)
                    if not safe:
                        reason = f"skip:holder_concentration({c_reason})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    # Bundle block: reject if coordinated bundle detected recently.
                    # Enrichment (rare, credit-cheap): one bundle_get per CA per
                    # hour journals confidence/jito/side-wallet detail. The
                    # block itself never depends on the fetch (fail-open).
                    if s.cabalspy_bundle_block and ca in _bundle_flags:
                        bundle_age = time.time() - _bundle_flags[ca].get("detected_at", 0)
                        if bundle_age < 600:  # block for 10 minutes
                            if (cabalspy_rest is not None and _bundle_detail_cache.get(ca, 0)
                                    < time.time() - 3600):
                                _bundle_detail_cache[ca] = time.time()
                                try:
                                    _bd = await cabalspy_rest.bundle_get(ca)
                                    if _bd:
                                        _bs = (_bd.get("bundles") or [])
                                        _top = max(_bs, key=lambda b: b.get("confidence", 0),
                                                   default=None)
                                        logs.journal(
                                            "cabalspy_bundle_detail", ca=ca, symbol=sym,
                                            bundles=len(_bs),
                                            confidence=(_top or {}).get("confidence"),
                                            jito=(_top or {}).get("jito_confirmed"),
                                            wallets=(_top or {}).get("wallet_count"))
                                except Exception as exc:
                                    log.debug("bundle detail fetch failed for %s: %s",
                                              ca[:10], exc)
                            reason = f"skip:bundle_detected({bundle_age:.0f}s ago)"
                            if _skip_log.get(ca, 0) < time.time() - 300:
                                _skip_log[ca] = time.time()
                                log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                            return
                except Exception as exc:
                    log.debug("cabalspy holder/bundle check failed for %s: %s", ca[:10], exc)
            # Jupiter Token Audit: pre-trade safety via /tokens/v2/search
            # Checks mint/freeze authority, dev balance, holder concentration,
            # organic flow (buyers/vol 5m — preferred over the relative
            # organicScore), and suspicious flags. Fail-open on errors.
            if s.jup_audit_enabled and jupiter is not None:
                try:
                    audit = await jupiter.token_audit(ca)
                    if audit.get("available"):
                        _reject_reasons = []
                        if not audit.get("mint_authority_disabled", True):
                            _reject_reasons.append("mint_authority")
                        if not audit.get("freeze_authority_disabled", True):
                            _reject_reasons.append("freeze_authority")
                        if audit.get("is_sus"):
                            _reject_reasons.append("flagged_suspicious")
                        if audit.get("top_holders_pct", 0) > s.jup_audit_max_top_holders_pct:
                            _reject_reasons.append(
                                f"top_holders={audit['top_holders_pct']:.1f}%"
                                f">{s.jup_audit_max_top_holders_pct}%")
                        if audit.get("dev_balance_pct", 0) > s.jup_audit_max_dev_balance_pct:
                            _reject_reasons.append(
                                f"dev_balance={audit['dev_balance_pct']:.1f}%"
                                f">{s.jup_audit_max_dev_balance_pct}%")
                        # dev_mints is a SOFT signal, not a veto: pump.fun serial
                        # deployers routinely exceed 100 mints, and the raw
                        # count blocked every consensus signal for ~2h
                        # (177–471 mints on otherwise clean tokens). Hard-block
                        # only at 10x the limit (rug factories minting
                        # thousands); below that, journal for offline analysis.
                        _dev_mints = audit.get("dev_mints", 0) or 0
                        _mint_limit = s.jup_audit_max_dev_mints
                        if _dev_mints > _mint_limit * 10:
                            _reject_reasons.append(
                                f"dev_mints={_dev_mints}>{_mint_limit}x10(factory)")
                        elif _dev_mints > _mint_limit:
                            logs.journal("jup_audit_dev_mints_warn", ca=ca, symbol=sym,
                                         dev_mints=_dev_mints, limit=_mint_limit)
                        if audit.get("organic_score", 100) < s.jup_audit_min_organic_score:
                            _reject_reasons.append(
                                f"organic={audit['organic_score']}"
                                f"<{s.jup_audit_min_organic_score}")
                        if audit.get("holder_count", 999999) < s.jup_audit_min_holder_count:
                            _reject_reasons.append(
                                f"holders={audit['holder_count']}"
                                f"<{s.jup_audit_min_holder_count}")
                        _min_ob = getattr(s, "jup_audit_min_organic_buyers_5m", 0)
                        if _min_ob and audit.get("organic_buyers_5m", 0) < _min_ob:
                            _reject_reasons.append(
                                f"org_buyers5m={audit.get('organic_buyers_5m', 0)}"
                                f"<{_min_ob}")
                        if _reject_reasons:
                            reason = f"skip:jup_audit({';'.join(_reject_reasons)})"
                            if _skip_log.get(ca, 0) < time.time() - 300:
                                _skip_log[ca] = time.time()
                                log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                            return
                        log.debug("jup_audit OK %s (%s): org=%d holders=%d ob5m=%d",
                                  ca[:10], sym, audit.get("organic_score", 0),
                                  audit.get("holder_count", 0),
                                  audit.get("organic_buyers_5m", 0))
                except Exception as exc:
                    log.debug("jup_audit failed for %s: %s", ca[:10], exc)
            pc = (snap or {}).get("price_change") or {}
            tfs = ("m5", "h1", "h6", "h24")
            avail = [k for k in tfs if pc.get(k) is not None]
            align = sum(1 for k in avail if (pc.get(k) or 0) > 0)
            # Multi-timeframe alignment: trend-shaped tokens (sling/SABL/Leafy:
            # green on all horizons) get a bonus; reversing/late ones (PINU:
            # +825% h24 but -56% h1) get a discount. Avoids entering tops.
            # No penalty when price data is unavailable (new tokens have no history).
            if avail:
                mkt_bonus = (align - 2) * s.mtf_align_bonus
            else:
                mkt_bonus = 0
            # Pair quality is a MULTIPLIER on the market score, not a veto: a weak
            # pair (AgmLJ+kEFiA) is down-weighted but may still trade when the
            # market confirms hard — so we don't overfit to a 6-trade sample.
            pmult, pnote = pair_multiplier(pair_perf, wallets)
            effective = (score + mkt_bonus) * pmult
            # Fresh-token flag: DexScreener has no m5/h1 history yet, so the
            # momentum gates below cannot evaluate. Such tokens route into
            # the pullback deferral (confirm-on-hold) instead of being
            # hard-skipped by gates that read missing data as 0.
            _nohist = pc.get("m5") is None and pc.get("h1") is None
            # Weak pair -> require strong confirmation: every AVAILABLE timeframe
            # positive (m5>0 & h1>0 at minimum) before it may open at all.
            # An empty book (no price history) is NOT confirmation — `all()`
            # over zero timeframes is vacuously True, so weak pairs must show
            # at least one positive timeframe or sit out.
            if pmult < 1.0 and (not avail or not all((pc.get(k) or 0) > 0 for k in avail)):
                reason = f"skip:pair_needs_confirmation({pnote},align={align}/{len(avail)})"
            elif effective < s.consensus_weight_threshold:
                reason = (f"skip:eff_score={effective:.2f}<{s.consensus_weight_threshold}"
                          f"(pmult={pmult:.2f},align={align})")
            elif not snap:
                # DexScreener blip with a genuine consensus: open flagged as
                # liq-unchecked rather than discarding the signal — but at
                # CAPPED size. Blind entries carried every catastrophic paper
                # loss (2026-09-12..15: -0.060/21, incl. five instant
                # -25..-38% rugs), so they never get full adaptive size.
                last_open["t"] = time.time()
                last_open["score"] = score
                logs.journal("open_liq_unchecked", ca=ca, symbol=sym,
                              note="dexscreener_unavailable")
                logs.journal("open_signal_momentum", ca=ca, symbol=sym,
                             score=score, effective=round(effective, 3),
                             pmult=pmult, align=align, price_change=pc,
                             source=source)
                _open_size = blind_open_size(
                    _adaptive_size(s, effective, source) if s.adaptive_sizing else None,
                    s.liq_unchecked_max_sol, s.size_sol)
                await book.open_position(ca, sym, usd, usd, n, wallets=wallets, size_sol=_open_size,
                                         source=source,
                                         mc=(snap or {}).get("mcap") or 0, score=score)
                if birdeye is not None and ca in book.open:
                    asyncio.get_running_loop().create_task(
                        _birdeye_enrich(ca, sym)).add_done_callback(_log_task_result)
                return
            elif (snap.get("liq") or tg_liq or 0) < s.open_min_liq_usd:
                reason = "skip:low_liq"
            elif (pc.get("h1") or 0) < s.open_min_h1_pct and not _nohist:
                reason = f"skip:no_momentum(h1={pc.get('h1')})"
            elif (pc.get("m5") or 0) < s.open_max_m5_dump_pct and not _nohist:
                reason = f"skip:dumping(m5={pc.get('m5')})"
            elif (pc.get("m5") or 0) < s.open_min_m5_pct and not _nohist:
                reason = f"skip:weak_m5(m5={pc.get('m5')})"
            elif _nohist or (pc.get("m5") or 0) > s.pullback_m5_pct:
                # Pullback entry: candle already vertical — don't chase.
                # Defer; a later signal in [wait, expire] may open if price
                # held within tol of the defer price. Falls through to the
                # normal open branch below only on a confirmed hold.
                _now_pb = time.time()
                _ref = snap.get("price_usd") or 0
                _pend = _pullback.get(ca)
                # Prune stale pendings opportunistically
                for _k in [k for k, v in _pullback.items()
                           if _now_pb - v.get("t", 0) > s.pullback_expire_s]:
                    _pullback.pop(_k, None)
                _age = _now_pb - (_pend.get("t", 0) if _pend else 0)
                if (_pend and _ref > 0 and (_pend.get("ref") or 0) > 0
                        and s.pullback_wait_s <= _age <= s.pullback_expire_s
                        and _ref / _pend["ref"] >= 1.0 - s.pullback_tol_pct / 100.0):
                    _pullback.pop(ca, None)
                    logs.journal("pullback_hold", ca=ca, symbol=sym,
                                 m5=pc.get("m5"), held=round(_ref / _pend["ref"], 4),
                                 wait_s=int(_age), source=source)
                    last_open["t"] = time.time()
                    last_open["score"] = score
                    logs.journal("open_signal_momentum", ca=ca, symbol=sym,
                                 score=score, effective=round(effective, 3),
                                 pmult=pmult, align=align, price_change=pc,
                                 source=source, note="pullback_hold")
                    _open_size = _adaptive_size(s, effective, source) if s.adaptive_sizing else None
                    await book.open_position(ca, sym, usd, usd, n, wallets=wallets, size_sol=_open_size,
                                             source=source,
                                             mc=(snap or {}).get("mcap") or 0, score=score)
                    if birdeye is not None and ca in book.open:
                        asyncio.get_running_loop().create_task(
                            _birdeye_enrich(ca, sym)).add_done_callback(_log_task_result)
                    return
                if _pend and _age > s.pullback_expire_s:
                    _pullback.pop(ca, None)
                    reason = f"skip:pullback_expired(m5={pc.get('m5')})"
                elif _pend and _ref > 0 and (_pend.get("ref") or 0) > 0:
                    _held = _ref / _pend["ref"]
                    if _held < 1.0 - s.pullback_tol_pct / 100.0:
                        _pullback.pop(ca, None)
                        reason = f"skip:pullback_failed(held={_held:.3f})"
                    else:
                        reason = f"skip:vertical_hold(m5={pc.get('m5')},wait={int(_age)}s)"
                else:
                    if _ref > 0:
                        _pullback[ca] = {"t": _now_pb, "ref": _ref}
                    reason = f"skip:vertical_hold(m5={pc.get('m5')})"
            else:
                last_open["t"] = time.time()
                last_open["score"] = score
                logs.journal("open_signal_momentum", ca=ca, symbol=sym,
                             score=score, effective=round(effective, 3),
                             pmult=pmult, align=align, price_change=pc,
                             source=source)
                _open_size = _adaptive_size(s, effective, source) if s.adaptive_sizing else None
                await book.open_position(ca, sym, usd, usd, n, wallets=wallets, size_sol=_open_size,
                                         source=source,
                                         mc=(snap or {}).get("mcap") or 0, score=score)
                if birdeye is not None and ca in book.open:
                    asyncio.get_running_loop().create_task(
                        _birdeye_enrich(ca, sym)).add_done_callback(_log_task_result)
                return
            if reason and _skip_log.get(ca, 0) < time.time() - 300:
                _skip_log[ca] = time.time()
                # pumpapi firehose is journal-only by design (343 distinct CAs
                # in ~2h on 2026-09-14): journal keeps the signal, DEBUG keeps
                # the log readable. All real gates stay at INFO.
                if reason == "skip:pumpapi_journal_only":
                    log.debug("open deferred %s (%s): %s", ca[:10], sym, reason)
                else:
                    log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
            return
        now = time.time()
        if reason and _skip_log.get(ca, 0) < now - 300:
            _skip_log[ca] = now
            if reason == "skip:pumpapi_journal_only":
                log.debug("open deferred %s (%s): %s", ca[:10], sym, reason)
            else:
                log.info("open deferred %s (%s): %s", ca[:10], sym, reason)

    w.on_smart_buy = _on_smart_buy

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_, stop.set)

    started = time.time()
    alerts = {"n": 0}
    last_detection_ts = {"t": started}  # updated on any consensus event

    async def status_loop() -> None:
        while not stop.is_set():
            await asyncio.sleep(max(60, s.status_every_min * 60))
            helius_ok = helius_ws.connected if helius_ws else "off"
            if not helius_ok and helius_ws is not None and helius_ws.degraded:
                helius_ok = "degraded"
            snap = book.snapshot(len(w.wallets), alerts["n"],
                                 w.consensus_fired, time.time() - started,
                                 {"dexscreener": True,
                                  "pumpapi": pump_stream.connected,
                                  "helius_ws": helius_ok,
                                  "vybe": vybe is not None and vybe.enabled,
                                  "cabalspy": cabalspy_client is not None and cabalspy_client.connected,
                                  "kolexplorer": kolexplorer_feed is not None and kolexplorer_feed._running,
                                  "memetracker": memetracker_feed is not None and memetracker_feed.health()["connected"]})
            log.info("status: %s", build_status(snap))
            if helius_ws:
                hs = helius_ws.stats
                log.info("helius ws: connected=%s msgs=%d buys=%d reconnects=%d",
                         hs["connected"], hs["total_msgs"],
                         hs["total_buys"], hs["reconnects"])
            ps = pump_stream.stats
            log.info("pumpapi: connected=%s buys=%d reconnects=%d uptime=%ds",
                     ps["connected"], ps["total_buys"],
                     ps["reconnects"], ps["uptime_s"])
            if cabalspy_client:
                cs = cabalspy_client.stats
                log.info("cabalspy: connected=%s signals=%d txs=%d holders=%d bundles=%d reconnects=%d",
                         cs["connected"], cs["total_signals"], cs["total_txs"],
                         cs["total_holders"], cs["total_bundles"], cs["reconnects"])
            if memetracker_feed is not None:
                # Flow counters (not just "connected"): a deaf TG listener
                # reports connected forever — silence must be visible.
                mh = memetracker_feed.health()
                _mt_age = (round(time.time() - mh["last_event_at"])
                           if mh["last_event_at"] else None)
                log.info("memetracker: msgs=%d parsed=%d forwarded=%d filtered=%d errors=%d last_event_age_s=%s",
                         mh["messages"], mh["parsed"], mh["forwarded"],
                         mh["filtered"], mh["errors"], _mt_age)
            if birdeye is not None:
                bs = birdeye.stats
                log.info("birdeye: calls=%d cached=%d errors=%d",
                         bs["calls"], bs["cached"], bs["errors"])

    # Live feeds are push-based (Helius WS, PumpAPI WS, CabalSpy WS,
    # Kolexplorer poll, MemeTracker TG) — no webhook receiver needed
    # (Tatum push removed 2026-09-12: never configured, dead port :8787).

    log.info("bot started: %s", build_status(book.snapshot(
        len(w.wallets), 0, 0, 0, {"dexscreener": True,
                                   "memetracker": memetracker_feed.health()["connected"] if memetracker_feed else False,
                                    "pumpapi": True,
                                    "vybe": vybe is not None and vybe.enabled})))
    if notifier is not None:
        asyncio.create_task(notifier.send_startup(
            summary=f"watching {len(w.wallets)} wallets · "
                    f"balance {s.start_balance_sol:.2f} SOL · "
                f"E4 ladder cw={s.watch_consensus_window_s:.0f}s · "
                f"weight_thr={s.consensus_weight_threshold}")
        ).add_done_callback(_log_task_result)
    w.start()

    async def _enable_live_opens() -> None:
        secs = float(s.watch_first_lookback_s)
        log.info("initial lookback: deferring live opens for %.0fs", secs)
        remaining = secs
        while remaining > 0:
            await asyncio.sleep(min(30.0, remaining))
            remaining -= 30.0
            if remaining > 0:
                log.info("still in initial lookback — %.0fs remaining", remaining)
        backfill_done.set()
        log.info("initial lookback complete — live position opening enabled")

    asyncio.create_task(_enable_live_opens()).add_done_callback(_log_task_result)

    async def _status_with_restart() -> None:
        """Run status_loop, auto-restart on crash."""
        while not stop.is_set():
            try:
                await status_loop()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("status_loop crashed — restarting in 60s")
                await asyncio.sleep(60)

    status_task = asyncio.create_task(_status_with_restart())
    status_task.add_done_callback(_log_task_result)

    async def _feed_watchdog() -> None:
        """Watch primary feeds; Telegram-alert on outage, log on recovery.

        PumpAPI reconnect storms and CabalSpy handshake failures have both
        caused silent blind spots. Alerts are rate-limited to 1 per 30min
        per feed so a flapping feed doesn't spam.
        """
        down_since: dict[str, float] = {}   # feed -> first-seen-down ts
        alerted: dict[str, float] = {}      # feed -> last alert ts
        was_up: dict[str, bool] = {"pumpapi": True, "cabalspy": True}
        while not stop.is_set():
            await asyncio.sleep(60)
            try:
                feeds = {
                    "pumpapi": pump_stream.connected,
                    "cabalspy": (cabalspy_client.connected
                                 if cabalspy_client else False),
                    # Log-only: Helius spends hours 429-banned (quota-side);
                    # Telegram would spam every 30min for something we can't
                    # fix from here. cabalspy is log-only for the same reason.
                    "helius_ws": (helius_ws.connected
                                  if helius_ws else True),
                }
                now = time.time()
                for name, up in feeds.items():
                    if not up:
                        down_since.setdefault(name, now)
                        if (was_up.get(name, True)
                                or now - alerted.get(name, 0) > 1800):
                            down_for = int(now - down_since[name])
                            # Helius 429 bans are quota-side and unactionable
                            # from here (observed: multi-hour bans, feed stays
                            # 🟡 degraded). Keep it at INFO so the log stops
                            # crying WARNING every 30min over it; pumpapi and
                            # cabalspy outages stay WARNING (actionable blind
                            # spots).
                            _degraded_helius = (
                                name == "helius_ws" and helius_ws is not None
                                and getattr(helius_ws, "degraded", False))
                            if _degraded_helius:
                                log.info("watchdog: %s DOWN for %ds (quota ban, degraded)",
                                         name, down_for)
                            else:
                                log.warning("watchdog: %s DOWN for %ds", name, down_for)
                            alerted[name] = now
                            if notifier is not None and name == "pumpapi":
                                asyncio.create_task(notifier.send_alert(
                                    f"feed down: {name}",
                                    f"no data for {down_for}s")).add_done_callback(
                                        _log_task_result)
                        was_up[name] = False
                    elif not was_up.get(name, True):
                        down_for = int(now - down_since.pop(name, now))
                        log.info("watchdog: %s recovered after %ds down",
                                 name, down_for)
                        was_up[name] = True
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("feed watchdog failed")

    asyncio.create_task(_feed_watchdog()).add_done_callback(_log_task_result)

    try:
        while not stop.is_set():
            # Adaptive polling: 1s when any position is fresh (< early_filter_window_s),
            # 5s otherwise.  This catches the 30-second adverse window without
            # burning CPU when all positions are mature.
            now = time.time()
            has_fresh = any(
                (now - p.get("ts", now)) < book.early_filter_window_s
                for p in book.open.values()
            )
            manage_s = 1.0 if has_fresh else 5.0
            await asyncio.sleep(manage_s)
            # Never let a transient pricing/quote error kill the whole bot; a
            # single bad token must not take down the live book.
            try:
                await book.refresh_prices()
            except Exception:
                log.exception("refresh_prices failed this cycle; continuing")
            # Feed watchdog: alert if no detections for >5 min with open
            # positions (the bot might be blind), or >15 min regardless.
            silence_s = time.time() - last_detection_ts["t"]
            has_open = bool(book.open)
            watchdog_thresh = 300 if has_open else 900
            if silence_s > watchdog_thresh and notifier is not None:
                alert_key = f"watchdog_{int(silence_s // 60)}"
                # Only alert once per minute-bucket to avoid spam
                if alerts.get(alert_key, 0) == 0:
                    alerts[alert_key] = 1
                    open_cas = list(book.open.keys())[:3]
                    await notifier._send(
                        f"⚠️ **Feed watchdog**\n"
                        f"▸ No detections for {int(silence_s)}s\n"
                        f"▸ Open: {', '.join(c[:8] for c in open_cas) or 'none'}"
                    )
    finally:
        try:
            if notifier is not None:
                asyncio.create_task(notifier.send_stopped(book.snapshot(
                    len(w.wallets), alerts["n"], w.consensus_fired,
                    time.time() - started,
                    {"dexscreener": True}))
                ).add_done_callback(_log_task_result)
        except Exception:
            log.exception("send_stopped failed")
        status_task.cancel()
        pump_stream.stop()
        pump_task.cancel()
        await w.stop()
        await ds.close()
        await dp.close()
        await dbx.close()
        if madeonsol is not None:
            await madeonsol.stop()
        if rugcheck is not None:
            await rugcheck.close()
        if helius is not None:
            await helius.close()
        if vybe is not None:
            await vybe.close()
        if birdeye is not None:
            await birdeye.close()
        if cabalspy_client is not None:
            await cabalspy_client.stop()
        if cabalspy_rest is not None:
            await cabalspy_rest.close()
        if kolexplorer_feed is not None:
            await kolexplorer_feed.stop()
        await jupiter.close()
    return 0


def cmd_watch(args) -> int:
    return asyncio.run(_run_watch(cfg.load_settings()))


async def _run_discover(s: cfg.Settings, args=None) -> int:
    env = cfg.load_env()
    debot_enabled = bool(cfg.get(env, "DEBOT_ENABLED", "1") not in ("0", "false", "no"))
    debot = None
    if debot_enabled:
        try:
            from debot import DeBotClient

            debot = DeBotClient(enabled=True)
            await debot.warmup()
        except Exception:
            log.warning("debot unavailable for discovery — using DexPaprika only")
    disc = WalletDiscovery(
        debot=debot,
        chain="solana",
        max_tokens=s.discover_max_tokens,
        max_wallets=s.discover_max_wallets,
        early_window_s=s.discover_early_window_s,
        tx_per_pool=s.discover_tx_per_pool,
        min_buy_usd=s.discover_min_buy_usd,
        max_buy_usd=s.discover_max_buy_usd,
        out_file=s.discover_out_file,
        pump_pct=s.discover_pump_pct,
        enrich=s.discover_enrich,
        replace=bool(getattr(args, "replace", False)),
        write_top_n=int(getattr(args, "top", 0) or 0),
    )
    top = await disc.run()
    await disc.close()
    if debot is not None:
        await debot.aclose()
    print(f"discovered {len(top)} candidate wallets "
          f"(written to {s.discover_out_file})")
    for st in top[:15]:
        print(f"  {st.address}  score={st.score:.3f}  "
              f"tokens={st.distinct_tokens} early={st.early_buys} "
              f"pumped={st.pumped_hits} ${st.total_usd:,.0f}")
    return 0


def cmd_discover(args) -> int:
    return asyncio.run(_run_discover(cfg.load_settings(), args))


def cmd_status(_args) -> int:
    book = ShadowBook(DexScreenerClient(), 0, 0, 0,
                      Path(cfg.load_settings().shadow_state_file), 0)
    st = json.loads(Path("watcher_state.json").read_text()) \
        if Path("watcher_state.json").exists() else {}
    wallets_n = len([k for k in st if k.startswith("ts:")])
    snap = book.snapshot(wallets_n,
                          st.get("alerts", 0), st.get("consensus", 0),
                          0, {})
    print(build_status(snap))
    return 0


# ------------------------------------------------------ throwaway wallet --
THROWAWAY_FILE = Path("throwaway_key.json")


def _load_throwaway() -> Keypair | None:
    if not THROWAWAY_FILE.exists():
        return None
    secret = json.loads(THROWAWAY_FILE.read_text())["secret"]
    return Keypair.from_base58_string(secret)


def cmd_wallet_new(_args) -> int:
    kp = Keypair.from_seed(os.urandom(32))
    secret = base58.b58encode(kp.to_bytes()).decode()
    THROWAWAY_FILE.write_text(json.dumps(
        {"pubkey": str(kp.pubkey()), "secret": secret}, indent=1))
    THROWAWAY_FILE.chmod(0o600)
    print(f"throwaway wallet: {kp.pubkey()}")
    print("fund it with a small amount of SOL, then:")
    print("  uv run main.py sim <CA> --size 0.05 --live --yes")
    return 0


def cmd_wallet_show(_args) -> int:
    kp = _load_throwaway()
    if kp is None:
        print("no throwaway wallet — run: uv run main.py wallet-new")
        return 1

    async def _run():
        j = JupiterSwap(dry_run=False,
                        private_key=base58.b58encode(kp.to_bytes()).decode())
        bal = await j.balance_sol() or 0.0
        print(f"{kp.pubkey()}  balance={bal:.4f} SOL")

    return asyncio.run(_run()) or 0


def _affordable_size(balance_sol: float, want_sol: float,
                     buffer_sol: float = 0.008,
                     dust_sol: float = 0.005) -> tuple[float | None, str]:
    """Clamp a live order size to what the wallet can actually fund.

    Returns (size_to_use, message). ``message`` is "" when no adjustment was
    needed; ``size_to_use`` None means even dust doesn't fit — abort instead
    of letting Jupiter answer with a cryptic 400. Buffer covers ATA rent
    (~0.0041 for payer+spl) + tx/priority fees + dust, same basis as the
    quote-gate pre-flight (6M lamports) with margin.
    """
    affordable = round(balance_sol - buffer_sol, 4)
    if affordable <= dust_sol:
        return None, (f"insufficient funds: wallet {balance_sol:.4f} SOL can't cover "
                      f"rent+fees (~{buffer_sol:.4f}); fund it or lower --size")
    if want_sol > affordable:
        return affordable, (f"downsized {want_sol:.4f} -> {affordable:.4f} SOL "
                            f"(wallet {balance_sol:.4f} SOL incl rent+fees buffer)")
    return want_sol, ""


# Human explanations for sell-quote failures (sim output + logs). The raw
# reason codes ("quote_no_route") read as "quote not found" to users.
_SELL_FAIL_HINTS = {
    "quote_no_route": "no sell route — pool drained/dead or too illiquid; tokens stay put",
    "quote_impact": "sell impact over cap — try a smaller size or wait for liquidity",
    "quote_timeout": "Jupiter timed out (retried) — transient, try again",
    "quote_rate_limited": "rate limited — wait a minute and retry",
    "quote_insufficient_funds": "wallet can't cover tx fees — add a little SOL",
    "quote_invalid_response": "bad response from Jupiter — try again",
    "quote_http_error": "Jupiter HTTP error (retried) — try again shortly",
    "quote_exception": "internal error — check logs",
}


def cmd_sim(args) -> int:
    """Jupiter round-trip for a CA — paper by default, live via --live."""
    size = args.size or cfg.load_settings().size_sol
    kp = _load_throwaway()
    live = args.live and kp is not None
    if args.live and kp is None:
        print("--live needs a throwaway wallet: uv run main.py wallet-new")
        return 1
    if live and not args.yes:
        print(f"this spends REAL SOL from {str(kp.pubkey())[:8]}… "
              f"add --yes to confirm")
        return 1
    if live and size > 0.2:
        print(f"size {size} SOL exceeds throwaway cap 0.2 — lower --size")
        return 1

    async def _run():
        nonlocal size
        j = (JupiterSwap(dry_run=False, private_key=base58.b58encode(
                kp.to_bytes()).decode()) if live else JupiterSwap(dry_run=True))
        if live:
            # Balance-aware sizing: a broke wallet gets generic-400 "no route"
            # from Jupiter. Size down (or abort) up front with a clear message
            # instead — and prime the quote-gate pre-flight balance cache.
            bal = await j.balance_sol()
            if bal is None:
                print("BAL    : ⚠ RPC balance check failed — proceeding, Jupiter decides")
            else:
                print(f"BAL    : {bal:.4f} SOL wallet={str(kp.pubkey())[:8]}…")
                size2, msg = _affordable_size(bal, size)
                if msg:
                    print(f"SIZE   : {msg}")
                if size2 is None:
                    await j.close()
                    return 1
                size = size2
        ds = DexScreenerClient(
            base_url=cfg.load_settings().dexscreener_base_url)
        snap = await ds.token_pairs("solana", args.ca)
        # token_pairs() returns a normalized single-pair dict or None
        if snap:
            print(f"market : ${float(snap.get('price_usd') or 0):.8f} "
                  f"liq=${snap.get('liq') or 0:,.0f} "
                  f"mcap=${snap.get('mcap') or 0:,.0f} "
                  f"m5={snap.get('vol_m5')}% h1={snap.get('vol_h1')}%")

        q = await j.quote(args.ca, int(size * 1e9), force=True)
        if q is None or not q.success:
            print(f"BUY    : ✗ no route ({q.reason if q else 'exception'})")
            return 0
        dec = await j.token_decimals(args.ca) or 6
        tokens_raw = q.output_amount
        tokens = tokens_raw / (10 ** dec)
        entry = size / tokens if tokens else 0
        print(f"BUY    : ✓ {size} SOL -> {tokens:,.0f} "
              f"@{entry:.10g} impact={q.price_impact_pct:.2f}% "
              f"router={q.router}/{q.mode}")

        if live:
            res = await j.execute_order(q.order)
            if not res.success:
                print(f"BUY EXEC ✗ {res.error}")
                return 1
            tokens_raw = res.output_amount
            tokens = tokens_raw / (10 ** dec)
            entry = size / tokens
            print(f"BUY EXEC ✓ sig={res.signature[:16]}… "
                  f"{tokens:,.0f} tokens @ {entry:.10g}")

        if live:
            held = await j.token_balance(args.ca)
            if held == 0:
                print("SELL   : ✗ no tokens held in wallet — buy leg failed or "
                      "tokens were swept; nothing to sell")
                await ds.close()
                await j.close()
                return 1
            # held None (RPC hiccup) -> proceed to quote, Jupiter decides.
        sq = await j.quote_sell(args.ca, tokens_raw)
        if sq is None or not sq.success:
            reason = sq.reason if sq else "exception"
            hint = _SELL_FAIL_HINTS.get(reason, "")
            print(f"SELL   : ✗ {reason}" + (f" — {hint}" if hint else "") +
                  f" (holding {tokens:,.0f} tokens — sell via jup.ag)")
            return 1 if live else 0
        sol_back = sq.output_amount / 1e9
        print(f"SELL   : quote -> {sol_back:.6f} SOL "
              f"impact={sq.price_impact_pct:.2f}%")

        if live:
            sres = await j.sell(args.ca, tokens_raw)
            if not sres.success:
                print("SELL EXEC ✗ — TOKENS STILL IN WALLET: "
                      f"{sres.error}")
                return 1
            sol_back = sres.output_amount / 1e9
            print(f"SELL EXEC ✓ sig={sres.signature[:16]}…")

        pnl = sol_back - size
        tag = "LIVE" if live else "PAPER"
        print(f"ROUNDTRIP[{tag}]: {pnl:+.6f} SOL ({pnl/size*100:+.2f}%) — "
              f"instant-exit cost incl fees+impact")
        await ds.close()
        await j.close()
        return 0

    return asyncio.run(_run())


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    watch = sub.add_parser("watch", help="run the 24/7 watcher")
    watch.set_defaults(func=cmd_watch)
    st = sub.add_parser("status", help="print status card")
    st.set_defaults(func=cmd_status)
    disc = sub.add_parser(
        "discover",
        help="batch-find smart-money wallets -> smart_money_wallets.json")
    disc.add_argument("--top", type=int, default=0,
                      help="only write the top-N scored wallets (0 = all)")
    disc.add_argument("--replace", action="store_true",
                      help="overwrite the wallet file instead of merging")
    disc.set_defaults(func=cmd_discover)
    sim = sub.add_parser("sim", help="Jupiter buy+sell round-trip for a CA")
    sim.add_argument("ca")
    sim.add_argument("--size", type=float, default=None,
                     help="override SIZE_SOL")
    sim.add_argument("--live", action="store_true",
                     help="execute on the THROWAWAY wallet (real SOL)")
    sim.add_argument("--yes", action="store_true", help="confirm live spend")
    sim.set_defaults(func=cmd_sim)
    wn = sub.add_parser("wallet-new", help="create throwaway wallet")
    wn.set_defaults(func=cmd_wallet_new)
    ws = sub.add_parser("wallet-show", help="throwaway address/balance")
    ws.set_defaults(func=cmd_wallet_show)
    return ap


if __name__ == "__main__":
    ap_ = build_parser()
    args_ = ap_.parse_args()
    log_file = "watcher.log" if args_.func is cmd_watch else None
    setup_logging(log_file=log_file)
    sys.exit(args_.func(args_) or 0)
