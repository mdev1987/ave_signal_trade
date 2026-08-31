"""Smart-money watcher — the only strategy.

    uv run main.py watch          # 24/7: alerts + shadow paper book + status
    uv run main.py tatum-setup    # register push subscriptions (needs public URL)
    uv run main.py status         # one-shot status card

Pipeline:
  Tatum ADDRESS_EVENT push (or Helius/Moralis polling fallback)
    → smart wallet bought something new
      → 🕵️/🔥 Telegram alert
      → shadow paper position opens at DexScreener price
        → trailing 40% / hard stop managed virtually → paper PnL stats
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

import base58  # noqa: E402
import os  # noqa: E402
import config as cfg  # noqa: E402
import logs  # noqa: E402
from dexscreener import DexScreenerClient  # noqa: E402
from dbotx import DBotXClient  # noqa: E402
from jupiter_swap import JupiterSwap  # noqa: E402
from solders.keypair import Keypair  # noqa: E402
from logs import setup_logging  # noqa: E402
from pair_perf import (load as load_pair_perf, save as save_pair_perf,  # noqa: E402
                       update as update_pair_perf, pair_multiplier)
from notifier import TelegramNotifier  # noqa: E402
from pump_stream import PumpApiStream  # noqa: E402
from tatum_notify import TatumNotifications  # noqa: E402
from watcher import SmartWalletWatcher  # noqa: E402
from wallet_discovery import WalletDiscovery  # noqa: E402
from wallet_weights import build_weights  # noqa: E402

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
    wins = sum(1 for c in closed if c.get("pnl_sol", 0) > 0)
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
    pnl = sum(c.get("pnl_sol", 0) for c in closed) + open_pnl
    pct = (pnl / start * 100.0) if start else 0.0
    icon = "🟢" if pnl >= 0 else "🔴"

    lines = [
        f"🕵️ **Smart-Watch** · {uptime}",
        f"👁️ {st.get('wallets', 0)} wallets · "
        f"🚨 {st.get('alerts', 0)} alerts (🔥{st.get('consensus', 0)})",
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
    wr = (wins / len(closed) * 100) if closed else 0.0
    lines.append(f"▸ Closed: {len(closed)} · win {wr:.0f}%")
    lines.append(f"{icon} **PnL `{pnl:+.4f}` SOL ({pct:+.1f}%)**")
    feeds = st.get("feeds") or {}
    feed_line = " · ".join(
        f"{'🟢' if ok else '🔴'} {name}" for name, ok in feeds.items())
    if feed_line:
        lines.append(feed_line)
    return "\n".join(lines)


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
                  trail_enabled: bool = False,
                  on_trade_close=None,
                  open_max_impact_pct: float = 4.0,
                 early_filter_window_s: float = 30.0,
                 early_filter_dd_pct: float = 20.0,
                 early_filter_gain_pct: float = 5.0) -> None:
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
        self.tp_ladder = tp_ladder or [(tp1_mult, 0.5)]
        self.trail_enabled = bool(trail_enabled)
        self.on_trade_close = on_trade_close  # async/normal fn(wallets, win: bool, pnl: float)
        self.open_max_impact_pct = float(open_max_impact_pct)
        self.early_filter_window_s = float(early_filter_window_s)
        self.early_filter_dd = float(early_filter_dd_pct) / 100.0  # store as fraction
        self.early_filter_gain = float(early_filter_gain_pct) / 100.0  # store as fraction
        self.state_file = state_file
        self.start_balance_sol = float(start_balance_sol)
        self.balance_sol = float(start_balance_sol)
        self.open: dict[str, dict] = {}
        self.closed: list[dict] = []
        self._lock = asyncio.Lock()
        self._load()

    def _win_rate(self) -> float:
        if not self.closed:
            return 0.0
        wins = sum(1 for c in self.closed if c.get("pnl_sol", 0.0) > 0.0)
        return wins / len(self.closed) * 100.0

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
        except Exception:
            log.exception("shadow book load failed")

    def save(self) -> None:
        try:
            self.state_file.write_text(json.dumps({
                "open": self.open, "closed": self.closed,
                "start_balance_sol": self.start_balance_sol,
                "balance_sol": self.balance_sol}, indent=1))
        except Exception:
            log.exception("shadow book save failed")

    async def _sol_usd(self) -> float:
        """Current SOL price in USD, used to derive a USD entry from a SOL quote."""
        try:
            s = await self.ds.token_pairs("solana", _SOL_MINT)
            return float(s.get("price_usd") or 0) if s else 0.0
        except Exception:
            return 0.0

    async def open_position(self, ca: str, symbol: str, usd_entry: float,
                            trigger_usd: float, n_wallets: int,
                            wallets: list[str] | None = None) -> None:
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

        # DexScreener snapshot: used for market context (liq, price_change) and
        # as fallback when Jupiter is unavailable.
        snap = await self.ds.token_pairs("solana", ca)
        market_px = float(snap.get("price_usd") or 0) if snap else 0.0

        if self.jupiter is not None:
            q = await self.jupiter.quote(ca, int(self.size_sol * 1e9),
                                         force=True)
            if q is None or not q.success:
                reason = q.reason if q else "quote_exception"
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason=f"no_buy_route:{reason}")
                log.info("shadow skip %s (%s): no buy route: %s", ca[:10], symbol, reason)
                return
            tokens_raw = q.output_amount
            entry_note = f"jup impact={q.price_impact_pct:.2f}%"
            if self.open_max_impact_pct > 0 and q.price_impact_pct > self.open_max_impact_pct:
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason=f"untradable:impact{q.price_impact_pct:.2f}%")
                log.info("shadow skip %s (%s): impact %.2f%%",
                         ca[:10], symbol, q.price_impact_pct)
                return
            if self.jupiter.quote_stability_checks > 0:
                buy_slip = None if self.jupiter._buy_rtse else self.jupiter._slippage_bps
                stable, stab_reason, stab_info = await self.jupiter.check_quote_stability(
                    ca, int(self.size_sol * 1e9), base=q, slippage_bps=buy_slip)
                if not stable:
                    logs.journal("shadow_skip", ca=ca, symbol=symbol,
                                 reason=f"unstable:{stab_reason}", info=stab_info)
                    log.info("shadow skip %s (%s): %s", ca[:10], symbol, stab_reason)
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
                exec_px = (self.size_sol * sol_usd) / (tokens_raw / (10 ** dec))
        # Use Jupiter executable price as canonical entry when available;
        # fall back to DexScreener mid only when Jupiter is absent.
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
            if self.balance_sol < self.size_sol:
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason="insufficient_balance")
                log.info("shadow skip %s (%s): insufficient balance %.4f",
                          ca[:10], symbol, self.balance_sol)
                return
            bal_before = self.balance_sol
            self.balance_sol -= self.size_sol
            self.open[ca] = {
                "symbol": symbol, "entry_usd": px, "peak_usd": px, "last_usd": px,
                "market_entry_px": market_px, "tokens_raw": tokens_raw,
                "entry_note": entry_note,
                "size_sol": self.size_sol, "ts": time.time(),
                "trigger_usd": trigger_usd, "n_wallets": n_wallets,
                "wallets": list(wallets or []),
                "tp_taken": [], "remaining": 1.0, "banked_pnl": 0.0,
                "be_armed": False, "peak_mult": 1.0,
                "entry_mode": entry_mode,
                # Early adverse filter state (one-shot at early_filter_window_s)
                "early_min_mult": 1.0,  # worst excursion during early window
                "early_max_mult": 1.0,  # best excursion during early window
                "early_checked": False,  # True after filter evaluated
            }
            logs.journal("shadow_entry_px", ca=ca, px=px, note=entry_note)
            logs.journal("shadow_open", ca=ca, symbol=symbol, entry_usd=px,
                         trigger=trigger_usd, n=n_wallets,
                         wallets=list(wallets or []))
            self.save()
        if self.notifier is not None:
            try:
                asyncio.get_running_loop().create_task(self.notifier.send_open(
                    ca=ca, name=symbol, price=px, size_sol=self.size_sol,
                    balance_before=bal_before, balance_after=self.balance_sol,
                    open_count=len(self.open), max_positions=self.max_positions,
                    n_wallets=n_wallets, trigger_usd=trigger_usd,
                    win_rate=self._win_rate(), wallets=wallets))
            except Exception:
                log.exception("send_open failed")

    async def refresh_prices(self) -> None:
        # Single lock around the whole scan: refresh and open_position run in
        # different tasks, and both read/write self.open / self.balance_sol.
        # Serializing them prevents lost updates (e.g. an open landing in the
        # middle of a close, or a balance miscount).
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
            for ca in fresh + mature:
                pos = self.open[ca]
                entry = pos["entry_usd"]
                # --- price discovery: prefer Jupiter executable sell quote
                # (authoritative for what we'd actually get on exit); fall
                # back to DexScreener mid only when Jupiter is unavailable.
                jup_mult = None
                dex_mult = None
                if self.jupiter is not None and pos.get("tokens_raw"):
                    # Quote sell for the REMAINING tokens only (not the full
                    # position).  After a partial TP, remaining may be <1.0,
                    # so the sell quote reflects the actual executable size.
                    remaining_raw = int(pos["tokens_raw"] * pos.get("remaining", 1.0))
                    if remaining_raw > 0:
                        try:
                            sq = await self.jupiter.quote_sell(ca, remaining_raw)
                            if sq is not None and sq.success:
                                jup_mult = (sq.output_amount / 1e9) / \
                                    (pos["size_sol"] * pos.get("remaining", 1.0))
                                pos["exit_note"] = f"jup impact={sq.price_impact_pct:.2f}%"
                        except Exception:
                            log.exception("refresh jup quote failed %s", ca[:10])
                snap = await self.ds.token_pairs("solana", ca)
                if snap and snap.get("price_usd"):
                    px = float(snap["price_usd"])
                    dex_mult = px / entry if entry else 0
                    pos["last_usd"] = px
                # Track peak using ONLY the executable price (Jupiter when
                # available, DexScreener only as fallback).  A non-executable
                # DEX spike must not arm TP/trail at a price we can't sell at.
                best_mult = jup_mult if jup_mult is not None else dex_mult
                if best_mult is not None and best_mult > 0:
                    pos["peak_usd"] = max(pos["peak_usd"],
                                          pos["entry_usd"] * best_mult)
                    pos["peak_mult"] = max(pos.get("peak_mult", 1.0), best_mult)
                # Use Jupiter price as authoritative for exit decisions.
                # Fall back to DexScreener only when Jupiter is unavailable.
                mult = jup_mult if jup_mult is not None else dex_mult
                if mult is None:
                    continue  # can't price this cycle; leave open (safe)
                peak_mult = pos.get("peak_mult", mult)
                exit_reason = None
                # ---- early adverse filter (one-shot at early_filter_window_s):
                # Track worst/best excursion during the early window, then
                # evaluate once.  If the position drew down >early_filter_dd
                # AND never gained >early_filter_gain, close immediately.
                # This is the key finding from the 2026-08-13 ablation:
                # rejecting trades with >20% adverse AND <5% favorable in
                # first 30s turns gross PnL from -0.447 to +0.335 SOL.
                age_s = time.time() - pos["ts"]
                if not pos.get("early_checked", False):
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
                if exit_reason != "early_invalid":
                    for lvl, frac in self.tp_ladder:
                        if lvl in pos["tp_taken"]:
                            continue
                        if peak_mult >= lvl:
                            exec_at_level = min(mult, lvl) if mult < lvl else lvl
                            pos["tp_taken"].append(lvl)
                            pos["banked_pnl"] += frac * pos["size_sol"] * (exec_at_level - 1.0)
                            pos["remaining"] = max(0.0, pos["remaining"] - frac)
                            logs.journal("shadow_tp", ca=ca, symbol=pos["symbol"],
                                         lvl=lvl, frac=frac,
                                         exec_px=round(exec_at_level, 3))
                            if pos["remaining"] <= 1e-9:
                                pos["remaining"] = 0.0
                    if pos["tp_taken"] and not pos["be_armed"]:
                        pos["be_armed"] = True
                        logs.journal("shadow_be", ca=ca, symbol=pos["symbol"])
                    if pos["remaining"] <= 0:
                        exit_reason = "tp"   # fully scaled out at the spike
                    else:
                        stop_mult = (1 - self.hard_stop)
                        if pos["be_armed"]:
                            stop_mult = max(stop_mult, 1.0 + self.be_buffer)
                        if self.hard_stop > 0 and mult <= stop_mult:
                            exit_reason = "sl"
                        elif self.trail_enabled and peak_mult >= self.trail_start_mult and \
                                mult <= peak_mult * (1 - self.retrace):
                            exit_reason = "trail"
                        elif self.max_hold_s > 0 and (time.time() - pos["ts"]) > self.max_hold_s:
                            exit_reason = "timeout"
                if exit_reason:
                    pnl = pos.get("banked_pnl", 0.0) + \
                        pos["remaining"] * pos["size_sol"] * (mult - 1.0)
                    # Trade-level multiple (incl. any banked TP) for honest
                    # reporting — the exit-leg `mult` alone misleads when a
                    # partial was already banked (e.g. Bear: exit 0.70x but net +).
                    trade_mult = (pos["size_sol"] + pnl) / pos["size_sol"]
                    rec = {"ca": ca, "symbol": pos["symbol"], "reason": exit_reason,
                            "mult": round(trade_mult, 3), "pnl_sol": round(pnl, 5),
                            "hold_min": int((time.time() - pos["ts"]) / 60),
                            "wallets": pos.get("wallets", [])}
                    self.closed.append(rec)
                    bal_before = self.balance_sol
                    self.balance_sol += self.size_sol + pnl
                    del self.open[ca]
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
            self.save()

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
    shyft_key = (cfg.get(env, "SHYFT_API_KEY") or "").strip()
    if not shyft_key:
        log.critical("SHYFT_API_KEY missing — watcher cannot poll wallets. "
                     "Copy the key from .env.bak (or your VPS .env) and retry.")
        return 2
    notifier = TelegramNotifier()
    ds = DexScreenerClient(base_url=s.dexscreener_base_url,
                           rpm=s.dexscreener_rpm)
    # Fail-open rug/safety filter (DBotX). Degrades to allow on any error.
    dbx = DBotXClient(api_key=s.dbotx_api_key, base_url=s.dbotx_base_url)

    # Data-driven wallet quality: weight each KOL by real win rate + PnL so the
    # consensus score reflects conviction, not just head-count.
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
    async def _on_pump_buy(wallet, ca, sym, usd, amount):
        await w._process_buy(wallet, {
            "ca": ca, "amount": amount, "usd": usd,
            "symbol": sym, "ts": time.time(),
        })
    pump_stream = PumpApiStream(
        wallets=w.wallets, on_buy=_on_pump_buy, http=w._http)
    pump_task = asyncio.create_task(pump_stream.run())
    pump_task.add_done_callback(_log_task_result)
    # Hard cap on concurrent positions: never more than capital allows, and
    # never above the configured max_open_positions (avoids a consensus burst
    # over-leveraging the paper book).
    max_positions = min(max(1, int(round(s.start_balance_sol / s.size_sol))),
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
                      tp_ladder=s.tp_ladder, trail_enabled=s.trail_enabled,
                      on_trade_close=_record_trade,
                      open_max_impact_pct=s.open_max_impact_pct,
                      early_filter_window_s=s.early_filter_window_s,
                      early_filter_dd_pct=s.early_filter_dd_pct,
                      early_filter_gain_pct=s.early_filter_gain_pct)

    # shadow book opens automatically via on_smart_buy callback. During the
    # initial lookback window we only TRACK buys (so consensus alerts still
    # fire) and defer opening, so we never enter late — after a wallet's move
    # has already happened — which would systematically buy high.
    backfill_done = asyncio.Event()
    # Space out opens so a backlog (e.g. post-lookback batch) can't dump a
    # burst of positions at once. Configurable via OPEN_GAP_S.
    last_open = {"t": 0.0, "score": 0.0}
    open_gap_s = s.open_gap_s

    _skip_log = {}

    async def _on_smart_buy(ca, sym, usd, score, wallets=None):
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
        elif score < s.consensus_weight_threshold:
            # Weighted consensus gate: the summed quality score of distinct
            # buying wallets must clear the threshold. A single proven winner
            # (weight >= 1) is enough; two mid winners sum to ~1; noise wallets
            # (weight ~0) can never manufacture a signal on their own.
            reason = f"skip:score<{s.consensus_weight_threshold}"
        elif n < s.open_min_wallets:
            reason = f"skip:min_wallets<{s.open_min_wallets}"
        elif overlap >= s.per_wallet_max_positions:
            reason = f"skip:per_wallet_cap>={s.per_wallet_max_positions}"
        elif usd < s.watch_min_buy_usd:
            reason = "skip:below_min_buy"
        elif ca in book.open:
            reason = "skip:already_open"
        elif len(book.open) >= book.max_positions:
            reason = "skip:max_positions"
        elif book.balance_sol < book.size_sol:
            reason = "skip:insufficient_balance"
        elif any(c.get("ca") == ca for c in book.closed[-100:]):
            reason = "skip:recently_closed"
        elif time.time() - last_open["t"] < open_gap_s:
            # Open-spacing override: a much stronger signal (score >= 2.5) can
            # bypass the gap if the last open was weak (score < 2.0).  This
            # prevents a mediocre signal from blocking a genuine multi-wallet
            # consensus that arrives within the cooldown.
            last_score = last_open.get("score", 0)
            if score >= 2.5 and last_score < 2.0:
                log.info("open_spacing override %s (%s) score=%.2f > last %.2f",
                         ca[:10], sym, score, last_score)
            else:
                reason = "skip:open_spacing"
        else:
            # Fetch the market snapshot once: it drives both the momentum floor
            # and the multi-timeframe alignment score modifier below.
            try:
                snap = await ds.token_pairs("solana", ca)
            except Exception:
                snap = None
            # Rug/safety gate (DBotX, fail-open): reject tokens that still hold a
            # mint or freeze authority, or are dangerously top-10 concentrated.
            # A 403 / missing key degrades to "allow" so an outage never blocks.
            if s.dbotx_safety:
                pair_addr = (snap or {}).get("pair_address") or ca
                info = await dbx.pair_safety("solana", pair_addr)
                if info.get("available"):
                    if info["mint_authority"] or info["freeze_authority"]:
                        reason = "skip:unsafe(mint/freeze authority)"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    if info["top10"] > s.dbotx_top10_max:
                        reason = f"skip:unsafe(top10={info['top10']:.0%})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    if info["dev_position"] not in (None, "cleared"):
                        reason = f"skip:unsafe(dev={info['dev_position']})"
                        if _skip_log.get(ca, 0) < time.time() - 300:
                            _skip_log[ca] = time.time()
                            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)
                        return
                    logs.journal("open_safety_ok", ca=ca, symbol=sym,
                                 safety=info)
            pc = (snap or {}).get("price_change") or {}
            tfs = ("m5", "h1", "h6", "h24")
            avail = [k for k in tfs if pc.get(k) is not None]
            align = sum(1 for k in avail if (pc.get(k) or 0) > 0)
            # Multi-timeframe alignment: trend-shaped tokens (sling/SABL/Leafy:
            # green on all horizons) get a bonus; reversing/late ones (PINU:
            # +825% h24 but -56% h1) get a discount. Avoids entering tops.
            mkt_bonus = (align - 2) * s.mtf_align_bonus
            # Pair quality is a MULTIPLIER on the market score, not a veto: a weak
            # pair (AgmLJ+kEFiA) is down-weighted but may still trade when the
            # market confirms hard — so we don't overfit to a 6-trade sample.
            pmult, pnote = pair_multiplier(pair_perf, wallets)
            effective = (score + mkt_bonus) * pmult
            # Weak pair -> require strong confirmation: every AVAILABLE timeframe
            # positive (m5>0 & h1>0 at minimum) before it may open at all.
            if pmult < 1.0 and not all((pc.get(k) or 0) > 0 for k in avail):
                reason = f"skip:pair_needs_confirmation({pnote},align={align}/{len(avail)})"
            elif effective < s.consensus_weight_threshold:
                reason = (f"skip:score<{s.consensus_weight_threshold}"
                          f"(eff={effective:.2f},pmult={pmult:.2f},align={align})")
            elif not snap:
                # DexScreener blip with a genuine consensus: open flagged as
                # liq-unchecked rather than discarding the signal.
                last_open["t"] = time.time()
                last_open["score"] = score
                logs.journal("open_liq_unchecked", ca=ca, symbol=sym,
                             note="dexscreener_unavailable")
                logs.journal("open_signal_momentum", ca=ca, symbol=sym,
                             score=score, effective=round(effective, 3),
                             pmult=pmult, align=align, price_change=pc)
                await book.open_position(ca, sym, usd, usd, n, wallets=wallets)
                return
            elif (snap.get("liq") or 0) < s.open_min_liq_usd:
                reason = "skip:low_liq"
            elif (pc.get("h1") or 0) < s.open_min_h1_pct:
                reason = f"skip:no_momentum(h1={pc.get('h1')})"
            elif (pc.get("m5") or 0) < s.open_max_m5_dump_pct:
                reason = f"skip:dumping(m5={pc.get('m5')})"
            else:
                last_open["t"] = time.time()
                last_open["score"] = score
                logs.journal("open_signal_momentum", ca=ca, symbol=sym,
                             score=score, effective=round(effective, 3),
                             pmult=pmult, align=align, price_change=pc)
                await book.open_position(ca, sym, usd, usd, n, wallets=wallets)
                return
            return
        now = time.time()
        if _skip_log.get(ca, 0) < now - 300:
            _skip_log[ca] = now
            log.info("open deferred %s (%s): %s", ca[:10], sym, reason)

    w.on_smart_buy = _on_smart_buy

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_ in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_, stop.set)

    started = time.time()
    alerts = {"n": 0}

    async def status_loop() -> None:
        while not stop.is_set():
            await asyncio.sleep(max(60, s.status_every_min * 60))
            snap = book.snapshot(len(w.wallets), alerts["n"],
                                 w.consensus_fired, time.time() - started,
                                 {"tatum": bool(w.tatum_push),
                                  "dexscreener": True})
            log.info("status: %s", build_status(snap))

    # tatum push (optional) ----------------------------------------------
    tatum_url = cfg.get(env, "WATCH_WEBHOOK_URL", "")
    tatum_key = (cfg.get(env, "TATUM_API_KEY") or "").strip()
    w.tatum_push = False

    from aiohttp import web

    async def _hook(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
            data = payload.get("data") or {}
            cand = {data.get("from"), data.get("to"),
                    payload.get("address")}
            hit = next((x for x in cand if x in set(w.wallets)), None)
            if hit:
                w.tatum_push = True
                asyncio.create_task(w.process_now(hit)).add_done_callback(
                    _log_task_result)
        except Exception:
            log.exception("webhook parse failed")
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_post("/tatum", _hook)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(cfg.get_float(env, "WATCH_WEBHOOK_PORT", 8787))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("webhook receiver on :%d/tatum", port)

    # register subscriptions once (idempotent)
    if tatum_key and tatum_url:
        try:
            t = TatumNotifications(tatum_key, tatum_url)
            created, present = t.ensure_subscriptions(w.wallets)
            w.tatum_push = True
            log.info("tatum subs ready (%d created / %d present)",
                     created, present)
        except Exception:
            log.exception("tatum registration failed — polling fallback only")

    log.info("bot started: %s", build_status(book.snapshot(
        len(w.wallets), 0, 0, 0, {"tatum": w.tatum_push, "dexscreener": True})))
    if notifier is not None:
        try:
            await notifier.send_startup(
                summary=f"watching {len(w.wallets)} wallets · "
                        f"balance {s.start_balance_sol:.2f} SOL · "
                    f"E4 ladder cw={s.watch_consensus_window_s:.0f}s · "
                    f"weight_thr={s.consensus_weight_threshold}")
        except Exception:
            log.exception("send_startup failed")
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
    status_task = asyncio.create_task(status_loop())
    status_task.add_done_callback(_log_task_result)

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
    finally:
        try:
            if notifier is not None:
                await notifier.send_stopped(book.snapshot(
                    len(w.wallets), alerts["n"], w.consensus_fired,
                    time.time() - started,
                    {"tatum": w.tatum_push, "dexscreener": True}))
        except Exception:
            log.exception("send_stopped failed")
        status_task.cancel()
        pump_stream.stop()
        pump_task.cancel()
        await w.stop()
        await runner.cleanup()
        await ds.close()
        await dbx.close()
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


def cmd_tatum_setup(_args) -> int:
    from tatum_notify import TatumNotifications
    env = cfg.load_env()
    url = cfg.get(env, "WATCH_WEBHOOK_URL", "")
    key = (cfg.get(env, "TATUM_API_KEY") or "").strip()
    wallets_path = Path("smart_money_wallets.json")
    if not (url and key):
        print("set TATUM_API_KEY + WATCH_WEBHOOK_URL in .env first")
        return 1
    wallets = list(json.loads(wallets_path.read_text()).keys()) \
        if wallets_path.exists() else []
    created, present = TatumNotifications(key, url).ensure_subscriptions(wallets)
    print(f"tatum alerts ready: {created} created, {present} present")
    return 0


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
        j = (JupiterSwap(dry_run=False, private_key=base58.b58encode(
                kp.to_bytes()).decode()) if live else JupiterSwap(dry_run=True))
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

        sq = await j.quote_sell(args.ca, tokens_raw)
        if sq is None or not sq.success:
            print(f"SELL   : ✗ {sq.reason if sq else 'exception'} "
                  f"(holding {tokens:,.0f} tokens — sell via jup.ag)")
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
    ts = sub.add_parser("tatum-setup", help="register Tatum push alerts")
    ts.set_defaults(func=cmd_tatum_setup)
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
