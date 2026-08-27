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

import base58
import os
import config as cfg
import logs
from dexscreener import DexScreenerClient
from jupiter_swap import JupiterSwap
from solders.keypair import Keypair
from logs import setup_logging
from notifier import TelegramNotifier
from tatum_notify import TatumNotifications
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
    wins = sum(1 for c in closed if c.get("pnl_sol", 0) > 0)
    pnl = sum(c.get("pnl_sol", 0) for c in closed) + sum(
        o.get("pnl_sol", 0) for o in open_pos)
    start = st.get("start_balance_sol", 0.0)
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
        lines.append(f"   `{o.get('symbol','?')}` {o.get('mult',1):.2f}x "
                     f"({o.get('pnl_sol',0):+.4f})")
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

    When a ``jupiter`` (paper mode) is provided, entries/exits are priced by
    real executable quotes — impact & slippage included — instead of raw
    DexScreener mid-prices.
    """

    def __init__(self, ds: DexScreenerClient, size_sol: float,
                 retrace_pct: float, hard_stop_pct: float,
                 state_file: Path, start_balance_sol: float,
                 jupiter=None, notifier=None, max_positions: int = 12) -> None:
        self.jupiter = jupiter
        self.notifier = notifier
        self.max_positions = int(max_positions)
        self.ds = ds
        self.size_sol = float(size_sol)
        self.retrace = float(retrace_pct)
        self.hard_stop = float(hard_stop_pct)
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
        wins = sum(1 for c in self.closed if c.get("pnl_sol", 0.0) >= 0.0)
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
        except Exception:
            log.exception("shadow book load failed")

    def save(self) -> None:
        try:
            self.state_file.write_text(json.dumps({
                "open": self.open, "closed": self.closed,
                "start_balance_sol": self.start_balance_sol}, indent=1))
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
                            trigger_usd: float, n_wallets: int) -> None:
        # Entry price MUST be in the same unit as refresh_prices' DexScreener
        # price_usd (USD per token). We use DexScreener as the canonical source
        # so TP1 / trail / hard-stop thresholds compare like-for-like. (An
        # earlier bug stored a SOL-per-token Jupiter price as USD-per-token,
        # producing a ~100x mismatch that fired TP1 instantly.)
        snap = await self.ds.token_pairs("solana", ca)
        px = float(snap.get("price_usd") or 0) if snap else 0.0
        tokens_raw = 0
        entry_note = "dexscreener"
        if self.jupiter is not None:
            q = await self.jupiter.quote(ca, int(self.size_sol * 1e9),
                                         force=True)
            if q is None or not q.success:
                reason = q.reason if q else "quote_exception"
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason=f"untradable:{reason}")
                log.info("shadow skip %s (%s): %s", ca[:10], symbol, reason)
                return
            tokens_raw = q.output_amount
            entry_note = f"jup impact={q.price_impact_pct:.2f}%"
            # Simulate the SELL side too: a token may be buyable yet have no
            # TOKEN->SOL route (CATE/ELON/Google-AI all passed buy but failed
            # sell). Opening such a position would error on live close, so we
            # gate it here in paper exactly as live would.
            sq = await self.jupiter.quote_sell(ca, tokens_raw)
            if sq is None or not sq.success:
                reason = sq.reason if sq else "quote_exception"
                logs.journal("shadow_skip", ca=ca, symbol=symbol,
                             reason=f"unsellable:{reason}")
                log.info("shadow skip %s (%s): unsellable %s", ca[:10], symbol, reason)
                return
            # Fallback if DexScreener had no usable price: derive USD/token from
            # the Jupiter fill (size_sol SOL spent -> tokens received) and SOL USD.
            if px <= 0:
                dec = await self.jupiter.token_decimals(ca) or 6
                sol_usd = await self._sol_usd()
                if sol_usd and tokens_raw:
                    px = (self.size_sol * sol_usd) / (tokens_raw / (10 ** dec))
        if px <= 0:
            logs.journal("shadow_skip", ca=ca, symbol=symbol, reason="no_price")
            log.info("shadow skip %s (%s): no price", ca[:10], symbol)
            return
        # Simulated wallet: deploy size_sol on open. Skip if it would
        # over-leverage the tracked balance (can't open what we can't fund).
        # The mutation is locked so an in-flight refresh_prices() (running in
        # the main loop) can never interleave and double-count balance/positions.
        async with self._lock:
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
                "tokens_raw": tokens_raw, "entry_note": entry_note,
                "size_sol": self.size_sol, "ts": time.time(),
                "trigger_usd": trigger_usd, "n_wallets": n_wallets,
                "tp1_done": False, "tp1_banked": False, "tp1_mult": 2.0,
            }
            logs.journal("shadow_entry_px", ca=ca, px=px, note=entry_note)
            logs.journal("shadow_open", ca=ca, symbol=symbol, entry_usd=px,
                         trigger=trigger_usd, n=n_wallets)
            self.save()
        if self.notifier is not None:
            try:
                asyncio.get_running_loop().create_task(self.notifier.send_open(
                    ca=ca, name=symbol, price=px, size_sol=self.size_sol,
                    balance_before=bal_before, balance_after=self.balance_sol,
                    open_count=len(self.open), max_positions=self.max_positions,
                    n_wallets=n_wallets, trigger_usd=trigger_usd,
                    win_rate=self._win_rate()))
            except Exception:
                log.exception("send_open failed")

    async def refresh_prices(self) -> None:
        # Single lock around the whole scan: refresh and open_position run in
        # different tasks, and both read/write self.open / self.balance_sol.
        # Serializing them prevents lost updates (e.g. an open landing in the
        # middle of a close, or a balance miscount).
        async with self._lock:
            for ca in list(self.open):
                snap = await self.ds.token_pairs("solana", ca)
                # token_pairs() returns a normalized single-pair dict or None
                if not snap:
                    continue
                pxs = snap.get("price_usd")
                if not pxs:
                    continue
                px = float(pxs)
                pos = self.open[ca]
                pos["last_usd"] = px
                pos["peak_usd"] = max(pos["peak_usd"], px)
                entry = pos["entry_usd"]
                mult = px / entry if entry else 0
                peak_mult = pos["peak_usd"] / entry if entry else 0
                exit_reason = None
                if self.hard_stop > 0 and mult <= (1 - self.hard_stop):
                    exit_reason = "sl"
                elif peak_mult >= 1.10 and px <= pos["peak_usd"] * (1 - self.retrace):
                    exit_reason = "trail"
                elif not pos.get("tp1_done") and peak_mult >= pos.get("tp1_mult", 2.0) \
                        and not pos.get("tp1_banked"):
                    # partial TP: bank half at 2x (virtual), runner continues
                    pos["tp1_banked"] = True
                    pos["banked_pnl"] = pos["size_sol"] * 0.5 * (2.0 - 1.0)
                    logs.journal("shadow_tp1", ca=ca, symbol=pos["symbol"])
                if exit_reason and self.jupiter is not None and pos.get("tokens_raw"):
                    try:
                        sq = await self.jupiter.quote_sell(ca, pos["tokens_raw"])
                        if sq is not None and sq.success:
                            sol_out = sq.output_amount / 1e9
                            mult = sol_out / pos["size_sol"]
                            pos["exit_note"] = f"jup impact={sq.price_impact_pct:.2f}%"
                    except Exception:
                        log.exception("sell-quote failed for %s; using price-ratio pnl",
                                      ca[:10])
                if exit_reason:
                    pnl = pos.get("banked_pnl", 0.0) + \
                        pos["size_sol"] * (mult - 1.0)
                    rec = {"ca": ca, "symbol": pos["symbol"], "reason": exit_reason,
                           "mult": round(mult, 3), "pnl_sol": round(pnl, 5),
                           "hold_min": int((time.time() - pos["ts"]) / 60)}
                    self.closed.append(rec)
                    bal_before = self.balance_sol
                    self.balance_sol += self.size_sol + pnl
                    del self.open[ca]
                    logs.journal("shadow_close", **rec)
                    if self.notifier is not None:
                        try:
                            asyncio.get_running_loop().create_task(
                                self.notifier.send_close(
                                    ca=ca, name=pos["symbol"], reason=exit_reason,
                                    mult=mult, pnl_sol=pnl,
                                    hold_s=time.time() - pos["ts"],
                                    entry_px=pos["entry_usd"], exit_px=pos["last_usd"],
                                    size_sol=pos["size_sol"],
                                    balance_before=bal_before,
                                    balance_after=self.balance_sol,
                                    open_count=len(self.open),
                                    max_positions=self.max_positions,
                                    win_rate=self._win_rate()))
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
    setup_logging()
    env = cfg.load_env()
    notifier = TelegramNotifier()
    ds = DexScreenerClient(base_url=s.dexscreener_base_url,
                           rpm=s.dexscreener_rpm)

    w = SmartWalletWatcher(
        shyft_key=(cfg.get(env, "SHYFT_API_KEY") or "").strip(),
        shyft_rpc=cfg.get(env, "SHYFT_RPC_URL", "https://rpc.shyft.to"),
        ds=ds,
        notifier=notifier,
        poll_s=s.watch_poll_s,
        min_buy_usd=s.watch_min_buy_usd,
        consensus_wallets=s.watch_consensus_wallets,
        first_lookback_s=s.watch_first_lookback_s,
        state_file="watcher_state.json",
    )
    jupiter = JupiterSwap(dry_run=True)
    # Cap positions by available capital so we never deploy more than the
    # tracked balance (0.5 SOL start / 0.05 size => 10 max, not 12).
    max_positions = max(1, int(round(s.start_balance_sol / s.size_sol)))
    book = ShadowBook(ds, s.size_sol, s.trail_retrace_pct, s.hard_stop_pct,
                      Path(s.shadow_state_file), s.start_balance_sol,
                      jupiter=jupiter, notifier=notifier,
                      max_positions=max_positions)

    # shadow book opens automatically via on_smart_buy callback. During the
    # initial lookback window we only TRACK buys (so consensus alerts still
    # fire) and defer opening, so we never enter late — after a wallet's move
    # has already happened — which would systematically buy high.
    backfill_done = asyncio.Event()
    # Space out opens so a backlog (e.g. post-lookback batch) can't dump a
    # burst of positions at once. 20s gap => at most ~3 opens/min.
    last_open = {"t": 0.0}
    open_gap_s = 20.0

    _skip_log = {}

    async def _on_smart_buy(ca, sym, usd, n):
        if not backfill_done.is_set():
            reason = "deferred:lookback"
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
            reason = "skip:open_spacing"
        else:
            last_open["t"] = time.time()
            await book.open_position(ca, sym, usd, usd, n)
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

    manage_s = 5.0
    try:
        while not stop.is_set():
            await asyncio.sleep(manage_s)
            await book.refresh_prices()
    finally:
        status_task.cancel()
        await w.stop()
        await runner.cleanup()
        await ds.close()
        await jupiter.close()
    return 0


def cmd_watch(args) -> int:
    return asyncio.run(_run_watch(cfg.load_settings()))


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
    setup_logging(log_file="watcher.log")
    sys.exit(args_.func(args_) or 0)
