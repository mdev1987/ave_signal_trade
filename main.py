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

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import config as cfg
import logs
from dexscreener import DexScreenerClient
from logs import setup_logging
from notifier import TelegramNotifier
from tatum_notify import TatumNotifications
from watcher import SmartWalletWatcher

log = logging.getLogger("main")


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
    """Virtual positions mirroring 'buy what smart money buys'."""

    def __init__(self, ds: DexScreenerClient, size_sol: float,
                 retrace_pct: float, hard_stop_pct: float,
                 state_file: Path, start_balance_sol: float) -> None:
        self.ds = ds
        self.size_sol = float(size_sol)
        self.retrace = float(retrace_pct)
        self.hard_stop = float(hard_stop_pct)
        self.state_file = state_file
        self.start_balance_sol = float(start_balance_sol)
        self.open: dict[str, dict] = {}
        self.closed: list[dict] = []
        self._load()

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

    async def open_position(self, ca: str, symbol: str, usd_entry: float,
                            trigger_usd: float, n_wallets: int) -> None:
        snap = await self.ds.token_pairs("solana", ca)
        pair = max(snap["pairs"],
                   key=lambda p: (p.get("liquidity") or {}).get("usd") or 0) \
            if snap and snap.get("pairs") else None
        px = float((pair or {}).get("priceUsd") or usd_entry or 0)
        if px <= 0:
            px = usd_entry
        self.open[ca] = {
            "symbol": symbol, "entry_usd": px, "peak_usd": px, "last_usd": px,
            "size_sol": self.size_sol, "ts": time.time(),
            "trigger_usd": trigger_usd, "n_wallets": n_wallets,
        }
        logs.journal("shadow_open", ca=ca, symbol=symbol, entry_usd=px,
                     trigger=trigger_usd, n=n_wallets)
        self.save()

    async def refresh_prices(self) -> None:
        for ca in list(self.open):
            snap = await self.ds.token_pairs("solana", ca)
            pair = max(snap["pairs"],
                       key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)\
                if snap and snap.get("pairs") else None
            pxs = (pair or {}).get("priceUsd")
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
            elif pos["tp1_done"] is False and peak_mult >= pos.get("tp1_mult", 2.0) \
                    and not pos.get("tp1_banked"):
                # partial TP: bank half at 2x (virtual), runner continues
                pos["tp1_banked"] = True
                pos["banked_pnl"] = pos["size_sol"] * 0.5 * (2.0 - 1.0)
                logs.journal("shadow_tp1", ca=ca, symbol=pos["symbol"])
            if exit_reason:
                pnl = pos.get("banked_pnl", 0.0) + \
                    pos["size_sol"] * (mult - 1.0)
                rec = {"symbol": pos["symbol"], "reason": exit_reason,
                       "mult": round(mult, 3), "pnl_sol": round(pnl, 5),
                       "hold_min": int((time.time() - pos["ts"]) / 60)}
                self.closed.append(rec)
                del self.open[ca]
                logs.journal("shadow_close", ca=ca, **rec)
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
        helius_keys=[x.strip() for x in cfg.get(env, "HELIUS_API_KEYS",
                                                "").split(",") if x.strip()],
        moralis_key=(cfg.get(env, "MORALIS_API_KEY") or "").strip(),
        notifier=notifier,
        poll_s=s.watch_poll_s,
        min_buy_usd=s.watch_min_buy_usd,
        consensus_wallets=s.watch_consensus_wallets,
        state_file="watcher_state.json",
    )
    book = ShadowBook(ds, s.size_sol, s.trail_retrace_pct, s.hard_stop_pct,
                      Path(s.shadow_state_file), s.start_balance_sol)

    # hook: every processed smart buy feeds the shadow book
    orig_process = w._process_buy

    def process_and_track(wallet: str, b: dict) -> None:
        orig_process(wallet, b)
        ca = b["ca"]
        if ca not in book.open and all(c["symbol"] != b["symbol"]
                                       for c in book.closed[-50:]):
            hit = w.token_hits.get(ca) or {}
            asyncio.ensure_future(book.open_position(
                ca, b["symbol"], b["usd"], b["usd"], len(hit.get("wallets", []))))

    w._process_buy = process_and_track  # type: ignore[method-assign]

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
            try:
                await notifier.send_alert("📊 Status", build_status(snap))
            except Exception:
                log.exception("status send failed")

    # alert counting hook via notifier wrapper
    orig_alert = notifier.send_alert
    async def counted(title, detail="", **kw):
        if title.startswith(("🕵️", "🔥")):
            alerts["n"] += 1
        await orig_alert(title, detail, **kw)
    notifier.send_alert = counted  # type: ignore[method-assign]

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
                asyncio.create_task(w.process_now(hit))
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

    await notifier.send_startup(build_status(book.snapshot(
        len(w.wallets), 0, 0, 0, {"tatum": w.tatum_push, "dexscreener": True})))
    w.start()
    status_task = asyncio.create_task(status_loop())

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
    from watcher import SmartWalletWatcher as _W  # noqa: F401 (state compat)
    book = ShadowBook(DexScreenerClient(), 0, 0, 0,
                      Path(cfg.load_settings().shadow_state_file), 0)
    st = json.loads(Path("watcher_state.json").read_text()) \
        if Path("watcher_state.json").exists() else {}
    snap = book.snapshot(len(st.get("last_sig", {})),
                         st.get("alerts", 0), st.get("consensus", 0),
                         0, {})
    print(build_status(snap))
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    watch = sub.add_parser("watch", help="run the 24/7 watcher")
    watch.set_defaults(func=cmd_watch)
    ts = sub.add_parser("tatum-setup", help="register Tatum push alerts")
    ts.set_defaults(func=cmd_tatum_setup)
    st = sub.add_parser("status", help="print status card")
    st.set_defaults(func=cmd_status)
    return ap


if __name__ == "__main__":
    ap_ = build_parser()
    args_ = ap_.parse_args()
    setup_logging(log_file="watcher.log")
    sys.exit(args_.func(args_) or 0)
