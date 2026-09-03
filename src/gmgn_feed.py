"""GMGN feed — smart money + KOL trades via gmgn-cli polling.

Replaces the broken gmgnapi WebSocket (403 on connect). Uses
``gmgn-cli track smartmoney`` and ``gmgn-cli track kol`` via subprocess
every ``poll_s`` seconds. Deduplicates by (wallet, ca, ts) and forwards
tracked wallet buys to the consensus pipeline.

Also fires on ALL smart money / KOL buys (not just tracked wallets) so the
consensus engine sees fresh activity from any tagged wallet.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

log = logging.getLogger(__name__)

# Cooldown: don't re-report the same (wallet, ca, ts) within this window
_DEDUP_MAX = 10_000


class GmgnFeed:
    """Polling-based GMGN smart money + KOL feed.

    Lifecycle::

        feed = GmgnFeed(...)
        task = asyncio.create_task(feed.run())
        # ...
        stats = feed.health()
        feed.stop()
    """

    def __init__(
        self,
        on_buy: Callable[[str, str, str, float, float], Awaitable[None]],
        wallets: set[str],
        poll_s: float = 15.0,
        min_usd: float = 50.0,
    ) -> None:
        self._on_buy = on_buy
        self._wallets = wallets
        self._poll_s = poll_s
        self._min_usd = min_usd
        self._stop = asyncio.Event()
        self._seen: set[tuple[str, str, float]] = set()

        # Health
        self._started_at: float = 0.0
        self._smartmoney_count: int = 0
        self._kol_count: int = 0
        self._tracked_count: int = 0
        self._error_count: int = 0
        self._last_event_at: float = 0.0

    async def run(self) -> None:
        """Main polling loop. Runs until ``stop()``."""
        self._started_at = time.time()
        log.info("gmgn feed: started (interval=%.0fs, wallets=%d)", self._poll_s, len(self._wallets))

        while not self._stop.is_set():
            try:
                await self._poll()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self._error_count += 1
                log.exception("gmgn feed: poll failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_s)
            except asyncio.TimeoutError:
                pass

    async def _poll(self) -> None:
        # Fetch smart money + KOL trades in parallel
        sm_task = asyncio.create_task(_run_cli("smartmoney", 100))
        kol_task = asyncio.create_task(_run_cli("kol", 100))

        sm_raw, kol_raw = await asyncio.gather(sm_task, kol_task)

        sm_list = _parse(sm_raw)
        kol_list = _parse(kol_raw)

        self._smartmoney_count += len(sm_list)
        self._kol_count += len(kol_list)

        # Process all trades
        for tr in sm_list + kol_list:
            await self._process_trade(tr)

    async def _process_trade(self, tr: dict) -> None:
        wallet = (tr.get("maker") or "").strip()
        ca = (tr.get("base_address") or "").strip()
        ts = float(tr.get("timestamp") or 0)
        side = tr.get("side") or ""
        usd = float(tr.get("amount_usd") or 0)

        if not wallet or not ca or side != "buy":
            return
        if usd < self._min_usd:
            return

        # Dedup
        key = (wallet, ca, ts)
        if key in self._seen:
            return
        self._seen.add(key)
        if len(self._seen) > _DEDUP_MAX:
            old = sorted(self._seen, key=lambda x: x[2])[:_DEDUP_MAX // 2]
            for k in old:
                self._seen.discard(k)

        self._last_event_at = time.time()
        sym = ""
        bt = tr.get("base_token") or {}
        if isinstance(bt, dict):
            sym = bt.get("symbol") or ""

        is_tracked = wallet in self._wallets
        if is_tracked:
            self._tracked_count += 1
            log.info("gmgn feed: TRACKED %s bought %s ($%.2f) [%s]", wallet[:8], ca[:8], usd, sym)

        try:
            await self._on_buy(wallet, ca, sym, usd, float(tr.get("token_amount") or 0))
        except Exception:
            log.exception("gmgn feed: on_buy failed for %s %s", wallet[:8], ca[:8])

    def health(self) -> dict:
        now = time.time()
        return {
            "connected": self._started_at > 0 and not self._stop.is_set(),
            "uptime_s": (now - self._started_at) if self._started_at else 0,
            "smartmoney_trades": self._smartmoney_count,
            "kol_trades": self._kol_count,
            "tracked_buys": self._tracked_count,
            "errors": self._error_count,
            "last_event_at": self._last_event_at,
        }

    def stop(self) -> None:
        self._stop.set()


async def _run_cli(subcmd: str, limit: int = 100) -> str:
    """Run gmgn-cli track <subcmd> and return raw stdout."""
    proc = await asyncio.create_subprocess_exec(
        "gmgn-cli", "track", subcmd,
        "--chain", "sol",
        "--limit", str(limit),
        "--raw",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
    if proc.returncode != 0:
        raise RuntimeError(f"gmgn-cli track {subcmd} failed: {stderr.decode()[:200]}")
    return stdout.decode()


def _parse(raw: str) -> list[dict]:
    """Parse gmgn-cli JSON output into a list of trade dicts."""
    try:
        obj = json.loads(raw)
        return obj.get("list") or []
    except Exception:
        return []
