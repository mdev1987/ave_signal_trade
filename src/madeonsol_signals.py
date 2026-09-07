"""MadeOnSol unified signal feed — KOL trades, first touches, sniper alerts.

Provides a unified async signal feed that integrates with the existing
SmartWalletWatcher consensus engine. Replaces Shyft polling as the
PRIMARY data source with MadeOnSol's curated KOL intelligence.

Architecture:
  - KOL feed polling (primary signal source, replaces Shyft)
  - First-touch detection (earliest possible alpha)
  - Sniper alerts (pre-confirmation deploy detection)
  - Token surges (momentum fires)
  - Almost-bonded (pre-graduation pump.fun tokens)
  - Alpha wallet discovery (periodic)

All signals feed into the existing _process_buy callback with
wallet/consensus metadata intact.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Awaitable

from madeonsol_x402 import MadeOnSolClient

log = logging.getLogger(__name__)

# Poll intervals (seconds) — free tier has 200/day quota
KOL_FEED_INTERVAL = 30.0      # poll KOL feed every 30s (free: 5min delay)
FIRST_TOUCH_INTERVAL = 60.0   # check first touches every 60s
SNIPER_INTERVAL = 30.0        # check sniper alerts every 30s
SURGES_INTERVAL = 120.0       # check token surges every 2min
ALMOST_BONDED_INTERVAL = 300.0 # check almost-bonded every 5min
ALPHA_DISCOVER_INTERVAL = 3600.0  # discover new alpha wallets every hour


class MadeOnSolSignals:
    """Unified signal feed from MadeOnSol API.

    Polls multiple endpoints and feeds signals into the consensus engine.
    """

    def __init__(
        self,
        client: MadeOnSolClient,
        process_buy: Callable[[str, dict], Awaitable[None]],
        smart_buy: Callable[[str, str, float, float, list[str]], Awaitable[None]] | None = None,
        wallet_set: set[str] | None = None,
        seen_cas: set[str] | None = None,
    ) -> None:
        self.client = client
        self.process_buy = process_buy  # feeds into consensus engine
        self.smart_buy = smart_buy     # bypass consensus for high-priority signals
        self.wallet_set = wallet_set or set()
        self.seen_cas = seen_cas or set()
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._last_feed_ts: dict[str, float] = {}  # wallet:ca -> last seen ts
        self._stats = {
            "kol_buys": 0,
            "first_touches": 0,
            "sniper_alerts": 0,
            "surges": 0,
            "almost_bonded": 0,
            "alpha_discovered": 0,
        }
        self._ratelimit_hits: dict[str, int] = {}  # task -> consecutive 429s

    def _on_429(self, task: str, backoff: float) -> float:
        """Circuit breaker for 429s: warn twice, then go quiet for 1h.

        The free tier (200 calls/day) exhausts fast with 6 pollers; without
        this each task warns every cycle and burns quota on retries.
        Any success resets via _on_success().
        """
        hits = self._ratelimit_hits.get(task, 0) + 1
        self._ratelimit_hits[task] = hits
        if hits >= 3:
            if hits == 3:
                log.warning("madeonsol %s rate-limited 3x — circuit open, quiet 1h",
                            task)
            else:
                log.debug("madeonsol %s still rate-limited (%dx), quiet 1h",
                          task, hits)
            return 3600.0
        new = min(backoff * 2, 600)
        log.warning("madeonsol %s 429, backing off %.0fs", task, new)
        return new

    def _on_success(self, task: str) -> None:
        if self._ratelimit_hits.pop(task, None) is not None:
            log.info("madeonsol %s recovered from rate limit", task)

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    async def run(self) -> None:
        """Start all polling tasks."""
        log.info("madeonsol signals: starting (kol_feed=%ds, first_touch=%ds, sniper=%ds)",
                 KOL_FEED_INTERVAL, FIRST_TOUCH_INTERVAL, SNIPER_INTERVAL)
        self._tasks = [
            asyncio.create_task(self._poll_kol_feed()),
            asyncio.create_task(self._poll_first_touches()),
            asyncio.create_task(self._poll_sniper_alerts()),
            asyncio.create_task(self._poll_surges()),
            asyncio.create_task(self._poll_almost_bonded()),
            asyncio.create_task(self._discover_alpha_wallets()),
        ]
        # Wait for stop signal
        await self._stop.wait()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Stop all polling tasks."""
        self._stop.set()

    async def _poll_kol_feed(self) -> None:
        """Poll KOL feed for real-time buy signals.

        Feeds into process_buy() so KOL buys contribute to consensus.
        """
        backoff = KOL_FEED_INTERVAL
        while not self._stop.is_set():
            try:
                feed = self.client.kol_feed(limit=50, action="buy")
                trades = feed.get("trades", []) if feed else []
                for t in trades:
                    wallet = t.get("wallet", "") or t.get("maker", "")
                    ca = t.get("token_mint", "") or t.get("mint", "")
                    sym = t.get("token_symbol", "") or t.get("symbol", "?")
                    sol_amount = t.get("sol_amount", 0)
                    ts = t.get("timestamp", 0) or time.time()

                    if not wallet or not ca:
                        continue

                    # Deduplicate: skip if we've already seen this wallet+ca
                    dedup_key = f"{wallet}:{ca}"
                    if ts <= self._last_feed_ts.get(dedup_key, 0):
                        continue
                    self._last_feed_ts[dedup_key] = ts

                    self._stats["kol_buys"] += 1

                    # Feed into consensus engine via _process_buy
                    buy = {
                        "ca": ca,
                        "symbol": sym,
                        "usd": sol_amount * 150,  # approximate SOL price
                        "ts": ts,
                        "source": "madeonsol_kol_feed",
                        "kol_name": t.get("kol_name", ""),
                    }
                    log.info("madeonsol kol buy: %s bought %s (%.2f SOL) — %s",
                             wallet[:8], sym, sol_amount, ca[:10])
                    await self.process_buy(wallet, buy)

                # Cleanup old dedup keys (>1h old)
                now = time.time()
                self._last_feed_ts = {
                    k: v for k, v in self._last_feed_ts.items()
                    if now - v < 3600
                }
                backoff = KOL_FEED_INTERVAL  # reset on success
                self._on_success("kol feed")

            except Exception as exc:
                if "429" in str(exc):
                    backoff = self._on_429("kol feed", backoff)
                else:
                    log.warning("madeonsol kol feed poll failed: %s", exc)
                    backoff = KOL_FEED_INTERVAL

            await asyncio.sleep(backoff)

    async def _poll_first_touches(self) -> None:
        """Poll first-touch events (earliest KOL buy on a token).

        First-touch is a high-conviction signal: feed directly into
        smart_buy to bypass consensus (the first KOL IS the consensus).
        """
        backoff = FIRST_TOUCH_INTERVAL
        while not self._stop.is_set():
            try:
                events = self.client.rest.first_touches(limit=20)
                for ev in (events.get("events", []) if events else []):
                    ca = ev.get("token_mint", "") or ev.get("mint", "")
                    fk = ev.get("first_kol", {})
                    wallet = fk.get("address", "") or fk.get("wallet", "")
                    sym = ev.get("token_symbol", "?")
                    scout_tier = fk.get("scout_tier", "?")

                    if not ca or not wallet:
                        continue
                    if ca in self.seen_cas:
                        continue

                    self._stats["first_touches"] += 1

                    # First-touch = strongest signal: bypass consensus
                    log.info("madeonsol first touch: %s scouted %s (tier=%s)",
                             wallet[:8], sym, scout_tier)
                    if self.smart_buy:
                        await self.smart_buy(ca, sym, 0.0, 3.0, [wallet])

                backoff = FIRST_TOUCH_INTERVAL  # reset on success
                self._on_success("first touch")

            except Exception as exc:
                if "429" in str(exc):
                    backoff = self._on_429("first touch", backoff)
                else:
                    log.warning("madeonsol first touch poll failed: %s", exc)
                    backoff = FIRST_TOUCH_INTERVAL

            await asyncio.sleep(backoff)

    async def _poll_sniper_alerts(self) -> None:
        """Poll sniper alerts (pre-confirmation deploy detection).
        Requires PRO tier. Backs off on 403 or 429.
        """
        pro_only = False
        backoff = SNIPER_INTERVAL
        while not self._stop.is_set():
            if pro_only:
                await asyncio.sleep(3600)  # sleep 1h if PRO required
                continue
            try:
                feed = self.client.rest.sniper_recent(limit=10, deployer_tier="elite")
                deploys = feed.get("deploys", []) if feed else []
                for d in deploys:
                    ca = d.get("mint", "") or d.get("token_mint", "")
                    deployer = d.get("deployer", "")
                    bond_rate = d.get("bond_rate", 0)
                    tier = d.get("tier", "?")
                    sym = d.get("symbol", "?")

                    if not ca or not deployer:
                        continue
                    if ca in self.seen_cas:
                        continue

                    self._stats["sniper_alerts"] += 1

                    # Elite deployer launch = strong signal
                    log.info("madeonsol sniper: %s deployed %s (tier=%s, bond=%.0f%%)",
                             deployer[:8], sym, tier, bond_rate * 100)
                    if self.smart_buy:
                        await self.smart_buy(ca, sym, 0.0, 2.5, [deployer])

                self._on_success("sniper")

            except Exception as exc:
                if "403" in str(exc):
                    log.info("madeonsol sniper: PRO tier required, backing off")
                    pro_only = True
                elif "429" in str(exc):
                    backoff = self._on_429("sniper", backoff)
                else:
                    log.warning("madeonsol sniper poll failed: %s", exc)

            await asyncio.sleep(backoff)

    async def _poll_surges(self) -> None:
        """Poll token surges (momentum fires). Requires PRO tier."""
        pro_only = False
        backoff = SURGES_INTERVAL
        while not self._stop.is_set():
            if pro_only:
                await asyncio.sleep(3600)
                continue
            try:
                data = self.client.rest.tokens_surges(kind="surge", limit=10)
                for s in (data.get("surges", []) if data else []):
                    ca = s.get("mint", "")
                    tier = s.get("tier", "?")
                    mc = s.get("market_cap_usd", 0)

                    if not ca or ca in self.seen_cas:
                        continue

                    self._stats["surges"] += 1
                    log.info("madeonsol surge: %s tier=%s mc=$%.0f",
                             ca[:10], tier, mc)

                self._on_success("surges")

            except Exception as exc:
                if "403" in str(exc):
                    log.info("madeonsol surges: PRO tier required, backing off")
                    pro_only = True
                elif "429" in str(exc):
                    backoff = self._on_429("surges", backoff)
                else:
                    log.warning("madeonsol surges poll failed: %s", exc)

            await asyncio.sleep(backoff)

    async def _poll_almost_bonded(self) -> None:
        """Poll almost-bonded pump.fun tokens (pre-graduation). Requires PRO."""
        pro_only = False
        backoff = ALMOST_BONDED_INTERVAL
        while not self._stop.is_set():
            if pro_only:
                await asyncio.sleep(3600)
                continue
            try:
                data = self.client.rest.almost_bonded(
                    min_progress=90, sort="velocity_desc", limit=10
                )
                for t in (data.get("tokens", []) if data else []):
                    ca = t.get("mint", "")
                    progress = t.get("progress_pct", 0)
                    velocity = t.get("velocity_pct_per_min", 0)

                    if not ca or ca in self.seen_cas:
                        continue

                    self._stats["almost_bonded"] += 1
                    log.info("madeonsol almost-bonded: %s progress=%.1f%% velocity=%.2f%%/min",
                             ca[:10], progress, velocity)

                self._on_success("almost-bonded")

            except Exception as exc:
                if "403" in str(exc):
                    log.info("madeonsol almost-bonded: PRO tier required, backing off")
                    pro_only = True
                elif "429" in str(exc):
                    backoff = self._on_429("almost-bonded", backoff)
                else:
                    log.warning("madeonsol almost-bonded poll failed: %s", exc)

            await asyncio.sleep(backoff)

    async def _discover_alpha_wallets(self) -> None:
        """Periodically discover new alpha wallets from leaderboard."""
        pro_only = False
        while not self._stop.is_set():
            if pro_only:
                await asyncio.sleep(3600)
                continue
            try:
                data = self.client.rest.alpha_leaderboard(period="7d", sort="win_rate")
                wallets = data.get("wallets", []) if data else []
                new_count = 0
                for w in wallets:
                    addr = w.get("address", "")
                    if addr and addr not in self.wallet_set:
                        self._stats["alpha_discovered"] += 1
                        new_count += 1
                        log.info("madeonsol alpha discovered: %s (win=%.0f%%, pnl=$%.0f)",
                                 addr[:8], w.get("win_rate", 0) * 100, w.get("pnl", 0))
                if new_count:
                    log.info("madeonsol: discovered %d new alpha wallets", new_count)

            except Exception as exc:
                if "403" in str(exc):
                    log.info("madeonsol alpha leaderboard: PRO tier required, backing off")
                    pro_only = True
                elif "429" in str(exc):
                    log.warning("madeonsol alpha leaderboard 429, backing off 1h")
                    pro_only = True
                else:
                    log.warning("madeonsol alpha discovery failed: %s", exc)

            await asyncio.sleep(ALPHA_DISCOVER_INTERVAL)
