"""Kolexplorer monitor feed — pre-computed KOL consensus tokens.

Polls the Kolexplorer Terminal AJAX endpoint to get tokens that multiple
top KOLs are actively buying, with aggregate PnL and entry market cap.

Unlike raw wallet tracking (PumpAPI/CabalSpy), Kolexplorer gives us
pre-filtered consensus: "12 KOLs bought token X in the last 4 hours."
We match KOL names to our tracked wallet performance data to compute
weighted consensus scores, then feed into the same _on_smart_buy pipeline.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Optional

import httpx

log = logging.getLogger(__name__)

# ── KOL slug → wallet address mapping ──────────────────────────────────────
# Sourced from KolScan/StalkChain leaderboards + wallet_performance.json.
# Unknown KOLs get default_weight (0.5) — they still contribute but less.
KOL_SLUG_TO_ADDR: dict[str, str] = {
    "kadenox": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
    "sebastian": "3BLjRcxWGtR7WRshJ3hL25U3RjWr5Ud98wMcczQqk4Ei",
    "dani": "AuPp4YTMTyqxYXQnHc5KUc6pUuCSsHQpBJhgnD45yqrf",
    "trunoest": "ardinRsN1mNYVeoJWTBsWeYeXvuR9UUDGMsCDKpb6AT",
    "clown": "EDXHdSFdadFbYFFjxPXBqMe1kCEDFqpPu552uvp48HR8",
    "west": "JDd3hy3gQn2V982mi1zqhNqUw1GfV2UL6g76STojCJPN",
    "gr3g": "J23qr98GjGJJqKq9CBEnyRhHbmkaVxtTJNNxKu597wsA",
    "cupsey": "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f",
    "radiance": "FAicXNV5FVqtfbpn4Zccs71XcfGeyxBSGbqLDyDJZjke",
    "cap": "CAPn1yH4oSywsxGU456jfgTrSSUidf9jgeAnHceNUJdw",
    "daumen": "8MaVa9kdt3NW4Q5HyNAm1X5LbR8PQRVDc1W8NMVK88D5",
    "cented": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
    "trenchman": "Hw5UKBU5k3YudnGwaykj5E8cYUidNMPuEewRRar5Xoc7",
    "dov": "8nqtxpFpuXwfXG4pBLsDkkuMMPK9FjSkBMCn542HiM3v",
    "alex": "H1TLERBQyQzSd5gbvbWwP97GjeL9txzWoN5BK4UG7xYm",
    "waiter1x": "4cXnf2z85UiZ5cyKsPMEULq1yufAtpkatmX4j4DBZqj2",
    "ljc": "6HJetMbdHBuk3mLUainxAPpBpWzDgYbHGTS2TqDAUSX2",
    "decu": "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9",
    "korean": "6KR7SorsUQtNH6CB6JpAnWCAKeTysa95iyXeWihdNeGT",
    "samsrep": "CUHBzSPSaNS3tArEtM3maSV6pNdJhHJFYZpurPPK9P7H",
    "chester": "PMJA8UQDyWTFw2Smhyp9jGA6aTaP7jKHR7BPudrgyYN",
    "qavec": "gangJEP5geDHjPVRhDS5dTF5e6GtRvtNogMEEVs91RV",
    "mr-frog": "4DdrfiDHpmx55i4SPssxVzS9ZaKLb8qr45NKY9Er9nNh",
    "yenni": "5B52w1ZW9tuwUduueP5J7HXz5AcGfruGoX6YoAudvyxG",
    "jijo": "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
    "flames": "6aXFYXbFob1ZKAEDCcqZnX2vooA3TgEqDoy5dAQbeWoV",
    "milito": "EeXvxkcGqMDZeTaVeawzxm9mbzZwqDUMmfG3bF7uzumH",
    "trey": "831yhv67QpKqLBJjbmw2xoDUeeFHGUx8RnuRj9imeoEs",
    "rilsio": "4fZFcK8ms3bFMpo1ACzEUz8bH741fQW4zhAMGd5yZMHu",
    "theo": "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt",
    "stigman": "",  # unknown
    "pain": "",     # unknown
    "letterbomb": "",  # unknown
    "esee": "",     # unknown
    "scharo": "",   # unknown
    "teddy": "",    # unknown
    "dali": "",     # unknown
}


class KolexplorerFeed:
    """Polls Kolexplorer monitor feed for pre-computed KOL consensus tokens."""

    def __init__(
        self,
        *,
        cookies: str,
        weights: dict[str, float],
        default_weight: float = 0.5,
        poll_s: float = 30.0,
        min_kols: int = 2,
        min_score: float = 0.0,
        max_entry_mc: float = 0.0,
        hours: int = 4,
        mode: int = 1,
        on_signal: Optional[Callable] = None,
    ):
        self._cookies = cookies
        self._weights = weights
        self._default_weight = default_weight
        self._poll_s = poll_s
        self._min_kols = min_kols
        self._min_score = min_score
        self._max_entry_mc = max_entry_mc
        self._hours = hours
        self._mode = mode
        self._on_signal = on_signal
        self._seen: set[str] = set()  # already processed token addresses
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_poll = 0.0
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._session = httpx.AsyncClient()
        self._task = asyncio.create_task(self._poll_loop(), name="kolexplorer")
        log.info("kolexplorer: started (poll=%ds, min_kols=%d, mode=%d, hours=%d)",
                 self._poll_s, self._min_kols, self._mode, self._hours)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.aclose()
        log.info("kolexplorer: stopped")

    def reset_seen(self) -> None:
        """Clear seen tokens (e.g. after bot restart)."""
        self._seen.clear()

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await self._fetch_and_process()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("kolexplorer poll error")
            await asyncio.sleep(self._poll_s)

    async def _fetch_and_process(self) -> None:
        url = (
            f"https://kolexplorer.com/terminal/"
            f"?ajax=monitor_feed&st_mode={self._mode}"
            f"&hours={self._hours}&min_buy=0&hot_min=0&limit=60"
        )
        headers = {
            "Cookie": self._cookies,
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
        if not self._session:
            return

        try:
            resp = await self._session.get(url, headers=headers, timeout=30)
            if resp.status_code in (401, 403):
                log.warning("kolexplorer: auth expired (HTTP %d) — refresh cookies", resp.status_code)
                return
            if resp.status_code != 200:
                log.debug("kolexplorer: HTTP %d", resp.status_code)
                return
            data = resp.json()
        except httpx.TimeoutException:
            log.debug("kolexplorer: timeout")
            return
        except Exception:
            log.debug("kolexplorer: fetch failed", exc_info=True)
            return

        if not data.get("ok"):
            log.debug("kolexplorer: ok=false")
            return

        rows = data.get("data", {}).get("rows", [])
        if not rows:
            return

        self._last_poll = time.time()
        new_count = 0

        for row in rows:
            ca = row.get("token_address", "")
            if not ca or ca in self._seen:
                continue

            sym = row.get("token_symbol", "?")
            kol_count = row.get("kol_count", 0)
            entry_mc = row.get("entry_mc", 0)
            total_pnl = row.get("total_pnl", 0)
            total_buy_vol = row.get("total_buy_vol", 0)
            score_raw = row.get("score", 0)

            # Minimum KOL threshold
            if kol_count < self._min_kols:
                continue

            # Max entry MC filter (0 = disabled)
            if self._max_entry_mc > 0 and entry_mc > self._max_entry_mc:
                continue

            # Parse KOL slugs and compute weighted score
            kol_slugs = [s.strip() for s in row.get("kol_slugs_csv", "").split("||") if s.strip()]
            kol_names = [n.strip() for n in row.get("kol_names_csv", "").split("||") if n.strip()]

            weighted_score = 0.0
            matched_wallets = []
            for slug in kol_slugs:
                addr = KOL_SLUG_TO_ADDR.get(slug)
                if addr and addr in self._weights:
                    w = self._weights[addr]
                    if w > 0:
                        weighted_score += w
                        matched_wallets.append(addr)
                elif addr == "":
                    # Unknown KOL — use default weight
                    weighted_score += self._default_weight
                else:
                    # Known slug but not in our wallet_performance.json
                    weighted_score += self._default_weight

            # Apply minimum score filter
            if self._min_score > 0 and weighted_score < self._min_score:
                continue

            self._seen.add(ca)
            new_count += 1

            log.info(
                "kolexplorer SIGNAL %s (%s) kols=%d weighted=%.2f mc=$%.0f pnl=$%.0f buy_vol=$%.0f",
                ca[:10], sym, kol_count, weighted_score, entry_mc, total_pnl, total_buy_vol,
            )

            if self._on_signal:
                try:
                    await self._on_signal(
                        ca, sym, entry_mc, weighted_score,
                        matched_wallets or kol_names[:kol_count],
                        source="kolexplorer",
                        kol_count=kol_count,
                        total_pnl=total_pnl,
                        total_buy_vol=total_buy_vol,
                    )
                except Exception:
                    log.exception("kolexplorer on_signal failed for %s", ca[:10])

        if new_count:
            log.debug("kolexplorer: %d new tokens from %d total", new_count, len(rows))
