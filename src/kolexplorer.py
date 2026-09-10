"""Kolexplorer — pre-computed KOL consensus tokens (heatmap + monitor feed).

Two data sources:
1. Token Heatmap (/token.php?hm_ajax=1&tf=2h) — PRIMARY
   Structured per-KOL data: slug, name, pnl, buy_vol, first_seen.
   Time-windowed (2h default), shows tokens gaining KOL traction.

2. Monitor Feed (/terminal/?ajax=monitor_feed) — FALLBACK
   Aggregate consensus: kol_count, total_pnl, entry_mc.
   Longer window (4h default), catches tokens the heatmap misses.

We match KOL slugs to tracked wallet performance data to compute
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
    "pain": "J6TDXvarvpBdPXTaTU8eJbtso1PUCYKGkVtMKUUY8iEa",
    "jason": "ACTbvbNm5qTLuofNRPxFPMtHAAtdH1CtzhCZatYHy831",
    "poorgoat": "HDixbrzwwLXczhDBk1JVrurPQsuLE8FUKnW2pucSXN3o",
    "loopierr": "9yYya3F5EJoLnBNKW6z4bZvyQytMXzDcpU5D6yYr4jqL",
    "jidn": "3h65MmPZksoKKyEpEjnWU2Yk2iYT5oZDNitGy5cTaxoE",
    "stigman": "",  # unmapped
    "letterbomb": "",  # unmapped
    "esee": "",    # unmapped
    "scharo": "",  # unmapped
    "teddy": "",   # unmapped
    "dali": "",    # unmapped
    "heyitsyolo": "",  # unmapped
    "xanse": "",   # unmapped
    "coler": "",   # unmapped
    "parsiiix": "",  # unmapped
    "kev": "BTf4A2exGK9BCVDNzy65b9dUzXgMqB4weVkvTMFQsadd",
    "tdmilky": "",  # unmapped
    "frost": "",   # unmapped
    "cook": "",    # unmapped
    "kay-the-doc": "",  # unmapped
    "ozark": "",   # unmapped
    "cottage": "",  # unmapped
    "bandit": "5B79fMkcFeRTiwm7ehsZsFiKsC7m7n1Bgv9yLxPp9q2X",
    "solana-degen": "",  # unmapped
    "vein": "",    # unmapped
    "tech": "",    # unmapped
    "sting": "",   # unmapped
    "japbitch": "DemfvB4iwd3NmVquvWqWbB92yVZWFFqybqBeJGdyEeM6",
    "leck": "98T65wcMEjoNLDTJszBHGZEX75QRe8QaANXokv4yw3Mp",
    "kaaox": "3j5c4aD1aznxQXJ3DWw1b7UD8kKuaqXVbpaVeWPR83TG",
    "smokez": "5t9xBNuDdGTGpjaPTx6hKd7sdRJbvtKS8Mhq6qVbo8Qz",
    "xander": "B3wagQZiZU2hKa5pUCj6rrdhWsX3Q6WfTTnki9PjwzMh",
    "ethan-prosper": "sAdNbe1cKNMDqDsa4npB3TfL62T14uAo2MsUQfLvzLT",
    "evening": "E7gozEiAPNhpJsdS52amhhN2XCAqLZa7WPrhyR6C8o4S",
    "tom": "CEUA7zVoDRqRYoeHTP58UHU6TR8yvtVbeLrX1dppqoXJ",
    "risk": "BHREKFkPQgAtDs8Vb1UfLkUpjG6ScidTjHaCWFuG2AtX",
    "wugi": "862TYSvRYoiHAK3F3WwTRYAfuGiQaGdxedN9AGvRGWo2",
}


class KolexplorerFeed:
    """Polls Kolexplorer for pre-computed KOL consensus tokens.

    Uses two endpoints:
    - Token Heatmap (primary): structured per-KOL data, 2h window
    - Monitor Feed (fallback): aggregate consensus, 4h window
    """

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
        heatmap_tf: str = "2h",
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
        self._heatmap_tf = heatmap_tf
        self._on_signal = on_signal
        self._seen: dict[str, float] = {}  # ca -> first_seen_ts (for TTL pruning)
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_poll = 0.0
        self._session: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._session = httpx.AsyncClient()
        self._task = asyncio.create_task(self._poll_loop(), name="kolexplorer")
        log.info("kolexplorer: started (poll=%ds, min_kols=%d, tf=%s, mc_max=$%.0f)",
                 self._poll_s, self._min_kols, self._heatmap_tf,
                 self._max_entry_mc)

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

    def _headers(self) -> dict:
        return {
            "Cookie": self._cookies,
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                # Primary: token heatmap (structured per-KOL data)
                await self._fetch_heatmap()
                # Fallback: monitor feed (catches tokens heatmap misses)
                await self._fetch_monitor_feed()
                # Prune _seen entries older than 24h
                if len(self._seen) > 5000:
                    cutoff = time.time() - 86400
                    stale = [ca for ca, ts in self._seen.items() if ts < cutoff]
                    for ca in stale:
                        del self._seen[ca]
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("kolexplorer poll error")
            await asyncio.sleep(self._poll_s)

    # ── Token Heatmap (primary) ───────────────────────────────────────────
    async def _fetch_heatmap(self) -> None:
        if not self._session:
            return
        url = (
            f"https://kolexplorer.com/token.php"
            f"?hm_ajax=1&tf={self._heatmap_tf}"
        )
        try:
            resp = await self._session.get(url, headers=self._headers(), timeout=30)
            if resp.status_code in (401, 403):
                log.warning("kolexplorer heatmap: auth expired (HTTP %d)", resp.status_code)
                return
            if resp.status_code != 200:
                log.debug("kolexplorer heatmap: HTTP %d", resp.status_code)
                return
            data = resp.json()
        except httpx.TimeoutException:
            log.debug("kolexplorer heatmap: timeout")
            return
        except Exception:
            log.debug("kolexplorer heatmap: fetch failed", exc_info=True)
            return

        if not data.get("ok"):
            return

        tokens = data.get("tokens", [])
        if not tokens:
            return

        self._last_poll = time.time()
        new_count = 0

        for tok in tokens:
            ca = tok.get("ca", "")
            if not ca or ca in self._seen:
                continue

            sym = tok.get("sym", "?")
            kol_count = tok.get("kols") or 0
            entry_mc = tok.get("buy_mc") or 0
            total_pnl = tok.get("pnl") or 0
            vol = tok.get("vol") or 0

            if kol_count < self._min_kols:
                continue

            if self._max_entry_mc > 0 and entry_mc > self._max_entry_mc:
                continue

            # Parse structured kol_list
            kol_list = tok.get("kol_list", [])
            if isinstance(kol_list, str):
                try:
                    kol_list = __import__("json").loads(kol_list)
                except Exception:
                    kol_list = []

            weighted_score = 0.0
            matched_wallets = []
            for kol in kol_list:
                slug = kol.get("slug", "")
                kol_pnl = kol.get("kol_pnl", 0)
                buy_vol = kol.get("buy_vol", 0)

                addr = KOL_SLUG_TO_ADDR.get(slug)
                if addr and addr in self._weights:
                    w = self._weights[addr]
                    if w > 0:
                        weighted_score += w
                        matched_wallets.append(addr)
                else:
                    # Unknown or unmapped KOL — use default weight
                    weighted_score += self._default_weight

            if self._min_score > 0 and weighted_score < self._min_score:
                continue

            self._seen[ca] = time.time()
            new_count += 1

            log.info(
                "kolexplorer HEATMAP %s (%s) kols=%d score=%.2f mc=$%.0f pnl=$%.0f vol=$%.0f",
                ca[:10], sym, kol_count, weighted_score, entry_mc, total_pnl, vol,
            )

            if self._on_signal:
                try:
                    await self._on_signal(
                        ca, sym, entry_mc, weighted_score,
                        matched_wallets or [k.get("slug", "?") for k in kol_list[:kol_count]],
                        source="kolexplorer",
                        kol_count=kol_count,
                        total_pnl=total_pnl,
                        total_buy_vol=vol,
                    )
                except Exception:
                    log.exception("kolexplorer heatmap on_signal failed for %s", ca[:10])

        if new_count:
            log.debug("kolexplorer heatmap: %d new from %d total", new_count, len(tokens))

    # ── Monitor Feed (fallback) ───────────────────────────────────────────
    async def _fetch_monitor_feed(self) -> None:
        if not self._session:
            return
        url = (
            f"https://kolexplorer.com/terminal/"
            f"?ajax=monitor_feed&st_mode={self._mode}"
            f"&hours={self._hours}&min_buy=0&hot_min=0&limit=60"
        )
        try:
            resp = await self._session.get(url, headers=self._headers(), timeout=30)
            if resp.status_code in (401, 403):
                log.debug("kolexplorer monitor: auth expired (HTTP %d)", resp.status_code)
                return
            if resp.status_code != 200:
                log.debug("kolexplorer monitor: HTTP %d", resp.status_code)
                return
            data = resp.json()
        except httpx.TimeoutException:
            log.debug("kolexplorer monitor: timeout")
            return
        except Exception:
            log.debug("kolexplorer monitor: fetch failed", exc_info=True)
            return

        if not data.get("ok"):
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

            if kol_count < self._min_kols:
                continue

            if self._max_entry_mc > 0 and entry_mc > self._max_entry_mc:
                continue

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
                else:
                    weighted_score += self._default_weight

            if self._min_score > 0 and weighted_score < self._min_score:
                continue

            self._seen[ca] = time.time()
            new_count += 1

            log.info(
                "kolexplorer MONITOR %s (%s) kols=%d score=%.2f mc=$%.0f pnl=$%.0f vol=$%.0f",
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
                    log.exception("kolexplorer monitor on_signal failed for %s", ca[:10])

        if new_count:
            log.debug("kolexplorer monitor: %d new from %d total", new_count, len(rows))
