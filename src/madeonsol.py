"""MadeOnSol KOL validator — read-only, journal-only, NEVER gates entries.

Polls the public KOL trade feed (``GET /kol/tokens`` family needs a real
key; the no-signup demo key only covers ``/kol/feed``) and keeps recent
KOL buys in memory. The signal path calls :meth:`kol_footprint` — a pure
in-memory lookup — and journals any overlap. It must never block, skip,
or resize a position.

Quota: demo key = 20 calls/hour/IP → default poll 200s. A real
``msk_`` free key (200 calls/day, 40+ endpoints) unlocks per-mint
``/kol/tokens/{mint}`` + ``/tokens/{mint}/kol-consensus``; swap
``token_flow()`` in then without touching callers.

Docs: https://madeonsol.com/api-docs (full response shape on every tier).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://madeonsol.com/api/v1"
# Public demo key from MadeOnSol docs — no signup, 20 calls/hour/IP.
_DEMO_KEY = "msk_demo_try_the_solana_api_2026"


class MadeOnSolClient:
    """Background KOL-feed validator. Fail-open on every error."""

    def __init__(
        self,
        api_key: str = "",
        base_url: str = _BASE_URL,
        poll_s: float = 200.0,
        window_s: float = 1800.0,
        timeout_s: float = 10.0,
    ) -> None:
        self.api_key = (api_key or "").strip() or _DEMO_KEY
        self.base_url = base_url.rstrip("/")
        self.poll_s = max(180.0, poll_s)  # never burn the 20/hr demo budget
        self.window_s = window_s
        self._client = httpx.AsyncClient(timeout=timeout_s)
        # mint -> list of {ts, wallet, kol, winrate_7d, sol}
        self._buys: dict[str, list[dict[str, Any]]] = {}
        self._task: asyncio.Task | None = None
        self._stop = False
        self._last_ok_ts = 0.0
        self._polls = 0
        self._backoff_s = 0.0

    @property
    def healthy(self) -> bool:
        """True if at least one poll succeeded in the last 3 intervals."""
        return self._last_ok_ts > 0 and (time.time() - self._last_ok_ts) < 3 * self.poll_s

    def start(self) -> None:
        if self._task is None:
            self._stop = False
            self._task = asyncio.create_task(self._loop())
            logger.info("madeonsol: validator started (poll=%.0fs, window=%.0fs)",
                        self.poll_s, self.window_s)

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        try:
            await self._client.aclose()
        except Exception:  # noqa: BLE001
            pass

    async def _loop(self) -> None:
        while not self._stop:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                logger.debug("madeonsol poll failed: %s", exc)
            wait = self.poll_s + self._backoff_s
            self._backoff_s = 0.0
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                break

    async def _poll_once(self) -> None:
        r = await self._client.get(
            f"{self.base_url}/kol/feed",
            params={"limit": 100, "exclude_sells": "true",
                    "min_sol": 0.5, "min_kol_winrate": 50},
            headers={"Authorization": f"Bearer {self.api_key}",
                     "accept": "application/json"},
        )
        if r.status_code == 429:
            self._backoff_s = 300.0
            logger.warning("madeonsol 429 — backing off 5m (quota)")
            return
        if r.status_code != 200:
            logger.debug("madeonsol feed HTTP %s", r.status_code)
            return
        data = r.json()
        trades = data.get("trades") or []
        now = time.time()
        for t in trades:
            try:
                ts = datetime.fromisoformat(t["traded_at"]).timestamp()
            except (KeyError, ValueError, TypeError):
                continue
            if now - ts > self.window_s:
                continue
            mint = t.get("token_mint")
            if not mint:
                continue
            self._buys.setdefault(mint, []).append({
                "ts": ts,
                "wallet": t.get("wallet_address", ""),
                "kol": t.get("kol_name", "?"),
                "winrate_7d": t.get("kol_winrate_7d") or 0,
                "sol": t.get("sol_amount") or 0,
            })
        # Prune outside window
        cutoff = now - self.window_s
        for mint in list(self._buys):
            kept = [b for b in self._buys[mint] if b["ts"] >= cutoff]
            if kept:
                self._buys[mint] = kept
            else:
                del self._buys[mint]
        self._polls += 1
        self._last_ok_ts = now
        logger.debug("madeonsol: poll #%d ok, tracking %d mints",
                     self._polls, len(self._buys))

    def kol_footprint(self, mint: str) -> dict[str, Any]:
        """In-memory overlap check. NEVER does I/O — safe in the hot path.

        Returns ``{"buys", "kols", "max_winrate_7d", "top_kol"}``.
        """
        rows = self._buys.get(mint) or []
        if not rows:
            return {"buys": 0, "kols": 0, "max_winrate_7d": 0, "top_kol": ""}
        kols = {b["wallet"] for b in rows}
        best = max(rows, key=lambda b: b["winrate_7d"])
        return {"buys": len(rows), "kols": len(kols),
                "max_winrate_7d": best["winrate_7d"], "top_kol": best["kol"]}

    async def token_flow(self, mint: str) -> dict[str, Any] | None:
        """Per-mint KOL flow (``/kol/tokens/{mint}``). Needs a real key.

        Demo key returns endpoint_not_allowed → None (fail-open). Provided
        so upgrading is a one-line caller change, not a new client.
        """
        try:
            r = await self._client.get(
                f"{self.base_url}/kol/tokens/{mint}",
                headers={"Authorization": f"Bearer {self.api_key}",
                         "accept": "application/json"},
            )
            if r.status_code != 200:
                return None
            return r.json()
        except Exception:  # noqa: BLE001
            return None
