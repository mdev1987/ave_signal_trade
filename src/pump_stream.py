"""Live KOL-buy detection + USD pricing via the pumpapi.io WebSocket.

pumpapi.io (`wss://stream.pumpapi.io/`) is a free firehose of every Solana
pump/launchpad trade. We filter client-side for buys made by our tracked KOL
wallets. Each `buy` event carries `quoteAmount` (SOL) and `tokenAmount`, so
USD is computed as ``quoteAmount * SOL price`` — accurate even for brand-new
tokens DexScreener hasn't indexed yet (fixes the ``usd == 0`` problem that
silenced the Shyft polling path).

Detected buys are forwarded to ``SmartWalletWatcher._process_buy`` so they flow
through the exact same consensus / qualification / open logic as polling.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

import websockets

log = logging.getLogger(__name__)

WS_URL = "wss://stream.pumpapi.io/"
WSOL = "So11111111111111111111111111111111111111112"
_COINGECKO = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"

# on_buy(wallet, ca, symbol, usd, amount) -> awaited
BuyHook = Callable[[str, str, str, float, float], Awaitable[None]]


class PumpApiStream:
    def __init__(self, wallets, on_buy: BuyHook, http, sol_fallback: float = 150.0):
        self.wallets = set(wallets)
        self.on_buy = on_buy
        self.http = http
        self._stop = asyncio.Event()
        self._sym: dict[str, str] = {}          # mint -> symbol (create events)
        self._sol = sol_fallback
        self._sol_t = 0.0

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._loop()
            except asyncio.CancelledError:
                raise
            except Exception as exc:                       # noqa: BLE001
                log.warning("pumpapi stream dropped: %s", exc)
                await asyncio.sleep(3)

    async def _loop(self) -> None:
        async with websockets.connect(WS_URL, ping_interval=20,
                                      ping_timeout=20) as ws:
            log.info("pumpapi ws connected (%d wallets)", len(self.wallets))
            async for message in ws:
                if self._stop.is_set():
                    break
                try:
                    ev = json.loads(message)
                except Exception:
                    continue
                action = ev.get("action")
                if action == "create":
                    m = ev.get("mint")
                    if m:
                        self._sym[m] = ev.get("symbol") or ev.get("name") or "?"
                    continue
                if action != "buy":
                    continue
                # Collect every wallet involved in this buy.
                traders = {ev.get("txSigner")}
                for bd in ev.get("breakdown") or []:
                    if bd.get("action") == "buy":
                        traders.add(bd.get("trader"))
                hit = traders & self.wallets
                if not hit:
                    continue
                ca = ev.get("mint")
                if not ca:
                    continue
                quote = float(ev.get("quoteAmount") or 0.0)
                amount = float(ev.get("tokenAmount") or 0.0)
                sym = self._sym.get(ca) or ev.get("symbol") or ev.get("name") or "?"
                usd = quote * await self._sol_usd()
                # Forward EVERY tracked wallet in this event, not just one.
                # A single PumpAPI buy can contain multiple KOL wallets; only
                # forwarding one destroys the consensus signal.
                for wallet in hit:
                    try:
                        await self.on_buy(wallet, ca, sym, usd, amount)
                    except Exception:
                        log.exception("pumpapi on_buy failed for %s", wallet[:10])

    async def _sol_usd(self) -> float:
        now = time.time()
        if self._sol_t and now - self._sol_t < 30:
            return self._sol
        try:
            r = await self.http.get(_COINGECKO)
            if r.status_code == 200:
                j = r.json()
                self._sol = float(j["solana"]["usd"])
                self._sol_t = now
        except Exception:
            pass
        return self._sol

    def stop(self) -> None:
        self._stop.set()
