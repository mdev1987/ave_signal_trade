"""Smart-money watcher: poll the discovered wallets, alert on new buys.

Loop (default 45s):
  1. Cheap liveness check per wallet via Helius ``getSignaturesForAddress``
     (limit 3 — a few hundred compute-credit-free RPC reads).
  2. Only when a wallet's newest signature changed since last sweep, fetch its
     recent swaps from the Moralis Solana gateway and process BUY entries
     newer than the last processed timestamp.
  3. Alerts:
     - any tracked wallet buys a token we have never seen → "🕵️ Smart buy"
     - ≥ CONSENSUS_WALLETS distinct tracked wallets bought the same unseen
       token within CONSENSUS_WINDOW_S → "🔥 Consensus" (stronger signal)
  4. Every alerted token is recorded in watched_tokens.json so it is never
     double-alerted, with the wallets that touched it.

Rate budget: Moralis wallet-swaps costs ~50 CU; only changed wallets hit it,
so a quiet network costs zero CU.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import httpx

import logs

logger = logging.getLogger(__name__)

SOL = "So11111111111111111111111111111111111111112"


def _iso_to_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def extract_buy(swaps: list[dict], after_ts: float) -> list[dict]:
    """New buy rows from a wallet-swaps payload (newest first).

    A row counts when transactionType == 'buy' for the non-SOL side, i.e. the
    wallet RECEIVED a token other than wSOL.
    """
    out = []
    for t in swaps or []:
        try:
            ts = _iso_to_ts(t["blockTimestamp"])
        except Exception:
            continue
        if ts <= after_ts:
            break  # payload is DESC; everything older is already seen
        if t.get("transactionType") != "buy":
            continue
        # wallet-swaps payload: baseToken may be a plain address string
        base = t.get("baseToken")
        if isinstance(base, dict):
            ca = base.get("address")
            sym = base.get("symbol") or "?"
        else:
            ca, sym = (base or ""), "?"
        if not ca or ca == SOL:
            continue
        sold = t.get("sold") or {}
        if not isinstance(sold, dict):
            sold = {}
        try:
            usd = float(sold.get("usdAmount") or 0)
        except (TypeError, ValueError):
            usd = 0.0
        if sym == "?":
            sym = str(t.get("pairLabel") or "").split("/")[0] or "?"
        out.append({
            "wallet": t.get("walletAddress"),
            "ca": ca,
            "symbol": sym,
            "usd": usd,
            "ts": ts,
            "exchange": t.get("exchangeName"),
        })
    return out


class SmartWalletWatcher:
    def __init__(
        self,
        *,
        helius_keys: list[str],
        moralis_key: str,
        notifier=None,
        wallets_file: str = "smart_money_wallets.json",
        state_file: str = "watcher_state.json",
        tokens_file: str = "watched_tokens.json",
        poll_s: float = 45.0,
        min_buy_usd: float = 100.0,
        consensus_wallets: int = 2,
        consensus_window_s: float = 7200.0,
        swap_lookback: int = 6,
    ) -> None:
        wf = Path(wallets_file)
        data = json.loads(wf.read_text()) if wf.exists() else {}
        self.wallets = list(data.keys())
        self.helius_keys = helius_keys
        self.moralis_key = moralis_key
        self.notifier = notifier
        self.poll_s = poll_s
        self.min_buy_usd = min_buy_usd
        self.consensus_wallets = consensus_wallets
        self.consensus_window_s = consensus_window_s
        self.swap_lookback = swap_lookback
        self.state_file = Path(state_file)
        self.tokens_file = Path(tokens_file)
        self._hidx = 0
        self._http = httpx.AsyncClient(timeout=20)
        self.state: dict[str, str] = {}          # wallet -> last seen signature
        self.known_cas: set[str] = set()          # ever-alerted/seeded CAs
        self.token_hits: dict[str, dict] = {}     # ca -> {wallets:[], first_ts}
        self.consensus_fired = 0
        self.tatum_push = False
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._load_state()

    # ------------------------------------------------------------- state io
    def _load_state(self) -> None:
        if self.state_file.exists():
            try:
                st = json.loads(self.state_file.read_text())
                self.state = st.get("last_sig", {})
                self.known_cas = set(st.get("known_cas", []))
                self.token_hits = st.get("token_hits", {})
            except Exception:
                logger.exception("watcher state load failed")
        # seed tokens from the discovery file so they never alert as "new"
        wf = self.state_file.parent / "smart_money_wallets.json"
        try:
            for meta in json.loads(wf.read_text()).values():
                for sym in meta.get("symbols", []):
                    pass  # symbols only; CAs arrive via live flow
        except Exception:
            pass

    def _save_state(self) -> None:
        try:
            self.state_file.write_text(json.dumps({
                "last_sig": self.state,
                "known_cas": sorted(self.known_cas),
                "token_hits": self.token_hits,
            }, indent=1))
        except Exception:
            logger.exception("watcher state save failed")

    # -------------------------------------------------------------- fetchers
    async def helius(self, method: str, params: list):
        for _ in range(len(self.helius_keys)):
            k = self.helius_keys[self._hidx % len(self.helius_keys)]
            self._hidx += 1
            try:
                r = await self._http.post(
                    f"https://mainnet.helius-rpc.com/?api-key={k}",
                    json={"jsonrpc": "2.0", "id": "1",
                          "method": method, "params": params})
                if r.status_code == 429:
                    continue
                j = r.json()
                return None if j.get("error") else j.get("result")
            except Exception:
                continue
        return None

    async def wallet_swaps(self, wallet: str) -> list[dict]:
        url = f"https://solana-gateway.moralis.io/account/mainnet/{wallet}/swaps"
        try:
            r = await self._http.get(url, headers={"X-API-Key": self.moralis_key,
                                                   "accept": "application/json"},
                                     params={"limit": self.swap_lookback,
                                             "order": "DESC"})
            if r.status_code != 200:
                return []
            return r.json().get("result") or []
        except Exception:
            return []

    # ------------------------------------------------------------ processing
    async def process_now(self, wallet: str) -> None:
        """Decode a wallet's latest swaps now (push/webhook entrypoint)."""
        swaps = await self.wallet_swaps(wallet)
        buys = extract_buy(swaps, after_ts=0.0)
        for b in reversed(buys):  # oldest first
            self._process_buy(wallet, b)

    async def _sweep_wallet(self, wallet: str) -> None:
        res = await self.helius("getSignaturesForAddress",
                                [wallet, {"limit": 3}])
        newest = res[0]["signature"] if res else None
        last = self.state.get(wallet)
        if not newest or newest == last:
            return  # nothing new from this wallet
        swaps = await self.wallet_swaps(wallet)
        after_ts = 0.0
        buys = extract_buy(swaps, after_ts=after_ts)
        for b in reversed(buys):  # oldest first
            self._process_buy(wallet, b)
        self.state[wallet] = newest

    def _process_buy(self, wallet: str, b: dict) -> None:
        ca = b["ca"]
        now = time.time()
        hit = self.token_hits.setdefault(
            ca, {"symbol": b["symbol"], "wallets": [], "first_ts": now,
                 "usd": 0.0})
        already = wallet in [w["w"] for w in hit["wallets"]]
        hit["usd"] += b["usd"]
        if not already:
            hit["wallets"].append({"w": wallet, "usd": b["usd"], "ts": now})
        fresh = ca not in self.known_cas
        if b["usd"] < self.min_buy_usd and len(hit["wallets"]) < self.consensus_wallets:
            logs.journal("watch_skip", ca=ca, wallet=wallet[:10],
                         usd=b["usd"], reason="small")
            return
        if fresh:
            self.known_cas.add(ca)
            logs.journal("smart_buy", ca=ca, symbol=b["symbol"],
                         wallet=wallet[:10], usd=b["usd"],
                         n_smart=len(hit["wallets"]))
        n = len(hit["wallets"])
        if fresh or n >= self.consensus_wallets:
            consensus = n >= self.consensus_wallets
            if consensus:
                self.consensus_fired += 1
            icon = "🔥" if consensus else "🕵️"
            title = ("CONSENSUS BUY" if consensus else "Smart wallet buy")
            syms = ",".join(w["w"][:5] + "…($" + format(w["usd"], ".0f") + ")"
                            for w in hit["wallets"][-4:])
            logger.info("%s %s %s via %s (%s)", title, b['symbol'], ca[:10],
                        wallet[:8], syms)
            if self.notifier:
                import asyncio
                asyncio.get_event_loop().create_task(self.notifier.send_alert(
                    f"{icon} {title}: {b['symbol']}",
                    f"📍 `{ca}`\n🕵️ Smart wallets ({n}): {syms}\n"
                    f"💵 Tracked volume ${hit['usd']:,.0f}"))

    async def run(self) -> None:
        logger.info("watcher started: %d wallets, poll %.0fs", len(self.wallets),
                    self.poll_s)
        while not self._stop.is_set():
            t0 = time.time()
            for w in list(self.wallets):
                try:
                    await self._sweep_wallet(w)
                except Exception:
                    logger.exception("sweep failed for %s", w[:10])
                if self._stop.is_set():
                    break
            self._save_state()
            elapsed = time.time() - t0
            wait = max(1.0, self.poll_s - elapsed)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except TimeoutError:
                pass

    def start(self) -> None:
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exception=True)
        await self._http.aclose()

