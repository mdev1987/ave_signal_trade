"""Smart-money watcher: poll the discovered wallets, alert on new buys.

Single-provider design (Shyft):
  One ``getTransactionsForAddress`` call per wallet per sweep returns FULL
  transactions since the last sweep — server-side blockTime filter, parsed
  token balances included. Buys are derived locally as "wallet received a
  non-SOL token it didn't hold before"; USD value is enriched from the
  DexScreener price feed that the shadow book already uses.

Alerts:
  - tracked wallet buys an unseen CA -> 🕵️ Smart buy
  - >= CONSENSUS_WALLETS into same unseen CA within window -> 🔥 Consensus

State: per-wallet last-seen blockTime persisted so restarts resume cleanly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path

import httpx

import logs

logger = logging.getLogger(__name__)

SOL = "So11111111111111111111111111111111111111112"
WSOL = SOL


def parse_shyft_buys(wallet: str, txs: list[dict]) -> list[dict]:
    """Derive buy rows from full Shyft transaction payloads.

    A row is emitted when the tracked wallet's balance of a non-wSOL token
    INCREASED inside a successful transaction. Returns oldest-first.
    """
    rows: list[dict] = []
    for tx in txs or []:
        if (tx.get("meta") or {}).get("err") is not None:
            continue
        bt = tx.get("blockTime") or 0
        meta = tx.get("meta") or {}
        pre = {(b.get("accountIndex")): b for b in meta.get("preTokenBalances") or []}
        for pb in meta.get("postTokenBalances") or []:
            mint = pb.get("mint")
            if not mint or mint == WSOL:
                continue
            if pb.get("owner") != wallet:
                continue
            pre_amt = 0.0
            old = pre.get(pb.get("accountIndex"))
            if old and old.get("mint") == mint:
                try:
                    pre_amt = float(old.get("uiTokenAmount", {}).get("uiAmount") or 0)
                except (TypeError, ValueError):
                    pre_amt = 0.0
            try:
                post_amt = float(
                    pb.get("uiTokenAmount", {}).get("uiAmount") or 0)
            except (TypeError, ValueError):
                continue
            delta = post_amt - pre_amt
            if delta <= 0:
                continue
            rows.append({"wallet": wallet, "ca": mint, "ts": float(bt),
                         "amount": delta})
    rows.sort(key=lambda r: r["ts"])
    return rows


class SmartWalletWatcher:
    def __init__(
        self,
        *,
        shyft_key: str,
        shyft_rpc: str = "https://rpc.shyft.to",
        ds=None,
        notifier=None,
        wallets_file: str = "smart_money_wallets.json",
        state_file: str = "watcher_state.json",
        tokens_file: str = "watched_tokens.json",
        poll_s: float = 45.0,
        min_buy_usd: float = 100.0,
        consensus_wallets: int = 2,
        consensus_window_s: float = 7200.0,
        on_smart_buy=None,  # async fn(ca, symbol, usd, n_wallets)
        first_lookback_s: float = 600.0,
        manage_interval_s: float = 1.0,
    ) -> None:
        wf = Path(wallets_file)
        data = json.loads(wf.read_text()) if wf.exists() else {}
        self.wallets = list(data.keys())
        self.shyft_key = shyft_key
        self.shyft_rpc = shyft_rpc.rstrip("/")
        self.ds = ds
        self.notifier = notifier
        self.poll_s = poll_s
        self.min_buy_usd = min_buy_usd
        self.consensus_wallets = consensus_wallets
        self.consensus_window_s = consensus_window_s
        self.first_lookback_s = first_lookback_s
        self.state_file = Path(state_file)
        self.tokens_file = Path(tokens_file)
        self._http = httpx.AsyncClient(timeout=25)
        self.state: dict[str, float] = {}         # wallet -> last blockTime
        self.known_cas: set[str] = set()
        self.consensus_alerted: set[str] = set()  # CAs that already had a consensus alert
        self.token_hits: dict[str, dict] = {}
        self.consensus_fired = 0
        self.on_smart_buy = on_smart_buy
        self.tatum_push = False
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._price_cache: dict[str, tuple[float, float]] = {}
        self._last_rpc_ts = 0.0
        self._load_state()

    # ------------------------------------------------------------- state io
    def _load_state(self) -> None:
        if not self.state_file.exists():
            return
        try:
            st = json.loads(self.state_file.read_text())
            sigs = st.get("last_sig") or {}
            # migrate legacy signature-based state: start fresh lookback
            self.state = {k: v for k, v in st.items()
                          if k.startswith("ts:") and isinstance(v, (int, float))}
            if not self.state and sigs:
                cutoff = time.time() - self.first_lookback_s
                for w in sigs:
                    self.state[f"ts:{w}"] = cutoff
            self.known_cas = set(st.get("known_cas", []))
            self.token_hits = st.get("token_hits", {})
        except Exception:
            logger.exception("watcher state load failed")

    def _save_state(self) -> None:
        try:
            self.state_file.write_text(json.dumps({
                **{f"ts:{w}": t for w, t in self.state.items()},
                "known_cas": sorted(self.known_cas),
                "token_hits": self.token_hits,
            }, indent=1))
        except Exception:
            logger.exception("watcher state save failed")

    # ---------------------------------------------------------------- shyft
    async def _throttle(self, min_gap_s: float = 0.15) -> None:
        """Keep Shyft RPC under the plan's 10 req/sec (burst-safe)."""
        now = time.monotonic()
        wait = self._last_rpc_ts + min_gap_s - now
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_rpc_ts = time.monotonic()

    async def _fetch_txs(self, wallet: str, since_ts: float) -> list[dict]:
        base = self.shyft_rpc.split("?")[0].rstrip("/")
        url = f"{base}?api_key={self.shyft_key}"
        txs: list[dict] = []
        cursor = None
        for _ in range(4):                      # up to ~400 txs per sweep
            filters = {"blockTime": {"gte": int(since_ts)},
                       "status": "any"}
            params: list = [wallet, {
                "transactionDetails": "full",
                "encoding": "json",
                "limit": 100,
                "commitment": {"commitment": "confirmed"},
                "filters": filters,
            }]
            if cursor:
                params[1]["paginationToken"] = cursor
            for attempt in range(2):          # one 429-retry
                await self._throttle()
                try:
                    r = await self._http.post(url, json={
                        "jsonrpc": "2.0", "id": "1",
                        "method": "getTransactionsForAddress",
                        "params": params})
                except Exception as exc:
                    logger.warning("shyft %s… error %s", wallet[:10], exc)
                    return txs
                if r.status_code == 429:
                    await asyncio.sleep(0.6 * (attempt + 1))
                    continue
                break
            if r.status_code != 200:
                logger.warning("shyft HTTP %s for %s…",
                               r.status_code, wallet[:10])
                return txs
            j = r.json()
            res = j.get("result") or {}
            batch = res.get("data") or []
            txs += batch
            cursor = res.get("paginationToken")
            if not cursor or len(batch) < 100:
                break
        return txs

    async def _usd(self, ca: str, amount: float) -> float:
        """USD value of ``amount`` tokens via DexScreener (60s cache)."""
        hit = self._price_cache.get(ca)
        now = time.time()
        if not hit or now - hit[0] > 60:
            px = 0.0
            if self.ds is not None:
                try:
                    snap = await self.ds.token_pairs("solana", ca)
                    pair = max(snap["pairs"], key=lambda p: (
                        p.get("liquidity") or {}).get("usd") or 0) \
                        if snap and snap.get("pairs") else None
                    px = float(pair.get("priceUsd") or 0) if pair else 0.0
                except Exception:
                    px = 0.0
            hit = (now, px)
            self._price_cache[ca] = hit
        return hit[1] * amount

    # ------------------------------------------------------------- pipeline
    async def process_now(self, wallet: str) -> None:
        """Decode a wallet's last hour of activity now (push entrypoint)."""
        await self._sweep_wallet(wallet, since=time.time() - 3600)

    async def _sweep_wallet(self, wallet: str,
                            since: float | None = None) -> None:  # noqa: C901
        if since is None:
            since = self.state.get(f"ts:{wallet}",
                                   time.time() - self.first_lookback_s)
        txs = await self._fetch_txs(wallet, since)
        if not txs:
            return
        buys = parse_shyft_buys(wallet, txs)
        newest = max((t.get("blockTime") or 0) for t in txs) if txs else int(since)
        for b in buys:                          # oldest first
            try:
                b["usd"] = await self._usd(b["ca"], b["amount"])
                await self._process_buy(wallet, b)
            except Exception:
                logger.exception("buy processing failed %s", b.get("ca","")[:10])
        cur = self.state.get(f"ts:{wallet}", 0)
        self.state[f"ts:{wallet}"] = max(cur, newest + 1)

    async def _process_buy(self, wallet: str, b: dict) -> None:
        ca = b["ca"]
        now = time.time()
        hit = self.token_hits.setdefault(
            ca, {"symbol": "?", "wallets": [], "first_ts": now, "usd": 0.0})
        already = wallet in [x["w"] for x in hit["wallets"]]
        usd = b.get("usd") or 0.0
        hit["usd"] += usd
        if not already:
            hit["wallets"].append({"w": wallet, "usd": usd, "ts": now})
        fresh = ca not in self.known_cas
        if not already:
            logs.journal("smart_buy_seen", ca=ca, wallet=wallet[:10],
                         usd=round(usd, 2), n_smart=len(hit["wallets"]),
                         fresh=fresh)
        if len(hit["wallets"]) < self.consensus_wallets and usd < self.min_buy_usd:
            return
        if fresh:
            self.known_cas.add(ca)
        n = len(hit["wallets"])
        if not fresh and n < self.consensus_wallets:
            return
        consensus = n >= self.consensus_wallets and ca not in getattr(self, "_consensus_sent", set())
        if consensus:
            self.consensus_fired += 1
            if not hasattr(self, "_consensus_sent"):
                self._consensus_sent = set()
            self._consensus_sent.add(ca)
        icon = "🔥" if consensus else "🕵️"
        title = "CONSENSUS BUY" if consensus else "Smart wallet buy"
        syms = ",".join(x["w"][:5] + "…($" + format(x["usd"], ".0f") + ")"
                        for x in hit["wallets"][-4:])
        # Telegram: CONSENSUS only — single-wallet buys go to journal/log
        if self.notifier and consensus:
            import asyncio as _aio
            _aio.get_running_loop().create_task(self.notifier.send_alert(
                f"{icon} {title}: {hit['symbol'] if hit['symbol']!='?' else ca[:6]+'…'}",
                f"📍 `{ca}`\n🕵️ Smart wallets ({n}): {syms}\n"
                f"💵 Tracked volume ${hit['usd']:,.0f}"))
        if not consensus:
            logger.info("%s %s %s (%s) — journal only", title,
                        b.get("symbol","?"), ca[:10], syms)
        if self.on_smart_buy:
            try:
                await self.on_smart_buy(ca, hit.get("symbol",""), usd, n)
            except Exception:
                logger.exception("on_smart_buy callback failed")

    async def run(self) -> None:
        logger.info("watcher started: %d wallets, poll %.0fs (shyft)",
                    len(self.wallets), self.poll_s)
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
            wait = max(1.0, self.poll_s - (time.time() - t0))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except TimeoutError:
                pass

    def start(self) -> None:
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
        await self._http.aclose()
