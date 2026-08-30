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


def _report_crash(task: asyncio.Task) -> None:
    """Surface (not swallow) an unexpected exception from a background task.

    Without this, a coroutine spawned via ``create_task`` that raises will
    fail silently and the bot keeps running minus that task (e.g. the whole
    sweep loop) — exactly the kind of "it just stopped finding trades" bug
    that is invisible in the logs.
    """
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("watcher background task crashed: %s", task.get_name())


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
        on_smart_buy=None,  # async fn(ca, symbol, usd, score, wallets)
        first_lookback_s: float = 600.0,
        manage_interval_s: float = 1.0,
        wallet_weights: dict | None = None,
        wallet_default_weight: float = 0.5,
        consensus_weight_threshold: float = 1.5,
        require_strong_wallet: bool = True,
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
        # Data-driven quality weights: address -> weight (0..max). Wallets absent
        # from the map (no perf data) fall back to wallet_default_weight so the
        # bot degrades to the legacy "equal wallets" behaviour instead of breaking.
        self.weights = dict(wallet_weights) if wallet_weights else {}
        self.default_weight = wallet_default_weight
        self.consensus_weight_threshold = consensus_weight_threshold
        self.require_strong_wallet = require_strong_wallet
        self.state_file = Path(state_file)
        self.tokens_file = Path(tokens_file)
        self._http = httpx.AsyncClient(timeout=25)
        self.state: dict[str, float] = {}         # wallet -> last blockTime
        self.known_cas: set[str] = set()
        self.consensus_alerted: set[str] = set()  # CAs that already had a consensus alert
        self.token_hits: dict[str, dict] = {}
        self.consensus_fired = 0
        self._consensus_sent: set[str] = set()  # legacy compat
        self._consensus_ts: dict[str, float] = {}  # ca -> timestamp for persistence
        self.wallet_perf: dict[str, dict] = {}   # addr -> {picks, hits} (live learning)
        self.on_smart_buy = on_smart_buy
        self.tatum_push = False
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._price_cache: dict[str, tuple[float, float, str | None]] = {}
        self._last_rpc_ts = 0.0
        # Wallet churn tracking: detect wallets that spray many tokens in
        # a short window (noise signal). Maps wallet -> [(ts, ca), ...].
        self._wallet_buys: dict[str, list[tuple[float, str]]] = {}
        self._load_state()

    # ------------------------------------------------------------- state io
    def _prune_token_hits(self) -> None:
        """Drop token_hits whose newest wallet buy is older than 2x consensus window.

        This prevents indefinite accumulation of stale consensus state
        (the data was lifetime-oriented before; now it has a bounded TTL).
        """
        now = time.time()
        ttl = self.consensus_window_s * 2
        stale = [ca for ca, h in self.token_hits.items()
                 if now - h.get("first_ts", 0) > ttl
                 and all(now - x.get("ts", 0) > ttl
                         for x in h.get("wallets", []))]
        for ca in stale:
            del self.token_hits[ca]

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
            self.wallet_perf = st.get("wallet_perf", {})
            # Restore consensus dedup (expire entries older than 2x consensus window)
            cons = st.get("consensus_sent", {})
            cutoff = time.time() - self.consensus_window_s * 2
            self._consensus_sent = {ca for ca, ts in cons.items() if ts > cutoff}
            self._prune_token_hits()
        except Exception:
            logger.exception("watcher state load failed")

    def _save_state(self) -> None:
        try:
            # self.state keys are already "ts:<wallet>" — do NOT re-prefix
            # Persist consensus dedup as {ca: timestamp} so restarts don't re-fire
            cons = {ca: ts for ca, ts in self._consensus_ts.items()}
            self.state_file.write_text(json.dumps({
                **self.state,
                "known_cas": sorted(self.known_cas),
                "token_hits": self.token_hits,
                "wallet_perf": self.wallet_perf,
                "consensus_sent": cons,
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
        """USD value of ``amount`` tokens via DexScreener (60s cache).

        The cache entry is ``(ts, price_usd, symbol)``; the symbol is reused
        by the caller to populate ``b["symbol"]`` for alerts.
        """
        hit = self._price_cache.get(ca)
        now = time.time()
        if not hit or now - hit[0] > 60:
            px, sym = 0.0, None
            if self.ds is not None:
                try:
                    snap = await self.ds.token_pairs("solana", ca)
                    # token_pairs() returns a normalized single-pair dict
                    # ({"price_usd": ...}) or None — not a {"pairs": [...]} wrapper
                    px = float(snap.get("price_usd") or 0) if snap else 0.0
                    sym = snap.get("symbol") if snap else None
                except Exception:
                    px, sym = 0.0, None
            hit = (now, px, sym)
            self._price_cache[ca] = hit
        return hit[1] * amount

    # ------------------------------------------------------------- pipeline
    async def process_now(self, wallet: str) -> None:
        """Decode a wallet's recent activity now (push entrypoint).

        Uses the persisted wallet timestamp + a small overlap buffer (5s)
        to handle late indexing, instead of blindly replaying the last hour
        which could re-fire stale consensus events.
        """
        persisted = self.state.get(f"ts:{wallet}", 0)
        since = max(
            persisted - 5 if persisted > 0 else 0,
            time.time() - self.first_lookback_s,
        )
        await self._sweep_wallet(wallet, since=since)

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
                cached = self._price_cache.get(b["ca"])
                if cached and cached[2]:
                    b["symbol"] = cached[2]
                await self._process_buy(wallet, b)
            except Exception:
                logger.exception("buy processing failed %s", b.get("ca","")[:10])
        cur = self.state.get(f"ts:{wallet}", 0)
        self.state[f"ts:{wallet}"] = max(cur, newest + 1)

    async def _process_buy(self, wallet: str, b: dict) -> None:
        ca = b["ca"]
        now = time.time()
        sym = b.get("symbol") or "?"
        hit = self.token_hits.setdefault(
            ca, {"symbol": sym, "wallets": [], "first_ts": now, "usd": 0.0})
        # upgrade from '?' once a real symbol is known
        if sym != "?" and hit["symbol"] == "?":
            hit["symbol"] = sym
        already = wallet in [x["w"] for x in hit["wallets"]]
        usd = b.get("usd") or 0.0
        # Use the ACTUAL transaction timestamp when available (b["ts"] comes
        # from Shyft blockTime). Fall back to now() only when absent (push
        # path that synthesises buys).  This makes churn + consensus window
        # measure wall-clock distance between trades, not processing lag.
        tx_ts = b.get("ts") or now
        qualifies = usd >= self.min_buy_usd
        # Wallet churn detection: a wallet spraying 40+ distinct tokens in 5
        # minutes is almost certainly noise (airdrops, bot activity, or a
        # non-selective accumulator). Penalise its weight by halving it.
        churn_ok = True
        if qualifies:
            buys = self._wallet_buys.setdefault(wallet, [])
            buys.append((tx_ts, ca))
            # Prune buys older than 5 minutes (using tx_ts, not now)
            cutoff = tx_ts - 300
            self._wallet_buys[wallet] = [(t, c) for t, c in buys if t > cutoff]
            distinct_tokens = len(set(c for _, c in self._wallet_buys[wallet]))
            if distinct_tokens >= 40:
                churn_ok = False
        # Quality weight: proven winners move the score; noise wallets (~0) can't
        # manufacture consensus on their own. Sub-threshold buys contribute 0.
        # Churning wallets get halved weight (still count, but less).
        wt = self.weights.get(wallet, self.default_weight) if qualifies else 0.0
        if qualifies and not churn_ok:
            wt *= 0.5
        if qualifies:
            hit["usd"] += usd
            if not already:
                hit["wallets"].append({"w": wallet, "usd": usd, "ts": tx_ts, "wt": wt})
        fresh = ca not in self.known_cas
        if not already:
            logs.journal("smart_buy_seen", ca=ca, wallet=wallet[:10],
                         usd=round(usd, 2), wt=wt, fresh=fresh)
        # Skip re-evaluation entirely for a sub-threshold buy on an already-known
        # token (we only re-score when a NEW qualifying wallet arrives, or it's
        # the first sighting).
        if not fresh and not qualifies:
            return
        if fresh:
            self.known_cas.add(ca)
        # ---- TIME-WINDOWED consensus: only wallets that bought within the
        # configured window contribute to the score.  This is the core fix
        # for noisy cross-session consensus (e.g. 10:00 + 10:08 + 10:25
        # treated as one event when the window is 600s).
        win_cutoff = now - self.consensus_window_s
        active = [x for x in hit["wallets"] if x.get("ts", 0) >= win_cutoff]
        # Weighted consensus score: sum of distinct buying wallets' quality weights.
        score = round(sum(x.get("wt", 0.0) for x in active), 3)
        # Only a genuine consensus (score >= threshold, not yet fired) is surfaced.
        # Sub-threshold / single-wallet activity is still recorded in the journal
        # but does NOT log or trigger opens — this removes the noisy low-conviction
        # signals that were flooding the log and opening bad positions.
        # require_strong_wallet: at least one contributing wallet must carry a real
        # edge (wt >= 1.0, i.e. >=60% win) so consensus is never manufactured by two
        # mediocre wallets alone.
        n_strong = sum(1 for x in active if x.get("wt", 0.0) >= 1.0)
        strong_ok = (not self.require_strong_wallet) or n_strong >= 1
        consensus = score >= self.consensus_weight_threshold and strong_ok and ca not in self._consensus_sent
        if not consensus:
            return
        self.consensus_fired += 1
        self._consensus_sent.add(ca)
        self._consensus_ts[ca] = now
        syms = ",".join(x["w"][:5] + "…(w" + format(x.get("wt", 0), ".2f") + ")"
                        for x in active[-4:])
        logger.info("CONSENSUS BUY %s %s (%s) score=%.2f — journal only",
                    hit.get("symbol", "?"), ca[:10], syms, score)
        if self.on_smart_buy:
            try:
                await self.on_smart_buy(ca, hit.get("symbol", ""), usd, score,
                                        [x["w"] for x in active])
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
            self._prune_token_hits()
            wait = max(1.0, self.poll_s - (time.time() - t0))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=wait)
            except TimeoutError:
                pass

    def start(self) -> None:
        self._task = asyncio.create_task(self.run())
        self._task.add_done_callback(_report_crash)

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
        await self._http.aclose()
