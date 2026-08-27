"""Smart-money wallet discovery (batch enrichment, NOT the live watch loop).

This module answers "which wallets should we track?" — the complement to the
live ``SmartWalletWatcher`` which only *follows* a fixed ``smart_money_wallets.json``.

No provider exposes a "top smart wallets" list directly, so discovery is a
2-hop pipeline built from what each source *does* give us:

  1. Candidate tokens — where smart money would be active:
       * DeBot activity_rank / heatmap  (tokens Telegram KOL channels are
         calling right now + their historical max pump gain)
       * DexPaprika getTopTokens        (hot movers by volume / % change)
       * DexPaprika fresh-pool filter   (brand-new pools, where entries are early)
  2. Buyer extraction — for each candidate token's top pool, pull recent swaps
     and collect the ``sender`` wallet of every swap that BOUGHT the target
     token. Proven against live data: a fresh +1600% token's pool returned
     13+ distinct buyer wallets in 20 transactions.

Wallets are then scored (default composite below; the scorer is swappable) and
the top-K are merged into ``smart_money_wallets.json`` (the same file the
live watcher reads), preserving any existing tracked wallets.

Run: ``uv run main.py discover`` — hitting live APIs, so rate-limited and
best scheduled nightly, not per-sweep.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import httpx

import logs

logger = logging.getLogger(__name__)

_BASE = "https://api.dexpaprika.com"
SOL = "So11111111111111111111111111111111111111112"
# Free tier is 15 req/min (30 with a key). Keep a safe margin for batch use.
_DEFAULT_RPM = 15


# --------------------------------------------------------------- dex paprika --
class DexPaprikaClient:
    """Minimal keyless DexPaprika REST client with a sliding-window limiter.

    Only the endpoints the discovery pipeline needs are wrapped. Every method
    returns ``None`` on failure so callers treat absence as "no data".
    """

    def __init__(self, rpm: int = _DEFAULT_RPM, timeout_s: float = 20.0) -> None:
        self.rpm = max(1, int(rpm))
        self.timeout_s = timeout_s
        self._client = httpx.AsyncClient(timeout=timeout_s, follow_redirects=True)
        self._lock = asyncio.Lock()
        self._last_ts = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def _throttle(self) -> None:
        """Enforce minimum spacing so bursts don't trip the 15 req/min cap."""
        async with self._lock:
            gap = 60.0 / self.rpm + 0.05
            now = time.monotonic()
            wait = self._last_ts + gap - now
            if wait > 0:
                await asyncio.sleep(min(wait, gap))
            self._last_ts = time.monotonic()

    async def _get(self, path: str, params: dict | None = None) -> Any | None:
        await self._throttle()
        try:
            r = await asyncio.wait_for(
                self._client.get(f"{_BASE}{path}", params=params),
                timeout=self.timeout_s + 2.0,
            )
            if r.status_code == 429:
                logger.warning("dexpaprika 429 (rate limit)")
                return None
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            logger.warning("dexpaprika GET %s failed: %s", path, e)
            return None

    async def get_token_details(self, chain: str, mint: str) -> dict | None:
        # REST: /networks/{chain}/tokens/{mint} -> single token object with
        # price_change_percentage_24h. Used to mark a candidate as "pumped"
        # without depending on DeBot (which is often auth-limited).
        j = await self._get(f"/networks/{chain}/tokens/{mint}")
        return j if isinstance(j, dict) else None

    async def get_top_tokens(self, chain: str = "solana", order_by: str = "volume_usd_24h",
                             limit: int = 20) -> list[dict] | None:
        # REST search endpoint (the MCP getTopTokens proxy): rows under "results".
        j = await self._get(f"/networks/{chain}/tokens/search",
                            {"order_by": order_by, "sort": "desc", "limit": limit})
        return j.get("results") if isinstance(j, dict) else None

    async def get_fresh_pools(self, chain: str = "solana",
                              created_after_ts: int | None = None,
                              limit: int = 20) -> list[dict] | None:
        # /pools/search with created_after (UNIX ts) + created_at desc.
        params: dict[str, Any] = {"order_by": "created_at", "sort": "desc",
                                  "limit": limit}
        if created_after_ts is not None:
            # DexPaprika requires an integer epoch (a fractional value 400s).
            params["created_after"] = int(created_after_ts)
        j = await self._get(f"/networks/{chain}/pools/search", params)
        return j.get("results") if isinstance(j, dict) else None

    async def get_token_pools(self, chain: str, token: str,
                              limit: int = 5) -> list[dict] | None:
        # getTokenPools proxies /pools/search with a token_address filter
        # (network-scoped), not /tokens/{token}/pools.
        j = await self._get(f"/networks/{chain}/pools/search",
                            {"token_address": token,
                             "order_by": "volume_usd_24h", "sort": "desc",
                             "limit": limit})
        return j.get("results") if isinstance(j, dict) else None

    async def get_pool_transactions(self, chain: str, pool: str,
                                    limit: int = 50) -> list[dict] | None:
        j = await self._get(f"/networks/{chain}/pools/{pool}/transactions",
                            {"limit": limit})
        return j.get("transactions") if isinstance(j, dict) else None


# ----------------------------------------------------------------- scoring ---
@dataclass
class WalletStat:
    address: str
    early_buys: int = 0           # bought within early_window of pool creation
    distinct_tokens: int = 0      # unique candidate tokens bought
    pumped_hits: int = 0          # of those, how many later pumped
    total_usd: float = 0.0        # total USD deployed across candidate tokens
    tokens: list[str] = field(default_factory=list)
    score: float = 0.0


def default_scorer(stats: list[WalletStat]) -> list[WalletStat]:
    """Composite: rewards early, consistent, winning entries.

    Weights (sum ~1.0):
      * 0.35 distinct_tokens       — breadth: betting on many independent winners
      * 0.30 early_buys            — timing: getting in at/near launch
      * 0.25 pumped_hits           — outcome: those picks actually pumped
      * 0.10 log(total_usd)        — conviction: size of deployment (minorized)
    Each component is normalized by the max observed value so the score stays
    in a stable 0..~1 range as the wallet universe grows.
    """
    if not stats:
        return stats
    max_tokens = max((s.distinct_tokens for s in stats), default=1) or 1
    max_early = max((s.early_buys for s in stats), default=1) or 1
    max_hits = max((s.pumped_hits for s in stats), default=1) or 1
    max_usd = max((s.total_usd for s in stats), default=1.0) or 1.0

    for s in stats:
        norm_usd = (s.total_usd / max_usd) ** 0.25  # compress large sizes
        s.score = round(
            0.35 * (s.distinct_tokens / max_tokens)
            + 0.30 * (s.early_buys / max_early)
            + 0.25 * (s.pumped_hits / max_hits)
            + 0.10 * norm_usd,
            4,
        )
    stats.sort(key=lambda s: s.score, reverse=True)
    return stats


# ------------------------------------------------------------ orchestrator ---
@dataclass
class CandidateToken:
    mint: str
    chain: str = "solana"
    symbol: str = "?"
    pumped: bool = False          # KOL-called or already big % move
    source: str = ""
    pool_addr: str | None = None  # known pool (fresh-pool source) -> skip re-query
    pool_born: float | None = None


class WalletDiscovery:
    """Turn token signals into a ranked wallet list for the live watcher."""

    def __init__(
        self,
        debot=None,                # DeBotClient (optional, supplementary)
        chain: str = "solana",
        max_tokens: int = 25,
        max_wallets: int = 40,
        early_window_s: float = 1800.0,   # "early" = within 30 min of pool birth
        tx_per_pool: int = 60,
        min_buy_usd: float = 50.0,
        out_file: str = "smart_money_wallets.json",
        pump_pct: float = 100.0,          # token "pumped" if 24h chg >= this
        enrich: bool = True,              # fetch token details to set `pumped`
        replace: bool = False,            # overwrite file instead of merging
        write_top_n: int = 0,             # if >0, only write top-N (0 = all)
        scorer: Callable[[list[WalletStat]], list[WalletStat]] = default_scorer,
    ) -> None:
        self.debot = debot
        self.chain = chain
        self.max_tokens = max_tokens
        self.max_wallets = max_wallets
        self.early_window_s = early_window_s
        self.tx_per_pool = tx_per_pool
        self.min_buy_usd = min_buy_usd
        self.out_file = Path(out_file)
        self.pump_pct = pump_pct
        self.enrich = enrich
        self.replace = replace
        self.write_top_n = int(write_top_n)
        self.scorer = scorer
        self.paprika = DexPaprikaClient()

    async def close(self) -> None:
        await self.paprika.close()

    # -------------------------------------------------------- candidate tokens
    async def gather_candidates(self) -> list[CandidateToken]:
        seen: dict[str, CandidateToken] = {}
        now = int(time.time())

        async def _add(tok: dict, source: str, pumped=False) -> None:
            mint = tok.get("address") or tok.get("id")
            if not mint or mint in seen:
                return
            sym = tok.get("symbol") or "?"
            chg = float(tok.get("price_change_percentage_24h") or 0)
            seen[mint] = CandidateToken(
                mint=mint, chain=tok.get("chain", self.chain),
                symbol=sym, pumped=(pumped or chg >= 100.0), source=source)

        # Candidate priority for *smart-money discovery*:
        #   1) FRESH pools  — where early entries are meaningful (best source)
        #   2) DeBot KOL signals (supplementary, when reachable)
        #   3) top-by-volume — only as filler; mega-caps (BTC/SOL/USDC) are
        #      noise for "early smart entry" and must not crowd out fresh pools.
        fresh = await self.paprika.get_fresh_pools(
            self.chain, created_after_ts=now - 24 * 3600, limit=self.max_tokens)
        for p in (fresh or []):
            # A fresh pool already gives us the pool address + birth time, so
            # carry it forward — extract_buyers can skip the re-query and the
            # early-buy window is measured against the real launch time.
            toks = p.get("tokens") or []
            mint = next((t.get("id") for t in toks
                         if t.get("id") != SOL), None)
            if not mint or mint in seen:
                continue
            sym = (p.get("token_0_symbol") or p.get("token_1_symbol") or "?")
            seen[mint] = CandidateToken(
                mint=mint, chain=self.chain, symbol=sym,
                pumped=False, source="dexpaprika:fresh",
                pool_addr=p.get("id"), pool_born=_to_ts(p.get("created_at")))

        # DeBot: tokens KOL channels are calling now (supplementary)
        if self.debot is not None:
            try:
                rank = await self.debot.activity_rank(chain=self.chain,
                                                      duration="1h", limit=30)
                for t in (rank or []):
                    await _add(t, "debot:rank", pumped=True)
                heat = await self.debot.heatmap(chain=self.chain)
                if heat:
                    for mint, h in heat.items():
                        if mint in seen and isinstance(h, dict):
                            gain = h.get("max_price_gain")
                            if isinstance(gain, (int, float)) and gain >= 1.0:
                                seen[mint].pumped = True
            except Exception:
                logger.warning("debot enrichment failed — continuing without it")

        # Top-by-volume filler (lowest priority — appended after fresh/DeBot).
        top = await self.paprika.get_top_tokens(self.chain, limit=self.max_tokens)
        for t in (top or [])[: self.max_tokens]:
            await _add(t, "dexpaprika:top")

        # Outcome-labeled candidates: tokens that ALREADY pumped hard. We want
        # the wallets that bought these BEFORE the move — i.e. real smart money.
        # This is the discovery signal that actually correlates with profit
        # (DeBot's heatmap was meant to supply it, but DeBot is often auth-limited).
        movers = await self.paprika.get_top_tokens(
            self.chain, order_by="price_change_percentage_24h", limit=self.max_tokens)
        for t in (movers or [])[: self.max_tokens]:
            chg = float(t.get("price_change_percentage_24h") or 0)
            if chg >= self.pump_pct:
                await _add(t, "dexpaprika:movers", pumped=True)

        cands = list(seen.values())[: self.max_tokens]
        if self.enrich:
            await self._enrich_pumped(cands)
        logs.journal("discovery_candidates", n=len(cands),
                     symbols=",".join(c.symbol for c in cands[:12]))
        return cands

    async def _enrich_pumped(self, cands: list["CandidateToken"]) -> None:
        """Mark candidates as pumped from their 24h price move.

        This gives the scorer a real *outcome* signal (early buyer of things
        that actually moved) without DeBot. DeBot's heatmap gain is OR-ed in
        earlier when reachable. The details endpoint exposes 24h high/low
        (not a direct % change), so we derive the range from those; for tokens
        that *do* carry ``price_change_percentage_24h`` we use it directly.
        """
        for c in cands:
            if c.pumped:
                continue
            try:
                d = await self.paprika.get_token_details(self.chain, c.mint)
            except Exception:
                continue
            if not d:
                continue
            chg = float(d.get("price_change_percentage_24h") or 0)
            if chg <= 0:
                ps = d.get("price_stats") or {}
                hi = float(ps.get("high_24h") or 0)
                lo = float(ps.get("low_24h") or 0)
                if hi > 0 and lo > 0:
                    chg = (hi / lo - 1.0) * 100.0
            if chg >= self.pump_pct:
                c.pumped = True

    # ---------------------------------------------------------- buyer extraction
    async def extract_buyers(self, tok: CandidateToken) -> list[dict]:
        """Return raw buyer rows for one token's top pool.

        A row = {wallet, ts, usd, early} where the swap BOUGHT the target token
        (target received: amount>0 on the token's side). USD is the target
        token's own UI volume x its USD price (consistent across DEXes).
        """
        rows: list[dict] = []
        if tok.pool_addr:
            pool_addr = tok.pool_addr
            pool_born = tok.pool_born
        else:
            pools = await self.paprika.get_token_pools(self.chain, tok.mint, limit=1)
            if not pools:
                return rows
            pool = pools[0]
            pool_addr = pool.get("id")
            pool_born = _to_ts(pool.get("created_at"))
        txs = await self.paprika.get_pool_transactions(
            self.chain, pool_addr, limit=self.tx_per_pool)
        if not txs:
            return rows
        for tx in txs:
            t0, t1 = tx.get("token_0"), tx.get("token_1")
            a0, a1 = _num(tx.get("amount_0")), _num(tx.get("amount_1"))
            # which side is the target token, and was it received (a buy)?
            # USD value = target token's own UI volume x its USD price. Using
            # the target's own fields is the only consistent normalization
            # (DexPaprika's volume_* on the quote side is unreliable per DEX).
            if t0 == tok.mint and a0 > 0:
                usd = _num(tx.get("volume_0")) * _num(tx.get("price_0_usd"))
            elif t1 == tok.mint and a1 > 0:
                usd = _num(tx.get("volume_1")) * _num(tx.get("price_1_usd"))
            else:
                continue
            wallet = tx.get("sender")
            if not wallet or usd < self.min_buy_usd:
                continue
            ts = _to_ts(tx.get("created_at"))
            early = pool_born is not None and ts is not None and \
                (ts - pool_born) <= self.early_window_s
            rows.append({"wallet": wallet, "ts": ts or 0,
                         "usd": usd, "early": early})
        return rows

    # --------------------------------------------------------------- pipeline
    async def run(self) -> list[WalletStat]:
        cands = await self.gather_candidates()
        agg: dict[str, WalletStat] = {}
        for tok in cands:
            try:
                rows = await self.extract_buyers(tok)
            except Exception:
                logger.exception("buyer extraction failed %s", tok.mint[:10])
                continue
            if not rows:
                continue
            seen_for_token: set[str] = set()
            for r in rows:
                w = r["wallet"]
                st = agg.setdefault(w, WalletStat(address=w))
                if w not in seen_for_token:
                    seen_for_token.add(w)
                    st.distinct_tokens += 1
                    st.tokens.append(tok.mint)
                    if tok.pumped:
                        st.pumped_hits += 1
                if r["early"]:
                    st.early_buys += 1
                st.total_usd += r["usd"]
            logs.journal("discovery_token", symbol=tok.symbol,
                         buyers=len({r["wallet"] for r in rows}))
        stats = self.scorer(list(agg.values()))
        top = stats[: self.max_wallets]
        if self.write_top_n > 0:
            top = top[: self.write_top_n]
        await self._merge_and_write(top)
        return top

    async def _merge_and_write(self, top: list[WalletStat]) -> None:
        """Write discoveries. By default merges with the existing wallet file
        (preserving curated wallets); with ``replace=True`` it overwrites.
        """
        existing: dict[str, Any] = {}
        if not self.replace and self.out_file.exists():
            try:
                existing = json.loads(self.out_file.read_text())
            except Exception:
                logger.warning("existing wallet file unreadable — starting fresh")
                existing = {}
        now = time.time()
        for st in top:
            prev = existing.get(st.address) or {}
            existing[st.address] = {
                "score": st.score,
                "distinct_tokens": st.distinct_tokens,
                "early_buys": st.early_buys,
                "pumped_hits": st.pumped_hits,
                "total_usd": round(st.total_usd, 2),
                "tokens": st.tokens,
                "added_ts": prev.get("added_ts", now),
                "updated_ts": now,
                "source": "discovery",
            }
        self.out_file.write_text(json.dumps(existing, indent=1))
        logs.journal("discovery_written", file=str(self.out_file),
                     total=len(existing), top_n=len(top))


# ------------------------------------------------------------------- helpers
def _to_ts(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        # DexPaprika returns RFC3339 strings; tolerate raw epoch seconds too
        return float(v)
    s = str(v).replace("Z", "+00:00")
    try:
        from datetime import datetime, timezone
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
