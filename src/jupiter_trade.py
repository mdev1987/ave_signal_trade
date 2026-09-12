"""Jupiter Swap API V2 client — /order + /execute managed swaps.

Two API calls, no RPC needed:

1. ``GET /swap/v2/order``  — Jupiter quotes and assembles a transaction
   (``transaction`` base64 + ``requestId``) with all routing engines
   competing for the best price.
2. ``POST /swap/v2/execute`` — we sign the transaction locally and Jupiter
   lands it with confirmation and retry.

We trade **SOL (WSOL)** only to match the bot's SOL-denominated paper model.
Amounts are passed in raw base units (WSOL has 9 decimals). Buy proceeds are
captured from ``/execute``'s raw ``totalOutputAmount`` so we never need the
token's decimals to sell later.

**DRY_RUN gating** — the whole client is safe in paper mode:

- ``DRY_RUN=true`` (default): a throwaway keypair is derived so the quote
  gate runs, but the ``taker`` is omitted and nothing is ever signed or
  executed (``/order`` returns just the quote, no transaction).
- ``DRY_RUN=false``: real trading. Requires a base58 ``PRIVATE_KEY`` in
  ``.env``; construction fails fast otherwise so a missing key can never
  masquerade as live trading.

**Quote gate** — before ever executing a buy we hit ``/order`` and validate
the assembled route: we reject when there is no usable route, the out amount
is zero, or price impact exceeds the configured cap. A new launch often
briefly has no route, so we retry with a short delay. All quote requests are
throttled (global rate limit) and briefly cached to collapse launch bursts,
with latency measured to catch the next bottleneck.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import httpx
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana_rpc_resilient import ResilientRPCClient, Ok, Err
from jupiter_swap.tokens import TokenClient

import config
import logs

log = logging.getLogger(__name__)

BASE_MINT = "So11111111111111111111111111111111111111112"  # WSOL
BASE_DECIMALS = 9

# Slippage escalation ladder when a sell keeps failing (basis points).
# Must be strictly ascending and deduplicated. The sell() method prepends
# the base slippage (self._slippage_bps) and deduplicates automatically.
SELL_SLIPPAGE_ESCALATION = (500, 1000)

# Recent-latency samples kept for p50/p95 percentiles in quote_summary().
_LATENCY_SAMPLES_MAX = 500

# Hard wall-clock bounds for every network call. httpx `timeout=` covers
# connect/read/write/pool but NOT DNS resolution — a wedged resolver can hang
# the coroutine past any httpx timeout, which froze the whole event loop for
# 5+ minutes (heartbeat + Telegram polling + sweep all went silent). Every
# network await is wrapped in asyncio.wait_for with these caps so a hung
# socket/resolver can only stall the caller, never the loop.
_DEFAULT_ORDER_TIMEOUT_S = 20.0
_DEFAULT_EXECUTE_TIMEOUT_S = 60.0   # Jupiter lands + confirms the tx server-side
_DEFAULT_RPC_TIMEOUT_S = 12.0
_DEFAULT_QUOTE_CACHE_MAX = 500      # bounded cache: (mint, amount) -> QuoteResult


class JupiterError(RuntimeError):
    """Raised when a swap order/execute fails and cannot be retried."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class SwapResult:
    """Outcome of an executed swap."""

    success: bool
    signature: str
    input_amount: int  # raw: what went in
    output_amount: int  # raw: what came out
    error: str = ""
    error_code: int | None = None  # Jupiter error code for classification

    @property
    def is_retryable_quote_error(self) -> bool:
        """True when the error means 're-quote needed' (not a market hostility)."""
        return self.error_code in (-2003,)  # quote expired

    @property
    def is_slippage_error(self) -> bool:
        """True when slippage was too aggressive — widen and retry."""
        return self.error_code in (6024, 6051)  # DexNotEnoughAmountOut, DexExceedsAmountInMax

    @property
    def is_hostile_market(self) -> bool:
        """True when the market is actively hostile — skip, don't retry."""
        return self.error_code in (-2004,)  # swap rejected (honeypot/bundled)


@dataclass(frozen=True)
class QuoteResult:
    """Outcome of a quote-gate call (verified order or a skip reason)."""

    success: bool
    order: dict | None  # valid ``/order`` payload, ready to execute
    input_amount: int  # raw SOL
    output_amount: int  # raw expected out
    price_impact_pct: float
    route_count: int
    latency_ms: float
    reason: str = ""  # see skip taxonomy below
    fetched_at: float = 0.0  # time.monotonic() when the order was fetched

    # Skip taxonomy — the trader counts these, so an API outage is never
    # mistaken for "no route":
    #   "ok"                     verified order, ready to execute
    #   "quote_no_route"         Jupiter found no usable route (400 "No route")
    #   "quote_impact"           route exists but price impact above the cap
    #   "quote_timeout"          the /order request timed out
    #   "quote_http_error"       transport/5xx/other 4xx failure
    #   "quote_invalid_response" 200 but payload unusable (bad JSON, no tx)
    #   "quote_rate_limited"     HTTP 429
    #   "quote_insufficient_funds" taker lacks balance to assemble the order
    #   "quote_exception"        unexpected error while quoting

    router: str = ""  # "metis" | "jupiterz" | "dflow" | "okx" (route diagnostics)
    mode: str = ""  # "ultra" (RTSE, all routers) | "manual" (optional params set)
    slippage_bps: int | None = None  # applied slippage (RTSE-chosen when omitted)

    @property
    def retryable(self) -> bool:
        """True when a retry could plausibly succeed (route not ready yet)."""
        return self.reason in (
            "quote_no_route",
            "quote_timeout",
            "quote_http_error",
            "quote_rate_limited",
            "quote_exception",
        )


class JupiterSwap:
    """Async wrapper around the managed /order + /execute swap path.

    Args:
        dry_run: Paper mode (default True). Derives a throwaway keypair for
            the quote gate, omits ``taker``, and never signs or executes.
        private_key: Base58 wallet key for live mode. Required when
            ``dry_run=False`` (fail-fast).
        base_url: Jupiter API base (default https://api.jup.ag).
        slippage_bps: Default sell slippage in basis points.
        max_price_impact_pct: Quote-gate cap on price impact (percent).
        retries: Max attempts for a quote-gate request.
    """

    def __init__(
        self,
        dry_run: bool = True,
        private_key: str = "",
        base_url: str = "https://api.jup.ag",
        slippage_bps: int = 300,
        max_price_impact_pct: float = 5.0,
        retries: int = 3,
    ) -> None:
        env = config.load_env()
        self._base = config.get(env, "JUPITER_BASE_URL", base_url)
        self._headers = {"accept": "application/json"}
        api_key = config.get(env, "JUPITER_API_KEY")
        if api_key:
            self._headers["x-api-key"] = api_key
        self._slippage_bps = int(config.get(env, "JUPITER_SLIPPAGE_BPS", slippage_bps))
        # RTSE (Real-Time Slippage Estimator): omit slippageBps on BUY /order
        # so Jupiter applies optimized slippage in ultra mode with all routers
        # eligible. Verified against Jupiter's routing-impact matrix: any
        # optional param (incl. slippageBps) flips /order to "manual" mode,
        # which may restrict routing. Sells keep explicit slippage because
        # execution certainty dominates on exits.
        self._buy_rtse = config.get_bool(env, "JUPITER_ORDER_RTSE", True)
        self._max_impact = float(
            config.get(env, "JUPITER_MAX_IMPACT_PCT", max_price_impact_pct)
        )
        self._retries = int(config.get(env, "JUPITER_QUOTE_RETRIES", retries))
        # Two-sided execution + stability gate (CATE/ELON defense).
        # These are read here so JupiterSwap can be used standalone;
        # PaperTrader also reads them via Settings for single-source config.
        self.require_sell_quote = config.get_bool(env, "REQUIRE_SELL_QUOTE", True)
        self.max_sell_impact_pct = float(
            config.get(env, "MAX_SELL_IMPACT_PCT", 5.0)
        )
        self.quote_stability_checks = int(
            config.get(env, "QUOTE_STABILITY_CHECKS", 3)
        )
        self.quote_stability_interval_ms = int(
            config.get(env, "QUOTE_STABILITY_INTERVAL_MS", 300)
        )
        self.max_quote_change_pct = float(
            config.get(env, "MAX_QUOTE_CHANGE_PCT", 10.0)
        )
        self.max_impact_change_pct = float(
            config.get(env, "MAX_IMPACT_CHANGE_PCT", 5.0)
        )
        self._quote_cache_s = config.get_float(env, "JUPITER_QUOTE_CACHE_S", 30.0)
        self._quote_throttle_s = config.get_float(env, "JUPITER_QUOTE_THROTTLE_S", 1.0)
        self._quote_retry_delay_s = config.get_float(env, "JUPITER_QUOTE_RETRY_DELAY_S", 1.0)
        self._sell_slippage_escalation = config.get_csv_ints(
            env, "SELL_SLIPPAGE_ESCALATION", SELL_SLIPPAGE_ESCALATION
        )
        # RPC used for real-wallet reads (getBalance, token decimals). Only
        # ever queried in live mode — never in paper/dry-run.
        self._rpc_keys: list[str] = [
            k.strip()
            for k in config.get(
                env, "HELIUS_API_KEYS", config.get(env, "HELIUS_API_KEY", "")
            ).split(",")
            if k.strip()
        ]
        self._order_timeout_s = config.get_float(env, "JUPITER_ORDER_TIMEOUT_S",
                                                 _DEFAULT_ORDER_TIMEOUT_S)
        self._execute_timeout_s = config.get_float(env, "JUPITER_EXECUTE_TIMEOUT_S",
                                                   _DEFAULT_EXECUTE_TIMEOUT_S)
        self._rpc_timeout_s = config.get_float(env, "RPC_TIMEOUT_S",
                                               _DEFAULT_RPC_TIMEOUT_S)
        self._quote_cache_max = int(config.get(env, "JUPITER_QUOTE_CACHE_MAX",
                                                _DEFAULT_QUOTE_CACHE_MAX))

        # Local banned token list (fallback when Jupiter DNS fails)
        from src.banned_tokens import LocalBanList
        ban_file = config.get(env, "BANNED_TOKENS_FILE", "banned_tokens.json")
        ban_ttl = float(config.get(env, "BANNED_TOKENS_TTL_S", 604800))  # 7 days
        self._local_ban_list = LocalBanList(ban_file=ban_file, ttl_s=ban_ttl)

        key = private_key or config.get(env, "PRIVATE_KEY")
        self._keypair: Keypair | None = None
        self._private_key: str = ""  # raw base58 for PumpAPI
        if dry_run:
            # Paper mode NEVER executes — even when a PRIVATE_KEY is configured.
            # DRY_RUN must win over the key: previously any present key forced
            # live=True, silently turning a "paper" run into real trading.
            self.live = False
        elif key:
            self._private_key = key.strip()
            self._keypair = Keypair.from_base58_string(self._private_key)
            self.live = True
        else:
            raise JupiterError(
                "DRY_RUN=false requires PRIVATE_KEY in .env — refusing to start "
                "live trading without a wallet key"
            )

        # Paper quoting: omit the taker so /order returns the quote with no
        # transaction (and never fails with "Insufficient funds"). Live mode
        # passes the real taker pubkey so Jupiter assembles an executable tx.
        self._paper_quoting = not self.live

        # Paper-mode simulated SOL balance — tracks buys/sells in paper mode
        # so notifications show realistic before/after balance.
        if not self.live:
            self._paper_balance_sol: float = float(config.get(
                env, "START_BALANCE_SOL", "2.0",
            ))
        else:
            self._paper_balance_sol = 0.0

        self._client = httpx.AsyncClient(timeout=20.0)

        # Resilient RPC client — circuit breaking, provider rotation, 429 retry
        self._rpc_client: ResilientRPCClient | None = None
        if self._rpc_keys:
            providers = []
            base_rpc = (
                config.get(env, "GATEKEEPER_RPC_URL")
                or config.get(env, "SOLANA_RPC_URL")
                or config.get(env, "HELIUS_BASE_URL", "https://mainnet.helius-rpc.com")
            )
            for i, k in enumerate(self._rpc_keys):
                url = f"{base_rpc}/?api-key={k}" if "api-key=" not in base_rpc else base_rpc
                providers.append({
                    "name": f"helius_{i}",
                    "url": url,
                    "weight": len(self._rpc_keys) - i,  # newest key gets highest weight
                    "tier": "paid",
                })
            # Add public endpoint as last resort
            providers.append({
                "name": "public",
                "url": "https://api.mainnet-beta.solana.com",
                "weight": 1,
                "tier": "free",
            })
            self._rpc_client = ResilientRPCClient(
                providers,
                rate_limit=10.0,
                burst=20.0,
                failure_threshold=5,
                recovery_seconds=60.0,
            )

        # -- quote gate state -------------------------------------------------
        self._quote_lock = asyncio.Lock()
        self._next_quote_ts: float = 0.0
        self._quote_cache: dict = {}  # (mint, amount) -> (ts, result)
        self._qstats: dict[str, int] = {
            "quotes": 0,
            "ok": 0,
            "quote_no_route": 0,
            "quote_impact": 0,
            "quote_timeout": 0,
            "quote_http_error": 0,
            "quote_invalid_response": 0,
            "quote_rate_limited": 0,
            "quote_insufficient_funds": 0,
            "quote_exception": 0,
        }

        # Token safety — Jupiter's banned token list for pre-trade scam checks
        self._token_client: TokenClient | None = None
        self._token_client_connected = False
        if config.get_bool(env, "JUPITER_TOKEN_CHECK_ENABLED", True):
            try:
                jup_api_key = config.get(env, "JUPITER_API_KEY", "")
                self._token_client = TokenClient(api_key=jup_api_key, cache_ttl=300.0)
            except Exception:  # noqa: BLE001
                log.debug("TokenClient init skipped")
        else:
            log.debug("TokenClient disabled by JUPITER_TOKEN_CHECK_ENABLED=false")
        self._lat_sum = 0.0
        self._lat_count = 0
        self._lat_max = 0.0
        self._lat_samples: deque = deque(maxlen=_LATENCY_SAMPLES_MAX)
        # Last known live wallet SOL balance (updated by balance_sol()). Used
        # for the pre-flight insufficient-funds check in _do_quote so a 0.025
        # SOL wallet does not waste 3×1s retries on a 0.02 SOL quote that
        # Jupiter will reject as generic 400 "Failed to get quotes".
        self._live_balance_sol: float | None = None

    async def close(self) -> None:
        """Release the underlying HTTP client."""
        await self._client.aclose()
        if self._rpc_client:
            await self._rpc_client.shutdown()
        if self._token_client and self._token_client_connected:
            try:
                await self._token_client.close()
            except Exception:  # noqa: BLE001
                pass

    async def is_token_banned(self, mint: str) -> bool:
        """Check if a token is on Jupiter's banned list (known scam).

        DNS/connect failures are transient — retry once with a short delay
        before disabling the client.  A single DNS blip should not permanently
        disable the scam check for the rest of the session.

        Also checks the local banned token list as a fallback.
        """
        # Check local ban list first (always available)
        if self._local_ban_list and self._local_ban_list.is_banned(mint):
            return True

        if not self._token_client:
            return False
        for attempt in range(2):
            try:
                if not self._token_client_connected:
                    await asyncio.wait_for(
                        self._token_client.connect(), timeout=5.0)
                    self._token_client_connected = True
                return await self._token_client.is_banned(mint)
            except Exception as exc:  # noqa: BLE001
                err_str = str(exc).lower()
                is_dns = any(kw in err_str for kw in ("name or service", "dns", "resolve", "getaddrinfo"))
                if is_dns and attempt == 0:
                    # DNS blip — wait briefly and retry once
                    await asyncio.sleep(1.0)
                    continue
                # Non-DNS or second failure — disable client to avoid spam
                if not self._token_client_connected:
                    log.debug("TokenClient disabled after connect failure: %s", exc)
                    self._token_client = None
                return False
        return False

    async def token_audit(self, mint: str) -> dict[str, Any]:
        """Fetch token audit data from Jupiter's /tokens/v2/search endpoint.

        Returns a dict with safety fields:
          - mint_authority_disabled: bool (safe if True)
          - freeze_authority_disabled: bool (safe if True)
          - is_sus: bool (True if flagged as suspicious)
          - top_holders_pct: float (0-100, lower is safer)
          - dev_balance_pct: float (0-100, lower is safer)
          - dev_mints: int (number of developer mint events)
          - organic_score: int (0-100, higher is safer; RELATIVE not
            absolute per Jupiter docs — prefer flow fields below)
          - organic_score_label: str ("high"/"medium"/"low")
          - holder_count: int
          - organic_buyers_5m: int (numOrganicBuyers 5m — genuine buyers)
          - organic_buy_vol_5m: float (buyOrganicVolume 5m USD)
          - organic_sell_vol_5m: float (sellOrganicVolume 5m USD)
          - net_buyers_5m: int (numNetBuyers 5m)
          - traders_5m: int (numTraders 5m)
          - available: bool (True if API responded successfully)

        Fail-open: returns available=False on any error so callers never block.
        Cost: 10 credits per call.
        """
        result: dict[str, Any] = {"available": False}
        try:
            r = await asyncio.wait_for(
                self._client.get(
                    f"{self._base}/tokens/v2/search",
                    params={"query": mint},
                    headers=self._headers,
                ),
                timeout=8.0,
            )
            if r.status_code != 200:
                log.debug("jupiter token audit HTTP %s for %s", r.status_code, mint[:8])
                return result
            data = r.json()
            items = data if isinstance(data, list) else data.get("items", [])
            if not items:
                return result
            item = items[0]
            audit = item.get("audit") or {}
            s5 = item.get("stats5m") or {}
            def _fi(v: Any) -> int:
                try:
                    return int(v or 0)
                except (TypeError, ValueError):
                    return 0
            def _ff(v: Any) -> float:
                try:
                    return float(v or 0)
                except (TypeError, ValueError):
                    return 0.0
            result.update({
                "available": True,
                "mint_authority_disabled": audit.get("mintAuthorityDisabled", True),
                "freeze_authority_disabled": audit.get("freezeAuthorityDisabled", True),
                "is_sus": "isSus" in audit and bool(audit["isSus"]),
                "top_holders_pct": float(audit.get("topHoldersPercentage") or 0),
                "dev_balance_pct": float(audit.get("devBalancePercentage") or 0),
                "dev_mints": int(audit.get("devMints") or 0),
                "organic_score": int(item.get("organicScore") or 0),
                "organic_score_label": str(item.get("organicScoreLabel") or ""),
                "holder_count": int(item.get("holderCount") or 0),
                "is_verified": bool(item.get("isVerified")),
                "organic_buyers_5m": _fi(s5.get("numOrganicBuyers")),
                "organic_buy_vol_5m": _ff(s5.get("buyOrganicVolume")),
                "organic_sell_vol_5m": _ff(s5.get("sellOrganicVolume")),
                "net_buyers_5m": _fi(s5.get("numNetBuyers")),
                "traders_5m": _fi(s5.get("numTraders")),
            })
        except asyncio.TimeoutError:
            log.debug("jupiter token audit timed out %s", mint[:8])
        except Exception as exc:  # noqa: BLE001
            log.debug("jupiter token audit failed %s: %s", mint[:8], exc)
        return result

    @property
    def ready(self) -> bool:
        """True when we can quote/sign (paper mode uses a throwaway keypair)."""
        return self._keypair is not None or self._paper_quoting

    @property
    def wallet_pubkey(self) -> str | None:
        """The live wallet's public key (None in paper mode)."""
        return str(self._keypair.pubkey()) if self._keypair is not None else None


    async def _rpc(self, method: str, params: list) -> Any:
        """POST a JSON-RPC call via the resilient RPC client.

        Uses solana-rpc-resilient for automatic provider rotation, circuit
        breaking, and 429 retry. Falls back to direct httpx if the resilient
        client is unavailable.
        """
        if self._rpc_client is None:
            raise JupiterError("no RPC providers configured")
        # Lazy-start the resilient client
        if self._rpc_client._http is None:
            await self._rpc_client.startup()
        result = await self._rpc_client.rpc_request(method, params)
        if result.is_ok:
            return result.unwrap()
        err = result.unwrap_err()
        raise JupiterError(f"rpc {method}: [{err.code}] {err.message}")

    async def balance_sol(self) -> float | None:
        """Return the wallet's SOL balance (live) or paper balance (paper mode)."""
        if self._keypair is None:
            return self._paper_balance_sol
        try:
            result = await self._rpc("getBalance", [str(self._keypair.pubkey())])
            bal = float(result.get("value", 0)) / 1e9
            self._live_balance_sol = bal
            return bal
        except Exception as e:  # noqa: BLE001
            log.warning("balance_sol failed: %s", e)
            return None

    def paper_deduct(self, sol: float) -> None:
        """Deduct from paper balance on simulated buy."""
        if not self.live:
            self._paper_balance_sol -= sol

    def paper_credit(self, sol: float) -> None:
        """Credit paper balance on simulated sell."""
        if not self.live:
            self._paper_balance_sol += sol

    async def token_decimals(self, mint: str) -> int | None:
        """Return the token's decimal count via RPC (live entry pricing).

        Uses ``getAccountInfo`` with jsonParsed: ``getParsedAccountInfo`` is
        not implemented on Helius RPC (returns ``-32601 Method not found`` on
        both Gatekeeper beta and mainnet), while ``getAccountInfo`` returns the
        same ``data.parsed.info.decimals`` shape.
        """
        try:
            result = await self._rpc(
                "getAccountInfo", [mint, {"encoding": "jsonParsed"}]
            )
            info = (result or {}).get("value")
            if info is not None:
                data = info.get("data", {})
                if isinstance(data, dict):
                    parsed = data.get("parsed", {})
                    return int(parsed.get("info", {}).get("decimals"))
        except Exception as e:  # noqa: BLE001
            log.warning("token_decimals %s failed: %s", mint, e)
        return None

    async def token_balance(self, mint: str) -> int | None:
        """Return the wallet's raw balance of ``mint`` via RPC.

        Used to reconcile token amounts after a restart, so restored open
        positions never sell a wrong (or zero) amount. Returns None on any
        failure so callers can fall back to the persisted checkpoint value.
        """
        if self._keypair is None:
            return None
        try:
            result = await self._rpc(
                "getTokenAccountsByOwner",
                [
                    str(self._keypair.pubkey()),
                    {"mint": mint},
                    {"encoding": "jsonParsed"},
                ],
            )
            accounts = (result or {}).get("value") or []
            for acct in accounts:
                parsed = (acct.get("account", {}).get("data") or {}).get("parsed") or {}
                raw = parsed.get("info", {}).get("tokenAmount", {}).get("amount")
                if raw is not None:
                    return int(raw)
        except Exception as e:  # noqa: BLE001
            log.warning("token_balance %s failed: %s", mint, e)
        return None

    async def paper_sell_proceeds(self, mint: str, amount_raw: int, slippage_bps: int) -> int | None:
        """Simulated sell proceeds (raw SOL) for paper mode — no signing.

        Mirrors :meth:`sell`'s first attempt: quotes a token→SOL swap at
        ``slippage_bps`` and returns the expected raw out amount. This is what
        a live sell would net before execution, so paper exits reflect real
        sell slippage instead of filling at the raw feed tick. Returns None on
        any failure so the trader keeps the position open (mirroring live) —
        no fabricated tick-based fill.
        """
        if self.live:
            return None
        try:
            await self._quote_slot()
            order = await self._order(mint, BASE_MINT, amount_raw, slippage_bps)
            return int(order.get("outAmount") or order.get("actualOutAmount") or 0)
        except Exception as e:  # noqa: BLE001
            log.warning("paper_sell_proceeds %s failed: %s", mint, e)
            return None

    def quote_summary(self) -> str:
        """One-line quote-gate + latency summary (avg/max/p50/p95)."""
        q = self._qstats
        avg = self._lat_sum / self._lat_count if self._lat_count else 0.0
        p50, p95 = self._latency_percentiles()
        return (
            f"quotes quotes={q['quotes']} ok={q['ok']} "
            f"no_route={q['quote_no_route']} impact={q['quote_impact']} "
            f"timeout={q['quote_timeout']} http={q['quote_http_error']} "
            f"invalid={q['quote_invalid_response']} "
            f"no_funds={q['quote_insufficient_funds']} "
            f"rate_limit={q['quote_rate_limited']} exc={q['quote_exception']} "
            f"latency avg={avg:.0f}ms max={self._lat_max:.0f}ms "
            f"p50={p50:.0f}ms p95={p95:.0f}ms"
        )

    def _latency_percentiles(self) -> tuple[float, float]:
        """p50/p95 of recent quote latencies (0,0 when no samples yet)."""
        if not self._lat_samples:
            return 0.0, 0.0
        samples = sorted(self._lat_samples)
        n = len(samples)
        p50 = samples[(n - 1) * 50 // 100]
        p95 = samples[(n - 1) * 95 // 100]
        return float(p50), float(p95)

    # ------------------------------------------------------------------- order
    async def _order(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int | None,
        taker: str | None = None,
        _retry_transient: bool = False,
    ) -> dict:
        """Request a swap quote (and, with a taker, an assembled transaction).

        Args:
            slippage_bps: Explicit slippage cap in bps, or ``None`` to omit
                the parameter entirely. On ``/order`` an omitted slippage
                enables Jupiter's RTSE (Real-Time Slippage Estimator) in
                **ultra** mode with ALL routers eligible (Metis / JupiterZ
                RFQ / Dflow / OKX); passing any optional param such as
                ``slippageBps`` flips the response to **manual** mode which
                may restrict routing (per Jupiter's routing-impact matrix).
                Buys default to RTSE (``JUPITER_ORDER_RTSE=true``); sells keep
                explicit slippage because execution certainty dominates.

        Live mode passes ``taker`` so Jupiter builds an executable transaction.
        Paper mode deliberately omits it: a throwaway taker would make Jupiter
        fail every paper quote with "Insufficient funds" (the paper wallet
        holds no SOL/tokens), which is not the question paper asks. Paper asks
        "does a tradable route exist right now and what would it net" — the
        taker-less quote answers exactly that, and a dead pool fails it with
        the same "Failed to get quotes" as live.

        When ``_retry_transient=True``, transient 5xx/timeout errors are
        retried up to 2 times with exponential backoff (1s, 2s) before
        raising — prevents a single Jupiter blip from aborting a sell.
        """
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
        }
        if slippage_bps is not None:
            params["slippageBps"] = str(slippage_bps)
        if taker is not None:
            params["taker"] = str(taker)

        max_attempts = 3 if _retry_transient else 1
        last_exc: JupiterError | None = None
        for attempt in range(max_attempts):
            async def _get() -> httpx.Response:
                return await self._client.get(
                    f"{self._base}/swap/v2/order", params=params, headers=self._headers
                )

            try:
                resp = await asyncio.wait_for(_get(), timeout=self._order_timeout_s)
            except (TimeoutError, httpx.TimeoutException) as exc:
                jexc = JupiterError(
                    f"order timed out after {self._order_timeout_s:.0f}s: {exc}",
                    status=0,
                )
                if _retry_transient and attempt < max_attempts - 1:
                    last_exc = jexc
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
                raise jexc from exc
            if resp.status_code == 503 or (500 <= resp.status_code < 600):
                if _retry_transient and attempt < max_attempts - 1:
                    last_exc = JupiterError(
                        f"order HTTP {resp.status_code}: {resp.text[:200]}",
                        status=resp.status_code,
                    )
                    await asyncio.sleep(1.0 * (attempt + 1))
                    continue
            if resp.status_code != 200:
                raise JupiterError(
                    f"order HTTP {resp.status_code}: {resp.text[:200]}",
                    status=resp.status_code,
                )
            try:
                data = resp.json()
            except ValueError as exc:
                raise JupiterError(
                    f"order invalid JSON: {resp.text[:200]}", status=200
                ) from exc
            transaction = data.get("transaction")
            if taker is not None and not transaction:
                raise JupiterError(
                    f"order failed: {data.get('errorMessage') or data.get('error') or data}",
                    status=200,
                )
            return data
        # All retries exhausted
        raise last_exc or JupiterError("order: all retries exhausted", status=0)

    # ---------------------------------------------------------------- signing
    def _sign(self, b64_transaction: str) -> str:
        """Sign a base64 transaction with the wallet keypair; return base64.

        Jupiter v0 transactions are signed over the versioned message bytes —
        a leading 0x80 version byte followed by the serialized MessageV0
        (``bytes(tx.message)``). Signing the bare message yields a signature
        that fails /execute with ``Invalid signature`` (verified offline:
        ``verify_with_results``). The taker's signature is placed in its slot
        while any other pre-existing signatures (JupiterZ market-maker slot)
        are preserved, matching @solana/kit's ``partiallySignTransaction``.
        """
        if self._keypair is None:
            raise JupiterError("no wallet key configured; cannot sign (dry-run?)")
        raw = base64.b64decode(b64_transaction)
        tx = VersionedTransaction.from_bytes(raw)
        # Sign the versioned message: [0x80][serialized MessageV0]. bytes() on
        # the message gives the bare MessageV0; the 0x80 header marks the
        # transaction as versioned and MUST be part of the signed payload.
        versioned_message = b"\x80" + bytes(tx.message)
        signature = self._keypair.sign_message(versioned_message)

        # Partial sign: keep existing slots (e.g. the JupiterZ market-maker
        # signature) and only fill the taker's slot so /execute sees every
        # required signer. Signers are the first num_required_signatures keys.
        sigs = list(tx.signatures)
        required = tx.message.header.num_required_signatures
        signer_keys = tx.message.account_keys[:required]
        try:
            slot = signer_keys.index(self._keypair.pubkey())
        except ValueError:
            raise JupiterError("wallet keypair is not a signer of this transaction")
        sigs[slot] = signature
        signed = VersionedTransaction.populate(tx.message, sigs)
        return base64.b64encode(bytes(signed)).decode()

    # ----------------------------------------------------------- simulate
    async def simulate_transaction(self, b64_transaction: str) -> dict:
        """Simulate a signed transaction via Shyft RPC before execution.

        Returns ``{"ok": True, "units": N, "logs": [...]}`` on success or
        ``{"ok": False, "reason": "...", "logs": [...]}`` on failure.
        Uses SIMULATE_PRIVATE_KEY in paper mode for signing.
        """
        # Use real keypair in live mode, throwaway keypair in paper mode
        sim_keypair = self._keypair
        if sim_keypair is None:
            sim_pk = config.get(config.load_env(), "SIMULATE_PRIVATE_KEY", "")
            if sim_pk:
                try:
                    sim_keypair = Keypair.from_base58_string(sim_pk.strip())
                except Exception:
                    return {"ok": False, "reason": "bad_simulate_key"}
            else:
                return {"ok": False, "reason": "paper_mode_no_sim_key"}
        try:
            raw = base64.b64decode(b64_transaction)
            tx = VersionedTransaction.from_bytes(raw)
            versioned_message = b"\x80" + bytes(tx.message)
            signature = sim_keypair.sign_message(versioned_message)
            sigs = list(tx.signatures)
            required = tx.message.header.num_required_signatures
            signer_keys = tx.message.account_keys[:required]
            try:
                slot = signer_keys.index(sim_keypair.pubkey())
            except ValueError:
                slot = 0  # use first slot if keypair not in signers
            sigs[slot] = signature
            signed = VersionedTransaction.populate(tx.message, sigs)
            signed_b64 = base64.b64encode(bytes(signed)).decode()
        except Exception as e:
            return {"ok": False, "reason": f"sign_failed:{e}"}

        shyft_key = config.get(config.load_env(), "SHYFT_API_KEY", "")
        if not shyft_key:
            return {"ok": False, "reason": "no_shyft_key"}

        rpc_url = f"https://rpc.shyft.to?api_key={shyft_key}"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "simulateTransaction",
            "params": [
                signed_b64,
                {
                    "encoding": "base64",
                    "replaceRecentBlockhash": True,
                    "sigVerify": False,
                },
            ],
        }
        try:
            resp = await asyncio.wait_for(
                self._client.post(rpc_url, json=payload),
                timeout=self._rpc_timeout_s,
            )
            data = resp.json()
            result = data.get("result", {}).get("value", {})
            err = result.get("err")
            logs_list = result.get("logs") or []
            units = result.get("unitsConsumed") or 0
            if err:
                return {"ok": False, "reason": f"sim_error:{err}", "logs": logs_list, "units": units}
            return {"ok": True, "units": units, "logs": logs_list}
        except Exception as e:
            return {"ok": False, "reason": f"sim_rpc_error:{e}"}

    # ----------------------------------------------------------------- execute
    async def execute(self, order: dict) -> SwapResult:
        """POST the signed transaction to /execute managed landing.

        On timeout the server may have received and landed the tx before we
        finished reading.  ``sell()`` uses the returned signature to reconcile
        via RPC before retrying, preventing double-sells.
        """
        signed = self._sign(order["transaction"])
        body = {
            "signedTransaction": signed,
            "requestId": order.get("requestId", ""),
        }

        async def _post() -> httpx.Response:
            return await self._client.post(
                f"{self._base}/swap/v2/execute", json=body, headers=self._headers
            )

        try:
            resp = await asyncio.wait_for(_post(), timeout=self._execute_timeout_s)
        except (TimeoutError, httpx.TimeoutException):
            # The server may have received and landed the tx before we timed
            # out reading.  Retry once with a short timeout to grab the
            # response; if that also fails, return with no signature so the
            # caller can fall back to an RPC check.
            try:
                resp = await asyncio.wait_for(_post(), timeout=5.0)
            except Exception:
                return SwapResult(
                    success=False,
                    signature="",
                    input_amount=0,
                    output_amount=0,
                    error=f"execute timed out after {self._execute_timeout_s:.0f}s"
                          " (tx may have landed — check manually)",
                )
            data = resp.json() if resp.content else {}
            if resp.status_code == 200 and data.get("status") == "Success":
                return SwapResult(
                    success=True,
                    signature=data.get("signature", ""),
                    input_amount=int(data.get("totalInputAmount") or 0),
                    output_amount=int(data.get("totalOutputAmount") or 0),
                )
            sig = data.get("signature", "")
            return SwapResult(
                success=False,
                signature=sig,
                input_amount=int(data.get("totalInputAmount") or 0),
                output_amount=int(data.get("totalOutputAmount") or 0),
                error=data.get("error")
                      or f"execute timeout+retry failed (sig={sig or 'none'})",
            )
        data = resp.json() if resp.content else {}
        if resp.status_code != 200 or data.get("status") != "Success":
            # Parse Jupiter error code for classification
            error_code = None
            error_msg = data.get("error") or f"execute HTTP {resp.status_code}"
            # Jupiter wraps errors in {"error": "...", "code": N}
            if isinstance(data.get("error"), dict):
                error_code = data["error"].get("code")
                error_msg = data["error"].get("message") or error_msg
            elif "code" in data:
                error_code = data.get("code")
            return SwapResult(
                success=False,
                signature=data.get("signature", ""),
                input_amount=int(data.get("totalInputAmount") or 0),
                output_amount=int(data.get("totalOutputAmount") or 0),
                error=error_msg,
                error_code=error_code,
            )
        return SwapResult(
            success=True,
            signature=data.get("signature", ""),
            input_amount=int(data.get("totalInputAmount") or 0),
            output_amount=int(data.get("totalOutputAmount") or 0),
        )

    async def check_tx_status(self, signature: str) -> str | None:
        """Check if a transaction landed on Solana via resilient RPC.

        Returns the confirmation status string ('processed', 'confirmed',
        'finalized') or 'error' if the tx failed on-chain, or None on
        network failure.  Used by sell() to reconcile execute-timeout before
        retrying — prevents double-sells.
        """
        if not signature or not self._rpc_client:
            return None
        try:
            if self._rpc_client._http is None:
                await self._rpc_client.startup()
            result = await self._rpc_client.get_signature_statuses(
                [signature], search_transaction_history=True,
            )
            if result.is_ok:
                statuses = result.unwrap() or []
                if statuses:
                    s = statuses[0]
                    if s is None:
                        return None
                    err = s.get("err")
                    if err is not None:
                        return "error"
                    return s.get("confirmationStatus")
        except Exception:
            log.debug("tx status check failed for %s", signature[:12])
        return None

    # ------------------------------------------------------------------- quote
    async def _quote_slot(self, min_spacing: float | None = None) -> None:
        """Throttle quote requests (Jupiter free tier).

        Args:
            min_spacing: Override the global spacing for this request. The
                stability burst passes ``QUOTE_STABILITY_INTERVAL_MS`` here so
                its samples run at the configured cadence (~300ms) instead of
                the 1s global throttle — a genuine short stability window
                rather than a multi-second stall on a fresh launch.
        """
        spacing = self._quote_throttle_s if min_spacing is None else min_spacing
        async with self._quote_lock:
            now = time.monotonic()
            wait = self._next_quote_ts - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_quote_ts = now + spacing

    def _record_latency(self, ms: float) -> None:
        self._lat_sum += ms
        self._lat_count += 1
        self._lat_max = max(self._lat_max, ms)
        self._lat_samples.append(ms)

    def _classify_error(self, exc: JupiterError) -> str:
        """Map a Jupiter order failure to a skip-taxonomy reason."""
        st = exc.status
        msg = str(exc).lower()
        if "insufficient" in msg:
            return "quote_insufficient_funds"
        if st == 429:
            return "quote_rate_limited"
        if st and 500 <= st < 600:
            return "quote_http_error"
        if st and 400 <= st < 500:
            # Keep explicit insufficient handling above; generic 400 without
            # "insufficient" and without balance proof is conservatively a route
            # miss (rugged pool) not a wallet issue — paper quote would also fail
            # if it were truly unroutable, but paper succeeds for these mints.
            # The insufficient case is handled at call-site via amount vs balance.
            return (
                "quote_no_route" if "route" in msg or "failed to get quotes" in msg else "quote_http_error"
            )
        if "route" in msg:
            return "quote_no_route"
        return "quote_invalid_response"

    async def _do_quote(
        self,
        mint: str,
        amount_raw: int,
        slippage_bps: int,
        min_spacing: float | None = None,
    ) -> QuoteResult:
        """Fetch one order and validate it against the quote-gate rules.

        Args:
            min_spacing: Optional throttle override (see :meth:`_quote_slot`);
                ``None`` uses the global ``JUPITER_QUOTE_THROTTLE_S``.
        """
        self._qstats["quotes"] += 1
        # Pre-flight insufficient-funds check (live only). Wallet 0.025 SOL
        # + size 0.02 SOL leaves <2M after ATA rent 4.07M (payer+spl) + fees,
        # so Jupiter returns generic 400 "Failed to get quotes" that looks like
        # no_route. Fail fast with quote_insufficient_funds (non-retryable)
        # when the amount clearly exceeds the last known live balance.
        # Rent is 4_078_560 (ATA + WSOL), plus 0.005 SOL dust buffer per
        # Helius docs. Using 5M buffer catches 0.02 on 0.025 wallet while
        # leaving 0.01 (10M+5M=15M<25M) to pass for healthy pools.
        if not self._paper_quoting and self._live_balance_sol is not None:
            # Buffer: ATA 2_039_280*2 + fee 10bps + sig/prio ~10k + dust 2M
            buffer_lamports = 6_000_000
            try:
                bal_lamports = int(self._live_balance_sol * 1e9)
                if amount_raw + buffer_lamports > bal_lamports:
                    reason = "quote_insufficient_funds"
                    self._qstats[reason] += 1
                    log.warning(
                        "quote %s for %s: amount %d + buffer %d > balance %d "
                        "(%.4f SOL) — skipping Jupiter call",
                        reason, mint, amount_raw, buffer_lamports,
                        bal_lamports, self._live_balance_sol,
                    )
                    return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, reason)
            except Exception:
                pass  # balance check is best-effort; fall through to Jupiter
        await self._quote_slot(min_spacing)
        t0 = time.monotonic()
        try:
            # RTSE buy: omit slippageBps -> ultra mode, all routers eligible.
            buy_slip = None if self._buy_rtse else self._slippage_bps
            if self._paper_quoting:
                order = await self._order(BASE_MINT, mint, amount_raw, buy_slip)
            else:
                order = await self._order(
                    BASE_MINT,
                    mint,
                    amount_raw,
                    buy_slip,
                    str(self._keypair.pubkey()),
                )
        except httpx.TimeoutException as exc:
            reason = "quote_timeout"
            self._qstats[reason] += 1
            log.warning("quote timeout for %s: %s", mint, exc)
            return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, reason)
        except JupiterError as exc:
            reason = self._classify_error(exc)
            self._qstats[reason] += 1
            log.info("quote %s for %s: %s", reason, mint, exc)
            return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, reason)
        except httpx.RequestError as exc:
            self._qstats["quote_http_error"] += 1
            log.warning("quote http error for %s: %s", mint, exc)
            return QuoteResult(
                False, None, amount_raw, 0, 0.0, 0, 0.0, "quote_http_error"
            )
        except Exception:
            self._qstats["quote_exception"] += 1
            log.exception("quote exception for %s", mint)
            return QuoteResult(
                False, None, amount_raw, 0, 0.0, 0, 0.0, "quote_exception"
            )

        latency_ms = (time.monotonic() - t0) * 1000
        self._record_latency(latency_ms)
        fetched_at = time.monotonic()

        out = int(order.get("outAmount") or order.get("actualOutAmount") or 0)
        impact = abs(float(order.get("priceImpact") or 0.0))
        if impact == 0.0 and order.get("priceImpactPct"):
            impact = abs(float(order["priceImpactPct"])) * 100.0
        router = str(order.get("router") or "")
        mode = str(order.get("mode") or "")
        try:
            slip_bps: int | None = (
                int(order["slippageBps"]) if order.get("slippageBps") is not None else None
            )
        except (TypeError, ValueError):
            slip_bps = None
        route = order.get("routePlan") or order.get("routes") or []
        if out <= 0 or not route:
            self._qstats["quote_no_route"] += 1
            return QuoteResult(
                False, order, amount_raw, out, impact, len(route),
                latency_ms, "quote_no_route", fetched_at,
                router=router, mode=mode, slippage_bps=slip_bps,
            )
        if impact > self._max_impact:
            self._qstats["quote_impact"] += 1
            return QuoteResult(
                False, order, amount_raw, out, impact, len(route),
                latency_ms, "quote_impact", fetched_at,
                router=router, mode=mode, slippage_bps=slip_bps,
            )
        self._qstats["ok"] += 1
        return QuoteResult(
            True, order, amount_raw, out, impact, len(route),
            latency_ms, "ok", fetched_at,
            router=router, mode=mode, slippage_bps=slip_bps,
        )

    async def quote(
        self,
        mint: str,
        amount_raw: int,
        force: bool = False,
    ) -> QuoteResult | None:
        """Verify tradability for ``mint`` and return a ready-to-execute order.

        Chooses slippage from ``liquidity_usd`` tiers, retries "no route"
        briefly (new launches race their liquidity), and caches the result
        briefly to collapse simultaneous evaluations of the same token.
        ``force=True`` bypasses the cache (used when a cached quote is stale).
        """
        # Pre-flight: reject Jupiter-banned tokens (known scams)
        if await self.is_token_banned(mint):
            log.info("quote %s rejected: Jupiter banned token", mint[:8])
            return None

        slippage = self._slippage_bps
        key = (mint, amount_raw, slippage)

        if not force:
            now = time.monotonic()
            cached = self._quote_cache.get(key)
            if cached and now - cached[0] < self._quote_cache_s:
                return cached[1]

        result: QuoteResult | None = None
        for attempt in range(max(self._retries, 1)):
            result = await self._do_quote(mint, amount_raw, slippage)
            if result.success or not result.retryable:
                break
            log.info("quote %s for %s (attempt %d)", result.reason, mint, attempt + 1)
            if attempt + 1 < self._retries:
                await asyncio.sleep(self._quote_retry_delay_s)

        if result is not None:
            self._quote_cache[key] = (time.monotonic(), result)
            # Bound the cache: prune entries older than the TTL, then drop the
            # oldest survivors if still over capacity. Without this the cache
            # grows forever with every unique (mint, amount) evaluated, which
            # contributed to the OOM kills seen in live operation.
            if len(self._quote_cache) > self._quote_cache_max:
                cutoff = time.monotonic() - self._quote_cache_s
                stale = [k for k, (ts, _) in self._quote_cache.items() if ts < cutoff]
                for k in stale:
                    self._quote_cache.pop(k, None)
            if len(self._quote_cache) > self._quote_cache_max:
                for k in list(self._quote_cache)[: max(len(self._quote_cache) - self._quote_cache_max, 0)]:
                    self._quote_cache.pop(k, None)
        return result

    async def quote_sell(
        self,
        mint: str,
        token_amount_raw: int,
        slippage_bps: int | None = None,
    ) -> QuoteResult | None:
        """Verify that the *sell* side is quotable for the exact token amount.

        This is the second half of the two-sided execution gate: a token may
        be buyable (SOL→TOKEN route exists) but un-sellable (TOKEN→SOL has no
        route, or price impact > cap). CATE/ELON/Google-AI all passed the buy
        quote but failed every sell quote with ``Failed to get quotes``.
        ``slippage_bps`` defaults to the buy slippage; callers that want the
        escalation ladder should quote explicitly per rung via ``_do_quote_sell``.
        """
        slippage = slippage_bps if slippage_bps is not None else self._slippage_bps
        return await self._do_quote_sell(mint, token_amount_raw, slippage)

    async def _do_quote_sell(
        self, mint: str, amount_raw: int, slippage_bps: int
    ) -> QuoteResult:
        """Single TOKEN→SOL order fetch + impact gate."""
        self._qstats["quotes"] += 1
        await self._quote_slot()
        t0 = time.monotonic()
        try:
            # Always quote with taker in live mode so the route reflects the
            # actual wallet's token balance/liquidity; in paper mode the taker
            # is omitted (same as buy side) – the route check is identical and
            # a drained pool still returns ``Failed to get quotes``.
            if self._paper_quoting:
                order = await self._order(mint, BASE_MINT, amount_raw, slippage_bps)
            else:
                # For live sell-quote we need a taker but may not have keypair
                # during tests – fall back to taker-less if no key.
                taker = str(self._keypair.pubkey()) if self._keypair else None
                order = await self._order(mint, BASE_MINT, amount_raw, slippage_bps, taker)
        except httpx.TimeoutException as exc:
            reason = "quote_timeout"
            self._qstats[reason] += 1
            log.warning("sell-quote timeout for %s: %s", mint, exc)
            return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, reason)
        except JupiterError as exc:
            reason = self._classify_error(exc)
            self._qstats[reason] += 1
            log.info("sell-quote %s for %s: %s", reason, mint, exc)
            return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, reason)
        except httpx.RequestError as exc:
            self._qstats["quote_http_error"] += 1
            log.warning("sell-quote http error for %s: %s", mint, exc)
            return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, "quote_http_error")
        except Exception:
            self._qstats["quote_exception"] += 1
            log.exception("sell-quote exception for %s", mint)
            return QuoteResult(False, None, amount_raw, 0, 0.0, 0, 0.0, "quote_exception")

        latency_ms = (time.monotonic() - t0) * 1000
        self._record_latency(latency_ms)
        fetched_at = time.monotonic()
        out = int(order.get("outAmount") or order.get("actualOutAmount") or 0)
        impact = abs(float(order.get("priceImpact") or 0.0))
        if impact == 0.0 and order.get("priceImpactPct"):
            impact = abs(float(order["priceImpactPct"])) * 100.0
        try:
            slip_bps: int | None = (
                int(order["slippageBps"]) if order.get("slippageBps") is not None else None
            )
        except (TypeError, ValueError):
            slip_bps = None
        diag = {"router": str(order.get("router") or ""),
                "mode": str(order.get("mode") or ""), "slippage_bps": slip_bps}
        route = order.get("routePlan") or order.get("routes") or []
        if out <= 0 or not route:
            self._qstats["quote_no_route"] += 1
            return QuoteResult(False, order, amount_raw, out, impact, len(route), latency_ms, "quote_no_route", fetched_at, **diag)
        # Sell impact is checked against the tighter sell cap when set.
        cap = self.max_sell_impact_pct if self.max_sell_impact_pct > 0 else self._max_impact
        if impact > cap:
            self._qstats["quote_impact"] += 1
            return QuoteResult(False, order, amount_raw, out, impact, len(route), latency_ms, "quote_impact", fetched_at, **diag)
        self._qstats["ok"] += 1
        return QuoteResult(True, order, amount_raw, out, impact, len(route), latency_ms, "ok", fetched_at, **diag)

    async def check_quote_stability(
        self,
        mint: str,
        amount_raw: int,
        base: QuoteResult | None = None,
        slippage_bps: int | None = None,
    ) -> tuple[bool, str, dict]:
        """Stability gate: the same quote sampled N times must stay within pct bounds.

        Args:
            base: The already-fetched FINAL entry quote. When given it is
                reused as sample #1 (no extra request) and all drift is
                measured against it — so the gate validates the exact order
                that is about to be executed, not a throwaway first sample.
                When omitted, ``checks`` fresh quotes are taken.
            slippage_bps: The slippage mode to use for samples. Must match
                the base quote's mode (None for RTSE/ultra, integer for
                manual slippage) so routing/output is comparable.

        Returns:
            ``(ok, reason, info)``. ``info`` carries measurable features for
            the trade journal: ``max_out_drift_pct``, ``max_impact_drift_pp``
            and ``routers`` (per-sample router/mode/slippage diagnostics).

        ``MAX_QUOTE_CHANGE_PCT`` bounds the ``outAmount`` drift,
        ``MAX_IMPACT_CHANGE_PCT`` bounds the impact drift. A pump-and-dump
        that loses ``-27%/-61%`` in 2s (vs healthy ``±1%``) is rejected here
        before any buy. Samples are spaced at ``QUOTE_STABILITY_INTERVAL_MS``
        (not the global 1s throttle) so the whole window costs ~600ms for the
        default 3×300ms config — fast enough to run inside a launch entry.
        """
        checks = max(self.quote_stability_checks, 1)
        interval_s = max(self.quote_stability_interval_ms, 0) / 1000.0
        outs: list[int] = []
        impacts: list[float] = []
        routers: list[str] = []
        if base is not None:
            if not base.success or base.output_amount <= 0:
                return False, "stability_no_quote:bad_base", {}
            outs.append(base.output_amount)
            impacts.append(base.price_impact_pct)
            routers.append(base.router or "?")
        samples_needed = max(checks - len(outs), 0)
        for i in range(samples_needed):
            q = await self._do_quote(
                mint, amount_raw, slippage_bps, min_spacing=interval_s
            )
            if not q.success:
                return False, f"stability_no_quote:{q.reason}", {}
            outs.append(q.output_amount)
            impacts.append(q.price_impact_pct)
            routers.append(q.router or "")
            if i + 1 < samples_needed and interval_s > 0:
                await asyncio.sleep(interval_s)
        if not outs or outs[0] <= 0:
            return False, "stability_no_samples", {}
        base_out = outs[0]
        base_impact = impacts[0]
        info = {
            "max_out_drift_pct": max(
                (abs(o - outs[0]) / outs[0] * 100.0 for o in outs[1:]), default=0.0
            ),
            "max_impact_drift_pp": max(
                (abs(i - impacts[0]) for i in impacts[1:]), default=0.0
            ),
            "routers": routers,
            "slippage_bps": getattr(base, "slippage_bps", None),
        }
        for out in outs[1:]:
            drift = abs(out - base_out) / base_out * 100.0
            if drift > self.max_quote_change_pct:
                return False, f"quote_drift:{drift:.1f}%>{self.max_quote_change_pct:.0f}%", info
        for imp in impacts[1:]:
            drift = abs(imp - base_impact)
            # impact is already a pct; drift is absolute pp difference.
            if drift > self.max_impact_change_pct:
                return False, f"impact_drift:{drift:.1f}pp>{self.max_impact_change_pct:.0f}pp", info
        return True, "", info

    # ------------------------------------------------------------- high level
    async def execute_order(self, order: dict | None) -> SwapResult:
        """Execute an ALREADY-VALIDATED ``/order`` payload — never re-quotes.

        This closes the final-quote gap: the caller passes the exact
        ``QuoteResult.order`` that just passed the entry gates, so the market
        state that was validated (route, impact, stability, sellability) is
        byte-for-byte the transaction that lands on chain. In paper mode the
        taker-less order carries no transaction and this returns a failure
        mirroring live's "nothing to execute".

        Invariant: there is NO ``buy()`` convenience method anymore — the only
        way to a live buy is quote() -> gates -> execute_order(order), so a
        second unvalidated ``/order`` cannot sneak into the executed path.
        """
        if not order or not order.get("transaction"):
            return SwapResult(
                False, "", 0, 0,
                "paper quote: no transaction to execute",
            )
        # --- Pre-flight simulation (live mode only) ---
        # Simulate the signed tx via Shyft RPC before hitting /execute.
        # Catches: honeypot transfers, frozen mints, compute budget blowouts,
        # and any on-chain rejection that Jupiter's quote gate cannot detect.
        env = config.load_env()
        if (self.live
                and config.get_bool(env, "SIMULATE_BEFORE_EXECUTE", True)
                and order.get("transaction")):
            sim = await self.simulate_transaction(order["transaction"])
            if not sim.get("ok"):
                reason = sim.get("reason", "sim_unknown")
                logs.journal("sim_skip", reason=reason,
                             compute_units=sim.get("units", 0))
                log.info("simulate SKIP: %s (units=%s)", reason, sim.get("units"))
                return SwapResult(False, "", 0, 0, f"sim_failed:{reason}")
            units = sim.get("units", 0)
            if units > 200_000:
                reason = f"sim_compute_exceeded:{units}"
                logs.journal("sim_skip", reason=reason, compute_units=units)
                log.info("simulate SKIP: %s", reason)
                return SwapResult(False, "", 0, 0, reason)
            log.info("simulate OK (units=%d)", units)
        return await self.execute(order)

    async def sell(self, mint: str, amount_raw: int) -> SwapResult:
        """Sell ``amount_raw`` of ``mint`` for SOL, escalating slippage.

        Args:
            mint: Token contract address to sell.
            amount_raw: Raw token amount (captured from the buy output).

        Reconciliation: after any failed execute, the returned signature is
        checked on-chain via ``check_tx_status`` before retrying.  If the tx
        landed (confirmed/finalized) or errored on-chain, we return immediately
        — preventing the dangerous double-sell that happens when a timeout
        caused a retry on an already-landed tx.

        Error classification:
            - -2003 (quote expired): re-quote at same slippage
            - 6024/6051 (slippage): widen slippage for next attempt
            - -2004 (swap rejected): market hostile, skip immediately
        """
        if self._keypair is None:
            return SwapResult(False, "", amount_raw, 0, "paper mode: cannot sign")
        # Build a strictly-ascending, deduplicated slippage ladder starting
        # from the base slippage.  E.g. base=300 + escalation=(500,1000)
        # -> [300, 500, 1000].  Older configs with overlapping values like
        # (200, 300, 500, 1000) are safely deduped.
        seen: set[int] = set()
        ladder: list[int] = []
        for s in (self._slippage_bps,) + tuple(self._sell_slippage_escalation):
            if s not in seen:
                seen.add(s)
                ladder.append(s)
        ladder.sort()
        last: SwapResult | None = None
        for slippage in ladder:
            try:
                order = await self._order(
                    mint,
                    BASE_MINT,
                    amount_raw,
                    slippage,
                    str(self._keypair.pubkey()),
                    _retry_transient=True,
                )
            except JupiterError as exc:
                log.warning("sell order @%dbps failed: %s", slippage, exc)
                last = SwapResult(False, "", amount_raw, 0, str(exc))
                continue
            # Pre-flight simulation for sells (live mode only)
            env = config.load_env()
            if (self.live
                    and config.get_bool(env, "SIMULATE_BEFORE_EXECUTE", True)
                    and order.get("transaction")):
                sim = await self.simulate_transaction(order["transaction"])
                if not sim.get("ok"):
                    reason = sim.get("reason", "sim_unknown")
                    log.info("sell simulate SKIP @%dbps: %s", slippage, reason)
                    last = SwapResult(False, "", amount_raw, 0, f"sim_failed:{reason}")
                    continue
            result = await self.execute(order)
            if result.success:
                return result
            last = result
            log.warning("sell execute @%dbps failed (code=%s): %s",
                        slippage, result.error_code, result.error)
            # --- Error classification ---
            if result.is_hostile_market:
                log.warning("sell %s: hostile market (code=%d) — skipping",
                            mint[:10], result.error_code or 0)
                break
            if result.is_retryable_quote_error:
                # Quote expired — re-quote at same slippage (don't widen)
                log.info("sell %s: quote expired — re-quoting at %dbps",
                         mint[:10], slippage)
                await asyncio.sleep(0.5)
                continue
            # For slippage errors (6024/6051), fall through to widen
            # --- Reconcile before retrying ---
            sig = result.signature
            if sig:
                # Give the network a moment to confirm, then check.
                await asyncio.sleep(3.0)
                status = await self.check_tx_status(sig)
                if status in ("confirmed", "finalized"):
                    log.info("sell tx %s landed (status=%s) — returning as success",
                             sig[:12], status)
                    return SwapResult(
                        True, sig, amount_raw,
                        int(result.output_amount or 0),
                        f"reconciled after timeout (status={status})",
                    )
                if status == "error":
                    log.warning("sell tx %s errored on-chain — retrying", sig[:12])
                    # Fall through to retry at next slippage level
                elif status == "processed":
                    # Processed but not confirmed yet — wait longer
                    await asyncio.sleep(5.0)
                    status2 = await self.check_tx_status(sig)
                    if status2 in ("confirmed", "finalized"):
                        log.info("sell tx %s confirmed after extra wait", sig[:12])
                        return SwapResult(
                            True, sig, amount_raw,
                            int(result.output_amount or 0),
                            f"reconciled (status={status2})",
                        )
                    log.info("sell tx %s still %s — retrying with higher slippage",
                             sig[:12], status2 or "unknown")
                else:
                    log.info("sell tx %s not found on-chain — retrying",
                             sig[:12])
            # Backoff before next escalation step
            if slippage < ladder[-1]:
                await asyncio.sleep(0.5)
            if slippage >= 1000:
                break
        if last and not last.success:
            log.warning("sell %s: all slippage levels exhausted (final error: %s)",
                        mint[:10], last.error)
            logs.journal("sell_failed", mint=mint, amount_raw=amount_raw,
                         error=last.error, error_code=last.error_code)
        return last or SwapResult(False, "", amount_raw, 0, "sell failed")

    # ── PumpAPI Trade API (bonding curve fallback) ──────────────────────

    def _pumpapi_enabled(self) -> bool:
        """Check if PumpAPI fallback is enabled and configured."""
        env = config.load_env()
        if not config.get_bool(env, "PUMPAPI_ENABLED", False):
            return False
        # In dry_run mode, allow PumpAPI (paper mode handles it)
        if self._dry_run:
            return True
        return bool(self._private_key)

    async def buy_via_pumpapi(
        self, mint: str, amount_sol: float
    ) -> SwapResult:
        """Buy a bonding curve token via PumpAPI Trade API.

        Used when Jupiter returns no route (non-migrated tokens).
        """
        if not self._pumpapi_enabled():
            return SwapResult(False, "", 0, 0, "pumpapi disabled or no api key")
        if self._dry_run:
            log.info("PAPER buy via pumpapi %s (%.4f SOL)", mint[:10], amount_sol)
            return SwapResult(True, f"paper_pumpapi_{mint[:8]}", 0, amount_sol, "paper")

        url = "https://api.pumpapi.io"
        env = config.load_env()
        payload = {
            "privateKey": self._private_key,
            "action": "buy",
            "mint": mint,
            "amount": amount_sol,
            "denominatedInQuote": True,
            "slippage": config.get_int(env, "PUMPAPI_SLIPPAGE", 99),
            "priorityFee": config.get_float(env, "PUMPAPI_PRIORITY_FEE", 0.00023),
            "guaranteedDelivery": config.get_bool(env, "PUMPAPI_GUARANTEED_DELIVERY", True),
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    data = await resp.json()
                    if resp.status != 200:
                        error = data.get("error", data.get("message", f"HTTP {resp.status}"))
                        log.warning("pumpapi buy failed %s: %s", mint[:10], error)
                        logs.journal("pumpapi_buy_failed", mint=mint, error=error,
                                     amount_sol=amount_sol)
                        return SwapResult(False, "", 0, 0, f"pumpapi: {error}")

                    signature = data.get("signature", "")
                    confirmed = data.get("confirmed", False)
                    log.info("pumpapi buy %s: sig=%s confirmed=%s",
                             mint[:10], signature[:16], confirmed)
                    logs.journal("pumpapi_buy", mint=mint, signature=signature,
                                 amount_sol=amount_sol, confirmed=confirmed)
                    return SwapResult(
                        True, signature, 0, amount_sol,
                        f"pumpapi{'_confirmed' if confirmed else '_pending'}",
                    )
        except Exception as e:
            log.warning("pumpapi buy error %s: %s", mint[:10], e)
            logs.journal("pumpapi_buy_error", mint=mint, error=str(e))
            return SwapResult(False, "", 0, 0, f"pumpapi: {e}")

    async def sell_via_pumpapi(
        self, mint: str, amount_pct: int = 100
    ) -> SwapResult:
        """Sell a bonding curve token via PumpAPI Trade API.

        Used when Jupiter fails on sell (token not on Jupiter).
        """
        if not self._pumpapi_enabled():
            return SwapResult(False, "", 0, 0, "pumpapi disabled or no api key")
        if self._dry_run:
            log.info("PAPER sell via pumpapi %s (%d%%)", mint[:10], amount_pct)
            return SwapResult(True, f"paper_pumpapi_sell_{mint[:8]}", 0, 0, "paper")

        url = "https://api.pumpapi.io"
        env = config.load_env()
        payload = {
            "privateKey": self._private_key,
            "action": "sell",
            "mint": mint,
            "amount": f"{amount_pct}%",
            "denominatedInQuote": True,
            "slippage": config.get_int(env, "PUMPAPI_SLIPPAGE", 99),
            "priorityFee": config.get_float(env, "PUMPAPI_PRIORITY_FEE", 0.00023),
            "guaranteedDelivery": config.get_bool(env, "PUMPAPI_GUARANTEED_DELIVERY", True),
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    data = await resp.json()
                    if resp.status != 200:
                        error = data.get("error", data.get("message", f"HTTP {resp.status}"))
                        log.warning("pumpapi sell failed %s: %s", mint[:10], error)
                        logs.journal("pumpapi_sell_failed", mint=mint, error=error)
                        return SwapResult(False, "", 0, 0, f"pumpapi: {error}")

                    signature = data.get("signature", "")
                    confirmed = data.get("confirmed", False)
                    log.info("pumpapi sell %s: sig=%s confirmed=%s",
                             mint[:10], signature[:16], confirmed)
                    logs.journal("pumpapi_sell", mint=mint, signature=signature,
                                 confirmed=confirmed)
                    return SwapResult(
                        True, signature, 0, 0,
                        f"pumpapi{'_confirmed' if confirmed else '_pending'}",
                    )
        except Exception as e:
            log.warning("pumpapi sell error %s: %s", mint[:10], e)
            logs.journal("pumpapi_sell_error", mint=mint, error=str(e))
            return SwapResult(False, "", 0, 0, f"pumpapi: {e}")