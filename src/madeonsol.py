"""MadeOnSol integration — KOL tracking, risk scoring, coordination detection.

Uses the official madeonsol-x402 SDK. Free tier: 200 calls/day, 5-min delay.
PRO tier ($49/mo): 10,000 calls/day, real-time feeds.

Key endpoints used:
  - token_risk()           — 0-100 rug risk score (replaces RugCheck/DBotX)
  - token_buyer_quality()  — 0-100 score for early buyers
  - kol_coordination()     — multi-KOL convergence detection
  - kol_feed()             — real-time KOL buy/sell stream
  - wallet_batch_classify() — batch wallet reputation
  - sniper_recent()        — pre-confirmation deploy detection
"""

from __future__ import annotations

import logging
import time

from madeonsol_x402 import MadeOnSolClient

log = logging.getLogger(__name__)


class MadeOnSolGate:
    """Wrapper around MadeOnSol SDK for trading bot integration."""

    def __init__(self, api_key: str) -> None:
        if not api_key:
            self.client = None
            log.warning("madeonsol: no API key, all checks disabled")
            return
        self.client = MadeOnSolClient(api_key=api_key)
        log.info("madeonsol: client initialized (key=%s…)", api_key[:12])

    @property
    def enabled(self) -> bool:
        return self.client is not None

    async def check_risk(self, mint: str, max_score: int = 70) -> tuple[bool, str]:
        """Check token risk score. Returns (safe, reason).
        Note: token_risk requires PRO tier. Free tier returns True (pass).
        """
        if not self.enabled:
            return True, ""
        try:
            risk = self.client.rest.token_risk(mint)
            score = risk.get("risk_score", 0)
            band = risk.get("band", "unknown")
            if score > max_score:
                return False, f"madeonsol_risk({score}>{max_score},band={band})"
            return True, ""
        except Exception as exc:
            # Free tier gets 403 on token_risk — fail-open
            if "403" in str(exc):
                log.debug("madeonsol risk: PRO required, skipping")
                return True, ""
            log.warning("madeonsol risk check failed for %s: %s", mint[:10], exc)
            return True, ""  # fail-open

    async def check_buyer_quality(self, mint: str, min_avg: int = 30) -> tuple[bool, str]:
        """Check average buyer quality score. Returns (safe, reason)."""
        if not self.enabled:
            return True, ""
        try:
            bq = self.client.rest.token_buyer_quality(mint)
            buyers = bq.get("buyers", [])
            scores = [b.get("score", 0) for b in buyers if b.get("score")]
            if scores:
                avg = sum(scores) / len(scores)
                if avg < min_avg:
                    return False, f"low_buyer_quality({avg:.0f}<{min_avg})"
            return True, ""
        except Exception as exc:
            log.warning("madeonsol buyer quality failed for %s: %s", mint[:10], exc)
            return True, ""

    async def check_coordination(self, mint: str, min_kols: int = 3) -> dict | None:
        """Check if token has multi-KOL coordination signal.
        Uses client.kol_coordination() (x402-priced endpoint).
        """
        if not self.enabled:
            return None
        try:
            signals = self.client.kol_coordination(min_kols=min_kols, period="24h")
            for sig in (signals.get("signals", []) if signals else []):
                if sig.get("mint") == mint or sig.get("token_mint") == mint:
                    return sig
            return None
        except Exception as exc:
            log.warning("madeonsol coordination check failed: %s", exc)
            return None

    async def get_kol_feed(self, limit: int = 20, action: str = "buy") -> list[dict]:
        """Get recent KOL trades. Uses client.kol_feed() (x402-priced).
        Free tier = 5min delay.
        """
        if not self.enabled:
            return []
        try:
            feed = self.client.kol_feed(limit=limit, action=action)
            return feed.get("trades", []) if feed else []
        except Exception as exc:
            log.warning("madeonsol kol feed failed: %s", exc)
            return []

    async def classify_wallets(self, addresses: list[str]) -> dict:
        """Batch classify wallets. Returns {address: {is_kol, is_sniper, ...}}."""
        if not self.enabled or not addresses:
            return {}
        try:
            result = self.client.rest.wallet_batch_classify(addresses[:100])
            wallets = result.get("wallets", [])
            return {w["address"]: w for w in wallets if w.get("address")}
        except Exception as exc:
            log.warning("madeonsol wallet classify failed: %s", exc)
            return {}

    async def get_sniper_alerts(self, limit: int = 10, deployer_tier: str = "elite") -> list[dict]:
        """Get recent pre-confirmation deploy alerts."""
        if not self.enabled:
            return []
        try:
            feed = self.client.rest.sniper_recent(limit=limit, deployer_tier=deployer_tier)
            return feed.get("deploys", [])
        except Exception as exc:
            log.warning("madeonsol sniper alerts failed: %s", exc)
            return []

    def quota_status(self) -> dict:
        """Return current quota usage."""
        if not self.enabled:
            return {"used": 0, "limit": 0}
        try:
            rl = self.client.rest.last_rate_limit or {}
            return {
                "used": rl.get("used", 0),
                "limit": rl.get("limit", 200),
                "remaining": rl.get("remaining", 0),
            }
        except Exception:
            return {"used": 0, "limit": 200}
