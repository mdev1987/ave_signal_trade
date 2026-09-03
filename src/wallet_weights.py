"""Wallet-quality weights derived from SolanaTracker PnL V2 performance.

The live consensus used to count every tracked wallet equally ("2 wallets =
signal"). That let low-win-rate noise wallets manufacture fake consensus. This
module turns the wallet_performance.json metrics into a *weight*: how many
"average consensus wallets" a given wallet is worth.

Weight model (all knobs in config.Settings / .env):

    win_rate is a percentage (e.g. 60.65). Convert to a fraction, then:
      base = clamp((wr - floor_win) / (full_win - floor_win), 0, 1)
             floor_win (0.40) -> 0      full_win (0.60) -> 1.0
      tier = 1.5 if pnl_total >= tier2 (5M)
             1.25 if pnl_total >= tier1 (1M)
             1.0  otherwise
      weight = clamp(base * tier, 0, max_weight)

So a single 60%+/multi-million-PnL wallet scores ~1.0-1.5 (enough on its own to
trigger, since the consensus threshold defaults to 1.0), while two ~50% wallets
(0.5 each) also clear it — same as the old "2 wallets" rule, but now quality-
weighted. Wallets below floor_win contribute 0 (filtered as noise). Wallets
missing from the performance file get ``default_weight`` (0.5) so the bot
degrades gracefully to the legacy behaviour instead of breaking.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def build_weights(
    path: str = "wallet_performance.json",
    *,
    floor_win: float = 0.40,
    full_win: float = 0.60,
    pnl_tier1: float = 1_000_000.0,
    pnl_tier2: float = 5_000_000.0,
    tier1_mult: float = 1.25,
    tier2_mult: float = 1.5,
    default_weight: float = 0.5,
    max_weight: float = 2.0,
    confidence_trades: int = 30,
) -> tuple[dict[str, float], float]:
    """Return (address->weight, default_weight) from a wallet_performance.json.

    Uses a Bayesian-inspired confidence factor so wallets with few trades
    don't immediately become "elite" from a tiny sample. A wallet with 60%
    win rate on 5 trades gets penalised vs 60% on 500 trades.
    """
    weights: dict[str, float] = {}
    data: list[dict] = []
    p = Path(path)
    if p.exists():
        try:
            data = json.loads(p.read_text())
        except Exception:
            data = []
    denom = (full_win - floor_win) or 1.0
    for rec in data:
        addr = rec.get("address")
        if not addr or not rec.get("ok"):
            continue
        wr = (rec.get("win_rate") or 0.0) / 100.0  # percent -> fraction
        if wr <= 0 or wr < floor_win:
            weights[addr] = 0.0
            continue
        # Sample-size confidence: shrink win rate toward 0.5 for small samples
        trades = rec.get("picks") or rec.get("trades") or 0
        confidence = min(1.0, trades / confidence_trades)
        adjusted_wr = 0.5 + confidence * (wr - 0.5)
        if adjusted_wr < floor_win:
            weights[addr] = 0.0
            continue
        base = min(1.0, (adjusted_wr - floor_win) / denom)
        pnl = rec.get("pnl_total") or 0.0
        tier = tier2_mult if pnl >= pnl_tier2 else tier1_mult if pnl >= pnl_tier1 else 1.0
        weights[addr] = round(min(max_weight, base * tier), 3)
    return weights, default_weight


async def build_weights_from_soltracker(
    soltracker,
    wallets: list[str],
    *,
    floor_win: float = 0.40,
    full_win: float = 0.60,
    pnl_tier1: float = 1_000_000.0,
    pnl_tier2: float = 5_000_000.0,
    tier1_mult: float = 1.25,
    tier2_mult: float = 1.5,
    default_weight: float = 0.5,
    max_weight: float = 2.0,
    confidence_trades: int = 30,
) -> tuple[dict[str, float], float]:
    """Build wallet weights from live SolanaTracker PnL V2 batch summaries.

    Falls back to empty dict on any failure so the caller can use the JSON
    file instead.
    """
    if not wallets:
        return {}, default_weight
    try:
        summaries = await soltracker.get_wallets_summary(wallets)
    except Exception:
        log.exception("soltracker wallet summary failed")
        return {}, default_weight
    if not summaries:
        return {}, default_weight
    weights: dict[str, float] = {}
    denom = (full_win - floor_win) or 1.0
    for addr in wallets:
        s = summaries.get(addr)
        if not s:
            weights[addr] = default_weight
            continue
        # Extract win rate from PnL V2 response
        wr_pct = 0.0
        pnl_data = s.get("pnl") or {}
        if isinstance(pnl_data, dict):
            wr_pct = pnl_data.get("winPercentage") or 0.0
        if wr_pct <= 0:
            # Try top-level winRate field
            wr_pct = s.get("winRate") or 0.0
        wr = wr_pct / 100.0 if wr_pct > 1 else wr_pct  # handle both % and fraction
        if wr <= 0 or wr < floor_win:
            weights[addr] = 0.0
            continue
        # Sample-size confidence
        trades = 0
        if isinstance(pnl_data, dict):
            trades = pnl_data.get("trades") or 0
        if trades <= 0:
            trades = s.get("trades") or 0
        confidence = min(1.0, trades / confidence_trades)
        adjusted_wr = 0.5 + confidence * (wr - 0.5)
        if adjusted_wr < floor_win:
            weights[addr] = 0.0
            continue
        base = min(1.0, (adjusted_wr - floor_win) / denom)
        pnl_total = 0.0
        if isinstance(pnl_data, dict):
            pnl_total = pnl_data.get("total") or pnl_data.get("realized") or 0.0
        if pnl_total <= 0:
            pnl_total = s.get("pnlTotal") or 0.0
        tier = tier2_mult if pnl_total >= pnl_tier2 else tier1_mult if pnl_total >= pnl_tier1 else 1.0
        weights[addr] = round(min(max_weight, base * tier), 3)
    n_scored = sum(1 for w in weights.values() if w > 0)
    log.info("soltracker weights: %d/%d wallets scored (live)", n_scored, len(wallets))
    return weights, default_weight


async def build_weights_from_gmgn(
    gmgn,
    wallets: list[str],
    *,
    floor_win: float = 0.40,
    full_win: float = 0.60,
    pnl_tier1: float = 1_000_000.0,
    pnl_tier2: float = 5_000_000.0,
    tier1_mult: float = 1.25,
    tier2_mult: float = 1.5,
    default_weight: float = 0.5,
    max_weight: float = 2.0,
    confidence_trades: int = 30,
) -> tuple[dict[str, float], float]:
    """Build wallet weights from GMGN batch wallet_profits (single call, weight=3).

    Falls back to empty dict on any failure so the caller can use soltracker
    or file-based weights instead.
    """
    if not wallets:
        return {}, default_weight
    try:
        profits = await gmgn.get_wallet_profits(wallets, period="all")
    except Exception:
        log.exception("gmgn wallet_profits failed")
        return {}, default_weight
    if not profits:
        return {}, default_weight
    weights: dict[str, float] = {}
    denom = (full_win - floor_win) or 1.0
    for addr in wallets:
        p = profits.get(addr)
        if not p:
            weights[addr] = default_weight
            continue
        # GMGN returns winrate as a percentage (e.g. 65.5)
        wr_pct = p.get("winrate") or p.get("win_rate") or 0.0
        wr = wr_pct / 100.0 if wr_pct > 1 else wr_pct
        if wr <= 0 or wr < floor_win:
            weights[addr] = 0.0
            continue
        # Sample-size confidence
        trades = (p.get("buy") or 0) + (p.get("sell") or 0)
        confidence = min(1.0, trades / confidence_trades)
        adjusted_wr = 0.5 + confidence * (wr - 0.5)
        if adjusted_wr < floor_win:
            weights[addr] = 0.0
            continue
        base = min(1.0, (adjusted_wr - floor_win) / denom)
        pnl_total = p.get("realized_profit") or p.get("total_realized_profit") or 0.0
        tier = tier2_mult if pnl_total >= pnl_tier2 else tier1_mult if pnl_total >= pnl_tier1 else 1.0
        weights[addr] = round(min(max_weight, base * tier), 3)
    n_scored = sum(1 for w in weights.values() if w > 0)
    log.info("gmgn weights: %d/%d wallets scored (live)", n_scored, len(wallets))
    return weights, default_weight
