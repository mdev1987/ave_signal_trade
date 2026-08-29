"""Configuration - smart-money watcher edition."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_env(path: str = ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return env
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        # strip a trailing inline comment (e.g. "KEY=val   # note")
        v = v.split("#", 1)[0].strip()
        env[k.strip()] = v
    return env


def get(env, key, default=None):
    v = env.get(key)
    return v if v not in (None, "") else default


def get_int(env, key, default=0) -> int:
    try:
        return int(get(env, key, default))
    except (TypeError, ValueError):
        return int(default)


def get_float(env, key, default=0.0) -> float:
    try:
        return float(get(env, key, default))
    except (TypeError, ValueError):
        return float(default)


def get_bool(env, key, default=False) -> bool:
    v = get(env, key)
    return default if v is None else str(v).strip().lower() in ("1", "true", "yes", "on")


def get_csv(env, key, default="") -> list[str]:
    raw = get(env, key, default) or ""
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def get_csv_ints(env, key, default="") -> list[int]:
    out: list[int] = []
    for x in get_csv(env, key, default):
        try:
            out.append(int(x))
        except ValueError:
            pass
    return out


def parse_ladder(env, key: str, default: str) -> list[tuple[float, float]]:
    """Parse a take-profit ladder from env: "1.3:1.0,1.6:0.3,2.5:0.2".

    Each pair is (price_multiple, fraction_of_remaining_size_to_sell). Fractions
    across levels should sum to ~1.0 (a level may sell the whole remainder).
    """
    raw = get(env, key, default) or default
    out: list[tuple[float, float]] = []
    for piece in str(raw).split(","):
        piece = piece.strip()
        if not piece or ":" not in piece:
            continue
        try:
            m, f = piece.strip('"').strip("'").split(":")
            out.append((float(m), float(f)))
        except ValueError:
            continue
    return out or [(1.3, 1.0)]


@dataclass(frozen=True)
class Settings:
    # telegram notify
    bot_token: str = ""
    chat_id: str = ""
    # strategy
    size_sol: float = 0.05
    # Take-profit ladder (backtest-validated). Each (mult, frac) sells `frac`
    # of the REMAINING size when the peak hits `mult`. Backtest showed a
    # full-exit-at-first-spike ladder is positive across all wallet regimes,
    # so the default is a single level that exits 100% at +30%.
    tp_ladder: list = None             # filled by load_settings -> [(1.3, 1.0)]
    tp1_mult: float = 1.30             # legacy single-TP fallback (bank 50%)
    trail_retrace_pct: float = 0.35
    trail_enabled: bool = False         # trailing stop OFF by default (full-spike exit wins)
    trail_start_mult: float = 1.30     # only trail after a peak >= this
    hard_stop_pct: float = 0.35        # hard stop (was 0.40)
    open_min_wallets: int = 2           # legacy distinct-wallet floor (kept for compat; weighted score is the real gate)
    open_min_liq_usd: float = 5000.0    # skip illiquid tokens (exit slippage)
    # --- data-driven wallet-quality weighting (consensus = weighted score) ---
    # Each KOL contributes a weight from its real win rate + PnL (see
    # wallet_weights.build_weights); a token "fires" when the summed weight of
    # distinct buying wallets reaches CONSENSUS_WEIGHT_THRESHOLD. This stops
    # low-win-rate noise wallets from manufacturing fake consensus.
    wallet_perf_path: str = "wallet_performance.json"
    wallet_weight_floor_win: float = 0.40   # win rate below this -> weight 0 (noise)
    wallet_weight_full_win: float = 0.60    # win rate at/above this -> base 1.0
    wallet_pnl_tier1: float = 1_000_000.0   # >=$1M PnL -> 1.25x weight
    wallet_pnl_tier2: float = 5_000_000.0   # >=$5M PnL -> 1.5x weight
    wallet_weight_tier1_mult: float = 1.25
    wallet_weight_tier2_mult: float = 1.5
    wallet_default_weight: float = 0.5      # weight when perf data is missing
    wallet_weight_max: float = 2.0
    consensus_weight_threshold: float = 1.0  # ~2 avg KOLs OR 1 strong (60%+/big)
    be_buffer_pct: float = 0.0          # after 1st TP, raise stop to entry+this (breakeven lock)
    max_hold_h: float = 72.0            # force-close dead positions after this many hours
    max_open_positions: int = 20        # hard cap on concurrent shadow positions (raised)
    start_balance_sol: float = 4.0      # larger paper book so the cap is capital-bound
    shadow_state_file: str = "shadow_book.json"
    status_every_min: float = 30.0
    # watcher
    watch_poll_s: float = 45.0
    watch_min_buy_usd: float = 50.0
    watch_consensus_wallets: int = 2
    watch_consensus_window_s: float = 600.0
    watch_first_lookback_s: float = 90.0
    open_gap_s: float = 20.0          # min seconds between new positions (anti-flood)
    # discovery (batch wallet finder)
    discover_max_tokens: int = 25
    discover_max_wallets: int = 40
    discover_early_window_s: float = 1800.0
    discover_tx_per_pool: int = 60
    discover_min_buy_usd: float = 50.0
    discover_max_buy_usd: float = 1_000_000.0  # ignore freak volume*price outliers
    discover_out_file: str = "smart_money_wallets.json"
    discover_pump_pct: float = 100.0  # token counts as "pumped" if 24h chg >= this
    discover_enrich: bool = True      # fetch token details to mark pumped (no DeBot)
    # dexscreener
    dexscreener_base_url: str = "https://api.dexscreener.com"
    dexscreener_rpm: int = 300


def load_settings(path: str = ".env") -> Settings:
    env = load_env(path)
    env.update(os.environ)
    return Settings(
        bot_token=get(env, "BOT_TOKEN", ""),
        chat_id=get(env, "CHAT_ID", ""),
        size_sol=get_float(env, "SIZE_SOL", 0.05),
        tp_ladder=parse_ladder(env, "TP_LADDER", "1.3:0.4,1.8:0.3,3.0:0.3"),
        trail_retrace_pct=get_float(env, "TRAIL_RETRACE_PCT", 0.25),
        hard_stop_pct=get_float(env, "HARD_STOP_PCT", 0.35),
        trail_enabled=get_bool(env, "TRAIL_ENABLED", True),
        trail_start_mult=get_float(env, "TRAIL_START_MULT", 1.4),
        tp1_mult=get_float(env, "TP1_MULT", 1.30),
        open_min_wallets=get_int(env, "OPEN_MIN_WALLETS", 2),
        open_min_liq_usd=get_float(env, "OPEN_MIN_LIQ_USD", 5000.0),
        wallet_perf_path=get(env, "WALLET_PERF_PATH", "wallet_performance.json"),
        wallet_weight_floor_win=get_float(env, "WALLET_WEIGHT_FLOOR_WIN", 0.40),
        wallet_weight_full_win=get_float(env, "WALLET_WEIGHT_FULL_WIN", 0.60),
        wallet_pnl_tier1=get_float(env, "WALLET_PNL_TIER1", 1_000_000.0),
        wallet_pnl_tier2=get_float(env, "WALLET_PNL_TIER2", 5_000_000.0),
        wallet_weight_tier1_mult=get_float(env, "WALLET_WEIGHT_TIER1_MULT", 1.25),
        wallet_weight_tier2_mult=get_float(env, "WALLET_WEIGHT_TIER2_MULT", 1.5),
        wallet_default_weight=get_float(env, "WALLET_DEFAULT_WEIGHT", 0.5),
        wallet_weight_max=get_float(env, "WALLET_WEIGHT_MAX", 2.0),
        consensus_weight_threshold=get_float(env, "CONSENSUS_WEIGHT_THRESHOLD", 1.0),
        be_buffer_pct=get_float(env, "BE_BUFFER_PCT", 0.0),
        max_hold_h=get_float(env, "MAX_HOLD_H", 72.0),
        max_open_positions=get_int(env, "MAX_OPEN_POSITIONS", 20),
        start_balance_sol=get_float(env, "START_BALANCE_SOL", 4.0),
        shadow_state_file=get(env, "SHADOW_STATE_FILE", "shadow_book.json"),
        status_every_min=get_float(env, "STATUS_EVERY_MIN", 30.0),
        watch_poll_s=get_float(env, "WATCH_POLL_S", 45.0),
        watch_min_buy_usd=get_float(env, "WATCH_MIN_BUY_USD", 50.0),
        watch_consensus_wallets=get_int(env, "WATCH_CONSENSUS_WALLETS", 2),
        watch_consensus_window_s=get_float(env, "WATCH_CONSENSUS_WINDOW_S", 600.0),
        watch_first_lookback_s=get_float(env, "WATCH_FIRST_LOOKBACK_S", 90.0),
        open_gap_s=get_float(env, "OPEN_GAP_S", 20.0),
        # discovery (batch wallet finder)
        discover_max_tokens=get_int(env, "DISCOVER_MAX_TOKENS", 25),
        discover_max_wallets=get_int(env, "DISCOVER_MAX_WALLETS", 40),
        discover_early_window_s=get_float(env, "DISCOVER_EARLY_WINDOW_S", 1800.0),
        discover_tx_per_pool=get_int(env, "DISCOVER_TX_PER_POOL", 60),
        discover_min_buy_usd=get_float(env, "DISCOVER_MIN_BUY_USD", 50.0),
        discover_max_buy_usd=get_float(env, "DISCOVER_MAX_BUY_USD", 1_000_000.0),
        discover_out_file=get(env, "DISCOVER_OUT_FILE", "smart_money_wallets.json"),
        discover_pump_pct=get_float(env, "DISCOVER_PUMP_PCT", 100.0),
        discover_enrich=get_bool(env, "DISCOVER_ENRICH", True),
        dexscreener_base_url=get(env, "DEXSCREENER_BASE_URL",
                                 "https://api.dexscreener.com"),
        dexscreener_rpm=get_int(env, "DEXSCREENER_RPM", 300),
    )
