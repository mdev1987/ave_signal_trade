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


@dataclass(frozen=True)
class Settings:
    # telegram notify
    bot_token: str = ""
    chat_id: str = ""
    # strategy
    size_sol: float = 0.05
    trail_retrace_pct: float = 0.35
    hard_stop_pct: float = 0.40
    trail_start_mult: float = 1.30     # only trail after a peak >= this
    tp1_mult: float = 1.50             # bank 50% at this multiple (was 2.0)
    open_min_wallets: int = 2           # consensus gate for OPENS (design: >=2)
    open_min_liq_usd: float = 5000.0    # skip illiquid tokens (exit slippage)
    start_balance_sol: float = 2.0
    shadow_state_file: str = "shadow_book.json"
    status_every_min: float = 30.0
    # watcher
    watch_poll_s: float = 45.0
    watch_min_buy_usd: float = 50.0
    watch_consensus_wallets: int = 2
    watch_first_lookback_s: float = 90.0
    open_gap_s: float = 20.0          # min seconds between new positions (anti-flood)
    # discovery (batch wallet finder)
    discover_max_tokens: int = 25
    discover_max_wallets: int = 40
    discover_early_window_s: float = 1800.0
    discover_tx_per_pool: int = 60
    discover_min_buy_usd: float = 50.0
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
        trail_retrace_pct=get_float(env, "TRAIL_RETRACE_PCT", 0.35),
        hard_stop_pct=get_float(env, "HARD_STOP_PCT", 0.40),
        trail_start_mult=get_float(env, "TRAIL_START_MULT", 1.30),
        tp1_mult=get_float(env, "TP1_MULT", 1.50),
        open_min_wallets=get_int(env, "OPEN_MIN_WALLETS", 2),
        open_min_liq_usd=get_float(env, "OPEN_MIN_LIQ_USD", 5000.0),
        start_balance_sol=get_float(env, "START_BALANCE_SOL", 2.0),
        shadow_state_file=get(env, "SHADOW_STATE_FILE", "shadow_book.json"),
        status_every_min=get_float(env, "STATUS_EVERY_MIN", 30.0),
        watch_poll_s=get_float(env, "WATCH_POLL_S", 45.0),
        watch_min_buy_usd=get_float(env, "WATCH_MIN_BUY_USD", 50.0),
        watch_consensus_wallets=get_int(env, "WATCH_CONSENSUS_WALLETS", 2),
        watch_first_lookback_s=get_float(env, "WATCH_FIRST_LOOKBACK_S", 90.0),
        open_gap_s=get_float(env, "OPEN_GAP_S", 20.0),
        # discovery (batch wallet finder)
        discover_max_tokens=get_int(env, "DISCOVER_MAX_TOKENS", 25),
        discover_max_wallets=get_int(env, "DISCOVER_MAX_WALLETS", 40),
        discover_early_window_s=get_float(env, "DISCOVER_EARLY_WINDOW_S", 1800.0),
        discover_tx_per_pool=get_int(env, "DISCOVER_TX_PER_POOL", 60),
        discover_min_buy_usd=get_float(env, "DISCOVER_MIN_BUY_USD", 50.0),
        discover_out_file=get(env, "DISCOVER_OUT_FILE", "smart_money_wallets.json"),
        discover_pump_pct=get_float(env, "DISCOVER_PUMP_PCT", 100.0),
        discover_enrich=get_bool(env, "DISCOVER_ENRICH", True),
        dexscreener_base_url=get(env, "DEXSCREENER_BASE_URL",
                                 "https://api.dexscreener.com"),
        dexscreener_rpm=get_int(env, "DEXSCREENER_RPM", 300),
    )
