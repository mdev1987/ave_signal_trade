"""Configuration loading and Telegram session setup.

Reads ``.env`` (mixed ``KEY=value`` and ``KEY: value`` separators), resolves
Telegram API credentials, and keeps a Telethon session file so an authorized
session is reused without re-prompting. The phone number is prompted
interactively on first run (and persisted back to ``.env``).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
SESSION_FILE = PROJECT_ROOT / "telegram_session"

_ENV_LINE = re.compile(r"^\s*([A-Z0-9_]+)\s*[:=]\s*(.*?)\s*$")


def load_env(path: Path = ENV_PATH) -> dict[str, str]:
    """Parse a ``.env`` file into a dict, handling both ``KEY=value`` and
    ``KEY: value`` separators and skipping blank/comment lines.

    Returns:
        Mapping of environment variable name to its value.
    """
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE.match(line)
        if m:
            env[m.group(1)] = m.group(2)
    return env


def get(env: dict[str, str], key: str, default: str = "") -> str:
    """Read a value from the env dict, falling back to the OS environment."""
    return env.get(key, os.environ.get(key, default))


def get_float(env: dict[str, str], key: str, default: float) -> float:
    """Read a float env value, falling back to ``default`` on parse failure."""
    try:
        return float(get(env, key, ""))
    except (TypeError, ValueError):
        return default


def get_int(env: dict[str, str], key: str, default: int) -> int:
    """Read an int env value, falling back to ``default`` on parse failure."""
    try:
        return int(float(get(env, key, "")))
    except (TypeError, ValueError):
        return default


def get_bool(env: dict[str, str], key: str, default: bool) -> bool:
    """Read a boolean env value (true/1/yes/on), falling back to default."""
    raw = get(env, key, "").strip().lower()
    if not raw:
        return default
    return raw not in ("0", "false", "no", "off")


def get_csv_ints(env: dict[str, str], key: str, default: tuple[int, ...]) -> tuple[int, ...]:
    """Read a comma-separated int list, falling back to default on failure."""
    raw = get(env, key, "").strip()
    if not raw:
        return default
    try:
        values = tuple(int(float(x.strip())) for x in raw.split(",") if x.strip())
        return values if values else default
    except (TypeError, ValueError):
        return default


def get_channels(
    env: dict[str, str],
    default: tuple[str, ...] = ("@DRBTSolanaPF",),
) -> tuple[str, ...]:
    """Read the signal channels as a comma-separated list.

    ``TELEGRAM_CHANNELS`` wins; the legacy single-channel ``TELEGRAM_CHANNEL``
    is still honored for old ``.env`` files.
    """
    raw = get(env, "TELEGRAM_CHANNELS", "") or get(env, "TELEGRAM_CHANNEL", "")
    values = tuple(v.strip() for v in raw.split(",") if v.strip())
    return values or default


@dataclass(frozen=True)
class Settings:
    """Every tunable knob, defaulted and overridable via ``.env``.

    Instantiate with :func:`load_settings` so a single env parse serves the
    whole process. All values fall back to sane defaults when unset.
    """

    # --- trading ---------------------------------------------------------
    position_size_sol: float = 0.2  # single POS focused sizing (profitable mode)
    start_balance_sol: float = 2.0
    max_positions: int = 1  # 1 POS — serial execution, avoids dilution & rug clustering
    take_profit: float = 4.0
    stop_loss: float = 0.3
    timeout_s: float = 1500.0  # 25min optimum: EV ~+99.6% ≈60min (+101%), frees slot 2.4× faster
    shutdown_grace_s: int = 60
    health_timeout_s: int = 300   # no sweep progress this long -> force exit
    checkpoint_save_s: float = 300.0  # periodic checkpoint flush (positions)
    # Signal sources in preference order: on a CA tie (same-second posts from
    # both channels during backfill) the earlier channel's signal wins dedup.
    channels: tuple[str, ...] = ("@DRBTSolanaPF",)
    checkpoint_file: str = "paper_positions.json"
    backfill_limit: int = 200
    # Price sanity + staleness guards (entry/exit marks):
    min_entry_px: float = 1e-11       # reject entry prices below this (dust)
    max_entry_px: float = 1e-3        # reject entry prices above this (junk)
    price_stale_s: float = 120.0      # last tick older than this is stale
    timeout_stale_grace_s: float = 300.0  # max extra wait for a fresh tick
    max_tick_mult: float = 1e5        # ticks beyond this multiple of entry are noise
    # Entry guards (wired from .env; the channel's own snapshot is used for
    # the mcap/liq filters — these gates run at arm/entry time):
    min_liquidity_usd: float = 4000.0      # reject signals below this liq
    entry_latency_s: float = 2.0           # wait after signal before entry
    # Armed signals older than this are never armed/entered (Ave reposts
    # hours-old pools during backfill; they can only pollute the funnel).
    # 0 disables.
    entry_max_age_s: float = 300.0
    # Copycat-CA resolution when a DRBT post's metadata links reference a
    # different pump token (see parser.alt_cas): "link" trades the referenced
    # original, "skip" rejects ambiguous posts outright, "mint" keeps the
    # posted Mint address (journal-only warning).
    ca_mismatch_policy: str = "link"
    # DexPaprika requests/minute cap (free tier: 15 keyless, 30 with key).
    dexpaprika_rpm: int = 14
    liq_confirm_window_s: float = 10.0     # how long to retry the DexPaprika check
    # Curve-phase fallback: admit `...pump` mints with no indexed external
    # pool yet when the PumpAPI stream shows fresh trading activity (oracle 2)
    # or DexScreener has an active pair (oracle 3). Bonding-curve liquidity is
    # mathematical pre-graduation, so activity is sufficient evidence.
    pool_curve_fallback: bool = True
    curve_stream_max_age_s: float = 90.0   # stream state older than this is stale
    # DexScreener REST oracle (docs: 60 req/min public tier; raise for higher)
    dexscreener_enabled: bool = True
    dexscreener_base_url: str = "https://api.dexscreener.com"
    dexscreener_rpm: int = 60
    max_entry_mult: float = 5.0            # skip entry if price already > N x init
    max_entry_peak_pct: float = 0.0        # skip if entry price is above init by %
    pool_check_enabled: bool = True        # DexPaprika liquidity/survival gate
    dex_paprika_key: str = ""
    dex_paprika_base_url: str = "https://api.dexpaprika.com"
    helius_api_keys: str = ""              # comma-separated, rotated on 429
    helius_base_url: str = "https://beta.helius-rpc.com"
    dev_rep_enabled: bool = True           # Helius creator/reputation veto
    dev_rep_max_creates_24h: int = 3
    dev_rep_min_age_hours: float = 0.0
    dev_rep_cache_ttl_min: float = 10.0
    dev_rep_timeout_s: float = 2.5
    # RugCheck pre-trade gate (arm-time token security veto). Fail-open by
    # default: sec-0 snipes race RugCheck's indexer, so a missing report must
    # not reject — only an explicit danger risk (e.g. "Large Amount of LP
    # Unlocked", the signature of the 2026-08-20 live LP-pull rugs) vetoes.
    rugcheck_enabled: bool = True
    rugcheck_api_key: str = ""
    rugcheck_base_url: str = "https://api.rugcheck.xyz"
    rugcheck_veto_risks: tuple[str, ...] = ("lp unlocked",)
    rugcheck_max_score: float = 0.0        # score_normalised ceiling; 0 = off
    rugcheck_timeout_s: float = 2.0
    rugcheck_cache_ttl_s: float = 120.0
    rugcheck_fail_closed: bool = False
    # Serial-relaunch damper: same token name on >= N distinct CAs within the
    # window => reject (NEX Ai x5 / 牛来 x20 relaunch farms, 2026-08-20 rugs).
    scam_damper_enabled: bool = True
    scam_damper_max_cas: int = 3
    scam_damper_window_min: float = 360.0
    # PumpAPI pool-state vetoes (zero latency, parsed from the stream we
    # already subscribe to). A liquidity REMOVAL seen within the veto window
    # rejects outright; burned-LP below MIN_BURNED_LIQ_PCT rejects when >0
    # (0 = log-only until measured — pumpapi docs flag 0% as rug-able).
    liq_remove_veto_s: float = 120.0
    min_burned_liq_pct: float = 0.0
    # Post-entry early warning: quote-side reserves dropping this many percent
    # within the window after entry force an immediate exit ("liq_collapse")
    # instead of riding to the timeout writeoff at ~zero (NASA failure mode).
    # 0 disables.
    liq_collapse_pct: float = 60.0
    liq_collapse_window_s: float = 180.0
    # Named filter regime (L1_PRODUCTION / L2_EXPERIMENT) so the two arms'
    # statistics are never mixed; explicit FILTER_* values still override.
    filter_profile: str = ""
    # Dev-reputation gate mode: "warn" journals + admits (collect samples
    # before letting it hard-reject); "reject" vetoes fail-closed.
    dev_rep_mode: str = "warn"

    # --- jupiter ---------------------------------------------------------
    dry_run: bool = True
    private_key: str = ""
    jupiter_api_key: str = ""
    jupiter_base_url: str = "https://api.jup.ag"
    jupiter_slippage_bps: int = 300
    jupiter_max_impact_pct: float = 5.0
    jupiter_quote_retries: int = 3
    jupiter_quote_cache_s: float = 30.0
    jupiter_quote_throttle_s: float = 1.0
    jupiter_quote_retry_delay_s: float = 1.0
    jupiter_quote_cache_max: int = 500
    jupiter_order_timeout_s: float = 20.0
    jupiter_execute_timeout_s: float = 60.0
    rpc_timeout_s: float = 12.0
    rpc_key_cooldown_s: float = 60.0
    sell_slippage_escalation: tuple[int, ...] = (200, 300, 500, 1000)
    # Sell robustness: a drained pool keeps failing to quote forever. After
    # ``max_sell_failures`` consecutive failures the position is written off
    # (closed at its last mark, slot freed, Telegram alert) instead of
    # retrying every sweep and occupying a position slot indefinitely.
    max_sell_failures: int = 6
    # Positions past their timeout exit have already sat for the whole hold
    # window, so a dead pool there is worth writing off much sooner: this many
    # consecutive sell failures (instead of ``max_sell_failures``) end the
    # position. Frees the slot faster without risking premature writeoffs of
    # young positions that may still recover.
    max_sell_failures_timeout: int = 3
    sell_backoff_s: float = 60.0  # min wait between failed sell retries
    # --- two-sided execution gate (CATE/ELON defense) -----------------------
    require_sell_quote: bool = True
    max_sell_impact_pct: float = 5.0
    # Quote stability gate: a quote that moves > threshold within ~1s is
    # rejected. CATE showed slippage failure before execution – instability
    # is a rug signal, not just an execution nuisance.
    quote_stability_checks: int = 3
    quote_stability_interval_ms: int = 300
    max_quote_change_pct: float = 5.0
    max_impact_change_pct: float = 3.0
    # Trailing stop: once the peak reaches ``trail_activate_mult`` x entry, a
    # stop is ratcheted to peak * (1 - trail_retrace_pct) so a winner that
    # reverses after the take-profit level is locked out with gains intact
    # instead of bleeding back to the timeout exit. 0 disables the trail.
    trail_activate_mult: float = 2.0
    trail_retrace_pct: float = 0.5
    # Paper fill simulation: fill entries at the real Jupiter quote price
    # (slippage + price impact) and mark exits with a simulated sell that
    # applies sell slippage, instead of filling at the raw feed tick.
    paper_fill_sim: bool = True

    # --- price feed ------------------------------------------------------
    pumpapi_wss: str = "wss://stream.pumpapi.io/"
    pumpapi_reconnect_s: float = 3.0
    price_wait_timeout_s: float = 30.0
    pumpapi_recv_timeout_s: float = 90.0  # wedged recv -> reconnect

    # --- telegram bot / session ------------------------------------------
    bot_token: str = ""
    chat_id: str = ""
    telegram_api_id: str = ""
    telegram_api_hash: str = ""
    telegram_phone: str = ""

    @classmethod
    def from_env(cls, env: dict[str, str]) -> Settings:
        """Build settings from a parsed env dict (see :func:`load_settings`)."""
        return cls(
            position_size_sol=get_float(env, "POSITION_SIZE_SOL", 0.2),
            start_balance_sol=get_float(env, "START_BALANCE_SOL", 2.0),
            max_positions=get_int(env, "MAX_POSITIONS", 1),
            take_profit=get_float(env, "TAKE_PROFIT", 4.0),
            stop_loss=get_float(env, "STOP_LOSS", 0.3),
            timeout_s=get_float(env, "TIMEOUT_S", 1500.0),
            shutdown_grace_s=get_int(env, "SHUTDOWN_GRACE_S", 60),
            health_timeout_s=get_int(env, "HEALTH_TIMEOUT_S", 300),
            checkpoint_save_s=get_float(env, "CHECKPOINT_SAVE_S", 300.0),
            channels=get_channels(env),
            checkpoint_file=get(env, "CHECKPOINT_FILE", "paper_positions.json"),
            backfill_limit=get_int(env, "BACKFILL_LIMIT", 200),
            min_entry_px=get_float(env, "MIN_ENTRY_PX", 1e-11),
            max_entry_px=get_float(env, "MAX_ENTRY_PX", 1e-3),
            price_stale_s=get_float(env, "PRICE_STALE_S", 120.0),
            timeout_stale_grace_s=get_float(env, "TIMEOUT_STALE_GRACE_S", 300.0),
            max_tick_mult=get_float(env, "MAX_TICK_MULT", 1e5),
            min_liquidity_usd=get_float(env, "MIN_LIQUIDITY_USD", 4000.0),
            entry_latency_s=get_float(env, "ENTRY_LATENCY_S", 2.0),
            entry_max_age_s=get_float(env, "ENTRY_MAX_AGE_S", 300.0),
            ca_mismatch_policy=(
                lambda v: v if v in ("skip", "link", "mint") else "link"
            )(get(env, "CA_MISMATCH_POLICY", "link").strip().lower()),
            dexpaprika_rpm=get_int(env, "DEXPAPRIKA_RPM", 14),
            liq_confirm_window_s=get_float(env, "LIQ_CONFIRM_WINDOW_S", 10.0),
            pool_curve_fallback=get_bool(env, "POOL_CURVE_FALLBACK", True),
            curve_stream_max_age_s=get_float(env, "CURVE_STREAM_MAX_AGE_S", 90.0),
            dexscreener_enabled=get_bool(env, "DEXSCREENER_ENABLED", True),
            dexscreener_base_url=get(env, "DEXSCREENER_BASE_URL", "https://api.dexscreener.com"),
            dexscreener_rpm=get_int(env, "DEXSCREENER_RPM", 60),
            max_entry_mult=get_float(env, "MAX_ENTRY_MULT", 5.0),
            max_entry_peak_pct=get_float(env, "MAX_ENTRY_PEAK_PCT", 0.0),
            pool_check_enabled=get_bool(env, "POOL_CHECK_ENABLED", True),
            dex_paprika_key=get(env, "DEX_PAPRIKA_KEY", ""),
            dex_paprika_base_url=get(env, "DEX_PAPRIKA_REST_URL", "https://api.dexpaprika.com"),
            helius_api_keys=get(env, "HELIUS_API_KEYS", get(env, "HELIUS_API_KEY", "")),
            helius_base_url=get(env, "HELIUS_BASE_URL", "https://beta.helius-rpc.com"),
            dev_rep_enabled=get_bool(env, "DEV_REP_ENABLED", True),
            dev_rep_max_creates_24h=get_int(env, "DEV_REP_MAX_CREATES_24H", 3),
            dev_rep_min_age_hours=get_float(env, "DEV_REP_MIN_AGE_HOURS", 0.0),
            dev_rep_cache_ttl_min=get_float(env, "DEV_REP_CACHE_TTL_MIN", 10.0),
            dev_rep_timeout_s=get_float(env, "DEV_REP_TIMEOUT_S", 2.5),
            rugcheck_enabled=get_bool(env, "RUGCHECK_ENABLED", True),
            rugcheck_api_key=get(env, "RUGCHECK_API_KEY", ""),
            rugcheck_base_url=get(env, "RUGCHECK_BASE_URL", "https://api.rugcheck.xyz"),
            rugcheck_veto_risks=tuple(
                s.strip().lower()
                for s in get(env, "RUGCHECK_VETO_RISKS", "lp unlocked").split(",")
                if s.strip()
            ),
            rugcheck_max_score=get_float(env, "RUGCHECK_MAX_SCORE", 0.0),
            rugcheck_timeout_s=get_float(env, "RUGCHECK_TIMEOUT_S", 2.0),
            rugcheck_cache_ttl_s=get_float(env, "RUGCHECK_CACHE_TTL_S", 120.0),
            rugcheck_fail_closed=get_bool(env, "RUGCHECK_FAIL_CLOSED", False),
            scam_damper_enabled=get_bool(env, "SCAM_DAMPER_ENABLED", True),
            scam_damper_max_cas=get_int(env, "SCAM_DAMPER_MAX_CAS", 3),
            scam_damper_window_min=get_float(env, "SCAM_DAMPER_WINDOW_MIN", 360.0),
            liq_remove_veto_s=get_float(env, "LIQ_REMOVE_VETO_S", 120.0),
            min_burned_liq_pct=get_float(env, "MIN_BURNED_LIQ_PCT", 0.0),
            liq_collapse_pct=get_float(env, "LIQ_COLLAPSE_PCT", 60.0),
            liq_collapse_window_s=get_float(env, "LIQ_COLLAPSE_WINDOW_S", 180.0),
            filter_profile=get(env, "FILTER_PROFILE", ""),
            dev_rep_mode=get(env, "DEV_REP_MODE", "warn"),
            dry_run=get_bool(env, "DRY_RUN", True),
            private_key=get(env, "PRIVATE_KEY", ""),
            jupiter_api_key=get(env, "JUPITER_API_KEY", ""),
            jupiter_base_url=get(env, "JUPITER_BASE_URL", "https://api.jup.ag"),
            jupiter_slippage_bps=get_int(env, "JUPITER_SLIPPAGE_BPS", 300),
            jupiter_max_impact_pct=get_float(env, "JUPITER_MAX_IMPACT_PCT", 5.0),
            jupiter_quote_retries=get_int(env, "JUPITER_QUOTE_RETRIES", 3),
            jupiter_quote_cache_s=get_float(env, "JUPITER_QUOTE_CACHE_S", 30.0),
            jupiter_quote_throttle_s=get_float(env, "JUPITER_QUOTE_THROTTLE_S", 1.0),
            jupiter_quote_retry_delay_s=get_float(env, "JUPITER_QUOTE_RETRY_DELAY_S", 1.0),
            jupiter_quote_cache_max=get_int(env, "JUPITER_QUOTE_CACHE_MAX", 500),
            jupiter_order_timeout_s=get_float(env, "JUPITER_ORDER_TIMEOUT_S", 20.0),
            jupiter_execute_timeout_s=get_float(env, "JUPITER_EXECUTE_TIMEOUT_S", 60.0),
            rpc_timeout_s=get_float(env, "RPC_TIMEOUT_S", 12.0),
            rpc_key_cooldown_s=get_float(env, "RPC_KEY_COOLDOWN_S", 60.0),
            sell_slippage_escalation=get_csv_ints(
                env, "SELL_SLIPPAGE_ESCALATION", (200, 300, 500, 1000)
            ),
            max_sell_failures=get_int(env, "MAX_SELL_FAILURES", 6),
            max_sell_failures_timeout=get_int(env, "MAX_SELL_FAILURES_TIMEOUT", 3),
            sell_backoff_s=get_float(env, "SELL_BACKOFF_S", 60.0),
            require_sell_quote=get_bool(env, "REQUIRE_SELL_QUOTE", True),
            max_sell_impact_pct=get_float(env, "MAX_SELL_IMPACT_PCT", 5.0),
            quote_stability_checks=get_int(env, "QUOTE_STABILITY_CHECKS", 3),
            quote_stability_interval_ms=get_int(env, "QUOTE_STABILITY_INTERVAL_MS", 300),
            max_quote_change_pct=get_float(env, "MAX_QUOTE_CHANGE_PCT", 5.0),
            max_impact_change_pct=get_float(env, "MAX_IMPACT_CHANGE_PCT", 3.0),
            trail_activate_mult=get_float(env, "TRAIL_ACTIVATE_MULT", 2.0),
            trail_retrace_pct=get_float(env, "TRAIL_RETRACE_PCT", 0.5),
            paper_fill_sim=get_bool(env, "PAPER_FILL_SIM", True),
            pumpapi_wss=get(env, "PUMPAPI_WSS", "wss://stream.pumpapi.io/"),
            pumpapi_reconnect_s=get_float(env, "PUMPAPI_RECONNECT_S", 3.0),
            price_wait_timeout_s=get_float(env, "PRICE_WAIT_TIMEOUT_S", 30.0),
            pumpapi_recv_timeout_s=get_float(env, "PUMPAPI_RECV_TIMEOUT_S", 90.0),
            bot_token=get(env, "BOT_TOKEN", ""),
            chat_id=get(env, "CHAT_ID", ""),
            telegram_api_id=get(env, "TELEGRAM_API_ID", ""),
            telegram_api_hash=get(env, "TELEGRAM_API_HASH", ""),
            telegram_phone=get(env, "TELEGRAM_PHONE", ""),
        )

    def to_env(self) -> str:
        """Render as ``.env.example``-style text with current values."""
        pairs = [
            ("POSITION_SIZE_SOL", f"{self.position_size_sol:g}"),
            ("START_BALANCE_SOL", f"{self.start_balance_sol:g}"),
            ("MAX_POSITIONS", str(self.max_positions)),
            ("TAKE_PROFIT", f"{self.take_profit:g}"),
            ("STOP_LOSS", f"{self.stop_loss:g}"),
            ("TIMEOUT_S", f"{self.timeout_s:g}"),
            ("SHUTDOWN_GRACE_S", str(self.shutdown_grace_s)),
            ("HEALTH_TIMEOUT_S", str(self.health_timeout_s)),
            ("CHECKPOINT_SAVE_S", f"{self.checkpoint_save_s:g}"),
            ("TELEGRAM_CHANNELS", ",".join(self.channels)),
            ("CHECKPOINT_FILE", self.checkpoint_file),
            ("BACKFILL_LIMIT", str(self.backfill_limit)),
            ("MIN_ENTRY_PX", f"{self.min_entry_px:g}"),
            ("MAX_ENTRY_PX", f"{self.max_entry_px:g}"),
            ("CA_MISMATCH_POLICY", self.ca_mismatch_policy),
            ("DEXPAPRIKA_RPM", str(self.dexpaprika_rpm)),
            ("PRICE_STALE_S", f"{self.price_stale_s:g}"),
            ("TIMEOUT_STALE_GRACE_S", f"{self.timeout_stale_grace_s:g}"),
            ("MAX_TICK_MULT", f"{self.max_tick_mult:g}"),
            ("POOL_CURVE_FALLBACK", "true" if self.pool_curve_fallback else "false"),
            ("CURVE_STREAM_MAX_AGE_S", f"{self.curve_stream_max_age_s:g}"),
            ("DEXSCREENER_ENABLED", "true" if self.dexscreener_enabled else "false"),
            ("DEXSCREENER_BASE_URL", self.dexscreener_base_url),
            ("DEXSCREENER_RPM", str(self.dexscreener_rpm)),
            ("DRY_RUN", "true" if self.dry_run else "false"),
            ("PRIVATE_KEY", self.private_key),
            ("JUPITER_API_KEY", self.jupiter_api_key),
            ("JUPITER_BASE_URL", self.jupiter_base_url),
            ("JUPITER_SLIPPAGE_BPS", str(self.jupiter_slippage_bps)),
            ("JUPITER_MAX_IMPACT_PCT", f"{self.jupiter_max_impact_pct:g}"),
            ("JUPITER_QUOTE_RETRIES", str(self.jupiter_quote_retries)),
            ("JUPITER_QUOTE_CACHE_S", f"{self.jupiter_quote_cache_s:g}"),
            ("JUPITER_QUOTE_THROTTLE_S", f"{self.jupiter_quote_throttle_s:g}"),
            ("JUPITER_QUOTE_RETRY_DELAY_S", f"{self.jupiter_quote_retry_delay_s:g}"),
            ("JUPITER_QUOTE_CACHE_MAX", str(self.jupiter_quote_cache_max)),
            ("JUPITER_ORDER_TIMEOUT_S", f"{self.jupiter_order_timeout_s:g}"),
            ("JUPITER_EXECUTE_TIMEOUT_S", f"{self.jupiter_execute_timeout_s:g}"),
            ("RPC_TIMEOUT_S", f"{self.rpc_timeout_s:g}"),
            ("RPC_KEY_COOLDOWN_S", f"{self.rpc_key_cooldown_s:g}"),
            ("SELL_SLIPPAGE_ESCALATION", ",".join(str(v) for v in self.sell_slippage_escalation)),
            ("MAX_SELL_FAILURES", str(self.max_sell_failures)),
            ("SELL_BACKOFF_S", f"{self.sell_backoff_s:g}"),
            ("TRAIL_ACTIVATE_MULT", f"{self.trail_activate_mult:g}"),
            ("TRAIL_RETRACE_PCT", f"{self.trail_retrace_pct:g}"),
            ("PAPER_FILL_SIM", "true" if self.paper_fill_sim else "false"),
            ("PUMPAPI_WSS", self.pumpapi_wss),
            ("PUMPAPI_RECONNECT_S", f"{self.pumpapi_reconnect_s:g}"),
            ("PRICE_WAIT_TIMEOUT_S", f"{self.price_wait_timeout_s:g}"),
            ("PUMPAPI_RECV_TIMEOUT_S", f"{self.pumpapi_recv_timeout_s:g}"),
            ("RUGCHECK_ENABLED", "true" if self.rugcheck_enabled else "false"),
            ("RUGCHECK_API_KEY", self.rugcheck_api_key),
            ("RUGCHECK_BASE_URL", self.rugcheck_base_url),
            ("RUGCHECK_VETO_RISKS", ",".join(self.rugcheck_veto_risks)),
            ("RUGCHECK_MAX_SCORE", f"{self.rugcheck_max_score:g}"),
            ("RUGCHECK_TIMEOUT_S", f"{self.rugcheck_timeout_s:g}"),
            ("RUGCHECK_CACHE_TTL_S", f"{self.rugcheck_cache_ttl_s:g}"),
            ("RUGCHECK_FAIL_CLOSED", "true" if self.rugcheck_fail_closed else "false"),
            ("SCAM_DAMPER_ENABLED", "true" if self.scam_damper_enabled else "false"),
            ("SCAM_DAMPER_MAX_CAS", str(self.scam_damper_max_cas)),
            ("SCAM_DAMPER_WINDOW_MIN", f"{self.scam_damper_window_min:g}"),
            ("LIQ_REMOVE_VETO_S", f"{self.liq_remove_veto_s:g}"),
            ("MIN_BURNED_LIQ_PCT", f"{self.min_burned_liq_pct:g}"),
            ("LIQ_COLLAPSE_PCT", f"{self.liq_collapse_pct:g}"),
            ("LIQ_COLLAPSE_WINDOW_S", f"{self.liq_collapse_window_s:g}"),
            ("FILTER_PROFILE", self.filter_profile),
            ("DEV_REP_MODE", self.dev_rep_mode),
            ("BOT_TOKEN", self.bot_token),
            ("CHAT_ID", self.chat_id),
            ("TELEGRAM_API_ID", self.telegram_api_id),
            ("TELEGRAM_API_HASH", self.telegram_api_hash),
            ("TELEGRAM_PHONE", self.telegram_phone),
        ]
        return "\n".join(f"{k}: {v}" for k, v in pairs) + "\n"


def load_settings() -> Settings:
    """Parse ``.env`` once and build the process-wide :class:`Settings`."""
    return Settings.from_env(load_env())


@dataclass(frozen=True)
class TelegramCreds:
    """Telegram user-session credentials + the Telethon session file path."""

    api_id: str
    api_hash: str
    phone: str
    session_file: str


def resolve_telegram_creds() -> TelegramCreds:
    """Resolve Telegram credentials from ``.env`` (prompting once for phone).

    Reads ``TELEGRAM_API_ID`` / ``TELEGRAM_API_HASH`` from ``.env`` (required)
    and ``TELEGRAM_PHONE`` (optional — only needed on the very first run, when
    no authorized ``telegram_session.session`` exists yet). The chosen phone is
    persisted back to ``.env`` so the prompt only happens once.

    Returns:
        Credentials + the Telethon session file path.
    """
    env = load_env()
    api_id = get(env, "TELEGRAM_API_ID")
    api_hash = get(env, "TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        raise SystemExit(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH missing from .env — get them "
            "from https://my.telegram.org/apps"
        )
    phone = get(env, "TELEGRAM_PHONE")
    if not phone and not (SESSION_FILE.with_suffix(".session").exists()):
        phone = input(
            "Telegram phone number (full, e.g. +905064004949): "
        ).strip()
        if phone:
            save_env = []
            if ENV_PATH.exists():
                save_env = ENV_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
            save_env.append(f"TELEGRAM_PHONE={phone}\n")
            ENV_PATH.write_text("".join(save_env), encoding="utf-8")
    return TelegramCreds(
        api_id=api_id,
        api_hash=api_hash,
        phone=phone or "",
        session_file=SESSION_FILE.as_posix(),
    )