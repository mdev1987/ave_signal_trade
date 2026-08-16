"""Configuration loading and tgdata session setup.

Reads ``.env`` (mixed ``KEY=value`` and ``KEY: value`` separators), resolves
Telegram API credentials, and generates the ``config.ini`` tgdata needs to
authenticate a user session. The phone number is prompted interactively on
first run (and persisted back to ``.env``).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
TGDATA_CONFIG = PROJECT_ROOT / "config.ini"
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


@dataclass(frozen=True)
class Settings:
    """Every tunable knob, defaulted and overridable via ``.env``.

    Instantiate with :func:`load_settings` so a single env parse serves the
    whole process. All values fall back to sane defaults when unset.
    """

    # --- trading ---------------------------------------------------------
    position_size_sol: float = 0.1
    start_balance_sol: float = 10.0
    take_profit: float = 4.0
    stop_loss: float = 0.3
    timeout_s: float = 3600.0
    shutdown_grace_s: int = 60
    channel: str = "@AveSolanaTokenScanner"
    checkpoint_file: str = "paper_positions.json"
    backfill_limit: int = 200
    # Entry guards (wired from .env; the channel's own snapshot is used for
    # the mcap/liq filters — these gates run at arm/entry time):
    min_liquidity_usd: float = 4000.0      # reject signals below this liq
    entry_latency_s: float = 2.0           # wait after signal before entry
    liq_confirm_window_s: float = 10.0     # how long to retry the DexPaprika check
    max_entry_mult: float = 5.0            # skip entry if price already > N x init
    max_entry_peak_pct: float = 0.0        # skip if entry price is above init by %
    pool_check_enabled: bool = True        # DexPaprika liquidity/survival gate
    dex_paprika_key: str = ""
    dex_paprika_base_url: str = "https://api.dexpaprika.com"
    helius_api_keys: str = ""              # comma-separated, rotated on 429
    helius_base_url: str = "https://mainnet.helius-rpc.com"
    dev_rep_enabled: bool = True           # Helius creator/reputation veto
    dev_rep_max_creates_24h: int = 3
    dev_rep_min_age_hours: float = 0.0
    dev_rep_cache_ttl_min: float = 10.0
    dev_rep_timeout_s: float = 2.5

    # --- jupiter ---------------------------------------------------------
    dry_run: bool = True
    private_key: str = ""
    jupiter_api_key: str = ""
    jupiter_base_url: str = "https://api.jup.ag"
    jupiter_slippage_bps: int = 500
    jupiter_max_impact_pct: float = 15.0
    jupiter_quote_retries: int = 3
    jupiter_quote_cache_s: float = 30.0
    jupiter_quote_throttle_s: float = 1.0
    jupiter_quote_retry_delay_s: float = 1.0
    sell_slippage_escalation: tuple[int, ...] = (200, 300, 500, 1000)

    # --- price feed ------------------------------------------------------
    pumpapi_wss: str = "wss://stream.pumpapi.io/"
    pumpapi_reconnect_s: float = 3.0
    price_wait_timeout_s: float = 30.0

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
            position_size_sol=get_float(env, "POSITION_SIZE_SOL", 0.1),
            start_balance_sol=get_float(env, "START_BALANCE_SOL", 10.0),
            take_profit=get_float(env, "TAKE_PROFIT", 4.0),
            stop_loss=get_float(env, "STOP_LOSS", 0.3),
            timeout_s=get_float(env, "TIMEOUT_S", 3600.0),
            shutdown_grace_s=get_int(env, "SHUTDOWN_GRACE_S", 60),
            channel=get(env, "TELEGRAM_CHANNEL", "@AveSolanaTokenScanner"),
            checkpoint_file=get(env, "CHECKPOINT_FILE", "paper_positions.json"),
            backfill_limit=get_int(env, "BACKFILL_LIMIT", 200),
            min_liquidity_usd=get_float(env, "MIN_LIQUIDITY_USD", 4000.0),
            entry_latency_s=get_float(env, "ENTRY_LATENCY_S", 2.0),
            liq_confirm_window_s=get_float(env, "LIQ_CONFIRM_WINDOW_S", 10.0),
            max_entry_mult=get_float(env, "MAX_ENTRY_MULT", 5.0),
            max_entry_peak_pct=get_float(env, "MAX_ENTRY_PEAK_PCT", 0.0),
            pool_check_enabled=get_bool(env, "POOL_CHECK_ENABLED", True),
            dex_paprika_key=get(env, "DEX_PAPRIKA_KEY", ""),
            dex_paprika_base_url=get(env, "DEX_PAPRIKA_REST_URL", "https://api.dexpaprika.com"),
            helius_api_keys=get(env, "HELIUS_API_KEYS", get(env, "HELIUS_API_KEY", "")),
            helius_base_url=get(env, "HELIUS_BASE_URL", "https://mainnet.helius-rpc.com"),
            dev_rep_enabled=get_bool(env, "DEV_REP_ENABLED", True),
            dev_rep_max_creates_24h=get_int(env, "DEV_REP_MAX_CREATES_24H", 3),
            dev_rep_min_age_hours=get_float(env, "DEV_REP_MIN_AGE_HOURS", 0.0),
            dev_rep_cache_ttl_min=get_float(env, "DEV_REP_CACHE_TTL_MIN", 10.0),
            dev_rep_timeout_s=get_float(env, "DEV_REP_TIMEOUT_S", 2.5),
            dry_run=get_bool(env, "DRY_RUN", True),
            private_key=get(env, "PRIVATE_KEY", ""),
            jupiter_api_key=get(env, "JUPITER_API_KEY", ""),
            jupiter_base_url=get(env, "JUPITER_BASE_URL", "https://api.jup.ag"),
            jupiter_slippage_bps=get_int(env, "JUPITER_SLIPPAGE_BPS", 500),
            jupiter_max_impact_pct=get_float(env, "JUPITER_MAX_IMPACT_PCT", 15.0),
            jupiter_quote_retries=get_int(env, "JUPITER_QUOTE_RETRIES", 3),
            jupiter_quote_cache_s=get_float(env, "JUPITER_QUOTE_CACHE_S", 30.0),
            jupiter_quote_throttle_s=get_float(env, "JUPITER_QUOTE_THROTTLE_S", 1.0),
            jupiter_quote_retry_delay_s=get_float(env, "JUPITER_QUOTE_RETRY_DELAY_S", 1.0),
            sell_slippage_escalation=get_csv_ints(
                env, "SELL_SLIPPAGE_ESCALATION", (200, 300, 500, 1000)
            ),
            pumpapi_wss=get(env, "PUMPAPI_WSS", "wss://stream.pumpapi.io/"),
            pumpapi_reconnect_s=get_float(env, "PUMPAPI_RECONNECT_S", 3.0),
            price_wait_timeout_s=get_float(env, "PRICE_WAIT_TIMEOUT_S", 30.0),
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
            ("TAKE_PROFIT", f"{self.take_profit:g}"),
            ("STOP_LOSS", f"{self.stop_loss:g}"),
            ("TIMEOUT_S", f"{self.timeout_s:g}"),
            ("SHUTDOWN_GRACE_S", str(self.shutdown_grace_s)),
            ("TELEGRAM_CHANNEL", self.channel),
            ("CHECKPOINT_FILE", self.checkpoint_file),
            ("BACKFILL_LIMIT", str(self.backfill_limit)),
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
            ("SELL_SLIPPAGE_ESCALATION", ",".join(str(v) for v in self.sell_slippage_escalation)),
            ("PUMPAPI_WSS", self.pumpapi_wss),
            ("PUMPAPI_RECONNECT_S", f"{self.pumpapi_reconnect_s:g}"),
            ("PRICE_WAIT_TIMEOUT_S", f"{self.price_wait_timeout_s:g}"),
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


def prompt_phone(env: dict[str, str]) -> str:
    """Prompt for a Telegram phone number if it isn't already configured.

    Checks, in order: ``.env``'s ``TELEGRAM_PHONE``, then the phone already
    recorded in an existing ``config.ini``, then the terminal prompt. The
    chosen value is written back to ``.env`` under ``TELEGRAM_PHONE`` so the
    prompt only happens once.

    Returns:
        The configured phone number in full international format.
    """
    phone = get(env, "TELEGRAM_PHONE")
    if not phone:
        phone = _config_phone()
    if not phone:
        phone = input("Telegram phone number (full, e.g. +905064004949): ").strip()
    save_env = []
    if ENV_PATH.exists():
        save_env = ENV_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    save_env.append(f"TELEGRAM_PHONE: {phone}\n")
    ENV_PATH.write_text("".join(save_env), encoding="utf-8")
    return phone


def _config_phone() -> str:
    """Read the phone number from an existing config.ini, if any."""
    import configparser

    if not TGDATA_CONFIG.exists():
        return ""
    parser = configparser.ConfigParser()
    try:
        parser.read(TGDATA_CONFIG)
    except configparser.Error:
        return ""
    section = parser["Telegram"] if parser.has_section("Telegram") else None
    if section is None:
        return ""
    return section.get("phone", "").strip()


def write_tgdata_config(api_id: str, api_hash: str, phone: str) -> Path:
    """Write the ``config.ini`` that tgdata reads for authentication.

    Args:
        api_id: Telegram API id from my.telegram.org/apps.
        api_hash: Telegram API hash from my.telegram.org/apps.
        phone: Full phone number including country code.

    Returns:
        Path to the written config file.
    """
    session = SESSION_FILE.as_posix()
    TGDATA_CONFIG.write_text(
        f"[Telegram]\n"
        f"api_id = {api_id}\n"
        f"api_hash = {api_hash}\n"
        f"phone = {phone}\n"
        f"session_file = {session}\n",
        encoding="utf-8",
    )
    return TGDATA_CONFIG


def resolve_tgdata_config() -> Path:
    """Ensure tgdata can authenticate and return its config path.

    Reuses an already-valid ``config.ini`` (correct api_id/api_hash + a phone)
    whenever possible so an authenticated session is never re-prompted. Only
    when a piece is missing does it read ``.env``, fall back to the phone in an
    existing config, or prompt the terminal.

    Returns:
        Path to the tgdata ``config.ini``.
    """
    env = load_env()
    api_id = get(env, "TELEGRAM_API_ID")
    api_hash = get(env, "TELEGRAM_API_HASH")
    if not api_id or not api_hash:
        raise SystemExit(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH missing from .env — get them "
            "from https://my.telegram.org/apps"
        )

    if _config_is_valid(api_id, api_hash):
        return TGDATA_CONFIG

    phone = prompt_phone(env)
    return write_tgdata_config(api_id, api_hash, phone)


def _config_is_valid(api_id: str, api_hash: str) -> bool:
    """True when config.ini already carries the right credentials + a phone."""
    import configparser

    if not TGDATA_CONFIG.exists():
        return False
    parser = configparser.ConfigParser()
    try:
        parser.read(TGDATA_CONFIG)
    except configparser.Error:
        return False
    if not parser.has_section("Telegram"):
        return False
    section = parser["Telegram"]
    return (
        section.get("api_id", "").strip() == api_id
        and section.get("api_hash", "").strip() == api_hash
        and bool(section.get("phone", "").strip())
    )