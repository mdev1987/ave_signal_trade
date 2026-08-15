"""Configuration loading and tgdata session setup.

Reads ``.env`` (mixed ``KEY=value`` and ``KEY: value`` separators), resolves
Telegram API credentials, and generates the ``config.ini`` tgdata needs to
authenticate a user session. The phone number is prompted interactively on
first run (and persisted back to ``.env``).
"""

from __future__ import annotations

import os
import re
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