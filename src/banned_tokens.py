"""Local banned token list — persistent ban storage.

Maintains a local JSON file of banned token addresses with reasons and
timestamps. Used as a fallback when Jupiter's banned token list DNS fails,
and for operator-defined bans.

The file is loaded at startup and saved after each modification.
Bans can expire after a configurable TTL (default: 7 days).
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BAN_FILE = "banned_tokens.json"
DEFAULT_TTL_S = 7 * 24 * 3600  # 7 days


class LocalBanList:
    """Local banned token list with persistence and TTL."""

    def __init__(
        self,
        ban_file: str | Path = DEFAULT_BAN_FILE,
        ttl_s: float = DEFAULT_TTL_S,
    ) -> None:
        self.ban_file = Path(ban_file)
        self.ttl_s = ttl_s
        self._bans: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        """Load bans from file."""
        if not self.ban_file.exists():
            return
        try:
            data = json.loads(self.ban_file.read_text())
            if isinstance(data, dict):
                self._bans = data
                # Prune expired bans on load
                now = time.time()
                expired = [
                    addr for addr, info in self._bans.items()
                    if info.get("expires_at", 0) < now
                ]
                for addr in expired:
                    del self._bans[addr]
                if expired:
                    logger.info("banned_tokens: pruned %d expired bans", len(expired))
                    self._save()
        except Exception as exc:  # noqa: BLE001
            logger.warning("banned_tokens: failed to load %s: %s", self.ban_file, exc)

    def _save(self) -> None:
        """Save bans to file."""
        try:
            self.ban_file.write_text(json.dumps(self._bans, indent=2))
        except Exception as exc:  # noqa: BLE001
            logger.warning("banned_tokens: failed to save: %s", exc)

    def is_banned(self, mint: str) -> bool:
        """Check if a token is banned."""
        ban = self._bans.get(mint)
        if not ban:
            return False
        # Check expiration
        expires_at = ban.get("expires_at", 0)
        if expires_at < time.time():
            del self._bans[mint]
            self._save()
            return False
        return True

    def ban(
        self,
        mint: str,
        reason: str = "manual",
        ttl_s: float | None = None,
    ) -> None:
        """Ban a token with a reason and optional TTL."""
        ttl = ttl_s if ttl_s is not None else self.ttl_s
        self._bans[mint] = {
            "reason": reason,
            "banned_at": time.time(),
            "expires_at": time.time() + ttl,
        }
        self._save()
        logger.info("banned_tokens: banned %s (%s, ttl=%dh)", mint[:8], reason, ttl // 3600)

    def unban(self, mint: str) -> bool:
        """Unban a token. Returns True if it was banned."""
        if mint in self._bans:
            del self._bans[mint]
            self._save()
            logger.info("banned_tokens: unbanned %s", mint[:8])
            return True
        return False

    def list_bans(self) -> dict[str, dict[str, Any]]:
        """Return all active bans."""
        now = time.time()
        return {
            addr: info for addr, info in self._bans.items()
            if info.get("expires_at", 0) > now
        }

    def count(self) -> int:
        """Return number of active bans."""
        now = time.time()
        return sum(
            1 for info in self._bans.values()
            if info.get("expires_at", 0) > now
        )
