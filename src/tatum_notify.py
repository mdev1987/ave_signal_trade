"""Tatum Notifications v4 client — push-based smart-wallet detection.

Creates one ADDRESS_EVENT subscription per tracked wallet (solana-mainnet).
Idempotent: existing type+chain+address+url alerts are skipped.

Credit math (free plan = 1M credits):
  50 credits/day standby per alert + 50 per fired webhook. 30 wallets ≈
  1,500/day standby — negligible vs polling cost.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE = "https://api.tatum.io"


class TatumNotifications:
    def __init__(self, api_key: str, webhook_url: str,
                 chain: str = "solana-mainnet") -> None:
        self.key = api_key.strip()
        self.webhook_url = webhook_url
        self.chain = chain
        self._h = {"x-api-key": self.key, "accept": "application/json"}
        self._c = httpx.Client(timeout=20)

    # ------------------------------------------------------------------ core
    def _req(self, method: str, path: str, **kw) -> Any:
        r = self._c.request(method, BASE + path, headers=self._h, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"tatum {method} {path}: {r.status_code} {r.text[:120]}")
        try:
            return r.json()
        except Exception:
            return r.text

    def list_subscriptions(self, page_size: int = 50) -> list[dict]:
        out, page = [], 1
        while True:
            j = self._req("GET", "/v4/subscription",
                          params={"pageSize": min(50, page_size), "page": page})
            rows = j if isinstance(j, list) else []
            out += rows
            if len(rows) < page_size or page > 40:
                break
            page += 1
        return out

    def existing_address_alerts(self) -> dict[str, dict]:
        """address -> subscription row (solana-mainnet ADDRESS_EVENT only)."""
        res: dict[str, dict] = {}
        for s_ in self.list_subscriptions():
            attr = s_.get("attr") or {}
            if (s_.get("type") == "ADDRESS_EVENT"
                    and attr.get("chain") == self.chain):
                addr = str(attr.get("address") or "")
                if addr:
                    res[addr] = s_
        return res

    def create_address_alert(self, address: str) -> str | None:
        j = self._req("POST", "/v4/subscription", json={
            "type": "ADDRESS_EVENT",
            "attr": {"chain": self.chain, "address": address,
                     "url": self.webhook_url},
        })
        return (j or {}).get("id")

    def delete_subscription(self, sub_id: str) -> None:
        self._req("DELETE", f"/v4/subscription/{sub_id}")

    def ensure_subscriptions(self, wallets: list[str]) -> tuple[int, int]:
        """Create missing alerts. Returns (created, already_present)."""
        have = self.existing_address_alerts()
        created = present = 0
        for w in wallets:
            if w in have:
                present += 1
                continue
            try:
                self.create_address_alert(w)
                created += 1
            except Exception as e:  # noqa: BLE001
                msg = str(e)
                if "exists" in msg or "unique" in msg:
                    present += 1
                else:
                    logger.warning("tatum subscribe %s failed: %s", w[:10], msg[:100])
        logger.info("tatum subscriptions: %d created, %d present",
                    created, present)
        return created, present

    def remove_all_address_alerts(self) -> int:
        n = 0
        for addr, row in self.existing_address_alerts().items():
            try:
                self.delete_subscription(row["id"])
                n += 1
            except Exception:  # noqa: BLE001
                pass
        return n
