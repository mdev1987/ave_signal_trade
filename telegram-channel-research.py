#!/usr/bin/env python3
"""Telegram Channel Research — find best GMGN/solana signal channels.

Usage:
  # Step 1: discover channels (first run, saves session)
  uv run telegram-channel-research.py discover

  # Step 2: pull historical messages from candidate channels
  uv run telegram-channel-research.py pull

  # Step 3: score channels by signal quality
  uv run telegram-channel-research.py score

  # Step 4: live listen to top channels
  uv run telegram-channel-research.py listen
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tgdata import TgData

load_dotenv()

log = logging.getLogger("channel-research")

# ── paths ──────────────────────────────────────────────────────────────
CONFIG_INI = Path(__file__).parent / "tgdata_config.ini"
CHANNELS_JSON = Path(__file__).parent / "channel_candidates.json"
SIGNALS_DB = Path(__file__).parent / "channel_signals.json"

# ── known GMGN / solana signal channels to seed discovery ──────────────
SEED_CHANNELS = [
    # ── GMGN official channels ──────────────────────────────
    "pump_sol_alert",           # Pump Alert Channel
    "gmgnsignal",               # FDV Surge Alert
    "gmgnsignals",              # Solana Signal Alert
    "sollpburnt",               # Sol LP Burn Alert
    "solnewlp",                 # Solana New Pool Alert
    "GMGN_alert_bot",           # SOL wallet tracking (bot)
    "GMGN_ETH_alert_bot",       # ETH wallet tracking (bot)
    "GMGN_smartmoney_bot",      # Smart Wallet PNL query (bot)
    "gmgn_degencalls",          # Degen Calls Channel
    "gmgn_degensearch",         # Degen Search Channel
    "GMGNAI_bot",               # Group Contract Query (bot)
    "Alert_GMGNBOT",            # Group Smart Money Tracking (bot)
    # ── general solana signal channels ──────────────────────
    "solana_signals",
    "SolanaGems",
    "SolanaWhales",
    "KOLalerts",
    "SmartMoneyAlerts",
    "solgemscalxi",
    "pumpfunalerts",
    "TrojanOnSolana",
    "maestrobot",
    "traderxy",
]

# ── Solana CA regex ────────────────────────────────────────────────────
CA_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")
TG_LINK_RE = re.compile(r"t\.me/(\w+)")


def ensure_config() -> None:
    """Write tgdata config.ini from .env if missing."""
    if CONFIG_INI.exists():
        return
    api_id = os.getenv("API_ID", "")
    api_hash = os.getenv("API_HASH", "")
    phone = os.getenv("PHONE", "")
    if not all([api_id, api_hash, phone]):
        sys.exit("Missing API_ID / API_HASH / PHONE in .env")
    session_path = str(Path(__file__).parent / "gmgn_research")
    CONFIG_INI.write_text(
        f"[Telegram]\n"
        f"api_id = {api_id}\n"
        f"api_hash = {api_hash}\n"
        f"phone = {phone}\n"
        f"session_file = {session_path}\n"
    )
    log.info("wrote %s", CONFIG_INI)


async def cmd_discover() -> None:
    """List all joined groups + search for GMGN-related channels."""
    ensure_config()
    tg = TgData(str(CONFIG_INI))
    async with tg:
        groups = await tg.list_groups()
        log.info("found %d joined chats", len(groups))

        candidates: list[dict] = []

        # ── all joined chats (filter crypto-ish names) ─────────────────
        crypto_kw = [
            "gmgn", "solana", "sol", "whale", "smart", "signal",
            "alert", "gem", "pump", "alpha", "trading", "crypto",
            "kol", "copy", "meme", "bonk", "raydium", "jupiter",
        ]
        for name, cid in groups.items():
            name_lower = name.lower() if name else ""
            if any(kw in name_lower for kw in crypto_kw):
                candidates.append({
                    "name": name,
                    "id": cid,
                    "source": "joined",
                    "pull_limit": 500,
                })
                log.info("  [joined] %s (%s)", name, cid)

        # ── try seed channels by username ──────────────────────────────
        for uname in SEED_CHANNELS:
            url = f"@{uname}"
            if url in [c.get("name") for c in candidates]:
                continue
            try:
                count = await tg.get_message_count(group_id=url)
                if count and count > 0:
                    candidates.append({
                        "name": url,
                        "id": None,
                        "source": "seed",
                        "pull_limit": 500,
                        "message_count": count,
                    })
                    log.info("  [seed]  %s — %d msgs", url, count)
            except Exception as exc:  # noqa: BLE001
                log.debug("  [skip]  %s — %s", url, exc)

        # ── save candidates ────────────────────────────────────────────
        CHANNELS_JSON.write_text(json.dumps(candidates, indent=2))
        log.info("saved %d candidates to %s", len(candidates), CHANNELS_JSON)
        print(f"\n=== Found {len(candidates)} candidate channels ===")
        for c in candidates:
            cnt = c.get("message_count", "?")
            print(f"  {c['name']:30s}  msgs={cnt}  source={c['source']}")


async def cmd_pull() -> None:
    """Pull recent messages from all candidate channels."""
    ensure_config()
    if not CHANNELS_JSON.exists():
        sys.exit("Run 'discover' first")
    candidates = json.loads(CHANNELS_JSON.read_text())
    tg = TgData(str(CONFIG_INI))
    all_signals: list[dict] = []

    async with tg:
        for ch in candidates:
            name = ch["name"]
            limit = ch.get("pull_limit", 500)
            log.info("pulling %s (limit=%d)", name, limit)
            try:
                df = await tg.get_messages(
                    group_id=name,
                    limit=limit,
                    with_progress=True,
                )
                if df is None or df.empty:
                    log.warning("  empty: %s", name)
                    continue

                for _, row in df.iterrows():
                    text = str(row.get("Message", "") or "")
                    if not text.strip():
                        continue

                    # extract CAs
                    cas = CA_RE.findall(text)
                    cas = [c for c in cas if len(c) >= 32 and len(c) <= 44]

                    # detect buy/sell signals
                    text_lower = text.lower()
                    is_buy = any(w in text_lower for w in [
                        "buy", "buying", "long", "bullish", "ape",
                        "enter", "entry", "moon", "pump",
                    ])
                    is_sell = any(w in text_lower for w in [
                        "sell", "selling", "short", "bearish",
                        "exit", "dump", "rug", "scam",
                    ])
                    has_link = bool(re.search(r"gmgn\.ai|dexscreener|birdeye", text_lower))

                    if cas:
                        for ca in cas:
                            all_signals.append({
                                "channel": name,
                                "ca": ca,
                                "text": text[:200],
                                "is_buy": is_buy,
                                "is_sell": is_sell,
                                "has_link": has_link,
                                "ts": str(row.get("Date", "")),
                                "msg_id": int(row.get("MessageId", 0)),
                            })

                log.info("  %s: %d messages, %d signals", name, len(df),
                         sum(1 for s in all_signals if s["channel"] == name))

            except Exception as exc:  # noqa: BLE001
                log.warning("  failed %s: %s", name, exc)

    SIGNALS_DB.write_text(json.dumps(all_signals, indent=2, default=str))
    log.info("saved %d signals to %s", len(all_signals), SIGNALS_DB)
    print(f"\n=== Extracted {len(all_signals)} CA mentions from {len(candidates)} channels ===")


async def cmd_score() -> None:
    """Score channels by signal quality metrics."""
    if not SIGNALS_DB.exists():
        sys.exit("Run 'pull' first")
    signals = json.loads(SIGNALS_DB.read_text())
    if not signals:
        sys.exit("No signals found")

    df = pd.DataFrame(signals)
    print(f"\n{'='*70}")
    print(f"  CHANNEL SIGNAL QUALITY REPORT")
    print(f"{'='*70}\n")

    rows = []
    for ch, grp in df.groupby("channel"):
        total = len(grp)
        buy_pct = grp["is_buy"].mean() * 100
        sell_pct = grp["is_sell"].mean() * 100
        link_pct = grp["has_link"].mean() * 100
        unique_ca = grp["ca"].nunique()
        # signal density: unique CAs per message
        density = unique_ca / max(total, 1)
        # quality score: more unique CAs + more links + more buy signals = better
        score = (unique_ca * 2.0) + (link_pct * 0.3) + (buy_pct * 0.1)
        rows.append({
            "channel": ch,
            "msgs": total,
            "unique_cas": unique_ca,
            "buy_%": f"{buy_pct:.0f}",
            "sell_%": f"{sell_pct:.0f}",
            "link_%": f"{link_pct:.0f}",
            "density": f"{density:.2f}",
            "score": f"{score:.1f}",
        })

    result = pd.DataFrame(rows).sort_values("score", ascending=False)
    print(result.to_string(index=False))

    # top 5
    top5 = result.head(5)["channel"].tolist()
    print(f"\n{'='*70}")
    print(f"  TOP 5 CHANNELS: {top5}")
    print(f"{'='*70}")

    # save ranking
    ranking_path = SIGNALS_DB.parent / "channel_ranking.json"
    ranking_path.write_text(json.dumps(result.to_dict(orient="records"), indent=2))
    print(f"\nSaved ranking to {ranking_path}")


async def cmd_listen() -> None:
    """Live-listen to top channels and print signals in real-time."""
    ensure_config()
    ranking_path = SIGNALS_DB.parent / "channel_ranking.json"
    if ranking_path.exists():
        ranking = json.loads(ranking_path.read_text())
        channels = [r["channel"] for r in ranking[:5]]
    else:
        channels = SEED_CHANNELS[:5]
        log.warning("no ranking found, using seed channels")

    log.info("listening to: %s", channels)

    tg = TgData(str(CONFIG_INI))
    async with tg:
        for ch in channels:
            try:
                @tg.on_new_message(group_id=ch)
                async def handler(event, _ch=ch):
                    text = event.message.text or ""
                    cas = CA_RE.findall(text)
                    cas = [c for c in cas if 32 <= len(c) <= 44]
                    if cas:
                        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                        print(f"[{ts}] {_ch}: CA={cas} | {text[:120]}")

                log.info("subscribed to %s", ch)
            except Exception as exc:  # noqa: BLE001
                log.warning("cannot subscribe to %s: %s", ch, exc)

        log.info("listening... (Ctrl+C to stop)")
        await tg.run_with_event_loop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    dispatch = {
        "discover": cmd_discover,
        "pull": cmd_pull,
        "score": cmd_score,
        "listen": cmd_listen,
    }
    fn = dispatch.get(cmd)
    if not fn:
        print(f"Unknown command: {cmd}")
        print("Commands: discover, pull, score, listen")
        sys.exit(1)

    asyncio.run(fn())


if __name__ == "__main__":
    main()
