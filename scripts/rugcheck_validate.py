"""Validate the anti-rug gates against the 2026-08-20 live-rug evidence.

Replays the labeled live-session trades (``docs/replay_zst/live_rugged_logs``)
through the new RugCheck gate variants and the serial-relaunch damper, then
reports which rugs would have been blocked and which winners would have been
spared. RugCheck summaries are fetched once and cached on disk so re-runs
never burn rate limit.

Labels (from journal.json close events):
    rug      : pnl <= -0.02  (near-total loss — TONK, NEX Ai x2, 牛来)
    survivor : pnl >  -0.02  (winners + positive writeoffs)

Usage:
    uv run python scripts/rugcheck_validate.py            # labeled set only
    uv run python scripts/rugcheck_validate.py --sweep 40 # + passing-signal sweep
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import httpx

import filter as filt
from parser import parse_message_dict
from scam_damper import ScamDamper

LIVE_DIR = ROOT / "docs/replay_zst/live_rugged_logs"
SIGNALS_0820 = ROOT / "docs/replay_zst/signal_2026_08_20.json"
CACHE_FILE = ROOT / "bot_logs/rugcheck_cache.json"
API = "https://api.rugcheck.xyz/v1/tokens/{}/report/summary"


def load_labels() -> dict[str, dict]:
    """CA -> {name, pnl_sol, reason} from the live session's close events."""
    closes: dict[str, dict] = {}
    names: dict[str, str] = {}
    for line in (LIVE_DIR / "journal.json").read_text(encoding="utf-8").splitlines():
        try:
            j = json.loads(line)
        except json.JSONDecodeError:
            continue
        ev = j.get("event")
        if ev == "close":
            closes[j["ca"]] = {
                "pnl": j.get("pnl_sol") or 0.0,
                "reason": j.get("reason") or "",
            }
        elif ev in ("signal", "reject", "arm", "skip") and j.get("name"):
            names.setdefault(j["ca"], j["name"])
        elif ev == "open":
            names.setdefault(j["ca"], "")
    out: dict[str, dict] = {}
    for ca, c in closes.items():
        label = "rug" if c["pnl"] <= -0.02 else "survivor"
        out[ca] = {"name": names.get(ca, ca[:8]), "label": label, **c}
    return out


def load_cache() -> dict[str, dict | None]:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache, indent=1), encoding="utf-8")


async def fetch_summaries(
    cas: list[str], api_key: str, min_gap_s: float = 0.35
) -> dict[str, dict | None]:
    """Fetch report summaries for all CAs (disk-cached, throttled)."""
    cache = load_cache()
    missing = [ca for ca in cas if ca not in cache]
    headers = {"accept": "application/json"}
    if api_key:
        headers["X-API-KEY"] = api_key
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for i, ca in enumerate(missing):
            data = None
            for attempt in (1, 2):
                try:
                    r = await client.get(API.format(ca), headers=headers)
                    if r.status_code == 404:
                        break
                    if r.status_code == 429:
                        await asyncio.sleep(2.5 * attempt)
                        continue
                    r.raise_for_status()
                    data = r.json()
                    break
                except Exception as e:  # noqa: BLE001
                    print(f"  ! {ca[:8]} fetch failed: {e}")
                    await asyncio.sleep(1.0)
            cache[ca] = data
            print(f"  fetched {i + 1}/{len(missing)} {ca[:10]} "
                  f"score={data.get('score_normalised') if data else '-'}")
            await asyncio.sleep(min_gap_s)
    save_cache(cache)
    return cache


def risk_names(summary: dict | None) -> list[tuple[str, str]]:
    if not summary:
        return []
    return [
        (str(r.get("name") or ""), str(r.get("level") or ""))
        for r in (summary.get("risks") or [])
    ]


def vetoed(summary: dict | None, needles: tuple[str, ...]) -> str:
    """Return the matched veto reason or '' when clean/missing."""
    if not summary:
        return ""
    for name, level in risk_names(summary):
        low = name.lower()
        for n in needles:
            if n in low and level != "info":
                return name
    return ""


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", type=int, default=0,
                    help="also gate the first N filter-passing signals of 08-20")
    args = ap.parse_args()

    env = {}
    env_file = ROOT / ".env"
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if raw and not raw.startswith("#") and "=" in raw:
            k, v = raw.split("=", 1)
            env[k.strip()] = v.strip()
    api_key = env.get("RUGCHECK_API_KEY", "")

    labels = load_labels()
    cas = sorted(labels)
    print(f"labeled live trades: {len(cas)} "
          f"({sum(1 for l in labels.values() if l['label'] == 'rug')} rugs, "
          f"{sum(1 for l in labels.values() if l['label'] == 'survivor')} survivors)")
    summaries = await fetch_summaries(cas, api_key)

    baseline_pnl = sum(l["pnl"] for l in labels.values())
    print(f"\nbaseline realized PnL: {baseline_pnl:+.4f} SOL")

    def run_variant(name: str, verdict) -> None:
        rugs_caught = winners_blocked = 0
        pnl_after = 0.0
        rows = []
        for ca, lab in labels.items():
            v = verdict(ca, lab)
            blocked = bool(v)
            rows.append((lab["name"], lab["label"], v or "-", blocked))
            if blocked:
                if lab["label"] == "rug":
                    rugs_caught += 1
                else:
                    winners_blocked += 1
            else:
                pnl_after += lab["pnl"]
        print(f"\n== {name}")
        for n, lab, v, b in rows:
            mark = "BLOCK" if b else "pass "
            flag = "OK " if (b == (lab == "rug")) else "MISS/BLOCK!"
            print(f"   {mark} {n:<12} {lab:<9} {v:<40} {flag if b or lab != 'rug' else ''}")
        print(f"   rugs caught {rugs_caught}/4, winners blocked {winners_blocked}, "
              f"PnL after gate {pnl_after:+.4f} SOL")

    def score_of(ca: str):
        s = summaries.get(ca)
        v = s.get("score_normalised") if s else None
        return v if isinstance(v, (int, float)) else None

    run_variant("lp unlocked only (.env default)",
                lambda ca, _lab: vetoed(summaries.get(ca), ("lp unlocked",)))
    run_variant("+ mint/freeze authority",
                lambda ca, _lab: vetoed(summaries.get(ca),
                                        ("lp unlocked", "mint authority", "freeze authority")))
    run_variant("any danger-level risk",
                lambda ca, _lab: next(
                    (f"{n} [{lv}]" for n, lv in risk_names(summaries.get(ca))
                     if lv.lower() == "danger"), ""))

    def score_gt(threshold: float):
        def verdict(ca: str, _lab) -> str:
            sc = score_of(ca)
            return f"score {sc}" if sc is not None and sc > threshold else ""
        return verdict

    run_variant("score_normalised > 60", score_gt(60))
    run_variant("score_normalised > 70", score_gt(70))

    def lp_below(floor: float):
        def verdict(ca: str, _lab) -> str:
            s = summaries.get(ca)
            v = s.get("lpLockedPct") if s else None
            return (f"lp locked {v}%"
                    if isinstance(v, (int, float)) and v < floor else "")
        return verdict

    run_variant("lpLockedPct < 50", lp_below(50))

    # --- serial-relaunch damper replay over the 08-20 signal stream ----------
    print("\n=== serial-relaunch damper replay (2026-08-20 signals) ===")
    msgs = json.loads(SIGNALS_0820.read_text(encoding="utf-8"))["messages"]
    sigs = sorted((parse_message_dict(m) for m in msgs), key=lambda s: s.unixtime)
    damper = ScamDamper(max_cas=3, window_s=6 * 3600.0)
    damped = passed = 0
    for s in sigs:
        if not s.ca:
            continue
        damper.record(s.ca, s.name, float(s.unixtime))
        ok, _ = filt.check_signal(s)
        if not ok:
            continue
        passed += 1
        if damper.is_serial(s.name, now=float(s.unixtime)):
            damped += 1
            live_rugs = {
                "DJoTcW6yc62rAcXPJxWN9UH3x15iJHtQopRu1nViDVQS",
                "4h7pLafWMNLuxR2BXnSr97uDPtNt3DQBGHXua4BhEF3b",
                "ACqWAhzfw4AGPdN9QqMHq3RNkHvBWuZseAec6VmvepgM",
            }
            tag = " <-- LIVE RUG" if s.ca in live_rugs else ""
            print(f"   DAMPED {s.unixtime} {s.name!r} {s.ca}{tag}")
    print(f"filter passed {passed}, damper rejected {damped}")

    if args.sweep:
        print(f"\n=== rugcheck sweep over first {args.sweep} passing CAs ===")
        sweep: list[str] = []
        seen: set[str] = set()
        for s in sigs:
            if not s.ca or s.ca in seen:
                continue
            seen.add(s.ca)
            ok, _ = filt.check_signal(s)
            if ok:
                sweep.append(s.ca)
            if len(sweep) >= args.sweep:
                break
        sw = await fetch_summaries(sweep, api_key)
        hits = sum(1 for ca in sweep if vetoed(sw.get(ca), ("lp unlocked",)))
        scores = [sw[ca].get("score_normalised") for ca in sweep if sw.get(ca)]
        print(f"swept {len(sweep)}: lp-unlocked vetoes {hits}; "
              f"scores {sorted(x for x in scores if x is not None)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
