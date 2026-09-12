"""CabalSpy wallet lookup — identify any address (manual tool).

Usage:
    uv run python scripts/cabalspy_lookup.py <address>
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

import config as cfg  # noqa: E402
from cabalspy_rest import CabalSpyREST  # noqa: E402


async def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    env = cfg.load_env()
    keys = (cfg.get(env, "CABALSPY_API_KEY", "") or "").split(",")
    rest = CabalSpyREST(api_keys=keys)
    try:
        data = await rest.wallets_lookup(sys.argv[1].strip())
    finally:
        await rest.close()
    if not data:
        print("not tracked (or lookup failed)")
        return 1
    print(json.dumps(data, indent=1)[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
