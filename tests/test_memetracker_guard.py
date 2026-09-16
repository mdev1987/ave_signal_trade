"""Tests: MemeTracker signal-price executability guard.

Regression cover for the 2026-09-16 review: signal-price entries bypassed
every Jupiter gate, and the whole catastrophic left tail of memetracker
paper (-0.14/9, exits at 0.40-0.89x within 0-4 min: launchcat, MAILED, tod,
GROYPER, LCAT, Democrat, LMEOW, PAID, DATBOI) was tokens that were never
sellable at entry. ``memetracker_executable`` enforces the same standard
as the Jupiter-routed path (live buy quote within impact limits + working
sell quote) before a signal-price open.
"""

import asyncio
import pathlib
import sys
import types

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

# main.py imports heavy third-party modules (base58, solders, telethon
# chains) at top level. Stub the ones that may be missing in CI so the
# helper under test stays importable — same approach as the other suites
# that import main (test_blind_open).
for _name in ("base58", "solders", "solders.keypair"):
    if _name not in sys.modules:
        try:
            __import__(_name)
        except ImportError:
            sys.modules[_name] = types.ModuleType(_name)
if not hasattr(sys.modules.get("solders"), "keypair"):
    _kp = types.ModuleType("solders.keypair")

    class Keypair:  # minimal stand-in
        pass

    _kp.Keypair = Keypair
    sys.modules["solders.keypair"] = _kp
if not hasattr(sys.modules.get("base58"), "b58decode"):
    sys.modules["base58"].b58decode = lambda *a, **k: b""

from main import memetracker_executable


class _Q:
    def __init__(self, success=True, reason="ok", output_amount=1000,
                 price_impact_pct=1.0):
        self.success = success
        self.reason = reason
        self.output_amount = output_amount
        self.price_impact_pct = price_impact_pct


class _Jup:
    def __init__(self, buy=None, sell=None, boom_buy=False, boom_sell=False):
        self._buy = buy if buy is not None else _Q()
        self._sell = sell if sell is not None else _Q()
        self._boom_buy = boom_buy
        self._boom_sell = boom_sell
        self.buy_calls = 0
        self.sell_calls = 0

    async def quote(self, ca, lamports, force=False):
        self.buy_calls += 1
        if self._boom_buy:
            raise RuntimeError("rpc down")
        return self._buy

    async def quote_sell(self, ca, raw):
        self.sell_calls += 1
        if self._boom_sell:
            raise RuntimeError("rpc down")
        return self._sell


def _run(coro):
    return asyncio.run(coro)


def test_executable_token_passes():
    j = _Jup()
    assert _run(memetracker_executable(j, "CA", 48_300_000, 10.0)) is None
    assert j.buy_calls == 1 and j.sell_calls == 1


def test_no_buy_route_skips_and_skips_sell_probe():
    j = _Jup(buy=_Q(success=False, reason="quote_no_route"))
    reason = _run(memetracker_executable(j, "CA", 48_300_000, 10.0))
    assert reason == "skip:no_buy_route(quote_no_route)"
    assert j.sell_calls == 0  # no point probing the sell side


def test_buy_quote_exception_skips():
    j = _Jup(boom_buy=True)
    assert _run(memetracker_executable(j, "CA", 48_300_000, 10.0)) == \
        "skip:no_buy_route(quote_exception)"


def test_high_impact_skips():
    j = _Jup(buy=_Q(price_impact_pct=25.0))
    reason = _run(memetracker_executable(j, "CA", 48_300_000, 10.0))
    assert reason == "skip:impact(25.00%>10.0%)"
    assert j.sell_calls == 0


def test_impact_cap_nonpositive_disables():
    j = _Jup(buy=_Q(price_impact_pct=25.0))
    assert _run(memetracker_executable(j, "CA", 48_300_000, 0.0)) is None


def test_unsellable_skips():
    j = _Jup(sell=_Q(success=False, reason="quote_no_route"))
    reason = _run(memetracker_executable(j, "CA", 48_300_000, 10.0))
    assert reason == "skip:unsellable(quote_no_route)"


def test_sell_quote_exception_skips():
    j = _Jup(boom_sell=True)
    assert _run(memetracker_executable(j, "CA", 48_300_000, 10.0)) == \
        "skip:unsellable(quote_exception)"
