"""Tests: funds-vs-route disambiguation, sell retry, sim sizing.

Covers the "not enough quote" family: live taker-mode 400s misclassified
as no-route, transient sell blips surfacing as quote-not-found, and sim
sizing against broke wallets.
"""

import asyncio
import pathlib
import sys

import httpx
import pytest

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from jupiter_trade import JupiterError, JupiterSwap
from main import _SELL_FAIL_HINTS, _affordable_size


def _jup():
    return JupiterSwap(dry_run=True)


def test_classify_error_table():
    j = _jup()
    assert j._classify_error(JupiterError("Insufficient funds for tx", status=400)) == \
        "quote_insufficient_funds"
    assert j._classify_error(JupiterError("x", status=429)) == "quote_rate_limited"
    assert j._classify_error(JupiterError("boom", status=503)) == "quote_http_error"
    assert j._classify_error(JupiterError("Failed to get quotes", status=400)) == \
        "quote_no_route"
    assert j._classify_error(JupiterError("No route found", status=400)) == "quote_no_route"
    assert j._classify_error(JupiterError("weird 400", status=400)) == "quote_http_error"
    assert j._classify_error(JupiterError("garbage", status=None)) == "quote_invalid_response"
    # Deterministic bad request: invalid mint must NOT retry (bot.log 2026-09-15:
    # USDC/invalid-mint probes retried 3x as quote_http_error).
    assert j._classify_error(JupiterError("Invalid outputMint", status=400)) == \
        "quote_invalid_response"
    assert j._classify_error(
        JupiterError('order HTTP 400: {"error":"Invalid outputMint"}', status=400)
    ) == "quote_invalid_response"
    # _order wraps httpx timeouts as JupiterError("order timed out after
    # 20s: ...", status=0) — must be retryable timeout, not invalid
    # (live 2026-09-16: GROK sell timeouts logged as quote_invalid_response
    # and gave up after 1 attempt instead of retrying x3).
    assert j._classify_error(
        JupiterError("order timed out after 20s: ", status=0)
    ) == "quote_timeout"
    assert j._classify_error(
        JupiterError("order timed out after 20s: TimeoutException", status=None)
    ) == "quote_timeout"


def _with_balance(j, bal):
    async def _fake():
        return bal
    j.balance_sol = _fake
    return j


def test_disambiguate_broke_wallet():
    j = _with_balance(_jup(), 0.02)
    out = asyncio.run(j._disambiguate_no_route(
        "MINT", int(0.05 * 1e9), JupiterError("Failed to get quotes", status=400),
        "quote_no_route"))
    assert out == "quote_insufficient_funds"


def test_disambiguate_rich_wallet_keeps_route():
    j = _with_balance(_jup(), 5.0)
    out = asyncio.run(j._disambiguate_no_route(
        "MINT", int(0.05 * 1e9), JupiterError("Failed to get quotes", status=400),
        "quote_no_route"))
    assert out == "quote_no_route"


def test_disambiguate_rpc_failure_keeps_reason():
    j = _with_balance(_jup(), None)
    out = asyncio.run(j._disambiguate_no_route(
        "MINT", int(0.05 * 1e9), JupiterError("Failed to get quotes", status=400),
        "quote_no_route"))
    assert out == "quote_no_route"


def test_affordable_size_table():
    size, msg = _affordable_size(5.0, 0.05)
    assert (size, msg) == (0.05, "")
    size, msg = _affordable_size(0.03, 0.05)
    assert size == pytest.approx(0.022, abs=1e-9) and "downsized" in msg
    size, msg = _affordable_size(0.006, 0.05)
    assert size is None and "insufficient funds" in msg
    size, msg = _affordable_size(0.013, 0.05)
    assert size is None  # 0.013-0.008=0.005 <= dust


def test_sell_hints_cover_all_reasons():
    for r in ("quote_no_route", "quote_impact", "quote_timeout",
              "quote_rate_limited", "quote_insufficient_funds",
              "quote_invalid_response", "quote_http_error", "quote_exception"):
        assert _SELL_FAIL_HINTS.get(r), r


_OK_ORDER = {"outAmount": "1000", "priceImpact": "0.1",
             "routePlan": [{"x": 1}], "router": "t", "mode": "m"}


def test_sell_retries_transient_then_succeeds():
    j = _jup()
    calls = []

    async def flaky(*a, **k):
        calls.append(1)
        if len(calls) < 3:
            raise httpx.TimeoutException("slow")
        return dict(_OK_ORDER)

    j._order = flaky
    res = asyncio.run(j._do_quote_sell("MINT", 100, 300))
    assert res.success and len(calls) == 3
    assert j._qstats["ok"] == 1


def test_sell_retries_jupiter_timeout_error_then_succeeds():
    # Production timeout path: _order raises JupiterError("order timed out
    # ...", status=0), not httpx.TimeoutException. Must retry x3.
    j = _jup()
    calls = []

    async def flaky_order(*a, **k):
        calls.append(1)
        if len(calls) < 3:
            raise JupiterError("order timed out after 20s: ", status=0)
        return dict(_OK_ORDER)

    j._order = flaky_order
    res = asyncio.run(j._do_quote_sell("MINT", 100, 300))
    assert res.success and len(calls) == 3
    assert j._qstats["ok"] == 1


def test_sell_no_route_fails_fast_without_retry():
    j = _jup()
    calls = []

    async def dead(*a, **k):
        calls.append(1)
        raise JupiterError("Failed to get quotes", status=400)

    j._order = dead
    res = asyncio.run(j._do_quote_sell("MINT", 100, 300))
    assert not res.success and res.reason == "quote_no_route"
    assert len(calls) == 1
    assert j._qstats["quote_no_route"] == 1


def test_sell_429_sets_cooldown_and_fails_fast():
    j = _jup()
    calls = []

    async def limited(*a, **k):
        calls.append(1)
        raise JupiterError("slow down", status=429)

    j._order = limited
    res = asyncio.run(j._do_quote_sell("MINT", 100, 300))
    assert not res.success and res.reason == "quote_rate_limited"
    assert len(calls) == 1
    # second call hits the cooldown without network
    res2 = asyncio.run(j._do_quote_sell("MINT", 100, 300))
    assert not res2.success and len(calls) == 1
