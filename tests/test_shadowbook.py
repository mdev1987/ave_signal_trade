"""Tests: ShadowBook refresh locking + close paths.

Regression cover for the 2026-09-14 fix where refresh_prices() held
self._lock across every Jupiter/DexScreener quote, blocking open_position()
(signal path) behind the whole scan and risking the 150s hung-signal
watchdog during quote outages.
"""

import asyncio
import sys
import time
import pathlib
from types import SimpleNamespace

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from main import ShadowBook  # noqa: E402

SOL_MINT = "So11111111111111111111111111111111111111112"
CA_A = "CA_A" + "x" * 40
CA_B = "CA_B" + "x" * 40


class FakeDS:
    """DexScreener stub: flat $1 price for every token, $100 SOL."""

    async def token_pairs(self, chain, ca):
        if ca == SOL_MINT:
            return {"price_usd": 100.0}
        return {"price_usd": 1.0, "liq": 50_000.0, "mcap": 1_000_000.0,
                "price_change": {}, "pair_address": "pair_" + ca[:8]}

    async def close(self):
        pass


class FakeJupiter:
    """Jupiter stub with per-CA blocking sell quotes (outage simulation)."""

    def __init__(self):
        self.block_cas: set[str] = set()
        self.release = asyncio.Event()
        self.sell_mult: dict[str, float] = {}
        self.quote_stability_checks = 0
        self._buy_rtse = True
        self._slippage_bps = 100

    def _pumpapi_enabled(self):
        return False

    async def quote(self, ca, amount_raw, force=False):
        return SimpleNamespace(success=True, reason="",
                               output_amount=5_000_000,
                               price_impact_pct=0.1)

    async def quote_sell(self, ca, amount_raw, slippage_bps=None):
        if ca in self.block_cas:
            await self.release.wait()
        # size_sol=0.05 default in tests -> out for mult m: m * 0.05 SOL
        m = self.sell_mult.get(ca, 1.0)
        return SimpleNamespace(success=True, reason="",
                               output_amount=int(m * 0.05 * 1e9),
                               price_impact_pct=0.1)

    async def token_decimals(self, ca):
        return 6

    async def close(self):
        pass


def _book(tmp_path, jup=None, **kw):
    ds = FakeDS()
    params = dict(size_sol=0.05, retrace_pct=0.35, hard_stop_pct=0.25,
                  state_file=tmp_path / "shadow.json",
                  start_balance_sol=4.0, jupiter=jup or FakeJupiter(),
                  max_positions=12, early_filter_window_s=30.0,
                  reentry_cooldown_s=3600.0)
    params.update(kw)
    return ShadowBook(ds, params.pop("size_sol"), params.pop("retrace_pct"),
                      params.pop("hard_stop_pct"), params.pop("state_file"),
                      params.pop("start_balance_sol"), **params)


def test_open_not_blocked_by_slow_refresh(tmp_path):
    """open_position() must proceed while a refresh quote is in-flight."""
    async def _run():
        jup = FakeJupiter()
        book = _book(tmp_path, jup=jup)
        await book.open_position(CA_A, "AAA", 1.0, 1.0, 2,
                                 wallets=["w1", "w2"])
        assert CA_A in book.open
        # Simulate a Jupiter outage hanging A-law's sell quote.
        jup.block_cas.add(CA_A)
        refresh = asyncio.ensure_future(book.refresh_prices())
        await asyncio.sleep(0.2)
        assert not refresh.done()
        # Pre-fix this timed out: refresh held the lock across the quote.
        await asyncio.wait_for(
            book.open_position(CA_B, "BBB", 1.0, 1.0, 2,
                               wallets=["w3", "w4"]),
            timeout=5.0)
        assert CA_B in book.open
        jup.release.set()
        await asyncio.wait_for(refresh, timeout=10.0)
        # Both still open (flat $1 price -> no exit).
        assert CA_A in book.open and CA_B in book.open

    asyncio.run(_run())


def test_hard_stop_close(tmp_path):
    """Price below the hard stop closes the position and credits balance."""
    async def _run():
        jup = FakeJupiter()
        jup.sell_mult[CA_A] = 0.5  # -50% -> below 0.75 stop
        ds = FakeDS()
        # DexScreener mid must agree (authoritative leg is Jupiter here).
        book = _book(tmp_path, jup=jup)
        book.ds = ds
        await book.open_position(CA_A, "AAA", 1.0, 1.0, 2,
                                 wallets=["w1", "w2"])
        bal_after_open = book.balance_sol
        await book.refresh_prices()
        assert CA_A not in book.open
        assert len(book.closed) == 1
        rec = book.closed[0]
        assert rec["reason"] == "sl"
        assert rec["pnl_sol"] < 0
        # Balance: locked size returned plus pnl.
        assert abs(book.balance_sol - (bal_after_open + 0.05 + rec["pnl_sol"])) < 1e-9

    asyncio.run(_run())


def test_dead_liquidity_close(tmp_path):
    """Both pricers dead past the grace limit force-closes the zombie."""
    async def _run():
        class DeadJup(FakeJupiter):
            async def quote_sell(self, ca, amount_raw, slippage_bps=None):
                return SimpleNamespace(success=False, reason="quote_no_route",
                                       output_amount=0, price_impact_pct=0.0)

        class DeadDS:
            async def token_pairs(self, chain, ca):
                if ca == SOL_MINT:
                    return {"price_usd": 100.0}
                return None

        book = _book(tmp_path, jup=DeadJup())
        book.ds = DeadDS()
        # Seed one open position directly (no pricers available for open).
        book.open[CA_A] = {"symbol": "AAA", "entry_usd": 1.0,
                           "peak_usd": 1.0, "last_usd": 1.0,
                           "market_entry_px": 1.0, "tokens_raw": 5_000_000,
                           "entry_note": "test", "size_sol": 0.05,
                           "ts": time.time() - 10, "trigger_usd": 1.0,
                           "n_wallets": 2, "wallets": ["w1"],
                           "tp_taken": [], "remaining": 1.0,
                           "banked_pnl": 0.0, "be_armed": False,
                           "peak_mult": 1.0, "source": "cabalspy",
                           "tp_level": -1, "entry_mode": "executable",
                           "early_min_mult": 1.0, "early_max_mult": 1.0,
                           "early_checked": True}
        book.open[CA_A]["_quote_fail_count"] = 10
        await book.refresh_prices()
        assert CA_A not in book.open
        assert book.closed and book.closed[-1]["reason"] == "dead_liquidity"

    asyncio.run(_run())


def _seed_open(book, ca=CA_A):
    book.open[ca] = {"symbol": "AAA", "entry_usd": 1.0,
                     "peak_usd": 1.0, "last_usd": 1.0,
                     "market_entry_px": 1.0, "tokens_raw": 5_000_000,
                     "entry_note": "test", "size_sol": 0.05,
                     "ts": time.time() - 10, "trigger_usd": 1.0,
                     "n_wallets": 2, "wallets": ["w1"],
                     "tp_taken": [], "remaining": 1.0,
                     "banked_pnl": 0.0, "be_armed": False,
                     "peak_mult": 1.0, "source": "cabalspy",
                     "tp_level": -1, "entry_mode": "executable",
                     "early_min_mult": 1.0, "early_max_mult": 1.0,
                     "early_checked": True}


def test_transient_429_does_not_kill_position(tmp_path):
    """Jupiter 429s are infra, not dead liquidity: hold via DexScreener.

    Regression for 2026-09-14: a gateway 429 storm force-closed FwEm…
    at mult=0.0/-100% after 10 consecutive quote failures.
    """
    async def _run():
        class TransientJup(FakeJupiter):
            async def quote_sell(self, ca, amount_raw, slippage_bps=None):
                return SimpleNamespace(success=False, reason="quote_rate_limited",
                                       output_amount=0, price_impact_pct=0.0)

        book = _book(tmp_path, jup=TransientJup(), early_filter_window_s=0.0,
                     flat_timeout_s=0.0, max_hold_s=0.0)
        _seed_open(book)
        for _ in range(12):
            await book.refresh_prices()
        assert CA_A in book.open
        assert book.open[CA_A].get("_quote_fail_count", 0) == 0
        assert book.open[CA_A].get("_transient_fail_count", 0) == 12

    asyncio.run(_run())


def test_oracle_fail_close_is_honest_and_excluded(tmp_path):
    """Infra close books ~0 (last price), not -100%, and is strategy-excluded."""
    async def _run():
        from main import build_status
        class DeadJup(FakeJupiter):
            async def quote_sell(self, ca, amount_raw, slippage_bps=None):
                return SimpleNamespace(success=False, reason="quote_no_route",
                                       output_amount=0, price_impact_pct=0.0)

        class DeadDS:
            async def token_pairs(self, chain, ca):
                if ca == SOL_MINT:
                    return {"price_usd": 100.0}
                return None

        book = _book(tmp_path, jup=DeadJup())
        book.ds = DeadDS()
        _seed_open(book)
        book.open[CA_A]["_quote_fail_count"] = 10
        await book.refresh_prices()
        rec = book.closed[-1]
        assert rec["reason"] == "dead_liquidity"
        assert rec["oracle_fail"] is True
        # last_usd == entry -> honest pnl ~0, not a fabricated full loss
        assert abs(rec["pnl_sol"]) < 1e-9
        assert book._win_rate() == 0.0
        card = build_status(book.snapshot(1, 0, 0, 0.0, {}))
        assert "infra excluded" in card
        assert "Closed: 0" in card

    asyncio.run(_run())


def test_pumpapi_entry_no_dead_counter(tmp_path):
    """PumpAPI entries (tokens_raw==0) must not accrue Jupiter dead quotes."""
    async def _run():
        book = _book(tmp_path, early_filter_window_s=0.0,
                     flat_timeout_s=0.0, max_hold_s=0.0)
        _seed_open(book)
        book.open[CA_A]["tokens_raw"] = 0
        book.open[CA_A]["entry_mode"] = "pumpapi"
        for _ in range(12):
            await book.refresh_prices()
        # DexScreener prices $1 flat -> no exit, still open, no dead count
        assert CA_A in book.open
        assert book.open[CA_A].get("_quote_fail_count", 0) == 0

    asyncio.run(_run())
