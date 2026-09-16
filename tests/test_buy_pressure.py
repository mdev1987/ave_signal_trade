"""Tests: CVD-lite buy-pressure gate predicate."""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from main import buy_pressure_ok


def test_accumulation_passes():
    ok, pct = buy_pressure_ok({"buys_m5": 80, "sells_m5": 20}, 0.5)
    assert ok is True and pct == 0.8


def test_distribution_blocked():
    ok, pct = buy_pressure_ok({"buys_m5": 20, "sells_m5": 80}, 0.5)
    assert ok is False and pct == 0.2


def test_boundary_is_inclusive():
    ok, _ = buy_pressure_ok({"buys_m5": 50, "sells_m5": 50}, 0.5)
    assert ok is True


def test_missing_or_zero_data_fails_open():
    assert buy_pressure_ok(None, 0.5) == (True, 0.5)
    assert buy_pressure_ok({}, 0.5) == (True, 0.5)
    assert buy_pressure_ok({"buys_m5": 0, "sells_m5": 0}, 0.9) == (True, 0.5)
    assert buy_pressure_ok({"buys_m5": 0, "sells_m5": 100}, 0.0) == (True, 0.5)


def test_threshold_off_disables():
    ok, _ = buy_pressure_ok({"buys_m5": 0, "sells_m5": 100}, 0)
    assert ok is True
