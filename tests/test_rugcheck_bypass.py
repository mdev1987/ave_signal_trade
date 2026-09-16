"""Tests: RugCheck DANGER bypass rule (locked-LP requirement + sticky rejects)."""

import pathlib
import sys
from types import SimpleNamespace

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from main import (
    RUG_BYPASS_MIN_LP_LOCKED_PCT,
    RUG_REJECT_TTL_S,
    rugcheck_danger_bypass_ok,
)


def _rc(**kw):
    base = {"score_normalised": 10, "rugged": False, "has_danger": True,
            "lp_locked_pct": 100.0}
    base.update(kw)
    return SimpleNamespace(**base)


def test_unlocked_lp_never_bypasses_even_at_huge_mc():
    # INUTILITY 2026-09-16: $80M MC, lp_locked=0% -> must stay blocked.
    ok, why = rugcheck_danger_bypass_ok(_rc(lp_locked_pct=0.0),
                                        80_000_000, 5_000_000, 50, 500_000)
    assert ok is False and "lp_locked" in why


def test_locked_lp_high_mc_bypasses():
    ok, _ = rugcheck_danger_bypass_ok(_rc(lp_locked_pct=80.0),
                                      800_000, 50_000, 50, 500_000)
    assert ok is True


def test_locked_lp_high_vol_bypasses_despite_low_mc():
    ok, _ = rugcheck_danger_bypass_ok(_rc(lp_locked_pct=60.0),
                                      100_000, 500_000, 50, 500_000)
    assert ok is True


def test_low_mc_low_vol_denied():
    ok, why = rugcheck_danger_bypass_ok(_rc(lp_locked_pct=100.0),
                                        20_000, 5_000, 50, 500_000)
    assert ok is False and why == "low_mc_vol"


def test_high_score_denied_despite_locked_lp_and_high_mc():
    ok, why = rugcheck_danger_bypass_ok(_rc(score_normalised=61, lp_locked_pct=100.0),
                                        80_000_000, 5_000_000, 50, 500_000)
    assert ok is False and why.startswith("score=")


def test_rugged_never_bypasses():
    ok, _ = rugcheck_danger_bypass_ok(_rc(rugged=True, lp_locked_pct=100.0),
                                      80_000_000, 5_000_000, 50, 500_000)
    assert ok is False


def test_no_report_never_bypasses():
    # Fail-open (None) must not clear a sticky reject via the bypass path.
    ok, _ = rugcheck_danger_bypass_ok(None, 80_000_000, 5_000_000, 50, 500_000)
    assert ok is False


def test_pure_score_reject_not_a_bypass_case():
    ok, why = rugcheck_danger_bypass_ok(_rc(has_danger=False, score_normalised=61),
                                        80_000_000, 5_000_000, 50, 500_000)
    assert ok is False and why == "not_danger"


def test_lp_boundary_is_inclusive():
    ok, _ = rugcheck_danger_bypass_ok(
        _rc(lp_locked_pct=RUG_BYPASS_MIN_LP_LOCKED_PCT),
        800_000, 0, 50, 500_000)
    assert ok is True


def test_reject_ttl_is_24h():
    assert RUG_REJECT_TTL_S == 24 * 3600
