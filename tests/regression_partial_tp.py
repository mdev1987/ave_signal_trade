"""Regression tests: partial take-profit + breakeven-floor trail.

Scheme: bank tp1_frac of the position at tp1_mult x entry (limit fill), then
the runner rides the trail with a floor at entry — a +13x Sinopec-style spike
can never round-trip to a loss on the banked leg.
"""

from models import Position


def make_pos(**kw):
    base = dict(
        ca="T", name="T", signal_time=0.0, entry_time=1000.0,
        entry_px=1.0, size_sol=0.05, take_profit=10.0,
        trail_retrace_pct=0.5, tp1_mult=2.0, tp1_frac=0.5,
    )
    base.update(kw)
    return Position(**base)


def test_tp1_banks_half_and_keeps_position():
    p = make_pos()
    r = p.update(1100.0, 2.4)  # peak crosses 2x
    assert r == "tp1" and p.tp1_done and not p.is_closed
    assert abs(p.realized_sol - 0.05 * 0.5 * 1.0) < 1e-12  # 0.025 SOL
    # PnL = banked + remaining mark-to-market
    assert abs(p.pnl_sol - (0.025 + 0.05 * 0.5 * 1.4)) < 1e-9


def test_tp1_fires_once_only():
    p = make_pos()
    assert p.update(1100.0, 3.0) == "tp1"
    assert p.update(1110.0, 3.5) is None  # no repeat


def test_breakeven_floor_after_tp1():
    p = make_pos(stop_loss=0.3)
    p.update(1100.0, 2.4)          # tp1 banked @2x -> stop ratchets to >=entry
    # Normal fade: crosses the floored stop ABOVE entry -> exit >= breakeven
    assert p.update(1150.0, 1.25) is None   # just above floored stop (1.2)
    r = p.update(1200.0, 1.19)
    assert r == "trail" and p.exit_px >= p.entry_px


def test_gap_crash_exits_early_even_below_floor():
    # Stop-market honesty: a gap through the floor still exits IMMEDIATELY
    # (at the gapped price) instead of waiting for the hard SL.
    p = make_pos(stop_loss=0.3)
    p.update(1100.0, 2.4)
    r = p.update(1200.0, 0.9)
    assert r == "trail" and p.exit_reason == "trail"


def test_full_pnl_with_partial():
    p = make_pos()
    p.update(1050.0, 2.0)                       # banks 0.025 SOL
    r = p.update(1150.0, 6.0)                   # runner exits via... peak 6<tp10; trail: peak*0.5=3 -> 6>3 hold
    r = p.update(1250.0, 2.5)                   # <= peak*(1-0.5)=3 -> trail exit
    assert r == "trail"
    total = p.realized_sol + 0.05 * 0.5 * (p.exit_px / p.entry_px - 1)
    assert abs(p.pnl_sol - total) < 1e-9 and p.pnl_sol > 0.05


if __name__ == "__main__":
    test_tp1_banks_half_and_keeps_position()
    test_tp1_fires_once_only()
    test_breakeven_floor_after_tp1()
    test_full_pnl_with_partial()
    print("partial-TP regression tests passed")
