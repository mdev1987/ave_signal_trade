"""Regression tests: flat-exit (no-momentum cut) + trading-hours window.

2026-08-25 session: 12 of 20 closes were 0.95-0.97x timeouts — dead tokens
bleeding ~4%/trip in fees+slippage while holding slots for 25 minutes. The
flat-exit cuts them once the grace period passes without a real move.
"""


from models import Position
from paper_trader import in_trading_window


def make_pos(**kw):
    base = dict(
        ca="TEST", name="T", signal_time=0.0, entry_time=1000.0,
        entry_px=1.0, size_sol=0.05,
    )
    base.update(kw)
    return Position(**base)


def test_flat_exit_fires_after_grace_without_gain():
    p = make_pos(flat_after_s=900, flat_max_gain_pct=1.0)
    p.update(1100.0, 1.004)  # tiny wiggle below the 1% peak-gain bar
    assert p.update(1000.0 + 901) == "flat"
    assert p.exit_reason == "flat"


def test_flat_exit_spares_real_movers():
    p = make_pos(flat_after_s=900, flat_max_gain_pct=1.0)
    p.update(1050.0, 1.30)  # peaked +30% early
    p.update(1000.0 + 2000, 1.01)  # long past grace, faded back near entry
    # Not flat: trail/timeout/sl govern from here (peak 1.3 < tp 3 default).
    assert not p.is_closed or p.exit_reason != "flat"


def test_flat_disabled_by_default():
    p = make_pos()
    # Within the default 3600s timeout, price flat -> no exit (flat off).
    assert p.update(1000.0 + 3000, 1.0000001) is None


def test_window_basic_and_wrap():
    assert in_trading_window("", "03:00")            # empty = always
    assert in_trading_window("22:00-02:00", "23:30")
    assert in_trading_window("22:00-02:00", "01:59")
    assert not in_trading_window("22:00-02:00", "05:00")
    assert in_trading_window("00:00-23:59", "12:00")
    assert not in_trading_window("10:00-11:00", "11:00")  # end exclusive
    assert in_trading_window("garbage", "12:00")     # malformed never blocks


if __name__ == "__main__":
    test_flat_exit_fires_after_grace_without_gain()
    test_flat_exit_spares_real_movers()
    test_flat_disabled_by_default()
    test_window_basic_and_wrap()
    print("flat-exit + trading-window regression tests passed")
