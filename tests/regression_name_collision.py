"""Regression tests: same-name copycat collision resolution.

2026-08-25 Sinopec case: DRBT posted "Sinopec" (vH6Hyo…pump) with NO
cross-reference in its metadata links, so the link-based ca_mismatch detector
had nothing to catch. Two same-name twins existed on-chain — one +1000%, one
-95% — and the bot armed the -95% laggard. Resolution: when a name collides,
the twin with real liquidity+volume is the market leader (original).
"""

from paper_trader import PaperTrader


def test_leader_by_liquidity():
    cands = ["AAA", "BBB"]
    metrics = {"AAA": (56_000.0, 1_000.0), "BBB": (910_000.0, 40_000.0)}
    assert PaperTrader._pick_leader(cands, metrics) == "BBB"


def test_current_ca_can_lose_to_sibling():
    cands = ["AAA", "CURRENT"]
    metrics = {"AAA": (500_000.0, 20_000.0), "CURRENT": (4_000.0, 100.0)}
    assert PaperTrader._pick_leader(cands, metrics) == "AAA"


def test_missing_data_ranks_lowest():
    cands = ["A", "B"]
    metrics = {"A": (-1.0, 0.0), "B": (12.0, 0.0)}
    assert PaperTrader._pick_leader(cands, metrics) == "B"
    # both silent -> stable first-candidate
    assert PaperTrader._pick_leader(["X", "Y"], {}) in ("X", "Y")


if __name__ == "__main__":
    test_leader_by_liquidity()
    test_current_ca_can_lose_to_sibling()
    test_missing_data_ranks_lowest()
    print("name-collision regression tests passed")
