"""Tests: wallet-quality weighting model + pair expectancy multiplier."""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from pair_perf import pair_key, pair_multiplier, penalty, update
from wallet_weights import build_weights


def _rec(addr, wr, pnl=0.0, trades=500, ok=True):
    return {"address": addr, "ok": ok, "win_rate": wr,
            "pnl_total": pnl, "trades": trades}


def _weights(recs, path, **kw):
    import json
    path.write_text(json.dumps(recs))
    return build_weights(str(path), **kw)


def test_weight_floor_zeroes_noise(tmp_path):
    w, _dflt = _weights([_rec("A", 30.0)], tmp_path / "w.json")
    assert w["A"] == 0.0
    assert _dflt == 0.5


def test_weight_full_win_plus_tiers(tmp_path):
    w, _ = _weights([
        _rec("FULL", 65.0, pnl=100.0, trades=500),       # base 1.0, no tier
        _rec("T1", 60.0, pnl=2_000_000.0, trades=500),   # base 1.0 * 1.25
        _rec("T2", 60.0, pnl=6_000_000.0, trades=500),   # base 1.0 * 1.5
    ], tmp_path / "w.json")
    assert w["FULL"] == 1.0
    assert w["T1"] == 1.25
    assert w["T2"] == 1.5


def test_weight_capped_and_confidence_shrunk(tmp_path):
    w, _ = _weights([
        _rec("CAP", 90.0, pnl=99_000_000.0, trades=1000),
        _rec("TINY", 60.0, pnl=0.0, trades=3),  # shrunk toward 0.5
    ], tmp_path / "w.json", max_weight=2.0, tier2_mult=3.0)
    assert w["CAP"] == 2.0  # 1.0 * 3.0 capped
    # 0.5 + (3/30)*(0.6-0.5) = 0.51 -> base (0.51-0.4)/0.2 = 0.55
    assert abs(w["TINY"] - 0.55) < 1e-9


def test_weight_skips_bad_records_and_missing_file(tmp_path):
    w, _dflt = _weights([
        {"address": "", "ok": True, "win_rate": 80.0},
        {"address": "BAD", "ok": False, "win_rate": 80.0},
        {"address": "ZERO", "ok": True, "win_rate": 0.0},
    ], tmp_path / "w.json")
    assert set(w) == {"ZERO"}
    assert w["ZERO"] == 0.0
    w2, d2 = build_weights(str(tmp_path / "nope.json"), default_weight=0.7)
    assert w2 == {} and d2 == 0.7


def test_pair_key_sorted_deduped():
    assert pair_key(["b", "a", "b"]) == "a+b"
    assert pair_key([]) == ""
    assert pair_key(["tg_signal"]) == "tg_signal"


def test_pair_update_counts_and_caps_history():
    perf = {}
    for i in range(12):
        update(perf, ["a", "b"], 0.01 if i % 2 else -0.01)
    d = perf["a+b"]
    assert d["trades"] == 12 and d["wins"] == 6
    assert len(d["history"]) == 10  # rolling cap
    assert abs(d["pnl"] - 0.0) < 1e-9


def test_pair_multiplier_unknown_and_profitable():
    assert pair_multiplier({}, ["x", "y"]) == (1.0, "")
    perf = {}
    update(perf, ["x", "y"], 0.05)
    update(perf, ["x", "y"], 0.05)
    assert pair_multiplier(perf, ["x", "y"])[0] == 1.0  # <3 trades
    update(perf, ["x", "y"], 0.05)
    assert pair_multiplier(perf, ["x", "y"]) == (1.0, "")  # profitable


def test_pair_multiplier_weak_tiers():
    perf = {}
    for _ in range(6):  # 0 wins, 6 losses -> very weak
        update(perf, ["p", "q"], -0.02)
    mult, note = pair_multiplier(perf, ["p", "q"])
    assert mult == 0.5 and "pair_weak" in note
    # recovery un-restricts: profitable rolling window
    for _ in range(4):
        update(perf, ["p", "q"], 0.10)
    assert pair_multiplier(perf, ["p", "q"])[0] == 1.0


def test_pair_multiplier_soft_tier():
    perf = {}
    update(perf, ["m", "n"], -0.02)
    update(perf, ["m", "n"], -0.02)
    update(perf, ["m", "n"], 0.01)  # 1/3 wins, negative pnl
    mult, note = pair_multiplier(perf, ["m", "n"])
    assert mult == 0.7 and "pair_soft" in note


def test_penalty_backcompat():
    perf = {}
    for _ in range(6):
        update(perf, ["p", "q"], -0.02)
    pen, _note = penalty(perf, ["p", "q"])
    assert abs(pen - 1.0) < 1e-9  # (1-0.5)*2.0
    assert penalty({}, ["a"])[0] == 0.0
