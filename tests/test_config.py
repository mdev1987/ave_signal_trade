"""Tests: config parsing + Settings (incl. removal lock-ins).

Locks in the AveSignalMonitor + duplicate TG session removals: the
corresponding Settings attributes must NOT exist anymore.
"""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from config import (
    get_bool,
    get_csv,
    get_float,
    get_int,
    load_settings,
    parse_ladder,
)


def test_parse_ladder_triples():
    out = parse_ladder({}, "TP_LADDER", "1.2:0.4:0.15,3.0:0.2:0.25")
    assert out == [(1.2, 0.4, 0.15), (3.0, 0.2, 0.25)]


def test_parse_ladder_legacy_pairs_default_trail():
    out = parse_ladder({}, "TP_LADDER", "1.3:0.4,1.8:0.3")
    assert out == [(1.3, 0.4, 0.30), (1.8, 0.3, 0.30)]


def test_parse_ladder_garbage_falls_back():
    # Unparseable env does NOT reuse the `default` string — it falls back to
    # the hardcoded strategy ladder (fail-safe for a trading bot).
    out = parse_ladder({"TP_LADDER": "junk,,1.5:xx"}, "TP_LADDER", "2.0:0.5:0.2")
    assert len(out) == 9 and out[0] == (1.5, 0.30, 0.15)


def test_get_bool_variants():
    for v in ("1", "true", "yes", "on", "True", "YES"):
        assert get_bool({"K": v}, "K", False) is True
    for v in ("0", "false", "no", "off"):
        assert get_bool({"K": v}, "K", True) is False
    # empty string counts as unset -> default applies
    assert get_bool({"K": ""}, "K", True) is True
    assert get_bool({"K": ""}, "K", False) is False
    assert get_bool({}, "MISSING", True) is True


def test_get_int_float_invalid_fall_back():
    assert get_int({"K": "abc"}, "K", 7) == 7
    assert get_float({"K": "xyz"}, "K", 1.5) == 1.5
    assert get_int({"K": "12"}, "K", 0) == 12
    assert get_float({"K": "0.25"}, "K", 0.0) == 0.25


def test_get_csv():
    assert get_csv({"K": "a, b,,c "}, "K", "") == ["a", "b", "c"]
    assert get_csv({}, "K", "x,y") == ["x", "y"]


def test_removed_sources_have_no_settings():
    s = load_settings()
    for gone in ("avesm_enabled", "avesm_channel", "avesm_session",
                 "avesm_min_mc", "avesm_min_kols", "avesm_min_buy_sol",
                 "avesm_max_mc", "tg_session_name"):
        assert not hasattr(s, gone), gone


def test_helius_ws_toggle_defaults_off():
    s = load_settings()
    assert s.helius_ws_enabled is False


def test_txn_floor_setting():
    s = load_settings()
    assert s.open_min_txns_m5 == 20.0


def test_birdeye_settings():
    s = load_settings()
    assert s.birdeye_enabled is True
    assert s.birdeye_min_credits == 0.0


def test_memetracker_chase_guard_setting(monkeypatch, tmp_path):
    # hermetic: empty env file + patched os.environ (repo .env must not leak in)
    empty = str(tmp_path / "empty.env")
    monkeypatch.delenv("MEMETRACKER_MAX_PC1H_PCT", raising=False)
    assert load_settings(empty).memetracker_max_pc1h_pct == 1000.0
    monkeypatch.setenv("MEMETRACKER_MAX_PC1H_PCT", "500")
    assert load_settings(empty).memetracker_max_pc1h_pct == 500.0
    # garbage falls back to default, never crashes the gate
    monkeypatch.setenv("MEMETRACKER_MAX_PC1H_PCT", "junk")
    assert load_settings(empty).memetracker_max_pc1h_pct == 1000.0
    # non-positive disables the guard
    monkeypatch.setenv("MEMETRACKER_MAX_PC1H_PCT", "0")
    assert load_settings(empty).memetracker_max_pc1h_pct == 0.0
