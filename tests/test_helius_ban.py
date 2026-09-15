"""Tests: Helius WS 429-ban persistence across restarts.

The bot reboots every ~2h; without persisted backoff each boot reset
_consec_429 to 0 and churned all keys with 30s retries before the circuit
breaker re-tripped (observed 2026-09-14).
"""

import json
import pathlib
import sys
import time

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from helius_ws import (
    _RECONNECT_CIRCUIT_AFTER,
    _RECONNECT_LONG_BAN_AFTER,
    HeliusWS,
    clear_ban_count,
    load_ban_count,
    save_ban_count,
)


def _write(path, consec, age_s):
    path.write_text(json.dumps({"consec_429": consec,
                                "ts": time.time() - age_s}))


def test_fresh_start_without_state(tmp_path):
    ws = HeliusWS(api_keys=["k1", "k2"],
                  ban_state_file=str(tmp_path / "ban.json"))
    assert ws._consec_429 == 0
    assert not ws.degraded


def test_long_ban_resumes_hourly_probes(tmp_path):
    p = tmp_path / "ban.json"
    _write(p, _RECONNECT_LONG_BAN_AFTER + 5, age_s=600)
    ws = HeliusWS(api_keys=["k1"], ban_state_file=str(p))
    assert ws._consec_429 == _RECONNECT_LONG_BAN_AFTER + 5
    assert ws.degraded


def test_stale_long_ban_steps_down_to_circuit(tmp_path):
    p = tmp_path / "ban.json"
    _write(p, _RECONNECT_LONG_BAN_AFTER + 5, age_s=7200)
    assert load_ban_count(str(p)) == _RECONNECT_CIRCUIT_AFTER


def test_expired_state_ignored(tmp_path):
    p = tmp_path / "ban.json"
    _write(p, _RECONNECT_LONG_BAN_AFTER + 5, age_s=26 * 3600)
    assert load_ban_count(str(p)) == 0


def test_day_old_long_ban_still_resumes_quietly(tmp_path):
    # Regression: the 2026-09-14/15 quota ban outlasted 22h. A ban saved
    # 20h ago must still resume at circuit level (15-min probes), not
    # reset to aggressive 30s key-churn.
    p = tmp_path / "ban.json"
    _write(p, _RECONNECT_LONG_BAN_AFTER + 5, age_s=20 * 3600)
    assert load_ban_count(str(p)) == _RECONNECT_CIRCUIT_AFTER


def test_corrupt_state_is_fail_open(tmp_path):
    p = tmp_path / "ban.json"
    p.write_text("not json{")
    assert load_ban_count(str(p)) == 0


def test_save_clear_roundtrip(tmp_path):
    p = str(tmp_path / "ban.json")
    save_ban_count(_RECONNECT_CIRCUIT_AFTER + 2, p)
    assert load_ban_count(p) == _RECONNECT_CIRCUIT_AFTER + 2
    clear_ban_count(p)
    assert load_ban_count(p) == 0
