"""Tests: blind (liq-unchecked) open gating.

Regression cover for the 2026-09-15 review: over 2026-09-12..15, blind
entries (no DexScreener/DexPaprika snapshot, pc={}) went 11W/10L for
-0.060 SOL and carried every catastrophic loss (oracle_fail full loss +
five instant -25..-38% rugs), while data-confirmed entries netted +0.020.
Blind flow stayed open but at capped risk via LIQ_UNCHECKED_MAX_SOL.

2026-09-16 follow-up (71 paper closes): the cap did NOT fix expectancy —
blind went 28 closes for -0.097 (-0.0035/trade, 15 SL; capped era 8 for
-0.038 with 1 win) vs non-blind cabalspy -0.006/10. Blind flow is now
journal-only by default via LIQ_UNCHECKED_JOURNAL_ONLY (same pattern as
pumpapi_journal_only); the cap below applies only when re-enabled.
"""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from config import Settings, load_settings
from main import blind_open_size


def test_blind_cap_clips_large_adaptive():
    assert blind_open_size(0.06, 0.02, 0.025) == 0.02


def test_blind_cap_keeps_small_adaptive():
    assert blind_open_size(0.015, 0.02, 0.025) == 0.015


def test_blind_cap_falls_back_when_no_adaptive():
    assert blind_open_size(None, 0.02, 0.025) == 0.02
    assert blind_open_size(0.0, 0.02, 0.025) == 0.02


def test_blind_cap_nonpositive_disables():
    assert blind_open_size(0.06, 0.0, 0.025) == 0.06
    assert blind_open_size(0.06, -1.0, 0.025) == 0.06


def test_blind_cap_default_is_below_min_size():
    # The cap must bite: default max for blind opens sits below the normal
    # adaptive floor so unconfirmed entries always trade smaller.
    assert Settings().liq_unchecked_max_sol == 0.02
    assert Settings().liq_unchecked_max_sol <= Settings().size_sol_min


def test_blind_cap_env_override(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("LIQ_UNCHECKED_MAX_SOL=0.01\n")
    monkeypatch.delenv("LIQ_UNCHECKED_MAX_SOL", raising=False)
    s = load_settings(str(env_file))
    assert s.liq_unchecked_max_sol == 0.01


def test_blind_cap_env_garbage_falls_back(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("LIQ_UNCHECKED_MAX_SOL=junk\n")
    monkeypatch.delenv("LIQ_UNCHECKED_MAX_SOL", raising=False)
    s = load_settings(str(env_file))
    assert s.liq_unchecked_max_sol == Settings().liq_unchecked_max_sol


def test_blind_journal_only_default_true():
    assert Settings().liq_unchecked_journal_only is True


def test_blind_journal_only_env_false(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("LIQ_UNCHECKED_JOURNAL_ONLY=false\n")
    monkeypatch.delenv("LIQ_UNCHECKED_JOURNAL_ONLY", raising=False)
    assert load_settings(str(env_file)).liq_unchecked_journal_only is False


def test_blind_journal_only_env_garbage_is_falsy(tmp_path, monkeypatch):
    # get_bool treats anything not in (1/true/yes/on) as False.
    env_file = tmp_path / ".env"
    env_file.write_text("LIQ_UNCHECKED_JOURNAL_ONLY=junk\n")
    monkeypatch.delenv("LIQ_UNCHECKED_JOURNAL_ONLY", raising=False)
    assert load_settings(str(env_file)).liq_unchecked_journal_only is False
