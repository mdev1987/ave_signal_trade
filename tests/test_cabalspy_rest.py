"""Tests: CabalSpy REST timestamp parsing (UTC discipline)."""

import datetime
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from cabalspy_rest import parse_api_date


def test_z_suffix_is_utc():
    dt = parse_api_date("2026-09-14T12:00:00Z")
    assert dt.tzinfo is not None
    assert (dt.hour, dt.minute) == (12, 0)
    assert dt.utcoffset() == datetime.timedelta(0)


def test_explicit_offset_preserved():
    dt = parse_api_date("2026-09-14T14:00:00+02:00")
    assert dt.utcoffset() == datetime.timedelta(hours=2)


def test_naive_assumed_utc_never_local():
    dt = parse_api_date("2026-09-14T12:00:00")
    assert dt.tzinfo is not None
    assert dt.utcoffset() == datetime.timedelta(0)


def test_garbage_yields_epoch():
    assert parse_api_date("not-a-date") == datetime.datetime.fromtimestamp(
        0, tz=datetime.UTC)
    assert parse_api_date("") == datetime.datetime.fromtimestamp(
        0, tz=datetime.UTC)
    assert parse_api_date(None) == datetime.datetime.fromtimestamp(
        0, tz=datetime.UTC)
