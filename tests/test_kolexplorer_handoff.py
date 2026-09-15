"""Tests: Kolexplorer wallet handoff (score math + identity honesty).

Regression cover for the 2026-09-14 finding that aggregate kol_count with
a short identity list dropped strong signals on min_wallets (EYE: 11 KOLs,
score 6.25, <2 wallets passed) while bare KOL slugs polluted the pair
store (cooker+gh0stee+korean). Uses stub HTTP sessions — no network.
"""

import asyncio
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from kolexplorer import KOL_SLUG_TO_ADDR, KolexplorerFeed

ADDR_A = "AddrA" + "x" * 39
ADDR_B = "AddrB" + "x" * 39


def _feed(calls, weights, **kw):
    params = {"cookies": "x", "weights": weights, "default_weight": 0.5,
              "min_kols": 2, "on_signal": None}
    params.update(kw)
    feed = KolexplorerFeed(**params)

    async def _capture(ca, sym, mc, score, wallets, **k):
        calls.append({"ca": ca, "score": score, "wallets": wallets})

    feed._on_signal = _capture
    return feed


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload


class _Sess:
    def __init__(self, payload):
        self._payload = payload

    async def get(self, *a, **k):
        return _Resp(self._payload)


def _run(feed, payload, heatmap=True):
    feed._session = _Sess(payload)
    if heatmap:
        asyncio.run(feed._fetch_heatmap())
    else:
        asyncio.run(feed._fetch_monitor_feed())


def test_heatmap_score_and_mapping():
    calls = []
    feed = _feed(calls, {ADDR_A: 1.0})
    payload = {"ok": True, "tokens": [{
        "ca": "CA1", "sym": "T1", "kols": 3, "buy_mc": 9999,
        "pnl": 100, "vol": 50,
        "kol_list": [{"slug": "cupsey"}, {"slug": "ghost-x"}, {"slug": "ghost-y"}],
    }]}
    import kolexplorer
    kolexplorer.KOL_SLUG_TO_ADDR["cupsey-test"] = ADDR_A
    payload["tokens"][0]["kol_list"] = [
        {"slug": "cupsey-test"}, {"slug": "ghost-x"}, {"slug": "ghost-y"}]
    try:
        _run(feed, payload)
    finally:
        del kolexplorer.KOL_SLUG_TO_ADDR["cupsey-test"]
    assert len(calls) == 1
    # 1.0 mapped + 0.5 + 0.5 default
    assert abs(calls[0]["score"] - 2.0) < 1e-9
    assert ADDR_A in calls[0]["wallets"]
    # unmapped become namespaced pseudo-ids, never bare slugs
    assert "kol:ghost-x" in calls[0]["wallets"]
    assert "ghost-x" not in calls[0]["wallets"]


def test_ghost_padding_matches_kol_count():
    calls = []
    feed = _feed(calls, {})
    payload = {"ok": True, "tokens": [{
        "ca": "CA2abcdef", "sym": "T2", "kols": 5, "buy_mc": 100,
        "pnl": 0, "vol": 0, "kol_list": [{"slug": "only-one"}],
    }]}
    _run(feed, payload)
    assert len(calls) == 1
    # 1 pseudo-id + 4 per-token placeholders == kol_count
    assert len(calls[0]["wallets"]) == 5
    pads = [w for w in calls[0]["wallets"] if w.startswith("kol:unknown-CA2abc")]
    assert len(pads) == 4
    assert len(set(calls[0]["wallets"])) == 5  # no collisions


def test_min_kols_and_min_score_filters():
    calls = []
    feed = _feed(calls, {}, min_kols=3, min_score=1.5)
    payload = {"ok": True, "tokens": [
        {"ca": "LOW1", "sym": "L", "kols": 2, "buy_mc": 1, "pnl": 0,
         "vol": 0, "kol_list": [{"slug": "a"}, {"slug": "b"}]},
        {"ca": "LOW2", "sym": "L", "kols": 3, "buy_mc": 1, "pnl": 0,
         "vol": 0, "kol_list": [{"slug": "a"}]},  # score 0.5 < 1.5
    ]}
    _run(feed, payload)
    assert calls == []


def test_max_entry_mc_filter():
    calls = []
    feed = _feed(calls, {}, max_entry_mc=10000.0)
    payload = {"ok": True, "tokens": [{
        "ca": "BIG", "sym": "B", "kols": 5, "buy_mc": 999999,
        "pnl": 0, "vol": 0, "kol_list": [{"slug": "a"}] * 5,
    }]}
    _run(feed, payload)
    assert calls == []


def test_monitor_path_namespaced_and_padded():
    calls = []
    feed = _feed(calls, {ADDR_B: 2.0})
    import kolexplorer
    kolexplorer.KOL_SLUG_TO_ADDR["mon-test"] = ADDR_B
    payload = {"ok": True, "data": {"rows": [{
        "token_address": "CA3uvwxyz", "token_symbol": "M", "kol_count": 4,
        "entry_mc": 500, "total_pnl": 10, "total_buy_vol": 20,
        "kol_slugs_csv": "mon-test||Some Name",
        "kol_names_csv": "Mon||Some Name",
    }]}}
    try:
        _run(feed, payload, heatmap=False)
    finally:
        del kolexplorer.KOL_SLUG_TO_ADDR["mon-test"]
    assert len(calls) == 1
    assert abs(calls[0]["score"] - 2.5) < 1e-9  # 2.0 + 0.5 default
    wallets = calls[0]["wallets"]
    assert ADDR_B in wallets
    assert "kol:Some Name" in wallets  # namespaced, not bare
    assert "Some Name" not in wallets
    assert len(wallets) == 4  # padded to kol_count


def test_cooker_slug_maps_to_tracked_address():
    assert KOL_SLUG_TO_ADDR.get("cooker") == \
        "8deJ9xeUvXSJwicYptA9mHsU2rN2pDx37KWzkDkEXhU6"


def test_dedup_seen_tokens():
    calls = []
    feed = _feed(calls, {})
    payload = {"ok": True, "tokens": [{
        "ca": "DUP", "sym": "D", "kols": 2, "buy_mc": 1, "pnl": 0,
        "vol": 0, "kol_list": [{"slug": "a"}, {"slug": "b"}],
    }]}
    _run(feed, payload)
    _run(feed, payload)
    assert len(calls) == 1
