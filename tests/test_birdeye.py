"""Tests: Birdeye holder-cohort + top-trader summarizers and client."""

import asyncio
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from birdeye import (
    BirdeyeClient,
    summarize_holder_profile,
    summarize_top_traders,
)


def _profile(insider=5.0, bundler=10.0, smart=3.0, kol=1.0, top10=32.0):
    def row(tag, pct):
        return {"tag": tag, "holder_count": 2, "hold_amount": "1",
                "percent_of_supply": pct, "buy_volume_usd": "10",
                "sell_volume_usd": "4"}
    return {
        "token": {"top10_holder": {"percent_of_supply": top10}},
        "holder_summary": {"total_holder": 9},
        "tags": [row("insider", insider), row("bundler", bundler),
                 row("sniper", 1.5), row("dev", 0.5),
                 row("smart_trader", smart), row("kol", kol)],
    }


def test_holder_profile_cohort_math():
    s = summarize_holder_profile(_profile())
    assert s["top10_pct"] == 32.0
    assert s["insider_pct"] == 5.0 and s["bundler_pct"] == 10.0
    assert s["risk_pct"] == 17.0  # 5+10+1.5+0.5
    assert s["good_pct"] == 4.0   # 3+1
    assert s["labeled_holders"] == 9


def test_holder_profile_degrades_on_garbage():
    assert summarize_holder_profile({})["risk_pct"] == 0.0
    assert summarize_holder_profile(None)["good_pct"] == 0.0
    assert summarize_holder_profile({"tags": [{"tag": "insider"}]})["insider_pct"] == 0.0
    assert summarize_holder_profile({"tags": "nope"})["risk_pct"] == 0.0


def test_top_traders_exited_fraction_and_tags():
    rows = [
        {"holdVolumeUsd": 0, "tags": ["bundler", "sniper"],
         "volumeBuyUSD": 100.0, "volumeSellUSD": 90.0},
        {"holdVolumeUsd": 25.5, "tags": ["smart_trader"],
         "volumeBuyUSD": 50.0, "volumeSellUSD": 10.0},
        {"holdVolumeUsd": 0, "tags": [],
         "volumeBuyUSD": 5.0, "volumeSellUSD": 5.0},
    ]
    s = summarize_top_traders(rows)
    assert s["n"] == 3
    assert s["exited_frac"] == round(2 / 3, 3)
    assert s["tags"] == {"bundler": 1, "sniper": 1, "smart_trader": 1}
    assert s["buy_usd"] == 155.0 and s["sell_usd"] == 105.0


def test_top_traders_empty():
    assert summarize_top_traders([]) == {
        "n": 0, "exited_frac": 0.0, "tags": {}, "buy_usd": 0.0, "sell_usd": 0.0}
    assert summarize_top_traders(None)["n"] == 0


class _Resp:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self._body = body if body is not None else {"success": True, "data": {}}

    def json(self):
        return self._body


class _FakeHttp:
    def __init__(self, resp):
        self._resp = resp
        self.calls = 0

    async def get(self, *a, **k):
        self.calls += 1
        return self._resp

    async def aclose(self):
        pass


def _client(resp, **kw):
    c = BirdeyeClient(api_key="k", **kw)
    c._client = _FakeHttp(resp)
    return c


def test_client_disabled_without_key():
    c = BirdeyeClient(api_key="")
    assert c.enabled is False
    assert asyncio.run(c.holder_profile("X")) is None
    assert c.calls == 0


def test_client_caches_and_counts():
    c = _client(_Resp(body={"success": True, "data": {"a": 1}}))
    r1 = asyncio.run(c.holder_profile("MINT"))
    r2 = asyncio.run(c.holder_profile("MINT"))
    assert r1 == {"a": 1} and r2 == {"a": 1}
    assert c.calls == 1 and c.cached == 1


def test_client_fail_open_on_http_and_bad_body():
    c = _client(_Resp(status=500, body={}))
    assert asyncio.run(c.holder_profile("M")) is None
    c2 = _client(_Resp(body={"success": False}))
    assert asyncio.run(c2.top_traders("M")) == []
    assert c.errors == 1 and c2.errors == 1


def test_enrich_combines_and_journals_shape():
    async def go():
        c = _client(_Resp(body={"success": True, "data": {}}))
        async def prof(mint):
            return {"token": {"top10_holder": {"percent_of_supply": 20.0}},
                    "holder_summary": {"total_holder": 4},
                    "tags": [{"tag": "kol", "holder_count": 1,
                              "percent_of_supply": 2.0}]}
        async def tops(mint, limit=10):
            return [{"holdVolumeUsd": 5.0, "tags": [],
                     "volumeBuyUSD": 9.0, "volumeSellUSD": 1.0}]
        c.holder_profile = prof
        c.top_traders = tops
        return await c.enrich("MINT")
    out = asyncio.run(go())
    assert out["profile"]["kol_pct"] == 2.0
    assert out["flow"]["exited_frac"] == 0.0


def test_enrich_none_when_both_empty():
    out = asyncio.run(_enrich_with_helpers())
    assert out is None


async def _enrich_with_helpers():
    c = BirdeyeClient(api_key="k")

    async def _none(mint):
        return None

    async def _list(mint, limit=10):
        return []

    c.holder_profile = _none
    c.top_traders = _list
    return await c.enrich("MINT")
