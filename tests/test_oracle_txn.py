"""Tests: DexScreener buy/sell split plumbing (CVD-lite enabler)."""

import pathlib
import sys
from types import SimpleNamespace

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from dexscreener_oracle import DexScreenerClient


def _ns_pair(buys=70, sells=30):
    return SimpleNamespace(
        base_token_symbol="TKN", liquidity_usd=9000.0, market_cap=50000.0,
        fdv=None, price_usd=0.001, volume_5m=1000.0, volume_1h=5000.0,
        volume_24h=20000.0, buys_5m=buys, sells_5m=sells, dex_id="raydium",
        pair_address="PAIR", price_change_5m=5.0, price_change_1h=10.0,
        price_change_6h=0.0, price_change_24h=0.0, pair_created_at=123,
    )


def test_object_normalizer_keeps_split():
    d = DexScreenerClient._pair_to_dict(_ns_pair())
    assert d["txns_m5"] == 100
    assert d["buys_m5"] == 70
    assert d["sells_m5"] == 30


def test_dict_normalizer_keeps_split():
    raw = {
        "baseToken": {"symbol": "TKN"},
        "liquidity": {"usd": 9000.0},
        "marketCap": 50000.0,
        "priceUsd": 0.001,
        "volume": {"m5": 1000.0, "h1": 5000.0, "h24": 20000.0},
        "txns": {"m5": {"buys": 70, "sells": 30}},
        "dexId": "raydium",
        "pairAddress": "PAIR",
        "pairCreatedAt": 123,
        "priceChange": {"m5": 5.0, "h1": 10.0, "h6": 0.0, "h24": 0.0},
    }
    d = DexScreenerClient._dict_to_normalized(raw)
    assert (d["txns_m5"], d["buys_m5"], d["sells_m5"]) == (100, 70, 30)


def test_split_missing_degrades_to_zero():
    raw = {"baseToken": {"symbol": "T"}, "txns": {}}
    d = DexScreenerClient._dict_to_normalized(raw)
    assert (d["txns_m5"], d["buys_m5"], d["sells_m5"]) == (0, 0, 0)


def test_buy_pressure_ratio_derivable():
    d = DexScreenerClient._dict_to_normalized({
        "baseToken": {"symbol": "T"},
        "txns": {"m5": {"buys": 80, "sells": 20}},
    })
    pressure = d["buys_m5"] / max(1, d["txns_m5"])
    assert abs(pressure - 0.8) < 1e-9
