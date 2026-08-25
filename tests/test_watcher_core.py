"""Tests: CA-safe buy extraction, trail math, status card."""

from watcher import extract_buy

import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from main import build_status  # noqa: E402


def _swap(ts, typ="buy", usd="250", base="ABCpump", label="ABC/SOL"):
    return {"blockTimestamp": ts, "transactionType": typ,
            "baseToken": {"address": base, "symbol": base[:3]},
            "sold": {"usdAmount": usd}, "pairLabel": label}


def test_extract_buy_shapes():
    rows = [_swap("2026-08-26T10:00:00Z"),
            _swap("2026-08-26T09:59:00Z", typ="sell"),
            {"blockTimestamp": "2026-08-26T09:58:00Z", "transactionType": "buy",
             "baseToken": "XYZpump", "sold": {"usdAmount": "80"},
             "pairLabel": "XYZ/SOL"}]
    out = extract_buy(rows, after_ts=0)
    assert len(out) == 2
    assert out[0]["ca"] == "ABCpump" and out[0]["symbol"] == "ABC"
    assert out[1]["ca"] == "XYZpump" and out[1]["symbol"] == "XYZ"


def test_extract_buy_respects_after_ts():
    rows = [_swap("2026-08-26T10:00:00Z"), _swap("2026-08-26T09:00:00Z")]
    import datetime as dt
    after = dt.datetime(2026, 8, 26, 9, 30, tzinfo=dt.timezone.utc).timestamp()
    assert len(extract_buy(rows, after_ts=after)) == 1


def test_status_card_compact():
    st = {"uptime_s": 3725, "wallets": 30, "alerts": 14, "consensus": 2,
          "open": [{"symbol": "GOON", "mult": 2.4, "pnl_sol": 0.07}],
          "closed": [{"pnl_sol": 0.02}, {"pnl_sol": -0.01}],
          "start_balance_sol": 2.0, "feeds": {"tatum": True}}
    card = build_status(st)
    assert "Smart-Watch" in card and "PnL" in card
    assert "🟢" in card and "🔥2" in card
    assert len(card.splitlines()) <= 12


if __name__ == "__main__":
    test_extract_buy_shapes()
    test_extract_buy_respects_after_ts()
    test_status_card_compact()
    print("watcher-core tests passed")
