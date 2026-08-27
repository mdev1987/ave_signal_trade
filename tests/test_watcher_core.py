"""Tests: Shyft full-tx buy parsing + status card."""

import sys
import pathlib
_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from watcher import parse_shyft_buys  # noqa: E402
from main import build_status        # noqa: E402

W = "64hP97Bwr5PubotcTeGgfhkFrGiLVVxT2kVo9M9b4AEz"
MINT = "8LPbe61qTA7r7QzVzEs57DSnEoXACvLUE1c45LTSpump"
WSOL = "So11111111111111111111111111111111111112"


def _tx(bt, deltas):
    """deltas: {accountIndex: (mint, owner, pre, post)}"""
    pre, post = [], []
    for idx, (mint_, owner_, p0, p1) in deltas.items():
        def ui(v):
            return {"uiAmount": v, "decimals": 6}
        pre.append({"accountIndex": idx, "mint": mint_,
                    "owner": owner_, "uiTokenAmount": ui(p0)})
        post.append({"accountIndex": idx, "mint": mint_,
                     "owner": owner_, "uiTokenAmount": ui(p1)})
    return {"blockTime": bt, "meta": {
        "err": None, "preTokenBalances": pre, "postTokenBalances": post}}


def test_buy_detected_on_balance_increase():
    txs = [_tx(1000, {1: (MINT, W, 0.0, 500.0),
                      2: (WSOL, W, 3.0, 1.0)})]
    rows = parse_shyft_buys(W, txs)
    assert len(rows) == 1 and rows[0]["ca"] == MINT
    assert abs(rows[0]["amount"] - 500.0) < 1e-6


def test_sell_ignored():
    txs = [_tx(1000, {1: (MINT, W, 500.0, 100.0)})]
    assert parse_shyft_buys(W, txs) == []


def test_failed_tx_ignored():
    tx = _tx(1000, {1: (MINT, W, 0.0, 400.0)})
    tx["meta"]["err"] = {"SomeError": []}
    assert parse_shyft_buys(W, [tx]) == []


def test_other_wallet_ignored():
    txs = [_tx(1000, {1: (MINT, "OtherWallet111", 0.0, 900.0)})]
    assert parse_shyft_buys(W, txs) == []


def test_status_card_compact():
    st = {"uptime_s": 3725, "wallets": 30, "alerts": 14, "consensus": 2,
          "open": [{"symbol": "GOON", "mult": 2.4, "pnl_sol": 0.07}],
          "closed": [{"pnl_sol": 0.02}, {"pnl_sol": -0.01}],
          "start_balance_sol": 2.0, "feeds": {"tatum": True}}
    card = build_status(st)
    assert "Smart-Watch" in card and "PnL" in card and len(card.splitlines()) <= 12


if __name__ == "__main__":
    test_buy_detected_on_balance_increase()
    test_sell_ignored()
    test_failed_tx_ignored()
    test_other_wallet_ignored()
    test_status_card_compact()
    print("watcher-core tests passed (shyft parser)")
