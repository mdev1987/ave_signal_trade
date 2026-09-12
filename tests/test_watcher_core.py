"""Tests: buy parsing (Helius WS primary + Shyft fallback), status card,
pair scoring, MadeOnSol footprint.

Covers the live pipeline only — Tatum push was removed 2026-09-12.
"""

import sys
import pathlib
_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from watcher import parse_shyft_buys, WSOL  # noqa: E402 (Shyft polling fallback)
from helius_ws import parse_helius_tx, WSOL as H_WSOL  # noqa: E402 (primary WS path)
from pair_perf import pair_multiplier  # noqa: E402 (live scoring)
from madeonsol import MadeOnSolClient  # noqa: E402 (validator, in-memory)
from main import build_status        # noqa: E402

assert WSOL == H_WSOL == "So11111111111111111111111111111111111111112"

W = "64hP97Bwr5PubotcTeGgfhkFrGiLVVxT2kVo9M9b4AEz"
MINT = "8LPbe61qTA7r7QzVzEs57DSnEoXACvLUE1c45LTSpump"


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


def _helius_msg(bt=1000, wallet=W, pre_sol=3_000_000_000, post_sol=2_000_000_000,
                mints=((MINT, W, 0.0, 500.0),), err=None):
    """Minimal transactionSubscribe message (jsonParsed account keys)."""
    pre_tb, post_tb = [], []
    for i, (mint_, owner_, p0, p1) in enumerate(mints):
        def ui(v):
            return {"uiAmount": v, "decimals": 6}
        pre_tb.append({"accountIndex": 1 + i, "mint": mint_, "owner": owner_,
                       "uiTokenAmount": ui(p0)})
        post_tb.append({"accountIndex": 1 + i, "mint": mint_, "owner": owner_,
                        "uiTokenAmount": ui(p1)})
    return {"transaction": {
        "blockTime": bt,
        "transaction": {"message": {"accountKeys": [wallet, "other1", "other2"]}},
        "meta": {"err": err,
                 "preBalances": [pre_sol, 0, 0],
                 "postBalances": [post_sol, 0, 0],
                 "preTokenBalances": pre_tb,
                 "postTokenBalances": post_tb}}}


# --- Shyft fallback parser (still live in _sweep_wallet) ---

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


# --- Helius WS parser (primary path) ---

def test_helius_buy_detected():
    row = parse_helius_tx(W, _helius_msg())
    assert row is not None and row["ca"] == MINT and row["wallet"] == W
    assert abs(row["amount"] - 500.0) < 1e-6


def test_helius_no_sol_spent_ignored():
    msg = _helius_msg(pre_sol=1_000_000_000, post_sol=1_000_000_000,
                      mints=((MINT, W, 0.0, 500.0),))
    # no native decrease and no WSOL leg -> not a buy
    assert parse_helius_tx(W, msg) is None


def test_helius_failed_tx_ignored():
    assert parse_helius_tx(W, _helius_msg(err={"SomeError": []})) is None


def test_helius_unknown_wallet_ignored():
    assert parse_helius_tx("NobodyHere111", _helius_msg()) is None


# --- Live scoring helpers ---

def test_pair_multiplier_unknown_pair_neutral():
    mult, _note = pair_multiplier({}, ["walletA", "walletB"])
    assert mult == 1.0


def test_madeonsol_footprint_empty():
    c = MadeOnSolClient()
    fp = c.kol_footprint("NoSuchMint11111111111111111111111111111111")
    assert fp == {"buys": 0, "kols": 0, "max_winrate_7d": 0, "top_kol": ""}


def test_status_card_compact():
    st = {"uptime_s": 3725, "wallets": 30, "alerts": 14, "consensus": 2,
          "open": [{"symbol": "GOON", "mult": 2.4, "pnl_sol": 0.07}],
          "closed": [{"pnl_sol": 0.02}, {"pnl_sol": -0.01}],
          "start_balance_sol": 2.0,
          "feeds": {"helius_ws": True, "pumpapi": True, "dexscreener": True}}
    card = build_status(st)
    assert "Smart-Watch" in card and "PnL" in card and len(card.splitlines()) <= 12
    assert "helius_ws" in card and "tatum" not in card


if __name__ == "__main__":
    test_buy_detected_on_balance_increase()
    test_sell_ignored()
    test_failed_tx_ignored()
    test_other_wallet_ignored()
    test_helius_buy_detected()
    test_helius_no_sol_spent_ignored()
    test_helius_failed_tx_ignored()
    test_helius_unknown_wallet_ignored()
    test_pair_multiplier_unknown_pair_neutral()
    test_madeonsol_footprint_empty()
    test_status_card_compact()
    print("watcher-core tests passed (live pipeline)")
