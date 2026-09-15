"""Tests: Helius transactionSubscribe buy parsing (pure function)."""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from helius_ws import parse_helius_tx

WALLET = "W" + "x" * 43
MINT = "M" + "y" * 43
WSOL = "So11111111111111111111111111111111111111112"


def _tx(pre_sol=1_000_000_000, post_sol=900_000_000, token_delta=None,
        err=None, object_keys=False, wsol_move=None):
    keys = [WALLET, "ATA11111111111111111111111111111111111111111"]
    if object_keys:
        keys = [{"pubkey": k} for k in keys]
    pre_tb, post_tb = [], []
    if token_delta is not None:
        if token_delta[0] > 0:
            pre_tb.append({"accountIndex": 1, "mint": MINT, "owner": WALLET,
                           "uiTokenAmount": {"uiAmount": token_delta[0]}})
        post_tb.append({"accountIndex": 1, "mint": MINT, "owner": WALLET,
                        "uiTokenAmount": {"uiAmount": token_delta[1]}})
    if wsol_move is not None:
        pre_tb.append({"accountIndex": 2, "mint": WSOL, "owner": WALLET,
                       "uiTokenAmount": {"uiAmount": wsol_move[0]}})
        post_tb.append({"accountIndex": 2, "mint": WSOL, "owner": WALLET,
                        "uiTokenAmount": {"uiAmount": wsol_move[1]}})
    return {"transaction": {
        "blockTime": 1234567890,
        "meta": {"err": err, "preBalances": [pre_sol, 0],
                 "postBalances": [post_sol, 0],
                 "preTokenBalances": pre_tb, "postTokenBalances": post_tb},
        "transaction": {"message": {"accountKeys": keys}},
    }}


def test_native_sol_buy_detected():
    buy = parse_helius_tx(WALLET, _tx(token_delta=(0.0, 50.0)))
    assert buy == {"wallet": WALLET, "ca": MINT, "ts": 1234567890.0,
                   "amount": 50.0}


def test_existing_position_topup_delta():
    buy = parse_helius_tx(WALLET, _tx(token_delta=(30.0, 80.0)))
    assert buy["amount"] == 50.0


def test_jsonparsed_object_keys():
    buy = parse_helius_tx(WALLET, _tx(token_delta=(0.0, 5.0), object_keys=True))
    assert buy is not None and buy["ca"] == MINT


def test_wsol_spend_path():
    buy = parse_helius_tx(WALLET, _tx(pre_sol=1_000_000_000, post_sol=1_000_000_000,
                                      token_delta=(0.0, 7.0), wsol_move=(1.0, 0.5)))
    assert buy is not None and buy["amount"] == 7.0


def test_failed_tx_ignored():
    assert parse_helius_tx(WALLET, _tx(token_delta=(0.0, 9.0), err={"InstructionError": []})) is None


def test_no_sol_spent_ignored():
    assert parse_helius_tx(WALLET, _tx(pre_sol=5, post_sol=5, token_delta=(0.0, 9.0))) is None


def test_sell_only_ignored():
    assert parse_helius_tx(WALLET, _tx(token_delta=(100.0, 20.0))) is None


def test_unrelated_wallet_ignored():
    assert parse_helius_tx("OTHER" + "z" * 39, _tx(token_delta=(0.0, 9.0))) is None


def test_empty_message():
    assert parse_helius_tx(WALLET, {}) is None
    assert parse_helius_tx(WALLET, {"transaction": {}}) is None
