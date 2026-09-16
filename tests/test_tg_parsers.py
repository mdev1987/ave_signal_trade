"""Tests: Telegram signal parsers + glued-CA extraction.

Regression cover for the 2026-09-15 finding that `$SYMBOL<CA>` glued posts
(e.g. ``$Retire5EwSzz...U5pump``) produced an invalid CA with the symbol
prefix swallowed in — silently breaking the only profitable source.
Fixtures use real @memetrackersol message shapes.
"""

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from tg_signal_feed import (
    _parse_value,
    extract_mint,
    memetracker_chase_blocked,
    parse_memetracker_signal,
    parse_tg_signal,
)

MINT = "5EwSzzVyQmCgs5jaAHcq43VvUqsik6pBVLGhhZU5pump"

# Real @memetrackersol shape (glued $SYMBOL<CA>, copycat suffix, red migrated)
RETIRE_GLUED = (
    "\U0001f514Retire $Retire" + MINT + " \U0001f4cb\u23f05min \u00b7 \U0001f45bInsiders: 4"
    "\U0001f4b5USD       $0.0000217 (+316.96%)\U0001f4b0MC         $21.7K"
    "\U0001f4b8LP           $5.6K\U0001f4caTxns       \u24b7 270 / \u24c8 148"
    "\U0001f4c8Vol \u24b7 $14.2K / \u24c8 $10.0K\U0001f642Hold       159"
    "\U0001f50eRug Score: 29\u2514Copycat token\u2708\ufe0fMigrated: \U0001f534"
)

RETIRE_SEPARATE = (
    "\U0001f514Retire $Retire\n" + MINT + "\n\U0001f4cb"
    "\U0001f4b0MC $21.7K \U0001f4b8LP $5.6K \U0001f642Hold 159"
)

TITTIES = (
    "\U0001f514titties $TITTIESDKoyn9FjoGCcZGbGqVxNyPY8N35rjVCru2czgiFDpump \U0001f4cb"
    "\u23f01min \u00b7 \U0001f45bInsiders: 5\U0001f4b5USD $0.0000117 (+323.64%)"
    "\U0001f4b0MC $11.7K \U0001f4b8LP $6.1K \U0001f642Hold 165"
    "\U0001f50eRug Score: 1 \u2708\ufe0fMigrated: \U0001f7e2"
)


def test_extract_mint_glued_takes_last_44():
    ca, pos = extract_mint("$Retire" + MINT + " ")
    assert ca == MINT
    assert pos == len("$Retire")


def test_extract_mint_exact_run_unchanged():
    ca, pos = extract_mint("foo " + MINT + " bar")
    assert (ca, pos) == (MINT, 4)


def test_extract_mint_none():
    assert extract_mint("no mint here!!") == ("", -1)
    assert extract_mint("") == ("", -1)


def test_memetracker_glued_full_row():
    r = parse_memetracker_signal(RETIRE_GLUED)
    assert r["ca"] == MINT
    assert r["symbol"] == "Retire"
    assert r["mc"] == 21700.0
    assert r["liq"] == 5600.0
    assert r["holders"] == 159
    assert r["insiders"] == 4
    assert r["rug_score"] == 29
    assert r["migrated"] is False
    assert abs(r["price_usd"] - 0.0000217) < 1e-9
    assert abs(r["pc_1h"] - 316.96) < 1e-6


def test_memetracker_separate_line_format():
    r = parse_memetracker_signal(RETIRE_SEPARATE)
    assert r["ca"] == MINT
    assert r["symbol"] == "Retire"
    assert r["mc"] == 21700.0


def test_memetracker_green_migrated():
    r = parse_memetracker_signal(TITTIES)
    assert r["symbol"] == "TITTIES"
    assert r["migrated"] is True
    assert r["rug_score"] == 1
    assert r["holders"] == 165


def test_memetracker_rejects_garbage():
    assert parse_memetracker_signal("") is None
    assert parse_memetracker_signal(None) is None
    assert parse_memetracker_signal("hello world, no mint!!") is None


def test_memetracker_chase_blocked():
    """Vertical-chase guard: tokens already up 10x+/1h are chases, not
    entries (paper 2026-09-12..15: 4/4 such memetracker opens lost)."""
    assert memetracker_chase_blocked(1757.5, 1000.0) is True   # GROYPER
    assert memetracker_chase_blocked(3943.5, 1000.0) is True   # PAID
    assert memetracker_chase_blocked(1000.01, 1000.0) is True
    assert memetracker_chase_blocked(1000.0, 1000.0) is False  # boundary: not over
    assert memetracker_chase_blocked(538.4, 1000.0) is False   # DOOM (winner)
    assert memetracker_chase_blocked(114.1, 1000.0) is False
    assert memetracker_chase_blocked(-50.0, 1000.0) is False   # dumps never chase
    assert memetracker_chase_blocked(0.0, 1000.0) is False     # missing pc never blocks
    assert memetracker_chase_blocked(None, 1000.0) is False    # garbage never blocks
    assert memetracker_chase_blocked(99999.0, 0) is False      # cap<=0 disables
    assert memetracker_chase_blocked(99999.0, -1) is False


GMGN_MSG = (
    "$DOGE(Dogecoin) fresh pump\n"
    + MINT + "\n"
    + "MCP: $25.9K Liq: 10 SOL($9.3K) Holders: 258\n"
    + "\U0001f4c8 1h | 6h: 12.5% | 45.2%\n"
    + "pump in progress"
)


def test_gmgn_parser_row():
    r = parse_tg_signal(GMGN_MSG)
    assert r["ca"] == MINT
    assert r["symbol"] == "DOGE"
    assert r["name"] == "Dogecoin"
    assert r["mc"] == 25900.0
    assert r["liq"] == 9300.0
    assert r["holders"] == 258
    assert r["signal_type"] == "pump"


def test_gmgn_signal_types():
    # NOTE: fixture mints must not contain "pump" — the classifier checks
    # "pump" before "koth", so a pump-suffixed mint always types as "pump".
    neutral = "B" * 43
    base = "$X(Y)\n" + neutral + "\n"
    assert parse_tg_signal(base + "dev sold everything")["signal_type"] == "dev_sold"
    assert parse_tg_signal(base + "new pool listing")["signal_type"] == "new_pool"
    assert parse_tg_signal(base + "king of the hill")["signal_type"] == "koth"
    assert parse_tg_signal(base + "liquidity burn done")["signal_type"] == "burn"
    assert parse_tg_signal(base + "just vibing")["signal_type"] == "unknown"


def test_gmgn_rejects_garbage():
    assert parse_tg_signal("") is None
    assert parse_tg_signal("no ca here") is None


def test_parse_value_units():
    assert _parse_value("1.5K") == 1500.0
    assert _parse_value("2.3M") == 2300000.0
    assert _parse_value("1B") == 1000000000.0
    assert _parse_value("50%") == 50.0
    assert _parse_value("100") == 100.0
    assert _parse_value("") == 0.0
    assert _parse_value("abc") == 0.0
