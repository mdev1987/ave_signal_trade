"""Regression tests for price_feed event parsing.

Catches the 2026-08-24 outage: pumpapi changed the ``pool`` field from a dict
to a string ("pump-amm"). ``_mint_of`` previously did
``(event.get("pool") or {}).get("mint")`` which raised ``AttributeError`` on
the *first* stream event, killed the feed task, and — because no buy events
ever reached ``trader.on_event`` — produced 188 arms and 0 open positions.
"""

import asyncio

from price_feed import PriceFeed


def test_mint_of_string_pool():
    ev = {"action": "buy", "mint": "ABC123pump", "pool": "pump-amm", "price": 1e-9}
    assert PriceFeed._mint_of(ev) == "ABC123pump"


def test_mint_of_legacy_dict_pool():
    ev = {"action": "buy", "mint": "DEF456", "pool": {"mint": "DEF456"}}
    assert PriceFeed._mint_of(ev) == "DEF456"


def test_mint_of_missing():
    assert PriceFeed._mint_of({"action": "transfer"}) is None
    assert PriceFeed._mint_of({"pool": "pump-amm"}) is None


def test_handle_does_not_raise_on_string_pool():
    """The original crash: _handle threw on the first buy with a string pool."""
    feed = PriceFeed()
    # No on_event callback needed; we just must not raise.
    ev = {
        "action": "buy",
        "mint": "GHI789",
        "pool": "pump-amm",
        "price": 2.5e-9,
        "quoteInPool": 100.0,
        "burnedLiquidity": 5,
        "mintAuthority": None,
        "freezeAuthority": None,
    }
    # Should process without raising (pool state folded, price stored).
    asyncio.run(feed._handle(ev))
    assert feed.pool_state("GHI789") is not None
    assert feed.prices.get("GHI789") == 2.5e-9


if __name__ == "__main__":
    test_mint_of_string_pool()
    test_mint_of_legacy_dict_pool()
    test_mint_of_missing()
    test_handle_does_not_raise_on_string_pool()
    print("all price_feed regression tests passed")
