"""Regression tests: copycat-CA detection in DRBT posts.

2026-08-24 GrokBot/WASTED pattern: the posted "Mint:" is a copycat token whose
metadata links reference the ORIGINAL pump token (solscan.io/token/<mint>).
On-chain evidence (Helius getAsset): the copycat's own image URL embeds the
original's mint. The parser must surface those referenced addresses as
``alt_cas`` so PaperTrader can resolve them per CA_MISMATCH_POLICY.
"""

from parser import parse_signal

GROK = """GrokBot | GrokBot

Type: SPL Token 2022
Mint: FpLvhWo1se3S8G9fVb5iSgssJK1WrfE5es2AfUDypump
Supply: 1,000,000,000

📝 Description:
GrokBot

🔗 Links:
https://x.ai/bot
https://solscan.io/token/GeSfrQiscfsEv4Hx2TKaB9Nfid12qND1YYRYS1vSpump#metadata"""


def test_copycat_alt_detected():
    sig = parse_signal(GROK, unixtime=1771000000)
    assert sig.ca == "FpLvhWo1se3S8G9fVb5iSgssJK1WrfE5es2AfUDypump"
    assert sig.alt_cas == ("GeSfrQiscfsEv4Hx2TKaB9Nfid12qND1YYRYS1vSpump",)


def test_plain_launch_no_alts():
    plain = parse_signal(
        "Foo | FOO\nMint: 5WFAVHgS55owuTnLC9ppVKW7PgTVbotoZt9LUm7dpump\n"
        "https://x.com/someone/status/123",
        unixtime=1771000000,
    )
    assert plain.ca == "5WFAVHgS55owuTnLC9ppVKW7PgTVbotoZt9LUm7dpump"
    assert plain.alt_cas == ()


def test_dex_inferred_and_source():
    sig = parse_signal(GROK, unixtime=1771000000)
    assert sig.dex == "Pumpfunamm"
    sig.source = "@DRBTSolanaPF"  # set by telegram_feed, not the parser
    assert sig.source == "@DRBTSolanaPF"


if __name__ == "__main__":
    test_copycat_alt_detected()
    test_plain_launch_no_alts()
    test_dex_inferred_and_source()
    print("copycat parser regression tests passed")
