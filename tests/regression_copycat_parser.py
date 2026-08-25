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


FULL_DRBT = """[GrokBot](bold) | [GrokBot](code)

[Type](bold): SPL Token 2022
[Mint](https://solscan.io/account/FpLvhWo1se3S8G9fVb5iSgssJK1WrfE5es2AfUDypump): FpLvhWo1se3S8G9fVb5iSgssJK1WrfE5es2AfUDypump
Supply: 1,000,000,000
Deci: 9 | Fee: 0%

👨🏻‍🎨 Owner ([G8stDjATNckEYdJoj9ZJnGXjqWoJp2zCpmjiycoZJBwm](https://solscan.io/account/G8stDjATNckEYdJoj9ZJnGXjqWoJp2zCpmjiycoZJBwm)):
├─ G8stDjATNckEYdJoj9ZJnGXjqWoJp2zCpmjiycoZJBwm
├─ From: BJ..fko ([BJG5VdZw5pq2PkrGbNUDAUPXM2xjs2AY8c3veBBirfko](https://solscan.io/account/BJG5VdZw5pq2PkrGbNUDAUPXM2xjs2AY8c3veBBirfko)) (1000+ TX | 0 SOL)
├─ TX: 1 | Balance: 782.53 SOL
└─ Age: 55 seconds ago

📝 Description:
GrokBot

🔗 Links:
https://x.ai/bot
[https://x.com/elonmusk/status/2091191054439682373](https://x.com/elonmusk/status/2091191054439682373)
[https://solscan.io/token/GeSfrQiscfsEv4Hx2TKaB9...](https://solscan.io/token/GeSfrQiscfsEv4Hx2TKaB9Nfid12qND1YYRYS1vSpump#metadata)"""


def test_owner_wallet_links_are_not_alts():
    """2026-08-25 regression: owner-block solscan.io/ACCOUNT links (creator +
    funding wallet) were captured as alt_cas on EVERY post, so policy=link
    substituted the trading CA to a wallet address -> 204/204 mismatches,
    0 arms. Only the Links section counts, and only token pages."""
    sig = parse_signal(FULL_DRBT, unixtime=1771000000)
    assert sig.ca == "FpLvhWo1se3S8G9fVb5iSgssJK1WrfE5es2AfUDypump"
    assert sig.alt_cas == ("GeSfrQiscfsEv4Hx2TKaB9Nfid12qND1YYRYS1vSpump",)


def test_no_links_section_no_alts():
    body = FULL_DRBT.split("🔗 Links:")[0]
    sig = parse_signal(body, unixtime=1771000000)
    assert sig.alt_cas == ()


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
