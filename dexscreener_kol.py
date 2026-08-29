#!/usr/bin/env python3
"""Extract Top Holders and KOL / Top-Traders for DexScreener tokens.

DexScreener's public API (and every wrapper lib: dexscreener-python, dexscreen,
...) only exposes pairs / price / search / boosts. The "Holders" and
"Top Traders / KOL" tabs are rendered CLIENT-SIDE, so the only way to get both
straight from a URL is to render the page in a headless browser and scrape the
live DOM. That is what ``--mode page`` (single token) and ``--mode gainers``
(whole Top-Gainers page 1) do.

Modes
-----
  page     single token URL -> holders + KOLs (live DOM)
  gainers  Top-Gainers page URL -> every token on page 1 -> holders + KOLs
  api      hybrid fallback (needs API keys; see file header history)

The browser binary defaults to /opt/google/chrome/chrome (symlink you created)
and can be overridden with --chrome or CHROME_PATH.

OUTPUT
------
  --out FILE   write a JSON discovery file (token -> holders/kols)
  for gainers, results are written incrementally so a partial run is preserved.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from pathlib import Path

import httpx

# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
def _env() -> dict:
    env: dict = {}
    p = Path(".env")
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    env.update(os.environ)
    return env


_ADDR = re.compile(r"[1-9A-HJ-NP-Za-km-z]{30,}")
_URL_RE = re.compile(r"dexscreener\.com/([a-z0-9-]+)/([1-9A-HJ-NP-Za-km-z]{30,})")

DEFAULT_CHROME = os.environ.get("CHROME_PATH", "/opt/google/chrome/chrome")
DEFAULT_GAINERS = ("https://dexscreener.com/solana?rankBy=priceChangeH24&order=desc"
                   "&min24HTxns=50&min24HVol=10000&minLiq=25000&profile=1")


# --------------------------------------------------------------------------- #
# Playwright live-DOM scraper
# --------------------------------------------------------------------------- #
async def _new_page(ctx):
    page = await ctx.new_page()
    await page.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return page


async def _goto(page, url: str, timeout: int = 60_000, tries: int = 4) -> None:
    last: Exception | None = None
    for i in range(tries):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            await page.wait_for_timeout(3_000)
    raise last or RuntimeError("goto failed")


async def scrape_token(page, url: str, dump_html: str | None = None
                       ) -> dict:
    """Render one token page and pull holders + KOLs from the live DOM."""
    await _goto(page, url)
    try:
        await page.wait_for_selector("text=Holders", timeout=20_000)
    except Exception:
        pass
    await page.wait_for_timeout(4_000)
    if dump_html:
        Path(dump_html).write_text(await page.content())

    data = await page.evaluate(r"""async () => {
      const sleep = ms => new Promise(r => setTimeout(r, ms));
      for (let i=0;i<10;i++){ window.scrollBy(0,2000); await sleep(150); }
      await sleep(1000);
      const panels = [...document.querySelectorAll('div')].filter(
        e => /KOLs/.test(e.textContent) && e.querySelector('a[href*="solscan.io/account"]'));
      const panel = panels.sort((a,b)=> a.textContent.length - b.textContent.length)[0];
      if(!panel) return {error:'no-panel'};
      const click = t => { const b=[...panel.querySelectorAll('button,[role=tab]')]
        .find(x => x.textContent.trim().startsWith(t)); if(b){b.click();return true;} return false; };
      click('Top Traders'); await sleep(500);
      click('KOLs'); await sleep(1800);
      const kol = [...new Set([...panel.querySelectorAll('a[href*="solscan.io/account"]')]
        .map(a => a.href.split('/account/')[1]))].filter(Boolean);
      click('Holders'); await sleep(1800);
      const links = [...panel.querySelectorAll('a[href*="solscan.io/account"]')];
      const holders = []; const seen = new Set();
      for (const a of links){
        let el = a; while(el && !/%/.test(el.textContent)) el = el.parentElement;
        const t = el ? el.innerText : '';
        const pm = t.match(/([\d.]+)%/); const rm = t.match(/#(\d+)/);
        const addr = a.href.split('/account/')[1];
        if(!addr || seen.has(addr) || kol.includes(addr)) continue;
        seen.add(addr);
        holders.push({rank: rm?+rm[1]:null, address: addr, pct: pm?+pm[1]:null});
      }
      const m = document.title.match(/^([^\$]+?)\s*[\$]/);
      return {symbol: m?m[1].trim():null, kol, holders};
    }""")
    return data


async def scrape_gainers(browser, url: str, limit: int, out: str | None = None
                       ) -> list[dict]:
    ctx = await browser.new_context(
        user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        viewport={"width": 1440, "height": 900}, locale="en-US")
    page = await _new_page(ctx)
    await _goto(page, url)
    await page.wait_for_timeout(4_000)
    # the gainers list is virtualized: scroll to lazy-load all rows
    await page.evaluate(r"""async () => {
      const sleep = ms => new Promise(r => setTimeout(r, ms));
      for (let i=0;i<25;i++){ window.scrollBy(0,3000); await sleep(250); }
      window.scrollTo(0,0); await sleep(500);
    }""")
    await page.wait_for_timeout(3_000)
    tokens = await page.evaluate(r"""() => {
      const out=[]; const seen=new Set();
      for(const a of document.querySelectorAll('a[href^="/solana/"]')){
        const m=a.href.match(/dexscreener\.com\/solana\/([1-9A-HJ-NP-Za-km-z]{30,})/);
        if(m && !seen.has(m[1])){
          seen.add(m[1]);
          const sym=a.innerText.split('/')[0].replace(/^#\d+\s*/,'').trim();
          out.push({url:'https://dexscreener.com/solana/'+m[1], symbol:sym});
        }
      }
      return out;
    }""")
    await page.close()
    tokens = tokens[:limit]
    print(f"[gainers] found {len(tokens)} tokens on page 1", file=sys.stderr)

    results = []
    for i, tk in enumerate(tokens, 1):
        pg = await _new_page(ctx)
        try:
            d = await scrape_token(pg, tk["url"])
            d["url"] = tk["url"]; d["symbol"] = d.get("symbol") or tk.get("symbol")
            d["rank_on_page"] = i
            results.append(d)
            print(f"  [{i}/{len(tokens)}] {d.get('symbol')} -> "
                  f"{len(d.get('holders',[]))} holders, {len(d.get('kol',[]))} KOLs",
                  file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            print(f"  [{i}/{len(tokens)}] {tk['url']} FAILED: {exc}", file=sys.stderr)
            results.append({"url": tk["url"], "symbol": tk.get("symbol"),
                            "rank_on_page": i, "error": str(exc)})
        finally:
            await pg.close()
        if out:  # incremental write so partial runs survive
            Path(out).write_text(json.dumps(results, indent=2))
    await ctx.close()
    return results


# --------------------------------------------------------------------------- #
# API hybrid mode (fallback, unchanged behaviour)
# --------------------------------------------------------------------------- #
async def get_token_price(chain: str, token: str, env: dict) -> dict:
    base = env.get("DEXSCREENER_BASE_URL", "https://api.dexscreener.com").rstrip("/")
    try:
        from dexscreener import DexScreenerClient
        async with DexScreenerClient() as client:
            pairs = await client.get_token_pairs(chain, token)
            if pairs:
                p = pairs[0]
                return {"symbol": p.base_token_symbol,
                        "price_usd": float(p.price_usd),
                        "liquidity_usd": float(p.liquidity_usd)}
    except Exception as exc:  # noqa: BLE001
        print(f"[price] dexscreener-python unavailable ({exc}); raw API", file=sys.stderr)
    r = await httpx.AsyncClient().get(f"{base}/token-pairs/v1/{chain}/{token}", timeout=30)
    r.raise_for_status()
    pairs = r.json()
    if not pairs:
        return {}
    p = pairs[0]
    return {"symbol": p["baseToken"]["symbol"],
            "price_usd": float(p.get("priceUsd", 0) or 0),
            "liquidity_usd": float((p.get("liquidity") or {}).get("usd", 0) or 0)}


async def get_top_holders(token: str, helius_key: str, limit: int = 20,
                          max_pages: int = 15) -> list[dict]:
    if not helius_key:
        print("[holders] HELIUS_API_KEY missing -> skipped", file=sys.stderr)
        return []
    rpc = os.environ.get("SOLANA_RPC_URL", "https://beta.helius-rpc.com").split("?")[0].rstrip("/")
    accs: list[tuple[str, int]] = []
    async with httpx.AsyncClient() as c:
        for page in range(1, max_pages + 1):
            r = await c.post(rpc, headers={"Authorization": f"Bearer {helius_key}",
                            "Content-Type": "application/json"},
                json={"jsonrpc": "2.0", "id": "1", "method": "getTokenAccounts",
                      "params": {"mint": token, "options": {"page": page, "limit": 1000}}},
                timeout=30)
            batch = (r.json().get("result") or {}).get("token_accounts") or []
            if not batch:
                break
            for a in batch:
                try:
                    accs.append((a["owner"], int(a["amount"])))
                except (KeyError, ValueError):
                    continue
            if len(batch) < 1000:
                break
    accs.sort(key=lambda x: x[1], reverse=True)
    total = sum(a for _, a in accs) or 1
    return [{"rank": i, "owner": o, "amount": a, "pct": a / total * 100.0}
            for i, (o, a) in enumerate(accs[:limit], 1)]


async def get_top_traders(chain: str, token: str, limit: int, env: dict) -> list[dict]:
    key = env.get("SOLANATRACKER_API_KEY", "")
    if not key:
        print("[kol] SOLANATRACKER_API_KEY missing -> skipped", file=sys.stderr)
        return []
    url = f"https://data.solanatracker.io/trades/{token}"
    agg: dict[str, dict] = {}
    async with httpx.AsyncClient() as c:
        cursor = None
        for _ in range(10):
            u = url + (f"?cursor={cursor}" if cursor else "")
            r = await c.get(u, headers={"x-api-key": key}, timeout=30)
            if r.status_code != 200:
                return []
            for t in (r.json().get("trades") or []):
                w = t.get("wallet")
                if not w:
                    continue
                side = (t.get("type") or "buy").lower()
                usd = float(t.get("volume") or 0)
                d = agg.setdefault(w, {"buys": 0.0, "sells": 0.0, "n": 0})
                d["n"] += 1
                if side == "sell":
                    d["sells"] += usd
                else:
                    d["buys"] += usd
            if not r.json().get("hasNextPage"):
                break
            cursor = r.json().get("nextCursor")
    rows = [{"wallet": w, "buys_usd": v["buys"], "sells_usd": v["sells"],
             "net_usd": v["sells"] - v["buys"], "txns": v["n"]}
            for w, v in agg.items()]
    rows.sort(key=lambda x: x["buys_usd"] + x["sells_usd"], reverse=True)
    return rows[:limit]


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url", nargs="?", default=DEFAULT_GAINERS)
    ap.add_argument("--mode", default="page", choices=["page", "gainers", "api"])
    ap.add_argument("--chrome", default=DEFAULT_CHROME)
    ap.add_argument("--holders", type=int, default=40)
    ap.add_argument("--kol", type=int, default=30)
    ap.add_argument("--limit", type=int, default=100,
                    help="gainers mode: how many page-1 tokens to process")
    ap.add_argument("--out", help="write discovery JSON here")
    ap.add_argument("--dump-html", help="debug: save rendered page HTML (page mode)")
    args = ap.parse_args()
    env = _env()

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise SystemExit("Playwright not installed: uv add playwright")

    if args.mode in ("page", "gainers"):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True, executable_path=args.chrome,
                args=["--no-sandbox", "--disable-dev-shm-usage",
                      "--disable-blink-features=AutomationControlled"])
            try:
                if args.mode == "gainers":
                    results = await scrape_gainers(browser, args.url, args.limit, args.out)
                    if args.out:
                        Path(args.out).write_text(json.dumps(results, indent=2))
                        print(f"[ok] wrote {len(results)} tokens -> {args.out}")
                    else:
                        print(json.dumps(results, indent=2))
                else:
                    ctx = await browser.new_context(
                        user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
                        viewport={"width": 1440, "height": 900}, locale="en-US")
                    page = await _new_page(ctx)
                    d = await scrape_token(page, args.url, dump_html=args.dump_html)
                    await page.close(); await ctx.close()
                    holders = (d.get("holders") or [])[:args.holders]
                    kols = (d.get("kol") or [])[:args.kol]
                    print(f"symbol={d.get('symbol')}")
                    print("\n=== TOP HOLDERS ===")
                    for h in holders:
                        pct = f"{h['pct']:.2f}%" if h.get("pct") is not None else "?"
                        print(f"  {h.get('rank','?'):>3}. {h['address']:<44} {pct}")
                    print("\n=== KOLs ===")
                    for k in kols:
                        print(f"  {k}")
                    if args.out:
                        Path(args.out).write_text(
                            json.dumps({"holders": holders, "kol": kols}, indent=2))
                        print(f"\n[ok] wrote -> {args.out}")
            finally:
                await browser.close()
        return

    # api mode
    chain, token = parse_dexscreener_url(args.url)
    price = await get_token_price(chain, token, env)
    holders = await get_top_holders(token, env.get("HELIUS_API_KEYS", "").split(",")[0], args.holders)
    traders = await get_top_traders(chain, token, args.kol, env)
    print(f"symbol={price.get('symbol')}  price=${price.get('price_usd')}")
    print("\n=== TOP HOLDERS ===")
    for h in holders:
        print(f"  {h['rank']:>2}. {h['owner']:<44} {h['pct']:.2f}%")
    print("\n=== TOP TRADERS / KOL ===")
    for t in traders:
        print(f"  {t['wallet']:<44} net=${t['net_usd']:.0f} txns={t['txns']}")


def parse_dexscreener_url(url: str) -> tuple[str, str]:
    m = _URL_RE.search(url)
    if not m:
        raise SystemExit(f"Could not parse chain+mint from URL: {url}")
    return m.group(1), m.group(2)


if __name__ == "__main__":
    asyncio.run(main())
