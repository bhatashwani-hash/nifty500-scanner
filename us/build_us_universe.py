"""
build_us_universe.py  —  Build the US scanner universe (top-1000 by market cap).

Downloads the full NASDAQ screener list (NASDAQ + NYSE + AMEX, ~6,000 names),
filters to investable stocks, keeps the TOP N BY MARKET CAP (default 1000),
and upserts into `us_stocks`:

    price >= $3,  common stock symbols only
    (drops units/warrants/preferreds: symbols containing '^' or '/')

Every screened symbol gets `cap_rank` (1 = largest); only the top N are
is_active = true. Symbols missing from the latest list go is_active = false.
Also registers the tracked indices + sector/theme ETFs in `us_indices`.
Run once to seed, then occasionally (e.g. monthly) to refresh membership.

    python us/build_us_universe.py
    python us/build_us_universe.py --top 1500 --min-price 5
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")

SCREENER_URL = ("https://api.nasdaq.com/api/screener/stocks"
                "?tableonly=true&limit=25&offset=0&download=true")
# S&P 500 constituents (Wikipedia-sourced, maintained dataset) — drives the
# us_stocks.is_sp500 flag used by the dashboards' "S&P 500 only" filter.
SP500_URL = ("https://raw.githubusercontent.com/datasets/s-and-p-500-companies"
             "/main/data/constituents.csv")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
}


def fetch_sp500_symbols():
    """S&P 500 member symbols, upper-cased. Empty set on failure (flag is
    then left untouched rather than wiped)."""
    try:
        r = requests.get(SP500_URL, timeout=30)
        r.raise_for_status()
        lines = r.text.splitlines()
        syms = {ln.split(",", 1)[0].strip().upper() for ln in lines[1:] if ln.strip()}
        return {s for s in syms if s}
    except Exception as e:
        print(f"  ! S&P 500 list fetch failed ({e}) — keeping existing is_sp500 flags")
        return set()


def parse_cap(v):
    try:
        return float(str(v).replace("$", "").replace(",", "") or 0)
    except ValueError:
        return 0.0


ETFS = [
    # 11 GICS sector SPDRs
    ("XLK", "Technology Select SPDR", "Sector ETF"),
    ("XLF", "Financial Select SPDR", "Sector ETF"),
    ("XLV", "Health Care Select SPDR", "Sector ETF"),
    ("XLE", "Energy Select SPDR", "Sector ETF"),
    ("XLY", "Consumer Discretionary SPDR", "Sector ETF"),
    ("XLP", "Consumer Staples SPDR", "Sector ETF"),
    ("XLI", "Industrial Select SPDR", "Sector ETF"),
    ("XLB", "Materials Select SPDR", "Sector ETF"),
    ("XLRE", "Real Estate Select SPDR", "Sector ETF"),
    ("XLU", "Utilities Select SPDR", "Sector ETF"),
    ("XLC", "Communication Services SPDR", "Sector ETF"),
    # granular theme ETFs (must stay in sync with THEMES in scan_us_all.py)
    ("SMH", "VanEck Semiconductor", "Theme ETF"),
    ("IGV", "iShares Expanded Tech-Software", "Theme ETF"),
    ("KBE", "SPDR S&P Bank", "Theme ETF"),
    ("KRE", "SPDR S&P Regional Banking", "Theme ETF"),
    ("IAI", "iShares US Broker-Dealers", "Theme ETF"),
    ("IAK", "iShares US Insurance", "Theme ETF"),
    ("IPAY", "Amplify Digital Payments", "Theme ETF"),
    ("XBI", "SPDR S&P Biotech", "Theme ETF"),
    ("XPH", "SPDR S&P Pharmaceuticals", "Theme ETF"),
    ("IHI", "iShares US Medical Devices", "Theme ETF"),
    ("IHF", "iShares US Healthcare Providers", "Theme ETF"),
    ("XOP", "SPDR S&P Oil & Gas E&P", "Theme ETF"),
    ("OIH", "VanEck Oil Services", "Theme ETF"),
    ("AMLP", "Alerian MLP", "Theme ETF"),
    ("ITA", "iShares US Aerospace & Defense", "Theme ETF"),
    ("XHB", "SPDR S&P Homebuilders", "Theme ETF"),
    ("XRT", "SPDR S&P Retail", "Theme ETF"),
    ("CARZ", "First Trust S-Network Future Vehicles", "Theme ETF"),
    ("IYT", "iShares US Transportation", "Theme ETF"),
    ("GDX", "VanEck Gold Miners", "Theme ETF"),
    ("SLX", "VanEck Steel", "Theme ETF"),
    ("PEJ", "Invesco Leisure & Entertainment", "Theme ETF"),
    ("PAVE", "Global X US Infrastructure", "Theme ETF"),
    ("PBJ", "Invesco Food & Beverage", "Theme ETF"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=1000, help="keep top N by market cap")
    ap.add_argument("--min-price", type=float, default=3.0)
    args = ap.parse_args()

    print("Downloading NASDAQ screener list …")
    r = requests.get(SCREENER_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    rows = r.json()["data"]["rows"]
    print(f"  {len(rows)} raw listings")

    keep = []
    for x in rows:
        sym = (x.get("symbol") or "").strip()
        if not sym or "^" in sym or "/" in sym or len(sym) > 6:
            continue
        price = parse_cap(x.get("lastsale"))
        cap = parse_cap(x.get("marketCap"))
        if price < args.min_price or cap <= 0:
            continue
        keep.append((sym, (x.get("name") or "")[:120],
                     x.get("sector") or None, x.get("industry") or None, cap))
    keep.sort(key=lambda t: t[4], reverse=True)
    print(f"  {len(keep)} pass filters (price>=${args.min_price}); "
          f"keeping top {args.top} by market cap")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO us_stocks (symbol, name, sector, industry, market_cap, cap_rank, is_active)
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                  name=EXCLUDED.name, sector=EXCLUDED.sector, industry=EXCLUDED.industry,
                  market_cap=EXCLUDED.market_cap, cap_rank=EXCLUDED.cap_rank,
                  is_active=EXCLUDED.is_active
            """, [(s, n, sec, ind, cap, i + 1, i < args.top)
                  for i, (s, n, sec, ind, cap) in enumerate(keep)],
                page_size=500)
            cur.execute("UPDATE us_stocks SET is_active=false, cap_rank=NULL "
                        "WHERE symbol <> ALL(%s)", ([s for s, *_ in keep],))
            # refresh S&P 500 membership (dot/dash ticker styles both matched)
            sp500 = fetch_sp500_symbols()
            if sp500:
                sp_list = sorted(sp500)
                cur.execute("UPDATE us_stocks SET is_sp500=false WHERE is_sp500")
                cur.execute("""
                    UPDATE us_stocks
                       SET is_sp500 = true
                     WHERE upper(symbol) = ANY(%s)
                        OR upper(replace(symbol,'-','.')) = ANY(%s)
                        OR upper(replace(symbol,'.','-')) = ANY(%s)
                """, (sp_list, sp_list, sp_list))
                print(f"  flagged is_sp500 for S&P 500 members ({len(sp_list)} in list)")
            # tracked indices + sector/theme ETFs
            execute_values(cur, """
                INSERT INTO us_indices (symbol, name, category) VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET name=EXCLUDED.name, category=EXCLUDED.category
            """, [("^GSPC", "S&P 500", "Benchmark"), ("^NDX", "NASDAQ 100", "Benchmark"),
                  ("^DJI", "Dow Jones", "Benchmark"), ("^RUT", "Russell 2000", "Benchmark"),
                  ("^VIX", "VIX", "Volatility")] + ETFS)
        conn.commit()
        print(f"Upserted {len(keep)} into us_stocks "
              f"(top {args.top} active, +{5 + len(ETFS)} indices/ETFs)")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
