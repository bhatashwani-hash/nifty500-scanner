"""
build_us_universe.py  —  Build the US scanner universe (Wilshire-style coverage).

Downloads the full NASDAQ screener list (NASDAQ + NYSE + AMEX, ~6,000 names),
filters to investable stocks, and upserts into `us_stocks`:

    price >= $3,  market cap >= $150M,  common stock symbols only
    (drops units/warrants/preferreds: symbols containing '^' or '/')

Symbols missing from the latest list are marked is_active = false.
Run once to seed, then occasionally (e.g. monthly) to refresh membership.

    python us/build_us_universe.py
    python us/build_us_universe.py --min-cap 300e6 --min-price 5
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-cap", type=float, default=150e6)
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
        if price < args.min_price or cap < args.min_cap:
            continue
        keep.append((sym, (x.get("name") or "")[:120],
                     x.get("sector") or None, x.get("industry") or None, cap))
    print(f"  {len(keep)} pass filters (price>=${args.min_price}, cap>=${args.min_cap:,.0f})")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO us_stocks (symbol, name, sector, industry, market_cap, is_active)
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                  name=EXCLUDED.name, sector=EXCLUDED.sector, industry=EXCLUDED.industry,
                  market_cap=EXCLUDED.market_cap, is_active=true
            """, [(s, n, sec, ind, cap, True) for s, n, sec, ind, cap in keep],
                page_size=500)
            cur.execute("UPDATE us_stocks SET is_active=false WHERE symbol <> ALL(%s)",
                        ([s for s, *_ in keep],))
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
            # tracked indices
            execute_values(cur, """
                INSERT INTO us_indices (symbol, name, category) VALUES %s
                ON CONFLICT (symbol) DO NOTHING
            """, [("^GSPC", "S&P 500", "Benchmark"), ("^NDX", "NASDAQ 100", "Benchmark"),
                  ("^DJI", "Dow Jones", "Benchmark"), ("^RUT", "Russell 2000", "Benchmark"),
                  ("^VIX", "VIX", "Volatility")])
        conn.commit()
        print(f"Upserted {len(keep)} into us_stocks (+5 indices)")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
