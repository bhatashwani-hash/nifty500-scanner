"""
enrich_nifty500.py  —  Mark Nifty 500 constituents in zerodha_stocks.

Fetches the official Nifty 500 list from NSE Archives, matches symbols
to zerodha_stocks, and sets is_nifty500 = true for matches.

Run locally:
    python ingestion/enrich_nifty500.py
"""

import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

NSE_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/",
}


def fetch_nifty500_symbols() -> list[str]:
    log.info("Fetching Nifty 500 list from NSE …")
    r = requests.get(NSE_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(pd.io.common.StringIO(r.text))
    # NSE CSV has a 'Symbol' column
    col = next((c for c in df.columns if c.strip().lower() == "symbol"), None)
    if not col:
        raise ValueError(f"Could not find Symbol column. Columns: {df.columns.tolist()}")
    symbols = df[col].str.strip().tolist()
    log.info("Fetched %d Nifty 500 symbols from NSE", len(symbols))
    return symbols


def update_db(conn, nifty500: list[str]):
    with conn.cursor() as cur:
        # Reset all to false first
        cur.execute("UPDATE zerodha_stocks SET is_nifty500 = false")

        # Get all symbols in DB to find matches
        cur.execute("SELECT symbol FROM zerodha_stocks")
        db_symbols = {r[0] for r in cur.fetchall()}

        matched   = [s for s in nifty500 if s in db_symbols]
        unmatched = [s for s in nifty500 if s not in db_symbols]

        if matched:
            cur.execute(
                "UPDATE zerodha_stocks SET is_nifty500 = true WHERE symbol = ANY(%s)",
                (matched,)
            )

    conn.commit()
    log.info("Marked %d symbols as is_nifty500 = true", len(matched))
    if unmatched:
        log.warning("%d NSE symbols not found in zerodha_stocks: %s",
                    len(unmatched), unmatched[:20])
    return len(matched)


def main():
    nifty500 = fetch_nifty500_symbols()

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        n = update_db(conn, nifty500)
        log.info("Done. %d stocks flagged as Nifty 500.", n)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
