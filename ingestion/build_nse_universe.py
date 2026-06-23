"""
build_nse_universe.py — Build the full NSE equity universe with market cap.

Two phases, so you can CONFIRM the list before the stocks table is touched:

  Dry run (default)   fetch the NSE equity list + market caps, write
                      data/nse_all_stocks.csv, and print a summary.
                      *** Does NOT modify the database. ***

  Commit (--commit)   upsert data/nse_all_stocks.csv into the stocks table
                      (symbol, name, sector, market_cap, is_active=true).

Sources
  Symbols     NSE official EQUITY_L.csv (SERIES EQ/BE). If NSE blocks the
              download, grab it manually from
              https://www.nseindia.com/market-data/securities-available-for-trading
              and pass it with --from-file data/EQUITY_L.csv
  Market cap  yfinance  <SYMBOL>.NS  → fast_info.market_cap (fallback: .info)

Usage
  python ingestion/build_nse_universe.py                       # dry run → CSV + summary
  python ingestion/build_nse_universe.py --from-file data/EQUITY_L.csv
  python ingestion/build_nse_universe.py --limit 50            # quick test
  python ingestion/build_nse_universe.py --commit              # upsert CSV → stocks

Notes
  * Fetching market cap for ~2000 names takes a while (Yahoo rate-limits); the
    --limit flag is handy for a quick smoke test first.
  * Re-running is safe: the CSV is overwritten, and the upsert is idempotent.
"""

import argparse
import io
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")
DB_URL = os.environ.get("DATABASE_URL")
OUT_CSV = ROOT / "data" / "nse_all_stocks.csv"

NSE_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/csv,application/csv,*/*",
    "Referer": "https://www.nseindia.com/",
}


def fetch_nse_list(from_file: str | None = None) -> pd.DataFrame:
    """Return a DataFrame with columns: symbol, name (EQ/BE series only)."""
    if from_file:
        df = pd.read_csv(from_file)
    else:
        # NSE needs a cookie warm-up before it serves the archive file.
        sess = requests.Session()
        sess.headers.update(HEADERS)
        try:
            sess.get("https://www.nseindia.com", timeout=30)
        except Exception:
            pass
        r = sess.get(NSE_URL, timeout=30)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={"SYMBOL": "symbol", "NAME OF COMPANY": "name", "SERIES": "series"})
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    if "series" in df.columns:
        df = df[df["series"].astype(str).str.strip().isin(["EQ", "BE"])]
    return df[["symbol", "name"]].drop_duplicates("symbol").reset_index(drop=True)


def fetch_market_cap(symbol: str):
    """Return (market_cap, sector) for an NSE symbol via yfinance, or (None, None)."""
    t = yf.Ticker(f"{symbol}.NS")
    mc, sector = None, None
    try:
        fi = t.fast_info
        mc = fi.get("market_cap") if hasattr(fi, "get") else getattr(fi, "market_cap", None)
    except Exception:
        pass
    if not mc or not sector:
        try:
            info = t.info
            mc = mc or info.get("marketCap")
            sector = info.get("sector")
        except Exception:
            pass
    return mc, sector


def build(from_file=None, limit=None) -> pd.DataFrame:
    base = fetch_nse_list(from_file)
    if limit:
        base = base.head(limit)
    print(f"NSE equity list: {len(base)} symbols. Fetching market caps …")

    rows = []
    for i, r in base.iterrows():
        sym = r["symbol"]
        mc, sector = fetch_market_cap(sym)
        rows.append({"symbol": sym, "name": r["name"], "sector": sector, "market_cap": mc})
        print(f"[{i + 1}/{len(base)}] {sym}: mcap={mc}")
        time.sleep(0.2)  # be polite to Yahoo

    out = pd.DataFrame(rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    got = int(out["market_cap"].notna().sum())
    print(f"\nWrote {len(out)} rows → {OUT_CSV}")
    print(f"market_cap populated for {got}/{len(out)} symbols")
    print("\nTop 15 by market cap:")
    print(
        out.sort_values("market_cap", ascending=False, na_position="last")
        .head(15)[["symbol", "name", "market_cap"]]
        .to_string(index=False)
    )
    print("\nReview data/nse_all_stocks.csv, then run with --commit to update the stocks table.")
    return out


def commit(conn, df: pd.DataFrame):
    rows = [
        (
            r.symbol,
            r.name,
            (r.sector if pd.notna(r.sector) else None),
            (float(r.market_cap) if pd.notna(r.market_cap) else None),
            True,
        )
        for r in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO stocks (symbol, name, sector, market_cap, is_active)
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE SET
                name       = EXCLUDED.name,
                sector     = COALESCE(EXCLUDED.sector, stocks.sector),
                market_cap = EXCLUDED.market_cap,
                is_active  = true
            """,
            rows,
            page_size=500,
        )
    conn.commit()
    print(f"Upserted {len(rows)} rows into stocks.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-file", default=None, help="Local EQUITY_L.csv if the NSE download is blocked")
    ap.add_argument("--commit", action="store_true", help="Upsert data/nse_all_stocks.csv into the stocks table")
    ap.add_argument("--limit", type=int, default=None, help="Only the first N symbols (for a quick test)")
    args = ap.parse_args()

    if args.commit:
        if not OUT_CSV.exists():
            print(f"{OUT_CSV} not found — run the dry run first to build it.")
            sys.exit(1)
        if not DB_URL:
            print("DATABASE_URL not set (check .env).")
            sys.exit(1)
        df = pd.read_csv(OUT_CSV)
        conn = psycopg2.connect(DB_URL)
        try:
            commit(conn, df)
        finally:
            conn.close()
    else:
        build(args.from_file, args.limit)
