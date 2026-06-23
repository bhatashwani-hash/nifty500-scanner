"""
Daily / backfill ingestion script for Nifty 500 EOD data, plus a curated
set of benchmark and sectoral indices.

Usage:
    python fetch_data.py --mode backfill --years 5
    python fetch_data.py --mode daily

Reads tickers from data/nifty500_test.csv (swap this file for the real
NSE Nifty 500 list once you have it — same column format works as-is).

Also reads data/indices.csv for the index list (NIFTY 50, NIFTY BANK,
sector indices, etc.). Index symbols are used exactly as listed there —
some are '^'-prefixed (e.g. ^NSEI), some are '.NS'-suffixed (e.g.
NIFTYMIDCAP150.NS) — unlike stocks, no suffix is added automatically.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

DB_URL = os.environ.get("DATABASE_URL")
TICKER_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "nifty500_test.csv")
INDEX_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "indices.csv")


def load_tickers() -> list[str]:
    """Read the Nifty 500 constituent list and return NSE-suffixed yfinance symbols."""
    df = pd.read_csv(TICKER_CSV)
    symbols = df["Symbol"].str.strip().tolist()
    return symbols


def load_indices() -> list[str]:
    """Read the tracked index list and return their yfinance symbols as-is."""
    df = pd.read_csv(INDEX_CSV)
    symbols = df["Symbol"].str.strip().tolist()
    return symbols


def load_active_stock_symbols(conn) -> list[str]:
    """Return the active 500-stock universe straight from the stocks table.
    This keeps the daily ingest in lock-step with what the scanners read."""
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks WHERE is_active = true ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


def ensure_stocks_table(conn, tickers_df: pd.DataFrame):
    """Upsert the stock reference table (symbol, name, sector)."""
    rows = list(
        tickers_df[["Symbol", "Company Name", "Industry"]].itertuples(index=False, name=None)
    )
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO stocks (symbol, name, sector)
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE
                SET name = EXCLUDED.name,
                    sector = EXCLUDED.sector
            """,
            rows,
        )
    conn.commit()
    print(f"Upserted {len(rows)} rows into stocks")


def ensure_indices_table(conn, indices_df: pd.DataFrame):
    """Upsert the index reference table (symbol, name, category)."""
    rows = list(
        indices_df[["Symbol", "Name", "Category"]].itertuples(index=False, name=None)
    )
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO indices (symbol, name, category)
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE
                SET name = EXCLUDED.name,
                    category = EXCLUDED.category
            """,
            rows,
        )
    conn.commit()
    print(f"Upserted {len(rows)} rows into indices")


def fetch_ohlcv(symbol: str, period: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """Fetch OHLCV data for one symbol from yfinance. NSE symbols need a .NS suffix."""
    yf_symbol = f"{symbol}.NS"
    ticker = yf.Ticker(yf_symbol)
    if period:
        df = ticker.history(period=period, auto_adjust=False)
    else:
        df = ticker.history(start=start, end=end, auto_adjust=False)

    if df.empty:
        return df

    df = df.reset_index()
    df["symbol"] = symbol
    df = df.rename(
        columns={
            "Date": "time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    # Normalize to the NSE session calendar date: strip intraday time + timezone
    # so the value stores cleanly and time::date is never shifted across the
    # UTC/IST day boundary (this is what caused Friday bars to land on Sunday).
    _ts = pd.to_datetime(df["time"])
    if getattr(_ts.dt, "tz", None) is not None:
        _ts = _ts.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df["time"] = _ts.dt.normalize()
    return df[["time", "symbol", "open", "high", "low", "close", "volume"]]


def upsert_ohlcv(conn, df: pd.DataFrame):
    """Bulk upsert OHLCV rows. Safe to re-run — duplicate (symbol, time) rows are updated, not duplicated."""
    if df.empty:
        return 0
    rows = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ohlcv (time, symbol, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, time) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def fetch_index_ohlcv(symbol: str, period: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    """Fetch OHLCV data for one index from yfinance. Index symbols are used
    exactly as given (no .NS suffix added — indices.csv already has the
    correct form, e.g. '^NSEI' or 'NIFTYMIDCAP150.NS')."""
    ticker = yf.Ticker(symbol)
    if period:
        df = ticker.history(period=period, auto_adjust=False)
    else:
        df = ticker.history(start=start, end=end, auto_adjust=False)

    if df.empty:
        return df

    df = df.reset_index()
    df["symbol"] = symbol
    df = df.rename(
        columns={
            "Date": "time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    # Normalize to the NSE session calendar date: strip intraday time + timezone
    # so the value stores cleanly and time::date is never shifted across the
    # UTC/IST day boundary (this is what caused Friday bars to land on Sunday).
    _ts = pd.to_datetime(df["time"])
    if getattr(_ts.dt, "tz", None) is not None:
        _ts = _ts.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    df["time"] = _ts.dt.normalize()
    return df[["time", "symbol", "open", "high", "low", "close", "volume"]]


def upsert_index_ohlcv(conn, df: pd.DataFrame):
    """Bulk upsert index OHLCV rows. Safe to re-run — duplicate (symbol, time) rows are updated, not duplicated."""
    if df.empty:
        return 0
    rows = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO index_ohlcv (time, symbol, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, time) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def run(mode: str, years: int, universe: str = "both", days: int = 1, truncate: bool = False):
    if not DB_URL:
        print("ERROR: DATABASE_URL not set. Copy .env.example to .env and fill it in.")
        sys.exit(1)

    do_stocks = universe in ("stocks", "both")
    do_indices = universe in ("indices", "both")

    conn = psycopg2.connect(DB_URL)

    if truncate:
        with conn.cursor() as cur:
            if do_stocks:
                cur.execute("TRUNCATE TABLE ohlcv")
                print("Truncated ohlcv — existing stock OHLCV removed.")
            if do_indices:
                cur.execute("TRUNCATE TABLE index_ohlcv")
                print("Truncated index_ohlcv.")
        conn.commit()

    if mode == "backfill":
        period = f"{years}y"
        kwargs = {"period": period}
    else:
        # Daily mode: fetch only the last `days` calendar days (default 1 = that day).
        # end is exclusive in yfinance, so today's bar is covered by end = tomorrow.
        days = max(1, days)
        start = (datetime.now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
        end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        kwargs = {"start": start, "end": end}

    total_rows = 0
    failed = []
    if do_stocks:
        # Universe = the stocks table (is_active). Same source for BOTH daily and
        # backfill, so a backfill covers the full universe — not just the seed CSV.
        symbols = load_active_stock_symbols(conn)
        if symbols:
            print(f"Loaded {len(symbols)} active symbols from stocks table")
        else:  # safety fallback only if the table is empty
            tickers_df = pd.read_csv(TICKER_CSV)
            symbols = tickers_df["Symbol"].str.strip().tolist()
            ensure_stocks_table(conn, tickers_df)
            print(f"stocks table empty — fell back to {len(symbols)} tickers from CSV")

        for i, symbol in enumerate(symbols, 1):
            try:
                df = fetch_ohlcv(symbol, **kwargs)
                n = upsert_ohlcv(conn, df)
                total_rows += n
                print(f"[{i}/{len(symbols)}] {symbol}: {n} rows")
            except Exception as e:
                print(f"[{i}/{len(symbols)}] {symbol}: FAILED ({e})")
                failed.append(symbol)
            time.sleep(0.3)  # be polite to Yahoo's API, avoid rate limiting

        print(f"\nStocks done. Total rows upserted: {total_rows}")
        if failed:
            print(f"Failed symbols ({len(failed)}): {failed}")
    else:
        print("Skipping stocks (--universe indices)")

    index_total_rows = 0
    index_failed = []
    if do_indices:
        indices_df = pd.read_csv(INDEX_CSV)
        index_symbols = indices_df["Symbol"].str.strip().tolist()
        print(f"Loaded {len(index_symbols)} indices from {INDEX_CSV}")

        ensure_indices_table(conn, indices_df)

        for i, symbol in enumerate(index_symbols, 1):
            try:
                df = fetch_index_ohlcv(symbol, **kwargs)
                n = upsert_index_ohlcv(conn, df)
                index_total_rows += n
                print(f"[{i}/{len(index_symbols)}] {symbol}: {n} rows")
            except Exception as e:
                print(f"[{i}/{len(index_symbols)}] {symbol}: FAILED ({e})")
                index_failed.append(symbol)
            time.sleep(0.3)  # be polite to Yahoo's API, avoid rate limiting

        print(f"\nIndices done. Total rows upserted: {index_total_rows}")
        if index_failed:
            print(f"Failed indices ({len(index_failed)}): {index_failed}")
    else:
        print("Skipping indices (--universe stocks)")

    conn.close()
    print(f"\nGrand total rows upserted: {total_rows + index_total_rows}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "daily"], default="daily")
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument(
        "--universe",
        choices=["stocks", "indices", "both"],
        default="both",
        help="Which dataset to fetch: stocks only, indices only, or both (default).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Daily mode lookback window in calendar days (default 1 = that day only).",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Remove ALL existing rows from the target OHLCV table(s) before loading.",
    )
    args = parser.parse_args()
    run(args.mode, args.years, args.universe, args.days, args.truncate)
