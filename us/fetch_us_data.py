"""
fetch_us_data.py  —  US OHLCV ingest (Yahoo Finance, batched).

Mirrors the Nifty pipeline but for the US universe (`us_stocks`):
  backfill : 1 year of daily bars (default) for all active stocks + indices
  daily    : last N days delta (default 1) — the nightly cron mode

    python us/fetch_us_data.py --mode backfill --years 1
    python us/fetch_us_data.py --mode daily --days 1
    python us/fetch_us_data.py --mode daily --universe indices

Yahoo symbol mapping: '.' in US tickers becomes '-' (BRK.B -> BRK-B).
Timestamps are normalized to the US/Eastern session date.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")
BATCH = 100


def yahoo_sym(s):  # BRK.B -> BRK-B
    return s.replace(".", "-")


def load_universe(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT symbol FROM {table} WHERE COALESCE(is_active, true) ORDER BY symbol"
                    if table == "us_stocks" else f"SELECT symbol FROM {table} ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


def fetch_batch(symbols, start, end, is_index=False):
    ymap = {(s if is_index else yahoo_sym(s)): s for s in symbols}
    df = yf.download(tickers=list(ymap.keys()), start=start, end=end, interval="1d",
                     group_by="ticker", threads=True, progress=False, auto_adjust=False)
    if df is None or df.empty:
        return pd.DataFrame()
    out = []
    for ysym, sym in ymap.items():
        try:
            sub = df[ysym] if isinstance(df.columns, pd.MultiIndex) else df
        except KeyError:
            continue
        sub = sub.dropna(subset=["Close"])
        if sub.empty:
            continue
        sub = sub.reset_index()
        ts = pd.to_datetime(sub["Date"])
        if getattr(ts.dt, "tz", None) is not None:
            ts = ts.dt.tz_convert("America/New_York").dt.tz_localize(None)
        sub["time"] = ts.dt.normalize()
        sub["symbol"] = sym
        out.append(sub.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                       "Close": "close", "Volume": "volume"})
                   [["time", "symbol", "open", "high", "low", "close", "volume"]])
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def upsert(conn, table, df):
    if df.empty:
        return 0
    df = df.dropna(subset=["close"])
    df["volume"] = df["volume"].fillna(0).astype("int64")
    rows = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        execute_values(cur, f"""
            INSERT INTO {table} (time, symbol, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, time) DO UPDATE SET
              open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
              close=EXCLUDED.close, volume=EXCLUDED.volume
        """, rows, page_size=1000)
    conn.commit()
    return len(rows)


def run(conn, symbols, table, start, end, is_index=False):
    total = 0
    nb = (len(symbols) + BATCH - 1) // BATCH
    for i in range(0, len(symbols), BATCH):
        b = symbols[i:i + BATCH]
        try:
            total += upsert(conn, table, fetch_batch(b, start, end, is_index))
        except Exception as e:
            print(f"  batch {i//BATCH + 1}/{nb} FAILED: {e}")
            time.sleep(5)
        if (i // BATCH + 1) % 10 == 0 or i + BATCH >= len(symbols):
            print(f"  {min(i+BATCH, len(symbols))}/{len(symbols)} symbols … {total} rows")
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "daily"], default="daily")
    ap.add_argument("--years", type=float, default=1.0, help="backfill lookback (default 1yr)")
    ap.add_argument("--days", type=int, default=1, help="daily delta window")
    ap.add_argument("--universe", choices=["stocks", "indices", "both"], default="both")
    args = ap.parse_args()

    end = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
    if args.mode == "backfill":
        start = (datetime.utcnow() - timedelta(days=int(args.years * 365) + 7)).strftime("%Y-%m-%d")
    else:
        start = (datetime.utcnow() - timedelta(days=args.days + 4)).strftime("%Y-%m-%d")
    print(f"{args.mode}: {start} -> {end}")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        if args.universe in ("stocks", "both"):
            syms = load_universe(conn, "us_stocks")
            print(f"stocks: {len(syms)} symbols -> us_ohlcv")
            run(conn, syms, "us_ohlcv", start, end)
        if args.universe in ("indices", "both"):
            idx = load_universe(conn, "us_indices")
            print(f"indices: {len(idx)} symbols -> us_index_ohlcv")
            run(conn, idx, "us_index_ohlcv", start, end, is_index=True)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
