"""
fetch_metals.py  —  Gold / silver futures + macro context bars → metals_ohlcv.

Feeds the Gold & Silver Desk artifact (voice analyst + dashboard). Pulls from
Yahoo Finance:

    GC=F      COMEX gold futures (front month, continuous)
    SI=F      COMEX silver futures
    DX-Y.NYB  US dollar index          — the single biggest external driver of gold
    ^TNX      10-year Treasury yield   — proxy for real-rate pressure on bullion

at three resolutions, each sized to what the desk actually reads:

    1d   730 days   trend, ADX, Keltner, NR7, 20-EMA pullbacks, gold/silver ratio
    1h    60 days   session structure, Holy Grail on the hourly
    5m     5 days   opening thrust / Early Entry on today's session

COMEX metals trade ~23h/day on Globex (Sun 18:00 – Fri 17:00 ET, 60-min halt
at 17:00), so unlike the NSE ingest this runs around the clock on weekdays.

Usage:
    python ingestion/fetch_metals.py              # all four symbols, all intervals
    python ingestion/fetch_metals.py --interval 5m
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("fetch_metals")

SYMBOLS = ["GC=F", "SI=F", "DX-Y.NYB", "^TNX"]
PLAN = {"1d": "730d", "1h": "60d", "5m": "5d"}


def fetch(interval: str, period: str) -> pd.DataFrame:
    try:
        raw = yf.download(tickers=SYMBOLS, period=period, interval=interval,
                          group_by="ticker", threads=True, progress=False,
                          auto_adjust=False)
    except Exception as e:
        log.warning("download failed (%s): %s", interval, e)
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()
    frames = []
    for sym in SYMBOLS:
        try:
            sub = raw[sym] if isinstance(raw.columns, pd.MultiIndex) else raw
        except KeyError:
            continue
        sub = sub.dropna(subset=["Close"])
        if sub.empty:
            continue
        sub = sub.reset_index()
        tcol = "Datetime" if "Datetime" in sub.columns else sub.columns[0]
        ts = pd.to_datetime(sub[tcol])
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize("UTC")
        sub["ts"] = ts.dt.tz_convert("UTC")
        sub["symbol"] = sym
        sub["interval"] = interval
        frames.append(sub.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                          "Close": "close", "Volume": "volume"})
                      [["symbol", "interval", "ts", "open", "high", "low", "close", "volume"]])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["volume"] = out["volume"].fillna(0).astype("int64")
    return out.dropna(subset=["open", "high", "low", "close"])


def upsert(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute("""CREATE TEMP TABLE IF NOT EXISTS stage_metals
                       (LIKE metals_ohlcv INCLUDING DEFAULTS) ON COMMIT DROP""")
        cur.copy_expert("COPY stage_metals (symbol, interval, ts, open, high, low, close, volume) "
                        "FROM STDIN WITH (FORMAT csv)", buf)
        cur.execute("""INSERT INTO metals_ohlcv (symbol, interval, ts, open, high, low, close, volume)
                       SELECT symbol, interval, ts, open, high, low, close, volume FROM stage_metals
                       ON CONFLICT (symbol, interval, ts) DO UPDATE SET
                         open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                         close=EXCLUDED.close, volume=EXCLUDED.volume""")
        n = cur.rowcount
        # keep the intraday tables from growing without bound
        cur.execute("DELETE FROM metals_ohlcv WHERE interval='5m' AND ts < now() - interval '10 days'")
        cur.execute("DELETE FROM metals_ohlcv WHERE interval='1h' AND ts < now() - interval '90 days'")
    conn.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", choices=list(PLAN) + ["all"], default="all")
    args = ap.parse_args()
    ivs = list(PLAN) if args.interval == "all" else [args.interval]

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        for iv in ivs:
            t0 = time.time()
            df = fetch(iv, PLAN[iv])
            n = upsert(conn, df)
            log.info("%s: %d bars across %d symbols in %.0fs",
                     iv, n, df.symbol.nunique() if not df.empty else 0, time.time() - t0)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
