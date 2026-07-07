"""
fetch_hourly.py  —  Hourly OHLCV ingest (Yahoo Finance 1h bars) + intraday feed.

Runs hourly during market hours (9:30–15:30 IST) via hourly_ingest.yml:

  1. Downloads today's 1-hour candles for every active stock in the `stocks`
     table (batched yf.download) and upserts them into `ohlcv_hourly`.
     The in-progress bar is refreshed on every run and finalizes by the
     15:30 IST run.

  2. Rebuilds the `hourly_feed` snapshot (one row per symbol):
       prev_close   — last EOD close from `ohlcv` (before today)
       chg_pct      — last hourly close vs prev_close
       cum_vol      — today's cumulative volume from the hourly bars
       avg_vol_20d  — 20-day average daily volume from `ohlcv`
       session_frac — fraction of the 9:15–15:30 session elapsed
       rvol         — cum_vol / (avg_vol_20d * session_frac)   [time-adjusted]
     The dashboard's Hourly F&O tab polls this and highlights movers with
     rvol >= 1.5 that are up or down on the day.

Usage:
    python ingestion/fetch_hourly.py               # today's session
    python ingestion/fetch_hourly.py --batch 80    # yfinance batch size
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("fetch_hourly")

IST = timezone(timedelta(hours=5, minutes=30))
SESSION_OPEN_MIN  = 9 * 60 + 15          # 09:15 IST
SESSION_CLOSE_MIN = 15 * 60 + 30         # 15:30 IST
SESSION_LEN       = SESSION_CLOSE_MIN - SESSION_OPEN_MIN   # 375 min


def load_symbols(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks WHERE is_active = true ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


def fetch_hourly_bars(symbols, batch_size=80):
    """Download today's 1h bars for all symbols. Returns tidy DataFrame."""
    frames = []
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        tickers = [f"{s}.NS" for s in batch]
        try:
            df = yf.download(tickers=tickers, period="1d", interval="1h",
                             group_by="ticker", threads=True, progress=False,
                             auto_adjust=False)
        except Exception as e:
            log.warning("batch %d download failed: %s", i // batch_size, e)
            continue
        if df is None or df.empty:
            continue
        for s, t in zip(batch, tickers):
            try:
                sub = df[t] if isinstance(df.columns, pd.MultiIndex) else df
            except KeyError:
                continue
            sub = sub.dropna(subset=["Close"])
            if sub.empty:
                continue
            sub = sub.reset_index()
            ts_col = "Datetime" if "Datetime" in sub.columns else sub.columns[0]
            ts = pd.to_datetime(sub[ts_col])
            if getattr(ts.dt, "tz", None) is None:
                ts = ts.dt.tz_localize("UTC")
            sub["ts"] = ts.dt.tz_convert("Asia/Kolkata")
            sub["symbol"] = s
            frames.append(sub.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume",
            })[["symbol", "ts", "open", "high", "low", "close", "volume"]])
        log.info("batch %d/%d done", i // batch_size + 1,
                 (len(symbols) + batch_size - 1) // batch_size)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["volume"] = out["volume"].fillna(0).astype("int64")
    return out


def upsert_hourly(conn, df):
    if df.empty:
        return 0
    rows = [(r.symbol, r.ts.to_pydatetime(), float(r.open), float(r.high),
             float(r.low), float(r.close), int(r.volume))
            for r in df.itertuples(index=False)]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO ohlcv_hourly (symbol, ts, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, ts) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume
        """, rows, page_size=500)
    conn.commit()
    return len(rows)


def session_fraction(now_ist):
    mins = now_ist.hour * 60 + now_ist.minute
    frac = (mins - SESSION_OPEN_MIN) / SESSION_LEN
    return max(0.10, min(1.0, frac))   # clamp: avoid divide-by-tiny at the open


def rebuild_feed(conn, bars, trade_date):
    """Recompute hourly_feed from today's bars + daily history."""
    # prev EOD close + 20d avg daily volume, per symbol, from the daily table
    hist = pd.read_sql("""
        SELECT symbol,
               (array_agg(close ORDER BY time DESC))[1]      AS prev_close,
               avg(volume)                                    AS avg_vol_20d
        FROM (
            SELECT symbol, time, close, volume,
                   row_number() OVER (PARTITION BY symbol ORDER BY time DESC) AS rn
            FROM ohlcv WHERE time::date < %s
        ) t
        WHERE rn <= 20
        GROUP BY symbol
    """, conn, params=(trade_date,))
    hist = hist.set_index("symbol")

    agg = bars.sort_values("ts").groupby("symbol").agg(
        last_ts=("ts", "last"), last_close=("close", "last"),
        cum_vol=("volume", "sum"))

    now_ist = datetime.now(IST)
    frac = session_fraction(now_ist)

    rows = []
    for sym, r in agg.iterrows():
        h = hist.loc[sym] if sym in hist.index else None
        prev = float(h["prev_close"]) if h is not None and pd.notna(h["prev_close"]) else None
        av20 = float(h["avg_vol_20d"]) if h is not None and pd.notna(h["avg_vol_20d"]) else None
        chg = round((float(r.last_close) / prev - 1) * 100, 2) if prev else None
        rvol = round(float(r.cum_vol) / (av20 * frac), 2) if av20 and av20 > 0 else None
        rows.append((sym, trade_date, r.last_ts.to_pydatetime(),
                     round(float(r.last_close), 2), prev, chg,
                     int(r.cum_vol), int(av20) if av20 else None,
                     round(frac, 3), rvol))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO hourly_feed
                (symbol, trade_date, last_ts, last_close, prev_close, chg_pct,
                 cum_vol, avg_vol_20d, session_frac, rvol)
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE SET
                trade_date = EXCLUDED.trade_date, last_ts = EXCLUDED.last_ts,
                last_close = EXCLUDED.last_close, prev_close = EXCLUDED.prev_close,
                chg_pct = EXCLUDED.chg_pct, cum_vol = EXCLUDED.cum_vol,
                avg_vol_20d = EXCLUDED.avg_vol_20d, session_frac = EXCLUDED.session_frac,
                rvol = EXCLUDED.rvol, updated_at = now()
        """, rows, page_size=500)
        cur.execute("DELETE FROM hourly_feed WHERE trade_date < %s", (trade_date,))
    conn.commit()
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description="Hourly OHLCV ingest + intraday feed")
    ap.add_argument("--batch", type=int, default=80, help="yfinance batch size")
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        symbols = load_symbols(conn)
        log.info("fetching 1h bars for %d symbols …", len(symbols))
        bars = fetch_hourly_bars(symbols, args.batch)
        if bars.empty:
            log.warning("no hourly bars returned (market holiday?) — nothing to do")
            return
        n = upsert_hourly(conn, bars)
        trade_date = bars["ts"].max().date()
        m = rebuild_feed(conn, bars, trade_date)
        log.info("upserted %d hourly bars, refreshed feed for %d symbols (%s)",
                 n, m, trade_date)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
