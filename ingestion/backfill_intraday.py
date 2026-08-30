"""
backfill_intraday.py  —  30-day hourly + 5-minute backfill for the top-N NSE stocks.

Pulls Yahoo Finance 1h and 5m candles for the top 1000 stocks by market cap and
upserts them into `ohlcv_hourly` and `ohlcv_5min`.

Yahoo's intraday windows are capped (5m is only served for ~60 days, 1h for
~730), so 30 days sits comfortably inside both.

Loading strategy: each batch is COPYed into an UNLOGGED temp table and then
merged with INSERT .. ON CONFLICT DO UPDATE.  Row-by-row execute_values over a
remote Supabase connection is roughly an order of magnitude slower at the
~1.7M rows a 5-minute pull produces.

Usage:
    python ingestion/backfill_intraday.py                    # both, 30d, top 1000
    python ingestion/backfill_intraday.py --interval 5m      # just 5-minute
    python ingestion/backfill_intraday.py --days 15 --top 500
    python ingestion/backfill_intraday.py --resume           # skip symbols already loaded
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
log = logging.getLogger("backfill_intraday")

TARGET = {"1h": "ohlcv_hourly", "5m": "ohlcv_5min"}

# The Supabase instance has ~1 GB of disk. A full 5-minute load for 1000 symbols
# x 30 sessions is 338 MB and took the database over the edge on 2026-08-30 —
# Postgres flips to read-only when the volume fills, which stops every writer
# (scanners, hourly ingest, tick streamer), not just the load. Never load more
# than the consuming scanner actually reads: --slots 09:15,09:20,09:25 is 4% of
# the rows and is all the 09:30 Early Entry scan needs.
DISK_BUDGET_MB = 850          # refuse to start if the DB is already past this
BYTES_PER_ROW  = 165          # measured: 338 MB / 2.07M rows, incl. index+PK


def preflight(conn, projected_rows):
    with conn.cursor() as cur:
        cur.execute("SELECT pg_database_size(current_database())/1024/1024")
        cur_mb = cur.fetchone()[0]
    add_mb = projected_rows * BYTES_PER_ROW / 1024 / 1024
    log.info("preflight: db at %d MB, this load adds ~%d MB (budget %d MB)",
             cur_mb, add_mb, DISK_BUDGET_MB)
    if cur_mb + add_mb > DISK_BUDGET_MB:
        raise SystemExit(
            f"ABORT: projected {cur_mb + add_mb:.0f} MB exceeds the {DISK_BUDGET_MB} MB "
            f"budget. Narrow the load (--slots / --top / --days) or raise the budget "
            f"only if you have verified the volume can take it.")


def top_symbols(conn, n):
    with conn.cursor() as cur:
        cur.execute("""SELECT symbol FROM stocks
                       WHERE is_active AND market_cap IS NOT NULL
                       ORDER BY market_cap DESC LIMIT %s""", (n,))
        return [r[0] for r in cur.fetchall()]


def already_loaded(conn, table, days):
    """Symbols that already have bars inside the window — for --resume."""
    with conn.cursor() as cur:
        cur.execute(f"""SELECT symbol FROM {table}
                        WHERE ts >= now() - interval '%s days'
                        GROUP BY symbol HAVING COUNT(*) > 0""" % days)
        return {r[0] for r in cur.fetchall()}


def fetch(symbols, interval, days, slots=None):
    """Tidy frame of bars for one batch, or empty."""
    tickers = [f"{s}.NS" for s in symbols]
    try:
        raw = yf.download(tickers=tickers, period=f"{days}d", interval=interval,
                          group_by="ticker", threads=True, progress=False,
                          auto_adjust=False)
    except Exception as e:
        log.warning("download failed (%s): %s", interval, e)
        return pd.DataFrame()
    if raw is None or raw.empty:
        return pd.DataFrame()

    frames = []
    for sym, tkr in zip(symbols, tickers):
        try:
            sub = raw[tkr] if isinstance(raw.columns, pd.MultiIndex) else raw
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
        sub["ts"] = ts.dt.tz_convert("Asia/Kolkata")
        sub["symbol"] = sym
        frames.append(sub.rename(columns={"Open": "open", "High": "high", "Low": "low",
                                          "Close": "close", "Volume": "volume"})
                      [["symbol", "ts", "open", "high", "low", "close", "volume"]])
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["volume"] = out["volume"].fillna(0).astype("int64")
    out = out.dropna(subset=["open", "high", "low", "close"])
    if slots:
        keep = out.ts.dt.strftime("%H:%M").isin(slots)
        out = out[keep]
    return out


def upsert(conn, table, df):
    """COPY into a temp table, then merge. Much faster than row-wise inserts."""
    if df.empty:
        return 0
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with conn.cursor() as cur:
        cur.execute(f"""CREATE TEMP TABLE IF NOT EXISTS stage_{table}
                        (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP""")
        cur.copy_expert(
            f"COPY stage_{table} (symbol, ts, open, high, low, close, volume) "
            f"FROM STDIN WITH (FORMAT csv)", buf)
        # drop symbols not in `stocks` — the FK would abort the whole batch
        cur.execute(f"""DELETE FROM stage_{table} s
                        WHERE NOT EXISTS (SELECT 1 FROM stocks k WHERE k.symbol = s.symbol)""")
        cur.execute(f"""INSERT INTO {table} (symbol, ts, open, high, low, close, volume)
                        SELECT symbol, ts, open, high, low, close, volume
                        FROM stage_{table}
                        ON CONFLICT (symbol, ts) DO UPDATE SET
                          open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                          close=EXCLUDED.close, volume=EXCLUDED.volume""")
        n = cur.rowcount
    conn.commit()
    return n


def run_interval(conn, symbols, interval, days, batch, slots=None):
    table = TARGET[interval]
    total, t0 = 0, time.time()
    nb = (len(symbols) + batch - 1) // batch
    for i in range(0, len(symbols), batch):
        chunk = symbols[i:i + batch]
        df = fetch(chunk, interval, days, slots)
        n = upsert(conn, table, df)
        total += n
        log.info("%s batch %d/%d — %d symbols, %d bars (%d total, %.0fs elapsed)",
                 interval, i // batch + 1, nb, len(chunk), n, total, time.time() - t0)
    log.info("%s DONE: %d rows into %s in %.0fs", interval, total, table, time.time() - t0)
    return total


def main():
    ap = argparse.ArgumentParser(description="Backfill intraday bars from Yahoo")
    ap.add_argument("--interval", choices=["1h", "5m", "both"], default="both")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--top", type=int, default=1000)
    ap.add_argument("--batch", type=int, default=40)
    ap.add_argument("--slots", default="",
                    help="comma-separated IST times to keep, e.g. 09:15,09:20,09:25. "
                         "Empty = every bar. Use this: a full 5m load is 25x the rows.")
    ap.add_argument("--force", action="store_true", help="skip the disk preflight")
    ap.add_argument("--resume", action="store_true",
                    help="skip symbols that already have bars inside the window")
    args = ap.parse_args()

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        syms = top_symbols(conn, args.top)
        slots = [x.strip() for x in args.slots.split(",") if x.strip()] or None
        log.info("universe: %d symbols, %d days, interval=%s, slots=%s",
                 len(syms), args.days, args.interval, slots or "ALL")
        if not args.force:
            per_day = {"1h": 7, "5m": 75}
            ivs = ["1h", "5m"] if args.interval == "both" else [args.interval]
            projected = sum(len(syms) * args.days * (len(slots) if slots else per_day[i])
                            for i in ivs)
            preflight(conn, projected)
        for interval in (["1h", "5m"] if args.interval == "both" else [args.interval]):
            todo = syms
            if args.resume:
                done = already_loaded(conn, TARGET[interval], args.days)
                todo = [s for s in syms if s not in done]
                log.info("%s resume: %d already loaded, %d to go", interval, len(done), len(todo))
            if todo:
                run_interval(conn, todo, interval, args.days, args.batch, slots)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
