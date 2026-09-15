"""
us_ticks.py — poll Yahoo Finance for live S&P 500 quotes into the `us_ticks`
table. The dashboard's US live page (dashboard/us_ticks.html) reads that table.

Unlike kite_ticks.py there is no daily token — Yahoo needs no auth. This is a
CONTINUOUS process during US market hours (9:30–16:00 ET ≈ 7:00 PM–1:30 AM IST
during daylight saving). It polls in batches every ~20s; if started outside
market hours it writes one snapshot of the last session and exits.

Universe: us_stocks.is_sp500 (+ the five headline indices).

Run:
    python ingestion/us_ticks.py
"""

import os
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")
DB_URL = os.environ["DATABASE_URL"]

ET = ZoneInfo("America/New_York")
POLL_SECS = 20
BATCH = 150
EOD_ET = dtime(16, 10)     # stop shortly after the 16:00 ET close
SOD_ET = dtime(9, 25)      # ...but only once the session has begun

INDICES = {"^GSPC": "S&P 500", "^NDX": "NASDAQ 100", "^DJI": "Dow Jones",
           "^RUT": "Russell 2000", "^VIX": "VIX"}

UPSERT = """
INSERT INTO us_ticks (symbol, is_index, ltp, prev_close, chg_pct, vol, ts, updated_at)
VALUES %s
ON CONFLICT (symbol) DO UPDATE SET
  ltp=EXCLUDED.ltp, prev_close=COALESCE(EXCLUDED.prev_close, us_ticks.prev_close),
  chg_pct=EXCLUDED.chg_pct, vol=EXCLUDED.vol, ts=EXCLUDED.ts, updated_at=now()
"""


def ysym(s):  # BRK.B -> BRK-B
    return s.replace(".", "-")


def batches(lst, n=BATCH):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def load_universe(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM us_stocks WHERE is_sp500 ORDER BY symbol")
        return [r[0] for r in cur.fetchall()]


def prev_closes(symbols):
    """Previous session close per DB symbol from 5d daily bars."""
    out, today_et = {}, datetime.now(ET).date()
    for b in batches(symbols):
        ymap = {ysym(s): s for s in b}
        df = yf.download(list(ymap), period="5d", interval="1d", group_by="ticker",
                         threads=True, progress=False, auto_adjust=False)
        if df is None or df.empty:
            continue
        for yk, sym in ymap.items():
            try:
                sub = (df[yk] if isinstance(df.columns, pd.MultiIndex) else df).dropna(subset=["Close"])
            except KeyError:
                continue
            if sub.empty:
                continue
            closes = sub["Close"]
            # if the last daily bar is today's (partial) session, prev = the one before
            if closes.index[-1].date() >= today_et and len(closes) > 1:
                out[sym] = float(closes.iloc[-2])
            else:
                out[sym] = float(closes.iloc[-1])
        time.sleep(0.3)
    return out


def poll_once(conn, symbols, pc):
    rows, now = [], datetime.now(ZoneInfo("UTC"))
    for b in batches(symbols):
        ymap = {ysym(s): s for s in b}
        try:
            df = yf.download(list(ymap), period="1d", interval="1m", group_by="ticker",
                             threads=True, progress=False, auto_adjust=False, prepost=False)
        except Exception as e:
            print(f"[poll] batch failed: {e}")
            continue
        if df is None or df.empty:
            continue
        for yk, sym in ymap.items():
            try:
                sub = (df[yk] if isinstance(df.columns, pd.MultiIndex) else df).dropna(subset=["Close"])
            except KeyError:
                continue
            if sub.empty:
                continue
            ltp = float(sub["Close"].iloc[-1])
            vol = int(sub["Volume"].fillna(0).sum())
            ts = sub.index[-1].to_pydatetime()
            p = pc.get(sym)
            chg = round((ltp / p - 1) * 100, 2) if p else None
            rows.append((sym, sym in INDICES, round(ltp, 2),
                         round(p, 2) if p else None, chg,
                         vol if vol > 0 else None, ts, now))
    if rows:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT, rows, page_size=300)
        conn.commit()
    return len(rows)


def main():
    conn = psycopg2.connect(DB_URL)
    stocks = load_universe(conn)
    symbols = stocks + list(INDICES)
    print(f"S&P 500 live poll: {len(stocks)} members + {len(INDICES)} indices "
          f"(every {POLL_SECS}s, Yahoo Finance)")

    print("Fetching previous closes …")
    pc = prev_closes(symbols)
    print(f"  prev close for {len(pc)}/{len(symbols)} symbols")

    in_session = SOD_ET <= datetime.now(ET).time() < EOD_ET and datetime.now(ET).weekday() < 5
    if not in_session:
        n = poll_once(conn, symbols, pc)
        print(f"US market closed — wrote one snapshot ({n} symbols) and stopping.")
        conn.close()
        return

    print("Polling. Ctrl+C to stop.")
    try:
        while True:
            t0 = time.time()
            try:
                n = poll_once(conn, symbols, pc)
            except psycopg2.Error as e:
                print(f"[db] write failed ({e.__class__.__name__}) — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                time.sleep(2)
                conn = psycopg2.connect(DB_URL)
                n = 0
            et = datetime.now(ET)
            print(f"{et:%H:%M:%S} ET · {n} symbols · {time.time()-t0:.1f}s")
            # outside the session window in EITHER direction: also catches the
            # machine sleeping through the close and waking after midnight ET
            if not (SOD_ET <= et.time() < EOD_ET):
                print("US market closed — stopping.")
                break
            time.sleep(max(1, POLL_SECS - (time.time() - t0)))
    except KeyboardInterrupt:
        print("Stopping.")
    finally:
        conn.close()


if __name__ == "__main__":
    # supervisor: restart on crash; clean return (EOD / Ctrl+C) exits
    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[supervisor] crashed: {e.__class__.__name__}: {e} — restarting in 20s")
            time.sleep(20)
