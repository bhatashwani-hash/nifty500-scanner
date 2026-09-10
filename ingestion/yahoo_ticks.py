"""
yahoo_ticks.py — CLOUD FALLBACK for kite_ticks.py: poll Yahoo Finance 1-minute
data for the F&O universe + NSE indices and upsert the `ticks` table, so the
dashboard's "Ticks" tab stays live when the Kite streamer isn't running.

Trade-offs vs kite_ticks.py:
  * ~40-60s refresh cadence (Yahoo polling) instead of 3s WebSocket ticks
  * no futures (`ticks_fut`) and no `sector_history` minute bars
Run kite_ticks.py on the trading PC whenever possible; this feeder is for the
GitHub Actions runner (.github/workflows/live_ticks.yml). Both write the same
rows, so whichever wrote last wins — stop this workflow if Kite is streaming.

Env:
    DATABASE_URL   Supabase connection string
    POLL_SECS      seconds between polls (default 40)
    RUN_MINUTES    hard cap on runtime, for CI job windows (default 210)
"""

import os
import time
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")
DB_URL = os.environ["DATABASE_URL"]
POLL_SECS = int(os.environ.get("POLL_SECS", "40"))
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "210"))

IST = timezone(timedelta(hours=5, minutes=30))
OPEN_IST, CLOSE_IST = dtime(9, 15), dtime(15, 35)

# DB symbols for indices are already Yahoo symbols (see kite_ticks.INDEX_MAP keys)
INDEX_SYMBOLS = [
    "^NSEI", "^NSEBANK", "NIFTY_FIN_SERVICE.NS", "^CNXIT", "^CNXAUTO",
    "^CNXPHARMA", "^CNXFMCG", "^CNXMETAL", "^CNXENERGY", "^CNXREALTY",
    "^CNXPSUBANK", "^CRSLDX", "^NSMIDCP",
]

UPSERT = """
INSERT INTO ticks (symbol, is_index, ltp, prev_close, chg_pct, vol, ts, updated_at)
VALUES %s
ON CONFLICT (symbol) DO UPDATE SET
  ltp=EXCLUDED.ltp, prev_close=COALESCE(EXCLUDED.prev_close, ticks.prev_close),
  chg_pct=EXCLUDED.chg_pct, vol=EXCLUDED.vol, ts=EXCLUDED.ts, updated_at=now()
"""


def log(msg):
    print(f"{datetime.now(IST):%H:%M:%S} {msg}", flush=True)


def yahoo_symbol(db_sym: str) -> str:
    return db_sym if db_sym.startswith("^") or db_sym.endswith(".NS") else db_sym + ".NS"


def load_universe(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM stocks WHERE is_active AND is_fno")
        stocks = [r[0] for r in cur.fetchall()]
    return stocks


def fetch_prev_closes(ysyms):
    """Last completed daily close (excluding today's in-progress bar)."""
    today = datetime.now(IST).date()
    data = yf.download(ysyms, period="5d", interval="1d", group_by="ticker",
                       auto_adjust=False, threads=True, progress=False)
    prev = {}
    for y in ysyms:
        try:
            closes = (data[y]["Close"] if len(ysyms) > 1 else data["Close"]).dropna()
            closes = closes[closes.index.date < today]
            if len(closes):
                prev[y] = float(closes.iloc[-1])
        except Exception:
            pass
    return prev


def poll_once(ysyms):
    """Return {yahoo_sym: (ltp, day_volume, bar_ts)} from today's 1m bars."""
    out = {}
    data = yf.download(ysyms, period="1d", interval="1m", group_by="ticker",
                       auto_adjust=False, threads=True, progress=False)
    for y in ysyms:
        try:
            df = data[y] if len(ysyms) > 1 else data
            closes = df["Close"].dropna()
            if not len(closes):
                continue
            vol = int(pd.to_numeric(df["Volume"], errors="coerce").fillna(0).sum())
            out[y] = (float(closes.iloc[-1]), vol, closes.index[-1].to_pydatetime())
        except Exception:
            pass
    return out


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    stocks = load_universe(conn)
    if not stocks:
        raise SystemExit("stocks table returned no active F&O symbols")
    sym_of = {yahoo_symbol(s): (s, False) for s in stocks}
    sym_of.update({yahoo_symbol(i): (i, True) for i in INDEX_SYMBOLS})
    ysyms = list(sym_of)
    log(f"universe: {len(stocks)} F&O stocks + {len(INDEX_SYMBOLS)} indices")

    prev = fetch_prev_closes(ysyms)
    log(f"prev closes loaded for {len(prev)} symbols")

    deadline = time.monotonic() + RUN_MINUTES * 60
    cycles = 0
    while True:
        now_ist = datetime.now(IST)
        if now_ist.weekday() >= 5 or now_ist.time() >= CLOSE_IST:
            log("market closed — writing final snapshot and exiting")
        t0 = time.monotonic()
        quotes = poll_once(ysyms)
        rows = []
        now_utc = datetime.now(timezone.utc)
        for y, (ltp, vol, ts) in quotes.items():
            db_sym, is_index = sym_of[y]
            pc = prev.get(y)
            chg = (ltp - pc) / pc * 100.0 if pc else None
            rows.append((db_sym, is_index, ltp, pc, chg, None if is_index else vol,
                         ts, now_utc))
        if rows:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT, rows)
        cycles += 1
        log(f"cycle {cycles}: upserted {len(rows)}/{len(ysyms)} in {time.monotonic()-t0:.0f}s")

        if now_ist.weekday() >= 5 or now_ist.time() >= CLOSE_IST:
            break
        if time.monotonic() >= deadline:
            log(f"RUN_MINUTES={RUN_MINUTES} reached — exiting (next CI shift takes over)")
            break
        time.sleep(max(5, POLL_SECS - (time.monotonic() - t0)))
    conn.close()


if __name__ == "__main__":
    main()
