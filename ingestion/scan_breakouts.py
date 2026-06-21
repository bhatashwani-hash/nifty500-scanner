"""
scan_breakouts.py  —  Stocks at N-period price highs or lows.

A stock qualifies if today's close is the highest (or lowest) close
over the given lookback window (inclusive of today).

Lookback windows (trading days):
  1M  = 21    3M = 63    6M = 126    1Y = 252    2Y = 504

Results written to scanner_breakouts (one row per symbol per matched type).
"""

import logging
import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
log = logging.getLogger(__name__)

WINDOWS = {
    "1M":  21,
    "3M":  63,
    "6M":  126,
    "1Y":  252,
    "2Y":  504,
}

UPSERT_SQL = """
    INSERT INTO scanner_breakouts (run_date, symbol, close, change_pct, volume, breakout_type)
    VALUES %s
    ON CONFLICT (run_date, symbol, breakout_type) DO UPDATE SET
        close      = EXCLUDED.close,
        change_pct = EXCLUDED.change_pct,
        volume     = EXCLUDED.volume
"""


def run(conn, run_date: date | None = None):
    log.info("scan_breakouts: loading data …")

    close_df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.close
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true
           ORDER BY date""",
        conn, parse_dates=["date"],
    ).pivot(index="date", columns="symbol", values="close").sort_index()

    vol_df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true
           ORDER BY date""",
        conn, parse_dates=["date"],
    ).pivot(index="date", columns="symbol", values="volume").sort_index()

    today = close_df.index[-1]
    run_date = run_date or (today.date() if hasattr(today, "date") else today)

    today_close  = close_df.iloc[-1]
    today_volume = vol_df.iloc[-1]
    prev_close   = close_df.iloc[-2] if len(close_df) > 1 else today_close
    change_pct   = ((today_close - prev_close) / prev_close * 100).round(2)

    rows = []
    for label, window in WINDOWS.items():
        if len(close_df) < window:
            continue
        window_close = close_df.iloc[-window:]
        period_high  = window_close.max()
        period_low   = window_close.min()

        for sym in close_df.columns:
            c = today_close.get(sym)
            if c is None or np.isnan(c):
                continue
            ph = period_high.get(sym)
            pl = period_low.get(sym)
            ch = change_pct.get(sym)
            vol = today_volume.get(sym)
            vol = int(vol) if vol and not np.isnan(vol) else None

            if ph and c >= ph:
                rows.append((run_date, sym, float(c), float(ch) if ch else None, vol, f"{label}_HIGH"))
            if pl and c <= pl:
                rows.append((run_date, sym, float(c), float(ch) if ch else None, vol, f"{label}_LOW"))

    if rows:
        # Delete today's rows first so re-runs are clean
        with conn.cursor() as cur:
            cur.execute("DELETE FROM scanner_breakouts WHERE run_date = %s", (run_date,))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
        conn.commit()
        log.info("scan_breakouts: %d rows written for %s", len(rows), run_date)
    else:
        log.info("scan_breakouts: no breakouts found for %s", run_date)

    return len(rows)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        run(conn)
    finally:
        conn.close()
