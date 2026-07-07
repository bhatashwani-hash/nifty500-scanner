"""
scan_breakouts.py  —  FRESH long-timeframe breakouts / breakdowns.

A stock qualifies only on the FIRST day it makes a new N-period closing
high (or low) — i.e. today's close exceeds the max (min) close of the
prior N sessions, AND yesterday's close had NOT already done the same
against its own prior window. Continuation days are excluded.

Windows (trading days):  6M = 126    1Y = 252    2Y = 504

If a stock is fresh on several windows the same day, only the LONGEST
window is reported (a fresh 2Y high subsumes 1Y/6M).

Results written to scanner_breakouts, breakout_type ∈
  6M_HIGH / 1Y_HIGH / 2Y_HIGH / 6M_LOW / 1Y_LOW / 2Y_LOW
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

WINDOWS = {"6M": 126, "1Y": 252, "2Y": 504}   # ordered short -> long

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
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true
           ORDER BY date""",
        conn, parse_dates=["date"],
    )
    close_df = df.pivot(index="date", columns="symbol", values="close").sort_index()
    vol_df   = df.pivot(index="date", columns="symbol", values="volume").sort_index()

    if len(close_df) < 130:
        log.warning("scan_breakouts: not enough history")
        return 0

    today = close_df.index[-1]
    run_date = run_date or (today.date() if hasattr(today, "date") else today)

    c0 = close_df.iloc[-1]                  # today
    c1 = close_df.iloc[-2]                  # yesterday
    v0 = vol_df.iloc[-1]
    chg = ((c0 - c1) / c1 * 100).round(2)

    best = {}   # (symbol, side) -> (window_rank, label)
    for rank, (label, n) in enumerate(WINDOWS.items()):
        if len(close_df) < n + 2:
            continue
        prior_max = close_df.shift(1).rolling(n).max()   # max of closes t-N..t-1
        prior_min = close_df.shift(1).rolling(n).min()
        pm0, pm1 = prior_max.iloc[-1], prior_max.iloc[-2]
        pn0, pn1 = prior_min.iloc[-1], prior_min.iloc[-2]

        fresh_hi = (c0 > pm0) & ~(c1 > pm1) & pm0.notna() & pm1.notna()
        fresh_lo = (c0 < pn0) & ~(c1 < pn1) & pn0.notna() & pn1.notna()

        for sym in close_df.columns[fresh_hi.fillna(False)]:
            best[(sym, "HIGH")] = (rank, label)          # longer window overwrites
        for sym in close_df.columns[fresh_lo.fillna(False)]:
            best[(sym, "LOW")] = (rank, label)

    rows = []
    for (sym, side), (_, label) in best.items():
        c = c0.get(sym)
        if c is None or np.isnan(c):
            continue
        vol = v0.get(sym)
        vol = int(vol) if vol is not None and not np.isnan(vol) else None
        ch = chg.get(sym)
        rows.append((run_date, sym, round(float(c), 2),
                     float(ch) if ch is not None and not np.isnan(ch) else None,
                     vol, f"{label}_{side}"))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_breakouts WHERE run_date = %s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()
    log.info("scan_breakouts: %d fresh breakouts/breakdowns for %s", len(rows), run_date)
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
