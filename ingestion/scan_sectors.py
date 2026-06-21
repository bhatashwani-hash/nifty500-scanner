"""
scan_sectors.py  —  Sector leaders / laggards for day, week, and month.

Reads the 500-stock `stocks` table (sector column) joined to `ohlcv`.
Stocks with NULL sector are grouped under "Unknown".

For each (period, sector) computes:
  avg_return    — mean % return across all stocks in the sector
  med_return    — median % return
  up_count      — number of stocks with positive return
  down_count    — number of stocks with negative return
  stock_count   — total stocks with data in that sector

Periods:
  day   = 1 trading day
  week  = 5 trading days
  month = 21 trading days

Results written to scanner_sectors.
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

UPSERT_SQL = """
    INSERT INTO scanner_sectors
        (run_date, period, sector, avg_return, med_return, up_count, down_count, stock_count)
    VALUES %s
    ON CONFLICT (run_date, period, sector) DO UPDATE SET
        avg_return  = EXCLUDED.avg_return,
        med_return  = EXCLUDED.med_return,
        up_count    = EXCLUDED.up_count,
        down_count  = EXCLUDED.down_count,
        stock_count = EXCLUDED.stock_count
"""

PERIODS = {"day": 1, "week": 5, "month": 21}


def run(conn, run_date: date | None = None):
    log.info("scan_sectors: loading data …")

    # Load sector mapping — Nifty 500 only
    sectors = pd.read_sql(
        "SELECT symbol, COALESCE(sector, 'Unknown') AS sector FROM stocks WHERE is_active = true",
        conn,
    ).set_index("symbol")["sector"]

    close_df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.close
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true ORDER BY date""",
        conn, parse_dates=["date"],
    ).pivot(index="date", columns="symbol", values="close").sort_index()

    if len(close_df) < 22:
        log.warning("scan_sectors: not enough history")
        return 0

    today    = close_df.index[-1]
    run_date = run_date or (today.date() if hasattr(today, "date") else today)

    rows = []
    for period_name, n_days in PERIODS.items():
        if len(close_df) <= n_days:
            continue

        today_close = close_df.iloc[-1]
        past_close  = close_df.iloc[-(n_days + 1)]
        returns     = ((today_close - past_close) / past_close.replace(0, np.nan) * 100).dropna()

        ret_df = returns.to_frame("return")
        ret_df["sector"] = sectors.reindex(ret_df.index).fillna("Unknown")

        for sector, grp in ret_df.groupby("sector"):
            r = grp["return"]
            rows.append((
                run_date, period_name, sector,
                round(float(r.mean()), 4),
                round(float(r.median()), 4),
                int((r > 0).sum()),
                int((r < 0).sum()),
                int(len(r)),
            ))

    if rows:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM scanner_sectors WHERE run_date = %s", (run_date,))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=200)
        conn.commit()
        log.info("scan_sectors: %d sector-period rows written for %s", len(rows), run_date)
    else:
        log.info("scan_sectors: no data for %s", run_date)

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
