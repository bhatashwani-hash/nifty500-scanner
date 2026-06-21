"""
scan_vcp.py  —  Volatility Contraction Pattern (VCP) scanner.

Criteria (both must be true as of today):
  1. Trend:  close today / close 63 trading days ago  ≥ 1.25  (+25% in 3 months)
  2. Tight:  (max High last 15d - min Low last 15d) / min Low last 15d  < 0.15

Stocks meeting both criteria are in a VCP setup — a strong prior trend
followed by a tight sideways consolidation, often preceding a breakout.

Results written to scanner_vcp.
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
    INSERT INTO scanner_vcp
        (run_date, symbol, close, return_3m, high_15d, low_15d, range_15d_pct)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
        close         = EXCLUDED.close,
        return_3m     = EXCLUDED.return_3m,
        high_15d      = EXCLUDED.high_15d,
        low_15d       = EXCLUDED.low_15d,
        range_15d_pct = EXCLUDED.range_15d_pct
"""


def run(conn, run_date: date | None = None):
    log.info("scan_vcp: loading data …")

    close_df = pd.read_sql(
        "SELECT symbol, time::date AS date, close FROM zerodha_ohlcv ORDER BY date",
        conn, parse_dates=["date"],
    ).pivot(index="date", columns="symbol", values="close").sort_index()

    high_df = pd.read_sql(
        "SELECT symbol, time::date AS date, high FROM zerodha_ohlcv ORDER BY date",
        conn, parse_dates=["date"],
    ).pivot(index="date", columns="symbol", values="high").sort_index()

    low_df = pd.read_sql(
        "SELECT symbol, time::date AS date, low FROM zerodha_ohlcv ORDER BY date",
        conn, parse_dates=["date"],
    ).pivot(index="date", columns="symbol", values="low").sort_index()

    if len(close_df) < 64:
        log.warning("scan_vcp: not enough history (need ≥ 64 days)")
        return 0

    today    = close_df.index[-1]
    run_date = run_date or (today.date() if hasattr(today, "date") else today)

    today_close  = close_df.iloc[-1]
    close_63d    = close_df.iloc[-64] if len(close_df) >= 64 else None   # 63 days ago
    high_15d     = high_df.iloc[-15:].max()
    low_15d      = low_df.iloc[-15:].min()
    range_15d    = (high_15d - low_15d) / low_15d.replace(0, np.nan)

    rows = []
    for sym in close_df.columns:
        c    = today_close.get(sym)
        c63  = close_63d.get(sym) if close_63d is not None else None
        h15  = high_15d.get(sym)
        l15  = low_15d.get(sym)
        r15  = range_15d.get(sym)

        if any(v is None or np.isnan(v) for v in [c, c63, h15, l15, r15]):
            continue
        if c63 <= 0:
            continue

        ret_3m = (c - c63) / c63

        if ret_3m >= 0.25 and r15 < 0.15:
            rows.append((
                run_date, sym,
                round(float(c), 2),
                round(ret_3m * 100, 2),
                round(float(h15), 2),
                round(float(l15), 2),
                round(r15 * 100, 2),
            ))

    if rows:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM scanner_vcp WHERE run_date = %s", (run_date,))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=200)
        conn.commit()
        log.info("scan_vcp: %d VCP setups found for %s", len(rows), run_date)
    else:
        log.info("scan_vcp: no VCPs found for %s", run_date)

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
