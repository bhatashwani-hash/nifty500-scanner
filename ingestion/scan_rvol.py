"""
scan_rvol.py  —  Daily RVOL movers (all stocks, EOD data).

For the latest trading day, for every active stock:
    chg_pct     = close vs previous close
    avg_vol_20d = average volume of the prior 20 sessions (excluding today)
    rvol        = today's volume / avg_vol_20d

All stocks with valid metrics are written; the dashboard highlights names
that are up or down on elevated relative volume (rvol >= 1.5).

Results written to scanner_rvol (delete+insert per run_date).
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

INSERT_SQL = """
    INSERT INTO scanner_rvol
        (run_date, symbol, close, chg_pct, volume, avg_vol_20d, rvol)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
        close = EXCLUDED.close, chg_pct = EXCLUDED.chg_pct,
        volume = EXCLUDED.volume, avg_vol_20d = EXCLUDED.avg_vol_20d,
        rvol = EXCLUDED.rvol
"""


def run(conn, run_date: date | None = None):
    log.info("scan_rvol: loading data …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true AND o.time >= now() - interval '60 days'
           ORDER BY date""",
        conn, parse_dates=["date"],
    )
    if df.empty:
        log.warning("scan_rvol: no data")
        return 0

    close_df = df.pivot(index="date", columns="symbol", values="close").sort_index()
    vol_df   = df.pivot(index="date", columns="symbol", values="volume").sort_index()
    if len(close_df) < 22:
        log.warning("scan_rvol: not enough history")
        return 0

    today = close_df.index[-1]
    run_date = run_date or (today.date() if hasattr(today, "date") else today)

    c0, c1 = close_df.iloc[-1], close_df.iloc[-2]
    v0 = vol_df.iloc[-1]
    av20 = vol_df.iloc[-21:-1].mean()          # prior 20 sessions, excl. today

    rows = []
    for sym in close_df.columns:
        c, p, v, a = c0.get(sym), c1.get(sym), v0.get(sym), av20.get(sym)
        if any(x is None or (isinstance(x, float) and np.isnan(x)) for x in (c, p, v, a)):
            continue
        if p <= 0 or a <= 0:
            continue
        rows.append((run_date, sym, round(float(c), 2),
                     round((float(c) / float(p) - 1) * 100, 2),
                     int(v), int(a), round(float(v) / float(a), 2)))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_rvol WHERE run_date = %s", (run_date,))
        if rows:
            psycopg2.extras.execute_values(cur, INSERT_SQL, rows, page_size=500)
    conn.commit()
    log.info("scan_rvol: %d rows for %s", len(rows), run_date)
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
