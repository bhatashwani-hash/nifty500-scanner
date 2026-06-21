"""
scan_ep.py  —  Episodic Pivot scanner (Bullish & Bearish).

Bullish EP (LONG) — all three must be true on the SAME day:
  Gap:    Open  ≥ prev_close × 1.01   (gap up ≥ 1%)
  Move:   Close ≥ prev_close × 1.07   (close up ≥ 7%)
  Volume: volume ≥ vol_sma50_shifted × 3   (3× average)

Bearish EP (SHORT) — mirror:
  Gap:    Open  ≤ prev_close × 0.99
  Move:   Close ≤ prev_close × 0.93
  Volume: volume ≥ vol_sma50_shifted × 3

vol_sma50_shifted = 50-day simple moving average of volume, shifted forward
by 1 day so the breakout day's own volume is NOT included in the average.

Scans the most recent trading day only. Results written to scanner_ep.
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
    INSERT INTO scanner_ep
        (run_date, symbol, ep_type, open, close, prev_close,
         gap_pct, move_pct, vol_ratio, volume, vol_avg_50d)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
        ep_type    = EXCLUDED.ep_type,
        open       = EXCLUDED.open,
        close      = EXCLUDED.close,
        prev_close = EXCLUDED.prev_close,
        gap_pct    = EXCLUDED.gap_pct,
        move_pct   = EXCLUDED.move_pct,
        vol_ratio  = EXCLUDED.vol_ratio,
        volume     = EXCLUDED.volume,
        vol_avg_50d= EXCLUDED.vol_avg_50d
"""


def run(conn, run_date: date | None = None):
    log.info("scan_ep: loading OHLCV …")

    df_raw = pd.read_sql(
        """
        SELECT symbol, time::date AS date, open, close, volume
        FROM zerodha_ohlcv
        ORDER BY date
        """,
        conn, parse_dates=["date"],
    )

    open_df   = df_raw.pivot(index="date", columns="symbol", values="open").sort_index()
    close_df  = df_raw.pivot(index="date", columns="symbol", values="close").sort_index()
    vol_df    = df_raw.pivot(index="date", columns="symbol", values="volume").sort_index()

    # Need at least 51 rows for shifted 50-day vol SMA
    if len(close_df) < 52:
        log.warning("scan_ep: not enough history (need ≥ 52 days)")
        return 0

    # 50-day vol SMA, shifted by 1 so today's volume isn't in the average
    vol_sma50 = vol_df.rolling(50, min_periods=30).mean().shift(1)

    today      = close_df.index[-1]
    run_date   = run_date or (today.date() if hasattr(today, "date") else today)

    today_open   = open_df.iloc[-1]
    today_close  = close_df.iloc[-1]
    today_vol    = vol_df.iloc[-1]
    today_sma50  = vol_sma50.iloc[-1]
    prev_close   = close_df.iloc[-2]

    rows = []
    for sym in close_df.columns:
        c    = today_close.get(sym)
        o    = today_open.get(sym)
        pc   = prev_close.get(sym)
        vol  = today_vol.get(sym)
        sma  = today_sma50.get(sym)

        if any(v is None or np.isnan(v) for v in [c, o, pc, vol, sma]):
            continue
        if sma <= 0:
            continue

        gap_pct   = (o - pc) / pc
        move_pct  = (c - pc) / pc
        vol_ratio = vol / sma

        ep_type = None
        if gap_pct >= 0.01 and move_pct >= 0.07 and vol_ratio >= 3.0:
            ep_type = "BULLISH"
        elif gap_pct <= -0.01 and move_pct <= -0.07 and vol_ratio >= 3.0:
            ep_type = "BEARISH"

        if ep_type:
            rows.append((
                run_date, sym, ep_type,
                round(float(o), 2), round(float(c), 2), round(float(pc), 2),
                round(gap_pct * 100, 2), round(move_pct * 100, 2),
                round(vol_ratio, 2),
                int(vol), round(float(sma), 0),
            ))

    if rows:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM scanner_ep WHERE run_date = %s", (run_date,))
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=200)
        conn.commit()
        bullish = sum(1 for r in rows if r[2] == "BULLISH")
        bearish = sum(1 for r in rows if r[2] == "BEARISH")
        log.info("scan_ep: %d bullish, %d bearish EPs for %s", bullish, bearish, run_date)
    else:
        log.info("scan_ep: no EPs found for %s", run_date)

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
