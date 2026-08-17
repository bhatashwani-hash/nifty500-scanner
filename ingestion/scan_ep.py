"""
scan_ep.py  —  Episodic Pivot scanner (Bullish & Bearish), 6-month leaderboard.

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

Universe: the 500-stock `stocks` table (active), prices from `ohlcv`.

Unlike the other scanners (latest day only), this keeps EVERY EP that fired in
the trailing 6 months and ranks them by return from the pivot close to the
latest available close:

  return_since_pivot = (last_close / pivot_close - 1) × 100   (raw price return)
  rnk                = 1 = highest return-since-pivot (descending)

The whole table is refreshed on each run. Results written to scanner_ep.
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

LOOKBACK_MONTHS = 6

INSERT_SQL = """
    INSERT INTO scanner_ep
        (run_date, symbol, ep_type, open, close, prev_close,
         gap_pct, move_pct, vol_ratio, volume, vol_avg_50d,
         last_date, last_close, return_since_pivot, rnk)
    VALUES %s
"""


def run(conn, run_date: date | None = None):
    log.info("scan_ep: loading OHLCV (stocks universe) …")

    df_raw = pd.read_sql(
        """
        SELECT o.symbol, o.time::date AS date, o.open, o.close, o.volume
        FROM ohlcv o
        JOIN stocks s ON o.symbol = s.symbol
        WHERE s.is_active = true AND s.is_liquid = true
        ORDER BY date
        """,
        conn, parse_dates=["date"],
    )

    open_df  = df_raw.pivot(index="date", columns="symbol", values="open").sort_index()
    close_df = df_raw.pivot(index="date", columns="symbol", values="close").sort_index()
    vol_df   = df_raw.pivot(index="date", columns="symbol", values="volume").sort_index()

    # Need at least 51 rows for shifted 50-day vol SMA
    if len(close_df) < 52:
        log.warning("scan_ep: not enough history (need ≥ 52 days)")
        return 0

    # 50-day vol SMA, shifted by 1 so today's volume isn't in the average
    vol_sma50 = vol_df.rolling(50, min_periods=30).mean().shift(1)

    prev_close = close_df.shift(1)
    gap   = (open_df  - prev_close) / prev_close
    move  = (close_df - prev_close) / prev_close
    vratio = vol_df / vol_sma50.replace(0, np.nan)

    bull = (gap >= 0.01) & (move >= 0.07) & (vratio >= 3.0)
    bear = (gap <= -0.01) & (move <= -0.07) & (vratio >= 3.0)

    last_date  = close_df.index[-1]
    run_date   = run_date or (last_date.date() if hasattr(last_date, "date") else last_date)
    cutoff     = pd.Timestamp(last_date) - pd.DateOffset(months=LOOKBACK_MONTHS)

    # Latest valid close / date per symbol (the "today" reference for returns)
    last_close_s = close_df.apply(lambda s: s.dropna().iloc[-1] if s.notna().any() else np.nan)
    last_date_s  = close_df.apply(lambda s: s.dropna().index[-1] if s.notna().any() else pd.NaT)

    rows = []
    for ep_type, mask in (("BULLISH", bull), ("BEARISH", bear)):
        hits = mask[mask.index >= cutoff].stack()
        hits = hits[hits].index  # MultiIndex (date, symbol) of True cells
        for dt, sym in hits:
            o  = open_df.at[dt, sym]
            c  = close_df.at[dt, sym]
            pc = prev_close.at[dt, sym]
            vol = vol_df.at[dt, sym]
            sma = vol_sma50.at[dt, sym]
            lc  = last_close_s.get(sym)
            ld  = last_date_s.get(sym)
            if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in [o, c, pc, vol, sma, lc]):
                continue
            ret = (lc - c) / c if c else None
            rows.append([
                dt.date() if hasattr(dt, "date") else dt, sym, ep_type,
                float(round(float(o), 2)),
                float(round(float(c), 2)),
                float(round(float(pc), 2)),
                float(round(float(gap.at[dt, sym]) * 100, 2)),
                float(round(float(move.at[dt, sym]) * 100, 2)),
                float(round(float(vratio.at[dt, sym]), 2)),
                int(float(vol)),
                float(round(float(sma), 2)),
                ld.date() if hasattr(ld, "date") else ld,
                float(round(float(lc), 2)),
                float(round(float(ret) * 100, 2)) if ret is not None else None,
            ])

    # Rank globally by return-since-pivot (descending); rnk appended as last field
    rows.sort(key=lambda r: (r[13] is not None, r[13]), reverse=True)
    for i, r in enumerate(rows, start=1):
        r.append(i)

    # Full refresh — this is a rolling 6-month leaderboard
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_ep")
    if rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, INSERT_SQL, [tuple(r) for r in rows], page_size=300)
    conn.commit()

    bullish = sum(1 for r in rows if r[2] == "BULLISH")
    bearish = sum(1 for r in rows if r[2] == "BEARISH")
    log.info("scan_ep: %d EPs over last %d months (%d bullish, %d bearish) as of %s",
             len(rows), LOOKBACK_MONTHS, bullish, bearish, run_date)
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
