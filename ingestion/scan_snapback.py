"""
scan_snapback.py  —  undercut & rally ("snapback") scanner, F&O universe only.

The setup: a stock flushes to a fresh multi-week/month low, then snaps back —
either reversing hard the SAME day (spring/hammer: close in the top of the
day's range) or reclaiming the broken low level within the next 1-2 sessions.

Detection (lows from the last 5 trading days):
  * Low day D: intraday low undercuts the min low of the prior N sessions.
    Windows checked deepest-first:  1Y=252  6M=126  3M=63  1M=21.
  * SAME_DAY snap: close of D in the top 35% of D's range.
  * D1/D2 snap: close of D+1 or D+2 back ABOVE the broken level.
  * One row per symbol — deepest window wins, then most recent low.

Score (0-4): +1 window >= 3M, +1 SAME_DAY reversal bar,
             +1 flush volume >= 1.5x 20d avg, +1 snap volume >= 1.5x.

Full refresh into scanner_snapback (table holds only the latest run).
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

WINDOWS = [("1Y", 252), ("6M", 126), ("3M", 63), ("1M", 21)]   # deepest first
LOOKBACK_DAYS = 5          # low days considered: last 5 sessions
CLOSE_POS_MIN = 0.65       # SAME_DAY bar: close in top 35% of range
VOL_HOT = 1.5              # volume ratio that scores a point

UPSERT_SQL = """
    INSERT INTO scanner_snapback
      (run_date, symbol, low_window, low_date, extreme_low, broken_level,
       snap_type, snap_date, snap_close, last_close, pct_off_low, close_pos,
       flush_vol_ratio, snap_vol_ratio, score)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
       low_window=EXCLUDED.low_window, low_date=EXCLUDED.low_date,
       extreme_low=EXCLUDED.extreme_low, broken_level=EXCLUDED.broken_level,
       snap_type=EXCLUDED.snap_type, snap_date=EXCLUDED.snap_date,
       snap_close=EXCLUDED.snap_close, last_close=EXCLUDED.last_close,
       pct_off_low=EXCLUDED.pct_off_low, close_pos=EXCLUDED.close_pos,
       flush_vol_ratio=EXCLUDED.flush_vol_ratio,
       snap_vol_ratio=EXCLUDED.snap_vol_ratio, score=EXCLUDED.score
"""


def _f(v, nd=2):
    return None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), nd)


def run(conn, run_date: date | None = None):
    log.info("scan_snapback: loading F&O OHLCV …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.high, o.low, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true AND s.is_fno = true
           ORDER BY date""",
        conn, parse_dates=["date"],
    )
    lows   = df.pivot(index="date", columns="symbol", values="low").sort_index()
    highs  = df.pivot(index="date", columns="symbol", values="high").sort_index()
    closes = df.pivot(index="date", columns="symbol", values="close").sort_index()
    vols   = df.pivot(index="date", columns="symbol", values="volume").sort_index()

    n = len(lows)
    if n < 25:
        log.warning("scan_snapback: not enough history")
        return 0
    run_date = run_date or lows.index[-1].date()

    # gap-tolerant windows: a single missing bar (NaN in the pivot) must not
    # blank the whole rolling min/mean for a month — require ~90% coverage
    avg20 = vols.shift(1).rolling(20, min_periods=10).mean()
    # prior N-session min low, per window (value at row t = min of lows t-N..t-1)
    prior_min = {label: lows.shift(1).rolling(N, min_periods=max(5, int(N * 0.9))).min()
                 for label, N in WINDOWS}

    rows, first_pos = [], max(22, n - LOOKBACK_DAYS)
    for sym in lows.columns:
        lo, hi, cl, vo = (x[sym].values for x in (lows, highs, closes, vols))
        best = None    # (window_rank, low_pos, label)
        for pos in range(first_pos, n):
            if np.isnan(lo[pos]):
                continue
            for rank, (label, N) in enumerate(WINDOWS):
                if pos < N:
                    continue
                pm = prior_min[label][sym].values[pos]
                if np.isnan(pm) or lo[pos] >= pm:
                    continue
                # deepest window for this day found (WINDOWS is deepest-first)
                if best is None or rank < best[0] or (rank == best[0] and pos > best[1]):
                    best = (rank, pos, label)
                break
        if best is None:
            continue

        _, p, label = best
        broken = float(prior_min[label][sym].values[p])
        rng = hi[p] - lo[p]
        close_pos = (cl[p] - lo[p]) / rng if rng > 0 else 0.0
        same_day = close_pos >= CLOSE_POS_MIN

        snap_type = snap_pos = None
        if same_day:
            snap_type, snap_pos = "SAME_DAY", p
        else:
            for k in (1, 2):
                if p + k < n and not np.isnan(cl[p + k]) and cl[p + k] > broken:
                    snap_type, snap_pos = f"D{k}", p + k
                    break
        if snap_type is None:
            continue                       # flushed but never snapped — not a signal

        a20 = avg20[sym].values
        flush_vr = vo[p] / a20[p] if a20[p] and not np.isnan(a20[p]) and a20[p] > 0 else None
        snap_vr = (vo[snap_pos] / a20[snap_pos]
                   if a20[snap_pos] and not np.isnan(a20[snap_pos]) and a20[snap_pos] > 0 else None)

        last_close = cl[-1] if not np.isnan(cl[-1]) else None
        score = int(label in ("3M", "6M", "1Y")) + int(same_day) \
              + int(bool(flush_vr and flush_vr >= VOL_HOT)) \
              + int(bool(snap_vr and snap_vr >= VOL_HOT))

        rows.append((
            run_date, sym, label, lows.index[p].date(), _f(lo[p]), _f(broken),
            snap_type, lows.index[snap_pos].date(), _f(cl[snap_pos]), _f(last_close),
            _f((last_close / lo[p] - 1) * 100) if last_close else None,
            _f(close_pos, 2), _f(flush_vr, 2), _f(snap_vr, 2), score,
        ))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_snapback")
        if rows:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    log.info("scan_snapback: %d snapbacks for %s", len(rows), run_date)
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
