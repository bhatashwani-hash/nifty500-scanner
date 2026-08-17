"""
scan_vcp_bear.py  —  inverse VCP (bearish contraction) short setups.

The mirror image of the long VCP:
  1. BIG FALL on HIGH VOLUME — 3-month return <= -25%, and the heaviest
     down-days of that fall ran >= 1.5x the 20-day average volume.
  2. CONTRACTION — the last 15 sessions trade in a range under 15%
     (sellers resting, weak hands done, volatility drying up).
  3. FALLING MOVING AVERAGE overhead — the 20 DMA is declining
     (lower than it was 10 sessions ago) and price sits BELOW it.
     `touching` flags stocks whose 15-day high rallied to within 2% of
     that falling 20 DMA — the classic short-the-touch location.

Full refresh into scanner_vcp_bear for the latest bar date.
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

FALL_3M_MAX = -15.0     # % — minimum size of the down leg (large caps rarely drop 25%+)
VOLX_MIN    = 1.5       # top down-day volume vs 20d avg
RANGE_MAX   = 15.0      # % — 15-day contraction ceiling
TOUCH_PCT   = 0.98      # 15d high >= 98% of the falling 20 DMA = "touching"

UPSERT_SQL = """
    INSERT INTO scanner_vcp_bear
      (run_date, symbol, close, fall_3m_pct, fall_volx, range_15d_pct,
       high_15d, low_15d, sma20, sma20_falling, touching)
    VALUES %s
    ON CONFLICT (run_date, symbol) DO UPDATE SET
      close=EXCLUDED.close, fall_3m_pct=EXCLUDED.fall_3m_pct,
      fall_volx=EXCLUDED.fall_volx, range_15d_pct=EXCLUDED.range_15d_pct,
      high_15d=EXCLUDED.high_15d, low_15d=EXCLUDED.low_15d,
      sma20=EXCLUDED.sma20, sma20_falling=EXCLUDED.sma20_falling,
      touching=EXCLUDED.touching
"""


def run(conn, run_date: date | None = None):
    log.info("scan_vcp_bear: loading data …")
    df = pd.read_sql(
        """SELECT o.symbol, o.time::date AS date, o.high, o.low, o.close, o.volume
           FROM ohlcv o
           JOIN stocks s ON o.symbol = s.symbol
           WHERE s.is_active = true AND s.is_liquid = true AND o.time >= now() - interval '200 days'
           ORDER BY date""",
        conn, parse_dates=["date"],
    )
    closes = df.pivot(index="date", columns="symbol", values="close").sort_index()
    highs  = df.pivot(index="date", columns="symbol", values="high").sort_index()
    lows   = df.pivot(index="date", columns="symbol", values="low").sort_index()
    vols   = df.pivot(index="date", columns="symbol", values="volume").sort_index()

    n = len(closes)
    if n < 80:
        log.warning("scan_vcp_bear: not enough history")
        return 0
    run_date = run_date or closes.index[-1].date()

    sma20 = closes.rolling(20, min_periods=15).mean()
    rows = []
    for sym in closes.columns:
        c = closes[sym].values
        if np.isnan(c[-1]) or n < 80 or np.isnan(c[-64]):
            continue
        # 1. big fall
        fall_3m = (c[-1] / c[-64] - 1) * 100
        if not fall_3m <= FALL_3M_MAX:
            continue
        # ... on high volume: top-3 down-day volumes in the 3M leg vs 20d avg
        v = vols[sym].values
        seg_c, seg_v = c[-64:], v[-64:]
        down = seg_v[1:][np.diff(seg_c) < 0]
        down = down[~np.isnan(down)]
        avg20 = np.nanmean(v[-20:])
        if len(down) < 3 or not avg20 or np.isnan(avg20) or avg20 <= 0:
            continue
        volx = float(np.sort(down)[-3:].mean() / avg20)
        if volx < VOLX_MIN:
            continue
        # 2. contraction
        h15 = np.nanmax(highs[sym].values[-15:])
        l15 = np.nanmin(lows[sym].values[-15:])
        if not l15 or np.isnan(h15) or np.isnan(l15) or l15 <= 0:
            continue
        rng = (h15 - l15) / l15 * 100
        if not rng < RANGE_MAX:
            continue
        # 3. falling 20 DMA overhead
        s20 = sma20[sym].values
        if np.isnan(s20[-1]) or np.isnan(s20[-11]):
            continue
        falling = s20[-1] < s20[-11]
        below = c[-1] < s20[-1]
        if not (falling and below):
            continue
        touching = h15 >= s20[-1] * TOUCH_PCT

        rows.append((run_date, sym, round(float(c[-1]), 2), round(float(fall_3m), 2),
                     round(volx, 2), round(float(rng), 2), round(float(h15), 2),
                     round(float(l15), 2), round(float(s20[-1]), 2), bool(falling),
                     bool(touching)))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_vcp_bear")
        if rows:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, rows, page_size=200)
    conn.commit()
    log.info("scan_vcp_bear: %d bear-VCP setups for %s (%d touching the falling 20DMA)",
             len(rows), run_date, sum(1 for r in rows if r[-1]))
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
