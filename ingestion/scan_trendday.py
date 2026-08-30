"""
scan_trendday.py  —  Intraday "strong trend day" scanner (hourly bars).

Flags stocks that are building a +6% to +10% trend day while the session is
still running, and scores how likely that is to finish.

Calibration
-----------
Derived from 16,434 stock-days across the NSE top 500 by market cap,
2026-07-13 -> 2026-08-26 (33 sessions, 172 days closing >= +6%).  Findings that
drive the model:

  * Base rate of a >= +6% close is 1.05% of stock-days.
  * The 10:15 return is by far the strongest single predictor: >= +3% lifts the
    probability to 22.6%, >= +4% to 37.4%.
  * First-hour RVOL (vs the stock's *own* trailing first-hour volume, not its
    daily average) is cleanly monotonic and roughly doubles or halves the base:
    <1.5x -> 0.43x, >=10x -> 1.86x.
  * The 11:15 bar is the cheapest filter available: a green hour 2 raises
    P(>= +6%) from 22.6% to 37.6%, a red one drops it to 10.1%.
  * On a real trend day the open is the low (median open_vs_low = 0.04) and
    volume keeps arriving — hour 1 is only 32% of the day's volume for winners
    vs 42% for fades.
  * "Closes at the top of the first-hour range" carries NO information once you
    condition on being up 3% — winners 0.85, fades 0.86.  Deliberately unused.

The score multiplies three measured marginals (return band x RVOL band x
hour-2 confirmation) rather than a fitted 2-D grid: the joint cells are too
thin (several sit on n < 25) to trust.  Calibration is in-sample, so treat the
absolute percentages as optimistic and the *ordering* as the useful part.
Recalibrate with tools/calibrate_trendday.py as more hourly history accrues.

Written to scanner_trendday (one row per symbol, current session only).
Called from fetch_hourly.py after the hourly bars are upserted.

Usage:
    python ingestion/scan_trendday.py          # score the current session
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(Path(__file__).parent.parent / ".env")
log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# --- calibrated constants (see module docstring) -------------------------
MIN_H1_RET = 0.015          # below +1.5% at 10:15 nothing is worth listing
BASE = [(.06, 76.0), (.04, 23.6), (.03, 6.6), (.02, 3.7), (.015, 1.0)]
RVOL_MULT = [(10, 1.86), (5, 1.51), (3, 0.76), (1.5, 0.54), (0, 0.43)]
H2_MULT = [(.01, 3.34), (.005, 1.23), (-.005, 0.68), (-9, 0.43)]
SCORE_CAP = 85.0            # top band measured 75% actual vs 95% modelled

INSERT_SQL = """
INSERT INTO scanner_trendday
    (symbol, trade_date, last_ts, slot_no, prev_close, day_open, last_close,
     day_high, day_low, gap_pct, cum_pct, h1_ret, h1_rvol, h1_pos, h1_range,
     h2_bar, v2_v1, open_vs_low, held_open, score, stage, updated_at)
VALUES %s
ON CONFLICT (symbol) DO UPDATE SET
    trade_date=EXCLUDED.trade_date, last_ts=EXCLUDED.last_ts,
    slot_no=EXCLUDED.slot_no, prev_close=EXCLUDED.prev_close,
    day_open=EXCLUDED.day_open, last_close=EXCLUDED.last_close,
    day_high=EXCLUDED.day_high, day_low=EXCLUDED.day_low,
    gap_pct=EXCLUDED.gap_pct, cum_pct=EXCLUDED.cum_pct,
    h1_ret=EXCLUDED.h1_ret, h1_rvol=EXCLUDED.h1_rvol, h1_pos=EXCLUDED.h1_pos,
    h1_range=EXCLUDED.h1_range, h2_bar=EXCLUDED.h2_bar, v2_v1=EXCLUDED.v2_v1,
    open_vs_low=EXCLUDED.open_vs_low, held_open=EXCLUDED.held_open,
    score=EXCLUDED.score, stage=EXCLUDED.stage, updated_at=now()
"""


def _pick(table, value, default=1.0):
    for threshold, factor in table:
        if value >= threshold:
            return factor
    return default


def score_row(h1_ret, h1_rvol, h2_bar=None):
    """Modelled P(day closes >= +6%), in percent. 0 = not a candidate."""
    if h1_ret is None or not np.isfinite(h1_ret) or h1_ret < MIN_H1_RET:
        return 0.0
    p = _pick(BASE, h1_ret, 0.0)
    p *= _pick(RVOL_MULT, h1_rvol if np.isfinite(h1_rvol) else 0.0, 0.43)
    if h2_bar is not None and np.isfinite(h2_bar):
        p *= _pick(H2_MULT, h2_bar, 0.43)
    return round(min(SCORE_CAP, max(0.2, p)), 1)


def classify(score, h1_ret, h2_bar, held_open, cum_pct):
    """Human-readable stage for the dashboard.

    Judged on where the stock is *now*, not only on the hour-2 bar — a name can
    confirm at 11:15 and still bleed out by 14:00, so the give-back check below
    is evaluated against the latest close on every re-score.
    """
    h1_pct = h1_ret * 100
    if cum_pct is not None and cum_pct >= 6.0:
        return "CONFIRMED"
    # given back more than half the first-hour gain, or lost the opening price
    if cum_pct is not None and h1_pct > 0 and (cum_pct < h1_pct * 0.5 or not held_open):
        return "FADING"
    if h2_bar is not None and np.isfinite(h2_bar) and h2_bar < -0.005:
        return "FADING"
    if score >= 20:
        return "SETUP"
    return "WATCH"


def run(conn, run_date: date | None = None):
    """Score the current (or most recent) session from ohlcv_hourly."""
    log.info("scan_trendday: loading hourly bars …")
    bars = pd.read_sql(
        """SELECT h.symbol,
                  (h.ts AT TIME ZONE 'Asia/Kolkata')::date AS d,
                  extract(hour FROM h.ts AT TIME ZONE 'Asia/Kolkata')::int AS hr,
                  h.ts, h.open, h.high, h.low, h.close, h.volume
           FROM ohlcv_hourly h
           JOIN stocks s ON s.symbol = h.symbol
           WHERE s.is_active AND s.is_liquid
             AND h.ts >= now() - interval '30 days'
           ORDER BY h.symbol, h.ts""",
        conn,
    )
    if bars.empty:
        log.warning("scan_trendday: no hourly bars")
        return 0

    # hour 9 = the 09:15 bar, 10 = 10:15, ... 15 = 15:15
    bars = bars[(bars.hr >= 9) & (bars.hr <= 15)].copy()
    bars["slot"] = bars.hr - 8                       # 1..7

    session = run_date or bars.d.max()
    today = bars[bars.d == session]
    if today.empty:
        log.warning("scan_trendday: no bars for %s", session)
        return 0

    # Each stock's own trailing first-hour volume (prior sessions, excl. today).
    h1_hist = bars[(bars.slot == 1) & (bars.d < session)]
    h1_avg = (h1_hist.sort_values("d").groupby("symbol").volume
              .apply(lambda s: s.tail(10).mean()))

    prev = pd.read_sql(
        """SELECT DISTINCT ON (symbol) symbol, close AS prev_close
           FROM ohlcv WHERE time::date < %s ORDER BY symbol, time DESC""",
        conn, params=(session,),
    ).set_index("symbol").prev_close

    rows = []
    for sym, g in today.groupby("symbol"):
        g = g.sort_values("slot")
        by_slot = {int(r.slot): r for r in g.itertuples(index=False)}
        b1 = by_slot.get(1)
        if b1 is None:
            continue
        pc = prev.get(sym)
        if pc is None or not np.isfinite(pc) or pc <= 0:
            continue

        last = g.iloc[-1]
        day_open, day_high, day_low = float(b1.open), float(g.high.max()), float(g.low.min())
        last_close = float(last.close)

        rng1 = float(b1.high) - float(b1.low)
        h1_ret = float(b1.close) / float(pc) - 1
        h1_pos = (float(b1.close) - float(b1.low)) / rng1 if rng1 > 0 else 0.5
        h1_range = rng1 / float(pc)
        av1 = h1_avg.get(sym, np.nan)
        h1_rvol = float(b1.volume) / av1 if av1 and np.isfinite(av1) and av1 > 0 else np.nan

        b2 = by_slot.get(2)
        h2_bar = (float(b2.close) / float(b1.close) - 1) if b2 is not None else None
        v2_v1 = (float(b2.volume) / float(b1.volume)
                 if b2 is not None and float(b1.volume) > 0 else None)

        sc = score_row(h1_ret, h1_rvol, h2_bar)
        if sc <= 0:
            continue

        rngd = day_high - day_low
        open_vs_low = (day_open - day_low) / rngd if rngd > 0 else 0.5
        held_open = bool((g.close >= day_open * 0.998).all())
        cum_pct = (last_close / float(pc) - 1) * 100
        gap_pct = (day_open / float(pc) - 1) * 100

        rows.append((
            sym, session, last.ts.to_pydatetime(), int(last.slot),
            round(float(pc), 2), round(day_open, 2), round(last_close, 2),
            round(day_high, 2), round(day_low, 2),
            round(gap_pct, 2), round(cum_pct, 2),
            round(h1_ret * 100, 2),
            round(float(h1_rvol), 2) if np.isfinite(h1_rvol) else None,
            round(h1_pos, 2), round(h1_range * 100, 2),
            round(h2_bar * 100, 2) if h2_bar is not None else None,
            round(v2_v1, 2) if v2_v1 is not None else None,
            round(open_vs_low, 2), held_open, sc,
            classify(sc, h1_ret, h2_bar, held_open, cum_pct),
            datetime.now(IST),
        ))

    with conn.cursor() as cur:
        cur.execute("DELETE FROM scanner_trendday")
        if rows:
            execute_values(cur, INSERT_SQL, rows, page_size=500)
    conn.commit()

    n_hot = sum(1 for r in rows if r[19] >= 20)
    log.info("scan_trendday: %d candidates for %s (%d scoring >= 20%%)",
             len(rows), session, n_hot)
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
